# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "dask>=2024.1",
#     "ngff-zarr>=0.49",
#     "numpy",
#     "pyyaml",
#     "tensorstore>=0.1.70",
#     "tifffile>=2024.1",
# ]
# ///
"""Convert BrainSaw acquisitions to sharded OME-Zarr 0.6 for neuroglancer.

Each input directory is one acquisition as BakingTray and StitchIt leave
it: ``recipe_*.yml`` plus ``stitchedImages_100/<channel>/
section_<SSS>_<OO>.tif``, one uncompressed TIFF per optical plane.
Planes are stacked in acquisition order (section, then optical plane).
It is written as ``<dir>/<dir name>.ome.zarr``, or into ``--out-dir``.

Voxel size: X = Y = the mean of the recipe's StitchingParameters
VoxelSize, as StitchIt itself uses; Z = VoxelSize.Z, the optical plane
spacing. Channels are labelled by number (ch02, ch03, ...).

Under a Slurm array job each task converts ``dirs[SLURM_ARRAY_TASK_ID]``;
run outside Slurm it converts every directory in turn. E.g. with one
directory per line in dirs.txt::

    #!/bin/bash
    #SBATCH --job-name=ome-zarr
    #SBATCH --array=0-9            # 0 .. (lines in dirs.txt - 1)
    #SBATCH --cpus-per-task=8
    #SBATCH --mem=64G              # see the memory note below
    #SBATCH --time=3:00:00
    #SBATCH --output=ome-zarr_%A_%a.log
    python brainsaw_to_zarr.py --shard 512 $(cat dirs.txt)

How it is written. The output has one array per pyramid level: s0 at
full resolution, then s1, s2, ... each half the size of the one before
in Y and X (and in Z once the voxels are close to isotropic). Each
array is stored as shards, cubes of --shard voxels (or files of --chunk
cubes without --shard).

s0 is copied from the source one *band* at a time. A band is all
channels, one shard deep in Z and one shard tall in Y, across the full
width in X: a row of shards side by side along X, (C, shard, shard, X).

    one shard-thick layer of s0, seen looking down Z:

          X ->
     +-------+-------+-------+-------+
  Y  | shard | shard | shard | shard |   <- one band
  |  +-------+-------+-------+-------+
  v  |    next band, below in Y      |
     +-------+-------+-------+-------+

Bands are read in order, so every s0 shard is written once, whole. s1
is block-meaned from each band while it is still in memory; each later
level is made from the one before it, one shard at a time. Chunks are
blosc(zstd, byte-shuffle) compressed; tensorstore encodes and writes
the shards on one thread per allocated CPU.

Memory, worst case (reads outpacing writes), is about 3 GB plus
(2.25 x workers + 1) bands, a band being C x shard x shard x X x 2
bytes (6.1 GB for 3 channels, X = 3873, --shard 512). When Z doesn't
halve at level 1 the s1 bands are twice as large, so allow about 7 bands
at the default 2 workers. --shard 256 quarters it. The input is read
with pread and dropped from the page cache, so it never counts against
the job's memory.

Progress lines say what share of the time the reader (the thread
reading bands) spends reading: near 100% means read-bound (try more
--readers), well below means the workers (CPU or output writes) are the
limit.
"""

import argparse
import dataclasses
import json
import math
import os
import re
import socket
import sys
import threading
import time
import warnings
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from pathlib import Path
from typing import Callable, NamedTuple

import dask.array as da
import ngff_zarr
import numpy as np
import tensorstore as ts
import tifffile
import yaml


class Source(NamedTuple):
    """What an opener returns: everything the writer needs."""

    shape: tuple  # (C, Z, Y, X)
    dtype: np.dtype
    read: Callable  # read(z, y) -> one (C, z, y, X) band, see band_reader
    voxel_size: tuple  # (x, y, z) µm
    names: list  # channel labels


# --- reading -----------------------------------------------------------


def band_reader(plane, n_channels, dtype, shape_zyx, readers):
    """Return ``read(z, y)``, which reads one band (or any z, y region of
    full X) and returns it as a (C, len(z), len(y), X) array.

    ``z`` and ``y`` are slices of plane and row indices. ``plane(c, z)``
    gives the file and byte offset of channel c's plane z, whose rows
    are stored uncompressed and back to back, so rows ``y`` of a plane
    are one contiguous run of bytes. ``read`` fetches that run from each
    of the band's C x len(z) planes with one pread, ``readers`` at a
    time on a thread pool: network filesystems such as Ceph serve
    parallel reads much faster than one after another.

    Read pages are dropped from the page cache, so the input never
    counts against the job's memory. Files are opened per read: a
    BrainSaw stack is one file per plane, more than the usual open-file
    limit."""
    _, _, nx = shape_zyx
    row_bytes = nx * dtype.itemsize
    pool = ThreadPoolExecutor(readers, thread_name_prefix="reader")

    def read_plane(out, c, z, y0):
        """Fill ``out`` with rows y0 .. y0 + len(out) of channel c's plane
        z."""
        path, plane_start = plane(c, z)
        offset = plane_start + y0 * row_bytes
        # out's memory as raw bytes, so preadv reads straight into it
        buf = memoryview(out).cast("B")
        fd = os.open(path, os.O_RDONLY)
        try:
            got = 0
            # a read may return fewer bytes than asked for (network
            # filesystems often do), so read until the buffer is full
            while got < len(buf):
                n = os.preadv(fd, [buf[got:]], offset + got)
                if n == 0:
                    raise EOFError(f"{path} ends at {offset + got}")
                got += n
            # these bytes are not read again: drop them from the page cache
            os.posix_fadvise(fd, offset, got, os.POSIX_FADV_DONTNEED)
        finally:
            os.close(fd)

    def read(z, y):
        zs = range(z.start, z.stop)
        band = np.empty((n_channels, len(zs), y.stop - y.start, nx), dtype)
        # one pread per (channel, plane), each into its own slice of band
        jobs = [
            pool.submit(read_plane, band[c, i], c, zi, y.start)
            for c in range(n_channels)
            for i, zi in enumerate(zs)
        ]
        for job in jobs:
            job.result()  # wait, re-raising any read error
        return band

    return read


# --- BrainSaw ----------------------------------------------------------


_PLANE = re.compile(r"section_(\d+)_(\d+)\.tiff?")  # not ._section_ (macOS)


def _layout(path):
    """(shape, dtype, data offset) of one stitched plane."""
    with tifffile.TiffFile(path) as t:
        p = t.pages[0]
        assert p.is_contiguous and p.compression == 1, (
            f"{path} is compressed or not contiguous"
        )
        return p.shape, p.dtype.newbyteorder(t.byteorder), p.dataoffsets[0]


def _channel_planes(channel_dir):
    """A channel's plane files in z order, and planes per section. The
    sections x optical planes grid must be complete."""
    found = {}
    for f in channel_dir.iterdir():
        if m := _PLANE.fullmatch(f.name):
            found[int(m[1]), int(m[2])] = f
    assert found, f"no section_*.tif in {channel_dir}"
    sections, opticals = (range(min(v), max(v) + 1) for v in zip(*found))
    want = [(s, o) for s in sections for o in opticals]
    missing = [
        f"section_{s:03d}_{o:02d}" for s, o in want if (s, o) not in found
    ]
    assert not missing, (
        f"{channel_dir}: {len(missing)} planes missing: {missing[:3]}"
    )
    return [found[k] for k in want], len(opticals)


def open_brainsaw(directory, readers=8):
    """Open a BrainSaw directory as an ``Source``."""
    d = Path(directory)
    (recipe,) = d.glob("recipe_*.yml")
    r = yaml.safe_load(recipe.read_text())
    stitch = r["StitchingParameters"]["VoxelSize"]
    xy = (stitch["X"] + stitch["Y"]) / 2
    voxel_size = (xy, xy, float(r["VoxelSize"]["Z"]))

    chans = sorted(
        (c for c in (d / "stitchedImages_100").iterdir() if c.name.isdigit()),
        key=lambda c: int(c.name),
    )
    assert chans, f"no channel directories in {d / 'stitchedImages_100'}"
    per_channel = [_channel_planes(c) for c in chans]
    assert len({len(f) for f, _ in per_channel}) == 1, (
        "channels differ in planes"
    )
    files = [f for fs, _ in per_channel for f in fs]  # channel-major
    nz, per_section = len(per_channel[0][0]), per_channel[0][1]
    cut = r["mosaic"]["sliceThickness"] * 1000  # mm -> µm
    if abs(cut - per_section * voxel_size[2]) > 1e-6:
        warnings.warn(
            f"{per_section} planes x {voxel_size[2]} µm per {cut} µm section: "
            "z spacing is not uniform, but is written as if it were"
        )

    with ThreadPoolExecutor(readers) as pool:  # thousands of headers
        layouts = list(pool.map(_layout, files))
    assert len({(s, str(t)) for s, t, _ in layouts}) == 1, (
        "stitched planes differ in shape or dtype"
    )
    (ny, nx), dtype, _ = layouts[0]

    def plane(c, z):
        i = c * nz + z
        return files[i], layouts[i][2]

    return Source(
        shape=(len(chans), nz, ny, nx),
        dtype=dtype,
        read=band_reader(plane, len(chans), dtype, (nz, ny, nx), readers),
        voxel_size=voxel_size,
        names=[f"ch{int(c.name):02d}" for c in chans],
    )


# --- pyramid -----------------------------------------------------------


def level_factors(voxel_size, n_levels):
    """Cumulative (z, y, x) factor per level: X and Y halve every level,
    Z only once the XY spacing has caught up with it."""
    vx, vy, vz = voxel_size
    xy = min(vx, vy)
    fz = fy = fx = 1
    rows = [(1, 1, 1)]
    for _ in range(1, n_levels):
        fy *= 2
        fx *= 2
        # Z halves too once this level's XY spacing reaches its Z spacing
        if xy * fx >= vz * fz:
            fz *= 2
        rows.append((fz, fy, fx))
    return np.array(rows)


def auto_levels(shape_zyx, voxel_size, chunk):
    """Levels until the top one is at most one chunk across in Y and X,
    or until one more would shrink an axis to nothing."""
    shape = np.array(shape_zyx)
    n = 1
    while True:
        top = shape // level_factors(voxel_size, n)[-1]
        one_more = shape // level_factors(voxel_size, n + 1)[-1]
        if top[1:].max() <= chunk or one_more.min() < 1:
            return n
        n += 1


def _sum_by(a, axis, s):
    """Add up each run of ``s`` neighbouring elements along ``axis``, as
    uint32 so sums of uint16 can't overflow; ``a`` itself for s = 1. For
    s = 2 along the last axis this is a[..., 0::2] + a[..., 1::2]. The
    length along ``axis`` must be a multiple of ``s``.

    Adding strided views like this is several times faster than
    reshaping ``a`` and calling ``.sum()``."""
    if s == 1:
        return a

    def every_sth(start):  # a[start::s] along axis
        index = [slice(None)] * a.ndim
        index[axis] = slice(start, None, s)
        return a[tuple(index)]

    total = every_sth(0).astype(np.uint32)
    for start in range(1, s):
        total += every_sth(start)
    return total


def _mean(block, step):
    """Block mean of a (C, Z, Y, X) array: each output voxel is the mean
    of one step = (sz, sy, sx) block of input voxels, rounded half up as
    ngff-zarr's bin shrink does. Partial blocks at the far edges are
    dropped."""
    sz, sy, sx = step
    n = sz * sy * sx  # voxels per block
    c, z, y, x = block.shape
    z, y, x = z - z % sz, y - y % sy, x - x % sx  # whole blocks only
    out = np.empty((c, z // sz, y // sy, x // sx), block.dtype)
    # 64 output planes at a time, to keep the uint32 sums small
    for z0 in range(0, z, 64 * sz):
        sums = block[:, z0 : min(z0 + 64 * sz, z), :y, :x]
        for axis, s in ((1, sz), (2, sy), (3, sx)):
            sums = _sum_by(sums, axis, s)
        # in integers, (sum + n/2) // n is the mean rounded half up
        out[:, z0 // sz : z0 // sz + sums.shape[1]] = (sums + n // 2) // n
    return out


# --- writing -----------------------------------------------------------


class _Progress:
    """Thread-safe progress lines for one level: blocks done, input GB/s
    and ETA, at most every 10 s plus one at the end."""

    def __init__(self, label, nbytes, total):
        self.label, self.nbytes, self.total = label, nbytes, total
        self.done = 0
        self.lock = threading.Lock()
        self.t0 = self.last = time.monotonic()
        # time and bytes s0's reader thread spends reading (_write_s0)
        self.read_seconds = self.read_bytes = 0

    def step(self):
        """Count one finished block; print at most every 10 s, and once
        at the end."""
        with self.lock:
            self.done += 1
            now = time.monotonic()
            finished = self.done == self.total
            if now - self.last < 10 and not finished:
                return
            self.last = now
            elapsed = now - self.t0
            frac = self.done / self.total
            rate = self.nbytes * frac / elapsed / 1e9  # GB/s of input
            reading = ""
            if self.read_seconds:
                reading = (
                    f", reading {self.read_seconds / elapsed:.0%} of the "
                    f"time at {self.read_bytes / self.read_seconds / 1e9:.2f}"
                    " GB/s"
                )
            if finished:
                print(
                    f"  {self.label}: done, {self.total} blocks in "
                    f"{elapsed / 60:.1f} min ({rate:.2f} GB/s in{reading})"
                )
            else:
                print(
                    f"  {self.label}: {self.done}/{self.total} blocks "
                    f"({frac:.0%}), {rate:.2f} GB/s in, "
                    f"{elapsed / 60:.1f} min elapsed, "
                    f"ETA {elapsed / frac - elapsed:.0f} s{reading}"
                )


def _spans(start, stop, step):
    """Slices covering start .. stop in steps of ``step``; the last one
    may be shorter."""
    return [slice(a, min(a + step, stop)) for a in range(start, stop, step)]


def _run(tasks, workers):
    """Run each zero-argument callable from the iterator ``tasks`` on a
    pool of ``workers`` threads.

    At most workers + 1 tasks exist at once (one per worker, plus one
    ready to go): the next task is only taken from ``tasks`` once one
    has finished. For s0, taking a task is what reads a band, so this
    caps the bands in memory. After a failure no more tasks are taken,
    and the error is raised once the running ones have finished."""
    slots = threading.Semaphore(workers + 1)
    failed = threading.Event()

    def run(task):
        try:
            task()
        except BaseException:
            failed.set()
            raise
        finally:
            slots.release()

    with ThreadPoolExecutor(workers) as pool:
        futures = []
        while True:
            slots.acquire()  # wait until fewer than workers + 1 exist
            if failed.is_set():
                break
            task = next(tasks, None)
            if task is None:
                break
            futures.append(pool.submit(run, task))
        for future in futures:
            future.result()  # re-raises a task's error


class _S1Band:
    """One band of s1 (one s1 shard deep and tall, full X). It covers
    ``parts`` s0 bands, as s1 shards span 2 s0 shards in Y (and in Z
    when Z halves): each is block-meaned into its slice of the buffer
    as it is written, and whichever worker adds the last one writes the
    s1 band, so each s1 shard is also written once, whole."""

    def __init__(self, s1, z0, y0, step, parts):
        """The s1 band made from the s0 rows that start at (z0, y0)."""
        n_channels, nz1, ny1, nx1 = s1.shape
        _, shard_z, shard_y, _ = _shard(s1)
        sz, sy, _ = step
        self.s1, self.z0, self.y0, self.step = s1, z0, y0, step
        # where the band sits in s1; the last band stops at s1's edge
        self.z = slice(z0 // sz, min(z0 // sz + shard_z, nz1))
        self.y = slice(y0 // sy, min(y0 // sy + shard_y, ny1))
        # s0 rows past s1's last whole block start past s1's edge: then
        # the slice is empty, and so is the buffer (see add)
        rows_z = max(0, self.z.stop - self.z.start)
        rows_y = max(0, self.y.stop - self.y.start)
        self.buf = np.zeros(
            (n_channels, rows_z, rows_y, nx1), s1.dtype.numpy_dtype
        )
        self.left = parts  # s0 bands still to add
        self.lock = threading.Lock()

    def add(self, z, y, data):
        """Block-mean the s0 band ``data`` at rows (z, y) into the buffer,
        and write the buffer to s1 once every part is in. Workers call
        this at the same time: each part fills its own slice of the
        buffer, so only the count needs the lock."""
        if not self.buf.size:  # past s1's edge: no s1 rows to make
            return
        part = _mean(data, self.step)
        oz = (z.start - self.z0) // self.step[0]
        oy = (y.start - self.y0) // self.step[1]
        self.buf[:, oz : oz + part.shape[1], oy : oy + part.shape[2]] = part
        with self.lock:
            self.left -= 1
            last = self.left == 0
        if last:
            self.s1[:, self.z, self.y].write(self.buf).result()
            self.buf = None  # free the memory now


def _write_s0(s0, s1, read, step, workers, nbytes):
    """Write s0 band by band from ``read(z, y)``: the calling thread
    reads the bands in order and ``workers`` threads write them, at most
    workers + 1 bands held at once. With ``s1``, also block-mean each
    band into the s1 band it falls in (``_S1Band``). Returns whether s1
    was written: not if its shards don't line up with s0's (only small,
    clamped arrays)."""
    _, nz0, ny0, _ = s0.shape
    _, shard_z, shard_y, _ = _shard(s0)
    # The s0 rows that make up one s1 band (one s1 shard deep and tall,
    # times the step), read one after another. Without s1, just one band.
    group_z, group_y = shard_z, shard_y
    if s1 is not None:
        _, s1_shard_z, s1_shard_y, _ = _shard(s1)
        group_z, group_y = s1_shard_z * step[0], s1_shard_y * step[1]
        if group_z % shard_z or group_y % shard_y:
            # an s0 band would straddle two s1 bands; leave s1 to _fill
            s1 = None
            group_z, group_y = shard_z, shard_y
    label = "s0 + s1" if s1 is not None else "s0"
    print(f"writing {label}")
    total = len(_spans(0, nz0, shard_z)) * len(_spans(0, ny0, shard_y))
    progress = _Progress(label, nbytes, total)

    def put(z, y, data, s1_band):
        s0[:, z, y].write(data).result()
        if s1_band is not None:
            s1_band.add(z, y, data)
        progress.step()

    def tasks():
        """Read the s0 bands in order and yield a task to write each."""
        for gz0 in range(0, nz0, group_z):
            for gy0 in range(0, ny0, group_y):
                parts = [
                    (z, y)
                    for z in _spans(gz0, min(gz0 + group_z, nz0), shard_z)
                    for y in _spans(gy0, min(gy0 + group_y, ny0), shard_y)
                ]
                s1_band = None
                if s1 is not None:
                    s1_band = _S1Band(s1, gz0, gy0, step, len(parts))
                for z, y in parts:
                    t = time.monotonic()
                    data = read(z, y)
                    progress.read_seconds += time.monotonic() - t
                    progress.read_bytes += data.nbytes
                    yield partial(put, z, y, data, s1_band)

    _run(tasks(), workers)
    return s1 is not None


def _fill(label, arr, prev, step, workers):
    """Write ``arr`` (C, Z, Y, X) as the block mean of ``prev``, the
    level before it, by ``step``. Each task makes one shard of ``arr``:
    it reads the region of ``prev`` that averages down to it, means it
    and writes it."""
    _, nz, ny, nx = arr.shape
    _, shard_z, shard_y, shard_x = _shard(arr)
    shards = [
        (z, y, x)
        for z in _spans(0, nz, shard_z)
        for y in _spans(0, ny, shard_y)
        for x in _spans(0, nx, shard_x)
    ]
    nbytes = math.prod(prev.shape) * prev.dtype.numpy_dtype.itemsize
    progress = _Progress(label, nbytes, len(shards))

    def put(shard):
        # the same region in prev's coordinates: each index times the step
        src = tuple(
            slice(r.start * s, r.stop * s) for r, s in zip(shard, step)
        )
        data = prev[(slice(None), *src)].read().result()
        arr[(slice(None), *shard)].write(_mean(data, step)).result()
        progress.step()

    print(f"writing {label} {arr.shape}")
    _run((partial(put, s) for s in shards), workers)


def _shard(arr):
    """A tensorstore zarr3 array's shard shape, or its chunk shape when it
    is not sharded: the unit tensorstore writes."""
    return arr.chunk_layout.write_chunk.shape


def _dask(arr):
    """A tensorstore array as a lazy dask array with one block per zarr
    chunk, read when computed (``da.from_array`` can't take tensorstore's
    dtype). With no input arrays, ``map_blocks`` calls ``read`` once per
    block and passes ``block_info``; its ``[None]["array-location"]`` is
    the block's (start, stop) along each axis."""

    def read(block_info=None):
        location = block_info[None]["array-location"]
        return arr[tuple(slice(*b) for b in location)].read().result()

    chunks = arr.chunk_layout.read_chunk.shape
    return da.map_blocks(
        read,
        chunks=da.core.normalize_chunks(chunks, arr.shape),
        dtype=arr.dtype.numpy_dtype,
    )


def write_zarr(out, src, *, levels, chunk, shard, clevel, workers):
    """Write ``src`` as OME-Zarr 0.6 at ``out``: the metadata through
    ngff-zarr, s0 from ``src.read`` and the pyramid from s0."""
    levels = levels or auto_levels(src.shape[1:], src.voxel_size, chunk)
    # factors[k]: level k's (z, y, x) downsampling relative to s0
    factors = level_factors(src.voxel_size, levels)
    nbytes = math.prod(src.shape) * src.dtype.itemsize
    print(
        f"channels={src.names} shape_czyx={src.shape} "
        f"voxel_um={src.voxel_size} levels={levels} "
        f"({nbytes / 1e9:.0f} GB in)"
    )
    # ngff-zarr only writes the metadata, so it gets a lazy all-zeros
    # stand-in of the source's shape and dtype, which is never computed.
    # The data is written below, from src.read.
    vx, vy, vz = src.voxel_size
    image = ngff_zarr.to_ngff_image(
        da.zeros(src.shape, dtype=src.dtype, chunks=(1, 64, 512, -1)),
        dims=["c", "z", "y", "x"],
        scale={"z": vz, "y": vy, "x": vx},
        name=Path(out).name.removesuffix(".ome.zarr"),
        axes_units=dict.fromkeys("zyx", "micrometer"),
        channel_names=src.names,
    )
    multiscales = ngff_zarr.to_multiscales(
        image,
        # plain ints, not numpy ones
        scale_factors=[dict(zip("zyx", map(int, f))) for f in factors[1:]],
        method=ngff_zarr.Methods.DASK_BIN_SHRINK,
        chunks=chunk,
        cache=False,
    )
    # to_multiscales names the levels "scaleN/<name>"; neuroglancer
    # doesn't care, but s0, s1, ... is the convention. The coordinate
    # transform names its input array too, so rename that as well.
    datasets = multiscales.metadata.datasets
    for k, d in enumerate(datasets):
        d.path = f"s{k}"
        d.coordinateTransformations[0].input.path = f"s{k}"
    blosc = {
        "cname": "zstd",
        "clevel": clevel,
        "shuffle": "shuffle",
        "typesize": src.dtype.itemsize,
        "blocksize": 0,
    }
    ngff_zarr.to_ngff_zarr(
        out,
        multiscales,
        version="0.6",
        compressors=[{"name": "blosc", "configuration": blosc}],
        chunks_per_shard=None if shard is None else shard // chunk,
        metadata_only=True,
    )

    t0 = time.monotonic()
    # tensorstore encodes and writes shards on its own threads, one per
    # CPU the job was given (sched_getaffinity: Slurm's allocation, not
    # every CPU on the node)
    cpus = len(os.sched_getaffinity(0))
    context = ts.Context({"data_copy_concurrency": {"limit": cpus}})
    arrays = [
        ts.open(
            {
                "driver": "zarr3",
                "kvstore": {"driver": "file", "path": str(Path(out) / d.path)},
            },
            context=context,
            open=True,
        ).result()
        for d in datasets
    ]
    # steps[k - 1]: level k's (z, y, x) downsampling relative to level k - 1
    steps = [tuple(map(int, s)) for s in factors[1:] // factors[:-1]]
    if levels > 1:
        s1, s1_step = arrays[1], steps[0]
    else:
        s1, s1_step = None, None
    wrote_s1 = _write_s0(arrays[0], s1, src.read, s1_step, workers, nbytes)
    for k in range(2 if wrote_s1 else 1, levels):
        _fill(f"s{k}", arrays[k], arrays[k - 1], steps[k - 1], workers)

    # omero metadata: channel labels and a 2-98% quantile display window.
    # ngff-zarr computes it from multiscales.images, which still hold the
    # zero stand-ins, so point them at the written levels first. Its
    # update_root_attributes won't set OME keys, so the result is added
    # to the root zarr.json by hand.
    for image, arr in zip(multiscales.images, arrays):
        image.data = _dask(arr)
    omero = ngff_zarr.compute_omero_from_multiscales(
        multiscales, labels=src.names
    )
    root = Path(out) / "zarr.json"
    meta = json.loads(root.read_text())
    meta["attributes"]["ome"]["omero"] = dataclasses.asdict(omero)
    root.write_text(json.dumps(meta, indent=2))
    print(f"done: {out} in {(time.monotonic() - t0) / 60:.1f} min")


# --- command line ------------------------------------------------------


def main(argv=None):
    # the docstring's first paragraph; the rest is for readers of the code
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument(
        "dirs", nargs="+", type=Path, help="BrainSaw acquisition directories"
    )
    p.add_argument(
        "--out-dir",
        type=Path,
        help="where to write <name>.ome.zarr (default: inside each input "
        "directory)",
    )
    p.add_argument(
        "--levels",
        type=int,
        help="pyramid levels (default: until the top level is at most one "
        "chunk across in Y and X)",
    )
    p.add_argument("--chunk", type=int, default=64, help="chunk edge (64)")
    p.add_argument(
        "--shard",
        type=int,
        help="shard edge, a multiple of --chunk (default: no sharding)",
    )
    p.add_argument(
        "--clevel", type=int, default=3, help="blosc-zstd level (3)"
    )
    p.add_argument(
        "--workers",
        type=int,
        default=2,
        help="bands encoded and written at once, see the memory note "
        "above (2)",
    )
    p.add_argument(
        "--readers",
        type=int,
        default=8,
        help="parallel reads per band, for network filesystems like Ceph; "
        "1 is fastest on a single local disk (8)",
    )
    a = p.parse_args(argv)
    if a.shard is not None and a.shard % a.chunk:
        p.error(f"--shard {a.shard} is not a multiple of --chunk {a.chunk}")
    sys.stdout.reconfigure(line_buffering=True)  # progress shows up in logs

    task = os.environ.get("SLURM_ARRAY_TASK_ID")
    dirs = a.dirs
    if task is not None:
        if int(task) >= len(dirs):
            p.error(f"array task {task} but only {len(dirs)} directories")
        dirs = [dirs[int(task)]]
    for d in dirs:
        assert (d / "stitchedImages_100").is_dir(), f"{d}: not BrainSaw"
        out = Path(a.out_dir or d) / f"{d.resolve().name}.ome.zarr"
        print(f"[{socket.gethostname()} task={task}] {d} -> {out}")
        # a half-written store from a killed job would look complete
        assert not out.exists(), f"{out} exists; delete it to redo"
        write_zarr(
            out,
            open_brainsaw(d, a.readers),
            levels=a.levels,
            chunk=a.chunk,
            shard=a.shard,
            clevel=a.clevel,
            workers=a.workers,
        )


if __name__ == "__main__":
    main()
