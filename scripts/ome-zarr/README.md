# ome-zarr

Convert light-sheet and serial two-photon data to sharded OME-Zarr 0.6 and
view it in [neuroglancer](https://github.com/google/neuroglancer).

| Script | Input | Output |
|---|---|---|
| `brainsaw_to_zarr.py` | BrainSaw acquisition directories (`recipe_*.yml` + `stitchedImages_100/`) | `<dir>/<dir name>.ome.zarr` |
| `ome_tiff_to_zarr.py` | Directories of OME-TIFFs (Imspector `*_C00.ome.tif`, ... or a multi-channel `*.ome.tif`) | `<name>.ome.zarr` beside each input |
| `neuroglancer_state_files.py` | An `.ome.zarr`, or a directory of them | `<name>.ngl.json` beside each store, plus a neuroglancer link |

Each script's docstring has the full details (layout, memory use, progress
output); `--help` prints the options.

## Converting

The converters are [uv](https://docs.astral.sh/uv/) scripts: `uv run`
installs their dependencies.

```bash
uv run brainsaw_to_zarr.py --shard 512 /data/brain1 /data/brain2
uv run ome_tiff_to_zarr.py --shard 512 /data/lsfm_scan --out-dir /data/zarr
```

Both take the same options:

| Option | Default | |
|---|---|---|
| `--out-dir` | beside the input | where to write the `.ome.zarr` |
| `--levels` | auto | pyramid levels; by default until the top level is one chunk across in Y and X |
| `--chunk` | 64 | chunk size in voxels |
| `--shard` | none | shard size, a multiple of `--chunk`; 512 is a good choice for neuroglancer |
| `--clevel` | 3 | blosc-zstd compression level |
| `--workers` | 2 | bands encoded and written at once; more workers, more memory |
| `--readers` | 8 | parallel reads per band; use 1 on a single local disk |

An existing output is never overwritten: delete it to redo a conversion.

Memory is roughly 3 GB plus `(2.25 x workers + 1)` bands, a band being
`channels x shard x shard x X x 2` bytes. For 3 channels, `--shard 512` and
X = 6481 that is ~60 GB; `--shard 256` quarters it.

### On Slurm

In an array job each task converts one directory, `dirs[SLURM_ARRAY_TASK_ID]`.
With one directory per line in `dirs.txt`:

```bash
#!/bin/bash
#SBATCH -J ome-zarr-convert
#SBATCH -p cpu
#SBATCH -N 1
#SBATCH -t 0-3:0
#SBATCH --cpus-per-task 8
#SBATCH --mem 64G
#SBATCH -o ome-zarr_%A_%a.log
#SBATCH -e ome-zarr_%A_%a.err
#SBATCH --array 0-4            # 0 .. (lines in dirs.txt - 1)

module load uv
cd /path/to/script/location
uv run brainsaw_to_zarr.py --shard 512 $(cat dirs.txt)
```

## Viewing in neuroglancer

Serve the stores over HTTPS (e.g. a Globus guest collection with public read
access), then write a state file for each one. `--url` is the URL the given
directory is served at:

```bash
python neuroglancer_state_files.py /ceph/.../vibraint_lsfm_rat/26_300051_ULLS \
    --url https://g-5d10b4.3cf00.03c0.data.globus.org/26_300051_ULLS
```

It finds `*.ome.zarr` and `*/*.ome.zarr`, writes `<name>.ngl.json` next to
each, and prints a link to open it, e.g.
`https://neuroglancer-demo.appspot.com/#!https://.../<acquisition>/<name>.ngl.json`.
Each channel is an additive layer coloured and contrast-windowed from the
store's `omero` metadata.

| Option | Default | |
|---|---|---|
| `--url` | required | URL the directory (or the store's directory) is served at |
| `--viewer` | `https://neuroglancer-demo.appspot.com/` | neuroglancer instance for the printed links |
| `--volume-rendering [GAIN]` | off | volume render the 3D view (experimental); gain 0 if not given, lower is dimmer |

Standard library only, so plain `python` works.
