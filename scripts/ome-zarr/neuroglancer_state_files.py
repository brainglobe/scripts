"""Write a neuroglancer state file next to an OME-Zarr, or every one
under a directory.

Given ``scan.ome.zarr``, writes ``scan.ngl.json`` beside it, which
neuroglancer loads from the printed ``#!<url>`` link. Given a directory,
does that for every ``.ome.zarr`` in it and one level down: a directory
of stores, or of acquisition subdirectories with a store in each (as the
converters write them). Each channel is its own additive layer,
coloured and contrast-windowed from the store's ``omero`` metadata. The
axes are ordered x, y, z so the "xy" panel shows true xy slices.
Standard library only.

``--url`` is the HTTPS URL the given directory (or, for one store, the
store's directory) is served at: on a Globus guest collection with
public read access, the collection's URL for that directory. E.g. with
vibraint_lsfm_rat/ shared at the collection's root:

    cd /ceph/scratch/globus/neuroinformatics/vibraint_lsfm_rat
    python neuroglancer_state_files.py 26_300051_ULLS \\
        --url https://g-5d10b4.3cf00.03c0.data.globus.org/26_300051_ULLS
    -> https://neuroglancer-demo.appspot.com/#!https://g-5d10b4.3cf00.03c0.data.globus.org/26_300051_ULLS/<acquisition>/<name>.ngl.json
"""

import argparse
import json
import math
import sys
from pathlib import Path

_PER_M = {"nanometer": 1e9, "micrometer": 1e6, "millimeter": 1e3}
_PANEL_PX = 1200  # ponytail: fits a 4panel half-screen; tune for big monitors
_SHADER = (  # also volume renders, so the 3D toggle works in the viewer
    "#uicontrol invlerp contrast\n"
    "#uicontrol vec3 color color\n"
    "void main() {\n"
    "  float v = contrast();\n"
    "  if (VOLUME_RENDERING) emitRGBA(vec4(color * v, v));\n"
    "  else emitRGB(color * v);\n"
    "}\n"
)


def state(zarr, url, gain=None):
    ome = json.loads((zarr / "zarr.json").read_text())["attributes"]["ome"]
    (ms,) = ome["multiscales"]
    axes = ms["coordinateSystems"][0]["axes"]
    s0 = ms["datasets"][0]
    shape = json.loads((zarr / s0["path"] / "zarr.json").read_text())["shape"]
    (scale,) = [
        t["scale"]
        for t in s0["coordinateTransformations"][0]["transformations"]
        if "scale" in t
    ]
    space = [
        (a, n, s)
        for a, n, s in zip(axes, shape, scale)
        if a["type"] == "space"
    ]
    dims = {a["name"]: [s / _PER_M[a["unit"]], "m"] for a, _, s in space}
    # Zoom is in canonical voxels (the finest axis) per screen pixel; the
    # default, 1, shows a few hundred voxels of a whole-brain scan.
    extent = max(n * s for _, n, s in space) / min(s for *_, s in space)
    source = f"{url.rstrip('/')}/{zarr.name}/|zarr3:"
    layers = []
    for i, ch in enumerate(ome["omero"]["channels"]):
        w = ch["window"]
        layers.append(
            {
                "type": "image",
                "name": ch["label"],
                "source": source,
                "localDimensions": {"c'": [1, ""]},
                "localPosition": [i],
                "blend": "additive",
                "shader": _SHADER,
                "shaderControls": {
                    "color": f"#{ch['color']}",
                    "contrast": {
                        "range": [w["start"], w["end"]],
                        "window": [w["min"], w["max"]],
                    },
                },
            }
        )
    view = {
        "dimensions": {k: dims[k] for k in "xyz" if k in dims},
        "layers": layers,
        "layout": "4panel-alt",  # xy | yz / xz | 3d
        "crossSectionScale": extent / _PANEL_PX,
        # every layer's contrast and colour controls, docked on the left
        "toolPalettes": {
            "Shader controls": {
                "side": "left",
                "row": 1,
                "query": "type:shaderControl",
            }
        },
    }
    if gain is not None:
        samples = math.sqrt(2) * 2 ** math.ceil(math.log2(extent))
        for layer in layers:
            layer |= {
                "volumeRendering": "on",
                "volumeRenderingGain": gain,
                "volumeRenderingDepthSamples": samples,
            }
        view["showSlices"] = False  # slice planes would hide the volume
    return view


def main(argv=None):
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    p.add_argument(
        "path",
        type=Path,
        help="an .ome.zarr, or a directory of *.ome.zarr or */*.ome.zarr",
    )
    p.add_argument(
        "--url",
        required=True,
        help="URL the directory (or the store's directory) is served at, "
        "e.g. its Globus HTTPS URL",
    )
    p.add_argument(
        "--viewer", default="https://neuroglancer-demo.appspot.com/"
    )
    p.add_argument(
        "--volume-rendering",
        nargs="?",
        const=0,
        type=float,
        metavar="GAIN",
        help="volume render the 3D view (experimental), at this gain "
        "(default %(const)s; lower is dimmer)",
    )
    a = p.parse_args(argv)
    path = a.path.resolve()
    if path.name.endswith(".zarr"):
        root, zarrs = path.parent, [path]
    else:
        root = path
        found = [*root.glob("*.ome.zarr"), *root.glob("*/*.ome.zarr")]
        zarrs = sorted(z for z in found if z.is_dir())
    if not zarrs:
        sys.exit(f"no .ome.zarr in {root} or its subdirectories")
    for z in zarrs:
        # the store's directory under root: () when it sits in root itself
        rel = z.parent.relative_to(root).parts
        url = "/".join([a.url.rstrip("/"), *rel])
        name = z.name.removesuffix(".ome.zarr") + ".ngl.json"
        st = state(z, url, a.volume_rendering)
        (z.parent / name).write_text(json.dumps(st, indent=1))
        print(f"{a.viewer}#!{url}/{name}")


if __name__ == "__main__":
    main()
