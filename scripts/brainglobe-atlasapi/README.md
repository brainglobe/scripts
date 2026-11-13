# BrainGlobe Atlas API scripts
This directory contains useful scripts relating to the BrainGlobe Atlas API (`brainglobe-atlasapi`) and its usage. The directory is comprised of the following scripts: 

## `open_packaged_atlas_napari.py`
Allows the user to open an existing packaged BrainGlobe atlas in Napari. The NMT ARM symmetric macaque atlas is opened as an example. 

## `sync_logs.py`
Mirrors the raw S3 server access logs of the public atlas bucket (`s3://brainglobe-logs`) into local per-day Parquet files, fetching only logs newer than the last one already mirrored. Needs AWS credentials with read access to the log bucket.

```bash
$ uv run sync_logs.py brainglobe-logs/
```
