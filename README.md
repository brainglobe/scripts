# Scripts Repository
A collection of useful BrainGlobe related scripts. This repo consists of the following directories: 

## Scripts
Scripts organised by package to aid in the use of BrainGlobe tools. 

## Skills
Various skills which may be useful for matters relating to BrainGlobe without necessarily being Python scripts. 

# Usage

## Python scripts
### Instructions for macOS / Linux / Windows users
Most scripts list their dependencies in an inline metadata block at the top of the file, so they are best run with [uv](https://docs.astral.sh/uv/), which installs them for you.

In case it is not installed, on macOS and Linux install uv with the following command:
```bash
$ curl -LsSf https://astral.sh/uv/install.sh | sh
```
and on Windows:
```powershell
> powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```
On an HPC cluster it may be available as a module instead:

```bash
$ module load uv
```
Then, you can run a script by running the following command in your terminal:

```bash
$ uv run script.py
```

uv creates an isolated environment with the script's dependencies the first time, and reuses it afterwards.
Scripts that only use the standard library can also be run with plain Python:

```bash
$ python script.py
```

Scripts that take arguments print their options with:
```bash
$ uv run script.py --help
```
