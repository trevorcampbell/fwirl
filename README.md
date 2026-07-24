# fwirl

fwirl is a lightweight Python library for building and maintaining collections of **assets** (files, records, objects, etc.) with dependency tracking, stale detection, scheduling, and optional remote control.

## Features

- Define assets and dependency graphs
- Refresh/build only what is stale
- Schedule refresh/build jobs with cron expressions
- Run as a local script or as a RabbitMQ-backed server
- Control running graphs through a CLI and Python API
- Optional notifier integrations for alerts

## Requirements

- Python 3.9+
- RabbitMQ (only required for server mode and remote CLI/API control)

## Installation

Install from PyPI:

```bash
pip install fwirl
```

Install from source (editable):

```bash
git clone https://github.com/trevorcampbell/fwirl.git
cd fwirl
pip install -e .
```

Install docs dependencies:

```bash
pip install -e ".[docs]"
```

## Basic usage

Define assets, create a graph, and build:

```python
import fwirl

class MyAsset(fwirl.Asset):
    async def timestamp(self):
        return fwirl.AssetStatus.Unavailable

    async def build(self):
        pass

g = fwirl.AssetGraph("my_graph")
root = MyAsset("root", [])
leaf = MyAsset("leaf", [root])
g.add_assets([leaf])
g.build()
```

For a fuller walkthrough, see:

- `docs/quickstart.rst`
- `example/`

## CLI usage

The package installs a `fwirl` command:

```bash
fwirl summarize my_graph
fwirl ls my_graph --assets
fwirl build my_graph
fwirl refresh my_graph
fwirl shutdown my_graph
```

For full command reference, see:

- `docs/cli.rst`

## Building documentation

From the repository root:

```bash
pip install -e ".[docs]"
cd docs
make html
```

Built docs will be in:

- `docs/_build/html`

## License

fwirl is distributed under the terms of the license in `LICENSE`.
