# HydraStream

[![PyPI version](https://badge.fury.io/py/hydrastream.svg)](https://pypi.org/project/hydrastream/)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Coverage: 78%](https://img.shields.io/badge/coverage-78%25-brightgreen.svg)](https://github.com/Zhukovetski/HydraStream)
[![Tests](https://github.com/Zhukovetski/HydraStream/actions/workflows/tests.yml/badge.svg)](https://github.com/Zhukovetski/HydraStream/actions/workflows/tests.yml)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/HydraStream/HydraStream)

<p align="center">
  <img src="https://raw.githubusercontent.com/Zhukovetski/HydraStream/main/assets/Demo.gif" alt="HydraStream Demo" width="800">
</p>

HydraStream downloads a file over many parallel HTTP connections and reassembles the chunks in memory, in order — so you can pipe huge files straight into another program, at full parallel speed, without touching the disk.

```bash
hs "https://ftp.ncbi.nlm.nih.gov/.../genome.fna.gz" --stream -q | zcat | wc -l
```

Classic downloaders (`aria2c`, `axel`, `wget2`) parallelize downloads by writing to a file, and `curl` can pipe but only over a single connection. HydraStream does both at once: chunks arrive out of order over parallel connections, an internal min-heap restores their order in memory, and `stdout` receives a clean sequential byte stream. This matters when the files are huge (genomics datasets, ML model weights, database dumps), the source is slow, and you don't want — or don't have room for — an intermediate copy on disk.

It also works as a regular downloader: fast, resumable, and verified.

## Features

* **Stream to stdout**: chunks are downloaded in parallel and re-ordered in memory, so `stdout` receives clean sequential data. No temp files, bounded memory (`--buffer`).
* **Pipe-friendly by design**: all progress bars, logs, and warnings go to `stderr`; `stdout` carries nothing but your data. Structured JSON Lines logging (`--json`) for CI/CD.
* **Survives bad networks**: dropped connections, total outages, even OS suspend (close your laptop lid, open it later) — downloads resume from the exact byte via HTTP Range requests. Hardened with continuous chaos testing in CI (`tc qdisc`, `iptables` fault injection) and property-based testing (Hypothesis).
* **On-the-fly hashing**: MD5 / SHA-256 / BLAKE2 checksums are computed while the data streams through — the file is never buffered in full. Size and checksum mismatches fail loudly, never silently.
* **Adaptive concurrency**: worker count scales up and down automatically (AIMD, the same idea as TCP congestion control) based on server responses like `429`/`503`. Optional hard bandwidth cap (`--limit`).
* **Fast disk writes**: scattered chunks are written with native positional I/O (`pwrite` on Linux/macOS, Win32 API on Windows) from a dedicated thread pool, outside the GIL and without file locks.
* **Browser TLS fingerprint**: uses `curl_cffi` to present a real browser TLS signature (e.g., Chrome 120) for servers that reject generic HTTP clients.
* **Dry-run mode**: `--dry-run` fetches remote metadata, checks available disk space, and resolves target hashes without downloading anything.
* **Embeddable**: a daemon-style async Python API (`HydraDaemon`) for use inside data pipelines, schedulers, and services — no external RPC process required.

Under the hood, the pipeline is built as isolated asynchronous actors (resolvers, dispatchers, workers, writers) communicating through prioritized message queues — no shared-memory locks. See the [DeepWiki](https://deepwiki.com/HydraStream/HydraStream) for architecture details.

## Installation

Requires Python 3.12+.

```bash
uv tool install hydrastream
```
or
```bash
pipx install hydrastream
```

## Usage

### 1. Download to Disk
Downloads the specified file to the output directory using dynamically scaled connections:
```bash
hs "https://ftp.ncbi.nlm.nih.gov/.../genome.fna.gz" -t 20 --output ./data
```
<p align="center">
  <img src="https://raw.githubusercontent.com/Zhukovetski/HydraStream/main/assets/HydraStream-Demo.gif" alt="HydraStream Demo" width="800">
</p>

### 2. Stream to stdout (Pipe)
Downloads the file in memory and streams binary data to `stdout`. The `--quiet` (`-q`) flag suppresses logging output to `stderr`:
```bash
hs "https://ftp.ncbi.nlm.nih.gov/.../genome.fna.gz" -t 20 --stream -q | zcat | wc -l
```
<p align="center">
  <img src="https://raw.githubusercontent.com/Zhukovetski/HydraStream/main/assets/Pipeline-Streaming-Demo.gif" alt="Pipeline Streaming Demo" width="800">
</p>

### 3. Batch Processing
Reads target URLs from a local file.

```bash
hs --input urls.txt --threads 20 --output ./datasets
```

## Configuration

HydraStream supports layered configuration. Default parameters can be defined in a TOML file located at `~/.config/hydrastream/config.toml`. CLI arguments override these defaults.

```toml
# ~/.config/hydrastream/config.toml
threads = 128
output_dir = "~/downloads"
verify = true
speed_limit = 50.0
min-chunk-mb = 5
```

### 4. Python API

```python
import asyncio
import sys

from hydrastream import HydraDaemon, HydraConfig, UIConfig


async def main():
    config = HydraConfig(threads=20)
    ui_config = UIConfig(quiet=True)
    url = "https://example.com/file1.gz"

    async with HydraDaemon(config=config, ui_config=ui_config) as daemon:
        task_id = await daemon.add_download(url)

        if task_id is None:
            return

        file_stream = await daemon.get_stream(task_id)

        if file_stream is not None:
            # An async generator yielding ordered chunks of bytes
            async for chunk in file_stream:
                sys.stdout.buffer.write(chunk)


if __name__ == "__main__":
    asyncio.run(main())
```

## CLI Options

HydraStream supports layered configuration. Options can be passed as CLI arguments or defined in `~/.config/hydrastream/config.toml`. CLI flags take precedence.

| Option | Shortcut | Default | Description |
| :--- | :---: | :---: | :--- |
| `LINKS` | - | `None` | One or multiple target URLs to download (positional argument). |
| `--input` | `-i` | `None` | Read URLs from a text file or `-` for stdin. |
| `--typehash` | `-th` | `md5` | Hash algorithm type (e.g., `md5`, `sha256`). |
| `--checksum` | `-c` | `None` | Expected hash checksum (applicable only for a single URL). |
| `--output` | `-o` | `downloads/` | Destination directory for downloaded files. |
| `--threads` | `-t` | `Auto` | Number of concurrent download connections (scales up to 128). |
| `--stream` | `-s` | `False` | Enable streaming mode (redirects binary data to `stdout`). |
| `--dry-run` | `-dr` | `False` | Simulate the process (fetch metadata, check disk space) without downloading. |
| `--min-chunk-mb` | `-mcm` | `1` | Minimum chunk size in Megabytes for standard disk downloads. |
| `--stream-chunk-mb` | `-scm` | `5` | Target chunk size in Megabytes for streaming mode. |
| `--buffer` | `-b` | `None` | Maximum stream buffer size in Megabytes to prevent OOM. |
| `--limit` | `-l` | `None` | Global download bandwidth throttle limit in MB/s. |
| `--no-ui` | `-nu` | `False` | Disable GUI (progress bars). Leaves plain text logs. |
| `--quiet` | `-q` | `False` | Dead silence. No console output at all. Logs are still written to file. |
| `--json` | `-j` | `False` | Output logs in structured JSON Lines format. |
| `--verify` / `--no-verify` | `-V` / `-N` | `True` | Verify the downloaded file hash. Use `--no-verify` to skip. |
| `--browser` | `-B` | `chrome120` | Browser TLS fingerprint to impersonate (e.g., `chrome120`, `safari153`). |
| `--debug` | `-d` | `False` | Enable debug mode (propagates full exception tracebacks). |
| `--version` | `-v` | - | Show application version and exit. |

## Roadmap

### **v2.0: Rust Core:**

Port the core engine to Rust (`tokio`/`reqwest`) with a `PyO3` wrapper to bypass the Python GIL and improve multi-core execution.

## License

MIT License. See the [LICENSE](LICENSE) file for details.
