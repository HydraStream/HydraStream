# HydraStream

[![PyPI version](https://badge.fury.io/py/hydrastream.svg)](https://pypi.org/project/hydrastream/)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Tests](https://github.com/Zhukovetski/HydraStream/actions/workflows/tests.yml/badge.svg)](https://github.com/Zhukovetski/HydraStream/actions/workflows/tests.yml)

<p align="center">
  <img src="https://raw.githubusercontent.com/Zhukovetski/HydraStream/main/assets/HydraStream-Demo.gif" alt="HydraStream Demo" width="800">
</p>

A concurrent streaming downloader. Built with pure Python, `uvloop`, and `httpx`.

## Overview

Standard download tools (`wget`, `curl`) are limited to a single connection, which can be a bottleneck on high-latency networks. Multi-connection tools (`aria2`) typically require writing intermediate chunks to disk, causing I/O overhead when downloading and processing large datasets (e.g., ML weights, DB dumps).

HydraStream implements concurrent chunk downloading to improve network throughput. It uses a Sequential Reordering Buffer to convert asynchronous chunk downloads into a sequential byte stream. This allows piping data directly to other processes without intermediate disk I/O.

---

## Features

* **Concurrent downloading:** Utilizes `uvloop` for asynchronous HTTP requests.
* **In-Memory Streaming:** Assembles asynchronous chunks into a sequential stream in memory, enabling direct piping to standard input of other tools.
* **Network resilience:**
  * AIMD rate limiting and circuit breaker pattern to manage request rates.
  * Exponential backoff and full jitter for network drops.
  * Tracks exact byte offsets to resume interrupted downloads.
* **Interruption recovery:** Handles network disconnects, system sleep, or process suspension without data corruption, resuming from the last downloaded byte.
* **Integrity validation:** Extracts and verifies MD5 checksums from AWS S3 (`ETag`), Google Cloud (`x-goog-hash`), and standard HTTP headers.
* **Lock-free I/O:** Uses `os.pwrite` to minimize Global Interpreter Lock (GIL) contention during disk writes.
* **Adaptive UI:** Terminal progress visualization via `Rich`, with `--no-ui` and `--quiet` modes for strict POSIX compliance in CI/CD pipelines.

---

## Benchmarks

* **High-bandwidth environments:** On gigabit networks (e.g., AWS, GitHub Actions), HydraStream demonstrates comparable throughput to C++ tools like `aria2`, saturating available server-side bandwidth.
* **High-latency networks:** By utilizing multiple concurrent TCP connections, it mitigates the impact of TCP window size limits on high-latency links.
* **Streaming mode:** Reorders chunks in memory using a `heapq`, allowing multi-connection download speeds while streaming directly to `stdout`.

---

## Installation

Requires Python 3.11+. Install globally via `uv` (recommended) or `pipx`:

```bash
uv tool install hydrastream
```


To use it as a Python library:

```bash
uv add hydrastream
```


---

## Usage

### 1. Basic Download (Disk Mode)
Download a file using 20 concurrent connections:
```bash
hs "https://ftp.ncbi.nlm.nih.gov/.../genome.fna.gz" -t 20 --output ./data
```
<p align="center">
  <img src="https://raw.githubusercontent.com/Zhukovetski/HydraStream/main/assets/HydraStream-Demo.gif" alt="Disk Download Demo" width="800">
</p>

*(If interrupted, rerun the exact command to resume from the last saved byte).*

### 2. Unix Pipeline Streaming
Download a file, decompress it in memory, and process it sequentially without saving to disk:
```bash
hs "https://ftp.ncbi.nlm.nih.gov/.../genome.fna.gz" -t 20 --stream --quiet | zcat | grep -c "^>"
```
<p align="center">
  <img src="https://raw.githubusercontent.com/Zhukovetski/HydraStream/main/assets/Pipeline-Streaming-Demo.gif" alt="Pipeline Streaming Demo" width="800">
</p>

### 3. Python API (For Data Science / MLOps)
Embed the streaming engine directly into asynchronous Python applications:
```python
import asyncio
from hydrastream import HydraStream

async def main():
    urls =["https://url1.gz", "https://url2.gz"]
    async with HydraStream(threads=10, quiet=True) as loader:
        async for filename, stream in loader.stream_all(urls):
            print(f"Processing {filename}...")
            async for chunk_bytes in stream:
                process_data(chunk_bytes)

asyncio.run(main())
```

---

## CLI Options

| Option | Shortcut | Default | Description |
| :--- | :---: | :---: | :--- |
| `URLS` | - | Required | One or multiple URLs to download. |
| `--threads` | `-t` | `1` | Number of concurrent connections. |
| `--output` | `-o` | `download/` | Directory to save files and state trackers. |
| `--stream` | `-s` | `False` | Enable streaming mode (redirects data to `stdout`). |
| `--no-ui` | `-nu` | `False` | Disables progress bars, leaves plain text logs. |
| `--quiet` | `-q` | `False` | Suppresses all console output (stderr/stdout routing). |
| `--md5` | | `None` | Expected MD5 hash (single URL only). |
| `--buffer` | `-b` | `threads * 5MB` | Maximum stream buffer size in bytes. |

---

## Architecture / Implementation Details

* **Congestion Control:** Implements an Additive Increase / Multiplicative Decrease (AIMD) algorithm and a Circuit Breaker pattern to dynamically scale requests.
* **Sequential Reordering:** Uses `asyncio.PriorityQueue` for LIFO retry handling and `heapq` as a sliding reordering buffer to convert concurrent HTTP ranges into a strict sequential byte stream.
* **UI Debouncing:** Batches terminal rendering operations in a detached asynchronous loop to reduce CPU utilization.
* **State Persistence:** Uses atomic writes (`NamedTemporaryFile` and POSIX directory `fsync`) for state files to prevent corruption on power loss.
* **Process Lifecycle:** Handles `SIGINT`/`SIGTERM` for graceful shutdown and instant cancellation of worker tasks on fatal HTTP errors.

---

## Roadmap

* **v1.1: Autonomous Worker Scaling:** Transition from a static thread pool to adaptive concurrency based on network conditions and downstream backpressure.
* **v2.0: Rust Core:** Port the core engine to Rust (`tokio`/`reqwest`) with a `PyO3` wrapper to bypass the Python GIL and improve multi-core execution.

---

## License
MIT License.
