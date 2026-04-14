#!/usr/bin/env python3
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from statistics import mean, stdev

# Настройки бенчмарка
ITERATIONS = 5
URL = "https://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/000/001/405/GCF_000001405.40_GRCh38.p14/GCF_000001405.40_GRCh38.p14_genomic.fna.gz"

# Конфигурация команд
COMMANDS = {
    "wget (1 thread)": {
        "cmd": ["wget", "-q", "-O", "bench_test.bin", URL],
        "cleanup": lambda: Path("bench_test.bin").unlink(missing_ok=True),
    },
    "aria2c (16 threads)": {
        "cmd": ["aria2c", "-x", "16", "-s", "16", "-q", "-o", "bench_test.bin", URL],
        "cleanup": lambda: Path("bench_test.bin").unlink(missing_ok=True),
    },
    "HydraStream (Auto)": {
        "cmd": ["hs", URL, "-q", "--no-verify", "-o", "./bench_out"],
        "cleanup": lambda: shutil.rmtree("./bench_out", ignore_errors=True),
    },
}


def run_benchmark() -> None:
    print(f"Starting benchmark: {ITERATIONS} iterations per tool.\nTarget: {URL}\n")

    results = {}

    for name, config in COMMANDS.items():
        print(f"Testing {name}...")
        wall_times = []
        user_times = []
        sys_times = []

        for i in range(1, ITERATIONS + 1):
            # Убеждаемся, что перед тестом чисто
            config["cleanup"]()

            start_time = time.perf_counter()

            # Запускаем процесс
            proc = subprocess.Popen(config["cmd"])

            # Ждем завершения и собираем точную статистику процесса (только для Unix)
            _, exit_status, rusage = os.wait4(proc.pid, 0)

            wall_time = time.perf_counter() - start_time
            user_time = rusage.ru_utime
            sys_time = rusage.ru_stime

            if exit_status != 0:
                print(f"  [!] Run {i} failed! Exit status: {exit_status}")
                continue

            wall_times.append(wall_time)
            user_times.append(user_time)
            sys_times.append(sys_time)

            print(
                f"  Run {i}: Wall: {wall_time:.2f}s | "
                f"CPU: {user_time:.2f}s | Sys: {sys_time:.2f}s"
            )

            # Убираем за собой сразу после прогона
            config["cleanup"]()
            time.sleep(2)  # Небольшая пауза, чтобы сервер сбросил rate limits

        if wall_times:
            results[name] = {
                "wall": (
                    mean(wall_times),
                    stdev(wall_times) if len(wall_times) > 1 else 0,
                ),
                "user": (
                    mean(user_times),
                    stdev(user_times) if len(user_times) > 1 else 0,
                ),
                "sys": (mean(sys_times), stdev(sys_times) if len(sys_times) > 1 else 0),
            }
        print("-" * 40)

    # Печатаем Markdown таблицу
    print("\n### Benchmark Results (Markdown Table)\n")
    print("| Tool | Real Time (Wall-clock) | User Time (CPU) | Sys Time (Kernel) |")
    print("| :--- | :---: | :---: | :---: |")

    for name, metrics in results.items():
        wall_m, wall_s = metrics["wall"]
        user_m, user_s = metrics["user"]
        sys_m, sys_s = metrics["sys"]

        # Если это HydraStream, выделяем жирным
        wall_str = f"{wall_m:.3f}s ±{wall_s:.2f}s"
        if "HydraStream" in name:
            wall_str = f"**{wall_str}**"

        print(
            f"| **{name}** | {wall_str} | {user_m:.3f}s ±"
            f"{user_s:.2f}s | {sys_m:.3f}s ±{sys_s:.2f}s |"
        )


if __name__ == "__main__":
    # Проверка, что мы на Linux/Mac (os.wait4 не работает на Windows)
    if os.name != "posix":
        print(
            "This benchmark script requires a POSIX OS (Linux/macOS) for "
            "accurate CPU timing."
        )
        sys.exit(1)

    run_benchmark()
