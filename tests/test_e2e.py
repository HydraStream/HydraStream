# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.
import hashlib
import re
import shutil
import warnings
from pathlib import Path

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st
from typer.testing import CliRunner
from werkzeug import Request, Response

from hydrastream.main import app

warnings.filterwarnings("ignore", message=".*chunk_size is ignored.*")

DUMMY_DATA = b"0123456789" * 100  # 1000 байт
DUMMY_MD5 = hashlib.md5(DUMMY_DATA).hexdigest()


def range_request_handler(request: Request) -> Response:
    """Универсальный обработчик для любых URL на нашем фейковом сервере."""
    if request.method == "HEAD":
        return Response(
            status=200,
            headers={"Content-Length": str(len(DUMMY_DATA)), "Accept-Ranges": "bytes"},
        )

    range_header = request.headers.get("Range")
    if range_header and range_header.startswith("bytes="):
        byte_range = range_header.replace("bytes=", "")
        start_str, end_str = byte_range.split("-")
        start, end = int(start_str), int(end_str)

        chunk = DUMMY_DATA[start : end + 1]

        return Response(
            chunk,
            status=206,
            headers={
                "Content-Range": f"bytes {start}-{end}/{len(DUMMY_DATA)}",
                "Content-Length": str(len(chunk)),
            },
        )
    return Response(
        DUMMY_DATA, status=200, headers={"Content-Length": str(len(DUMMY_DATA))}
    )


runner = CliRunner()


# --- СТРАТЕГИЯ ГЕНЕРАЦИИ ДАННЫХ ---
@st.composite
def cli_fuzz_strategy(draw):
    """Генерирует случайные, но логически допустимые комбинации аргументов"""

    # Генерируем от 1 до 3 случайных путей файлов (только буквы и цифры)
    paths = draw(
        st.lists(
            st.text(
                alphabet="abcdefghijklmnopqrstuvwxyz0123456789", min_size=3, max_size=10
            ),
            min_size=1,
            max_size=3,
        )
    )

    # 0 - только CLI, 1 - только файл, 2 - и то, и другое
    input_mode = draw(st.integers(min_value=0, max_value=2))

    # Флаги
    stream = draw(st.booleans())
    dry_run = draw(st.booleans())
    no_ui = draw(st.booleans())
    quiet = draw(st.booleans())
    json_logs = draw(st.booleans())

    # Числа (берем адекватные диапазоны, чтобы не переполнить RAM в тестах)
    threads = draw(st.integers(min_value=1, max_value=10))
    min_chunk_mb = draw(st.integers(min_value=1, max_value=10))

    return {
        "paths": [f"{p}.bin" for p in paths],
        "input_mode": input_mode,
        "stream": stream,
        "dry_run": dry_run,
        "no_ui": no_ui,
        "quiet": quiet,
        "json_logs": json_logs,
        "threads": threads,
        "min_chunk_mb": min_chunk_mb,
    }


@given(data=cli_fuzz_strategy())
@settings(
    max_examples=200,  # Прогонит 30 уникальных случайных комбинаций
    deadline=None,  # Отключаем таймаут Hypothesis, т.к. асинхронщина бывает непредсказуемой
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
def test_hypothesis_nuclear_fuzzer(httpserver, tmp_path: Path, data: dict):
    # 1. Заводим сервер
    httpserver.expect_request(re.compile("^/.*$")).respond_with_handler(
        range_request_handler
    )
    base_url = httpserver.url_for("")
    out_dir = tmp_path / "downloads"

    if out_dir.exists():
        shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    # 2. Собираем аргументы как настоящий юзер в консоли
    args = []

    # Распределяем ссылки между CLI и файлом
    cli_urls = []
    file_urls = []
    for i, path in enumerate(data["paths"]):
        url = f"{base_url}{path}"
        if data["input_mode"] == 0 or (data["input_mode"] == 2 and i % 2 == 0):
            cli_urls.append(url)
        else:
            file_urls.append(url)

    args.extend(cli_urls)

    if file_urls:
        input_txt = tmp_path / "urls.txt"
        input_txt.write_text("\n".join(file_urls))
        args.extend(["--input", str(input_txt)])

    # Добавляем флаги
    if data["stream"]:
        args.append("--stream")
    if data["dry_run"]:
        args.append("--dry-run")
    if data["no_ui"]:
        args.append("--no-ui")
    if data["quiet"]:
        args.append("--quiet")
    if data["json_logs"]:
        args.append("--json")

    args.extend([
        "--threads",
        str(data["threads"]),
        "--min-chunk-mb",
        str(data["min_chunk_mb"]),
        "--output",
        str(out_dir),
        "--no-verify",  # Отключаем MD5, т.к. генерируем случайные файлы без хешей
    ])

    # 3. УДАР! (Запускаем CLI)

    result = runner.invoke(app, args)

    # 4. ПРОВЕРКА ИНВАРИАНТОВ (ГЛАВНАЯ МАГИЯ PBT)

    # Инвариант 1: Программа НИКОГДА не должна падать с необработанным исключением (Traceback)
    assert result.exception is None, (
        f"КРАШ ПРОГРАММЫ! Комбинация: {args}\nВывод: {result.stdout}"
    )

    # Инвариант 2: Если это DRY-RUN, на диске НЕ ДОЛЖНО быть создано ни одного файла генома
    if data["dry_run"]:
        downloaded_files = list(out_dir.glob("*.bin"))
        assert len(downloaded_files) == 0, (
            "DRY-RUN нарушил обещание и скачал файлы на диск!"
        )

    # Инвариант 3: Если это STREAM, на диске тоже пусто
    if data["stream"]:
        downloaded_files = list(out_dir.glob("*.bin"))
        assert len(downloaded_files) == 0, "STREAM записал бинарники на диск!"

    # Инвариант 4: Если это обычная загрузка, файлы должны лежать на диске
    if not data["stream"] and not data["dry_run"] and result.exit_code == 0:
        downloaded_files = list(out_dir.glob("*.bin"))
        # Количество скачанных файлов должно совпадать с количеством уникальных ссылок
        assert len(downloaded_files) == len(set(data["paths"])), (
            f"Файлы не скачались! Лог терминала:\n{result.stdout}"
        )
