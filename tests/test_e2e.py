# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.
import asyncio
import contextlib
import hashlib
import logging
import os
import re
import shlex
import shutil
import sys
import threading
import traceback
import warnings
from collections import defaultdict
from collections.abc import Callable, Generator
from pathlib import Path
from typing import Any, cast, get_args

from curl_cffi import BrowserTypeLiteral
from hypothesis import HealthCheck, Phase, Verbosity, given, settings
from hypothesis import strategies as st
from pytest_httpserver import HTTPServer
from typer.testing import CliRunner
from viztracer import VizTracer
from werkzeug import Request, Response

import hydrastream.facade
import hydrastream.main
from hydrastream.domain.context import HydraContext
from hydrastream.main import app

warnings.filterwarnings("ignore", message=".*chunk_size is ignored.*")

DUMMY_DATA = b"0123456789" * 10000
DUMMY_MD5 = hashlib.md5(DUMMY_DATA).hexdigest()

hydrastream.main.ON_TEST_HOOK = True
hydrastream.facade.ON_TEST_HOOK = True

default = {
    "links": None,
    "input": None,
    "typehash": "md5",
    "checksum": None,
    "output": "download",
    "threads": None,
    "stream": False,
    "dry-run": False,
    "min-chunk-mb": 1,
    "stream-chunk-mb": 5,
    "buffer": None,
    "limit": None,
    "no-ui": False,
    "quiet": False,
    "json": False,
    "verify": True,
    "browser": "chrome120",
    "debug": False,
}
_current_tracer: VizTracer | None = None
_context_holder: HydraContext | None = None
_loop_holder: asyncio.AbstractEventLoop | None = None


# Оставляем функцию регистрации в файле тестов
def register_loop_in_monitor(ctx: HydraContext) -> None:
    global _loop_holder, _current_tracer, _context_holder  # noqa
    _context_holder = ctx  # type: ignore

    with contextlib.suppress(RuntimeError):
        # Перехватываем петлю из контекста главного потока,
        # когда запустился asyncio.run() движка
        _loop_holder = asyncio.get_running_loop()

    if _current_tracer is not None:
        _current_tracer.start()


# Подменяем пустышку в коде приложения на нашу функцию из теста
hydrastream.facade.ON_ENGINE_START_HOOK = register_loop_in_monitor


def collapse_worker_names(names: list[str]) -> str:  # noqa: PLR0912
    """
    Превращает ['worker_1', 'worker_2', 'worker_3', 'other_actor']
    в 'worker_1-3, other_actor'
    """
    if not names:
        return ""

    # Регулярка для поиска базового имени и номера на конце (например, worker_87)
    pattern = re.compile(r"^(.*?)(_|-)?(\d+)$")
    groups: defaultdict[tuple[str, str], list[int]] = defaultdict(list)
    standalone: list[str] = []

    for name in names:
        match = pattern.match(name)
        if match:
            base, sep, num = match.groups()
            sep = sep or ""
            groups.setdefault((base, sep), []).append(int(num))
        else:
            standalone.append(name)

    result_parts: list[str] = []

    # Сжимаем последовательности чисел для каждой группы
    for (base, sep), nums in groups.items():
        nums_ = sorted(list(set(nums)))
        ranges: list[str] = []
        if not nums_:
            continue

        start = nums_[0]
        prev = nums_[0]

        for n in nums_[1:]:
            if n == prev + 1:
                prev = n
            else:
                if start == prev:
                    ranges.append(f"{start}")
                else:
                    ranges.append(f"{start}-{prev}")
                start = n
                prev = n
        if start == prev:
            ranges.append(f"{start}")
        else:
            ranges.append(f"{start}-{prev}")

        # Собираем обратно: worker_1-100 или worker_1,3,5-10
        joined_ranges = ",".join(ranges)
        if "," in joined_ranges or "-" in joined_ranges:
            result_parts.append(f"{base}{sep}[{joined_ranges}]")
        else:
            result_parts.append(f"{base}{sep}{joined_ranges}")

    result_parts.extend(standalone)
    return ", ".join(result_parts)


@contextlib.contextmanager
def actor_system_timeout_monitor(  # noqa: PLR0915
    timeout: float = 3.0, tracer: VizTracer | None = None
) -> Generator[None, None, None]:
    stop_event = threading.Event()
    main_thread_id = threading.get_ident()

    # Сбрасываем старый контейнер перед началом итерации Hypothesis
    global _loop_holder, _context_holder  # noqa
    _loop_holder = None

    def watchdog() -> None:  # noqa
        if stop_event.wait(timeout=timeout):
            return  # Тест прошел успешно

        assert sys.__stderr__
        out = sys.__stderr__
        out.write("\n" + "!" * 60 + "\n")
        out.write("🚨 HYDRASTREAM MONITOR: DEADLOCK DETECTED! 🚨\n")
        out.write("!" * 60 + "\n")

        # Достаем цикл из глобального холдера (туда его запишет наш кастомный хук/таска)
        active_loop = _loop_holder

        # Если холдер пуст, отчаянно пытаемся найти loop в других потоках (на всякий случай)
        if not active_loop:
            with contextlib.suppress(RuntimeError):
                active_loop = asyncio.get_running_loop()

        # Проверяем наличие тасок напрямую через объект цикла, игнорируя проверку .is_running()
        # (в момент дедлока флаг запущенности в Си рантайме может вести себя непредсказуемо)
        if active_loop:  # noqa: PLR1702
            try:
                tasks = list(asyncio.all_tasks(active_loop))
                out.write(
                    f"Snapshotting loop memory. Active tasks found: {len(tasks)}\n"
                )

                # Собираем ВСЕ активные асинхронные генераторы в памяти процесса
                # Это позволит найти `file_streamer`, даже если asyncio спрятал его стек в Си

                aggregated_traces: defaultdict[str, list[str]] = defaultdict(list)

                for task in tasks:
                    if task.done():
                        continue

                    task_name = task.get_name()
                    frames = task.get_stack(limit=None)

                    trace_lines: list[str] = []
                    if frames:
                        # 1. Извлекаем стандартный срез, который видит asyncio
                        top_frame = frames[-1]
                        extracted = traceback.extract_stack(top_frame)
                        for ext_frame in extracted:
                            if (
                                "hydrastream" in ext_frame.filename
                                or "test_e2e" in ext_frame.filename
                            ):
                                trace_lines.append(
                                    f"  👉 FILE: {ext_frame.filename}:{ext_frame.lineno} in {ext_frame.name}\n"
                                )
                            else:
                                trace_lines.append(
                                    f"     File: {ext_frame.filename}:{ext_frame.lineno} in {ext_frame.name}\n"
                                )

                    else:
                        trace_lines.append("  (No async stack frames extracted)\n")

                    trace_string = "".join(trace_lines)
                    aggregated_traces.setdefault(trace_string, []).append(task_name)

                # Сортировка блоков по алфавиту имен групп задач
                sorted_blocks: list[tuple[str, int, str]] = []
                for trace_string, task_names in aggregated_traces.items():
                    collapsed_names = collapse_worker_names(task_names)
                    sorted_blocks.append((
                        collapsed_names,
                        len(task_names),
                        trace_string,
                    ))

                sorted_blocks.sort(key=lambda x: x[0].lower())

                out.write("\n--- TRUE DEEP AGGREGATED ASYNC TRACES (SORTED) ---\n")
                for collapsed_names, count, trace_string in sorted_blocks:
                    out.write(
                        f"\n[TASKS] Count: {count} | Names: '{collapsed_names}'\n"
                    )
                    out.write("Async Traceback (Full Call Hierarchy):\n")
                    out.write(trace_string)
                    out.write("-" * 60 + "\n")

            except Exception as e:
                out.write(f"Critical error while dumping tasks: {e}\n")

        else:
            out.write(
                "❌ FATAL: Watchdog thread could not reference the main asyncio loop.\n"
            )
            out.write(
                "Make sure 'register_loop_in_monitor()' is called inside your async startup!\n"
            )
            # ... (Ваш прошлый код вывода async-стека тасок) ...
        out.flush()

        # ДИНАМИЧЕСКИЙ ДАМП ВСЕХ ОЧЕРЕДЕЙ ИЗ КОНТЕКСТА
        if _context_holder:
            out.write("\n" + "=" * 60 + "\n")
            out.write("📊 ACTOR SYSTEM QUEUES SNAPSHOT (DYNAMIC DUMP)\n")
            out.write("=" * 60 + "\n")

            ctx = _context_holder

            # Получаем все атрибуты объекта, включая унаследованные и свойства
            # Если это dataclass, можно читать напрямую из ctx.__dict__.items()
            try:
                attrs = {
                    name: getattr(ctx, name)
                    for name in dir(ctx)
                    if not name.startswith("__")
                }
            except Exception as e:
                out.write(f"Failed to read context attributes: {e}\n")
                attrs = {}

            found_queues = 0
            for attr_name, q_obj in attrs.items():
                # Проверяем строгое условие: имя заканчивается на _q
                if attr_name.endswith("_q") and q_obj is not None:
                    found_queues += 1

                    # Пытаемся безопасно узнать текущий размер очереди.
                    # Работает для asyncio.Queue, PriorityQueue, ваших кастомных оберток
                    q_size = "unknown"
                    if hasattr(q_obj._raw_queue, "qsize"):
                        q_size = q_obj._raw_queue.qsize()

                    # Проверяем, пустая ли очередь, чтобы визуально подсветить забитые каналы
                    status_bracket = (
                        "🟢 EMPTY" if q_size == 0 else f"🔴 PENDING ITEMS: {q_size}"
                    )
                    if q_size == "unknown":
                        status_bracket = "❓ UNKNOWN SIZE"

                    out.write(
                        f"📍 Queue: '{attr_name:<18}' | Status: {status_bracket}\n"
                    )

                    # ИНЖЕНЕРНЫЙ ХАК: Если в очереди ЕСТЬ элементы, мы можем заглянуть внутрь
                    # и посмотреть типы застрявших сообщений БЕЗ вычитывания (non-destructive)
                    if isinstance(q_size, int) and q_size > 0:
                        internal_q = getattr(
                            q_obj._raw_queue, "_queue", q_obj._raw_queue
                        )
                        if hasattr(
                            internal_q, "queue"
                        ):  # для стандартных asyncio/PriorityQueue
                            # Показываем типы первых 3-х застрявших объектов для понимания контекста
                            queued_items = list(internal_q.queue)[:3]
                            item_types = [type(item).__name__ for item in queued_items]
                            out.write(f"   ↳ Inside types: {', '.join(item_types)}\n")

            if found_queues == 0:
                out.write(
                    "No attributes ending with '_q' were found in the provided context.\n"
                )
            out.write("=" * 60 + "\n")
        else:
            out.write(
                "\n❌ MONITOR: No global context registered. Queues cannot be dumped.\n"
            )

        # Дампим нативный стек главного потока, чтобы подстраховаться
        out.write("\n--- NATIVE MAIN THREAD STACK ---\n")
        frame = sys._current_frames().get(main_thread_id)  # type: ignore
        if frame:
            traceback.print_stack(frame, file=out)

        if tracer:
            out.write(
                "\n💾 Saving VizTracer async timeline to deadlock_report.json...\n"
            )
            out.flush()
            tracer.stop()
            # Сохраняем в формате JSON (он легче и быстрее пишется при os._exit)
            tracer.save(output_file="deadlock_report.json")

        out.write("\n💥 HARD EXIT VIA os._exit(1)\n")
        out.flush()
        os._exit(1)

    t = threading.Thread(target=watchdog, daemon=True)
    t.start()
    try:
        yield
    finally:
        stop_event.set()
        t.join(timeout=0.1)


def make_chaos_handler(
    seed: int, current_test_filenames: list[str]
) -> Callable[[Request], Response | None]:
    """
    Фабрика. Создает обработчик, поведение которого на 100% зависит от seed.
    Если Hypothesis перезапустит тест с этим же seed, сервер поведет себя ИДЕНТИЧНО.
    """
    request_counts: defaultdict[str, int] = defaultdict(int)
    lock = threading.Lock()

    def handler(request: Request) -> Response | None:  # noqa
        # 1. Уникальная подпись запроса (Учитываем Range, чтобы чанки отличались)
        sig = f"{request.path}|{request.method}|{request.headers.get('Range', '')}"

        # 2. Считаем, какая это попытка для данного запроса
        # (важно для выхода из ретраев!)
        with lock:
            request_counts[sig] += 1
            attempt = request_counts[sig]

        # 3. Генерируем ДЕТЕРМИНИРОВАННЫЕ "случайные" числа
        chaos_key = f"{seed}|{sig}|{attempt}"
        h = hashlib.md5(chaos_key.encode()).hexdigest()

        # Превращаем куски MD5 хэша в числа от 0.0 до 1.0 (заменяем random.random())
        # Берем разные куски хэша для разных проверок, чтобы они были независимы
        rand_md5 = int(h[0:8], 16) / 0xFFFFFFFF
        rand_waf = int(h[8:16], 16) / 0xFFFFFFFF
        rand_dumb = int(h[16:24], 16) / 0xFFFFFFFF
        rand_cloud = int(h[24:32], 16) / 0xFFFFFFFF

        path = request.path

        # --- 1. ОТДАЧА ФАЙЛА ХЕШЕЙ ---
        if "md5checksums.txt" in path:
            content = ""
            # 50% шанс, что файл хешей нормальный
            if rand_md5 > 0.5:
                content = "\n".join(
                    f"{DUMMY_MD5}  {name}" for name in current_test_filenames
                )
            return Response(content, status=200)

        # --- 2. ИМИТАЦИЯ WAF И СБОЕВ ---
        # 15% шанс отдать 429 Too Many Requests
        if rand_waf < 0.15:
            return Response("Slow down!", status=429, headers={"Retry-After": "1"})

        # 10% шанс отдать 503 Service Unavailable
        if 0.15 <= rand_waf < 0.25:
            return Response("Backend dead", status=503)

        # --- 3. ИМИТАЦИЯ ТУПЫХ СЕРВЕРОВ (Без Range) ---
        # 20% шанс, что сервер притворится тупым
        is_dumb_server = rand_dumb < 0.2

        if request.method == "HEAD":
            headers = {"Content-Length": str(len(DUMMY_DATA))}
            if not is_dumb_server:
                headers["Accept-Ranges"] = "bytes"

            # 70% шанс, что сервер отдаст облачный ETag (тестируем CloudProvider)
            if rand_cloud < 0.7:
                headers["ETag"] = f'"{DUMMY_MD5}"'

            return Response(status=200, headers=headers)

        # --- 4. ОБРАБОТКА GET ---

        range_header = request.headers.get("Range")

        if range_header and range_header.startswith("bytes=") and not is_dumb_server:
            byte_range = range_header.replace("bytes=", "")
            start_str, end_str = byte_range.split("-")
            start, end = int(start_str), int(end_str)

            chunk = DUMMY_DATA[start : end + 1]

            if rand_cloud < 0.1:
                half_chunk = chunk[: len(chunk) // 2]
                # Заметь: мы врем в заголовках! Говорим, что отдаем весь,
                # а отдаем половину.
                return Response(
                    half_chunk,
                    status=206,
                    headers={
                        "Content-Range": f"bytes {start}-{end}/{len(DUMMY_DATA)}",
                        "Content-Length": str(len(chunk)),  # Вранье!
                    },
                )

            return Response(
                chunk,
                status=206,
                headers={
                    "Content-Range": f"bytes {start}-{end}/{len(DUMMY_DATA)}",
                    "Content-Length": str(len(chunk)),
                },
            )

        # Фолбек: если сервер тупой ИЛИ запросили без Range - отдаем весь файл
        return Response(
            DUMMY_DATA, status=200, headers={"Content-Length": str(len(DUMMY_DATA))}
        )

    return handler


runner = CliRunner(catch_exceptions=False)


@st.composite
def filenames_strategy(draw: st.DrawFn) -> str:
    # 1. Генерируем основу имени (stem)
    # Используем алфавит с цифрами, тире и подчеркиванием
    stem_alphabet = "abcdefghijklmnopqrstuvwxyz0123456789-_"

    # 2. Генерируем случайное расширение (для разнообразия)
    ext = draw(st.sampled_from([".bin", ".txt", ".gz", ".zip", ""]))

    # 3. Генерируем само имя
    # min_size=1, max_size=30 (проверим длинные пути)
    name = draw(st.text(alphabet=stem_alphabet, min_size=1, max_size=30))

    return f"{name}{ext}"


@st.composite
def cli_fuzz_strategy(draw: st.DrawFn) -> dict[str, Any]:
    all_paths = draw(
        st.lists(filenames_strategy(), min_size=1, max_size=10, unique=True)
    )

    split_idx = draw(st.integers(min_value=0, max_value=len(all_paths)))

    cli_paths = all_paths[:split_idx]
    file_paths = all_paths[split_idx:]

    if not cli_paths and not file_paths:
        cli_paths = [draw(filenames_strategy())]

    params = {
        "threads": draw(st.one_of(st.none(), st.integers(1, 128))),
        "browser": draw(
            st.one_of(st.none(), st.sampled_from(list(get_args(BrowserTypeLiteral))))
        ),
        "buffer": draw(st.one_of(st.none(), st.integers(50, 100))),
        "limit": draw(st.one_of(st.none(), st.floats(0.1, 100.0))),
        "min-chunk-mb": draw(st.one_of(st.none(), st.integers(1, 20))),
        "stream-chunk-mb": draw(st.one_of(st.none(), st.integers(1, 20))),
        "flags": draw(
            st.fixed_dictionaries({
                "stream": st.booleans(),
                "dry-run": st.booleans(),
                "no-ui": st.booleans(),
                "quiet": st.booleans(),
                "json": st.booleans(),
                "no-verify": st.booleans(),
                "debug": st.booleans(),
            })
        ),
    }

    placeholder = "http://localhost:SERVER_PORT/"
    cli_urls = [
        f"{placeholder}{'ncbi.nlm.nih.gov/' if draw(st.booleans()) else ''}{p}"
        for p in cli_paths
    ]
    file_urls = [
        f"{placeholder}{'ncbi.nlm.nih.gov/' if draw(st.booleans()) else ''}{p}"
        for p in file_paths
    ]

    args = build_args_list(params, cli_urls)

    if len(all_paths) == 1 and draw(st.booleans()):
        args.extend(["--checksum", DUMMY_MD5, "--typehash", "md5"])

    # args.append("--debug")

    return {
        "args_template": args,
        "paths": all_paths,
        "existing_copies": draw(st.integers(0, 5)),
        "existing_files": draw(st.sampled_from(all_paths)),
        "file_urls_template": file_urls,
        "server_seed": draw(st.integers(0, 999999)),
    }


def build_args_list(
    params: dict[str, dict[str, bool] | Any], urls: list[str]
) -> list[str]:
    args = urls[:]

    # Флаги
    for name, value in params.items():
        if isinstance(value, dict):
            for flag, enabled in cast(dict[str, bool], value).items():
                if enabled:
                    args.append(f"--{flag}")
            continue

        if value is not None:
            args.extend([f"--{name}", str(value)])

    return args


@given(data=cli_fuzz_strategy())
@settings(
    max_examples=20,
    deadline=None,
    phases=[Phase.reuse, Phase.generate],  # Phase.explicit
    suppress_health_check=[HealthCheck.function_scoped_fixture],
    verbosity=Verbosity.verbose,
)
def test_hypothesis_nuclear_fuzzer(  # noqa
    data: dict[str, Any],
    httpserver: HTTPServer,
    tmp_path: Path,
) -> None:
    logging.getLogger("werkzeug").setLevel(logging.ERROR)
    filenames = [Path(p).name for p in data["paths"]]
    chaos_handler = make_chaos_handler(data["server_seed"], filenames)
    # 1. Заводим сервер
    httpserver.expect_request(re.compile(r"^/.*$")).respond_with_handler(chaos_handler)  # type: ignore
    base_url = httpserver.url_for("").rstrip("/")

    out_dir = tmp_path / "downloads"

    shutil.rmtree(out_dir, ignore_errors=True)
    out_dir.mkdir(parents=True, exist_ok=True)

    if data["existing_copies"] > 0:
        stem = Path(data["existing_files"]).stem
        suffix = Path(data["existing_files"]).suffix
        for i in range(data["existing_copies"]):
            name = data["existing_files"] if i == 0 else f"{stem} ({i}){suffix}"
            (out_dir / name).touch()
            # Добавляем .state.json для веса
            state_dir = out_dir / ".state"
            state_dir.mkdir(exist_ok=True)
            (state_dir / f"{name}.state.json").touch()

    final_args = [
        a.replace("http://localhost:SERVER_PORT/", f"{base_url}/")
        for a in data["args_template"]
    ]
    final_args.extend(["--output", str(out_dir)])

    if data["file_urls_template"]:
        urls_txt = tmp_path / "urls.txt"
        content = "\n".join(
            u.replace("http://localhost:SERVER_PORT/", f"{base_url}/")
            for u in data["file_urls_template"]
        )
        urls_txt.write_text(content)
        final_args.extend(["--input", str(urls_txt)])
    # 3. УДАР! (Запускаем CLI)
    prev_files = set(out_dir.glob("*"))
    print(
        f" prev files: {[x.name for x in out_dir.glob('*')]}",
        file=sys.__stderr__,
        flush=True,
    )
    debug = False
    if debug:
        global _current_tracer  # noqa: PLW0603
        _current_tracer = VizTracer(
            log_async=True,
            # Заставляем трекер игнорировать стандартный шум библиотек
            ignore_frozen=True,
            # Пишем только те функции, которые лежат в вашей папке проекта
            exclude_files=["site-packages", "_pytest", "hypothesis", "typer", "click"],
        )
    print(f"Running: {data}", file=sys.__stderr__, flush=True)
    print(
        f"Running: my-tool {' '.join(shlex.quote(a) for a in final_args)}",
        file=sys.__stderr__,
    )
    print("Your debug message here 1000", file=sys.__stderr__, flush=True)
    if debug:
        try:
            with actor_system_timeout_monitor(timeout=95, tracer=_current_tracer):
                result = runner.invoke(app, final_args, catch_exceptions=False)
        finally:
            if _current_tracer:
                _current_tracer.stop()
            _current_tracer = None
    else:
        result = runner.invoke(app, final_args)
    print("Your debug message here 2", file=sys.__stderr__, flush=True)

    # 4. ПРОВЕРКА ИНВАРИАНТОВ (ГЛАВНАЯ МАГИЯ PBT)

    # Инвариант 1: Программа НИКОГДА не должна падать с необработанным исключением
    # (Traceback)
    if result.exit_code == 0:
        # ... проверяем DUMMY_DATA ...

        # 1. Список исключений
        ignored = {"hydra.log", ".states"}

        # 2. Находим всё "запрещенное"

        leftovers = set([f for f in out_dir.glob("*") if f.name not in ignored]) - set(
            prev_files
        )
        print(
            f" post files: {[x.name for x in out_dir.glob('*')]}",
            file=sys.__stderr__,
            flush=True,
        )

        # Инвариант 2: Если это DRY-RUN, на диске НЕ ДОЛЖНО быть создано ни одного
        # файла генома

        if "--dry-run" in data["args_template"]:
            assert len(leftovers) == 0, (
                f"DRY-RUN нарушил обещание и скачал файлы на диск!"
                f"Было {prev_files}. Стало {len(leftovers)}"
            )

        # Инвариант 3: Если это STREAM, на диске тоже пусто
        elif "--stream" in data["args_template"]:
            print("11", file=sys.__stderr__, flush=True)
            assert len(leftovers) == 0, "STREAM записал бинарники на диск!"
            print("22", file=sys.__stderr__, flush=True)

            # 1. Проверяем чтение из буфера CliRunner
            actual_bytes = result.stdout_bytes
            print("22.1 - Прочитали stdout_bytes", file=sys.__stderr__, flush=True)

            # 2. Проверяем генерацию ожидаемых данных (память)
            expected_len = len(data["paths"])
            expected_bytes = DUMMY_DATA * expected_len
            print(
                "22.2 - Выделили память под ожидаемые байты",
                file=sys.__stderr__,
                flush=True,
            )

            # 3. Делаем простое сравнение длин, прежде чем сравнивать гигантские массивы байт
            assert len(actual_bytes) == len(expected_bytes), (
                f"Разная длина! {len(actual_bytes)} != {len(expected_bytes)}"
            )
            print("22.3 - Проверили длины", file=sys.__stderr__, flush=True)

            # 4. Финальное сравнение контента
            assert actual_bytes == expected_bytes
            print("33", file=sys.__stderr__, flush=True)

        # Инвариант 4: Если это обычная загрузка, файлы должны лежать на диске
        else:
            # Количество скачанных файлов должно совпадать с количеством уникальных ссылок
            assert len(leftovers) == len(data["paths"]), (
                f"Файлы не скачались! Лог терминала:\n{result.stdout}"
            )

            for f in leftovers:
                actual_bytes = f.read_bytes()
                assert actual_bytes == DUMMY_DATA, f"DATA CORRUPTION in {f.name}!"

    else:
        # Если программа упала, мы проверяем, что она упала ЛЕГАЛЬНО!
        # Например, из-за StreamError на тупом сервере.
        tb_string = ""
        if result.exc_info:
            tb_string = "\n" + "".join(traceback.format_exception(*result.exc_info))
        error = result.exception
        assert result.exit_code == 4, f"Exception: {error!r}{tb_string}"
        print(f"Done with network erroe {error}", file=sys.__stderr__, flush=True)
