import mimetypes
import re
from email.utils import unquote
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

from curl_cffi import Headers


def format_size(size_bytes: float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(size_bytes) < 1024:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024
    return f"{size_bytes:.2f} PB"


def redact_url(url: str) -> str:
    """Return the URL with any embedded userinfo (user:pass) masked.

    Prevents basic-auth credentials from leaking into logs, error messages,
    or structured JSON output when the user supplies URLs of the form
    ``https://user:pass@host/path``.
    """
    try:
        parts = urlsplit(url)
    except ValueError:
        return url

    if "@" not in (parts.netloc or ""):
        return url

    host = parts.hostname or ""
    if parts.port is not None:
        host = f"{host}:{parts.port}"

    netloc = f"***:***@{host}" if host else "***:***@"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def extract_filename(url: str, headers: Headers) -> str:
    filename = None
    cd = headers.get("Content-Disposition", "")

    match_utf8 = re.search(r"filename\*=\s*([^']+)''([^;]+)", cd)
    if match_utf8:
        filename = unquote(match_utf8.group(2))

    if not filename:
        match_std = re.search(r'filename="?([^";]+)"?', cd)
        if match_std:
            filename = unquote(match_std.group(1))

    if not filename:
        clean_url = url.rstrip("/")
        clean_url = clean_url.split("?")[0].split("#")[0]
        clean_url, name = clean_url.rsplit("/", 1)
        if "/" in clean_url and not clean_url.endswith(":/"):
            filename = unquote(name)

    if not filename or filename in [".", ""]:
        filename = "downloaded_file"

    filename = re.sub(r'[\\/*?:"<>|]', "_", filename)
    filename = Path(filename).name

    if "." not in filename:
        content_type = headers.get("Content-Type", "").split(";")[0]
        ext = mimetypes.guess_extension(content_type)
        if ext:
            filename += ext
        elif not filename.endswith(".bin"):
            filename += ".bin"

    return filename


def debug_allocated_file(
    original_path: Path, local_path: Path, chunk_size: int = 64 * 1024
) -> bool:
    """Сравнивает аллоцированный файл с эталоном и ищет паттерны повреждений."""

    with original_path.open("rb") as f_orig, local_path.open("rb") as f_local:
        offset = 0
        corrupted_blocks = 0
        first_error_pos = None
        is_all_zeros = True

        print(f"[*] Начинаю анализ файлов. Размер блока проверки: {chunk_size} байт...")

        while True:
            b_orig = f_orig.read(chunk_size)
            b_local = f_local.read(chunk_size)

            if not b_orig and not b_local:
                break

            if b_orig != b_local:
                corrupted_blocks += 1
                if first_error_pos is None:
                    first_error_pos = offset
                    # Проверяем, что именно записалось вместо оригинала в первой ошибке
                    for i in range(max(len(b_orig), len(b_local))):
                        byte_orig = b_orig[i : i + 1] if i < len(b_orig) else b""
                        byte_local = b_local[i : i + 1] if i < len(b_local) else b""
                        if byte_orig != byte_local:
                            print(
                                f"\n[!] Первое расхождение на позиции: "
                                f"{offset + i} (0x{offset + i:X})"
                            )
                            print(f"    Ожидалось (оригинал): {byte_orig}")
                            print(f"    Записано на диск:    {byte_local}")

                            # Проверяем на затирание нулями (аллокация без записи)
                            if byte_local == b"\x00" and byte_orig != b"\x00":
                                print(
                                    "    👉 Анализ: На диске остался пустой "
                                    "аллоцированный блок (нули). "
                                    "Сюда запись вообще НЕ ДОШЛА."
                                )
                            else:
                                is_all_zeros = False
                                print(
                                    "    👉 Анализ: Сюда записались ДРУГИЕ данные. "
                                    "Возможно, смещение съехало или чанки перепутались."
                                )
                            break

            offset += len(b_orig) if b_orig else len(b_local)

        if corrupted_blocks == 0:
            print("\n🎉 Успех! Файлы побайтово идентичны.")
            return True
        print(f"\n[X] Итог проверки: Найдено поврежденных блоков: {corrupted_blocks}.")
        if is_all_zeros and first_error_pos is not None:
            print(
                "🚨 Заключение: Все повреждения — это нетронутые нули аллокации. "
                "Запись промахнулась мимо этих позиций."
            )
        return False


def verify_memory_chunk(
    data_bytes: list[bytes],
    offset: int,
    original_path: Path | None = None,
) -> bool:
    """
    Сравнивает кусок данных из памяти (список байт) с эталонным файлом по смещению.
    Выводит точное место расхождения, если оно есть.
    """
    if original_path is None:
        original_path = Path().expanduser().resolve()
    # 1. Склеиваем текущие байты из списка, чтобы получить один монолитный кусок
    current_data = b"".join(data_bytes)
    chunk_len = len(current_data)

    if chunk_len == 0:
        print(
            f"[VERIFY] ⚠ Предупреждение: Передан пустой список байт на offset {offset}"
        )
        return True

    # 2. Читаем этот же участок из оригинального файла
    if not original_path.exists():
        print(f"[VERIFY] ❌ Ошибка: Оригинальный файл {original_path} не найден")
        return False

    with original_path.open("rb") as f:
        f.seek(offset)
        original_data = f.read(chunk_len)

    # 3. Сравниваем длины (на случай, если вышли за пределы файла)
    if len(original_data) != chunk_len:
        print(
            f"[VERIFY] ❌ Расхождение длин на offset {offset}! "
            f"В файле: {len(original_data)} байт, в памяти: {chunk_len} байт"
        )
        return False

    # 4. Побайтовое сравнение
    if original_data == current_data:
        # Все отлично, данные чистые
        return True

    # 5. Если данные не совпали, находим точную локальную позицию ошибки
    for i in range(chunk_len):
        if original_data[i] != current_data[i]:
            global_pos = offset + i
            print("\n[VERIFY_FAIL] 🚨 ДАННЫЕ ПОВРЕЖДЕНЫ!")
            print(f"  Стартовый offset чанка: {offset}")
            print(
                f"  Абсолютная позиция ошибки в файле: {global_pos} (0x{global_pos:X})"
            )
            print(f"  Ожидалось (файл):  {original_data[i : i + 1]}")
            print(f"  В памяти сейчас:   {current_data[i : i + 1]}")

            # Дополнительный контекст: выведем по 10 байт вокруг ошибки для наглядности
            start_ctx = max(0, i - 10)
            end_ctx = min(chunk_len, i + 10)
            print(f"  Контекст файла:   {original_data[start_ctx:end_ctx]}")
            print(f"  Контекst памяти:  {current_data[start_ctx:end_ctx]}")
            return False

    return False
