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
