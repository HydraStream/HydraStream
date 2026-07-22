# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import sys
import tomllib
from collections.abc import Generator
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from typing import Annotated, Any, TextIO
from urllib.parse import urlparse

import typer
from curl_cffi import BrowserTypeLiteral

from hydrastream.__init__ import __version__
from hydrastream.domain.config import HydraConfig, UIConfig
from hydrastream.domain.context import create_monitor
from hydrastream.domain.entities import Checksum, TypeHash
from hydrastream.exceptions import (
    ExitCode,
    FileReadError,
    HydraError,
    InvalidParameterError,
    LogStatus,
    StreamError,
    ValidationError,
)
from hydrastream.facade import HydraDaemon
from hydrastream.interfaces import MonitorBackend

ON_TEST_HOOK: bool = False

if sys.platform != "win32":
    try:
        import uvloop

        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        pass


def load_user_config() -> dict[str, Any]:
    config_path = Path.home() / ".config" / "hydrastream" / "config.toml"

    if not config_path.is_file():
        return {}

    try:
        with config_path.open("rb") as f:
            return tomllib.load(f)
    except Exception:
        return {}


USER_CONFIG: dict[str, Any] = load_user_config()

app = typer.Typer(
    no_args_is_help=True,
    rich_markup_mode="rich",
    epilog="[bold]Examples:[/]\n  hs https://example.com/file.gz\n  hs -i urls.txt "
    "-t 10 -o ./data\n  hs --stream https://example.com/file.gz | zcat | grep pattern",
)


def version_callback(value: bool) -> None:
    if value:
        typer.echo(f"HydraStream v{__version__}")
        raise typer.Exit()


def validate(
    ui: MonitorBackend,
    links: list[str] | str | None,
    input_file: str | None,
) -> list[str]:
    if not links and not input_file:
        raise ValidationError(
            param="links",
            reason="You must provide either[LINKS] or an --input file.",
        )
    validate_input_file(input_file)
    if links is not None:
        links = [links] if isinstance(links, str) else list(links)
    valid_links = parse_urls(ui, links, input_file)
    if not valid_links:
        raise ValidationError(param="links", reason="No valid URLs found to process!")

    return valid_links


def validate_input_file(value: str | None) -> None:
    if value is None or value == "-":
        return

    path = Path(value)
    if not path.exists():
        raise ValidationError(param="input", value=value, reason="File does not exist")
    if not path.is_file():
        raise ValidationError(param="input", value=value, reason="Target is not a file")


def is_valid_url(url: str) -> bool:
    """Checks if a given string is a structurally valid HTTP/HTTPS URL."""
    try:
        result = urlparse(url)
        return result.scheme in {"http", "https"} and bool(result.netloc)
    except ValueError:
        return False


@contextmanager
def get_input_stream(filepath: str) -> Generator[TextIO, None, None]:
    if filepath == "-":
        yield sys.stdin
        return

    path = Path(filepath).expanduser().resolve()

    if not path.exists():
        raise FileReadError(path=str(path), reason="Path does not exist")
    if not path.is_file():
        raise FileReadError(path=str(path), reason="Target is a directory, not a file")

    try:
        with path.open(encoding="utf-8") as f:
            yield f
    except PermissionError as e:
        raise FileReadError(path=str(path), reason="Permission denied") from e
    except OSError as e:
        raise FileReadError(path=str(path), reason=str(e)) from e


def parse_urls(
    ui: MonitorBackend, links_from_args: list[str] | None, filepath: str | None
) -> list[str]:
    all_links: list[str] = []

    # Обработка аргументов
    if links_from_args:
        for url in links_from_args:
            if is_valid_url(url):
                all_links.append(url)
            else:
                ui.report(
                    InvalidParameterError(
                        param="url", value=url, reason="Invalid HTTP/HTTPS format"
                    ),
                )

    if filepath:
        with get_input_stream(filepath) as stream:
            for line in stream:
                clean_line = line.strip()
                if not clean_line or clean_line.startswith("#"):
                    continue

                url = clean_line.split()[0]
                if is_valid_url(url):
                    all_links.append(url)
                else:
                    ui.report(
                        InvalidParameterError(
                            param="file_link",
                            value=url,
                            reason="Invalid URL in input file",
                        ),
                    )

    return list(dict.fromkeys(all_links))


def _prepare_checksums(
    links: list[str], checksum: str | None, typehash: TypeHash
) -> dict[str, Checksum]:
    if not checksum or not links:
        return {}

    if len(links) == 1:
        return {links[0]: Checksum(algorithm=typehash, value=checksum)}

    raise ValidationError(
        param="checksum",
        reason=(
            "Warning: The --checksum flag is ignored when multiple URLs are provided."
        ),
    )


async def _stream_download_tasks(
    daemon: HydraDaemon,
    tasks: list[int],
    ui: MonitorBackend,
) -> None:
    loop = asyncio.get_running_loop()

    def _blocking_write(data: bytes) -> None:
        sys.stdout.buffer.write(data)
        sys.stdout.buffer.flush()

    for i in tasks:
        if file_stream := await daemon.get_stream(i):
            try:
                async for chunk in file_stream:
                    await loop.run_in_executor(None, _blocking_write, chunk)

            except StreamError as e:
                ui.report(e)
                sys.exit(e.exit_code)


async def async_main(  # noqa
    links: list[str] | None,
    input_file: str | None,
    stream: bool,
    typehash: TypeHash,
    checksum: str | None,
    output_dir: str,
    dry_run: bool,
    no_ui: bool,
    quiet: bool,
    json_logs: bool,
    verify: bool,
    threads: int,
    min_chunk_size_mb: int,
    max_stream_chunk_size_mb: int,
    buffer_size_mb: int | None,
    speed_limit: float | None,
    impersonate: BrowserTypeLiteral,
    debug: bool,
) -> None:
    """
    Core asynchronous orchestrator for downloading or streaming files.

    Args:
        links: List of target URLs provided via positional arguments.
        input_file: Path to a text file containing URLs, or '-' for stdin.
        stream: Whether to stream data to stdout instead of writing to disk.
        typehash: Hash algorithm type (e.g., md5, sha256).
        checksum: Expected checksum checksum (only evaluated if a single valid link is provided).
        output_dir: Destination directory for downloaded files.
        dry_run: Simulate the download process (metadata fetch only).
        no_ui: Disable GUI (plain text logs only) if set to True.
        quiet: Dead silence mode. No console output at all if set to True.
        json_logs: Output logs in structured JSON Lines format.
        verify: Enable or disable post-download checksum verification.
        threads: Maximum number of concurrent download connections.
        min_chunk_size_mb: Minimum chunk size in MB for disk mode.
        max_stream_chunk_size_mb: Target chunk size in MB for stream mode.
        buffer_size_mb: Maximum memory buffer size in MB for streaming.
        speed_limit: Global bandwidth throttle limit in MB/s.
        browser: Browser TLS fingerprint to impersonate.
        debug: Enable debug mode to propagate full tracebacks on failure.
    """  # noqa: E501

    ui_config = UIConfig(
        is_verify=verify,
        is_dry_run=dry_run,
        quiet=quiet,
        no_ui=no_ui,
        json_logs=json_logs,
        log_file_dir=Path(output_dir),
        is_debug=debug,
    )
    ui = create_monitor(ui_config)
    ui.start()

    try:
        config = HydraConfig(
            is_stream=stream,
            threads=threads,
            dry_run=dry_run,
            min_chunk_size_mb=min_chunk_size_mb,
            max_stream_chunk_size_mb=max_stream_chunk_size_mb,
            speed_limit=speed_limit,
            output_dir=Path(output_dir),
            buffer_size_mb=buffer_size_mb,
            client_kwargs=None,
            impersonate=impersonate,
            debug=True,
        )

        active_links = validate(links=links, input_file=input_file, ui=ui)

        expected_checksums = _prepare_checksums(active_links, checksum, typehash)

        assert sys.__stdout__ is not None
        is_terminal = sys.__stdout__.isatty()
        if stream and is_terminal and not dry_run and not ON_TEST_HOOK:
            ui.report(
                InvalidParameterError(
                    param="stream",
                    reason="Warning: You are running in --stream mode but output is "
                    "not redirected!"
                    "\nThe downloaded binary data will be discarded.",
                )
            )

            if not expected_checksums:
                ui.report(
                    ValidationError(
                        param="stream",
                        reason="Please use a pipe (e.g., '| zcat') or redirect "
                        "to a file (e.g., '> file.gz').\nAborting to save bandwidth.",
                    )
                )
            else:
                ui.report(
                    InvalidParameterError(
                        param="stream",
                        reason="Proceeding in 'Verification Only' mode "
                        "since --checksum is provided.",
                    )
                )
        else:
            async with HydraDaemon(config=config, initial_ui=ui) as daemon:
                tasks: list[int] = []

                for i in active_links:
                    if (
                        task := await daemon.add_download(
                            i,
                            expected_checksums=checksum,
                            type_hash=typehash if checksum else None,
                        )
                    ) is not None:
                        tasks.append(task)

                if stream and not config.dry_run:
                    await _stream_download_tasks(
                        daemon,
                        tasks,
                        ui,
                    )

                else:
                    for i in tasks:
                        await daemon.wait_for_file(i)

    except (KeyboardInterrupt, asyncio.CancelledError):
        if debug:
            raise
        sys.exit(ExitCode.INTERRUPTED)

    except (Exception, ExceptionGroup) as e:
        if debug:
            raise

        all_errors = flatten_exceptions(e)

        codes: list[int] = []
        for err in all_errors:
            if isinstance(err, HydraError):
                ui.report(err)
                ui.log(f"FATAL: {err!r}", status=LogStatus.CRITICAL)
                codes.append(err.exit_code)

        exit_code = max(codes) if codes else ExitCode.GENERAL_ERROR

        sys.exit(exit_code)

    finally:
        await ui.stop()


def flatten_exceptions(e: BaseException) -> list[BaseException]:
    """Разворачивает BaseExceptionGroup в плоский список ошибок."""
    if isinstance(e, BaseExceptionGroup):
        result: list[BaseException] = []
        # Pyright bug: .exceptions is typed
        # as tuple[Unknown | BaseExceptionGroup[Unknown], ...]
        for child in e.exceptions:  # type: ignore[reportUnknownVariableType]
            result.extend(flatten_exceptions(child))  # type: ignore[reportUnknownVariableType]
        return result
    return [e]


def get_cfg(key: str, default: Any = None) -> Any:  # noqa: ANN401
    return USER_CONFIG.get(key, default)


@app.command()
def cli(
    *,
    links: Annotated[
        list[str] | None,
        typer.Argument(
            help="List of target URLs to download.",
            default_factory=partial(get_cfg, "links"),
        ),
    ],
    input_file: Annotated[
        str | None,
        typer.Option(
            "-i",
            "--input",
            help="Read URLs from file or '-' for stdin",
            default_factory=partial(get_cfg, "input"),
        ),
    ],
    typehash: Annotated[
        TypeHash,
        typer.Option(
            "--typehash",
            "-th",
            help="Hash algorithm type (e.g., md5, sha256).",
            default_factory=partial(get_cfg, "typehash", "md5"),
        ),
    ],
    checksum: Annotated[
        str | None,
        typer.Option(
            "--checksum",
            help="Expected checksum checksum (applicable only for a single URL).",
            default_factory=partial(get_cfg, "checksum"),
        ),
    ],
    output_dir: Annotated[
        str | None,
        typer.Option(
            "-o",
            "--output",
            help="Destination directory for downloaded files.",
            default_factory=partial(get_cfg, "output", None),
        ),
    ],
    threads: Annotated[
        int | None,
        typer.Option(
            "-t",
            "--threads",
            help="Number of concurrent download connections.",
            default_factory=partial(get_cfg, "threads"),
        ),
    ],
    stream: Annotated[
        bool,
        typer.Option(
            "-s",
            "--stream",
            help="Enable streaming mode (outputs to stdout without saving to disk).",
            default_factory=partial(get_cfg, "stream", False),
        ),
    ],
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            "-dr",
            help="""Simulate the process: fetch metadata, check disk space, and print a
             report without downloading data.""",
            default_factory=partial(get_cfg, "dry-run", False),
        ),
    ],
    min_chunk_size_mb: Annotated[
        int | None,
        typer.Option(
            "--min-chunk-mb",
            help="Minimum chunk size in Megabytes for standard disk downloads.",
            default_factory=partial(get_cfg, "min-chunk-mb", 1),
        ),
    ],
    max_stream_chunk_size_mb: Annotated[
        int | None,
        typer.Option(
            "--stream-chunk-mb",
            help="Target chunk size in Megabytes for streaming mode.",
            default_factory=partial(get_cfg, "stream-chunk-mb", 5),
        ),
    ],
    buffer_size_mb: Annotated[
        int | None,
        typer.Option(
            "--buffer",
            "-b",
            help="Maximum stream buffer size in Megabytes to prevent OOM.",
            default_factory=partial(get_cfg, "buffer"),
        ),
    ],
    speed_limit: Annotated[
        float | None,
        typer.Option(
            "--limit",
            "-l",
            help="Global download speed limit in MB/s.",
            default_factory=partial(get_cfg, "limit"),
        ),
    ],
    no_ui: Annotated[
        bool,
        typer.Option(
            "--no-ui",
            "-nu",
            help="Disable GUI (plain text logs only) if set to True.",
            default_factory=partial(get_cfg, "no-ui", False),
        ),
    ],
    quiet: Annotated[
        bool,
        typer.Option(
            "--quiet",
            "-q",
            help="Dead silence. No console output at all.",
            default_factory=partial(get_cfg, "quiet", False),
        ),
    ],
    json_logs: Annotated[
        bool,
        typer.Option(
            "--json",
            "-j",
            help="Output logs in JSON Lines format (for machines).",
            default_factory=partial(get_cfg, "json", False),
        ),
    ],
    verify: Annotated[
        bool,
        typer.Option(
            "--verify/--no-verify",
            "-V/-N",
            help="Verify the downloaded file checksum. Use --no-verify to skip check.",
            default_factory=partial(get_cfg, "verify", True),
        ),
    ],
    browser: Annotated[
        BrowserTypeLiteral,
        typer.Option(
            "-B",
            "--browser",
            help="Browser TLS fingerprint to impersonate (e.g., chrome120, safari153).",
            default_factory=partial(get_cfg, "browser", "chrome120"),
        ),
    ],
    version: Annotated[
        bool | None,
        typer.Option("--version", "-v", callback=version_callback, is_eager=True),
    ] = None,
    debug: Annotated[bool, typer.Option("--debug", "-d")] = False,
) -> None:
    """
    HydraStream: Concurrent HTTP downloader with in-memory stream reordering
    (curl_cffi + uvloop).
    """

    if threads is None:
        threads = 128
    output_dir = output_dir + "/downloads" if output_dir is not None else "downloads"
    if min_chunk_size_mb is None:
        min_chunk_size_mb = 5
    if max_stream_chunk_size_mb is None:
        max_stream_chunk_size_mb = 5

    asyncio.run(
        async_main(
            links=links,
            input_file=input_file,
            stream=stream,
            typehash=typehash,
            checksum=checksum,
            threads=threads,
            dry_run=dry_run,
            min_chunk_size_mb=min_chunk_size_mb,
            max_stream_chunk_size_mb=max_stream_chunk_size_mb,
            speed_limit=speed_limit,
            no_ui=no_ui,
            quiet=quiet,
            output_dir=output_dir,
            buffer_size_mb=buffer_size_mb,
            json_logs=json_logs,
            verify=verify,
            impersonate=browser,
            debug=debug,
        )
    )


if __name__ == "__main__":
    app()
