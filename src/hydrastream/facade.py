# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.


import asyncio
import sys
from collections.abc import AsyncGenerator, Generator
from contextlib import contextmanager
from pathlib import Path
from types import TracebackType
from typing import Any, Self, TextIO
from urllib.parse import urlparse

from curl_cffi import BrowserTypeLiteral

from hydrastream.domain.config import HydraConfig
from hydrastream.domain.context import HydraContext
from hydrastream.domain.entities import Checksum, TypeHash
from hydrastream.engine import run_downloads, stream_all, teardown_engine
from hydrastream.exceptions import FileReadError, InvalidParameterError, ValidationError
from hydrastream.interfaces import (
    Hasher,
    HashProvider,
    MonitorBackend,
    NetworkBackend,
    StorageBackend,
)


class HydraClient:
    def __init__(
        self,
        config: HydraConfig | None = None,
        threads: int = 1,
        no_ui: bool = False,
        quiet: bool = False,
        output_dir: str = "download",
        dry_run: bool = False,
        min_chunk_size_mb: int = 1,
        max_stream_chunk_size_mb: int = 5,
        buffer_size_mb: int | None = None,
        speed_limit: float | None = None,
        json_logs: bool = False,
        verify: bool = True,
        debug: bool = False,
        impersonate: BrowserTypeLiteral = "chrome120",
        client_kwargs: dict[str, Any] | None = None,
        custom_providers: dict[str, "HashProvider"] | None = None,
        custom_storage: StorageBackend | None = None,
        custom_monitor: MonitorBackend | None = None,
        custom_network: NetworkBackend | None = None,
        custom_hasher: Hasher | None = None,
    ) -> None:
        if config:
            self.config = config
        else:
            self.config = HydraConfig(
                threads=threads,
                dry_run=dry_run,
                min_chunk_size_mb=min_chunk_size_mb,
                max_stream_chunk_size_mb=max_stream_chunk_size_mb,
                speed_limit=speed_limit,
                no_ui=no_ui,
                quiet=quiet,
                output_dir=Path(output_dir),
                buffer_size_mb=buffer_size_mb,
                json_logs=json_logs,
                verify=verify,
                impersonate=impersonate,
                debug=debug,
                client_kwargs=client_kwargs,
                custom_providers=custom_providers,
                custom_storage=custom_storage,
                custom_monitor=custom_monitor,
                custom_network=custom_network,
                custom_hasher=custom_hasher,
            )
        self.state: HydraContext | None = None

    async def __aenter__(self) -> Self:
        if self.ui_init:
            await log_start(self.ui)
        return self

    async def __aexit__(
        self,
        _exc_type: type[BaseException] | None,
        _exc: BaseException | None,
        _tb: TracebackType | None,
    ) -> None:
        if self.state is not None:
            loop = asyncio.get_running_loop()
            await teardown_engine(self.state, loop)
        if self.ui_init:
            await log_stop(self.ui)

    async def run(
        self,
        links: list[str] | str | None = None,
        input_file: str | None = None,
        expected_checksums: dict[str, tuple[TypeHash, str] | Checksum] | None = None,
    ) -> None:
        links = await self.validate(links, input_file)
        self.state = HydraContext(config=self.config, is_stream=False)
        await run_downloads(self.state, links, expected_checksums)

    async def stream(
        self,
        links: list[str] | str | None = None,
        input_file: str | None = None,
        expected_checksums: dict[str, tuple[TypeHash, str] | Checksum] | None = None,
    ) -> AsyncGenerator[tuple[str, AsyncGenerator[bytes]]]:
        links = await self.validate(links, input_file)
        self.state = HydraContext(config=self.config, is_stream=True)
        return stream_all(self.state, links, expected_checksums)

    async def validate(
        self,
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
        valid_links = await parse_urls(self.ui, links, input_file)
        if not valid_links:
            raise ValidationError(
                param="links", reason="No valid URLs found to process!"
            )

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
        return result.scheme in ("http", "https") and bool(result.netloc)
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


async def parse_urls(
    ui: MonitorBackend, links_from_args: list[str] | None, filepath: str | None
) -> list[str]:
    all_links: list[str] = []

    # Обработка аргументов
    if links_from_args:
        for url in links_from_args:
            if is_valid_url(url):
                all_links.append(url)
            else:
                await ui.report(
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
                    await ui.report(
                        InvalidParameterError(
                            param="file_link",
                            value=url,
                            reason="Invalid URL in input file",
                        ),
                    )

    return list(dict.fromkeys(all_links))
