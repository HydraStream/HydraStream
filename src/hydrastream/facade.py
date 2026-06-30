# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.


import asyncio
import sys
from collections.abc import AsyncGenerator, Generator
from contextlib import contextmanager
from pathlib import Path
from types import TracebackType
from typing import Self, TextIO
from urllib.parse import urlparse

from hydrastream.domain.config import HydraConfig
from hydrastream.domain.context import HydraContext, build_context
from hydrastream.domain.entities import Checksum, TypeHash
from hydrastream.engine import run_downloads, stream_all, teardown_engine
from hydrastream.exceptions import FileReadError, InvalidParameterError, ValidationError
from hydrastream.interfaces import (
    MonitorBackend,
)
from hydrastream.monitor import (
    BaseMonitorKwargs,
    JsonMonitor,
    PlainMonitor,
    QuietMonitor,
    RichMonitor,
)


class HydraClient:
    def __init__(
        self,
        config: HydraConfig,
    ) -> None:

        self.config = config
        self.ui = _create_monitor(config=config)
        self.state: HydraContext | None = None

    async def __aenter__(self) -> Self:
        await self.ui.start()
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

        await self.ui.stop()

    async def run(
        self,
        links: list[str] | str | None = None,
        input_file: str | None = None,
        expected_checksums: dict[str, tuple[TypeHash, str] | Checksum] | None = None,
    ) -> None:

        links = await self.validate(links, input_file)
        self.state = build_context(config=self.config, ui=self.ui, is_stream=False)
        await run_downloads(self.state, links, expected_checksums)

    async def stream(
        self,
        links: list[str] | str | None = None,
        input_file: str | None = None,
        expected_checksums: dict[str, tuple[TypeHash, str] | Checksum] | None = None,
    ) -> AsyncGenerator[tuple[str, AsyncGenerator[bytes]]]:
        links = await self.validate(links, input_file)
        self.state = build_context(config=self.config, ui=self.ui, is_stream=True)
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


def _create_monitor(config: HydraConfig) -> MonitorBackend:
    if config.custom_monitor is None:
        base_resolver_kwargs: BaseMonitorKwargs = {
            "is_verify": config.verify,
            "log_file": config.output_dir,
            "is_debug": config.debug,
        }

        if config.json_logs:
            ui = JsonMonitor(**base_resolver_kwargs)
        elif config.quiet:
            ui = QuietMonitor(**base_resolver_kwargs)
        elif config.no_ui:
            ui = PlainMonitor(**base_resolver_kwargs)
        else:
            ui = RichMonitor(**base_resolver_kwargs)
    else:
        ui = config.custom_monitor
    return ui
