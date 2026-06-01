# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

from __future__ import annotations

from pathlib import Path
from typing import (
    Any,
)
from urllib.parse import urlparse

from curl_cffi import (
    BrowserTypeLiteral,
)
from pydantic import (
    Field,
    PositiveInt,
    computed_field,
    field_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

from hydrastream.interfaces import HashProvider


class HydraConfig(BaseSettings):
    # Настройки загрузки из окружения и .env
    model_config = SettingsConfigDict(
        env_prefix="HYDRA_",
        env_file=".env",
        extra="ignore",
        frozen=True,
        arbitrary_types_allowed=True,
    )

    # --- Поля с базовой валидацией Pydantic ---
    threads: int = Field(default=128, ge=1, le=128)
    no_ui: bool = False
    quiet: bool = False
    output_dir: str = "download"
    speed_limit: float | None = Field(default=None, gt=0.0)
    dry_run: bool = False
    json_logs: bool = False
    verify: bool = True
    impersonate: BrowserTypeLiteral = "chrome120"
    debug: bool = False

    # Входные параметры в МБ (аналог InitVar)
    min_chunk_size_mb: PositiveInt = 1
    max_stream_chunk_size_mb: PositiveInt = 5
    buffer_size_mb: int | None = Field(default=None, ge=50)

    links: list[str] = Field(default_factory=list)
    client_kwargs: dict[str, Any] | None = Field(default=None, exclude=True)

    custom_providers: dict[str, HashProvider] | None = Field(
        default=None, repr=False, exclude=True
    )

    @field_validator("output_dir")
    @classmethod
    def validate_output(cls, v: str) -> str:
        path = Path(v)
        if path.exists() and not path.is_dir():
            raise ValueError(f"Path '{v}' exists but is not a directory")
        try:
            path.resolve()
        except Exception as e:
            raise ValueError(f"Invalid path format: {v}") from e
        return v

    @field_validator("links")
    @classmethod
    def validate_links_logic(cls, v: list[str]) -> list[str]:
        for url in v:
            result = urlparse(url)
            if not (result.scheme in ("http", "https") and result.netloc):
                raise ValueError(f"Only HTTP/HTTPS are supported: {url}")
        return v

    @computed_field
    @property
    def MIN_CHUNK(self) -> int:  # noqa: N802
        return self.min_chunk_size_mb * 1024**2

    @computed_field
    @property
    def STREAM_CHUNK_SIZE(self) -> int:  # noqa: N802
        return self.max_stream_chunk_size_mb * 1024**2

    @computed_field
    @property
    def BUFFER_SIZE(self) -> int:  # noqa: N802
        if self.buffer_size_mb:
            return self.buffer_size_mb * 1024**2
        return 50 * 1024**2
