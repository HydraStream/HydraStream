# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

from __future__ import annotations

import os
from pathlib import Path
from typing import (
    Any,
)

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

from hydrastream.interfaces import (
    Hasher,
    HashProvider,
    MonitorBackend,
    NetworkBackend,
    StorageBackend,
)


class UIConfig(BaseSettings):
    is_verify: bool = True
    quiet: bool = True
    no_ui: bool = False
    json_logs: bool = False
    log_file_dir: Path = Field(default=Path("download"))
    is_debug: bool = False

    @field_validator("log_file_dir")
    @classmethod
    def validate_output(cls, v: Path) -> Path:
        # 1. Превращаем путь в абсолютный
        try:
            # resolve() делает путь абсолютным и убирает симлинки
            # strict=False позволяет работать даже с еще не созданными папками
            v = v.resolve(strict=False)
        except Exception as e:
            raise ValueError(f"Invalid path format: {v}") from e

        # 2. Проверяем, что это не файл
        if v.exists() and not v.is_dir():
            raise ValueError(f"Path '{v}' exists but is not a directory")

        # 3. Проверяем права на запись (os.W_OK)
        # Если папка существует, проверяем её. Если нет — проверяем родительскую папку.
        target_to_check = v if v.exists() else v.parent

        if not os.access(target_to_check, os.W_OK):
            raise ValueError(f"No write permissions for path: '{target_to_check}'")

        return v


class HydraConfig(BaseSettings):
    # Настройки загрузки из окружения и .env
    model_config = SettingsConfigDict(
        env_prefix="HYDRA_",
        env_file=".env",
        extra="ignore",
        frozen=True,
        arbitrary_types_allowed=True,
    )
    is_stream: bool = False
    is_verify: bool = True
    dry_run: bool = False
    debug: bool = False
    # --- Поля с базовой валидацией Pydantic ---
    threads: int = Field(default=128, ge=1, le=128)
    output_dir: Path = Field(default=Path("download"))
    speed_limit: float | None = Field(default=None, gt=0.0)
    impersonate: BrowserTypeLiteral = "chrome120"

    # Входные параметры в МБ (аналог InitVar)
    min_chunk_size_mb: PositiveInt = 1
    max_stream_chunk_size_mb: PositiveInt = 5
    buffer_size_mb: int | None = Field(default=None, ge=50)

    client_kwargs: dict[str, Any] | None = Field(default=None, exclude=True)

    custom_providers: dict[str, HashProvider] | None = Field(
        default=None, repr=False, exclude=True
    )
    custom_storage: StorageBackend | None = None
    custom_monitor: MonitorBackend | None = None
    custom_network: NetworkBackend | None = None
    custom_hasher: Hasher | None = None

    @field_validator("output_dir")
    @classmethod
    def validate_output(cls, v: Path) -> Path:
        # 1. Превращаем путь в абсолютный
        try:
            # resolve() делает путь абсолютным и убирает симлинки
            # strict=False позволяет работать даже с еще не созданными папками
            v = v.resolve(strict=False)
        except Exception as e:
            raise ValueError(f"Invalid path format: {v}") from e

        # 2. Проверяем, что это не файл
        if v.exists() and not v.is_dir():
            raise ValueError(f"Path '{v}' exists but is not a directory")

        # 3. Проверяем права на запись (os.W_OK)
        # Если папка существует, проверяем её. Если нет — проверяем родительскую папку.
        target_to_check = v if v.exists() else v.parent

        if not os.access(target_to_check, os.W_OK):
            raise ValueError(f"No write permissions for path: '{target_to_check}'")

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
