# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

from __future__ import annotations

import sys
from dataclasses import field, replace
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Literal,
    Self,
)

import orjson
from pydantic import (
    BaseModel,
    ConfigDict,
    StringConstraints,
    TypeAdapter,
    model_validator,
)

from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.exceptions import (
    OrphanedChunkError,
)

if TYPE_CHECKING:
    pass


@hydra_dataclass
class Chunk:
    current_pos: int
    start: int
    end: int
    _file: File | None = field(repr=False, default=None)

    @property
    def file(self) -> File:
        if self._file is None:
            # Выбрасываем структурированную ошибку вместо безликого RuntimeError
            raise OrphanedChunkError(start_pos=self.start, end_pos=self.end)
        return self._file

    @property
    def is_finished(self) -> bool:
        return self.current_pos > self.end

    @property
    def size(self) -> int:
        return self.end - self.start + 1

    @property
    def uploaded(self) -> int:
        return self.current_pos - self.start + 1

    @property
    def remaining(self) -> int:
        return max(0, self.end - self.current_pos + 1)

    @property
    def get_header(self) -> dict[str, str]:
        return {"Range": f"bytes={self.current_pos}-{self.end}"}


TypeHash = Literal[
    "md5",
    "sha1",
    "sha224",
    "sha256",
    "sha384",
    "sha512",
    "blake2b",
    "blake2s",
    "sha3_224",
    "sha3_256",
    "sha3_384",
    "sha3_512",
    "shake_128",
    "shake_256",
]


EXPECTED_LENGTHS = {
    "md5": 32,
    "sha1": 40,
    "sha224": 56,
    "sha256": 64,
    "sha384": 96,
    "sha512": 128,
    "blake2b": 128,
    "blake2s": 64,
    "sha3_224": 56,
    "sha3_256": 64,
    "sha3_384": 96,
    "sha3_512": 128,
}

VARIABLE_LENGTH = {"shake_128", "shake_256"}

HashStr = Annotated[
    str, StringConstraints(pattern=r"^[0-9a-f]+$", strip_whitespace=True, to_lower=True)
]


class Checksum(BaseModel):
    model_config = ConfigDict(frozen=True)

    algorithm: TypeHash
    value: HashStr

    @model_validator(mode="after")
    def validate_hash_logic(self) -> Self:
        algo = self.algorithm
        length = len(self.value)

        # Если длина фиксирована — проверяем строго
        if algo in EXPECTED_LENGTHS:
            expected = EXPECTED_LENGTHS[algo]
            if length != expected:
                raise ValueError(
                    f"Invalid length for {algo}: expected {expected}, got {length}"
                )

        # Если это SHAKE — проверяем только, что это полные байты
        # (четное кол-во символов)
        elif algo in VARIABLE_LENGTH:
            if length % 2 != 0:
                raise ValueError(
                    f"Invalid length for {algo}: must be even, got {length}"
                )

        return self


@hydra_dataclass(frozen=True)
class FileMeta:
    id: int
    original_filename: str
    url: str
    content_length: int
    expected_checksum: Checksum | None = field(default=None)
    supports_ranges: bool


@hydra_dataclass
class File:
    meta: FileMeta
    actual_filename: str = ""
    chunk_size: int
    chunks: list[Chunk] = field(default_factory=list[Chunk])
    fd: int | None = field(default=None, repr=False)
    verified: bool = field(default=False)
    is_failed: bool = field(default=False)
    # _stream_queue: ActorFifoQueue[StreamChunk | None]] | None = None

    def create_chunks(self) -> None:
        if self.chunks:
            return
        if not self.meta.supports_ranges or self.meta.content_length <= 0:
            self.chunks.append(
                Chunk(
                    start=0,
                    end=sys.maxsize
                    if not self.meta.content_length
                    else self.meta.content_length - 1,
                    current_pos=0,
                    _file=self,
                )
            )
            return
        if self.chunk_size <= 0:
            raise ValueError(f"Chunk size must be positive, got {self.chunk_size}")
        part_count = -(-self.meta.content_length // self.chunk_size)

        for i in range(part_count):
            start = i * self.chunk_size
            end = min((i + 1) * self.chunk_size - 1, self.meta.content_length - 1)

            self.chunks.append(
                Chunk(
                    start=start,
                    end=end,
                    current_pos=start,
                    _file=self,
                )
            )

    @property
    def is_complete(self) -> bool:
        if not self.chunks:
            return False
        return all(c.is_finished for c in self.chunks)

    @property
    def downloaded_size(self) -> int:
        return sum(c.current_pos - c.start for c in (self.chunks or []))

    @property
    def progress(self) -> float:
        if self.meta.content_length <= 0:
            return 0.0
        return (self.downloaded_size / self.meta.content_length) * 100

    @classmethod
    def from_json(cls, content: bytes) -> Self:
        # 1. Сначала превращаем JSON в один большой словарь (через fast orjson)
        raw_data = orjson.loads(content)

        # 2. Используем TypeAdapter для автоматической сборки всей вложенности.
        # Он сам создаст FileMeta, внутри него Checksum, и список Chunk.
        file_obj = TypeAdapter(cls).validate_python(raw_data)

        # 3. Единственное, что Pydantic не сделает сам —
        # не проставит обратную ссылку _file в каждый чанк (т.к. это цикл)
        for chunk in file_obj.chunks:
            chunk._file = file_obj  # pyright: ignore[reportPrivateUsage]

        return file_obj

    def to_json(self) -> bytes:
        clear_file = replace(self, fd=None)
        return orjson.dumps(
            clear_file,
            default=pydantic_default,
            option=orjson.OPT_SERIALIZE_DATACLASS | orjson.OPT_INDENT_2,
        )


def pydantic_default(obj: object) -> dict[Any, Any]:
    if isinstance(obj, BaseModel):
        return obj.model_dump()
    raise TypeError(f"Type {type(obj)} not serializable by orjson")
