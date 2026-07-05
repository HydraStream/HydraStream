from dataclasses import dataclass, field

from hydrastream.domain.entities import Checksum, TypeHash
from hydrastream.domain.hydra_dataclass import hydra_dataclass


@dataclass(frozen=True)
class RawLinkItem:
    """The clean singular item sent by any external producer or network listener."""

    url: str
    checksum: Checksum | tuple[TypeHash, str] | None = None


@hydra_dataclass(frozen=True)
class LinkData:
    id: int
    url: str
    checksum: Checksum | None


@hydra_dataclass(order=True, frozen=True)
class WriteChunk:
    fd: int
    offset: int
    length: int = field(compare=False)
    data: list[bytes] = field(compare=False)


@hydra_dataclass(order=True, frozen=True)
class StreamChunk:
    start: int
    data: list[bytes] = field(compare=False)
