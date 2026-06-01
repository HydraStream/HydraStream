import sys
from dataclasses import field
from typing import Generic, TypeVar

from hydrastream.domain.hydra_dataclass import hydra_dataclass

T_co = TypeVar("T_co", covariant=True)


@hydra_dataclass(order=True, frozen=True)
class Envelope(Generic[T_co]):
    sort_key: tuple[int, ...] = field(default=(0,))

    payload: T_co = field(compare=False)

    @classmethod
    def poison_pill(cls) -> "Envelope[StopMsg]":
        return Envelope(sort_key=(sys.maxsize,), payload=StopMsg())


@hydra_dataclass(frozen=True)
class StopMsg:
    pass
