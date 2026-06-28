import asyncio
import contextlib
import sys
from abc import ABC, abstractmethod
from collections.abc import Coroutine
from dataclasses import field
from typing import Any, Generic, TypeVar

from hydrastream.domain.hydra_dataclass import hydra_dataclass

T_co = TypeVar("T_co", covariant=True)
T = TypeVar("T")


@hydra_dataclass(frozen=True)
class _Envelope(Generic[T_co]):
    sort_key: tuple[int, ...] = field(default=(0,))

    payload: T_co = field(compare=False)

    def __lt__(self, other: "_Envelope[Any]") -> bool:
        return self.sort_key < other.sort_key


@hydra_dataclass(frozen=True)
class TerminalPill:
    pass


@hydra_dataclass(frozen=True)
class StandardPill(TerminalPill):
    """Обычная пилюля. Останавливает любой свободный воркер."""

    pass


@hydra_dataclass(frozen=True)
class ActorQueue(Generic[T], ABC):
    """Абстрактный интерфейс для работы с очередью актора."""

    @abstractmethod
    async def send_data(self, data: T, sort_key: tuple[int, ...] = (0,)) -> None:
        pass

    @abstractmethod
    async def send_poison_pills(
        self, count: int = 1
    ) -> Coroutine[Any, Any, None] | None:
        pass

    @abstractmethod
    def send_poison_pills_nowait(self, count: int = 1) -> None:
        """Экстренная неблокирующая отправка пилюль (безопасная к переполнению)."""
        pass

    @abstractmethod
    async def get(self) -> T | TerminalPill:
        pass

    @abstractmethod
    def empty(self) -> bool:
        pass


@hydra_dataclass(frozen=True)
class ActorFifoQueue(ActorQueue[T]):
    """Реализация для стандартной FIFO очереди."""

    maxsize: int = 0
    _raw_queue: asyncio.Queue[T | TerminalPill] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_raw_queue", asyncio.Queue(maxsize=self.maxsize))

    async def send_data(self, data: T, sort_key: tuple[int, ...] = (0,)) -> None:
        await self._raw_queue.put(data)

    async def send_poison_pills(self, count: int = 1) -> None:
        for _ in range(count - 1):
            await self._raw_queue.put(StandardPill())
        await self._raw_queue.put(TerminalPill())

    def send_poison_pills_nowait(self, count: int = 1) -> None:
        with contextlib.suppress(asyncio.QueueFull):
            for _ in range(count - 1):
                self._raw_queue.put_nowait(StandardPill())
            self._raw_queue.put_nowait(TerminalPill())

    async def get(self) -> T | TerminalPill:
        return await self._raw_queue.get()

    def empty(self) -> bool:
        return self._raw_queue.empty()


@hydra_dataclass(frozen=True)
class ActorPriorityQueue(ActorQueue[T]):
    """Реализация для приоритетной очереди."""

    maxsize: int = 0
    _raw_queue: asyncio.PriorityQueue[_Envelope[T | TerminalPill]] = field(
        init=False, repr=False
    )

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "_raw_queue", asyncio.PriorityQueue(maxsize=self.maxsize)
        )

    async def send_data(self, data: T, sort_key: tuple[int, ...] = (0,)) -> None:
        await self._raw_queue.put(_Envelope(sort_key=sort_key, payload=data))

    async def send_poison_pills(self, count: int = 1) -> None:

        for i in range(count - 1, 0, -1):
            await self._raw_queue.put(
                _Envelope(sort_key=(sys.maxsize - i,), payload=StandardPill())
            )
        await self._raw_queue.put(
            _Envelope(sort_key=(sys.maxsize,), payload=TerminalPill())
        )

    def send_poison_pills_nowait(self, count: int = 1) -> None:
        with contextlib.suppress(asyncio.QueueFull):
            for i in range(count - 1, 0, -1):
                self._raw_queue.put_nowait(
                    _Envelope(sort_key=(sys.maxsize - i,), payload=StandardPill())
                )
            self._raw_queue.put_nowait(
                _Envelope(sort_key=(sys.maxsize,), payload=TerminalPill())
            )

    async def get(self) -> T | TerminalPill:
        env = await self._raw_queue.get()
        return env.payload

    def empty(self) -> bool:
        return self._raw_queue.empty()
