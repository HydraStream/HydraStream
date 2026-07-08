import asyncio
import contextlib
import sys
from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine
from dataclasses import field
from typing import Any, Generic, TypeAlias, TypeVar, TypeVarTuple

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
    """Последняя пилюля. Должна достаться только 'последнему выжившему'."""


@hydra_dataclass(frozen=True)
class StandardPill:
    """Обычная пилюля. Останавливает любой свободный воркер."""

    pass


PoisonPill: TypeAlias = StandardPill | TerminalPill


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
    async def get(self) -> T | PoisonPill:
        pass

    @abstractmethod
    def get_nowait(self) -> T | PoisonPill:
        pass

    @abstractmethod
    def empty(self) -> bool:
        pass


@hydra_dataclass(frozen=True)
class ActorFifoQueue(ActorQueue[T]):
    """Реализация для стандартной FIFO очереди."""

    maxsize: int = 0
    _raw_queue: asyncio.Queue[T | PoisonPill] = field(init=False, repr=False)

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

    async def get(self) -> T | PoisonPill:
        return await self._raw_queue.get()

    def get_nowait(self) -> T | PoisonPill:
        return self._raw_queue.get_nowait()

    def empty(self) -> bool:
        return self._raw_queue.empty()


@hydra_dataclass(frozen=True)
class ActorPriorityQueue(ActorQueue[T]):
    """Реализация для приоритетной очереди."""

    maxsize: int = 0
    _raw_queue: asyncio.PriorityQueue[_Envelope[T | PoisonPill]] = field(
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

    async def get(self) -> T | PoisonPill:
        env = await self._raw_queue.get()
        return env.payload

    def get_nowait(self) -> T | PoisonPill:
        env = self._raw_queue.get_nowait()
        return env.payload

    def empty(self) -> bool:
        return self._raw_queue.empty()


T_Res = TypeVar("T_Res")
T_Msg = TypeVar("T_Msg")
Ts = TypeVarTuple("Ts")


async def ask(
    *args: *Ts,
    inbox: ActorQueue[T_Msg],
    msg_factory: Callable[[*Ts, asyncio.Future[T_Res]], T_Msg],
    timeout: float = 10.0,
    sort_key: tuple[int, ...] = (0,),
) -> T_Res:
    """
    Sends a message to an actor and awaits a single response via Future.
    Fails fast with a RuntimeError if the target actor deadlocks or hangs.
    """
    loop = asyncio.get_running_loop()
    reply_future: asyncio.Future[T_Res] = loop.create_future()

    # Construct the message embedding our future
    msg = msg_factory(*args, reply_future)
    await inbox.send_data(msg, sort_key=sort_key)

    try:
        async with asyncio.timeout(timeout):
            return await reply_future
    except TimeoutError as e:
        # Crucial clean-up: if we timeout, cancel the future so the
        # target actor doesn't try to set a result on a dead request.
        reply_future.cancel()
        raise RuntimeError(
            f"Ask request timed out after {timeout}s. "
            "Target actor is dead or overloaded."
        ) from e
