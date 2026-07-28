# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import sys
from abc import ABC, abstractmethod
from collections.abc import Callable, Coroutine
from dataclasses import field
from typing import Any, override

from hydrastream.domain.hydra_dataclass import hydra_dataclass


@hydra_dataclass(frozen=True)
class _Envelope[T]:
    sort_key: tuple[int, ...] = field(default=(0,))

    payload: T = field(compare=False)

    def __lt__(self, other: "_Envelope[Any]") -> bool:
        return self.sort_key < other.sort_key


@hydra_dataclass(frozen=True)
class TerminalPill:
    """Последняя пилюля. Должна достаться только 'последнему выжившему'."""


@hydra_dataclass(frozen=True)
class StandardPill:
    """Обычная пилюля. Останавливает любой свободный воркер."""

    pass


type PoisonPill = StandardPill | TerminalPill


@hydra_dataclass(frozen=True)
class ActorQueue[T](ABC):
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
class ActorFifoQueue[T](ActorQueue[T]):
    """Реализация для стандартной FIFO очереди."""

    maxsize: int = 0
    _raw_queue: asyncio.Queue[T | PoisonPill] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "_raw_queue", asyncio.Queue(maxsize=self.maxsize))

    @override
    async def send_data(self, data: T, sort_key: tuple[int, ...] = (0,)) -> None:
        await self._raw_queue.put(data)

    @override
    async def send_poison_pills(self, count: int = 1) -> None:
        for _ in range(count - 1):
            await self._raw_queue.put(StandardPill())
        await self._raw_queue.put(TerminalPill())

    @override
    def send_poison_pills_nowait(self, count: int = 1) -> None:
        loop = asyncio.get_running_loop()
        for _ in range(count - 1):
            loop.call_soon(self._raw_queue.put_nowait, StandardPill())
        loop.call_soon(self._raw_queue.put_nowait, TerminalPill())

    @override
    async def get(self) -> T | PoisonPill:
        return await self._raw_queue.get()

    @override
    def get_nowait(self) -> T | PoisonPill:
        return self._raw_queue.get_nowait()

    @override
    def empty(self) -> bool:
        return self._raw_queue.empty()


@hydra_dataclass(frozen=True)
class ActorPriorityQueue[T](ActorQueue[T]):
    """Реализация для приоритетной очереди."""

    maxsize: int = 0
    _raw_queue: asyncio.PriorityQueue[_Envelope[T | PoisonPill]] = field(
        init=False, repr=False
    )

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "_raw_queue", asyncio.PriorityQueue(maxsize=self.maxsize)
        )

    @override
    async def send_data(self, data: T, sort_key: tuple[int, ...] = (0,)) -> None:
        await self._raw_queue.put(_Envelope(sort_key=sort_key, payload=data))

    @override
    async def send_poison_pills(self, count: int = 1) -> None:
        for i in range(count - 1, 0, -1):
            await self._raw_queue.put(
                _Envelope(sort_key=(sys.maxsize - i,), payload=StandardPill())
            )
        await self._raw_queue.put(
            _Envelope(sort_key=(sys.maxsize,), payload=TerminalPill())
        )

    @override
    def send_poison_pills_nowait(self, count: int = 1, interrupt: bool = False) -> None:
        loop = asyncio.get_running_loop()
        priority = -1 if interrupt else sys.maxsize

        for i in range(count - 1, 0, -1):
            loop.call_soon(
                self._raw_queue.put_nowait,
                _Envelope(sort_key=(priority - i,), payload=StandardPill()),
            )

        loop.call_soon(
            self._raw_queue.put_nowait,
            _Envelope(sort_key=(priority,), payload=TerminalPill()),
        )

    @override
    async def get(self) -> T | PoisonPill:
        env = await self._raw_queue.get()
        return env.payload

    @override
    def get_nowait(self) -> T | PoisonPill:
        env = self._raw_queue.get_nowait()
        return env.payload

    @override
    def empty(self) -> bool:
        return self._raw_queue.empty()


async def ask[T_Res, T_Msg, *Ts](
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
    reply_future: asyncio.Future[T_Res] = asyncio.Future()

    # Construct the message embedding our future
    msg = msg_factory(*args, reply_future)
    await inbox.send_data(msg, sort_key=sort_key)

    try:
        return await asyncio.wait_for(reply_future, timeout=timeout)
    except TimeoutError as e:
        reply_future.cancel()
        raise RuntimeError(
            f"Ask request timed out after {timeout}s. "
            "Target actor is dead or overloaded."
        ) from e
    except asyncio.CancelledError:
        reply_future.cancel()
        raise
    except Exception:
        reply_future.cancel()
        raise
