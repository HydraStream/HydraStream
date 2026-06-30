from abc import ABC, abstractmethod
from typing import Generic, TypeVar, final

from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.exceptions import LogStatus
from hydrastream.interfaces import MonitorBackend
from hydrastream.messages.base import ActorQueue, PoisonPill, StandardPill, TerminalPill

T_Payload = TypeVar("T_Payload")


@hydra_dataclass
class BaseActor(ABC, Generic[T_Payload]):
    """Универсальный каркас для любого актора в системе."""

    is_debug: bool
    inbox: ActorQueue[T_Payload | PoisonPill]
    ui: MonitorBackend

    @final
    async def run(self) -> None:
        """Шаблонный метод. Ни один наследник НЕ ДОЛЖЕН его переопределять!"""
        await self._on_start()
        msg = None
        try:
            while True:
                msg = await self.inbox.get()

                if isinstance(msg, TerminalPill):
                    await self._on_terminal_pill()
                    break

                if isinstance(msg, StandardPill):
                    await self._on_standard_pill()
                    break

                await self._handle_msg(msg)

        except Exception as e:
            await self._on_error(e, msg)

        finally:
            await self._on_stop()

    async def _on_start(self) -> None:
        pass  # Выполняется до начала цикла

    @abstractmethod
    async def _handle_msg(self, msg: T_Payload) -> None:
        """Базовая реализация, которая сработает, ТОЛЬКО если наследник
        не обработал сообщение в своем match/case и провалился в самый низ.
        """
        actor_name = self.__class__.__name__
        msg_type = type(msg).__name__
        error_text = (
            f"[{actor_name}] Message '{msg_type}' was matched by type, "
            f"but NO case branch handled it! Msg: {msg}"
        )

        if self.is_debug:
            raise RuntimeError(error_text)

        if self.ui:
            await self.ui.log(error_text, status=LogStatus.ERROR)

    async def _on_standard_pill(self) -> None:
        pass  # Что делать при обычной остановке?

    async def _on_terminal_pill(self) -> None:
        pass  # Что делать, если ты Последний Выживший?

    async def _on_error(
        self, e: Exception, msg: T_Payload | PoisonPill | None = None
    ) -> None:
        """Дефолтная обработка ошибок (можно переопределить для логов)"""
        raise e

    async def _on_stop(self) -> None:
        pass  # Выполняется в finally
