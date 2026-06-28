# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import time
from dataclasses import field

from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.exceptions import (
    LogStatus,
)
from hydrastream.interfaces import MonitorBackend, NetworkStream
from hydrastream.messages.base import ActorFifoQueue, TerminalPill
from hydrastream.messages.traffic import (
    CheckpointReachedCmd,
    DiskBufferClearedSignal,
    DiskBufferFullSignal,
    RegisterStreamCmd,
    RemoveStreamCmd,
    ThrottlerMsg,
)


@hydra_dataclass
class ThrottleController:
    throttler_input: ActorFifoQueue[ThrottlerMsg]

    active_stream: set[NetworkStream] = field(default_factory=set[NetworkStream])

    speed_limit: float | None
    _frequency_speed_limit: int = 10
    _time_speed_limit: float = field(init=False)
    _bytes_to_check: int = field(init=False)
    _prev_bytes: int = 0
    _last_checkpoint_time: float = 0.0
    _target_time: float = 0.0

    is_debug: bool
    ui: MonitorBackend

    all_complete: asyncio.Event
    throttler_checkpoint_event: asyncio.Event

    is_disk_choked: bool = False

    def __post_init__(self) -> None:
        self._time_speed_limit = 1 / self._frequency_speed_limit
        if self.speed_limit:
            self.speed_limit = self.speed_limit * 1024**2
            self._bytes_to_check = int(self.speed_limit / self._frequency_speed_limit)
            self._target_time = self._bytes_to_check / self.speed_limit
        else:
            self._bytes_to_check = 5 * 1024**2

    async def run(self) -> None:  # noqa
        self._last_checkpoint_time = time.monotonic()

        while not self.all_complete.is_set():
            try:
                # Актор слушает ТОЛЬКО одну очередь!
                msg = await self.throttler_input.get()

                match msg:
                    case RegisterStreamCmd(stream=s):
                        self.active_stream.add(s)
                        # Если диск УЖЕ тупит, сразу режем скорость новичку!
                        if self.is_disk_choked:
                            s.set_speed_limit(1)

                    case RemoveStreamCmd(stream=s):
                        self.active_stream.discard(s)

                    case DiskBufferFullSignal():
                        # Авария на диске! Режем скорость ВСЕМ.
                        self.is_disk_choked = True
                        self._set_curl_speed_limit(limit=1)

                    case DiskBufferClearedSignal():
                        # Диск разгреб завалы!
                        self.is_disk_choked = False
                        self._set_curl_speed_limit(limit=0)

                    case CheckpointReachedCmd():
                        # Пришла порция байтов для ограничения скорости юзера
                        await self.enforce_throttling()

                    case TerminalPill():
                        break

                    case _:
                        if self.is_debug:
                            raise RuntimeError(
                                f"Unknown message type in throttler_input: {type(msg)}"
                            )
                        await self.ui.log(
                            f"Received unknown message: {msg}",
                            status=LogStatus.ERROR,
                        )

            except Exception as e:
                if self.is_debug:
                    raise
                await self.ui.log(
                    f"Throttle controller failed: {e}", status=LogStatus.ERROR
                )

    async def enforce_throttling(self) -> None:
        now = time.monotonic()
        elapsed = min(1, now - self._last_checkpoint_time)

        if elapsed <= 0 or not self.speed_limit:
            return

        target_time = self._bytes_to_check / self.speed_limit

        if elapsed < target_time:
            sleep_duration = target_time - elapsed

            # Ставим на паузу
            self._set_curl_speed_limit(limit=1)

            await asyncio.sleep(sleep_duration)

            # СНИМАЕМ ПАУЗУ ТОЛЬКО ЕСЛИ ДИСК СВОБОДЕН!
            if not self.is_disk_choked:
                self._set_curl_speed_limit(limit=0)

        # Обновляем время после паузы!
        self._last_checkpoint_time = time.monotonic()

    def _set_curl_speed_limit(self, limit: int) -> None:
        """Вспомогательная функция для прохода по активным потокам."""
        for r in self.active_stream:
            r.set_speed_limit(limit)
