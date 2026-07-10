# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
import time
from dataclasses import field
from typing import assert_never, override

from hydrastream.domain.base_actor import BaseActor, ErrorVerdict
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.exceptions import (
    GracefulShutdownError,
    LogStatus,
)
from hydrastream.interfaces import NetworkStream
from hydrastream.messages.base import PoisonPill
from hydrastream.messages.traffic import (
    CheckpointReachedCmd,
    DiskBufferClearedSignal,
    DiskBufferFullSignal,
    RegisterStreamCmd,
    RemoveStreamCmd,
    ThrottlerMsg,
)


@hydra_dataclass
class ThrottleController(BaseActor[ThrottlerMsg]):
    active_stream: set[NetworkStream] = field(default_factory=set[NetworkStream])

    speed_limit: float | None
    bytes_to_check: int
    _prev_bytes: int = 0
    _last_checkpoint_time: float = 0.0

    is_disk_choked: bool = False

    def __post_init__(self) -> None:
        if self.speed_limit:
            self.speed_limit *= 1024**2

    @override
    async def _on_start(self) -> None:
        self._last_checkpoint_time = time.monotonic()

    @override
    async def _handle_msg(self, msg: ThrottlerMsg) -> None:
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

            case CheckpointReachedCmd(new_btc=btc):
                self.bytes_to_check = btc
                await self._enforce_throttling()

            case _ as unreachable:
                await super()._handle_msg(unreachable)
                assert_never(unreachable)

    @override
    async def _on_error(
        self, e: Exception, msg: ThrottlerMsg | PoisonPill | None = None
    ) -> ErrorVerdict:

        if isinstance(e, GracefulShutdownError):
            return ErrorVerdict.STOP

        self.ui.log(f"Throttle controller failed: {e}", status=LogStatus.ERROR)

        if self.is_debug:
            return ErrorVerdict.ESCALATE
        return ErrorVerdict.STOP

    async def _enforce_throttling(self) -> None:
        now = time.monotonic()
        elapsed = min(1, now - self._last_checkpoint_time)

        if elapsed <= 0 or not self.speed_limit:
            return

        target_time = self.bytes_to_check / self.speed_limit

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
