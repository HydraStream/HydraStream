import math
import time
from typing import assert_never, override

from hydrastream.domain.base_actor import BaseActor, ErrorVerdict
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.exceptions import LogStatus
from hydrastream.messages.base import ActorFifoQueue, PoisonPill
from hydrastream.messages.state import StateKeeperMsg, UpdateBytesToCheckCmd
from hydrastream.messages.traffic import ScaleDownSignal, ScaleUpSignal, TrafficSignal
from hydrastream.utils import format_size


@hydra_dataclass
class CheckpointEvent:
    pass


@hydra_dataclass
class TelemetryAnalyzer(BaseActor[CheckpointEvent]):
    threads: int
    current_limit: int
    controller_outbox: ActorFifoQueue[TrafficSignal | PoisonPill]
    state_outbox: ActorFifoQueue[StateKeeperMsg | PoisonPill]
    bytes_to_check: int
    _smoothed_speed: float = 0.0
    _prev_speed: float = 0.0
    _tau: float = 1.0
    _min_window: int = 1024
    _sensitivity: float = 0.05
    _last_checkpoint_time: float = 0.0
    _dynamic_limit: int = 1

    @override
    async def _handle_msg(self, msg: CheckpointEvent) -> None:
        match msg:
            case CheckpointEvent():
                await self._step()

            case _ as unreachable:
                await super()._handle_msg(unreachable)
                assert_never(unreachable)

    @override
    async def _on_error(
        self, e: Exception, msg: CheckpointEvent | PoisonPill | None = None
    ) -> ErrorVerdict:

        await self.ui.log(
            f"Adaptive controller failed: {e}",
            status=LogStatus.ERROR,
        )
        if self.is_debug:
            return ErrorVerdict.ESCALATE
        return ErrorVerdict.STOP

    def _calculate_ema(self, speed_now: float, elapsed: float) -> float:
        if self._smoothed_speed == 0.0:
            return speed_now
        alpha = 1.0 - math.exp(-elapsed / self._tau)
        return (alpha * speed_now) + ((1.0 - alpha) * self._smoothed_speed)

    async def _update_window(self, speed_now: float, elapsed: float) -> None:
        safe_speed = max(speed_now, 0.001)
        coef = 1 / safe_speed**0.25
        new_bytes = int(self.bytes_to_check * (1 - coef + elapsed))
        self.bytes_to_check = max(self._min_window, new_bytes)
        await self.state_outbox.send_data(
            UpdateBytesToCheckCmd(bytes_to_check=self.bytes_to_check)
        )

    async def _log_scale_event(self, direction: str, speed: float) -> None:
        """Вспомогательный метод для чистого логирования."""
        if direction == "up":
            msg = (
                f"Speed increased to {format_size(speed)}/s. "
                f"Scaling up to {self.current_limit} workers."
            )
            status = LogStatus.INFO
            key = "scale_up"
        else:
            msg = f"Network congested. Scaling down to {self.current_limit} workers."
            status = LogStatus.WARNING
            key = "scale_down"

        await self.ui.log(msg, status=status, throttle_key=key, throttle_sec=5.0)

    async def _step(self) -> None:
        """Один шаг адаптации."""
        now = time.monotonic()
        elapsed = min(1, now - self._last_checkpoint_time)
        if elapsed <= 0:
            return

        speed_now = self.bytes_to_check / elapsed
        self._smoothed_speed = self._calculate_ema(speed_now, elapsed)
        await self._update_window(speed_now, elapsed)

        # Логика изменения лимита
        if self._smoothed_speed > self._prev_speed * (1 + self._sensitivity):
            if self.current_limit < self.threads:
                self.current_limit += 1
                self._prev_speed = self._smoothed_speed
                await self._log_scale_event("up", speed_now)

        elif (
            self._smoothed_speed < self._prev_speed * (1 - self._sensitivity)
            and self.current_limit > 2
        ):
            self.current_limit -= 1
            self._prev_speed = self._smoothed_speed
            await self._log_scale_event("down", speed_now)

        # Применяем изменения
        if self._dynamic_limit != self.current_limit:
            await self.controller_outbox.send_data(
                ScaleUpSignal()
                if self.current_limit > self._dynamic_limit
                else ScaleDownSignal()
            )
            self._dynamic_limit = self.current_limit

        self._last_checkpoint_time = time.monotonic()
