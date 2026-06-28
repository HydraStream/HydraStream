import asyncio
import math
import time

from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.exceptions import LogStatus
from hydrastream.interfaces import MonitorBackend
from hydrastream.messages.base import ActorFifoQueue
from hydrastream.messages.traffic import ScaleDownSignal, ScaleUpSignal, TrafficSignal
from hydrastream.utils import format_size


@hydra_dataclass
class TelemetryAnalyzer:
    threads: int
    _current_limit: int
    analyzer_checkpoint_event: asyncio.Event
    controller_outbox: ActorFifoQueue[TrafficSignal]
    _smoothed_speed: float = 0.0
    _prev_speed: float = 0.0
    _tau: float = 1.0
    _min_window: int = 1024
    _sensitivity: float = 0.05
    is_debug: bool

    ui: MonitorBackend

    stop_analyzer: asyncio.Event

    async def run(self) -> None:
        while not self.stop_analyzer.is_set():
            try:
                await self.analyzer_checkpoint_event.wait()

                self.analyzer_checkpoint_event.clear()

                # Просто делаем шаг
                await self._step()

            except Exception as e:
                if self.is_debug:
                    raise
                await self.ui.log(
                    f"Adaptive controller failed: {e}",
                    status=LogStatus.ERROR,
                )

    def _calculate_ema(self, speed_now: float, elapsed: float) -> float:
        if self._smoothed_speed == 0.0:
            return speed_now
        alpha = 1.0 - math.exp(-elapsed / self._tau)
        return (alpha * speed_now) + ((1.0 - alpha) * self._smoothed_speed)

    def _update_window(self, speed_now: float, elapsed: float) -> None:
        safe_speed = max(speed_now, 0.001)
        coef = 1 / safe_speed**0.25
        new_bytes = int(self.bytes_to_check * (1 - coef + elapsed))
        self.bytes_to_check = max(self._min_window, new_bytes)

    async def _log_scale_event(self, direction: str, speed: float) -> None:
        """Вспомогательный метод для чистого логирования."""
        if direction == "up":
            msg = (
                f"Speed increased to {format_size(speed)}/s. "
                f"Scaling up to {self._current_limit} workers."
            )
            status = LogStatus.INFO
            key = "scale_up"
        else:
            msg = f"Network congested. Scaling down to {self._current_limit} workers."
            status = LogStatus.WARNING
            key = "scale_down"

        await self.ui.log(msg, status=status, throttle_key=key, throttle_sec=5.0)

    async def _step(self) -> None:
        """Один шаг адаптации."""
        now = time.monotonic()
        elapsed = min(1, now - self.last_checkpoint_time)
        if elapsed <= 0:
            return

        speed_now = self.bytes_to_check / elapsed
        self._smoothed_speed = self._calculate_ema(speed_now, elapsed)
        self._update_window(speed_now, elapsed)

        # Логика изменения лимита
        if self._smoothed_speed > self._prev_speed * (1 + self._sensitivity):
            if self._current_limit < self.threads:
                self._current_limit += 1
                self._prev_speed = self._smoothed_speed
                await self._log_scale_event("up", speed_now)

        elif (
            self._smoothed_speed < self._prev_speed * (1 - self._sensitivity)
            and self._current_limit > 2
        ):
            self._current_limit -= 1
            self._prev_speed = self._smoothed_speed
            await self._log_scale_event("down", speed_now)

        # Применяем изменения
        if self.dynamic_limit != self._current_limit:
            await self.controller_outbox.send_data(
                ScaleUpSignal()
                if self._current_limit > self.dynamic_limit
                else ScaleDownSignal()
            )
            self.dynamic_limit = self._current_limit

        self.last_checkpoint_time = time.monotonic()
