# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

from typing import assert_never, override

from hydrastream.domain.base_actor import BaseActor
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.messages.base import ActorFifoQueue, PoisonPill
from hydrastream.messages.traffic import (
    GoToSleepPill,
    NetworkCongestionSignal,
    ScaleDownSignal,
    ScaleUpSignal,
    TooManyRequests,
    TrafficSignal,
    WakeUpPill,
)


@hydra_dataclass
class TrafficController(BaseActor[TrafficSignal]):
    sleep_signals_outdox: ActorFifoQueue[GoToSleepPill | PoisonPill]
    wait_in_sleep_outbox: ActorFifoQueue[WakeUpPill | PoisonPill]
    workers: int
    dynamic_limit: int
    prev_dynamic_limit: int

    @override
    async def _on_start(self) -> None:
        for _ in range(self.workers - self.dynamic_limit):
            await self.sleep_signals_outdox.send_data(GoToSleepPill())

    @override
    async def _handle_msg(self, msg: TrafficSignal) -> None:
        match msg:
            case NetworkCongestionSignal() | ScaleDownSignal():
                self.dynamic_limit = max(1, self.dynamic_limit - 1)

            case ScaleUpSignal():
                self.dynamic_limit = min(self.workers, self.dynamic_limit + 1)

            case TooManyRequests():
                self.dynamic_limit = max(1, int(self.dynamic_limit / 2))

            case _ as unreachable:
                await super()._handle_msg(unreachable)
                assert_never(unreachable)

        if self.dynamic_limit != self.prev_dynamic_limit:
            await self._update_lights()

    async def _update_lights(self) -> None:

        if self.dynamic_limit > self.prev_dynamic_limit:
            for _ in range(self.dynamic_limit - self.prev_dynamic_limit):
                await self.wait_in_sleep_outbox.send_data(WakeUpPill())
        else:
            for _ in range(self.prev_dynamic_limit - self.dynamic_limit):
                await self.sleep_signals_outdox.send_data(GoToSleepPill())

    @override
    async def _on_terminal_pill(self) -> None:
        self.prev_dynamic_limit = self.dynamic_limit
        self.dynamic_limit = self.workers
        await self._update_lights()
