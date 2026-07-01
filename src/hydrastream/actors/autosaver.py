# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
from dataclasses import field
from typing import assert_never

from hydrastream.domain.base_actor import BaseActor
from hydrastream.domain.entities import File
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.exceptions import LogStatus
from hydrastream.interfaces import StorageBackend
from hydrastream.messages.base import ActorFifoQueue, PoisonPill, ask
from hydrastream.messages.io import WriteChunk
from hydrastream.messages.state import GetSnapshotCmd, StateKeeperMsg
from hydrastream.messages.traffic import FlushCmd


@hydra_dataclass
class FileAutosaver(BaseActor[None]):
    disk_q: ActorFifoQueue[WriteChunk | FlushCmd | PoisonPill]
    reg_events_q: ActorFifoQueue[StateKeeperMsg]
    interval: float
    fs: StorageBackend
    _ticker_task: asyncio.Task[None] = field(init=False)

    def save_all_states(self, files: dict[int, File]) -> None:
        for file in list(files.values()):
            if file.chunks and not all(
                c.current_pos > c.end for c in (file.chunks or [])
            ):
                self.fs.save_state(file)

    async def _on_start(self) -> None:
        await self.ui.log("File autosaver worker initiated.", status=LogStatus.INFO)
        self._ticker_task = asyncio.create_task(self._run_autosave_cron())

    async def _run_autosave_cron(self) -> None:
        """The clean background ticker loop."""
        loop = asyncio.get_running_loop()

        while True:
            try:
                await asyncio.sleep(self.interval)

                # --- The Core Trigger Logic ---
                # 1. Force Disk to write everything down
                await ask(
                    inbox=self.disk_q,
                    msg_factory=FlushCmd.create_request,
                    timeout=60.0,
                    sort_key=(-1,),
                )

                # 2. Get the state keeper's current snapshot safely
                files = await ask(
                    inbox=self.reg_events_q,
                    msg_factory=GetSnapshotCmd.create_request,
                    timeout=5.0,
                    sort_key=(-1,),
                )

                # 3. Offload the heavy blocking file I/O to a thread pool
                # Note: self._flush_event.wait() is GONE because `ask(FlushCmd)`
                # already guarantees the disk is completely done!
                await loop.run_in_executor(None, self.save_all_states, files)

            except asyncio.CancelledError:
                # Loop was cleanly shut down by on_stop
                break
            except Exception as e:
                if self.is_debug:
                    raise
                await self.ui.log(
                    f"Auto-save operation failed: {e}",
                    status=LogStatus.ERROR,
                )

    async def _handle_msg(self, msg: None) -> None:

        match msg:
            case None:
                pass
            case _ as unreachable:
                await super()._handle_msg(unreachable)
                assert_never(unreachable)

    async def on_stop(self) -> None:
        # Cleanly stop the background cron task when the actor dies
        self._ticker_task.cancel()
        await asyncio.gather(self._ticker_task, return_exceptions=True)
        await self.ui.log(
            "File autosaver worker stopped safely.", status=LogStatus.INFO
        )
