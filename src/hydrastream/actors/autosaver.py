# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio
from dataclasses import field

from hydrastream.domain.entities import File
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.exceptions import LogStatus
from hydrastream.interfaces import MonitorBackend, StorageBackend
from hydrastream.messages.base import StopMsg
from hydrastream.messages.io import WriteChunk
from hydrastream.messages.state import GetSnapshotCmd, StateKeeperCmd
from hydrastream.messages.traffic import FlushCmd


@hydra_dataclass
class FileAutosaver:
    all_complete: asyncio.Event
    flush_event: asyncio.Event
    disk_q: asyncio.Queue[WriteChunk | FlushCmd | StopMsg]
    reg_events_q: asyncio.Queue[StateKeeperCmd]
    _get_shapshot: asyncio.Queue[dict[int, File]] = field(
        default_factory=asyncio.Queue[dict[int, File]]
    )
    interval: float
    fs: StorageBackend
    ui: MonitorBackend

    is_debug: bool

    async def run(self) -> None:
        loop = asyncio.get_running_loop()

        while not self.all_complete.is_set():
            try:
                async with asyncio.timeout(self.interval):
                    await self.all_complete.wait()
                break
            except TimeoutError:
                try:
                    self.flush_event.clear()
                    await self.disk_q.put(FlushCmd())
                    await self.reg_events_q.put(
                        GetSnapshotCmd(reply_to=self._get_shapshot)
                    )
                    files = await self._get_shapshot.get()
                    await self.flush_event.wait()
                    await loop.run_in_executor(None, self.save_all_states, files)

                except Exception as e:
                    if self.is_debug:
                        raise
                    await self.ui.log(
                        f"Auto-save operation failed: {e}",
                        status=LogStatus.ERROR,
                    )

    def save_all_states(self, files: dict[int, File]) -> None:
        for file in list(files.values()):
            if file.chunks and not all(
                c.current_pos > c.end for c in (file.chunks or [])
            ):
                self.fs.save_state(file)
