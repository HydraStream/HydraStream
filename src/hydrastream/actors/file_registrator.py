import asyncio
from dataclasses import field

from hydrastream.models import File, my_dataclass


@my_dataclass(frozen=True)
class RegisterFileCmd:
    file_id: int
    file_obj: File


@my_dataclass(frozen=True)
class RemoveFileCmd:
    file_id: int


@my_dataclass(frozen=True)
class GetSnapshotCmd:
    reply_to: asyncio.Queue[dict[int, File]]


@my_dataclass
class FileRegistryActor:
    inbox: asyncio.Queue[object]
    autosaver_outbox: asyncio.Queue[object]
    _files: dict[int, File] = field(default_factory=dict[int, File])

    async def run(self) -> None:
        while True:
            cmd = await self.inbox.get()

            if cmd is None:  # Ядовитая пилюля
                break

            if isinstance(cmd, RegisterFileCmd):
                self._files[cmd.file_id] = cmd.file_obj

            elif isinstance(cmd, RemoveFileCmd):
                del self._files[cmd.file_id]

            elif isinstance(cmd, GetSnapshotCmd):
                snapshot = self._files.copy()
                await cmd.reply_to.put(snapshot)
