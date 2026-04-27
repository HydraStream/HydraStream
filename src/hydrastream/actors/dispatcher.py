# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import asyncio

from hydrastream.engine import send_poison_pills
from hydrastream.models import Chunk, Envelope, File, HydraContext, my_dataclass


@my_dataclass
class FileDispatcher:
    limit: int
    current_files: int = 0
    files_q: asyncio.PriorityQueue[Envelope[File | None]]
    file_limit_q: asyncio.Queue[object]
    chunks_q: asyncio.PriorityQueue[Envelope[Chunk | None]]
    num_memory_throtller: int

    async def chunk_dispatcher(self, ctx: HydraContext) -> None:
        pending_file: Envelope[File | None] | None = None

        while True:
            if pending_file is None:
                pending_file = await self.files_q.get()

                if pending_file.is_poison_pill:
                    await send_poison_pills(self.chunks_q, self.num_memory_throtller)
                    break

                if not (file := pending_file.payload):
                    continue

                if ctx.stream:
                    await ctx.queues.file_discovery.put(file.meta.id)

                if self.current_files >= self.limit:
                    await self.file_limit_q.get()
                    self.current_files -= 1
                    continue

                self.current_files += 1

                file.create_chunks()
                for c in file.chunks:
                    if c.current_pos <= c.end:
                        await self.chunks_q.put(
                            Envelope(
                                sort_key=(c.current_pos, file.meta.id),
                                payload=c,
                            )
                        )
                pending_file = None
                file = None

            new_filename = self.fs.allocate_space(filename=filename, size=total_size)
            if new_filename:
                await log(
                    self.ui,
                    f"{filename} already exists. Saving as {new_filename}.",
                    status=LogStatus.WARNING,
                )
                filename = new_filename

                if not self.is_stream:
                    file_obj.fd = self.fs.open_file(filename=file_obj.meta.filename)
