# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import sys

from hydrastream.models import Chunk, HydraContext


async def chunk_dispatcher(ctx: HydraContext) -> None:
    while True:
        priority_index, file = await ctx.queues.dispatch_file.get()
        if priority_index == sys.maxsize or file is None:
            break
        if ctx.stream:
            await ctx.queues.file_discovery.put(priority_index)
        file.create_chunks()
        for c in file.chunks:
            if c.current_pos <= c.end:
                if ctx.stream:
                    await ctx.queues.chunk.put((priority_index, c))
                else:
                    await ctx.queues.chunk.put((c, priority_index))  # type: ignore
        ctx.current_files_id.add(priority_index)

        async with ctx.sync.current_files:
            if ctx.stream:
                await ctx.sync.current_files.wait_for(lambda: not ctx.current_files_id)
            else:
                await ctx.sync.current_files.wait_for(
                    lambda: len(ctx.current_files_id) < ctx.config.threads
                )

    c = Chunk(current_pos=sys.maxsize, start=sys.maxsize, end=sys.maxsize)
    if ctx.stream:
        for i in range(ctx.tasks.workers - 1, -1, -1):
            await ctx.queues.chunk.put((sys.maxsize - i, c))  # type: ignore
    else:
        for i in range(ctx.tasks.workers - 1, -1, -1):
            await ctx.queues.chunk.put((c, sys.maxsize - i))  # type: ignore
