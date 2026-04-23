# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

import sys

from hydrastream.models import Envelope, HydraContext


async def chunk_dispatcher(ctx: HydraContext) -> None:  # noqa: C901
    if ctx.stream:

        def make_key(f_id: int, pos: int) -> tuple[int, int]:
            return (f_id, pos)

        def can_continue() -> bool:
            return not ctx.current_files_id

    else:

        def make_key(f_id: int, pos: int) -> tuple[int, int]:
            return (pos, f_id)

        limit = ctx.config.threads  # кэшируем лимит для скорости

        def can_continue() -> bool:
            return len(ctx.current_files_id) < limit

    while True:
        envelope = await ctx.queues.dispatch_file.get()

        if envelope.is_poison_pill:
            break

        if not (file := envelope.payload):
            continue

        if ctx.stream:
            await ctx.queues.file_discovery.put(file.meta.id)

        file.create_chunks()
        for c in file.chunks:
            if c.current_pos <= c.end:
                await ctx.queues.chunk.put(
                    Envelope(
                        sort_key=make_key(file.meta.id, c.current_pos),
                        payload=c,
                    )
                )

        ctx.current_files_id.add(file.meta.id)

        async with ctx.sync.current_files:
            await ctx.sync.current_files.wait_for(can_continue)

    for i in range(ctx.tasks.workers - 1, 0, -1):
        await ctx.queues.chunk.put(
            Envelope(sort_key=(sys.maxsize - i,), is_poison_pill=True)
        )
    await ctx.queues.chunk.put(
        Envelope(
            sort_key=(sys.maxsize,),
            is_poison_pill=True,
            is_last_survivor=True,
        )
    )
