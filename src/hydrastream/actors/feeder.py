import sys
from collections.abc import Iterable

from hydrastream.models import Checksum, Envelope, HydraContext, LinkData, TypeHash


async def link_feeder(
    ctx: HydraContext,
    links: str | Iterable[str],
    expected_checksums: dict[str, tuple[TypeHash, str] | Checksum] | None,
) -> None:
    checksums = None
    for id, link in enumerate(links):
        if expected_checksums is not None:
            checksums = expected_checksums.get(link)
            if checksums and not isinstance(checksums, Checksum):
                checksums = Checksum(algorithm=checksums[0], value=checksums[1])
        else:
            expected_checksums = None
        await ctx.queues.links.put(
            Envelope(
                sort_key=(id,), payload=LinkData(id=id, url=link, checksum=checksums)
            )
        )

    for i in range(ctx.tasks.resolvers - 1, 0, -1):
        await ctx.queues.links.put(
            Envelope(sort_key=(sys.maxsize - i,), is_poison_pill=True)
        )
    await ctx.queues.links.put(
        Envelope(sort_key=(sys.maxsize,), is_poison_pill=True, is_last_survivor=True)
    )
