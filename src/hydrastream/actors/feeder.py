import asyncio
from collections.abc import Iterable

from hydrastream.domain.entities import Checksum, TypeHash
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.messages.base import Envelope, StopMsg
from hydrastream.messages.io import LinkData


@hydra_dataclass
class LinkFeeder:
    links: str | Iterable[str]
    expected_checksums: dict[str, tuple[TypeHash, str] | Checksum] | None
    links_outbox: asyncio.PriorityQueue[Envelope[LinkData | StopMsg]]

    async def run(
        self,
    ) -> None:
        checksums = None
        for id, link in enumerate(self.links):
            if self.expected_checksums is not None:
                checksums = self.expected_checksums.get(link)
                if checksums and not isinstance(checksums, Checksum):
                    checksums = Checksum(algorithm=checksums[0], value=checksums[1])
            else:
                self.expected_checksums = None
            await self.links_outbox.put(
                Envelope(
                    sort_key=(id,),
                    payload=LinkData(id=id, url=link, checksum=checksums),
                )
            )
        await self.links_outbox.put(Envelope.poison_pill())
