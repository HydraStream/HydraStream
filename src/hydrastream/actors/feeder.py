from dataclasses import dataclass, field
from typing import assert_never

from hydrastream.domain.base_actor import BaseActor
from hydrastream.domain.entities import Checksum, TypeHash
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.messages.base import (
    ActorPriorityQueue,
    PoisonPill,
)
from hydrastream.messages.io import LinkData


@dataclass(frozen=True)
class RawLinkItem:
    """The clean singular item sent by any external producer or network listener."""

    url: str
    checksum: Checksum | tuple[TypeHash, str] | None = None


@hydra_dataclass
class LinkFeederDaemon(BaseActor[RawLinkItem]):
    links_outbox: ActorPriorityQueue[LinkData | PoisonPill]

    _link_counter: int = field(default=0, init=False)

    async def _handle_msg(self, msg: RawLinkItem) -> None:
        """Reacts to a single raw link item, stamps it, and forwards it."""
        match msg:
            case RawLinkItem(url=url, checksum=checksum):
                if isinstance(checksum, tuple):
                    checksum = Checksum(algorithm=checksum[0], value=checksum[1])

                stamped_data = LinkData(
                    id=self._link_counter, url=url, checksum=checksum
                )

                await self.links_outbox.send_data(
                    data=stamped_data,
                    sort_key=(self._link_counter,),
                )

                self._link_counter += 1

            case _ as unreachable:
                await super().handle_msg(unreachable)
                assert_never(unreachable)
