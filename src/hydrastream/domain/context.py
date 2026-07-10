# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

from __future__ import annotations

import asyncio
import math
from dataclasses import field
from typing import TypedDict

from hydrastream.actors.analyzer import CheckpointEvent
from hydrastream.adapters.network_curl import CurlNetworkAdapter
from hydrastream.domain.config import HydraConfig, UIConfig
from hydrastream.domain.entities import Chunk, File
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.interfaces import (
    MonitorBackend,
    NetworkBackend,
    StorageBackend,
)
from hydrastream.messages.base import (
    ActorFifoQueue,
    ActorPriorityQueue,
    PoisonPill,
)
from hydrastream.messages.io import LinkData, WriteChunk
from hydrastream.messages.state import StateKeeperMsg
from hydrastream.messages.traffic import (
    DiskMsg,
    FileCompleted,
    GoToSleepPill,
    ThrottlerMsg,
    TrafficSignal,
    WakeUpPill,
    WriteCompleted,
)
from hydrastream.monitor import (
    BaseMonitorKwargs,
    JsonMonitor,
    PlainMonitor,
    QuietMonitor,
    RichMonitor,
)
from hydrastream.providers import ProviderRouter
from hydrastream.storage import LocalStorageManager


@hydra_dataclass
class HydraContext:
    # =========================================================================
    # 1. ГЛОБАЛЬНЫЕ ЗАВИСИМОСТИ (Передаются снаружи при создании)
    # =========================================================================
    config: HydraConfig
    ui: MonitorBackend
    net: NetworkBackend
    fs: StorageBackend
    provider: ProviderRouter = field(default_factory=ProviderRouter)

    # =========================================================================
    # 2. ОЧЕРЕДИ С ПРИОРИТЕТОМ (Priority Queues)
    # =========================================================================
    links_q: ActorPriorityQueue[LinkData | PoisonPill]
    files_q: ActorPriorityQueue[File | PoisonPill]
    chunks_q: ActorPriorityQueue[Chunk | PoisonPill]
    ready_chunks_q: ActorPriorityQueue[Chunk | PoisonPill]
    aggregator_q: ActorPriorityQueue[DiskMsg | PoisonPill]
    # =========================================================================
    # 3. ОБЫЧНЫЕ ОЧЕРЕДИ (FIFO Queues)
    # =========================================================================
    # Диск
    writer_q: ActorFifoQueue[list[WriteChunk] | PoisonPill]
    ack_q: ActorFifoQueue[WriteCompleted | PoisonPill]
    # Управление и телеметрия
    analyzer_q: ActorFifoQueue[CheckpointEvent | PoisonPill]
    throttler_q: ActorFifoQueue[ThrottlerMsg | PoisonPill]
    controller_q: ActorFifoQueue[TrafficSignal | PoisonPill]
    state_q: ActorFifoQueue[StateKeeperMsg | PoisonPill]
    credit_q: ActorFifoQueue[int | PoisonPill]
    sleep_signals: ActorFifoQueue[GoToSleepPill | PoisonPill]
    wait_in_sleep: ActorFifoQueue[WakeUpPill | PoisonPill]
    # Жизненный цикл файлов
    file_limit_q: ActorFifoQueue[FileCompleted | PoisonPill]
    autosaver_q: ActorFifoQueue[PoisonPill | None]

    session_killer: asyncio.Event = field(default_factory=asyncio.Event)

    workers: int = field(init=False)
    start_works: int = field(init=False)
    resolvers: int = 1

    def __post_init__(self) -> None:

        if self.config.custom_providers:
            for domain, provider in self.config.custom_providers.items():
                self.provider.register(domain, provider)

        # self.resolvers = math.ceil(len(links) ** 0.4) if len(links) > 1 else 1
        # self.resolvers = min(self.resolvers, 20)

        if self.config.is_stream:
            self.workers = self.config.threads
        else:
            self.workers = (
                math.ceil(self.config.threads * 1.2)
                if self.config.threads > 1
                else self.config.threads
            )

        self.start_works = 5 if self.workers >= 5 else self.workers


def build_context(config: HydraConfig, ui: MonitorBackend) -> HydraContext:
    channels = _create_channels()
    ui.bind_to_state_keeper(channels["state_q"])
    net = _create_network(config)
    fs = _create_storage(config)

    return HydraContext(config=config, net=net, fs=fs, ui=ui, **channels)


class AppQueuesSchema(TypedDict):
    links_q: ActorPriorityQueue[LinkData | PoisonPill]
    files_q: ActorPriorityQueue[File | PoisonPill]
    chunks_q: ActorPriorityQueue[Chunk | PoisonPill]
    ready_chunks_q: ActorPriorityQueue[Chunk | PoisonPill]
    aggregator_q: ActorPriorityQueue[DiskMsg | PoisonPill]

    writer_q: ActorFifoQueue[list[WriteChunk] | PoisonPill]
    ack_q: ActorFifoQueue[WriteCompleted | PoisonPill]
    analyzer_q: ActorFifoQueue[CheckpointEvent | PoisonPill]
    throttler_q: ActorFifoQueue[ThrottlerMsg | PoisonPill]
    controller_q: ActorFifoQueue[TrafficSignal | PoisonPill]
    state_q: ActorFifoQueue[StateKeeperMsg | PoisonPill]
    credit_q: ActorFifoQueue[int | PoisonPill]
    sleep_signals: ActorFifoQueue[GoToSleepPill | PoisonPill]
    wait_in_sleep: ActorFifoQueue[WakeUpPill | PoisonPill]
    file_limit_q: ActorFifoQueue[FileCompleted | PoisonPill]
    autosaver_q: ActorFifoQueue[PoisonPill | None]


def _create_channels() -> AppQueuesSchema:

    channels = {}

    actor_p_queues = {
        "links_q": 0,
        "files_q": 0,
        "chunks_q": 0,
        "ready_chunks_q": 0,
    }

    for k, v in actor_p_queues.items():
        channels[k] = ActorPriorityQueue(maxsize=v)

    actor_queues = {
        "aggregator_q": 10,
        "writer_q": 10,
        "ack_q": 0,
        "throttler_q": 0,
        "analyzer_q": 0,
        "controller_q": 0,
        "state_q": 0,
        "file_limit_q": 0,
        "credit_q": 0,
        "sleep_signals": 0,
        "wait_in_sleep": 0,
        "autosaver_q": 0,
    }
    for k, v in actor_queues.items():
        channels[k] = ActorFifoQueue(maxsize=v)

    return channels  # type: ignore


def _create_network(config: HydraConfig) -> NetworkBackend:

    if config.custom_network is None:
        return CurlNetworkAdapter(
            threads=config.threads,
            impersonate=config.impersonate,
            client_kwargs=config.client_kwargs,
        )
    return config.custom_network


def _create_storage(config: HydraConfig) -> StorageBackend:
    if config.custom_storage is None:
        return LocalStorageManager(output_dir=config.output_dir, debug=config.debug)
    return config.custom_storage


def create_monitor(config: UIConfig) -> MonitorBackend:
    base_resolver_kwargs: BaseMonitorKwargs = {
        "is_verify": config.is_verify,
        "log_file": config.log_file_dir,
        "is_debug": config.is_debug,
    }

    if config.json_logs:
        ui = JsonMonitor(**base_resolver_kwargs)
    elif config.quiet:
        ui = QuietMonitor(**base_resolver_kwargs)
    elif config.no_ui:
        ui = PlainMonitor(**base_resolver_kwargs)
    else:
        ui = RichMonitor(**base_resolver_kwargs)
    return ui
