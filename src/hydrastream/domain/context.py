# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

from __future__ import annotations

import asyncio
import math
from dataclasses import field

from hydrastream.adapters.network_curl import CurlNetworkAdapter
from hydrastream.domain.config import HydraConfig
from hydrastream.domain.entities import Chunk, File
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.interfaces import (
    MonitorBackend,
    NetworkBackend,
    StorageBackend,
)
from hydrastream.messages.base import Envelope, StopMsg
from hydrastream.messages.io import LinkData, StreamChunk, WriteChunk
from hydrastream.messages.state import StateKeeperCmd
from hydrastream.messages.traffic import (
    FileCompleted,
    FlushCmd,
    ThrottlerMsg,
    TrafficSignal,
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
    is_running: bool = True
    is_stopping: bool = False
    is_cancelled: bool = False
    is_stream: bool
    # =========================================================================
    # 1. ГЛОБАЛЬНЫЕ ЗАВИСИМОСТИ (Передаются снаружи при создании)
    # =========================================================================
    config: HydraConfig
    ui: MonitorBackend = field(init=False)
    net: NetworkBackend = field(init=False)
    fs: StorageBackend = field(init=False)
    provider: ProviderRouter = field(default_factory=ProviderRouter)

    # =========================================================================
    # 2. ОЧЕРЕДИ С ПРИОРИТЕТОМ (Priority Queues)
    # =========================================================================
    links_q: asyncio.PriorityQueue[Envelope[LinkData | StopMsg]] = field(
        default_factory=asyncio.PriorityQueue[Envelope[LinkData | StopMsg]]
    )
    files_q: asyncio.PriorityQueue[Envelope[File | StopMsg]] = field(
        default_factory=asyncio.PriorityQueue[Envelope[File | StopMsg]]
    )
    chunks_q: asyncio.PriorityQueue[Envelope[Chunk | StopMsg]] = field(
        default_factory=asyncio.PriorityQueue[Envelope[Chunk | StopMsg]]
    )
    ready_chunks_q: asyncio.PriorityQueue[Envelope[Chunk | StopMsg]] = field(
        default_factory=asyncio.PriorityQueue[Envelope[Chunk | StopMsg]]
    )
    stream_chunks_q: asyncio.PriorityQueue[Envelope[StreamChunk | StopMsg]] = field(
        default_factory=asyncio.PriorityQueue[Envelope[StreamChunk | StopMsg]]
    )

    # =========================================================================
    # 3. ОБЫЧНЫЕ ОЧЕРЕДИ (FIFO Queues)
    # =========================================================================
    # Диск и агрегация
    disk_q: asyncio.Queue[WriteChunk | FlushCmd | StopMsg] = field(
        default_factory=asyncio.Queue[WriteChunk | FlushCmd | StopMsg]
    )
    writer_q: asyncio.Queue[list[WriteChunk] | StopMsg] = field(
        default_factory=asyncio.Queue[list[WriteChunk] | StopMsg]
    )
    ack_q: asyncio.Queue[WriteCompleted | StopMsg] = field(
        default_factory=asyncio.Queue[WriteCompleted | StopMsg]
    )

    # Управление и телеметрия
    throttler_q: asyncio.Queue[ThrottlerMsg] = field(
        default_factory=asyncio.Queue[ThrottlerMsg]
    )
    controller_q: asyncio.Queue[TrafficSignal] = field(
        default_factory=asyncio.Queue[TrafficSignal]
    )
    state_q: asyncio.Queue[StateKeeperCmd] = field(
        default_factory=asyncio.Queue[StateKeeperCmd]
    )
    credit_q: asyncio.Queue[int] = field(default_factory=asyncio.Queue[int])

    # Жизненный цикл файлов
    file_limit_q: asyncio.Queue[FileCompleted] = field(
        default_factory=asyncio.Queue[FileCompleted]
    )
    file_discovery_q: asyncio.Queue[File | StopMsg] = field(
        default_factory=asyncio.Queue[File | StopMsg]
    )

    workers: int = field(init=False)
    start_works: int = field(init=False)
    resolvers: int = 20

    # =========================================================================
    # 4. СОБЫТИЯ И БАРЬЕРЫ (Синхронизация)
    # =========================================================================
    all_complete: asyncio.Event = field(default_factory=asyncio.Event)
    flush_event: asyncio.Event = field(default_factory=asyncio.Event)

    analyzer_checkpoint_event: asyncio.Event = field(default_factory=asyncio.Event)
    throttler_checkpoint_event: asyncio.Event = field(default_factory=asyncio.Event)
    stop_analyzer: asyncio.Event = field(default_factory=asyncio.Event)

    # Будут созданы в __post_init__ в зависимости от конфига
    worker_events: list[asyncio.Event] = field(init=False)
    worker_barrier: asyncio.Barrier = field(init=False)
    resolver_barrier: asyncio.Barrier = field(init=False)

    def __post_init__(self) -> None:
        if self.config.custom_monitor is None:
            base_resolver_kwargs: BaseMonitorKwargs = {
                "is_running": self.is_running,
                "is_cancelled": self.is_cancelled,
                "is_stream": self.is_stream,
                "is_verify": self.config.verify,
                "log_file": self.config.output_dir,
                "is_debug": self.config.debug,
            }

            if self.config.json_logs:
                self.ui = JsonMonitor(**base_resolver_kwargs)
            elif self.config.quiet:
                self.ui = QuietMonitor(**base_resolver_kwargs)
            elif self.config.no_ui:
                self.ui = PlainMonitor(**base_resolver_kwargs)
            else:
                self.ui = RichMonitor(
                    **base_resolver_kwargs, state_keeper_q=self.state_q
                )
        else:
            self.ui = self.config.custom_monitor

        if self.config.custom_storage is None:
            self.fs = LocalStorageManager(
                output_dir=self.config.output_dir, debug=self.config.debug
            )
        else:
            self.fs = self.config.custom_storage

        if self.config.custom_network is None:
            self.net = CurlNetworkAdapter(
                threads=self.config.threads,
                impersonate=self.config.impersonate,
                client_kwargs=self.config.client_kwargs,
            )

        if self.config.custom_providers:
            for domain, provider in self.config.custom_providers.items():
                self.provider.register(domain, provider)

        # self.resolvers = math.ceil(len(links) ** 0.4) if len(links) > 1 else 1
        # self.resolvers = min(self.resolvers, 20)

        if self.is_stream:
            self.workers = self.config.threads
        else:
            self.workers = (
                math.ceil(self.config.threads * 1.2)
                if self.config.threads > 1
                else self.config.threads
            )

        self.start_works = 5 if self.workers >= 5 else self.workers
        # Создаем массив светофоров и барьер под конкретное количество потоков!
        self.worker_events = [asyncio.Event() for _ in range(self.workers)]
        self.worker_barrier = asyncio.Barrier(self.workers)
        self.resolver_barrier = asyncio.Barrier(self.resolvers)
