# Copyright (c) 2026 Valentin Zhukovetski
# Licensed under the MIT License.

from __future__ import annotations

import asyncio
import contextlib
import errno
import hashlib
import operator
import os
import re
import shutil
import sys
import tempfile
import time
from dataclasses import field
from pathlib import Path
from typing import TYPE_CHECKING, override

if sys.platform == "win32":
    import msvcrt

    import win32file
elif TYPE_CHECKING:
    import msvcrt

    import win32file  # type: ignore

from hydrastream.domain.entities import File, TypeHash
from hydrastream.domain.hydra_dataclass import hydra_dataclass
from hydrastream.exceptions import (
    FileSizeMismatchError,
    HashMismatchError,
    HydraFileNotFoundError,
    InsufficientSpaceError,
    StateSaveError,
)
from hydrastream.interfaces import StorageBackend


@hydra_dataclass
class LocalStorageManager(StorageBackend):
    output_dir: Path
    debug: bool = False

    state_dir: Path = field(init=False)
    _active_fds: set[int] = field(init=False, default_factory=set[int])

    def __post_init__(self) -> None:
        self.output_dir = Path(self.output_dir).expanduser().resolve()

        self.state_dir = self.output_dir / ".states"

        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    @override
    def allocate_space(self, filename: str, size: int) -> tuple[int, str | None]:
        free_space = shutil.disk_usage(self.output_dir).free
        if free_space < size:
            raise InsufficientSpaceError(
                path=self.output_dir, required=size, free=free_space
            )

        stem = Path(filename).stem
        suffix = Path(filename).suffix
        counter = 0

        while True:
            # Формируем имя: при counter=0 берем оригинал, далее добавляем (1), (2)...
            current_name = filename if counter == 0 else f"{stem} ({counter}){suffix}"
            filepath = self.output_dir / current_name

            fd = None
            try:
                # os.O_EXCL гарантирует, что если файл уже создается другим потоком,
                # мы упадем в FileExistsError на уровне ОС, исключая race condition.
                fd = os.open(filepath, os.O_RDWR | os.O_CREAT | os.O_EXCL)

                if hasattr(os, "posix_fallocate") and size > 0:
                    os.posix_fallocate(fd, 0, size)
                else:
                    os.ftruncate(fd, size)

                # Если успешно создали файл, выходим из цикла
                if current_name != filename:
                    return fd, current_name
                return fd, None

            except FileExistsError:
                counter += 1
                continue

    @override
    def open_file(self, filename: str) -> int:
        filepath = self.output_dir / filename
        fd = os.open(filepath, os.O_RDWR)
        self._active_fds.add(fd)
        return fd

    @override
    def write_chunk_data(
        self, fd_or_conn: int, data_bytes: list[bytes], len_data: int, offset: int
    ) -> None:
        """Главный диспетчер записи."""
        if sys.platform == "win32":
            self._sync_win32_pwrite(fd_or_conn, data_bytes, offset)
        elif hasattr(os, "pwritev"):
            self._write_posix_vectored(fd_or_conn, data_bytes, offset)

            # Сброс кэша (только для POSIX)
            if hasattr(os, "posix_fadvise"):
                with contextlib.suppress(Exception):
                    os.fdatasync(fd_or_conn)
                    os.posix_fadvise(
                        fd_or_conn, offset, len_data, os.POSIX_FADV_DONTNEED
                    )
        else:
            self._write_windows_merged(fd_or_conn, data_bytes, offset)

    def _write_posix_vectored(
        self, fd: int, data_bytes: list[bytes], offset: int
    ) -> None:
        """Сложная векторная запись (Zero-Copy) для
        Linux/macOS с обходом лимита IOV_MAX и точной обработкой частичной записи.
        """
        iov_max = getattr(os, "UIO_MAXIOV", 1024)

        current_offset = offset

        # Внешний цикл: двигаемся по списку data_bytes строгими пачками
        for i in range(0, len(data_bytes), iov_max):
            batch_bytes = data_bytes[i : i + iov_max]
            views = [memoryview(b) for b in batch_bytes]

            # Считаем точную длину только текущей пачки
            batch_len = sum(len(v) for v in views)

            written = 0
            retries = 0

            while written < batch_len:
                try:
                    # Теперь мы передаем ВЕСЬ массив views текущей пачки,
                    # так как он гарантированно меньше или равен IOV_MAX
                    n = os.pwritev(fd, views, current_offset)

                    if n <= 0:
                        raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))

                    written += n
                    # Сдвигаем смещение для следующего вызова pwritev
                    current_offset += n

                    if written < batch_len:
                        views = self._rebuild_memoryviews(views, n)

                except OSError as e:
                    self._handle_io_retry(e, retries)
                    retries += 1

    def _write_windows_merged(
        self, fd: int, data_bytes: list[bytes], offset: int
    ) -> None:
        current_offset = offset

        for chunk in data_bytes:
            view = memoryview(chunk)
            chunk_len = len(view)
            written = 0
            retries = 0

            while written < chunk_len:
                try:
                    # Пишем строго по текущему смещению текущего чанка
                    n = os.pwrite(fd, view[written:], current_offset)
                    if n <= 0:
                        raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))

                    written += n
                    current_offset += n

                except OSError as e:
                    self._handle_io_retry(e, retries)
                    retries += 1

    @staticmethod
    def _rebuild_memoryviews(
        views: list[memoryview], bytes_skipped: int
    ) -> list[memoryview]:
        """Утилита для обрезки записанных кусков из массива векторов."""
        new_views: list[memoryview] = []
        skip = bytes_skipped
        for v in views:
            if skip >= len(v):
                skip -= len(v)
            else:
                new_views.append(v[skip:])
                skip = 0
        return new_views

    @staticmethod
    def _handle_io_retry(e: OSError, retries: int) -> None:
        """Обрабатывает системные прерывания (EAGAIN, EINTR)."""
        if e.errno in {errno.EAGAIN, errno.EWOULDBLOCK, errno.EINTR}:
            if retries > 5:
                raise RuntimeError(f"Too many retries on EAGAIN/EINTR: {e}") from e
            time.sleep(0.01 * (2**retries))  # Exponential backoff
        else:
            raise e

    @staticmethod
    def _sync_win32_pwrite(fd: int, data_bytes: list[bytes], offset: int) -> None:
        """Синхронная атомарная запись для Windows через Win32 API."""
        merged_data = b"".join(data_bytes)

        handle = msvcrt.get_osfhandle(fd)  # type: ignore
        overlapped = win32file.OVERLAPPED()
        overlapped.Offset = offset & 0xFFFFFFFF
        overlapped.OffsetHigh = (offset >> 32) & 0xFFFFFFFF

        win32file.WriteFile(handle, merged_data, overlapped)  # type: ignore

    @override
    def close_file(self, fd_or_conn: int) -> None:
        if fd_or_conn in self._active_fds:
            try:
                os.close(fd_or_conn)
            except OSError:
                pass
            finally:
                self._active_fds.discard(fd_or_conn)

    @override
    def force_close_all(self) -> None:
        for fd in list(self._active_fds):
            with contextlib.suppress(OSError):
                self.close_file(fd)

    @override
    def delete_file(self, filename: str) -> None:
        filepath = self.output_dir / filename
        filepath.unlink(missing_ok=True)

    @override
    def save_state(self, file_obj: File) -> None:
        filename = file_obj.actual_filename
        path = Path(self.get_state_path(filename))
        temp_dir = path.parent
        temp_dir.mkdir(parents=True, exist_ok=True)
        try:
            with tempfile.NamedTemporaryFile("wb", dir=temp_dir, delete=False) as tf:
                tf.write(file_obj.to_json())
                tf.flush()
                os.fsync(tf.fileno())
                temp_path = Path(tf.name)

            try:
                temp_path.replace(path)
                if os.name != "nt":
                    dir_fd = os.open(str(temp_dir), os.O_RDONLY)
                    try:
                        os.fsync(dir_fd)
                    finally:
                        os.close(dir_fd)
            except OSError:
                if temp_path.exists():
                    temp_path.unlink()
                raise
        except OSError as e:
            raise StateSaveError(
                filename=filename, target_path=str(path), reason=str(e)
            ) from e

    @override
    def load_state(self, filename: str) -> tuple[File | None, int]:
        p = Path(filename)
        main_name = p.stem  # "GCF_..._genomic.fna"
        last_ext = p.suffix  # ".gz"

        # Экранируем обе части
        safe_main = re.escape(main_name)
        safe_ext = re.escape(last_ext)

        # Паттерн: Название + (число) + расширение + .state.json
        pattern = re.compile(rf"^{safe_main}(?: \((\d+)\))?{safe_ext}\.state\.json$")

        found_states: list[tuple[Path, int]] = []
        for f in self.state_dir.iterdir():
            match = pattern.match(f.name)
            if match:
                # Если скобок нет (оригинал), считаем номер 0
                # Если есть, берем число из первой группы
                counter = int(match.group(1)) if match.group(1) else 0
                found_states.append((f, counter))

        if not found_states:
            return None, 0

        # Сортируем по числу (второй элемент кортежа) и берем самый большой
        state_path, _ = max(found_states, key=operator.itemgetter(1))

        try:
            with state_path.open("rb") as f:
                content = f.read()
            file = File.from_json(content) if content else None
        except Exception:
            if self.debug:
                raise
            return None, len(found_states)

        if not file:
            return None, len(found_states)

        if (self.output_dir / file.meta.original_filename).is_file():
            return file, len(found_states)
        return None, len(found_states)

    @override
    def delete_state(self, filename: str) -> None:
        self.get_state_path(filename).unlink(missing_ok=True)

    @override
    def verify_size(self, filename: str, expected_size: int) -> bool:
        file_path = self.output_dir / filename

        if not file_path.is_file():
            raise HydraFileNotFoundError(filename=filename, path=str(file_path))

        actual_size = file_path.stat().st_size

        if expected_size and actual_size != expected_size:
            raise FileSizeMismatchError(
                filename=filename, expected=expected_size, actual=actual_size
            )

        return True

    @override
    async def verify_file_hash(
        self, filename: str, expected_checksum: str, algorithm: TypeHash
    ) -> None:
        if not expected_checksum:
            return

        def _compute_hash(algorithm: TypeHash) -> str:
            filepath = self.output_dir / filename
            if not filepath.exists():
                raise HydraFileNotFoundError(filename=filename, path=str(filepath))
            with filepath.open("rb") as f:
                digest = hashlib.file_digest(f, algorithm)
                return digest.hexdigest()

        loop = asyncio.get_running_loop()
        calculated = await loop.run_in_executor(None, _compute_hash, algorithm)

        if calculated != expected_checksum:
            self.delete_state(filename)
            raise HashMismatchError(
                filename=filename,
                algorithm=algorithm,
                expected=expected_checksum,
                actual=calculated,
            )

    @override
    def get_state_path(self, filename: str) -> Path:
        return self.state_dir / f"{filename}.state.json"
