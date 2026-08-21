"""Input sources: yield new stream data as it appears."""

from __future__ import annotations

import asyncio
import os
import signal
import sys
from collections.abc import AsyncIterator
from pathlib import Path

from watcher.config import SourceSpec

_POLL_SECONDS = 0.2


async def _tail_file(
    path: Path, from_start: bool, tail_chars: int, state: dict | None
) -> AsyncIterator[str]:
    """Tail a file. `state` (if given) carries {inode, position} across restarts:
    a matching inode resumes at the stored position, so content written while
    the watcher was down is still read (at-least-once semantics)."""
    handle = None
    inode: int | None = None
    first_open = True
    try:
        while True:
            if handle is None:
                try:
                    handle = path.open("r", encoding="utf-8", errors="replace")
                except FileNotFoundError:
                    await asyncio.sleep(_POLL_SECONDS)
                    continue
                stat = os.fstat(handle.fileno())
                inode = stat.st_ino
                resumed = False
                if first_open and state is not None:
                    if state.get("inode") == inode and 0 <= state.get("position", -1) <= stat.st_size:
                        handle.seek(state["position"])
                        resumed = True
                if first_open and not resumed:
                    if not from_start:
                        handle.seek(0, os.SEEK_END)
                    elif stat.st_size > tail_chars:
                        # The judge can never see more than one screening window,
                        # so existing content beyond that is not worth reading.
                        handle.seek(stat.st_size - tail_chars)
                first_open = False
                if state is not None:
                    state["inode"] = inode
                    state["position"] = handle.tell()
                    state["start_position"] = handle.tell()

            data = handle.read(65536)
            if data:
                if state is not None:
                    state["position"] = handle.tell()
                yield data
                continue

            # Detect truncation (size below our position) and rotation (new inode).
            try:
                stat = path.stat()
            except FileNotFoundError:
                handle.close()
                handle = None
                await asyncio.sleep(_POLL_SECONDS)
                continue
            if stat.st_size < handle.tell():
                handle.seek(0)
                if state is not None:
                    state["position"] = 0
                continue
            if stat.st_ino != inode:
                handle.close()
                handle = None
                if state is not None:
                    state.pop("inode", None)
                    state.pop("position", None)
                    state.pop("start_position", None)
                continue
            await asyncio.sleep(_POLL_SECONDS)
    finally:
        if handle is not None:
            handle.close()


async def _stream_command(argv: list[str]) -> AsyncIterator[str]:
    # Own process group, so stopping the watch also stops the command's
    # children (a watched dev server must not survive as an orphan).
    process = await asyncio.create_subprocess_exec(
        *argv,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        start_new_session=True,
    )
    assert process.stdout is not None
    try:
        while True:
            chunk = await process.stdout.read(65536)
            if not chunk:
                await process.wait()
                return
            yield chunk.decode("utf-8", errors="replace")
    finally:
        if process.returncode is None:
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            except (ProcessLookupError, PermissionError):
                pass
            try:
                async with asyncio.timeout(5):
                    await process.wait()
            except TimeoutError:
                # SIGTERM was trapped or ignored — do not hang shutdown forever.
                try:
                    os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                except (ProcessLookupError, PermissionError):
                    pass
                await process.wait()


async def _read_stdin() -> AsyncIterator[str]:
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
    while True:
        chunk = await reader.read(65536)
        if not chunk:
            return
        yield chunk.decode("utf-8", errors="replace")


def open_source(spec: SourceSpec, tail_chars: int, state: dict | None = None) -> AsyncIterator[str]:
    if spec.type == "file":
        assert spec.path is not None
        return _tail_file(spec.path, spec.from_start, tail_chars, state)
    if spec.type == "command":
        assert spec.command is not None
        return _stream_command(spec.command)
    return _read_stdin()
