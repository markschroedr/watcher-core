"""Input sources: yield new stream data as it appears."""

from __future__ import annotations

import asyncio
import codecs
import os
import signal
import sys
from collections.abc import AsyncIterator
from pathlib import Path

from watcher.config import SourceSpec

_POLL_SECONDS = 0.2


async def _tail_file(
    path: Path,
    from_start: bool,
    tail_chars: int,
    state: dict | None,
    start_inode: int | None,
    start_position: int | None,
) -> AsyncIterator[str]:
    """Tail a file. `state` (if given) carries {inode, position} across restarts:
    a matching inode resumes at the stored position, so content written while
    the watcher was down is still read (at-least-once semantics)."""
    handle = None
    inode: int | None = None
    first_open = True
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    try:
        while True:
            if handle is None:
                try:
                    handle = path.open("rb")
                    decoder.reset()
                except FileNotFoundError:
                    await asyncio.sleep(_POLL_SECONDS)
                    continue
                stat = os.fstat(handle.fileno())
                inode = stat.st_ino
                resume_positions: list[int] = []
                if first_open and state is not None:
                    position = state.get("position")
                    if state.get("inode") == inode and isinstance(position, int) and 0 <= position <= stat.st_size:
                        resume_positions.append(position)
                if first_open and start_inode == inode and start_position is not None and start_position <= stat.st_size:
                    resume_positions.append(start_position)
                resumed = bool(resume_positions)
                if resumed:
                    # The explicit cursor is a lower boundary. Persisted state
                    # can continue later, but it must never move before it.
                    handle.seek(max(resume_positions))
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
                text = decoder.decode(data)
                if state is not None:
                    state["position"] = handle.tell() - len(decoder.getstate()[0])
                if text:
                    yield text
                continue

            # Detect truncation (size below our position) and rotation (new inode).
            try:
                stat = path.stat()
            except FileNotFoundError:
                handle.close()
                handle = None
                await asyncio.sleep(_POLL_SECONDS)
                continue
            if stat.st_ino != inode:
                handle.close()
                handle = None
                if state is not None:
                    state.pop("inode", None)
                    state.pop("position", None)
                    state.pop("start_position", None)
                continue
            if stat.st_size < handle.tell():
                handle.seek(0)
                decoder.reset()
                if state is not None:
                    state.update(position=0, start_position=0)
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
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    try:
        while True:
            chunk = await process.stdout.read(65536)
            if not chunk:
                tail = decoder.decode(b"", final=True)
                if tail:
                    yield tail
                code = await process.wait()
                if code:
                    raise RuntimeError(f"source command exited with status {code}")
                return
            text = decoder.decode(chunk)
            if text:
                yield text
    finally:
        # The session leader may already have exited while children still own
        # the output pipe. Its PID remains the process-group ID.
        try:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                async with asyncio.timeout(5):
                    while True:
                        await asyncio.sleep(0.05)
                        os.killpg(process.pid, 0)
            except TimeoutError:
                os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        await process.wait()


async def _read_stdin() -> AsyncIterator[str]:
    loop = asyncio.get_running_loop()
    reader = asyncio.StreamReader()
    transport, _ = await loop.connect_read_pipe(lambda: asyncio.StreamReaderProtocol(reader), sys.stdin)
    decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
    try:
        while True:
            chunk = await reader.read(65536)
            text = decoder.decode(chunk, final=not chunk)
            if text:
                yield text
            if not chunk:
                return
    finally:
        transport.close()


def open_source(spec: SourceSpec, tail_chars: int, state: dict | None = None) -> AsyncIterator[str]:
    if spec.type == "file":
        assert spec.path is not None
        return _tail_file(
            spec.path,
            spec.from_start,
            tail_chars,
            state,
            spec.start_inode,
            spec.start_position,
        )
    if spec.type == "command":
        assert spec.command is not None
        return _stream_command(spec.command)
    return _read_stdin()
