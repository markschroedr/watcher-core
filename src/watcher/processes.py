"""Cleanup for commands started in a Watcher-owned process group."""

import asyncio
import os
import signal


async def stop_process_group(process: asyncio.subprocess.Process) -> None:
    # Children can outlive the session leader and keep its pipes open. The
    # leader's PID remains the group ID even after getpgid(pid) would fail.
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
    # Reap the leader and close pipe transports, including output left unread
    # when a source or action was cancelled.
    await process.communicate()
