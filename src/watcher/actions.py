"""Action execution: what happens when the model calls an exposed tool."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import time
import uuid
from pathlib import Path

from pydantic import BaseModel

from watcher.config import ActionSpec
from watcher.judge import Finding
from watcher.notifications import Alerts, notify

_COMMAND_TIMEOUT_SECONDS = 60


class ActionOutcome(BaseModel):
    action: str
    status: str  # "ok" | "error"
    detail: str = ""


def _append_finding(finding: Finding, spec: ActionSpec, watch_name: str, log_dir: Path) -> None:
    record = {
        "ts": time.time(),
        "watch": watch_name,
        "action": spec.name,
        "criterion": finding.criterion,
        "summary": finding.summary,
        "evidence": finding.evidence,
    }
    log_dir.mkdir(parents=True, exist_ok=True)
    with (log_dir / "findings.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False) + "\n")


async def execute(
    spec: ActionSpec,
    finding: Finding,
    watch_name: str,
    log_dir: Path,
    *,
    finding_id: str | None = None,
    observed_at: float | None = None,
) -> ActionOutcome:
    try:
        if spec.kind == "print":
            # One JSON line per finding on stdout — the event stream for
            # supervising processes (e.g. a Claude Code Monitor). Also recorded
            # durably, so print is a complete reporting sink on its own.
            record = {
                "watch": watch_name,
                "criterion": finding.criterion,
                "summary": finding.summary,
                "evidence": finding.evidence,
            }
            print(json.dumps(record, ensure_ascii=False), flush=True)
            _append_finding(finding, spec, watch_name, log_dir)
            return ActionOutcome(action=spec.name, status="ok")

        if spec.kind == "log":
            _append_finding(finding, spec, watch_name, log_dir)
            return ActionOutcome(action=spec.name, status="ok")

        if spec.kind == "notify":
            if spec.repeat_every_seconds is not None:
                identity = Alerts(log_dir).enqueue(
                    watch_name, spec.name, finding, spec.repeat_every_seconds,
                    alert_id=finding_id,
                )
                return ActionOutcome(action=spec.name, status="ok", detail=f"queued alert {identity}")
            await notify(finding.summary, watch_name)
            return ActionOutcome(action=spec.name, status="ok")

        if spec.kind == "spool":
            assert spec.directory is not None
            assert finding_id is not None
            spec.directory.mkdir(parents=True, exist_ok=True)
            destination = spec.directory / f"{finding_id}.json"
            if destination.exists():
                return ActionOutcome(action=spec.name, status="ok", detail="already delivered")
            record = {
                **spec.metadata,
                "finding_id": finding_id,
                "watch": watch_name,
                "criterion": finding.criterion,
                "summary": finding.summary,
                "evidence": finding.evidence,
                "observed_at": observed_at if observed_at is not None else time.time(),
            }
            temporary = spec.directory / f".{finding_id}.{uuid.uuid4().hex}.tmp"
            try:
                with temporary.open("x", encoding="utf-8") as handle:
                    handle.write(json.dumps(record, ensure_ascii=False))
                    handle.flush()
                    os.fsync(handle.fileno())
                temporary.replace(destination)
                directory_fd = os.open(spec.directory, os.O_RDONLY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            finally:
                temporary.unlink(missing_ok=True)
            return ActionOutcome(action=spec.name, status="ok")

        # kind == "command": fixed argv from config, finding as JSON on stdin.
        # Own process group, so a timeout kills forked children too.
        assert spec.command is not None
        payload = json.dumps({"watch": watch_name, **finding.model_dump()})
        process = await asyncio.create_subprocess_exec(
            *spec.command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.PIPE,
            start_new_session=True,
        )
        try:
            async with asyncio.timeout(_COMMAND_TIMEOUT_SECONDS):
                _, stderr = await process.communicate(payload.encode())
        except TimeoutError:
            for sig in (signal.SIGTERM, signal.SIGKILL):
                try:
                    os.killpg(os.getpgid(process.pid), sig)
                except (ProcessLookupError, PermissionError):
                    break
                try:
                    async with asyncio.timeout(5):
                        await process.wait()
                    break
                except TimeoutError:
                    continue
            return ActionOutcome(action=spec.name, status="error", detail="command timed out")
        if process.returncode != 0:
            return ActionOutcome(action=spec.name, status="error", detail=stderr.decode().strip())
        return ActionOutcome(action=spec.name, status="ok")
    except (OSError, RuntimeError, TimeoutError) as exc:
        return ActionOutcome(action=spec.name, status="error", detail=str(exc))
