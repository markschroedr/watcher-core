"""Action execution: what happens when the model calls an exposed tool."""

from __future__ import annotations

import asyncio
import json
import os
import platform
import shutil
import signal
import time
from pathlib import Path

from pydantic import BaseModel

from watcher.config import ActionSpec
from watcher.judge import Finding

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


async def execute(spec: ActionSpec, finding: Finding, watch_name: str, log_dir: Path) -> ActionOutcome:
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
            # terminal-notifier has its own app identity in Notification settings
            # (banner-capable); osascript notifications hide under "Script Editor",
            # which is often set to quiet delivery. On Linux, notify-send.
            if platform.system() == "Darwin":
                if shutil.which("terminal-notifier"):
                    argv = [
                        "terminal-notifier",
                        "-title", f"watcher: {watch_name}",
                        "-message", finding.summary,
                        "-sound", "Glass",
                    ]
                else:
                    script = (
                        f'display notification {json.dumps(finding.summary)} '
                        f'with title {json.dumps(f"watcher: {watch_name}")} sound name "Glass"'
                    )
                    argv = ["osascript", "-e", script]
            elif shutil.which("notify-send"):
                argv = ["notify-send", f"watcher: {watch_name}", finding.summary]
            else:
                return ActionOutcome(
                    action=spec.name, status="error",
                    detail="no notification backend (need terminal-notifier/osascript or notify-send)",
                )
            process = await asyncio.create_subprocess_exec(
                *argv,
                stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
            )
            _, stderr = await process.communicate()
            if process.returncode != 0:
                return ActionOutcome(action=spec.name, status="error", detail=stderr.decode().strip())
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
    except OSError as exc:
        return ActionOutcome(action=spec.name, status="error", detail=str(exc))
