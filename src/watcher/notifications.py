"""Desktop delivery and durable, human-acknowledged notification reminders."""

from __future__ import annotations

import asyncio
import json
import platform
import shlex
import shutil
import sqlite3
import sys
import time
import uuid
from contextlib import closing
from datetime import datetime
from pathlib import Path

from watcher.config import Config
from watcher.judge import Finding


async def _run(argv: list[str]) -> None:
    process = await asyncio.create_subprocess_exec(
        *argv, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with asyncio.timeout(10):
            _, stderr = await process.communicate()
    except BaseException:
        if process.returncode is None:
            process.kill()
        await process.wait()
        raise
    if process.returncode != 0:
        raise RuntimeError(stderr.decode().strip() or f"notification exit {process.returncode}")


async def notify(summary: str, watch: str, *, alert_id: str | None = None,
                 log_dir: Path | None = None, created_at: float | None = None) -> None:
    title = f"watcher: {watch}"
    ack_command = None
    if alert_id is not None:
        assert log_dir is not None and created_at is not None
        ack_command = shlex.join([
            sys.executable, "-m", "watcher.cli", "ack", alert_id,
            "--log-dir", str(log_dir.resolve()),
        ])
        # Keep relative claims explicitly anchored to the ORIGINAL alert.
        stamp = datetime.fromtimestamp(created_at).astimezone().strftime("%b %d, %H:%M %Z")
        summary = f"Original alert at {stamp}:\n{summary}"
    if platform.system() == "Darwin":
        if shutil.which("terminal-notifier"):
            argv = ["terminal-notifier", "-title", title, "-message", summary, "-sound", "Glass"]
            if ack_command is not None:
                argv += ["-subtitle", "Click to acknowledge and stop reminders",
                         "-group", alert_id, "-execute", ack_command]
        else:
            if alert_id is not None:
                summary += f"\nStop: watcher ack {alert_id}"
            script = (f"display notification {json.dumps(summary)} "
                      f"with title {json.dumps(title)} sound name \"Glass\"")
            argv = ["osascript", "-e", script]
    elif shutil.which("notify-send"):
        if alert_id is not None:
            summary += f"\nStop: watcher ack {alert_id}"
        argv = ["notify-send", title, summary]
    else:
        raise RuntimeError("no notification backend (need terminal-notifier/osascript or notify-send)")
    await _run(argv)


async def remove_notification(alert_id: str) -> None:
    if platform.system() == "Darwin" and shutil.which("terminal-notifier"):
        await _run(["terminal-notifier", "-remove", alert_id])


class Alerts:
    """SQLite serializes CLI acknowledgements with the engine's delivery receipts.

    Delivery is at least once. A crash after display can repeat an alert. An
    acknowledgement during display cannot retract already-started delivery.
    """

    def __init__(self, log_dir: Path) -> None:
        self.log_dir = log_dir.resolve()
        self.path = self.log_dir / "alerts.sqlite3"
        self._poll_at = 0.0

    def _connect(self) -> sqlite3.Connection:
        self.log_dir.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(self.path, timeout=10)
        db.row_factory = sqlite3.Row
        db.execute("""CREATE TABLE IF NOT EXISTS alerts (
            id TEXT PRIMARY KEY, watch TEXT NOT NULL, action TEXT NOT NULL,
            summary TEXT NOT NULL, evidence TEXT NOT NULL,
            created_at REAL NOT NULL, repeat_seconds REAL NOT NULL,
            next_at REAL NOT NULL, delivered_at REAL, deliveries INTEGER NOT NULL DEFAULT 0,
            acknowledged_at REAL, cancelled_at REAL, last_error TEXT
        )""")
        return db

    def enqueue(self, watch: str, action: str, finding: Finding, repeat_seconds: float,
                *, alert_id: str | None = None) -> str:
        identity = alert_id or uuid.uuid4().hex
        now = time.time()
        with closing(self._connect()) as db, db:
            db.execute("""INSERT OR IGNORE INTO alerts
                (id, watch, action, summary, evidence, created_at, repeat_seconds, next_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (identity, watch, action, finding.summary, finding.evidence, now, repeat_seconds, now))
        return identity

    def pending(self) -> list[dict]:
        if not self.path.exists():
            return []
        with closing(self._connect()) as db:
            return [dict(row) for row in db.execute("""SELECT * FROM alerts
                WHERE acknowledged_at IS NULL AND cancelled_at IS NULL ORDER BY created_at""")]

    def acknowledge(self, *, alert_id: str | None = None, watch: str | None = None) -> list[str]:
        if (alert_id is None) == (watch is None):
            raise ValueError("choose one alert ID or watch")
        if not self.path.exists():
            return []
        column, value = ("id", alert_id) if alert_id is not None else ("watch", watch)
        with closing(self._connect()) as db, db:
            rows = db.execute(f"""UPDATE alerts SET acknowledged_at = ?
                WHERE {column} = ? AND acknowledged_at IS NULL AND cancelled_at IS NULL
                RETURNING id""", (time.time(), value)).fetchall()
        return [row["id"] for row in rows]

    def reconcile(self, config: Config) -> None:
        """Turning off/removing repetition or disabling a watch cancels its alerts."""
        if not self.path.exists():
            return
        enabled = {
            (watch.name, action.name): action.repeat_every_seconds
            for watch in config.watches if watch.enabled
            for name in watch.actions
            for action in [config.action(name)]
            if action.repeat_every_seconds is not None
        }
        with closing(self._connect()) as db, db:
            for row in db.execute("""SELECT id, watch, action FROM alerts
                WHERE acknowledged_at IS NULL AND cancelled_at IS NULL""").fetchall():
                interval = enabled.get((row["watch"], row["action"]))
                if interval is None:
                    db.execute("UPDATE alerts SET cancelled_at = ? WHERE id = ?", (time.time(), row["id"]))
                else:
                    db.execute("UPDATE alerts SET repeat_seconds = ? WHERE id = ?", (interval, row["id"]))

    async def tick(self) -> None:
        if time.monotonic() < self._poll_at or not self.path.exists():
            return
        self._poll_at = time.monotonic() + 1.0
        with closing(self._connect()) as db:
            row = db.execute("""SELECT * FROM alerts WHERE acknowledged_at IS NULL
                AND cancelled_at IS NULL AND next_at <= ? ORDER BY next_at LIMIT 1""",
                (time.time(),)).fetchone()
        if row is None:
            return
        error = None
        try:
            await notify(row["summary"], row["watch"], alert_id=row["id"],
                         log_dir=self.log_dir, created_at=row["created_at"])
        except (OSError, RuntimeError, TimeoutError) as exc:
            error = str(exc) or type(exc).__name__
            print(f"[watcher] alert {row['id']}: {error}", file=sys.stderr, flush=True)
        now = time.time()
        with closing(self._connect()) as db, db:
            # Never overwrite acknowledgement/cancellation from another process.
            db.execute("""UPDATE alerts SET next_at = ?, last_error = ?,
                delivered_at = CASE WHEN ? IS NULL THEN ? ELSE delivered_at END,
                deliveries = deliveries + CASE WHEN ? IS NULL THEN 1 ELSE 0 END
                WHERE id = ? AND acknowledged_at IS NULL AND cancelled_at IS NULL""",
                (now + row["repeat_seconds"], error, error, now, error, row["id"]))
