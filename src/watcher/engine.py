"""Run loop: buffer stream data, wake per cadence, judge, dispatch actions."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import sys
import time
from collections.abc import Callable
from pathlib import Path

from watcher import actions as action_exec
from watcher.config import Config, WatchSpec, load
from watcher.judge import (
    TURNS_ADAPTER,
    Finding,
    FindingTurn,
    Judge,
    RawItemsTurn,
    StreamTurn,
    TextTurn,
    Turn,
    turn_chars,
)
from watcher.source import open_source

_TICK_SECONDS = 0.1
_RESTART_DELAY_SECONDS = 30.0
_INIT_RETRY_SECONDS = 60.0

# A buffered chunk with the source position/inode reached after reading it.
_Chunk = tuple[str, int | None, int | None]


def _log(message: str) -> None:
    print(f"[watcher] {message}", file=sys.stderr, flush=True)


class WatchRunner:
    def __init__(
        self,
        watch: WatchSpec,
        config: Config,
        judge_for: Callable[[str], Judge],
        state_path: Path | None = None,
        fingerprint: str = "",
    ) -> None:
        self.watch = watch
        self.fingerprint = fingerprint
        self.terminal_reason: str | None = None
        self._config = config
        self._judge_for = judge_for
        self._judge = judge_for(watch.profile)
        self._buffer: list[_Chunk] = []
        self._buffered_chars = 0
        self._trimmed_chars = 0
        self._turns: list[Turn] = []  # the watch conversation: stream chunks + past findings
        self._last_data_at: float | None = None
        self._last_eval_at = time.monotonic()
        self._last_api_at = time.monotonic()
        self._manual = asyncio.Event()
        self._spent_usd = 0.0
        self._consecutive_errors = 0
        self._backoff_until = 0.0
        self._profile = config.profiles[watch.profile]
        self._criteria = [config.criterion(cid) for cid in watch.criteria]
        self._actions = [config.action(name) for name in watch.actions]
        self._state_path = state_path
        self._source_state: dict | None = None
        # Position/inode of the last durably judged data. Only this is persisted;
        # the live read position may be ahead of what has been judged.
        self._committed_position: int | None = None
        self._committed_inode: int | None = None
        if state_path is not None:
            self._source_state = {}
            self._load_state()

    def _load_state(self) -> None:
        assert self._state_path is not None and self._source_state is not None
        try:
            data = json.loads(self._state_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return
        except (OSError, json.JSONDecodeError) as exc:
            self._quarantine_state(f"unreadable: {exc}")
            return
        if not isinstance(data, dict) or not isinstance(data.get("source", {}), dict):
            self._quarantine_state("not a state mapping")
            return
        source = data.get("source", {})
        if isinstance(source.get("inode"), int) and isinstance(source.get("position"), int):
            self._source_state.update(inode=source["inode"], position=source["position"])
            self._committed_inode = source["inode"]
            self._committed_position = source["position"]
        spent = data.get("spent_usd", 0.0)
        self._spent_usd = float(spent) if isinstance(spent, (int, float)) else 0.0
        try:
            self._turns = TURNS_ADAPTER.validate_python(data.get("turns", []))
        except Exception as exc:
            _log(f"watch {self.watch.name!r}: discarding unreadable conversation state: {exc}")
            self._turns = []
        _log(f"watch {self.watch.name!r}: state restored (spent ${self._spent_usd:.4f}, "
             f"{len(self._turns)} turns)")

    def _quarantine_state(self, why: str) -> None:
        assert self._state_path is not None
        _log(f"watch {self.watch.name!r}: quarantining corrupt state ({why})")
        try:
            self._state_path.replace(self._state_path.with_suffix(".corrupt"))
        except OSError:
            pass

    def _save_state(self) -> None:
        if self._state_path is None:
            return
        source: dict = {}
        live = self._source_state or {}
        if self._committed_inode is not None and self._committed_inode == live.get("inode"):
            source = {"inode": self._committed_inode, "position": self._committed_position}
        elif live.get("inode") is not None and live.get("start_position") is not None:
            # Nothing judged in this file yet: resume where reading began.
            source = {"inode": live["inode"], "position": live["start_position"]}
        data = {
            "source": source,
            "spent_usd": self._spent_usd,
            "turns": TURNS_ADAPTER.dump_python(self._turns),
        }
        self._state_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        tmp.replace(self._state_path)

    def request_manual(self) -> None:
        self._manual.set()

    async def run(self) -> None:
        consume = asyncio.create_task(self._consume())
        _log(f"watch {self.watch.name!r} running ({self.watch.cadence.preset})")
        try:
            while True:
                await asyncio.sleep(_TICK_SECONDS)
                fired = False
                if self._manual.is_set():
                    self._manual.clear()
                    fired = await self._evaluate("manual")
                else:
                    reason = self._due()
                    if reason is not None:
                        fired = await self._evaluate(reason)
                    elif self._keepalive_due():
                        await self._keepalive()
                if fired and self.watch.oneshot:
                    self.terminal_reason = "oneshot"
                    _log(f"watch {self.watch.name!r}: oneshot fired, stopping")
                    return
                budget = self.watch.max_cost_usd
                if budget is not None and self._spent_usd >= budget:
                    self.terminal_reason = "budget"
                    _log(
                        f"watch {self.watch.name!r}: budget threshold crossed "
                        f"(${self._spent_usd:.6f} of ${budget:.6f}), stopping"
                    )
                    return
                if consume.done() and not self._buffer:
                    if time.monotonic() < self._backoff_until:
                        continue  # a failed final chunk may still be requeued
                    _log(f"watch {self.watch.name!r}: stream ended")
                    return
        finally:
            consume.cancel()
            await asyncio.gather(consume, return_exceptions=True)
            self._save_state()

    async def _consume(self) -> None:
        async for data in open_source(self.watch.source, self.watch.window_max_chars, self._source_state):
            position = inode = None
            if self._source_state is not None:
                position = self._source_state.get("position")
                inode = self._source_state.get("inode")
            self._buffer.append((data, position, inode))
            self._buffered_chars += len(data)
            self._last_data_at = time.monotonic()
            # Memory cap: the evaluation only sends the newest window_max_chars
            # anyway. Trimmed data was never judged — count it and surface it.
            trimmed = 0
            while self._buffered_chars > self.watch.window_max_chars and len(self._buffer) > 1:
                old_text = self._buffer.pop(0)[0]
                self._buffered_chars -= len(old_text)
                trimmed += len(old_text)
            if self._buffered_chars > self.watch.window_max_chars:
                text, position0, inode0 = self._buffer[0]
                excess = self._buffered_chars - self.watch.window_max_chars
                self._buffer[0] = (text[excess:], position0, inode0)
                self._buffered_chars -= excess
                trimmed += excess
            if trimmed:
                if self._trimmed_chars == 0:
                    _log(f"watch {self.watch.name!r}: backlog exceeds one screening window — "
                         f"trimming oldest unjudged data (reported in the next decision)")
                self._trimmed_chars += trimmed

    def _keepalive_due(self) -> bool:
        seconds = self.watch.cadence.cache_keepalive_seconds
        return (
            seconds is not None
            and bool(self._turns)
            and time.monotonic() >= self._backoff_until
            and time.monotonic() - self._last_api_at >= seconds
        )

    async def _keepalive(self) -> None:
        """Touch the cached prefix without adding to the conversation."""
        self._last_api_at = time.monotonic()
        probe = self._turns + [
            StreamTurn(content="(cache keepalive — no new stream data; reply with the single word: noop)")
        ]
        try:
            result = await self._judge.evaluate(self.watch.name, self._criteria, self._actions, probe)
        except Exception as exc:
            _log(f"watch {self.watch.name!r}: keepalive error: {type(exc).__name__}: {exc}")
            return
        cost = self._profile.cost_usd(result.input_tokens, result.cached_tokens, result.output_tokens)
        self._spent_usd += cost
        self._write_decision(
            {
                "ts": time.time(),
                "watch": self.watch.name,
                "reason": "keepalive",
                "latency_ms": result.latency_ms,
                "input_tokens": result.input_tokens,
                "cached_tokens": result.cached_tokens,
                "output_tokens": result.output_tokens,
                "cost_usd": round(cost, 8),
            }
        )
        self._save_state()

    def _due(self) -> str | None:
        cadence = self.watch.cadence
        now = time.monotonic()
        if now < self._backoff_until or now - self._last_eval_at < cadence.min_gap_seconds:
            return None
        if cadence.preset == "realtime":
            if not self._buffer:
                return None
            if self._last_data_at is not None and now - self._last_data_at >= cadence.debounce_seconds:
                return "quiet"
            if self._buffered_chars >= cadence.max_buffer_chars:
                return "volume"
            if now - self._last_eval_at >= cadence.max_wait_seconds:
                return "max_wait"
        elif cadence.preset == "interval":
            if cadence.every_bytes is not None and self._buffered_chars >= cadence.every_bytes:
                return "volume"
            if cadence.every_seconds is not None and self._buffer and now - self._last_eval_at >= cadence.every_seconds:
                return "time"
        return None

    def _criterion_of(self, finding: Finding):
        return next((c for c in self._criteria if c.id == finding.criterion), None)

    async def _execute_finding(self, finding: Finding, allowed: list, label: str) -> dict:
        """Confirm (for effectful actions), execute, and record one finding."""
        spec = next((a for a in allowed if a.name == finding.action), None)
        if spec is None:
            self._turns.append(FindingTurn(finding=finding, outcome="error: unknown action"))
            return {"action": finding.action, "status": "error", "detail": "unknown action"}
        criterion = self._criterion_of(finding)
        if criterion is None:
            self._turns.append(FindingTurn(finding=finding, outcome="error: unknown criterion"))
            return {"action": spec.name, "status": "error", "detail": f"unknown criterion {finding.criterion!r}"}

        if spec.kind == "command":
            # Model output is a proposal, not authorization: an independent
            # skeptical judge on the confirm profile gates effectful actions.
            assert spec.confirm_profile is not None
            try:
                verdict = await self._judge_for(spec.confirm_profile).confirm(
                    self.watch.name, criterion, finding, self._turns
                )
            except Exception as exc:
                self._turns.append(FindingTurn(finding=finding, outcome="confirm error"))
                return {"action": spec.name, "status": "error", "detail": f"confirm failed: {exc}"}
            confirm_profile = self._config.profiles[spec.confirm_profile]
            self._spent_usd += confirm_profile.cost_usd(
                verdict.input_tokens, verdict.cached_tokens, verdict.output_tokens
            )
            if not verdict.confirmed:
                self._turns.append(FindingTurn(finding=finding, outcome=f"rejected: {verdict.reason}"))
                _log(f"watch {self.watch.name!r}: {label}{finding.criterion} -> {spec.name} "
                     f"REJECTED by verifier ({verdict.reason})")
                return {"action": spec.name, "status": "rejected", "detail": verdict.reason}

        outcome = await action_exec.execute(spec, finding, self.watch.name, self._config.log_dir)
        self._turns.append(FindingTurn(finding=finding, outcome=outcome.status))
        _log(f"watch {self.watch.name!r}: {label}{finding.criterion} -> {finding.action} ({outcome.status})")
        return outcome.model_dump()

    async def _escalate(self, spec, trigger_finding) -> tuple[str, dict]:
        """Re-judge the current conversation with a stronger profile; run its findings."""
        judge = self._judge_for(spec.profile)
        actions = [a for a in self._actions if a.kind != "escalate"]
        try:
            result = await judge.evaluate(self.watch.name, self._criteria, actions, self._turns)
        except Exception as exc:
            detail = f"escalation failed: {type(exc).__name__}: {exc}"
            _log(f"watch {self.watch.name!r}: {detail}")
            return detail, {"action": spec.name, "profile": spec.profile, "error": detail}
        self._spent_usd += self._config.profiles[spec.profile].cost_usd(
            result.input_tokens, result.cached_tokens, result.output_tokens
        )
        if result.reasoning_items:
            self._turns.append(RawItemsTurn(items=result.reasoning_items))
        outcomes = []
        for finding in result.findings:
            outcomes.append(await self._execute_finding(finding, actions, f"[escalated:{spec.profile}] "))
        detail = f"{spec.profile}: {len(result.findings)} findings"
        record = {
            "action": spec.name,
            "profile": spec.profile,
            "trigger": trigger_finding.summary,
            "latency_ms": result.latency_ms,
            "input_tokens": result.input_tokens,
            "cached_tokens": result.cached_tokens,
            "output_tokens": result.output_tokens,
            "findings": [f.model_dump() for f in result.findings],
            "outcomes": outcomes,
        }
        return detail, record

    async def _evaluate(self, reason: str) -> bool:
        self._last_eval_at = time.monotonic()
        self._last_api_at = time.monotonic()
        max_chars = self.watch.window_max_chars
        chunks = self._buffer[:]
        self._buffer.clear()
        self._buffered_chars = 0
        chunk = "".join(text for text, _, _ in chunks)[-max_chars:]
        chunk_position = next((p for _, p, _ in reversed(chunks) if p is not None), None)
        chunk_inode = next((i for _, _, i in reversed(chunks) if i is not None), None)
        if not chunk.strip():
            return False
        trimmed = self._trimmed_chars
        self._trimmed_chars = 0

        # Re-anchor when the screening window is full — keep a bounded suffix so
        # boundary context and recent findings survive.
        reanchored = False
        if turn_chars(self._turns) + len(chunk) > max_chars:
            keep_budget = max_chars // 4
            suffix: list[Turn] = []
            kept = 0
            for turn in reversed(self._turns):
                size = turn_chars([turn])
                if kept + size > keep_budget:
                    break
                suffix.insert(0, turn)
                kept += size
            self._turns = suffix
            reanchored = True

        self._turns.append(StreamTurn(content=chunk))
        record: dict = {
            "ts": time.time(),
            "watch": self.watch.name,
            "reason": reason,
            "chunk_chars": len(chunk),
            "window_chars": turn_chars(self._turns),
            "reanchored": reanchored,
        }
        if trimmed:
            record["trimmed_unjudged_chars"] = trimmed
        try:
            result = await self._judge.evaluate(self.watch.name, self._criteria, self._actions, self._turns)
        except asyncio.CancelledError:
            # Shutdown mid-call: leave no dangling stream turn behind — the
            # committed cursor still points before this chunk, so a restart
            # re-reads and judges it cleanly.
            self._turns.pop()
            raise
        except Exception as exc:  # judge errors must not kill the loop
            # Requeue the chunk so the cadence retries after backoff — history
            # alone schedules nothing when no new data arrives.
            self._turns.pop()
            self._buffer.insert(0, (chunk, chunk_position, chunk_inode))
            self._buffered_chars += len(chunk)
            self._trimmed_chars = trimmed
            self._consecutive_errors += 1
            backoff = min(5.0 * 2 ** (self._consecutive_errors - 1), 300.0)
            self._backoff_until = time.monotonic() + backoff
            record["error"] = f"{type(exc).__name__}: {exc}"
            record["backoff_seconds"] = backoff
            _log(f"watch {self.watch.name!r}: judge error ({record['error']}), backing off {backoff:.0f}s")
            self._write_decision(record)
            return False
        self._consecutive_errors = 0

        if result.reasoning_items:
            self._turns.append(RawItemsTurn(items=result.reasoning_items))

        outcomes = []
        escalations = []
        fired = False  # a real finding, not a mere hand-off to the escalation judge
        for finding in result.findings:
            spec = next((a for a in self._actions if a.name == finding.action), None)
            if spec is not None and spec.kind == "escalate":
                self._turns.append(FindingTurn(finding=finding, outcome="escalating"))
                detail, escalation = await self._escalate(spec, finding)
                escalations.append(escalation)
                status = "error" if "error" in escalation else "ok"
                outcomes.append({"action": spec.name, "status": status, "detail": detail})
                if escalation.get("findings"):
                    fired = True
                _log(f"watch {self.watch.name!r}: {finding.criterion} -> {spec.name} ({detail})")
                continue
            outcome = await self._execute_finding(finding, self._actions, "")
            outcomes.append(outcome)
            if outcome["status"] == "ok":
                fired = True

        if not result.findings:
            self._turns.append(TextTurn(content=result.text or "noop"))

        # Only now is the chunk durably judged: advance the committed cursor.
        if chunk_inode is not None:
            self._committed_inode = chunk_inode
            self._committed_position = chunk_position

        cost = self._profile.cost_usd(result.input_tokens, result.cached_tokens, result.output_tokens)
        self._spent_usd += cost
        record.update(
            latency_ms=result.latency_ms,
            input_tokens=result.input_tokens,
            cached_tokens=result.cached_tokens,
            output_tokens=result.output_tokens,
            service_tier=result.service_tier,
            cost_usd=round(cost, 8),
            spent_usd=round(self._spent_usd, 8),
            noop=not result.findings,
            findings=[f.model_dump() for f in result.findings],
            outcomes=outcomes,
        )
        if escalations:
            record["escalations"] = escalations
        self._write_decision(record)
        self._save_state()
        return fired

    def _write_decision(self, record: dict) -> None:
        log_dir = self._config.log_dir
        log_dir.mkdir(parents=True, exist_ok=True)
        with (log_dir / "decisions.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")


class Engine:
    def __init__(
        self,
        config: Config,
        config_path: Path | None = None,
        daemon: bool = False,
        persist: bool = False,
    ) -> None:
        self._config_path = config_path
        self._config = config
        self._daemon = daemon
        self._persist = persist
        self._config_mtime = self._current_mtime()
        self._runners: dict[str, WatchRunner] = {}
        self._tasks: dict[str, asyncio.Task] = {}
        self._judges: dict[str, Judge] = {}
        self._retry_at: dict[str, float] = {}
        self._reload_requested = False
        self._shutdown = asyncio.Event()

    def _current_mtime(self) -> float | None:
        if self._config_path is None:
            return None
        try:
            return self._config_path.stat().st_mtime
        except OSError:
            return None

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGUSR1, self._manual_all)
        loop.add_signal_handler(signal.SIGHUP, self._request_reload)
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._shutdown.set)

        await self._sync_runners()
        mode = "daemon" if self._daemon else "engine"
        _log(f"{mode} up, pid {os.getpid()} — SIGUSR1 = manual evaluate, SIGHUP = reload config")
        last_mtime_check = time.monotonic()
        while not self._shutdown.is_set():
            await asyncio.sleep(0.2)
            if self._daemon and time.monotonic() - last_mtime_check >= 2.0:
                last_mtime_check = time.monotonic()
                mtime = self._current_mtime()
                if mtime != self._config_mtime:
                    self._config_mtime = mtime
                    _log("registry changed on disk")
                    self._reload_requested = True
            if self._reload_requested:
                self._reload_requested = False
                await self._reload()
            for name, task in list(self._tasks.items()):
                if not task.done():
                    continue
                runner = self._runners.pop(name, None)
                self._tasks.pop(name)
                exc = task.exception()
                terminal = runner.terminal_reason if runner else None
                if exc is not None:
                    _log(f"watch {name!r} crashed: {exc!r}")
                if self._daemon and terminal is None:
                    _log(f"watch {name!r}: not a terminal stop — restarting in {_RESTART_DELAY_SECONDS:.0f}s")
                    self._retry_at[name] = time.monotonic() + _RESTART_DELAY_SECONDS
            if self._daemon and self._retry_at:
                now = time.monotonic()
                wanted = {w.name: w for w in self._config.watches if w.enabled}
                for name, when in list(self._retry_at.items()):
                    if name not in wanted:
                        del self._retry_at[name]
                    elif now >= when and name not in self._tasks:
                        del self._retry_at[name]
                        self._start_runner(wanted[name])
            if not self._tasks and not self._retry_at and not self._daemon:
                _log("all watches finished")
                return
        _log("shutting down")
        for task in self._tasks.values():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)

    def _judge_for(self, profile_name: str) -> Judge:
        if profile_name not in self._judges:
            self._judges[profile_name] = Judge(self._config.profiles[profile_name])
        return self._judges[profile_name]

    def _fingerprint(self, watch: WatchSpec) -> str:
        """Resolved effective configuration of one watch — a change in any
        referenced object must restart the runner, not only the WatchSpec."""
        actions = [self._config.action(name) for name in watch.actions]
        profile_names = sorted(
            {watch.profile}
            | {a.profile for a in actions if a.profile is not None}
            | {a.confirm_profile for a in actions if a.confirm_profile is not None}
        )
        parts = [
            watch.model_dump_json(),
            *(self._config.criterion(cid).model_dump_json() for cid in watch.criteria),
            *(a.model_dump_json() for a in actions),
            *(self._config.profiles[p].model_dump_json() for p in profile_names),
            str(self._config.log_dir),
        ]
        return "|".join(parts)

    async def _sync_runners(self) -> None:
        wanted = {w.name: w for w in self._config.watches if w.enabled}
        for name in list(self._tasks):
            new_fingerprint = self._fingerprint(wanted[name]) if name in wanted else None
            if new_fingerprint != self._runners[name].fingerprint:
                task = self._tasks.pop(name)
                task.cancel()
                # Await the old runner fully (its finally flushes state) before a
                # replacement may load that state.
                await asyncio.gather(task, return_exceptions=True)
                self._runners.pop(name, None)
                _log(f"watch {name!r} stopped")
        for name in list(self._retry_at):
            if name not in wanted:
                del self._retry_at[name]
        for name, watch in wanted.items():
            if name not in self._tasks and name not in self._retry_at:
                self._start_runner(watch)

    def _start_runner(self, watch: WatchSpec) -> None:
        try:
            state_path = (
                self._config.log_dir / "state" / f"{watch.name}.json" if self._persist else None
            )
            runner = WatchRunner(
                watch, self._config, self._judge_for, state_path, self._fingerprint(watch)
            )
        except Exception as exc:
            # One broken watch (missing key, bad profile) must not stop the rest.
            _log(f"watch {watch.name!r} failed to start ({exc}); retrying in {_INIT_RETRY_SECONDS:.0f}s")
            self._retry_at[watch.name] = time.monotonic() + _INIT_RETRY_SECONDS
            return
        self._runners[watch.name] = runner
        self._tasks[watch.name] = asyncio.create_task(runner.run())

    def _manual_all(self) -> None:
        _log("manual evaluation requested")
        for runner in self._runners.values():
            runner.request_manual()

    def _request_reload(self) -> None:
        self._reload_requested = True

    async def _reload(self) -> None:
        if self._config_path is None:
            _log("ad-hoc config has no file to reload")
            return
        try:
            self._config = load(self._config_path)
        except Exception as exc:
            _log(f"reload failed, keeping old config: {exc}")
            return
        self._judges.clear()
        await self._sync_runners()
        _log("config reloaded")
