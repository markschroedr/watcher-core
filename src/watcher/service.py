"""Background service management: install the daemon under launchd (macOS) or
systemd --user (Linux), so registry watches run permanently and survive reboots.

The unit runs the daemon through a login shell so the user's profile
(API keys, PATH) is loaded — launchd and systemd start with a bare environment.
"""

from __future__ import annotations

import os
import platform
import shlex
import shutil
import subprocess
import sys
from pathlib import Path

from watcher.config import ConfigError

_LABEL = "dev.watcher.daemon"

_PLIST = """<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key><string>{label}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{shell}</string>
        <string>-lc</string>
        <string>exec {binary} daemon --registry {registry}</string>
    </array>
    <key>RunAtLoad</key><true/>
    <key>KeepAlive</key><true/>
    <key>StandardOutPath</key><string>{log}</string>
    <key>StandardErrorPath</key><string>{log}</string>
</dict>
</plist>
"""

_SYSTEMD_UNIT = """[Unit]
Description=watcher daemon (semantic stream watching)

[Service]
ExecStart={shell} -lc 'exec {binary} daemon --registry {registry}'
Restart=on-failure
RestartSec=5

[Install]
WantedBy=default.target
"""


def _binary() -> str:
    path = shutil.which("watcher")
    if path is None:
        raise ConfigError("the 'watcher' binary is not on PATH; run `uv tool install --editable .` first")
    return path


def _login_shell() -> str:
    return os.environ.get("SHELL") or ("/bin/zsh" if platform.system() == "Darwin" else "/bin/sh")


def _run(argv: list[str], check: bool = True) -> subprocess.CompletedProcess:
    return subprocess.run(argv, check=check, capture_output=True, text=True)


class _Launchd:
    def __init__(self) -> None:
        self.plist = Path.home() / "Library" / "LaunchAgents" / f"{_LABEL}.plist"
        self.domain = f"gui/{os.getuid()}"

    def install(self, log: Path, registry: Path) -> None:
        self.plist.parent.mkdir(parents=True, exist_ok=True)
        self.plist.write_text(
            _PLIST.format(
                label=_LABEL, shell=_login_shell(), binary=_binary(),
                registry=shlex.quote(str(registry)), log=log,
            ),
            encoding="utf-8",
        )
        _run(["launchctl", "bootout", self.domain, str(self.plist)], check=False)
        _run(["launchctl", "bootstrap", self.domain, str(self.plist)])

    def uninstall(self) -> None:
        _run(["launchctl", "bootout", self.domain, str(self.plist)], check=False)
        self.plist.unlink(missing_ok=True)

    def status(self) -> str:
        if not self.plist.exists():
            return "not installed"
        result = _run(["launchctl", "print", f"{self.domain}/{_LABEL}"], check=False)
        if result.returncode != 0:
            return "installed, not loaded"
        for line in result.stdout.splitlines():
            if "state =" in line:
                return f"installed, {line.strip()}"
        return "installed, loaded"


class _Systemd:
    def __init__(self) -> None:
        self.unit = Path.home() / ".config" / "systemd" / "user" / "watcher.service"

    def install(self, log: Path, registry: Path) -> None:
        del log  # journald owns daemon output on systemd
        self.unit.parent.mkdir(parents=True, exist_ok=True)
        self.unit.write_text(
            _SYSTEMD_UNIT.format(
                shell=_login_shell(), binary=_binary(), registry=shlex.quote(str(registry))
            ),
            encoding="utf-8",
        )
        _run(["systemctl", "--user", "daemon-reload"])
        _run(["systemctl", "--user", "enable", "--now", "watcher.service"])

    def uninstall(self) -> None:
        _run(["systemctl", "--user", "disable", "--now", "watcher.service"], check=False)
        self.unit.unlink(missing_ok=True)
        _run(["systemctl", "--user", "daemon-reload"], check=False)

    def status(self) -> str:
        if not self.unit.exists():
            return "not installed"
        result = _run(["systemctl", "--user", "is-active", "watcher.service"], check=False)
        return f"installed, {result.stdout.strip() or 'unknown'}"


def _backend() -> _Launchd | _Systemd:
    system = platform.system()
    if system == "Darwin":
        return _Launchd()
    if system == "Linux":
        if shutil.which("systemctl") is None:
            raise ConfigError("no systemd found; run `watcher daemon` under your own supervisor")
        return _Systemd()
    raise ConfigError(f"unsupported platform for service install: {system}")


def install(log: Path, registry: Path) -> None:
    backend = _backend()
    log.parent.mkdir(parents=True, exist_ok=True)
    backend.install(log, registry)
    kind = "launchd" if isinstance(backend, _Launchd) else "systemd"
    print(f"service installed and started ({kind})", file=sys.stderr)


def uninstall() -> None:
    _backend().uninstall()
    print("service stopped and removed", file=sys.stderr)


def status() -> str:
    return _backend().status()
