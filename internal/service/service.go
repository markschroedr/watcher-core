package service

import (
	"context"
	"encoding/xml"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/markschroedr/watcher-core/internal/config"
)

const label = "dev.watcher.daemon"

func quote(s string) string  { return "'" + strings.ReplaceAll(s, "'", "'\\''") + "'" }
func escape(s string) string { var b strings.Builder; xml.EscapeText(&b, []byte(s)); return b.String() }
func run(ctx context.Context, argv ...string) (string, error) {
	b, e := exec.CommandContext(ctx, argv[0], argv[1:]...).CombinedOutput()
	if e != nil {
		return string(b), fmt.Errorf("%s: %w: %s", argv[0], e, strings.TrimSpace(string(b)))
	}
	return string(b), nil
}
func Manage(ctx context.Context, action, registry, envFile string) (string, error) {
	if action != "install" && action != "uninstall" && action != "status" {
		return "", fmt.Errorf("service action must be install, uninstall, or status")
	}
	if envFile != "" {
		p, e := filepath.Abs(envFile)
		if e != nil {
			return "", e
		}
		envFile = p
		if st, e := os.Stat(p); e != nil || !st.Mode().IsRegular() {
			return "", fmt.Errorf("env-file must be a regular trusted KEY=VALUE file")
		}
	}
	home, e := os.UserHomeDir()
	if e != nil {
		return "", e
	}
	binary, e := os.Executable()
	if e != nil {
		return "", e
	}
	shell := os.Getenv("SHELL")
	if shell == "" {
		shell = "/bin/sh"
		if runtime.GOOS == "darwin" {
			shell = "/bin/zsh"
		}
	}
	command := "exec " + quote(binary) + " daemon --registry " + quote(registry)
	if envFile != "" {
		command = "set -a; . " + quote(envFile) + "; set +a; " + command
	}
	if runtime.GOOS == "darwin" {
		path := filepath.Join(home, "Library", "LaunchAgents", label+".plist")
		domain := fmt.Sprintf("gui/%d", os.Getuid())
		switch action {
		case "status":
			if _, e := os.Stat(path); os.IsNotExist(e) {
				return "not installed", nil
			}
			text, e := run(ctx, "launchctl", "print", domain+"/"+label)
			if e != nil {
				return "installed, not loaded", nil
			}
			for _, line := range strings.Split(text, "\n") {
				if strings.Contains(line, "state =") {
					return "installed, " + strings.TrimSpace(line), nil
				}
			}
			return "installed, loaded", nil
		case "uninstall":
			run(ctx, "launchctl", "bootout", domain, path)
			if e = os.Remove(path); e != nil && !os.IsNotExist(e) {
				return "", e
			}
			return "uninstalled", nil
		case "install":
			if e = os.MkdirAll(filepath.Dir(registry), 0700); e != nil {
				return "", e
			}
			log := filepath.Join(filepath.Dir(registry), "daemon.log")
			plist := `<?xml version="1.0" encoding="UTF-8"?><!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd"><plist version="1.0"><dict><key>Label</key><string>` + label + `</string><key>ProgramArguments</key><array><string>` + escape(shell) + `</string><string>-lc</string><string>` + escape(command) + `</string></array><key>RunAtLoad</key><true/><key>KeepAlive</key><true/><key>StandardOutPath</key><string>` + escape(log) + `</string><key>StandardErrorPath</key><string>` + escape(log) + `</string></dict></plist>`
			if e = config.AtomicWrite(path, []byte(plist), 0600); e != nil {
				return "", e
			}
			run(ctx, "launchctl", "bootout", domain, path)
			if _, e = run(ctx, "launchctl", "bootstrap", domain, path); e != nil {
				return "", e
			}
			return "installed and started (launchd)", nil
		}
	}
	if runtime.GOOS == "linux" {
		if _, e = exec.LookPath("systemctl"); e != nil {
			return "", fmt.Errorf("no systemd; run watcher daemon under your supervisor")
		}
		path := filepath.Join(home, ".config", "systemd", "user", "watcher.service")
		switch action {
		case "status":
			if _, e = os.Stat(path); os.IsNotExist(e) {
				return "not installed", nil
			}
			text, _ := run(ctx, "systemctl", "--user", "is-active", "watcher.service")
			return "installed, " + strings.TrimSpace(text), nil
		case "uninstall":
			run(ctx, "systemctl", "--user", "disable", "--now", "watcher.service")
			if e = os.Remove(path); e != nil && !os.IsNotExist(e) {
				return "", e
			}
			_, e = run(ctx, "systemctl", "--user", "daemon-reload")
			return "uninstalled", e
		case "install":
			esc := func(s string) string {
				return strings.NewReplacer("\\", "\\\\", "\"", "\\\"", "%", "%%", "$", "$$").Replace(s)
			}
			unit := fmt.Sprintf("[Unit]\nDescription=watcher daemon (semantic stream watching)\n\n[Service]\nExecStart=\"%s\" -lc \"%s\"\nRestart=on-failure\nRestartSec=5\n\n[Install]\nWantedBy=default.target\n", esc(shell), esc(command))
			if e = config.AtomicWrite(path, []byte(unit), 0600); e != nil {
				return "", e
			}
			if _, e = run(ctx, "systemctl", "--user", "daemon-reload"); e != nil {
				return "", e
			}
			_, e = run(ctx, "systemctl", "--user", "enable", "--now", "watcher.service")
			return "installed and started (systemd)", e
		}
	}
	return "", fmt.Errorf("unsupported service platform %s", runtime.GOOS)
}
