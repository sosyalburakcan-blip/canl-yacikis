from __future__ import annotations

import argparse
import ctypes
import json
import os
from datetime import datetime, timezone

from spotbot import BotConfig, BotRuntime
from spotbot.web import create_server


UTC = timezone.utc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Polymarket BTC Up or Down 5m Bot + Web UI")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--state-file", default="runtime/polymarket_btc_5m_state.json")
    parser.add_argument("--secrets-file", default=".env")
    parser.add_argument("--autostart", action="store_true")
    parser.add_argument("--run-once", action="store_true")
    parser.add_argument("--reset", action="store_true")
    return parser


def _pid_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        process_query_limited_information = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(process_query_limited_information, False, pid)
        if handle:
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        return False
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def acquire_instance_lock(state_file: str) -> str:
    lock_path = os.path.abspath(f"{state_file}.lock")
    lock_dir = os.path.dirname(lock_path)
    if lock_dir:
        os.makedirs(lock_dir, exist_ok=True)
    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        except FileExistsError:
            try:
                with open(lock_path, "r", encoding="utf-8") as handle:
                    payload = json.load(handle)
            except Exception:
                payload = {}
            existing_pid = int(payload.get("pid", 0) or 0)
            if existing_pid and _pid_alive(existing_pid):
                raise RuntimeError(
                    f"Ayni state dosyasi zaten baska bir bot tarafindan kullaniliyor: pid={existing_pid}, lock={lock_path}"
                )
            try:
                os.remove(lock_path)
            except OSError as exc:
                raise RuntimeError(f"Stale lock temizlenemedi: {lock_path}") from exc
            continue
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(
                    {
                        "pid": os.getpid(),
                        "created_at": datetime.now(UTC).isoformat(),
                        "state_file": os.path.abspath(state_file),
                    },
                    handle,
                    ensure_ascii=False,
                )
        except Exception:
            try:
                os.remove(lock_path)
            except OSError:
                pass
            raise
        return lock_path


def release_instance_lock(lock_path: str) -> None:
    try:
        with open(lock_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception:
        payload = {}
    owner_pid = int(payload.get("pid", 0) or 0)
    if owner_pid not in {0, os.getpid()}:
        return
    try:
        os.remove(lock_path)
    except OSError:
        pass


def main() -> int:
    args = build_parser().parse_args()
    config = BotConfig(state_file=args.state_file, secrets_file=args.secrets_file)
    lock_path = acquire_instance_lock(config.state_file)
    runtime: BotRuntime | None = None

    try:
        runtime = BotRuntime(config)

        if args.reset:
            print(json.dumps(runtime.reset_state(), ensure_ascii=False, indent=2))
            if not args.run_once and not args.autostart:
                return 0

        if args.run_once:
            print(json.dumps(runtime.run_once(), ensure_ascii=False, indent=2))
            return 0

        if args.autostart:
            runtime.start()

        web_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")
        server = create_server(runtime, args.host, args.port, web_root)
        print(f"Panel hazır: http://{args.host}:{args.port}")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            runtime.close()
            server.server_close()
        return 0
    finally:
        release_instance_lock(lock_path)


if __name__ == "__main__":
    raise SystemExit(main())
