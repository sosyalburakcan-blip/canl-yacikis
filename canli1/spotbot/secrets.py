from __future__ import annotations

import os
from dataclasses import asdict, dataclass
from typing import Any

from .utils import atomic_write_json, load_json

DEFAULT_TELEGRAM_API_BASE_URL = "https://api.telegram.org"
ENV_FILE_FIELDS = {
    "telegram_bot_token": "TELEGRAM_BOT_TOKEN",
    "telegram_chat_id": "TELEGRAM_CHAT_ID",
    "telegram_api_base_url": "TELEGRAM_API_BASE_URL",
    "polymarket_private_key": "POLYMARKET_PRIVATE_KEY",
    "polymarket_funder_address": "POLYMARKET_FUNDER_ADDRESS",
    "polymarket_signature_type": "POLYMARKET_SIGNATURE_TYPE",
}


def normalize_telegram_api_base_url(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        return DEFAULT_TELEGRAM_API_BASE_URL
    if "://" not in raw:
        raw = f"https://{raw}"
    return raw.rstrip("/")


def _is_env_path(path: str) -> bool:
    return os.path.splitext(str(path or "").strip().lower())[1] == ".env" or os.path.basename(str(path or "")).lower() == ".env"


def _parse_env_value(raw: str) -> str:
    value = raw.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        value = value[1:-1]
        if raw.strip().startswith('"'):
            value = value.replace(r"\\", "\\").replace(r"\"", '"').replace(r"\n", "\n")
        return value
    parts: list[str] = []
    escaped = False
    for index, char in enumerate(value):
        if escaped:
            parts.append(char)
            escaped = False
            continue
        if char == "\\":
            escaped = True
            continue
        if char == "#" and (index == 0 or value[index - 1].isspace()):
            break
        parts.append(char)
    return "".join(parts).strip()


def _format_env_value(value: Any) -> str:
    raw = str(value if value is not None else "")
    escaped = raw.replace("\\", r"\\").replace('"', '\\"').replace("\n", r"\n")
    return f'"{escaped}"'


def _load_env(path: str) -> dict[str, str]:
    if not os.path.exists(path):
        return {}
    payload: dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            if raw.startswith("export "):
                raw = raw[7:].strip()
            key, sep, value = raw.partition("=")
            if not sep:
                continue
            key = key.strip()
            if key:
                payload[key] = _parse_env_value(value)
    return payload


def _coerce_signature_type(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def _atomic_write_env(path: str, secrets: "AppSecrets") -> None:
    directory = os.path.dirname(os.path.abspath(path))
    if directory:
        os.makedirs(directory, exist_ok=True)
    existing_lines: list[str] = []
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as handle:
            existing_lines = handle.readlines()

    values = {
        "TELEGRAM_BOT_TOKEN": secrets.telegram_bot_token,
        "TELEGRAM_CHAT_ID": secrets.telegram_chat_id,
        "TELEGRAM_API_BASE_URL": normalize_telegram_api_base_url(secrets.telegram_api_base_url),
        "POLYMARKET_PRIVATE_KEY": secrets.polymarket_private_key,
        "POLYMARKET_FUNDER_ADDRESS": secrets.polymarket_funder_address,
        "POLYMARKET_SIGNATURE_TYPE": int(secrets.polymarket_signature_type or 0),
    }
    remaining = set(values)
    output: list[str] = []
    for line in existing_lines:
        stripped = line.strip()
        candidate = stripped[7:].strip() if stripped.startswith("export ") else stripped
        key, sep, _value = candidate.partition("=")
        key = key.strip()
        if sep and key in values:
            output.append(f"{key}={_format_env_value(values[key])}\n")
            remaining.discard(key)
        else:
            output.append(line)
    if output and output[-1] and not output[-1].endswith("\n"):
        output[-1] = f"{output[-1]}\n"
    if remaining:
        if output and any(line.strip() for line in output):
            output.append("\n")
        for key in ENV_FILE_FIELDS.values():
            if key in remaining:
                output.append(f"{key}={_format_env_value(values[key])}\n")
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        handle.writelines(output)
    os.replace(tmp, path)


@dataclass
class AppSecrets:
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    telegram_api_base_url: str = DEFAULT_TELEGRAM_API_BASE_URL
    polymarket_private_key: str = ""
    polymarket_funder_address: str = ""
    polymarket_signature_type: int = 0

    def apply_env_overrides(self) -> "AppSecrets":
        telegram_token = os.getenv("TELEGRAM_BOT_TOKEN")
        telegram_chat = os.getenv("TELEGRAM_CHAT_ID")
        telegram_base = os.getenv("TELEGRAM_API_BASE_URL")
        polymarket_private_key = os.getenv("POLYMARKET_PRIVATE_KEY")
        polymarket_funder_address = os.getenv("POLYMARKET_FUNDER_ADDRESS")
        polymarket_signature_type = os.getenv("POLYMARKET_SIGNATURE_TYPE")
        if telegram_token:
            self.telegram_bot_token = telegram_token
        if telegram_chat:
            self.telegram_chat_id = telegram_chat
        if telegram_base:
            self.telegram_api_base_url = normalize_telegram_api_base_url(telegram_base)
        if polymarket_private_key:
            self.polymarket_private_key = polymarket_private_key
        if polymarket_funder_address:
            self.polymarket_funder_address = polymarket_funder_address
        if polymarket_signature_type:
            try:
                self.polymarket_signature_type = max(0, int(polymarket_signature_type))
            except ValueError:
                self.polymarket_signature_type = 0
        return self

    @property
    def has_telegram(self) -> bool:
        return bool(self.telegram_bot_token and self.telegram_chat_id)

    @property
    def has_polymarket_live(self) -> bool:
        return bool(self.polymarket_private_key)

    @staticmethod
    def _mask(value: str, keep: int = 4) -> str:
        if not value:
            return ""
        if len(value) <= keep * 2:
            return "*" * len(value)
        return f"{value[:keep]}...{value[-keep:]}"

    def masked(self) -> dict[str, Any]:
        return {
            "file": "",
            "telegram_configured": self.has_telegram,
            "telegram_chat_id_hint": self._mask(self.telegram_chat_id, keep=2),
            "telegram_token_hint": self._mask(self.telegram_bot_token, keep=4),
            "telegram_api_base_url": normalize_telegram_api_base_url(self.telegram_api_base_url),
            "polymarket_configured": self.has_polymarket_live,
            "polymarket_private_key_hint": self._mask(self.polymarket_private_key, keep=6),
            "polymarket_funder_address_hint": self._mask(self.polymarket_funder_address, keep=6),
            "polymarket_signature_type": int(self.polymarket_signature_type or 0),
        }


class SecretStore:
    def __init__(self, path: str) -> None:
        self.path = path

    def _load_raw(self) -> AppSecrets:
        if _is_env_path(self.path):
            payload = _load_env(self.path)
            return AppSecrets(
                telegram_bot_token=str(payload.get("TELEGRAM_BOT_TOKEN", "")),
                telegram_chat_id=str(payload.get("TELEGRAM_CHAT_ID", "")),
                telegram_api_base_url=normalize_telegram_api_base_url(payload.get("TELEGRAM_API_BASE_URL", DEFAULT_TELEGRAM_API_BASE_URL)),
                polymarket_private_key=str(payload.get("POLYMARKET_PRIVATE_KEY", "")),
                polymarket_funder_address=str(payload.get("POLYMARKET_FUNDER_ADDRESS", "")),
                polymarket_signature_type=_coerce_signature_type(payload.get("POLYMARKET_SIGNATURE_TYPE", 0)),
            )
        payload = load_json(self.path) or {}
        return AppSecrets(
            telegram_bot_token=str(payload.get("telegram_bot_token", "")),
            telegram_chat_id=str(payload.get("telegram_chat_id", "")),
            telegram_api_base_url=normalize_telegram_api_base_url(payload.get("telegram_api_base_url", DEFAULT_TELEGRAM_API_BASE_URL)),
            polymarket_private_key=str(payload.get("polymarket_private_key", "")),
            polymarket_funder_address=str(payload.get("polymarket_funder_address", "")),
            polymarket_signature_type=_coerce_signature_type(payload.get("polymarket_signature_type", 0)),
        )

    def load(self) -> AppSecrets:
        return self._load_raw().apply_env_overrides()

    def save(self, secrets: AppSecrets) -> None:
        if _is_env_path(self.path):
            _atomic_write_env(self.path, secrets)
            return
        atomic_write_json(self.path, asdict(secrets))

    def update(self, payload: dict[str, Any]) -> AppSecrets:
        secrets = self._load_raw()
        if "telegram_api_base_url" in payload:
            secrets.telegram_api_base_url = normalize_telegram_api_base_url(payload.get("telegram_api_base_url", ""))
        if "polymarket_signature_type" in payload:
            try:
                secrets.polymarket_signature_type = max(0, int(payload.get("polymarket_signature_type", 0)))
            except ValueError:
                secrets.polymarket_signature_type = 0
        for field_name in (
            "telegram_bot_token",
            "telegram_chat_id",
            "polymarket_private_key",
            "polymarket_funder_address",
        ):
            if field_name not in payload:
                continue
            value = str(payload.get(field_name, "") or "").strip()
            if value:
                setattr(secrets, field_name, value)
        self.save(secrets)
        return secrets
