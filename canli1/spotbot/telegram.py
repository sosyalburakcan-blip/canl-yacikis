from __future__ import annotations

import os
import time
from typing import Any

import requests


class TelegramNotifier:
    def __init__(
        self,
        token: str = "",
        chat_id: str = "",
        timeout: float = 15.0,
        base_url: str = "https://api.telegram.org",
    ) -> None:
        self.token = token.strip()
        self.chat_id = chat_id.strip()
        self.timeout = timeout
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "SolidSpotPaperBot/2.1"})

    def configure(self, token: str = "", chat_id: str = "", base_url: str | None = None) -> None:
        self.token = token.strip()
        self.chat_id = chat_id.strip()
        if base_url:
            self.base_url = base_url.rstrip("/")

    @classmethod
    def from_env(cls, timeout: float = 15.0) -> "TelegramNotifier":
        return cls(
            token=os.getenv("TELEGRAM_BOT_TOKEN", ""),
            chat_id=os.getenv("TELEGRAM_CHAT_ID", ""),
            timeout=timeout,
            base_url=os.getenv("TELEGRAM_API_BASE_URL", "https://api.telegram.org"),
        )

    @property
    def configured(self) -> bool:
        return bool(self.token and self.chat_id)

    @property
    def chat_id_hint(self) -> str:
        value = self.chat_id
        if not value:
            return ""
        if len(value) <= 4:
            return value
        return f"{value[:2]}...{value[-2:]}"

    def _request(self, method: str, api_method: str, payload: dict[str, Any] | None = None, timeout: float | None = None) -> dict[str, Any]:
        if not self.configured:
            raise RuntimeError("Telegram ayarlari eksik")
        url = f"{self.base_url}/bot{self.token}/{api_method}"
        last_error: str | None = None
        for attempt in range(1, 4):
            try:
                response = self.session.request(method.upper(), url, json=payload or {}, timeout=timeout or self.timeout)
            except requests.RequestException as exc:
                last_error = str(exc)
                if attempt >= 3:
                    raise RuntimeError(last_error) from exc
                time.sleep(0.4 * attempt)
                continue
            try:
                response_payload = response.json()
            except ValueError:
                response_payload = {}
            if response.status_code >= 500 and attempt < 3:
                time.sleep(0.4 * attempt)
                continue
            if response.status_code >= 400:
                detail = str(response_payload.get("description") or "").strip()
                raise RuntimeError(detail or f"Telegram HTTP {response.status_code}")
            if not response_payload.get("ok", False):
                raise RuntimeError(str(response_payload.get("description") or "Telegram API hatasi"))
            return response_payload
        raise RuntimeError(last_error or "Telegram istegi basarisiz")

    def send_message(self, text: str) -> dict[str, Any]:
        return self._request(
            "POST",
            "sendMessage",
            {
                "chat_id": self.chat_id,
                "text": text,
                "disable_web_page_preview": True,
            },
        )

    def get_updates(self, offset: int | None = None, timeout_seconds: int = 20) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {
            "timeout": max(1, int(timeout_seconds)),
            "allowed_updates": ["message"],
        }
        if offset is not None:
            payload["offset"] = int(offset)
        result = self._request("POST", "getUpdates", payload, timeout=max(self.timeout, timeout_seconds + 5))
        updates = result.get("result", [])
        return updates if isinstance(updates, list) else []

    def set_commands(self, commands: list[dict[str, str]]) -> dict[str, Any]:
        return self._request("POST", "setMyCommands", {"commands": commands})

    def close(self) -> None:
        self.session.close()
