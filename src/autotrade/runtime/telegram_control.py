from __future__ import annotations

import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from enum import StrEnum
from threading import Event
from threading import Thread
from time import monotonic

from autotrade.config import TelegramSettings
from autotrade.report import AlertSeverity
from autotrade.report import NotificationMessage
from autotrade.report import Notifier
from autotrade.report import TelegramHttpRequest
from autotrade.report import TelegramHttpResponse
from autotrade.report import telegram_http_transport
from autotrade.runtime.control import FileRunnerControlStore

logger = logging.getLogger(__name__)
_TELEGRAM_API_BASE_URL = "https://api.telegram.org"
_CONTROL_POLLER_FAILURE_LOG_INTERVAL_SECONDS = 300.0


class TelegramControlCommand(StrEnum):
    PAUSE = "pause"
    RESUME = "resume"
    ACCOUNT = "account"


@dataclass(slots=True)
class TelegramControlPoller:
    settings: TelegramSettings
    control_store: FileRunnerControlStore
    notifier: Notifier
    clock: Callable[[], datetime]
    account_status_provider: Callable[[], str] | None = None
    runner_control_enabled: bool = True
    transport: Callable[[TelegramHttpRequest], TelegramHttpResponse] = (
        telegram_http_transport
    )

    def poll(self) -> None:
        if not self.settings.enabled:
            return
        assert self.settings.bot_token is not None
        assert self.settings.chat_id is not None

        state = self.control_store.load()
        payload: dict[str, object] = {
            "allowed_updates": ["message"],
            "timeout": 0,
        }
        if state.telegram_update_offset is not None:
            payload["offset"] = state.telegram_update_offset

        request = TelegramHttpRequest(
            url=f"{_TELEGRAM_API_BASE_URL}/bot{self.settings.bot_token}/getUpdates",
            body=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            timeout=self.settings.control_timeout_seconds,
            force_ipv4=self.settings.force_ipv4,
        )
        response = self.transport(request)
        decoded_payload = _decode_payload(response.body)
        if response.status != 200 or decoded_payload.get("ok") is not True:
            description = decoded_payload.get(
                "description", "telegram getUpdates failed"
            )
            raise RuntimeError(
                f"telegram getUpdates failed: status={response.status} description={description}"
            )

        next_offset = state.telegram_update_offset
        notifications: list[NotificationMessage] = []
        for update in _require_update_list(decoded_payload.get("result")):
            update_id = update.get("update_id")
            if isinstance(update_id, int):
                next_offset = (
                    update_id + 1
                    if next_offset is None
                    else max(next_offset, update_id + 1)
                )
            command = _extract_control_command(
                update,
                allowed_chat_id=self.settings.chat_id,
                runner_control_enabled=self.runner_control_enabled,
            )
            if command is None:
                continue
            notifications.append(self._apply_command(command))

        if next_offset is not None:
            self.control_store.save_telegram_update_offset(next_offset)

        for notification in notifications:
            self._send_notification(notification)

    def _apply_command(
        self,
        command: TelegramControlCommand,
    ) -> NotificationMessage:
        if command is TelegramControlCommand.ACCOUNT:
            return self._build_account_status_notification()
        timestamp = self.clock()
        if command is TelegramControlCommand.PAUSE:
            state = self.control_store.pause(timestamp=timestamp, source="telegram")
        elif command is TelegramControlCommand.RESUME:
            state = self.control_store.resume(timestamp=timestamp, source="telegram")
        else:  # pragma: no cover - enum exhaustiveness guard
            raise ValueError(f"unsupported telegram control command: {command}")
        return NotificationMessage(
            created_at=timestamp,
            severity=AlertSeverity.INFO,
            subject=f"AutoTrade runner control [{command.value.upper()}]",
            body="\n".join(
                (
                    f"mode={state.mode.value}",
                    "source=telegram",
                )
            ),
        )

    def _build_account_status_notification(self) -> NotificationMessage:
        timestamp = self.clock()
        if self.account_status_provider is None:
            return NotificationMessage(
                created_at=timestamp,
                severity=AlertSeverity.INFO,
                subject="AutoTrade account performance [UNAVAILABLE]",
                body="계좌 조회 provider가 설정되어 있지 않습니다.",
            )
        try:
            body = self.account_status_provider()
        except Exception as error:
            logger.warning("telegram 계좌 조회 명령 처리에 실패했습니다. error=%s", error)
            body = f"계좌 조회에 실패했습니다: {error}"
            return NotificationMessage(
                created_at=timestamp,
                severity=AlertSeverity.INFO,
                subject="AutoTrade account performance [FAILED]",
                body=body,
            )
        return NotificationMessage(
            created_at=timestamp,
            severity=AlertSeverity.INFO,
            subject="AutoTrade account performance",
            body=body,
        )

    def _send_notification(self, notification: NotificationMessage) -> None:
        try:
            self.notifier.send(notification)
        except Exception as error:
            logger.warning(
                "telegram control 확인 알림 전송에 실패했습니다. subject=%s error=%s",
                notification.subject,
                error,
            )


@dataclass(slots=True)
class BackgroundTelegramControlPoller:
    poller: TelegramControlPoller
    poll_interval_seconds: float = 5.0
    stop_timeout_seconds: float = 2.0
    failure_log_interval_seconds: float = _CONTROL_POLLER_FAILURE_LOG_INTERVAL_SECONDS
    _stop_event: Event = field(init=False, repr=False)
    _worker: Thread | None = field(default=None, init=False, repr=False)
    _last_failure_logged_at: float | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _last_failure_message: str | None = field(default=None, init=False, repr=False)

    def __post_init__(self) -> None:
        if self.poll_interval_seconds <= 0:
            raise ValueError("poll_interval_seconds must be positive")
        if self.stop_timeout_seconds <= 0:
            raise ValueError("stop_timeout_seconds must be positive")
        if self.failure_log_interval_seconds < 0:
            raise ValueError("failure_log_interval_seconds must be non-negative")
        self._stop_event = Event()

    def start(self) -> None:
        if self._worker is not None and self._worker.is_alive():
            return
        if self._stop_event.is_set():
            self._stop_event = Event()
        self._worker = Thread(
            target=self._run,
            name="autotrade-telegram-control-poller",
            daemon=True,
        )
        self._worker.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._worker is not None:
            self._worker.join(timeout=self.stop_timeout_seconds)

    def close(self) -> None:
        self.stop()

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.poller.poll()
            except Exception as error:  # pragma: no cover - defensive worker guard
                self._log_failure(error)
            self._stop_event.wait(self.poll_interval_seconds)

    def _log_failure(self, error: Exception) -> None:
        error_message = str(error) or type(error).__name__
        logged_at = self._last_failure_logged_at
        now = monotonic()
        if (
            error_message == self._last_failure_message
            and logged_at is not None
            and now - logged_at < self.failure_log_interval_seconds
        ):
            return
        self._last_failure_message = error_message
        self._last_failure_logged_at = now
        logger.warning(
            "telegram control background poller 실행에 실패했습니다. error=%s",
            error_message,
        )


def _decode_payload(body: bytes) -> dict[str, object]:
    if not body:
        return {}
    try:
        decoded = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _require_update_list(raw_value: object) -> tuple[dict[str, object], ...]:
    if raw_value is None:
        return ()
    if not isinstance(raw_value, list):
        raise RuntimeError("telegram getUpdates result must be a list")
    updates: list[dict[str, object]] = []
    for item in raw_value:
        if isinstance(item, dict) and all(isinstance(key, str) for key in item):
            updates.append(item)
    return tuple(updates)


def _extract_control_command(
    update: dict[str, object],
    *,
    allowed_chat_id: str,
    runner_control_enabled: bool = True,
) -> TelegramControlCommand | None:
    message = update.get("message")
    if not isinstance(message, dict):
        return None
    chat = message.get("chat")
    if not isinstance(chat, dict):
        return None
    if str(chat.get("id")) != allowed_chat_id:
        return None
    text = message.get("text")
    if not isinstance(text, str):
        return None
    token = text.strip().split(maxsplit=1)[0] if text.strip() else ""
    command_name = token.split("@", maxsplit=1)[0].casefold()
    if runner_control_enabled and command_name == "/pause":
        return TelegramControlCommand.PAUSE
    if runner_control_enabled and command_name == "/resume":
        return TelegramControlCommand.RESUME
    if command_name in {"/account", "/balance"}:
        return TelegramControlCommand.ACCOUNT
    return None
