from __future__ import annotations

import json
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from time import monotonic
from time import sleep

from autotrade.config import TelegramSettings
from autotrade.data import KST
from autotrade.report import NotificationMessage
from autotrade.report import TelegramHttpResponse
from autotrade.runtime.control import FileRunnerControlStore
from autotrade.runtime.control import RunnerControlMode
from autotrade.runtime.telegram_control import BackgroundTelegramControlPoller
from autotrade.runtime.telegram_control import TelegramControlPoller


def test_telegram_control_poller_accepts_primary_chat_commands(tmp_path) -> None:
    control_store = FileRunnerControlStore(tmp_path / "runner_control.json")
    notifier = RecordingNotifier()
    timestamps = [
        datetime(2026, 4, 10, 9, 0, tzinfo=KST),
        datetime(2026, 4, 10, 9, 5, tzinfo=KST),
    ]
    requests = []

    def transport(request):
        requests.append(request)
        return TelegramHttpResponse(
            status=200,
            body=json.dumps(
                {
                    "ok": True,
                    "result": [
                        {
                            "update_id": 10,
                            "message": {"chat": {"id": "-100other"}, "text": "/pause"},
                        },
                        {
                            "update_id": 11,
                            "message": {"chat": {"id": "-100base"}, "text": "/pause"},
                        },
                        {
                            "update_id": 12,
                            "message": {
                                "chat": {"id": "-100base"},
                                "text": "/resume@AutoTradeBot",
                            },
                        },
                    ],
                }
            ).encode("utf-8"),
            headers={},
        )

    poller = TelegramControlPoller(
        settings=TelegramSettings(
            enabled=True,
            bot_token="bot-token",
            chat_id="-100base",
            control_timeout_seconds=2.5,
        ),
        control_store=control_store,
        notifier=notifier,
        clock=lambda: timestamps.pop(0),
        transport=transport,
    )

    poller.poll()

    state = control_store.load()
    assert state.mode is RunnerControlMode.RUNNING
    assert state.paused_by == "telegram"
    assert state.resumed_by == "telegram"
    assert state.telegram_update_offset == 13
    assert len(notifier.notifications) == 2
    assert notifier.notifications[0].subject == "AutoTrade runner control [PAUSE]"
    assert notifier.notifications[1].subject == "AutoTrade runner control [RESUME]"
    request_payload = json.loads(requests[0].body.decode("utf-8"))
    assert request_payload["allowed_updates"] == ["message"]
    assert requests[0].timeout == 2.5


def test_telegram_control_poller_ignores_other_chats_and_advances_offset(
    tmp_path,
) -> None:
    control_store = FileRunnerControlStore(tmp_path / "runner_control.json")
    notifier = RecordingNotifier()

    def transport(request):
        return TelegramHttpResponse(
            status=200,
            body=json.dumps(
                {
                    "ok": True,
                    "result": [
                        {
                            "update_id": 20,
                            "message": {"chat": {"id": "-100other"}, "text": "/pause"},
                        }
                    ],
                }
            ).encode("utf-8"),
            headers={},
        )

    poller = TelegramControlPoller(
        settings=TelegramSettings(
            enabled=True,
            bot_token="bot-token",
            chat_id="-100base",
        ),
        control_store=control_store,
        notifier=notifier,
        clock=lambda: datetime(2026, 4, 10, 9, 0, tzinfo=KST),
        transport=transport,
    )

    poller.poll()

    state = control_store.load()
    assert state.mode is RunnerControlMode.RUNNING
    assert state.telegram_update_offset == 21
    assert notifier.notifications == []


def test_telegram_control_poller_sends_account_status_for_primary_chat(
    tmp_path,
) -> None:
    control_store = FileRunnerControlStore(tmp_path / "runner_control.json")
    notifier = RecordingNotifier()
    requests = []

    def transport(request):
        requests.append(request)
        return TelegramHttpResponse(
            status=200,
            body=json.dumps(
                {
                    "ok": True,
                    "result": [
                        {
                            "update_id": 25,
                            "message": {
                                "chat": {"id": "-100base"},
                                "text": "/account@AutoTradeBot",
                            },
                        }
                    ],
                }
            ).encode("utf-8"),
            headers={},
        )

    poller = TelegramControlPoller(
        settings=TelegramSettings(
            enabled=True,
            bot_token="bot-token",
            chat_id="-100base",
        ),
        control_store=control_store,
        notifier=notifier,
        clock=lambda: datetime(2026, 4, 10, 9, 0, tzinfo=KST),
        account_status_provider=lambda: "계좌 수익률: +1.23%",
        transport=transport,
    )

    poller.poll()

    state = control_store.load()
    assert state.mode is RunnerControlMode.RUNNING
    assert state.telegram_update_offset == 26
    assert len(notifier.notifications) == 1
    assert notifier.notifications[0].subject == "AutoTrade account performance"
    assert notifier.notifications[0].body == "계좌 수익률: +1.23%"
    request_payload = json.loads(requests[0].body.decode("utf-8"))
    assert "offset" not in request_payload


def test_telegram_control_poller_reports_account_status_failure(
    tmp_path,
) -> None:
    control_store = FileRunnerControlStore(tmp_path / "runner_control.json")
    notifier = RecordingNotifier()

    def transport(request):
        del request
        return TelegramHttpResponse(
            status=200,
            body=json.dumps(
                {
                    "ok": True,
                    "result": [
                        {
                            "update_id": 26,
                            "message": {
                                "chat": {"id": "-100base"},
                                "text": "/balance",
                            },
                        }
                    ],
                }
            ).encode("utf-8"),
            headers={},
        )

    def failing_provider() -> str:
        raise RuntimeError("broker unavailable")

    poller = TelegramControlPoller(
        settings=TelegramSettings(
            enabled=True,
            bot_token="bot-token",
            chat_id="-100base",
        ),
        control_store=control_store,
        notifier=notifier,
        clock=lambda: datetime(2026, 4, 10, 9, 0, tzinfo=KST),
        account_status_provider=failing_provider,
        transport=transport,
    )

    poller.poll()

    state = control_store.load()
    assert state.mode is RunnerControlMode.RUNNING
    assert state.telegram_update_offset == 27
    assert len(notifier.notifications) == 1
    assert notifier.notifications[0].subject == "AutoTrade account performance [FAILED]"
    assert "broker unavailable" in notifier.notifications[0].body


def test_telegram_control_poller_ignores_pause_when_runner_control_disabled(
    tmp_path,
) -> None:
    control_store = FileRunnerControlStore(tmp_path / "runner_control.json")
    notifier = RecordingNotifier()

    def transport(request):
        del request
        return TelegramHttpResponse(
            status=200,
            body=json.dumps(
                {
                    "ok": True,
                    "result": [
                        {
                            "update_id": 27,
                            "message": {
                                "chat": {"id": "-100base"},
                                "text": "/pause",
                            },
                        },
                        {
                            "update_id": 28,
                            "message": {
                                "chat": {"id": "-100base"},
                                "text": "/account",
                            },
                        },
                    ],
                }
            ).encode("utf-8"),
            headers={},
        )

    poller = TelegramControlPoller(
        settings=TelegramSettings(
            enabled=True,
            bot_token="bot-token",
            chat_id="-100base",
        ),
        control_store=control_store,
        notifier=notifier,
        clock=lambda: datetime(2026, 4, 10, 9, 0, tzinfo=KST),
        account_status_provider=lambda: "계좌 수익률: +1.23%",
        runner_control_enabled=False,
        transport=transport,
    )

    poller.poll()

    state = control_store.load()
    assert state.mode is RunnerControlMode.RUNNING
    assert state.paused_by is None
    assert state.telegram_update_offset == 29
    assert len(notifier.notifications) == 1
    assert notifier.notifications[0].subject == "AutoTrade account performance"


def test_telegram_control_poller_persists_offset_when_ack_notification_fails(
    tmp_path,
) -> None:
    control_store = FileRunnerControlStore(tmp_path / "runner_control.json")
    requests = []

    def transport(request):
        requests.append(request)
        return TelegramHttpResponse(
            status=200,
            body=json.dumps(
                {
                    "ok": True,
                    "result": [
                        {
                            "update_id": 30,
                            "message": {"chat": {"id": "-100base"}, "text": "/pause"},
                        }
                    ],
                }
            ).encode("utf-8"),
            headers={},
        )

    poller = TelegramControlPoller(
        settings=TelegramSettings(
            enabled=True,
            bot_token="bot-token",
            chat_id="-100base",
        ),
        control_store=control_store,
        notifier=FailingNotifier(),
        clock=lambda: datetime(2026, 4, 10, 9, 0, tzinfo=KST),
        transport=transport,
    )

    poller.poll()
    poller.poll()

    state = control_store.load()
    assert state.mode is RunnerControlMode.PAUSED
    assert state.paused_by == "telegram"
    assert state.telegram_update_offset == 31
    second_payload = json.loads(requests[1].body.decode("utf-8"))
    assert second_payload["offset"] == 31


def test_background_telegram_control_poller_updates_store(tmp_path) -> None:
    control_store = FileRunnerControlStore(tmp_path / "runner_control.json")
    responses = [
        {
            "ok": True,
            "result": [
                {
                    "update_id": 40,
                    "message": {"chat": {"id": "-100base"}, "text": "/pause"},
                }
            ],
        },
        {"ok": True, "result": []},
    ]

    def transport(request):
        del request
        payload = responses.pop(0) if responses else {"ok": True, "result": []}
        return TelegramHttpResponse(
            status=200,
            body=json.dumps(payload).encode("utf-8"),
            headers={},
        )

    poller = TelegramControlPoller(
        settings=TelegramSettings(
            enabled=True,
            bot_token="bot-token",
            chat_id="-100base",
        ),
        control_store=control_store,
        notifier=RecordingNotifier(),
        clock=lambda: datetime(2026, 4, 10, 9, 0, tzinfo=KST),
        transport=transport,
    )
    background = BackgroundTelegramControlPoller(
        poller,
        poll_interval_seconds=0.01,
        stop_timeout_seconds=0.1,
    )

    background.start()
    deadline = monotonic() + 1.0
    while monotonic() < deadline:
        if control_store.load().mode is RunnerControlMode.PAUSED:
            break
        sleep(0.001)
    background.stop()

    state = control_store.load()
    assert state.mode is RunnerControlMode.PAUSED
    assert state.paused_by == "telegram"
    assert state.telegram_update_offset == 41


@dataclass(slots=True)
class RecordingNotifier:
    notifications: list[NotificationMessage] = field(default_factory=list)

    def send(self, notification: NotificationMessage) -> None:
        self.notifications.append(notification)


@dataclass(slots=True)
class FailingNotifier:
    def send(self, notification: NotificationMessage) -> None:
        raise RuntimeError("telegram send failed")
