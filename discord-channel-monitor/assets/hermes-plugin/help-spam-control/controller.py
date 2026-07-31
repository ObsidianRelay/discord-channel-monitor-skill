"""Telegram Help 提醒的人工垃圾消息控制。

只处理已配对用户在指定群组话题内发送的严格指令。默认支持
“垃圾消息”/“spam”和“恢复消息”/“restore”，插件不保存正文。
"""

from __future__ import annotations

import asyncio
import importlib.util
import logging
import os
import re
from pathlib import Path
from typing import Any


LOGGER = logging.getLogger(__name__)
DEFAULT_COMMANDS = {
    "垃圾消息": True,
    "spam": True,
    "恢复消息": False,
    "restore": False,
}
DISCORD_MESSAGE_URL_RE = re.compile(
    r"https?://(?:www\.)?discord(?:app)?\.com/channels/"
    r"(?P<guild>\d+)/(?P<channel>\d+)/(?P<message>\d+)"
)
_TASKS: set[asyncio.Task[Any]] = set()
_STATE_MODULE: Any = None


def _platform_name(value: Any) -> str:
    raw = getattr(value, "value", value)
    return str(raw or "").strip().lower().split(".")[-1]


def _load_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return values
    for raw_line in lines:
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        if key:
            values[key] = value
    return values


def _commands() -> dict[str, bool]:
    """读取可配置严格别名；空值时保留公开版中英文默认指令。"""
    env_path = Path(
        os.environ.get(
            "HERMES_DISCORD_MONITOR_ENV_FILE",
            "~/.hermes/discord-channel-monitor.env",
        )
    ).expanduser()
    values = _load_env_file(env_path)
    result = dict(DEFAULT_COMMANDS)
    for key, active in (
        ("HERMES_HELP_SPAM_ALIASES", True),
        ("HERMES_HELP_RESTORE_ALIASES", False),
    ):
        raw_value = os.environ.get(key) or values.get(key) or ""
        for alias in raw_value.split(","):
            command = alias.strip()
            if command:
                result[command] = active
    return result


def _runtime_config() -> tuple[str, str, str]:
    env_path = Path(
        os.environ.get(
            "HERMES_DISCORD_MONITOR_ENV_FILE",
            "~/.hermes/discord-channel-monitor.env",
        )
    ).expanduser()
    values = _load_env_file(env_path)
    target = (
        os.environ.get("HERMES_NOTIFY_TARGET")
        or values.get("HERMES_NOTIFY_TARGET")
        or ""
    )
    channel_id = (
        os.environ.get("DISCORD_MONITOR_CHANNEL_ID")
        or values.get("DISCORD_MONITOR_CHANNEL_ID")
        or ""
    )
    state_module_path = (
        os.environ.get("HERMES_HELP_SPAM_STATE_MODULE")
        or values.get("HERMES_HELP_SPAM_STATE_MODULE")
        or str(
            Path(
                values.get("DISCORD_MONITOR_STATE_DIR")
                or (
                    Path.home()
                    / ".hermes"
                    / "services"
                    / "discord-channel-monitor"
                )
            ).expanduser()
            / "help_spam_state.py"
        )
    )
    return target, channel_id, state_module_path


def _load_state_module() -> Any:
    global _STATE_MODULE
    if _STATE_MODULE is not None:
        return _STATE_MODULE
    _, _, module_path_raw = _runtime_config()
    module_path = Path(module_path_raw).expanduser()
    spec = importlib.util.spec_from_file_location(
        "hermes_help_spam_state",
        module_path,
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载 Help 垃圾状态模块：{module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    _STATE_MODULE = module
    return module


def _reply_message(event: Any) -> Any:
    raw = getattr(event, "raw_message", None)
    if isinstance(raw, dict):
        return raw.get("reply_to_message")
    return getattr(raw, "reply_to_message", None)


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


def _is_own_bot_reply(event: Any, gateway: Any) -> bool:
    reply = _reply_message(event)
    if reply is None:
        return False
    author = _field(reply, "from_user")
    if author is None:
        return False
    source = getattr(event, "source", None)
    adapter = gateway._adapter_for_source(source)
    bot = getattr(adapter, "_bot", None)
    bot_id = str(getattr(bot, "id", "") or "")
    author_id = str(_field(author, "id", "") or "")
    is_bot = bool(_field(author, "is_bot", False))
    return bool(bot_id and author_id == bot_id and is_bot)


def _entity_urls(value: Any) -> list[str]:
    urls: list[str] = []
    for name in ("entities", "caption_entities"):
        for entity in (_field(value, name, []) or []):
            url = str(_field(entity, "url", "") or "").strip()
            if url:
                urls.append(url)
    return urls


def _reply_texts(event: Any) -> list[str]:
    reply = _reply_message(event)
    values = [
        getattr(event, "reply_to_text", "") or "",
        _field(reply, "text", "") or "",
        _field(reply, "caption", "") or "",
        *_entity_urls(reply),
    ]
    return [str(value) for value in values if str(value)]


def _legacy_first_message_id(
    event: Any,
    *,
    expected_channel_id: str,
) -> str:
    for value in _reply_texts(event):
        for match in DISCORD_MESSAGE_URL_RE.finditer(value):
            if (
                expected_channel_id
                and match.group("channel") == expected_channel_id
            ):
                return match.group("message")
    return ""


def _source_matches_target(source: Any, target: str) -> bool:
    try:
        state_module = _load_state_module()
        parsed = state_module.parse_telegram_target(target)
    except Exception:
        LOGGER.exception("无法读取 Help Telegram 提醒目标")
        return False
    if not parsed:
        return False
    chat_id, thread_id = parsed
    return (
        str(getattr(source, "chat_id", "") or "") == chat_id
        and str(getattr(source, "thread_id", "") or "") == thread_id
    )


def _is_paired_operator(gateway: Any, source: Any) -> bool:
    user_id = str(getattr(source, "user_id", "") or "")
    if not user_id:
        return False
    try:
        pairing_store = gateway._pairing_store_for(source)
        return bool(
            pairing_store
            and pairing_store.is_approved("telegram", user_id)
        )
    except Exception:
        LOGGER.exception("检查 Telegram 配对权限失败")
        return False


async def _send_ack(gateway: Any, event: Any, text: str) -> None:
    source = getattr(event, "source", None)
    adapter = gateway._adapter_for_source(source)
    if adapter is None:
        raise RuntimeError("Telegram adapter 当前不可用。")
    metadata = dict(
        gateway._thread_metadata_for_source(
            source,
            getattr(event, "message_id", None),
        )
        or {}
    )
    metadata["thread_id"] = str(getattr(source, "thread_id", "") or "")
    metadata["notify"] = True
    result = await adapter.send(
        str(getattr(source, "chat_id", "") or ""),
        text,
        reply_to=str(getattr(event, "message_id", "") or ""),
        metadata=metadata,
    )
    if not bool(getattr(result, "success", False)):
        raise RuntimeError(
            str(getattr(result, "error", "") or "Telegram 回执发送失败")
        )


async def _process_command(
    gateway: Any,
    event: Any,
    *,
    active: bool,
    expected_channel_id: str,
) -> None:
    source = getattr(event, "source", None)
    if not getattr(event, "reply_to_message_id", None):
        await _send_ack(
            gateway,
            event,
            "⚠️ 请使用 Telegram 的“回复”功能引用对应的 Help 提醒。",
        )
        return
    if not _is_own_bot_reply(event, gateway):
        await _send_ack(
            gateway,
            event,
            "⚠️ 只能回复大白发送的 Help 提醒。",
        )
        return

    state_module = _load_state_module()
    first_discord_message_id = _legacy_first_message_id(
        event,
        expected_channel_id=expected_channel_id,
    )
    resolved = await asyncio.to_thread(
        state_module.resolve_alert_group,
        telegram_chat_id=str(getattr(source, "chat_id", "") or ""),
        telegram_thread_id=str(getattr(source, "thread_id", "") or ""),
        telegram_message_id=str(
            getattr(event, "reply_to_message_id", "") or ""
        ),
        legacy_first_discord_message_id=first_discord_message_id,
    )
    if not resolved:
        await _send_ack(
            gateway,
            event,
            "⚠️ 这条回复没有匹配到可管理的 Help 提醒。",
        )
        return
    alert_key, message_ids = resolved
    command_key = (
        f"{getattr(source, 'chat_id', '')}:"
        f"{getattr(source, 'thread_id', '')}:"
        f"{getattr(event, 'message_id', '')}"
    )
    result = await asyncio.to_thread(
        state_module.update_exclusions,
        discord_message_ids=message_ids,
        active=active,
        source_alert_key=alert_key,
        operator_id=str(getattr(source, "user_id", "") or ""),
        command_key=command_key,
    )
    count = len(result["message_ids"])
    if active:
        ack = f"✅ 已排除{count}条Help消息，不会进入后续日报。"
    else:
        ack = f"↩️ 已恢复{count}条Help消息，将重新参与后续日报。"
    await _send_ack(gateway, event, ack)


def _schedule(
    gateway: Any,
    event: Any,
    *,
    active: bool,
    expected_channel_id: str,
) -> None:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        LOGGER.error("Help 垃圾控制需要正在运行的事件循环")
        return
    task = loop.create_task(
        _process_command(
            gateway,
            event,
            active=active,
            expected_channel_id=expected_channel_id,
        )
    )
    _TASKS.add(task)

    def done(completed: asyncio.Task[Any]) -> None:
        _TASKS.discard(completed)
        if not completed.cancelled() and completed.exception():
            LOGGER.error(
                "Help 垃圾控制任务失败",
                exc_info=completed.exception(),
            )

    task.add_done_callback(done)


def handle_pre_gateway_dispatch(
    event: Any,
    gateway: Any,
    **kwargs: Any,
):
    del kwargs
    source = getattr(event, "source", None)
    if (
        source is None
        or _platform_name(getattr(source, "platform", None))
        != "telegram"
    ):
        return None
    command = str(getattr(event, "text", "") or "").strip()
    commands = _commands()
    if command not in commands:
        return None
    target, expected_channel_id, _ = _runtime_config()
    if not _source_matches_target(source, target):
        return None
    if not _is_paired_operator(gateway, source):
        return {
            "action": "skip",
            "reason": "help-spam-control-not-paired",
        }
    _schedule(
        gateway,
        event,
        active=commands[command],
        expected_channel_id=expected_channel_id,
    )
    return {
        "action": "skip",
        "reason": "help-spam-control-handled",
    }


def self_test() -> None:
    assert _platform_name("telegram") == "telegram"
    assert _platform_name("Platform.TELEGRAM") == "telegram"
    commands = _commands()
    assert commands["垃圾消息"] is True
    assert commands["spam"] is True
    assert commands["恢复消息"] is False
    assert commands["restore"] is False
    sample = "https://discord.com/channels/1001/2002/3003"
    match = DISCORD_MESSAGE_URL_RE.search(sample)
    assert match
    assert match.group("channel") == "2002"
    assert match.group("message") == "3003"


if __name__ == "__main__":
    self_test()
    print("Help 垃圾控制插件自检通过。")
