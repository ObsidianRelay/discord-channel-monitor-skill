#!/usr/bin/env python3
"""监听 Discord 消息与新工单，并通过 Hermes 推送到 Telegram。"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import os
import random
import re
import shutil
import signal
import sys
import time
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp


DEFAULT_ENV_FILE = Path.home() / ".hermes" / "discord-channel-monitor.env"
DEFAULT_STATE_DIR = Path.home() / ".hermes" / "services" / "discord-channel-monitor"
DISCORD_API_BASE = "https://discord.com/api/v10"

HERMES_BIN: Path | None = None
TICKET_EVENT_FILE = DEFAULT_STATE_DIR / "data" / "ticket-events.jsonl"
TICKET_ROUTES_FILE = DEFAULT_STATE_DIR / "ticket-routes.json"
PENDING_MESSAGE_FILE = DEFAULT_STATE_DIR / "data" / "pending-message-alerts.json"
HELP_MESSAGE_STATE_FILE = DEFAULT_STATE_DIR / "data" / "help-message-state.json"
DEFAULT_TICKET_RECONCILE_INTERVAL_SECONDS = 60.0
DEFAULT_MESSAGE_NOTIFY_DELAY_SECONDS = 300.0
PENDING_MESSAGE_CHECK_INTERVAL_SECONDS = 5.0
DEFAULT_HELP_MESSAGE_RECONCILE_INTERVAL_SECONDS = 30.0
HELP_COLLECTION_CHECK_INTERVAL_SECONDS = 5.0
MESSAGE_SEPARATOR = "━━━━━━━━━━━━━━━━"

# Discord Gateway Intents：服务器、服务器消息、消息正文。
INTENT_GUILDS = 1 << 0
INTENT_GUILD_MESSAGES = 1 << 9
INTENT_MESSAGE_CONTENT = 1 << 15
GATEWAY_INTENTS = INTENT_GUILDS | INTENT_GUILD_MESSAGES | INTENT_MESSAGE_CONTENT


def load_env_file(path: Path) -> dict[str, str]:
    """读取简单的 KEY=VALUE 配置文件，不执行其中的任何命令。"""
    values: dict[str, str] = {}
    if not path.exists():
        raise RuntimeError(f"缺少配置文件：{path}\n请先运行 install_monitor.py。")

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def require_config(config: dict[str, str], key: str) -> str:
    value = config.get(key, "").strip()
    if not value:
        raise RuntimeError(f"配置项 {key} 不能为空。")
    return value


def configured_path(config: dict[str, str], key: str, default: Path) -> Path:
    value = config.get(key, "").strip()
    return Path(value).expanduser() if value else default


def configure_runtime(config: dict[str, str]) -> None:
    global HERMES_BIN, TICKET_EVENT_FILE, TICKET_ROUTES_FILE
    global PENDING_MESSAGE_FILE, HELP_MESSAGE_STATE_FILE

    configured_hermes = config.get("HERMES_BIN", "").strip()
    discovered = shutil.which("hermes")
    fallback = Path.home() / ".local" / "bin" / "hermes"
    hermes_path = (
        Path(configured_hermes).expanduser()
        if configured_hermes
        else Path(discovered) if discovered else fallback
    )
    if not hermes_path.is_file():
        raise RuntimeError(
            "找不到 Hermes。请安装 Hermes，或设置 HERMES_BIN。"
        )
    HERMES_BIN = hermes_path

    state_dir = configured_path(
        config,
        "DISCORD_MONITOR_STATE_DIR",
        DEFAULT_STATE_DIR,
    )
    TICKET_EVENT_FILE = configured_path(
        config,
        "DISCORD_TICKET_EVENT_FILE",
        state_dir / "data" / "ticket-events.jsonl",
    )
    TICKET_ROUTES_FILE = configured_path(
        config,
        "DISCORD_TICKET_ROUTES_FILE",
        state_dir / "ticket-routes.json",
    )
    PENDING_MESSAGE_FILE = configured_path(
        config,
        "DISCORD_PENDING_MESSAGE_FILE",
        state_dir / "data" / "pending-message-alerts.json",
    )
    HELP_MESSAGE_STATE_FILE = configured_path(
        config,
        "DISCORD_HELP_MESSAGE_STATE_FILE",
        state_dir / "data" / "help-message-state.json",
    )


def require_hermes_bin() -> Path:
    if HERMES_BIN is None:
        raise RuntimeError("Hermes 尚未配置。")
    return HERMES_BIN


def env_bool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def parse_id_set(value: str | None, config_name: str) -> set[str]:
    """解析逗号分隔的 Discord ID，并防止错误配置悄悄放行消息。"""
    if not value or not value.strip():
        return set()
    result = {item.strip() for item in value.split(",") if item.strip()}
    invalid = sorted(item for item in result if not item.isdigit())
    if invalid:
        raise RuntimeError(f"配置项 {config_name} 包含无效 ID：{', '.join(invalid)}")
    return result


def parse_ticket_routes(value: str | None) -> dict[str, dict[str, str]]:
    """解析“Discord 分类 ID -> 工单类型/通知目标”的 JSON 配置。"""
    if not value or not value.strip():
        return {}

    try:
        raw_routes = json.loads(value)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"DISCORD_TICKET_ROUTES_JSON 不是有效 JSON：{exc}") from exc

    if not isinstance(raw_routes, dict):
        raise RuntimeError("DISCORD_TICKET_ROUTES_JSON 必须是 JSON 对象。")

    routes: dict[str, dict[str, str]] = {}
    for category_id, raw_route in raw_routes.items():
        category_id = str(category_id).strip()
        if not category_id.isdigit():
            raise RuntimeError(f"工单分类 ID 无效：{category_id}")
        if not isinstance(raw_route, dict):
            raise RuntimeError(f"工单分类 {category_id} 的路由配置必须是 JSON 对象。")

        label = str(raw_route.get("label") or "").strip()
        if not label:
            raise RuntimeError(f"工单分类 {category_id} 缺少 label。")

        route = {"label": label}
        for key in ("target", "owner"):
            item = str(raw_route.get(key) or "").strip()
            if item:
                route[key] = item
        routes[category_id] = route
    return routes


def load_ticket_routes(config: dict[str, str]) -> dict[str, dict[str, str]]:
    """环境变量优先；未设置时读取项目内不含凭据的路由文件。"""
    env_value = config.get("DISCORD_TICKET_ROUTES_JSON")
    if env_value and env_value.strip():
        return parse_ticket_routes(env_value)
    if TICKET_ROUTES_FILE.exists():
        return parse_ticket_routes(TICKET_ROUTES_FILE.read_text(encoding="utf-8"))
    return {}


def load_recorded_ticket_channel_ids() -> set[str]:
    """读取已处理工单 ID，确保程序重启或电脑唤醒后不会重复提醒。"""
    if not TICKET_EVENT_FILE.exists():
        return set()

    channel_ids: set[str] = set()
    for raw_line in TICKET_EVENT_FILE.read_text(encoding="utf-8").splitlines():
        try:
            event = json.loads(raw_line)
        except json.JSONDecodeError:
            continue
        channel_id = str(event.get("channel_id") or "").strip()
        if channel_id:
            channel_ids.add(channel_id)
    return channel_ids


def load_recorded_ticket_guild_ids() -> set[str]:
    """从既有工单记录识别工单所属服务器，避免依赖旧频道配置。"""
    if not TICKET_EVENT_FILE.exists():
        return set()

    guild_ids: set[str] = set()
    for raw_line in TICKET_EVENT_FILE.read_text(encoding="utf-8").splitlines():
        try:
            event = json.loads(raw_line)
        except json.JSONDecodeError:
            continue
        guild_id = str(event.get("guild_id") or "").strip()
        category_id = str(event.get("category_id") or "").strip()
        if guild_id.isdigit() and category_id:
            guild_ids.add(guild_id)
    return guild_ids


def load_pending_message_alerts() -> dict[str, dict[str, Any]]:
    """恢复尚未到期的聊天提醒；只保存 ID 和时间，不保存消息正文。"""
    if not PENDING_MESSAGE_FILE.exists():
        return {}
    try:
        raw_data = json.loads(PENDING_MESSAGE_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return {}
    if not isinstance(raw_data, dict):
        return {}

    pending: dict[str, dict[str, Any]] = {}
    for key, raw_item in raw_data.items():
        if not isinstance(raw_item, dict):
            continue
        channel_id = str(raw_item.get("channel_id") or "")
        guild_id = str(raw_item.get("guild_id") or "")
        user_id = str(raw_item.get("user_id") or "")
        first_message_id = str(raw_item.get("first_message_id") or "")
        message_ids = [
            str(message_id)
            for message_id in (raw_item.get("message_ids") or [])
            if str(message_id).isdigit()
        ]
        try:
            due_at = float(raw_item.get("due_at"))
            next_attempt_at = float(raw_item.get("next_attempt_at") or 0)
        except (TypeError, ValueError):
            continue
        if not (
            channel_id.isdigit()
            and guild_id.isdigit()
            and user_id.isdigit()
            and first_message_id.isdigit()
            and message_ids
        ):
            continue
        pending[str(key)] = {
            "channel_id": channel_id,
            "guild_id": guild_id,
            "user_id": user_id,
            "first_message_id": first_message_id,
            "message_ids": list(dict.fromkeys(message_ids)),
            "due_at": due_at,
            "next_attempt_at": next_attempt_at,
        }
    return pending


def load_help_message_state() -> dict[str, Any]:
    """恢复 help 频道扫描游标和未发送的汇总消息队列。"""
    empty_state: dict[str, Any] = {
        "last_seen_message_id": "",
        "collection_outbox": {},
    }
    if not HELP_MESSAGE_STATE_FILE.exists():
        return empty_state
    try:
        raw_state = json.loads(HELP_MESSAGE_STATE_FILE.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return empty_state
    if not isinstance(raw_state, dict):
        return empty_state

    last_seen_message_id = str(raw_state.get("last_seen_message_id") or "")
    if last_seen_message_id and not last_seen_message_id.isdigit():
        last_seen_message_id = ""

    raw_outbox = raw_state.get("collection_outbox")
    collection_outbox: dict[str, dict[str, Any]] = {}
    if isinstance(raw_outbox, dict):
        for message_id, raw_item in raw_outbox.items():
            message_id = str(message_id)
            if not message_id.isdigit() or not isinstance(raw_item, dict):
                continue
            channel_id = str(raw_item.get("channel_id") or "")
            guild_id = str(raw_item.get("guild_id") or "")
            try:
                next_attempt_at = float(raw_item.get("next_attempt_at") or 0)
            except (TypeError, ValueError):
                next_attempt_at = 0.0
            if channel_id.isdigit() and guild_id.isdigit():
                collection_outbox[message_id] = {
                    "channel_id": channel_id,
                    "guild_id": guild_id,
                    "next_attempt_at": next_attempt_at,
                }

    return {
        "last_seen_message_id": last_seen_message_id,
        "collection_outbox": collection_outbox,
    }


def member_has_any_role(payload: dict[str, Any], role_ids: set[str]) -> bool:
    """用户拥有给定集合中的任意一个身份组时返回 True。"""
    if not role_ids:
        return False
    member = payload.get("member") or {}
    member_role_ids = {str(role_id) for role_id in (member.get("roles") or [])}
    return not member_role_ids.isdisjoint(role_ids)


def member_has_allowed_role(payload: dict[str, Any], allowed_role_ids: set[str]) -> bool:
    """未设置白名单时全部放行；设置后只放行拥有任一指定身份组的成员。"""
    return not allowed_role_ids or member_has_any_role(payload, allowed_role_ids)


def member_passes_role_filters(
    payload: dict[str, Any],
    allowed_role_ids: set[str],
    excluded_role_ids: set[str],
) -> bool:
    """排除身份组优先；未被排除后再检查允许身份组。"""
    return (
        not member_has_any_role(payload, excluded_role_ids)
        and member_has_allowed_role(payload, allowed_role_ids)
    )


def staff_response_matches_pending(
    payload: dict[str, Any],
    pending: dict[str, Any],
    reply_role_ids: set[str],
) -> bool:
    """指定回复身份组直接回复或 @ 对应用户时，视为已响应。"""
    if not member_has_any_role(payload, reply_role_ids):
        return False

    referenced_message_id = str(
        (payload.get("message_reference") or {}).get("message_id") or ""
    )
    pending_message_ids = {
        str(message_id) for message_id in (pending.get("message_ids") or [])
    }
    if referenced_message_id and referenced_message_id in pending_message_ids:
        return True

    mentioned_user_ids = {
        str(mention.get("id") or "")
        for mention in (payload.get("mentions") or [])
        if isinstance(mention, dict)
    }
    return str(pending.get("user_id") or "") in mentioned_user_ids


def build_alert(payload: dict[str, Any]) -> str:
    """把 Discord MESSAGE_CREATE 事件整理成适合 Telegram 阅读的提醒。"""
    author = payload.get("author") or {}
    member = payload.get("member") or {}
    display_name = member.get("nick") or author.get("global_name") or author.get("username") or "未知用户"
    username = author.get("username") or "unknown"

    content = (payload.get("content") or "").strip()
    if not content:
        content = "（无文字内容）"

    attachments = payload.get("attachments") or []
    attachment_lines = []
    for item in attachments[:5]:
        filename = item.get("filename") or "附件"
        url = item.get("url") or ""
        attachment_lines.append(f"• {filename}: {url}" if url else f"• {filename}")

    guild_id = payload.get("guild_id") or "@me"
    channel_id = payload.get("channel_id") or ""
    message_id = payload.get("id") or ""
    message_url = f"https://discord.com/channels/{guild_id}/{channel_id}/{message_id}"

    lines = [
        "🔔 Discord 指定频道有新消息",
        f"用户：{display_name} (@{username})",
        "",
        content,
    ]
    if attachment_lines:
        lines.extend(["", "附件：", *attachment_lines])
    message_link = f"[点击进入 Discord]({message_url})"
    lines.extend(["", f"打开消息：{message_link}"])

    # Telegram 单条消息上限约 4096 字符，预留少量空间避免发送失败。
    result = "\n".join(lines)
    if len(result) > 3900:
        result = (
            result[:3820]
            + "\n\n（内容过长，已截断）\n"
            + f"打开消息：{message_link}"
        )
    return result


def build_help_collection_alert(payload: dict[str, Any]) -> str:
    """生成精简的 Telegram help 消息汇总，只保留用户名和正文。"""
    author = payload.get("author") or {}
    member = payload.get("member") or {}
    display_name = (
        member.get("nick")
        or author.get("global_name")
        or author.get("username")
        or "未知用户"
    )
    username = author.get("username") or ""
    sender = (
        display_name
        if not username or display_name == username
        else f"{display_name} (@{username})"
    )

    content = (payload.get("content") or "").strip()
    if not content:
        content = "（无文字内容）"

    result = f"用户：{sender}\n消息：{content}"
    if len(result) > 3900:
        result = result[:3888] + "…（内容过长，已截断）"
    return result


def build_delayed_alert(
    payload: dict[str, Any],
    *,
    delay_seconds: float,
    message_count: int,
) -> str:
    """生成“等待工作人员回复后仍未处理”的 Telegram 提醒。"""
    alert = build_alert(payload)
    delay_minutes = max(1, round(delay_seconds / 60))
    status_lines = [
        "",
        f"⏳ 状态：已等待 {delay_minutes} 分钟，尚未检测到工作人员回复。",
    ]
    if message_count > 1:
        status_lines.append(f"该用户等待期间共发送了 {message_count} 条消息，已合并提醒。")
    status_text = "\n".join(status_lines)
    return (
        f"{MESSAGE_SEPARATOR}\n"
        f"{alert}\n"
        f"{status_text}\n"
        f"{MESSAGE_SEPARATOR}"
    )


def discord_snowflake_time(snowflake: str) -> datetime:
    """从 Discord Snowflake ID 计算创建时间。"""
    timestamp_ms = (int(snowflake) >> 22) + 1420070400000
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=ZoneInfo("Asia/Shanghai"))


def build_ticket_alert(
    payload: dict[str, Any],
    route: dict[str, str],
    *,
    catch_up: bool = False,
) -> str:
    """把 Discord 新工单事件整理成 Telegram 提醒。"""
    guild_id = str(payload.get("guild_id") or "")
    channel_id = str(payload.get("id") or "")
    channel_name = str(payload.get("name") or "未知频道")
    channel_url = f"https://discord.com/channels/{guild_id}/{channel_id}"
    try:
        created_at = discord_snowflake_time(channel_id).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OverflowError):
        created_at = "未知"

    route_label = route["label"]
    normalized_label = route_label.casefold()
    if "support" in normalized_label:
        ticket_name = "问题工单"
        ticket_icon = "🛠️"
    elif "collab" in normalized_label:
        ticket_name = "合作工单"
        ticket_icon = "🤝"
    else:
        ticket_name = route_label
        ticket_icon = "🎫"

    title = (
        f"🕒 补发｜{ticket_name}"
        if catch_up
        else f"{ticket_icon} 新{ticket_name}"
    )
    lines = [
        MESSAGE_SEPARATOR,
        f"{title}｜#{channel_name}",
        f"🕒 {created_at}",
    ]
    if route.get("owner"):
        lines.append(f"👤 负责人：{route['owner']}")
    if catch_up:
        lines.extend(
            [
                "",
                "该工单在电脑休眠或监听离线期间创建，现已自动补发。",
            ]
        )
    lines.extend(
        [
            "",
            f"🔗 [打开 Discord 工单]({channel_url})",
            MESSAGE_SEPARATOR,
        ]
    )
    return "\n".join(lines)


def record_ticket_event(
    payload: dict[str, Any],
    route: dict[str, str],
    *,
    detection_source: str = "gateway",
) -> None:
    """只记录汇总需要的最小字段，不保存用户消息正文。"""
    channel_id = str(payload.get("id") or "")
    try:
        created_at = discord_snowflake_time(channel_id).isoformat()
    except (TypeError, ValueError, OverflowError):
        created_at = datetime.now(tz=ZoneInfo("Asia/Shanghai")).isoformat()

    event = {
        "channel_id": channel_id,
        "guild_id": str(payload.get("guild_id") or ""),
        "channel_name": str(payload.get("name") or ""),
        "category_id": str(payload.get("parent_id") or ""),
        "ticket_type": route["label"],
        "owner": route.get("owner", ""),
        "created_at": created_at,
        "detection_source": detection_source,
    }
    TICKET_EVENT_FILE.parent.mkdir(parents=True, exist_ok=True)
    with TICKET_EVENT_FILE.open("a", encoding="utf-8") as file_handle:
        file_handle.write(json.dumps(event, ensure_ascii=False) + "\n")


async def send_via_hermes(target: str, message: str, attempts: int = 3) -> None:
    """调用 Hermes 主动推送；网络短暂失败时自动重试。"""
    last_error = "未知错误"
    for attempt in range(1, attempts + 1):
        process = await asyncio.create_subprocess_exec(
            str(require_hermes_bin()),
            "send",
            "--to",
            target,
            "--quiet",
            message,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            _, stderr = await asyncio.wait_for(process.communicate(), timeout=60)
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            stderr = b"Hermes send timed out after 60 seconds"
        if process.returncode == 0:
            return

        last_error = stderr.decode("utf-8", errors="replace").strip() or f"退出码 {process.returncode}"
        if attempt < attempts:
            await asyncio.sleep(2 * attempt)

    raise RuntimeError(f"Hermes 推送失败：{last_error}")


async def resolve_hermes_target(target: str) -> str:
    """当只写 telegram 时，自动选择当前唯一的 Telegram 私聊。"""
    if target != "telegram":
        return target

    process = await asyncio.create_subprocess_exec(
        str(require_hermes_bin()),
        "send",
        "--list",
        "telegram",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, _ = await asyncio.wait_for(process.communicate(), timeout=15)
    except asyncio.TimeoutError:
        process.kill()
        await process.wait()
        return target

    if process.returncode != 0:
        return target

    text_output = stdout.decode("utf-8", errors="replace")
    chat_ids = list(dict.fromkeys(re.findall(r"\[(-?\d+)\]", text_output)))
    if len(chat_ids) == 1:
        return f"telegram:{chat_ids[0]}"
    return target


class DiscordChannelMonitor:
    def __init__(self, config: dict[str, str]) -> None:
        self.token = require_config(config, "DISCORD_MONITOR_BOT_TOKEN")
        self.channel_id = require_config(config, "DISCORD_MONITOR_CHANNEL_ID")
        if not self.channel_id.isdigit():
            raise RuntimeError("DISCORD_MONITOR_CHANNEL_ID 必须是纯数字频道 ID。")

        self.telegram_target = config.get("HERMES_NOTIFY_TARGET", "telegram").strip() or "telegram"
        self.help_collection_target = config.get(
            "HERMES_HELP_COLLECTION_TARGET",
            "",
        ).strip()
        self.ticket_default_target = (
            config.get("HERMES_TICKET_NOTIFY_TARGET", self.telegram_target).strip()
            or self.telegram_target
        )
        self.ticket_routes = load_ticket_routes(config)
        self.ticket_guild_id = config.get("DISCORD_TICKET_GUILD_ID", "").strip()
        if self.ticket_guild_id and not self.ticket_guild_id.isdigit():
            raise RuntimeError("DISCORD_TICKET_GUILD_ID 必须是纯数字服务器 ID。")
        try:
            configured_interval = float(
                config.get(
                    "TICKET_RECONCILE_INTERVAL_SECONDS",
                    str(DEFAULT_TICKET_RECONCILE_INTERVAL_SECONDS),
                )
            )
        except ValueError as exc:
            raise RuntimeError("TICKET_RECONCILE_INTERVAL_SECONDS 必须是数字。") from exc
        self.ticket_reconcile_interval_seconds = max(30.0, configured_interval)
        try:
            configured_message_delay = float(
                config.get(
                    "DISCORD_MESSAGE_NOTIFY_DELAY_SECONDS",
                    str(DEFAULT_MESSAGE_NOTIFY_DELAY_SECONDS),
                )
            )
        except ValueError as exc:
            raise RuntimeError("DISCORD_MESSAGE_NOTIFY_DELAY_SECONDS 必须是数字。") from exc
        self.message_notify_delay_seconds = max(30.0, configured_message_delay)
        try:
            configured_help_reconcile_interval = float(
                config.get(
                    "HELP_MESSAGE_RECONCILE_INTERVAL_SECONDS",
                    str(DEFAULT_HELP_MESSAGE_RECONCILE_INTERVAL_SECONDS),
                )
            )
        except ValueError as exc:
            raise RuntimeError("HELP_MESSAGE_RECONCILE_INTERVAL_SECONDS 必须是数字。") from exc
        self.help_message_reconcile_interval_seconds = max(
            15.0,
            configured_help_reconcile_interval,
        )
        self.allowed_role_ids = parse_id_set(
            config.get("DISCORD_MONITOR_ROLE_IDS"),
            "DISCORD_MONITOR_ROLE_IDS",
        )
        self.excluded_role_ids = parse_id_set(
            config.get("DISCORD_MONITOR_EXCLUDED_ROLE_IDS"),
            "DISCORD_MONITOR_EXCLUDED_ROLE_IDS",
        )
        self.reply_role_ids = parse_id_set(
            config.get("DISCORD_MONITOR_REPLY_ROLE_IDS")
            or config.get("DISCORD_MONITOR_EXCLUDED_ROLE_IDS"),
            "DISCORD_MONITOR_REPLY_ROLE_IDS",
        )
        self.notify_bot_messages = env_bool(config.get("NOTIFY_BOT_MESSAGES"), default=False)
        self.send_startup_notice = env_bool(config.get("SEND_STARTUP_NOTICE"), default=True)
        self.last_sequence: int | None = None
        self.session_id: str | None = None
        self.resume_gateway_url: str | None = None
        self.self_user_id: str | None = None
        self.help_guild_id: str = ""
        self.stop_event = asyncio.Event()
        self.heartbeat_ack_event = asyncio.Event()
        self.notification_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue(maxsize=1000)
        self.recent_message_ids: deque[str] = deque(maxlen=500)
        self.recent_message_id_set: set[str] = set()
        self.recent_ticket_channel_ids: deque[str] = deque(maxlen=500)
        self.recent_ticket_channel_id_set: set[str] = set()
        self.recorded_ticket_channel_ids = load_recorded_ticket_channel_ids()
        self.pending_message_alerts = load_pending_message_alerts()
        self.pending_message_lock = asyncio.Lock()
        self.help_message_state = load_help_message_state()
        self.help_message_lock = asyncio.Lock()
        self.help_reconciliation_requested = asyncio.Event()
        self.ticket_reconciliation_requested = asyncio.Event()
        self.hermes_send_lock = asyncio.Lock()

    def remember_message(self, message_id: str) -> bool:
        """返回 True 表示是新消息；避免重连时重复通知。"""
        if not message_id or message_id in self.recent_message_id_set:
            return False
        if len(self.recent_message_ids) == self.recent_message_ids.maxlen:
            oldest = self.recent_message_ids.popleft()
            self.recent_message_id_set.discard(oldest)
        self.recent_message_ids.append(message_id)
        self.recent_message_id_set.add(message_id)
        return True

    def remember_ticket_channel(self, channel_id: str) -> bool:
        """返回 True 表示该工单频道尚未提醒过。"""
        if (
            not channel_id
            or channel_id in self.recorded_ticket_channel_ids
            or channel_id in self.recent_ticket_channel_id_set
        ):
            return False
        if len(self.recent_ticket_channel_ids) == self.recent_ticket_channel_ids.maxlen:
            oldest = self.recent_ticket_channel_ids.popleft()
            self.recent_ticket_channel_id_set.discard(oldest)
        self.recent_ticket_channel_ids.append(channel_id)
        self.recent_ticket_channel_id_set.add(channel_id)
        return True

    def forget_recent_ticket_channel(self, channel_id: str) -> None:
        """补漏通知失败时解除临时占用，允许下一轮继续重试。"""
        self.recent_ticket_channel_id_set.discard(channel_id)
        with contextlib.suppress(ValueError):
            self.recent_ticket_channel_ids.remove(channel_id)

    def save_pending_message_alerts(self) -> None:
        """原子保存待确认消息；内容不落盘，仅保存 Discord ID 和到期时间。"""
        PENDING_MESSAGE_FILE.parent.mkdir(parents=True, exist_ok=True)
        temporary_file = PENDING_MESSAGE_FILE.with_suffix(".json.tmp")
        temporary_file.write_text(
            json.dumps(self.pending_message_alerts, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temporary_file.chmod(0o600)
        temporary_file.replace(PENDING_MESSAGE_FILE)

    async def queue_pending_message_alert(self, payload: dict[str, Any]) -> None:
        """按用户合并消息，并从第一条消息开始计算五分钟等待时间。"""
        channel_id = str(payload.get("channel_id") or "")
        guild_id = str(payload.get("guild_id") or "")
        user_id = str((payload.get("author") or {}).get("id") or "")
        message_id = str(payload.get("id") or "")
        if not all(item.isdigit() for item in (channel_id, guild_id, user_id, message_id)):
            raise RuntimeError("待确认消息缺少有效的 Discord ID。")

        pending_key = f"{channel_id}:{user_id}"
        async with self.pending_message_lock:
            pending = self.pending_message_alerts.get(pending_key)
            if pending:
                message_ids = pending.setdefault("message_ids", [])
                if message_id not in message_ids:
                    message_ids.append(message_id)
            else:
                try:
                    message_created_at = discord_snowflake_time(message_id).timestamp()
                except (TypeError, ValueError, OverflowError):
                    message_created_at = time.time()
                pending = {
                    "channel_id": channel_id,
                    "guild_id": guild_id,
                    "user_id": user_id,
                    "first_message_id": message_id,
                    "message_ids": [message_id],
                    "due_at": message_created_at + self.message_notify_delay_seconds,
                    "next_attempt_at": 0.0,
                }
                self.pending_message_alerts[pending_key] = pending
            self.save_pending_message_alerts()
            message_count = len(pending["message_ids"])

        delay_minutes = max(1, round(self.message_notify_delay_seconds / 60))
        print(
            f"消息进入 {delay_minutes} 分钟待确认队列："
            f"Discord 消息 {message_id} / 已合并 {message_count} 条",
            flush=True,
        )

    async def cancel_pending_from_staff_response(
        self,
        payload: dict[str, Any],
    ) -> None:
        """工作人员直接回复或 @ 用户时，取消对应的待确认通知。"""
        cancelled_keys: list[str] = []
        async with self.pending_message_lock:
            for pending_key, pending in list(self.pending_message_alerts.items()):
                if staff_response_matches_pending(
                    payload,
                    pending,
                    self.reply_role_ids,
                ):
                    cancelled_keys.append(pending_key)
                    self.pending_message_alerts.pop(pending_key, None)
            if cancelled_keys:
                self.save_pending_message_alerts()

        for pending_key in cancelled_keys:
            print(
                f"工作人员已回复，取消 Telegram 提醒：{pending_key}",
                flush=True,
            )

    async def has_staff_response_via_api(
        self,
        session: aiohttp.ClientSession,
        pending: dict[str, Any],
    ) -> bool:
        """发送前检查 Discord 历史，弥补休眠或短暂断线期间的回复事件。"""
        channel_id = str(pending["channel_id"])
        after_message_id = str(pending["first_message_id"])

        for _ in range(10):
            messages = await self.discord_api_get(
                session,
                f"/channels/{channel_id}/messages?after={after_message_id}&limit=100",
            )
            if not isinstance(messages, list):
                raise RuntimeError("Discord 消息复查返回了意外的数据格式。")
            if any(
                staff_response_matches_pending(
                    message,
                    pending,
                    self.reply_role_ids,
                )
                for message in messages
                if isinstance(message, dict)
            ):
                return True
            if len(messages) < 100:
                return False
            next_after = max(
                (str(message.get("id") or "0") for message in messages),
                key=int,
            )
            if next_after == after_message_id:
                return False
            after_message_id = next_after
        return False

    async def pending_message_notification_loop(
        self,
        session: aiohttp.ClientSession,
    ) -> None:
        """到期后复查工作人员回复；无人回复才发送 Telegram。"""
        while not self.stop_event.is_set():
            now = time.time()
            async with self.pending_message_lock:
                due_items = [
                    (pending_key, dict(pending))
                    for pending_key, pending in self.pending_message_alerts.items()
                    if float(pending.get("due_at") or 0) <= now
                    and float(pending.get("next_attempt_at") or 0) <= now
                ]

            for pending_key, pending in due_items:
                try:
                    if await self.has_staff_response_via_api(session, pending):
                        async with self.pending_message_lock:
                            current = self.pending_message_alerts.get(pending_key)
                            if current and current.get("first_message_id") == pending.get(
                                "first_message_id"
                            ):
                                self.pending_message_alerts.pop(pending_key, None)
                                self.save_pending_message_alerts()
                        print(
                            f"复查到工作人员已回复，取消 Telegram 提醒：{pending_key}",
                            flush=True,
                        )
                        continue

                    first_message = await self.discord_api_get(
                        session,
                        f"/channels/{pending['channel_id']}/messages/"
                        f"{pending['first_message_id']}",
                    )
                    if not isinstance(first_message, dict):
                        raise RuntimeError("Discord 原始消息返回了意外的数据格式。")
                    first_message["guild_id"] = pending["guild_id"]

                    async with self.pending_message_lock:
                        current = self.pending_message_alerts.get(pending_key)
                        if not current or current.get("first_message_id") != pending.get(
                            "first_message_id"
                        ):
                            continue
                        alert = build_delayed_alert(
                            first_message,
                            delay_seconds=self.message_notify_delay_seconds,
                            message_count=len(current.get("message_ids") or []),
                        )

                    async with self.hermes_send_lock:
                        await send_via_hermes(self.telegram_target, alert, attempts=1)

                    async with self.pending_message_lock:
                        current = self.pending_message_alerts.get(pending_key)
                        if current and current.get("first_message_id") == pending.get(
                            "first_message_id"
                        ):
                            self.pending_message_alerts.pop(pending_key, None)
                            self.save_pending_message_alerts()
                    print(
                        f"等待期结束且无人回复，已发送 Telegram：{pending_key}",
                        flush=True,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    async with self.pending_message_lock:
                        current = self.pending_message_alerts.get(pending_key)
                        if current:
                            if "HTTP 404" in str(exc):
                                self.pending_message_alerts.pop(pending_key, None)
                            else:
                                current["next_attempt_at"] = time.time() + 60
                            self.save_pending_message_alerts()
                    print(
                        f"延迟消息处理失败：{pending_key} / {exc}",
                        file=sys.stderr,
                        flush=True,
                    )

            try:
                await asyncio.wait_for(
                    self.stop_event.wait(),
                    timeout=PENDING_MESSAGE_CHECK_INTERVAL_SECONDS,
                )
            except asyncio.TimeoutError:
                pass

    def save_help_message_state(self) -> None:
        """原子保存 help 扫描游标和汇总发送队列。"""
        HELP_MESSAGE_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        temporary_file = HELP_MESSAGE_STATE_FILE.with_suffix(".json.tmp")
        temporary_file.write_text(
            json.dumps(self.help_message_state, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temporary_file.chmod(0o600)
        temporary_file.replace(HELP_MESSAGE_STATE_FILE)

    async def process_help_message(self, payload: dict[str, Any]) -> None:
        """处理 help 消息：立即汇总、延迟提醒，或根据工作人员回复取消。"""
        if str(payload.get("channel_id") or "") != self.channel_id:
            return

        message_id = str(payload.get("id") or "")
        if not message_id.isdigit():
            return

        async with self.help_message_lock:
            last_seen_message_id = str(
                self.help_message_state.get("last_seen_message_id") or ""
            )
            if (
                last_seen_message_id.isdigit()
                and int(message_id) <= int(last_seen_message_id)
            ):
                return

            author = payload.get("author") or {}
            author_id = str(author.get("id") or "")
            guild_id = str(payload.get("guild_id") or "")
            if guild_id.isdigit():
                self.help_guild_id = guild_id

            if self.self_user_id and author_id == self.self_user_id:
                pass
            elif author.get("bot") and not self.notify_bot_messages:
                pass
            elif member_has_any_role(payload, self.reply_role_ids):
                await self.cancel_pending_from_staff_response(payload)
            elif member_has_any_role(payload, self.excluded_role_ids):
                pass
            elif member_has_allowed_role(payload, self.allowed_role_ids):
                if self.help_collection_target:
                    collection_outbox = self.help_message_state.setdefault(
                        "collection_outbox",
                        {},
                    )
                    collection_outbox[message_id] = {
                        "channel_id": self.channel_id,
                        "guild_id": guild_id,
                        "next_attempt_at": 0.0,
                    }
                await self.queue_pending_message_alert(payload)

            self.help_message_state["last_seen_message_id"] = message_id
            self.save_help_message_state()

    async def resolve_help_guild_id(self, session: aiohttp.ClientSession) -> str:
        """从 help 频道读取所属服务器 ID，并在进程内缓存。"""
        if self.help_guild_id:
            return self.help_guild_id
        channel = await self.discord_api_get(session, f"/channels/{self.channel_id}")
        guild_id = str((channel or {}).get("guild_id") or "")
        if not guild_id.isdigit():
            raise RuntimeError("无法从 help 频道识别 Discord 服务器 ID。")
        self.help_guild_id = guild_id
        return guild_id

    async def reconcile_help_messages(
        self,
        session: aiohttp.ClientSession,
    ) -> None:
        """补收 Gateway 断线或电脑休眠期间的 help 频道消息。"""
        async with self.help_message_lock:
            last_seen_message_id = str(
                self.help_message_state.get("last_seen_message_id") or ""
            )

        if not last_seen_message_id:
            latest_messages = await self.discord_api_get(
                session,
                f"/channels/{self.channel_id}/messages?limit=1",
            )
            baseline_message_id = ""
            if isinstance(latest_messages, list) and latest_messages:
                baseline_message_id = str(latest_messages[0].get("id") or "")
            async with self.help_message_lock:
                if not self.help_message_state.get("last_seen_message_id"):
                    self.help_message_state["last_seen_message_id"] = baseline_message_id
                    self.save_help_message_state()
            print(
                f"Help 消息汇总基线已建立：{baseline_message_id or '频道暂无消息'}",
                flush=True,
            )
            return

        guild_id = await self.resolve_help_guild_id(session)
        after_message_id = last_seen_message_id
        unseen_messages: dict[str, dict[str, Any]] = {}
        for _ in range(10):
            messages = await self.discord_api_get(
                session,
                f"/channels/{self.channel_id}/messages?"
                f"after={after_message_id}&limit=100",
            )
            if not isinstance(messages, list):
                raise RuntimeError("Help 消息补收返回了意外的数据格式。")
            for message in messages:
                if not isinstance(message, dict):
                    continue
                message_id = str(message.get("id") or "")
                if message_id.isdigit():
                    message["guild_id"] = str(message.get("guild_id") or guild_id)
                    unseen_messages[message_id] = message
            if len(messages) < 100:
                break
            next_after = max(
                (str(message.get("id") or "0") for message in messages),
                key=int,
            )
            if next_after == after_message_id:
                break
            after_message_id = next_after

        for message_id in sorted(unseen_messages, key=int):
            await self.process_help_message(unseen_messages[message_id])

    async def help_message_reconciliation_loop(
        self,
        session: aiohttp.ClientSession,
    ) -> None:
        """定时补收，并在 Discord 重连后立即补收。"""
        while not self.stop_event.is_set():
            try:
                await self.reconcile_help_messages(session)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"Help 消息补收失败：{exc}", file=sys.stderr, flush=True)

            try:
                await asyncio.wait_for(
                    self.help_reconciliation_requested.wait(),
                    timeout=self.help_message_reconcile_interval_seconds,
                )
            except asyncio.TimeoutError:
                pass
            self.help_reconciliation_requested.clear()

    async def help_collection_outbox_loop(
        self,
        session: aiohttp.ClientSession,
    ) -> None:
        """可靠发送 help 汇总；失败时保留到本地并自动重试。"""
        while not self.stop_event.is_set():
            now = time.time()
            async with self.help_message_lock:
                outbox_items = [
                    (message_id, dict(item))
                    for message_id, item in (
                        self.help_message_state.get("collection_outbox") or {}
                    ).items()
                    if float(item.get("next_attempt_at") or 0) <= now
                ]

            for message_id, item in outbox_items:
                try:
                    message = await self.discord_api_get(
                        session,
                        f"/channels/{item['channel_id']}/messages/{message_id}",
                    )
                    if not isinstance(message, dict):
                        raise RuntimeError("Help 汇总原始消息返回了意外的数据格式。")
                    message["guild_id"] = str(
                        message.get("guild_id") or item["guild_id"]
                    )
                    alert = build_help_collection_alert(message)
                    async with self.hermes_send_lock:
                        await send_via_hermes(
                            self.help_collection_target,
                            alert,
                            attempts=1,
                        )
                    async with self.help_message_lock:
                        outbox = self.help_message_state.get("collection_outbox") or {}
                        outbox.pop(message_id, None)
                        self.save_help_message_state()
                    print(
                        f"Help 消息已发送到汇总话题：{message_id}",
                        flush=True,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    async with self.help_message_lock:
                        outbox = self.help_message_state.get("collection_outbox") or {}
                        current = outbox.get(message_id)
                        if current:
                            if "HTTP 404" in str(exc):
                                outbox.pop(message_id, None)
                            else:
                                current["next_attempt_at"] = time.time() + 60
                            self.save_help_message_state()
                    print(
                        f"Help 汇总发送失败：{message_id} / {exc}",
                        file=sys.stderr,
                        flush=True,
                    )

            try:
                await asyncio.wait_for(
                    self.stop_event.wait(),
                    timeout=HELP_COLLECTION_CHECK_INTERVAL_SECONDS,
                )
            except asyncio.TimeoutError:
                pass

    async def get_gateway_url(self, session: aiohttp.ClientSession) -> str:
        headers = {"Authorization": f"Bot {self.token}"}
        async with session.get(f"{DISCORD_API_BASE}/gateway/bot", headers=headers, timeout=20) as response:
            if response.status == 401:
                raise RuntimeError("Discord Bot Token 无效，请重新复制 Token。")
            if response.status != 200:
                body = await response.text()
                raise RuntimeError(f"Discord Gateway 查询失败：HTTP {response.status} {body[:200]}")
            data = await response.json()
            return str(data["url"])

    async def discord_api_get(
        self,
        session: aiohttp.ClientSession,
        path: str,
    ) -> Any:
        """调用 Discord REST API；用于电脑唤醒后的工单补漏扫描。"""
        headers = {"Authorization": f"Bot {self.token}"}
        async with session.get(
            f"{DISCORD_API_BASE}{path}",
            headers=headers,
            timeout=20,
        ) as response:
            if response.status == 401:
                raise RuntimeError("Discord Bot Token 无效，请重新复制 Token。")
            if response.status != 200:
                body = await response.text()
                raise RuntimeError(
                    f"Discord API 查询失败：HTTP {response.status} {body[:200]}"
                )
            return await response.json()

    async def resolve_ticket_guild_id(self, session: aiohttp.ClientSession) -> str:
        """优先使用配置/历史记录；否则按工单分类自动识别服务器。"""
        if self.ticket_guild_id:
            return self.ticket_guild_id

        recorded_guild_ids = load_recorded_ticket_guild_ids()
        if len(recorded_guild_ids) == 1:
            self.ticket_guild_id = next(iter(recorded_guild_ids))
            return self.ticket_guild_id

        guilds = await self.discord_api_get(session, "/users/@me/guilds")
        if isinstance(guilds, list):
            route_category_ids = set(self.ticket_routes)
            for guild in guilds:
                guild_id = str((guild or {}).get("id") or "")
                if not guild_id.isdigit():
                    continue
                channels = await self.discord_api_get(
                    session,
                    f"/guilds/{guild_id}/channels",
                )
                if any(
                    str((channel or {}).get("id") or "") in route_category_ids
                    for channel in channels
                    if isinstance(channel, dict)
                ):
                    self.ticket_guild_id = guild_id
                    return guild_id

        raise RuntimeError("无法根据 Support/Collaborate 分类识别 Discord 服务器 ID。")

    async def reconcile_ticket_channels(self, session: aiohttp.ClientSession) -> None:
        """扫描现存工单，补发休眠或断线期间遗漏的提醒。"""
        if not self.ticket_routes:
            return

        guild_id = await self.resolve_ticket_guild_id(session)
        channels = await self.discord_api_get(session, f"/guilds/{guild_id}/channels")
        if not isinstance(channels, list):
            raise RuntimeError("Discord 工单补漏扫描返回了意外的数据格式。")

        candidates = [
            channel
            for channel in channels
            if isinstance(channel, dict)
            and int(channel.get("type", -1)) in {0, 5, 11, 12}
            and str(channel.get("parent_id") or "") in self.ticket_routes
        ]
        candidates.sort(key=lambda channel: int(str(channel.get("id") or "0")))

        for payload in candidates:
            channel_id = str(payload.get("id") or "")
            if not self.remember_ticket_channel(channel_id):
                continue

            payload["guild_id"] = guild_id
            route = self.ticket_routes[str(payload.get("parent_id") or "")]
            target = route.get("target") or self.ticket_default_target
            alert = build_ticket_alert(payload, route, catch_up=True)
            try:
                # 补漏发送失败时不落盘，下一轮扫描会自动重试。
                async with self.hermes_send_lock:
                    await send_via_hermes(target, alert, attempts=1)
            except Exception:
                self.forget_recent_ticket_channel(channel_id)
                raise

            record_ticket_event(payload, route, detection_source="reconciliation")
            self.recorded_ticket_channel_ids.add(channel_id)
            print(
                f"已补发休眠/离线期间工单：{route['label']} / {channel_id}",
                flush=True,
            )

    async def ticket_reconciliation_loop(
        self,
        session: aiohttp.ClientSession,
    ) -> None:
        """启动时立即扫描；之后定时扫描，并在 Gateway 重连后立即扫描。"""
        while not self.stop_event.is_set():
            try:
                await self.reconcile_ticket_channels(session)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                print(f"工单补漏扫描失败：{exc}", file=sys.stderr, flush=True)

            try:
                await asyncio.wait_for(
                    self.ticket_reconciliation_requested.wait(),
                    timeout=self.ticket_reconcile_interval_seconds,
                )
            except asyncio.TimeoutError:
                pass
            self.ticket_reconciliation_requested.clear()

    async def heartbeat_loop(self, ws: aiohttp.ClientWebSocketResponse, interval_seconds: float) -> None:
        # Discord 要求首个心跳使用随机延迟，避免大量机器人同时发送。
        await asyncio.sleep(interval_seconds * random.random())
        while not self.stop_event.is_set() and not ws.closed:
            cycle_started = asyncio.get_running_loop().time()

            # 必须先清除上一次 ACK，再发送本次心跳。
            # 旧逻辑在发送之后才清除事件，Discord 如果很快返回 ACK，
            # 该 ACK 会被误删，进而造成“未收到心跳确认”的循环重连。
            self.heartbeat_ack_event.clear()
            await ws.send_json({"op": 1, "d": self.last_sequence})
            try:
                await asyncio.wait_for(
                    self.heartbeat_ack_event.wait(),
                    timeout=interval_seconds,
                )
            except asyncio.TimeoutError:
                print("未收到 Discord 心跳确认，主动重连。", file=sys.stderr, flush=True)
                await ws.close(code=4000, message=b"Heartbeat ACK timeout")
                return

            # Discord 的 heartbeat_interval 是两次心跳“开始发送”的间隔。
            # ACK 通常很快返回；剩余时间必须等待，否则会在短时间内连续
            # 发送心跳并触发 Discord 主动要求重新连接。
            elapsed = asyncio.get_running_loop().time() - cycle_started
            remaining = max(0.0, interval_seconds - elapsed)
            if remaining > 0:
                try:
                    await asyncio.wait_for(self.stop_event.wait(), timeout=remaining)
                except asyncio.TimeoutError:
                    pass

    async def notification_worker(self) -> None:
        """单独发送通知，避免 Telegram 网络波动阻塞 Discord 心跳。"""
        while not self.stop_event.is_set():
            target, alert = await self.notification_queue.get()
            try:
                async with self.hermes_send_lock:
                    await send_via_hermes(target, alert)
            except Exception as exc:
                print(f"Hermes 通知发送失败：{exc}", file=sys.stderr, flush=True)
            finally:
                self.notification_queue.task_done()

    async def handle_message_create(self, payload: dict[str, Any]) -> None:
        await self.process_help_message(payload)

    async def handle_ticket_channel(self, payload: dict[str, Any]) -> None:
        """分类命中时，将新工单频道推送到对应 Telegram 目标。"""
        if int(payload.get("type", -1)) not in {0, 5, 11, 12}:
            return

        parent_id = str(payload.get("parent_id") or "")
        route = self.ticket_routes.get(parent_id)
        if not route:
            return

        channel_id = str(payload.get("id") or "")
        if not self.remember_ticket_channel(channel_id):
            return

        target = route.get("target") or self.ticket_default_target
        alert = build_ticket_alert(payload, route)
        try:
            self.notification_queue.put_nowait((target, alert))
            record_ticket_event(payload, route, detection_source="gateway")
            self.recorded_ticket_channel_ids.add(channel_id)
            print(
                f"已捕获新工单并加入 Telegram 通知队列：{route['label']} / {channel_id}",
                flush=True,
            )
        except asyncio.QueueFull:
            self.forget_recent_ticket_channel(channel_id)
            print("通知队列已满，本条工单提醒未能加入。", file=sys.stderr, flush=True)

    async def connect_once(self, session: aiohttp.ClientSession) -> None:
        gateway_url = self.resume_gateway_url or await self.get_gateway_url(session)
        websocket_url = f"{gateway_url.rstrip('/')}/?v=10&encoding=json"

        async with session.ws_connect(websocket_url, heartbeat=None, receive_timeout=None) as ws:
            hello = await ws.receive_json(timeout=30)
            if hello.get("op") != 10:
                raise RuntimeError(f"Discord Gateway 未返回 HELLO：{hello}")

            heartbeat_interval = float(hello["d"]["heartbeat_interval"]) / 1000.0
            heartbeat_task = asyncio.create_task(self.heartbeat_loop(ws, heartbeat_interval))

            if self.session_id and self.last_sequence is not None:
                await ws.send_json(
                    {
                        "op": 6,
                        "d": {
                            "token": self.token,
                            "session_id": self.session_id,
                            "seq": self.last_sequence,
                        },
                    },
                )
            else:
                identify = {
                    "op": 2,
                    "d": {
                        "token": self.token,
                        "intents": GATEWAY_INTENTS,
                        "properties": {
                            "os": "darwin",
                            "browser": "hermes-dc-monitor",
                            "device": "hermes-dc-monitor",
                        },
                    },
                }
                await ws.send_json(identify)

            try:
                async for incoming in ws:
                    if incoming.type == aiohttp.WSMsgType.TEXT:
                        event = json.loads(incoming.data)
                        if event.get("s") is not None:
                            self.last_sequence = int(event["s"])

                        opcode = event.get("op")
                        if opcode == 1:
                            await ws.send_json({"op": 1, "d": self.last_sequence})
                            continue
                        if opcode == 11:
                            self.heartbeat_ack_event.set()
                            continue
                        if opcode == 7:
                            print("Discord 要求重新连接。", flush=True)
                            return
                        if opcode == 9:
                            can_resume = bool(event.get("d"))
                            if not can_resume:
                                self.session_id = None
                                self.resume_gateway_url = None
                                self.last_sequence = None
                            # Discord 对 IDENTIFY 有时间窗口限制。立即重试会持续收到
                            # INVALID_SESSION，因此至少等待超过 5 秒再重新连接。
                            wait_seconds = random.uniform(6.0, 9.0)
                            print(
                                f"Discord 会话无效（可恢复：{can_resume}），"
                                f"等待 {wait_seconds:.1f} 秒后重新连接。",
                                file=sys.stderr,
                                flush=True,
                            )
                            await asyncio.sleep(wait_seconds)
                            return
                        if opcode != 0:
                            continue

                        event_type = event.get("t")
                        data = event.get("d") or {}
                        if event_type == "READY":
                            self.self_user_id = str((data.get("user") or {}).get("id", ""))
                            self.session_id = str(data.get("session_id", "")) or None
                            self.resume_gateway_url = str(data.get("resume_gateway_url", "")) or None
                            self.ticket_reconciliation_requested.set()
                            self.help_reconciliation_requested.set()
                            print("Discord 连接成功，正在监听指定频道。", flush=True)
                        elif event_type == "RESUMED":
                            self.ticket_reconciliation_requested.set()
                            self.help_reconciliation_requested.set()
                            print("Discord 会话已恢复，继续监听。", flush=True)
                        elif event_type == "MESSAGE_CREATE":
                            try:
                                await self.handle_message_create(data)
                            except Exception as exc:  # 单条通知失败不能让长期监听退出
                                print(f"处理消息失败：{exc}", file=sys.stderr, flush=True)
                        elif event_type in {"CHANNEL_CREATE", "CHANNEL_UPDATE", "THREAD_CREATE"}:
                            try:
                                await self.handle_ticket_channel(data)
                            except Exception as exc:
                                print(f"处理工单频道失败：{exc}", file=sys.stderr, flush=True)

                    elif incoming.type in {aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR}:
                        return

                if ws.close_code == 4014:
                    raise RuntimeError(
                        "Discord 拒绝了 Message Content Intent。"
                        "请在 Developer Portal → Bot 中开启 Message Content Intent。"
                    )
            finally:
                heartbeat_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat_task

    async def run(self) -> None:
        self.telegram_target = await resolve_hermes_target(self.telegram_target)
        worker_task = asyncio.create_task(self.notification_worker())
        reconciliation_task: asyncio.Task[None] | None = None
        pending_message_task: asyncio.Task[None] | None = None
        help_reconciliation_task: asyncio.Task[None] | None = None
        help_collection_task: asyncio.Task[None] | None = None

        if self.send_startup_notice:
            delay_minutes = max(1, round(self.message_notify_delay_seconds / 60))
            try:
                self.notification_queue.put_nowait(
                    (
                        self.telegram_target,
                        "✅ Discord 监控已启动。"
                        f"聊天消息会等待 {delay_minutes} 分钟确认，"
                        "工作人员未回复时再通知；"
                        "Help 消息汇总和工单提醒仍然实时发送。",
                    ),
                )
            except asyncio.QueueFull:
                print("启动通知未能加入队列，但监控会继续运行。", file=sys.stderr, flush=True)

        timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_connect=30, sock_read=None)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                reconciliation_task = asyncio.create_task(
                    self.ticket_reconciliation_loop(session)
                )
                pending_message_task = asyncio.create_task(
                    self.pending_message_notification_loop(session)
                )
                help_reconciliation_task = asyncio.create_task(
                    self.help_message_reconciliation_loop(session)
                )
                help_collection_task = asyncio.create_task(
                    self.help_collection_outbox_loop(session)
                )
                try:
                    retry_delay = 2
                    while not self.stop_event.is_set():
                        try:
                            await self.connect_once(session)
                            retry_delay = 2
                        except asyncio.CancelledError:
                            raise
                        except Exception as exc:
                            print(
                                f"连接异常：{exc}；{retry_delay} 秒后重试。",
                                file=sys.stderr,
                                flush=True,
                            )

                        try:
                            await asyncio.wait_for(
                                self.stop_event.wait(),
                                timeout=retry_delay,
                            )
                        except asyncio.TimeoutError:
                            pass
                        retry_delay = min(retry_delay * 2, 60)
                finally:
                    tasks = (
                        help_collection_task,
                        help_reconciliation_task,
                        pending_message_task,
                        reconciliation_task,
                    )
                    for task in tasks:
                        task.cancel()
                    for task in tasks:
                        with contextlib.suppress(asyncio.CancelledError):
                            await task
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task


def self_test() -> None:
    sample = {
        "id": "123456789",
        "guild_id": "987654321",
        "channel_id": "111222333",
        "content": "请问这个商品可以购买吗？",
        "author": {"id": "444555666", "username": "buyer123", "global_name": "Alex", "bot": False},
        "member": {"nick": "Alex"},
        "attachments": [],
    }
    alert = build_alert(sample)
    assert "Alex" in alert
    assert "请问这个商品可以购买吗？" in alert
    assert "https://discord.com/channels/987654321/111222333/123456789" in alert
    collection_alert = build_help_collection_alert(sample)
    assert collection_alert == "用户：Alex (@buyer123)\n消息：请问这个商品可以购买吗？"
    assert "Help 频道消息汇总" not in collection_alert
    assert "https://discord.com/" not in collection_alert
    delayed_alert = build_delayed_alert(
        sample,
        delay_seconds=300,
        message_count=3,
    )
    assert "已等待 5 分钟" in delayed_alert
    assert "共发送了 3 条消息" in delayed_alert
    assert delayed_alert.startswith(MESSAGE_SEPARATOR)
    assert delayed_alert.endswith(MESSAGE_SEPARATOR)
    assert "[点击进入 Discord](https://discord.com/" in delayed_alert
    sample["member"]["roles"] = ["100000000000000001"]
    assert member_has_allowed_role(sample, {"100000000000000001", "100000000000000002"})
    assert not member_has_allowed_role(sample, {"100000000000000003"})
    assert member_has_any_role(sample, {"100000000000000001"})
    assert not member_has_any_role(sample, {"100000000000000003"})
    # 同时拥有“允许”和“排除”身份组时，调用方必须让排除规则优先。
    sample["member"]["roles"] = ["100000000000000001", "100000000000000004"]
    assert member_has_allowed_role(sample, {"100000000000000001"})
    assert member_has_any_role(sample, {"100000000000000004"})
    assert not member_passes_role_filters(
        sample,
        {"100000000000000001"},
        {"100000000000000004"},
    )
    sample["member"]["roles"] = ["100000000000000001"]
    assert member_passes_role_filters(
        sample,
        {"100000000000000001"},
        {"100000000000000004"},
    )
    pending = {
        "user_id": "444555666",
        "message_ids": ["123456789", "123456790"],
    }
    staff_reply = {
        "member": {"roles": ["100000000000000004"]},
        "message_reference": {"message_id": "123456790"},
        "mentions": [],
    }
    assert staff_response_matches_pending(
        staff_reply,
        pending,
        {"100000000000000004"},
    )
    staff_mention = {
        "member": {"roles": ["100000000000000004"]},
        "mentions": [{"id": "444555666"}],
    }
    assert staff_response_matches_pending(
        staff_mention,
        pending,
        {"100000000000000004"},
    )
    unrelated_staff_message = {
        "member": {"roles": ["100000000000000004"]},
        "mentions": [],
    }
    assert not staff_response_matches_pending(
        unrelated_staff_message,
        pending,
        {"100000000000000004"},
    )
    bd_reply = {
        "member": {"roles": ["100000000000000005"]},
        "message_reference": {"message_id": "123456790"},
        "mentions": [],
    }
    assert not staff_response_matches_pending(
        bd_reply,
        pending,
        {"100000000000000006", "100000000000000004"},
    )
    routes = parse_ticket_routes(
        '{"100000000000000007":{"label":"Support ticket",'
        '"target":"telegram:-100123:2","owner":"客服负责人"}}'
    )
    ticket_alert = build_ticket_alert(
        {
            "id": "100000000000000008",
            "guild_id": "100000000000000009",
            "name": "support-buyer123",
            "parent_id": "100000000000000007",
            "type": 0,
        },
        routes["100000000000000007"],
    )
    assert "新问题工单" in ticket_alert
    assert "客服负责人" in ticket_alert
    assert (
        "[打开 Discord 工单](https://discord.com/channels/"
        "100000000000000009/100000000000000008)"
    ) in ticket_alert
    assert ticket_alert.count(MESSAGE_SEPARATOR) == 2
    collab_alert = build_ticket_alert(
        {
            "id": "100000000000000010",
            "guild_id": "100000000000000009",
            "name": "ticket-0010",
            "type": 0,
        },
        {"label": "Collab ticket"},
    )
    assert "新合作工单" in collab_alert
    assert "#ticket-0010" in collab_alert
    catch_up_alert = build_ticket_alert(
        {
            "id": "100000000000000008",
            "guild_id": "100000000000000009",
            "name": "support-buyer123",
            "parent_id": "100000000000000007",
            "type": 0,
        },
        routes["100000000000000007"],
        catch_up=True,
    )
    assert "补发｜问题工单" in catch_up_alert
    assert "电脑休眠或监听离线期间创建" in catch_up_alert
    print("自检通过：消息提醒、实时工单和休眠补漏提醒生成正常。")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="监听 Discord 消息与新工单，并通过 Hermes 推送通知。",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=DEFAULT_ENV_FILE,
        help=f"配置文件路径（默认：{DEFAULT_ENV_FILE}）",
    )
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


async def async_main(args: argparse.Namespace) -> None:
    if args.self_test:
        self_test()
        return

    config = load_env_file(args.env_file.expanduser())
    configure_runtime(config)
    monitor = DiscordChannelMonitor(config)
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, monitor.stop_event.set)
    await monitor.run()


if __name__ == "__main__":
    try:
        asyncio.run(async_main(parse_args()))
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        print(f"启动失败：{exc}", file=sys.stderr)
        sys.exit(1)
