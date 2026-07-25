#!/usr/bin/env python3
"""监听 Discord 消息与新工单，并通过 Hermes 推送到 Telegram。"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import random
import re
import shutil
import signal
import sys
from collections import deque
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp


DEFAULT_ENV_FILE = Path.home() / ".hermes" / "discord-channel-monitor.env"
DEFAULT_STATE_DIR = Path.home() / ".hermes" / "services" / "discord-channel-monitor"
DISCORD_API_BASE = "https://discord.com/api/v10"

# 这些运行路径在读取配置后设置。运行数据始终保存在 Skill 目录之外，
# 避免误提交真实频道信息、日志或统计数据。
HERMES_BIN: Path | None = None
TICKET_EVENT_FILE = DEFAULT_STATE_DIR / "data" / "ticket-events.jsonl"
TICKET_ROUTES_FILE = DEFAULT_STATE_DIR / "ticket-routes.json"

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
    """读取可选路径配置，并安全展开用户目录。"""
    value = config.get(key, "").strip()
    return Path(value).expanduser() if value else default


def configure_runtime(config: dict[str, str]) -> None:
    """设置 Hermes 和运行数据路径，不在 Skill 目录内写入真实数据。"""
    global HERMES_BIN, TICKET_EVENT_FILE, TICKET_ROUTES_FILE

    configured_hermes = config.get("HERMES_BIN", "").strip()
    if configured_hermes:
        hermes_path = Path(configured_hermes).expanduser()
    else:
        discovered = shutil.which("hermes")
        fallback = Path.home() / ".local" / "bin" / "hermes"
        hermes_path = Path(discovered) if discovered else fallback

    if not hermes_path.is_file():
        raise RuntimeError(
            "找不到 Hermes 命令。请安装 Hermes，或在配置中设置 HERMES_BIN。"
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


def require_hermes_bin() -> Path:
    """返回已验证的 Hermes 路径。"""
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


def member_has_allowed_role(payload: dict[str, Any], allowed_role_ids: set[str]) -> bool:
    """未设置白名单时全部放行；设置后只放行拥有任一指定身份组的成员。"""
    if not allowed_role_ids:
        return True
    member = payload.get("member") or {}
    member_role_ids = {str(role_id) for role_id in (member.get("roles") or [])}
    return not member_role_ids.isdisjoint(allowed_role_ids)


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
    lines.extend(["", f"打开消息：{message_url}"])

    # Telegram 单条消息上限约 4096 字符，预留少量空间避免发送失败。
    result = "\n".join(lines)
    if len(result) > 3900:
        result = result[:3820] + "\n\n（内容过长，已截断）\n" + f"打开消息：{message_url}"
    return result


def discord_snowflake_time(snowflake: str) -> datetime:
    """从 Discord Snowflake ID 计算创建时间。"""
    timestamp_ms = (int(snowflake) >> 22) + 1420070400000
    return datetime.fromtimestamp(timestamp_ms / 1000, tz=ZoneInfo("Asia/Shanghai"))


def build_ticket_alert(payload: dict[str, Any], route: dict[str, str]) -> str:
    """把 Discord 新工单事件整理成 Telegram 提醒。"""
    guild_id = str(payload.get("guild_id") or "")
    channel_id = str(payload.get("id") or "")
    channel_name = str(payload.get("name") or "未知频道")
    channel_url = f"https://discord.com/channels/{guild_id}/{channel_id}"
    try:
        created_at = discord_snowflake_time(channel_id).strftime("%Y-%m-%d %H:%M:%S")
    except (TypeError, ValueError, OverflowError):
        created_at = "未知"

    lines = [
        "🎫 Discord 新工单提醒",
        "",
        f"工单类型：{route['label']}",
        f"工单频道：#{channel_name}",
        f"创建时间：{created_at}",
    ]
    if route.get("owner"):
        lines.append(f"对应负责人：{route['owner']}")
    lines.extend(["", f"打开工单：{channel_url}"])
    return "\n".join(lines)


def record_ticket_event(payload: dict[str, Any], route: dict[str, str]) -> None:
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
        self.ticket_default_target = (
            config.get("HERMES_TICKET_NOTIFY_TARGET", self.telegram_target).strip()
            or self.telegram_target
        )
        self.ticket_routes = load_ticket_routes(config)
        self.allowed_role_ids = parse_id_set(
            config.get("DISCORD_MONITOR_ROLE_IDS"),
            "DISCORD_MONITOR_ROLE_IDS",
        )
        self.notify_bot_messages = env_bool(config.get("NOTIFY_BOT_MESSAGES"), default=False)
        self.send_startup_notice = env_bool(config.get("SEND_STARTUP_NOTICE"), default=True)
        self.last_sequence: int | None = None
        self.session_id: str | None = None
        self.resume_gateway_url: str | None = None
        self.self_user_id: str | None = None
        self.stop_event = asyncio.Event()
        self.heartbeat_ack_event = asyncio.Event()
        self.notification_queue: asyncio.Queue[tuple[str, str]] = asyncio.Queue(maxsize=1000)
        self.recent_message_ids: deque[str] = deque(maxlen=500)
        self.recent_message_id_set: set[str] = set()
        self.recent_ticket_channel_ids: deque[str] = deque(maxlen=500)
        self.recent_ticket_channel_id_set: set[str] = set()

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
        if not channel_id or channel_id in self.recent_ticket_channel_id_set:
            return False
        if len(self.recent_ticket_channel_ids) == self.recent_ticket_channel_ids.maxlen:
            oldest = self.recent_ticket_channel_ids.popleft()
            self.recent_ticket_channel_id_set.discard(oldest)
        self.recent_ticket_channel_ids.append(channel_id)
        self.recent_ticket_channel_id_set.add(channel_id)
        return True

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
                await send_via_hermes(target, alert)
            except Exception as exc:
                print(f"Hermes 通知发送失败：{exc}", file=sys.stderr, flush=True)
            finally:
                self.notification_queue.task_done()

    async def handle_message_create(self, payload: dict[str, Any]) -> None:
        if str(payload.get("channel_id", "")) != self.channel_id:
            return

        author = payload.get("author") or {}
        author_id = str(author.get("id", ""))
        if self.self_user_id and author_id == self.self_user_id:
            return
        if author.get("bot") and not self.notify_bot_messages:
            return
        if not member_has_allowed_role(payload, self.allowed_role_ids):
            return

        message_id = str(payload.get("id", ""))
        if not self.remember_message(message_id):
            return

        alert = build_alert(payload)
        try:
            self.notification_queue.put_nowait((self.telegram_target, alert))
            print(f"已捕获并加入通知队列：Discord 消息 {message_id}", flush=True)
        except asyncio.QueueFull:
            print("通知队列已满，本条提醒未能加入。", file=sys.stderr, flush=True)

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

        record_ticket_event(payload, route)
        target = route.get("target") or self.ticket_default_target
        alert = build_ticket_alert(payload, route)
        try:
            self.notification_queue.put_nowait((target, alert))
            print(
                f"已捕获新工单并加入 Telegram 通知队列：{route['label']} / {channel_id}",
                flush=True,
            )
        except asyncio.QueueFull:
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
                            print("Discord 连接成功，正在监听指定频道。", flush=True)
                        elif event_type == "RESUMED":
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

        if self.send_startup_notice:
            try:
                self.notification_queue.put_nowait(
                    (
                        self.telegram_target,
                        "✅ Discord 频道实时监控已启动。出现新消息时，我会通过 Hermes 立即通知你。",
                    ),
                )
            except asyncio.QueueFull:
                print("启动通知未能加入队列，但监控会继续运行。", file=sys.stderr, flush=True)

        timeout = aiohttp.ClientTimeout(total=None, connect=30, sock_connect=30, sock_read=None)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                retry_delay = 2
                while not self.stop_event.is_set():
                    try:
                        await self.connect_once(session)
                        retry_delay = 2
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        print(f"连接异常：{exc}；{retry_delay} 秒后重试。", file=sys.stderr, flush=True)

                    try:
                        await asyncio.wait_for(self.stop_event.wait(), timeout=retry_delay)
                    except asyncio.TimeoutError:
                        pass
                    retry_delay = min(retry_delay * 2, 60)
        finally:
            worker_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await worker_task


def self_test() -> None:
    sample = {
        "id": "100000000000000003",
        "guild_id": "100000000000000001",
        "channel_id": "100000000000000002",
        "content": "请问这个商品可以购买吗？",
        "author": {
            "id": "100000000000000004",
            "username": "example-user",
            "global_name": "Example User",
            "bot": False,
        },
        "member": {"nick": "Alex"},
        "attachments": [],
    }
    alert = build_alert(sample)
    assert "Alex" in alert
    assert "请问这个商品可以购买吗？" in alert
    assert (
        "https://discord.com/channels/"
        "100000000000000001/100000000000000002/100000000000000003"
    ) in alert
    sample["member"]["roles"] = ["100000000000000005"]
    assert member_has_allowed_role(
        sample,
        {"100000000000000005", "100000000000000006"},
    )
    assert not member_has_allowed_role(sample, {"100000000000000099"})
    routes = parse_ticket_routes(
        '{"100000000000000010":{"label":"Support ticket",'
        '"target":"telegram:-1000000000000:2","owner":"客服负责人"}}'
    )
    ticket_alert = build_ticket_alert(
        {
            "id": "100000000000000011",
            "guild_id": "100000000000000001",
            "name": "support-example-user",
            "parent_id": "100000000000000010",
            "type": 0,
        },
        routes["100000000000000010"],
    )
    assert "Support ticket" in ticket_alert
    assert "客服负责人" in ticket_alert
    assert (
        "https://discord.com/channels/"
        "100000000000000001/100000000000000011"
    ) in ticket_alert
    print("自检通过：消息提醒和工单提醒文本生成正常。")


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
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="运行离线自检，不连接 Discord 或 Hermes。",
    )
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
