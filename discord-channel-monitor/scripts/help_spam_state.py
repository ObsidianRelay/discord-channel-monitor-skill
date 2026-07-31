"""Help 提醒映射与日报排除状态。

本模块只保存 Discord/Telegram 消息 ID、时间和开关状态，不保存消息正文。
监听器、Hermes 入站插件和日报进程通过同一把文件锁安全共享状态。
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator


STATE_VERSION = 1
MAX_ALERT_GROUPS = 5000
MAX_COMMAND_CLAIMS = 500

# 旧提醒的兼容索引在首次部署时已经写入本机私有状态文件。
# 源码保持为空，避免把真实 Discord/Telegram 消息 ID 打包到公开 Skill。
LEGACY_ALERT_GROUPS: dict[str, list[str]] = {}


def default_data_dir() -> Path:
    return (
        Path.home()
        / ".hermes"
        / "services"
        / "discord-ticket-monitor"
        / "data"
    )


def _paths(data_dir: Path | None = None) -> tuple[Path, Path, Path]:
    root = (data_dir or default_data_dir()).expanduser()
    return (
        root / "help-alert-index.json",
        root / "help-daily-exclusions.json",
        root / "help-spam-control.lock",
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    if not path.exists():
        return dict(default)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError(f"状态文件损坏或无法读取：{path}") from exc
    if not isinstance(payload, dict):
        raise RuntimeError(f"状态文件格式错误：{path}")
    return payload


def _atomic_write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.parent.chmod(0o700)
    serialized = json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
    ).encode("utf-8")
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=str(path.parent),
    )
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as file_handle:
            file_handle.write(serialized)
            file_handle.flush()
            os.fsync(file_handle.fileno())
        os.replace(temporary, path)
    finally:
        with contextlib.suppress(FileNotFoundError):
            temporary.unlink()
    path.chmod(0o600)


@contextlib.contextmanager
def state_lock(data_dir: Path | None = None) -> Iterator[None]:
    """跨进程独占锁；状态很小，读写统一加独占锁可避免竞态。"""
    _, _, lock_path = _paths(data_dir)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_path.parent.chmod(0o700)
    descriptor = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o600)
    try:
        os.chmod(lock_path, 0o600)
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        yield
    finally:
        with contextlib.suppress(OSError):
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def parse_telegram_target(target: str) -> tuple[str, str] | None:
    """解析 telegram:chat_id:thread_id；垃圾控制只接受明确话题目标。"""
    parts = str(target or "").strip().split(":")
    if len(parts) != 3 or parts[0].lower() != "telegram":
        return None
    chat_id, thread_id = parts[1].strip(), parts[2].strip()
    if not chat_id.lstrip("-").isdigit() or not thread_id.isdigit():
        return None
    return chat_id, thread_id


def _normalize_message_ids(values: Any) -> list[str]:
    return list(
        dict.fromkeys(
            str(value)
            for value in (values or [])
            if str(value).isdigit()
        )
    )


def _empty_alert_index() -> dict[str, Any]:
    return {
        "version": STATE_VERSION,
        "alerts": {},
        "legacy_by_first_message_id": {},
    }


def _normalize_alert_index(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _empty_alert_index()
    raw_alerts = payload.get("alerts")
    if isinstance(raw_alerts, dict):
        for key, raw_item in raw_alerts.items():
            if not isinstance(raw_item, dict):
                continue
            chat_id = str(raw_item.get("telegram_chat_id") or "")
            thread_id = str(raw_item.get("telegram_thread_id") or "")
            telegram_message_id = str(raw_item.get("telegram_message_id") or "")
            first_message_id = str(
                raw_item.get("first_discord_message_id") or ""
            )
            message_ids = _normalize_message_ids(
                raw_item.get("discord_message_ids")
            )
            if not (
                chat_id.lstrip("-").isdigit()
                and thread_id.isdigit()
                and telegram_message_id.isdigit()
                and first_message_id.isdigit()
                and message_ids
            ):
                continue
            canonical_key = f"{chat_id}:{thread_id}:{telegram_message_id}"
            normalized["alerts"][canonical_key] = {
                "telegram_chat_id": chat_id,
                "telegram_thread_id": thread_id,
                "telegram_message_id": telegram_message_id,
                "first_discord_message_id": first_message_id,
                "discord_message_ids": message_ids,
                "created_at": str(raw_item.get("created_at") or ""),
            }

    raw_legacy = payload.get("legacy_by_first_message_id")
    if isinstance(raw_legacy, dict):
        for first_message_id, raw_ids in raw_legacy.items():
            first_message_id = str(first_message_id)
            message_ids = _normalize_message_ids(raw_ids)
            if first_message_id.isdigit() and message_ids:
                normalized["legacy_by_first_message_id"][
                    first_message_id
                ] = message_ids
    return normalized


def bootstrap_legacy_alert_groups(
    *,
    data_dir: Path | None = None,
    groups: dict[str, list[str]] | None = None,
) -> None:
    """建立旧提醒兼容索引；不改变垃圾排除状态。"""
    alert_path, _, _ = _paths(data_dir)
    with state_lock(data_dir):
        payload = _normalize_alert_index(
            _read_json(alert_path, _empty_alert_index())
        )
        legacy = payload["legacy_by_first_message_id"]
        changed = False
        for first_message_id, raw_ids in (
            groups if groups is not None else LEGACY_ALERT_GROUPS
        ).items():
            message_ids = _normalize_message_ids(raw_ids)
            if (
                str(first_message_id).isdigit()
                and message_ids
                and legacy.get(str(first_message_id)) != message_ids
            ):
                legacy[str(first_message_id)] = message_ids
                changed = True
        if changed or not alert_path.exists():
            _atomic_write(alert_path, payload)


def record_alert_group(
    *,
    target: str,
    telegram_message_id: str,
    first_discord_message_id: str,
    discord_message_ids: list[str],
    data_dir: Path | None = None,
    created_at: str | None = None,
) -> str:
    """记录新 Help 提醒的 Telegram ID 与整组 Discord ID。"""
    parsed_target = parse_telegram_target(target)
    message_ids = _normalize_message_ids(discord_message_ids)
    telegram_message_id = str(telegram_message_id)
    first_discord_message_id = str(first_discord_message_id)
    if not (
        parsed_target
        and telegram_message_id.isdigit()
        and first_discord_message_id.isdigit()
        and message_ids
    ):
        raise ValueError("Help 提醒映射缺少有效的 Telegram/Discord ID。")
    chat_id, thread_id = parsed_target
    alert_path, _, _ = _paths(data_dir)
    key = f"{chat_id}:{thread_id}:{telegram_message_id}"
    with state_lock(data_dir):
        payload = _normalize_alert_index(
            _read_json(alert_path, _empty_alert_index())
        )
        payload["alerts"][key] = {
            "telegram_chat_id": chat_id,
            "telegram_thread_id": thread_id,
            "telegram_message_id": telegram_message_id,
            "first_discord_message_id": first_discord_message_id,
            "discord_message_ids": message_ids,
            "created_at": created_at or _utc_now(),
        }
        if len(payload["alerts"]) > MAX_ALERT_GROUPS:
            ordered = sorted(
                payload["alerts"].items(),
                key=lambda item: (
                    str(item[1].get("created_at") or ""),
                    item[0],
                ),
            )
            for old_key, _ in ordered[: len(ordered) - MAX_ALERT_GROUPS]:
                payload["alerts"].pop(old_key, None)
        _atomic_write(alert_path, payload)
    return key


def resolve_alert_group(
    *,
    telegram_chat_id: str,
    telegram_thread_id: str,
    telegram_message_id: str,
    legacy_first_discord_message_id: str = "",
    data_dir: Path | None = None,
) -> tuple[str, list[str]] | None:
    """按 Telegram 回复 ID 查找；旧提醒可回退到 Discord 首条消息 ID。"""
    alert_path, _, _ = _paths(data_dir)
    key = (
        f"{str(telegram_chat_id)}:{str(telegram_thread_id)}:"
        f"{str(telegram_message_id)}"
    )
    with state_lock(data_dir):
        payload = _normalize_alert_index(
            _read_json(alert_path, _empty_alert_index())
        )
    item = payload["alerts"].get(key)
    if item:
        return key, list(item["discord_message_ids"])
    legacy_first_discord_message_id = str(
        legacy_first_discord_message_id or ""
    )
    legacy_ids = payload["legacy_by_first_message_id"].get(
        legacy_first_discord_message_id
    )
    if legacy_ids:
        legacy_key = f"legacy:{legacy_first_discord_message_id}"
        return legacy_key, list(legacy_ids)
    return None


def _empty_exclusions() -> dict[str, Any]:
    return {
        "version": STATE_VERSION,
        "revision": 0,
        "entries": {},
        "commands": {},
    }


def _normalize_exclusions(payload: dict[str, Any]) -> dict[str, Any]:
    normalized = _empty_exclusions()
    try:
        normalized["revision"] = max(0, int(payload.get("revision") or 0))
    except (TypeError, ValueError):
        normalized["revision"] = 0
    raw_entries = payload.get("entries")
    if raw_entries is not None and not isinstance(raw_entries, dict):
        raise RuntimeError("Help 排除状态 entries 格式错误。")
    for message_id, raw_item in (raw_entries or {}).items():
        message_id = str(message_id)
        if not message_id.isdigit() or not isinstance(raw_item, dict):
            raise RuntimeError("Help 排除状态包含非法消息记录。")
        try:
            entry_revision = max(0, int(raw_item.get("revision") or 0))
        except (TypeError, ValueError) as exc:
            raise RuntimeError("Help 排除状态 revision 格式错误。") from exc
        if entry_revision > normalized["revision"]:
            raise RuntimeError("Help 排除记录 revision 超过顶层 revision。")
        normalized["entries"][message_id] = {
            "active": bool(raw_item.get("active")),
            "revision": entry_revision,
            "marked_at": str(raw_item.get("marked_at") or ""),
            "restored_at": str(raw_item.get("restored_at") or ""),
            "source_alert_key": str(
                raw_item.get("source_alert_key") or ""
            )[:160],
            "operator_id": str(raw_item.get("operator_id") or "")[:80],
        }
    raw_commands = payload.get("commands")
    if raw_commands is not None and not isinstance(raw_commands, dict):
        raise RuntimeError("Help 排除状态 commands 格式错误。")
    for command_key, raw_item in (raw_commands or {}).items():
        if not isinstance(raw_item, dict):
            continue
        message_ids = _normalize_message_ids(raw_item.get("message_ids"))
        if not message_ids:
            continue
        try:
            command_revision = max(
                0,
                int(raw_item.get("revision") or 0),
            )
        except (TypeError, ValueError):
            continue
        normalized["commands"][str(command_key)[:200]] = {
            "active": bool(raw_item.get("active")),
            "revision": command_revision,
            "message_ids": message_ids,
            "created_at": str(raw_item.get("created_at") or ""),
        }
    return normalized


def load_exclusion_snapshot(
    *,
    data_dir: Path | None = None,
) -> dict[str, Any]:
    """返回可安全传给日报的排除快照。"""
    _, exclusion_path, _ = _paths(data_dir)
    with state_lock(data_dir):
        payload = _normalize_exclusions(
            _read_json(exclusion_path, _empty_exclusions())
        )
    return payload


def update_exclusions(
    *,
    discord_message_ids: list[str],
    active: bool,
    source_alert_key: str,
    operator_id: str,
    command_key: str = "",
    data_dir: Path | None = None,
) -> dict[str, Any]:
    """整组标记或恢复；一次操作只递增一次版本号。"""
    message_ids = _normalize_message_ids(discord_message_ids)
    if not message_ids:
        raise ValueError("垃圾消息操作没有关联到有效 Discord 消息 ID。")
    _, exclusion_path, _ = _paths(data_dir)
    now = _utc_now()
    with state_lock(data_dir):
        payload = _normalize_exclusions(
            _read_json(exclusion_path, _empty_exclusions())
        )
        normalized_command_key = str(command_key or "")[:200]
        if normalized_command_key:
            prior_command = payload["commands"].get(
                normalized_command_key
            )
            if prior_command:
                return {
                    "revision": int(prior_command["revision"]),
                    "active": bool(prior_command["active"]),
                    "message_ids": list(prior_command["message_ids"]),
                    "duplicate": True,
                    "changed": False,
                }

        already_desired = all(
            bool((payload["entries"].get(message_id) or {}).get("active"))
            is bool(active)
            for message_id in message_ids
        )
        if already_desired:
            revision = int(payload["revision"])
            changed = False
        else:
            revision = int(payload["revision"]) + 1
            changed = True
            for message_id in message_ids:
                prior = payload["entries"].get(message_id) or {}
                payload["entries"][message_id] = {
                    "active": bool(active),
                    "revision": revision,
                    "marked_at": (
                        now
                        if active
                        else str(prior.get("marked_at") or "")
                    ),
                    "restored_at": "" if active else now,
                    "source_alert_key": str(source_alert_key or "")[:160],
                    "operator_id": str(operator_id or "")[:80],
                }
            payload["revision"] = revision
        if normalized_command_key:
            payload["commands"][normalized_command_key] = {
                "active": bool(active),
                "revision": revision,
                "message_ids": message_ids,
                "created_at": now,
            }
            if len(payload["commands"]) > MAX_COMMAND_CLAIMS:
                ordered = sorted(
                    payload["commands"].items(),
                    key=lambda item: (
                        str(item[1].get("created_at") or ""),
                        item[0],
                    ),
                )
                for old_key, _ in ordered[
                    : len(ordered) - MAX_COMMAND_CLAIMS
                ]:
                    payload["commands"].pop(old_key, None)
        _atomic_write(exclusion_path, payload)
    return {
        "revision": revision,
        "active": bool(active),
        "message_ids": message_ids,
        "duplicate": False,
        "changed": changed,
    }


def self_test() -> None:
    import tempfile

    with tempfile.TemporaryDirectory(prefix="help-spam-state-") as temp_dir:
        data_dir = Path(temp_dir)
        bootstrap_legacy_alert_groups(
            data_dir=data_dir,
            groups={"100": ["100", "101"]},
        )
        assert resolve_alert_group(
            telegram_chat_id="-1001",
            telegram_thread_id="2",
            telegram_message_id="9",
            legacy_first_discord_message_id="100",
            data_dir=data_dir,
        ) == ("legacy:100", ["100", "101"])
        key = record_alert_group(
            target="telegram:-1001:2",
            telegram_message_id="500",
            first_discord_message_id="100",
            discord_message_ids=["100", "101"],
            data_dir=data_dir,
        )
        assert key == "-1001:2:500"
        assert resolve_alert_group(
            telegram_chat_id="-1001",
            telegram_thread_id="2",
            telegram_message_id="500",
            data_dir=data_dir,
        ) == (key, ["100", "101"])
        marked = update_exclusions(
            discord_message_ids=["100", "101"],
            active=True,
            source_alert_key=key,
            operator_id="42",
            command_key="-1001:2:900",
            data_dir=data_dir,
        )
        assert marked["revision"] == 1
        snapshot = load_exclusion_snapshot(data_dir=data_dir)
        assert {
            message_id
            for message_id, item in snapshot["entries"].items()
            if item["active"]
        } == {"100", "101"}
        restored = update_exclusions(
            discord_message_ids=["100", "101"],
            active=False,
            source_alert_key=key,
            operator_id="42",
            command_key="-1001:2:901",
            data_dir=data_dir,
        )
        assert restored["revision"] == 2
        assert not any(
            item["active"]
            for item in load_exclusion_snapshot(
                data_dir=data_dir
            )["entries"].values()
        )
        duplicate = update_exclusions(
            discord_message_ids=["100", "101"],
            active=True,
            source_alert_key=key,
            operator_id="42",
            command_key="-1001:2:900",
            data_dir=data_dir,
        )
        assert duplicate["duplicate"]
        assert duplicate["revision"] == 1
        assert not any(
            item["active"]
            for item in load_exclusion_snapshot(
                data_dir=data_dir
            )["entries"].values()
        )
        broken = data_dir / "help-daily-exclusions.json"
        broken.write_text("{", encoding="utf-8")
        try:
            load_exclusion_snapshot(data_dir=data_dir)
        except RuntimeError:
            pass
        else:
            raise AssertionError("损坏状态文件必须 fail closed。")


if __name__ == "__main__":
    self_test()
    print("Help 垃圾消息状态自检通过。")
