#!/usr/bin/env python3
"""安全安装、检查并注册 Discord 频道监控 LaunchAgent。"""

from __future__ import annotations

import argparse
import getpass
import os
import plistlib
import re
import shutil
import stat
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path


SKILL_DIR = Path(__file__).resolve().parent.parent
MONITOR_SOURCE = Path(__file__).resolve().parent / "monitor.py"
HELP_DAILY_SOURCE = (
    Path(__file__).resolve().parent / "help_daily_summary_source.py"
)
REQUIREMENTS_FILE = SKILL_DIR / "requirements.txt"
DEFAULT_SERVICE_DIR = Path.home() / ".hermes" / "services" / "discord-channel-monitor"
DEFAULT_ENV_FILE = Path.home() / ".hermes" / "discord-channel-monitor.env"
DEFAULT_LAUNCH_AGENT = (
    Path.home() / "Library" / "LaunchAgents" / "local.discord-channel-monitor.plist"
)
LAUNCH_AGENT_LABEL = "local.discord-channel-monitor"
REQUIRED_CONFIG_KEYS = {
    "DISCORD_MONITOR_BOT_TOKEN",
    "DISCORD_MONITOR_CHANNEL_ID",
}
CONFIG_KEY_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="安装或检查 Discord Channel Monitor。",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--check",
        action="store_true",
        help="只读检查依赖、配置和服务状态。",
    )
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="只显示安装计划，不写文件或下载依赖。",
    )
    mode.add_argument(
        "--self-test",
        action="store_true",
        help="在临时目录测试配置和 LaunchAgent 生成逻辑。",
    )
    return parser.parse_args()


def prompt_yes_no(question: str, default: bool = False) -> bool:
    suffix = " [Y/n] " if default else " [y/N] "
    answer = input(question + suffix).strip().lower()
    if not answer:
        return default
    return answer in {"y", "yes"}


def validate_single_line(name: str, value: str) -> str:
    value = value.strip()
    if "\n" in value or "\r" in value:
        raise ValueError(f"{name} 不能包含换行。")
    return value


def validate_discord_id(name: str, value: str, allow_empty: bool = False) -> str:
    value = validate_single_line(name, value)
    if not value and allow_empty:
        return value
    if not value.isdigit():
        raise ValueError(f"{name} 必须是纯数字 Discord ID。")
    return value


def locate_hermes(config: dict[str, str] | None = None) -> Path | None:
    configured = (config or {}).get("HERMES_BIN", "").strip()
    candidates = [
        Path(configured).expanduser() if configured else None,
        Path(found) if (found := shutil.which("hermes")) else None,
        Path.home() / ".local" / "bin" / "hermes",
    ]
    for candidate in candidates:
        if candidate and candidate.is_file() and os.access(candidate, os.X_OK):
            return candidate
    return None


def read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    if not path.exists():
        return values
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if CONFIG_KEY_RE.fullmatch(key):
            values[key] = value.strip()
    return values


def read_env_keys(path: Path) -> set[str]:
    """只读取配置键名，用于不接触凭据值的状态检查。"""
    keys: set[str] = set()
    if not path.exists():
        return keys
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key = line.split("=", 1)[0].strip()
        if CONFIG_KEY_RE.fullmatch(key):
            keys.add(key)
    return keys


def render_env(config: dict[str, str]) -> str:
    lines = [
        "# Discord Channel Monitor configuration.",
        "# Keep this file outside Git and protect it with mode 600.",
    ]
    preferred_order = [
        "DISCORD_MONITOR_BOT_TOKEN",
        "DISCORD_MONITOR_CHANNEL_ID",
        "HERMES_NOTIFY_TARGET",
        "HERMES_HELP_COLLECTION_TARGET",
        "HERMES_TICKET_NOTIFY_TARGET",
        "DISCORD_MONITOR_ROLE_IDS",
        "DISCORD_MONITOR_EXCLUDED_ROLE_IDS",
        "DISCORD_MONITOR_REPLY_ROLE_IDS",
        "DISCORD_MESSAGE_NOTIFY_DELAY_SECONDS",
        "HELP_MESSAGE_RECONCILE_INTERVAL_SECONDS",
        "NOTIFY_BOT_MESSAGES",
        "SEND_STARTUP_NOTICE",
        "HERMES_BIN",
        "DISCORD_MONITOR_STATE_DIR",
        "DISCORD_TICKET_ROUTES_FILE",
        "DISCORD_TICKET_EVENT_FILE",
    ]
    ordered_keys = [key for key in preferred_order if key in config]
    ordered_keys.extend(sorted(set(config) - set(ordered_keys)))
    for key in ordered_keys:
        if not CONFIG_KEY_RE.fullmatch(key):
            raise ValueError(f"配置键无效：{key}")
        value = validate_single_line(key, config[key])
        lines.append(f"{key}={value}")
    return "\n".join(lines) + "\n"


def safe_write_text(path: Path, content: str, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    try:
        temp_path.write_text(content, encoding="utf-8")
        temp_path.chmod(mode)
        temp_path.replace(path)
        path.chmod(mode)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def backup_file(path: Path) -> Path | None:
    if not path.exists():
        return None
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    backup_path = path.with_name(f"{path.name}.backup-{timestamp}")
    shutil.copy2(path, backup_path)
    return backup_path


def render_launch_agent(
    python_path: Path,
    monitor_path: Path,
    env_file: Path,
    stdout_log: Path,
    stderr_log: Path,
) -> bytes:
    payload = {
        "Label": LAUNCH_AGENT_LABEL,
        "ProgramArguments": [
            str(python_path),
            str(monitor_path),
            "--env-file",
            str(env_file),
        ],
        "RunAtLoad": True,
        "KeepAlive": {"NetworkState": True},
        "ThrottleInterval": 10,
        "StandardOutPath": str(stdout_log),
        "StandardErrorPath": str(stderr_log),
        "ProcessType": "Background",
    }
    return plistlib.dumps(payload, fmt=plistlib.FMT_XML, sort_keys=True)


def run_command(args: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        check=check,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )


def collect_new_config(hermes_bin: Path, service_dir: Path) -> dict[str, str]:
    token = validate_single_line(
        "DISCORD_MONITOR_BOT_TOKEN",
        getpass.getpass("Discord Bot Token（输入不会显示）: "),
    )
    if not token:
        raise ValueError("Discord Bot Token 不能为空。")

    channel_id = validate_discord_id(
        "DISCORD_MONITOR_CHANNEL_ID",
        input("要监控的 Discord 频道 ID: "),
    )
    notify_target = validate_single_line(
        "HERMES_NOTIFY_TARGET",
        input("Hermes 通知目标 [telegram]: ") or "telegram",
    )
    ticket_target = validate_single_line(
        "HERMES_TICKET_NOTIFY_TARGET",
        input("工单默认通知目标 [与普通通知相同]: ") or notify_target,
    )
    help_target = validate_single_line(
        "HERMES_HELP_COLLECTION_TARGET",
        input("Help 即时汇总和日报目标 [与普通通知相同]: ")
        or notify_target,
    )
    role_ids_raw = validate_single_line(
        "DISCORD_MONITOR_ROLE_IDS",
        input("允许提醒的身份组 ID（多个用逗号分隔，可留空）: "),
    )
    if role_ids_raw:
        for role_id in role_ids_raw.split(","):
            validate_discord_id("DISCORD_MONITOR_ROLE_IDS", role_id)
    excluded_role_ids = validate_single_line(
        "DISCORD_MONITOR_EXCLUDED_ROLE_IDS",
        input("排除身份组 ID（Team/Mod/BD 等，多个用逗号分隔，可留空）: "),
    )
    if excluded_role_ids:
        for role_id in excluded_role_ids.split(","):
            validate_discord_id("DISCORD_MONITOR_EXCLUDED_ROLE_IDS", role_id)
    reply_role_ids = validate_single_line(
        "DISCORD_MONITOR_REPLY_ROLE_IDS",
        input("可取消未回复提醒的 Team/Mod 身份组 ID（多个用逗号分隔）: "),
    )
    if reply_role_ids:
        for role_id in reply_role_ids.split(","):
            validate_discord_id("DISCORD_MONITOR_REPLY_ROLE_IDS", role_id)

    return {
        "DISCORD_MONITOR_BOT_TOKEN": token,
        "DISCORD_MONITOR_CHANNEL_ID": channel_id,
        "HERMES_NOTIFY_TARGET": notify_target,
        "HERMES_HELP_COLLECTION_TARGET": help_target,
        "HERMES_TICKET_NOTIFY_TARGET": ticket_target,
        "DISCORD_MONITOR_ROLE_IDS": role_ids_raw,
        "DISCORD_MONITOR_EXCLUDED_ROLE_IDS": excluded_role_ids,
        "DISCORD_MONITOR_REPLY_ROLE_IDS": reply_role_ids,
        "DISCORD_MESSAGE_NOTIFY_DELAY_SECONDS": "300",
        "HELP_MESSAGE_RECONCILE_INTERVAL_SECONDS": "30",
        "NOTIFY_BOT_MESSAGES": "false",
        "SEND_STARTUP_NOTICE": "true",
        "HERMES_BIN": str(hermes_bin),
        "DISCORD_MONITOR_STATE_DIR": str(service_dir),
        "DISCORD_TICKET_ROUTES_FILE": str(service_dir / "ticket-routes.json"),
        "DISCORD_TICKET_EVENT_FILE": str(
            service_dir / "data" / "ticket-events.jsonl"
        ),
    }


def print_plan() -> None:
    print("安装计划（本次仅预览，不会执行）：")
    print(f"1. 检查 Hermes、Python 和源文件：{SKILL_DIR}")
    print(f"2. 创建独立运行目录：{DEFAULT_SERVICE_DIR}")
    print("3. 复制监听器和 Help 日报脚本到运行目录")
    print(f"4. 创建虚拟环境并安装：{REQUIREMENTS_FILE}")
    print(f"5. 写入受保护配置：{DEFAULT_ENV_FILE}（权限 600）")
    print(f"6. 写入 LaunchAgent：{DEFAULT_LAUNCH_AGENT}")
    print("7. 只有再次确认后才加载并启动后台服务")
    print("不会自动新增或修改 Hermes Cron；日报调度需单独审核配置。")
    print("不会读取、复制或写入 Git 仓库中的真实 Discord 消息。")


def check_installation() -> int:
    checks: list[tuple[str, bool, str]] = []
    checks.append(("macOS", sys.platform == "darwin", sys.platform))
    checks.append(
        (
            "Python 3.11+",
            sys.version_info >= (3, 11),
            sys.version.split()[0],
        )
    )
    checks.append(("监控源文件", MONITOR_SOURCE.is_file(), str(MONITOR_SOURCE)))
    checks.append(
        ("Help 日报源文件", HELP_DAILY_SOURCE.is_file(), str(HELP_DAILY_SOURCE))
    )
    checks.append(
        ("依赖文件", REQUIREMENTS_FILE.is_file(), str(REQUIREMENTS_FILE))
    )

    config_keys = read_env_keys(DEFAULT_ENV_FILE)
    missing_keys = sorted(REQUIRED_CONFIG_KEYS - config_keys)
    checks.append(
        (
            "配置文件",
            DEFAULT_ENV_FILE.is_file() and not missing_keys,
            "存在且必需键齐全"
            if DEFAULT_ENV_FILE.is_file() and not missing_keys
            else f"缺失或缺少键：{', '.join(missing_keys) or '配置文件'}",
        )
    )
    if DEFAULT_ENV_FILE.exists():
        mode = stat.S_IMODE(DEFAULT_ENV_FILE.stat().st_mode)
        checks.append(("配置权限 600", mode == 0o600, oct(mode)))

    hermes_bin = locate_hermes()
    checks.append(
        (
            "Hermes",
            hermes_bin is not None,
            str(hermes_bin) if hermes_bin else "未找到",
        )
    )
    runtime_monitor = DEFAULT_SERVICE_DIR / "monitor.py"
    runtime_help_daily = DEFAULT_SERVICE_DIR / "help_daily_summary_source.py"
    venv_python = DEFAULT_SERVICE_DIR / "venv" / "bin" / "python"
    checks.append(("运行副本", runtime_monitor.is_file(), str(runtime_monitor)))
    checks.append(
        ("Help 日报运行副本", runtime_help_daily.is_file(), str(runtime_help_daily))
    )
    checks.append(("独立 Python", venv_python.is_file(), str(venv_python)))
    checks.append(
        ("LaunchAgent 文件", DEFAULT_LAUNCH_AGENT.is_file(), str(DEFAULT_LAUNCH_AGENT))
    )

    service_loaded = False
    if sys.platform == "darwin":
        target = f"gui/{os.getuid()}/{LAUNCH_AGENT_LABEL}"
        result = run_command(["launchctl", "print", target], check=False)
        service_loaded = result.returncode == 0
    checks.append(("LaunchAgent 已加载", service_loaded, LAUNCH_AGENT_LABEL))

    for name, passed, detail in checks:
        marker = "OK" if passed else "WARN"
        print(f"[{marker}] {name}: {detail}")
    print("检查不会显示 Token、频道 ID或 Telegram 目标的值。")
    return 0 if all(passed for _, passed, _ in checks) else 1


def run_self_test() -> int:
    fake_config = {
        "DISCORD_MONITOR_BOT_TOKEN": "example-token-never-use",
        "DISCORD_MONITOR_CHANNEL_ID": "100000000000000002",
        "HERMES_NOTIFY_TARGET": "telegram",
    }
    env_text = render_env(fake_config)
    assert "example-token-never-use" in env_text
    with tempfile.TemporaryDirectory(prefix="discord-monitor-test-") as temp_dir:
        root = Path(temp_dir)
        env_file = root / "monitor.env"
        safe_write_text(env_file, env_text, 0o600)
        assert stat.S_IMODE(env_file.stat().st_mode) == 0o600

        plist_bytes = render_launch_agent(
            root / "venv" / "bin" / "python",
            root / "monitor.py",
            env_file,
            root / "stdout.log",
            root / "stderr.log",
        )
        parsed = plistlib.loads(plist_bytes)
        assert parsed["Label"] == LAUNCH_AGENT_LABEL
        assert "example-token-never-use" not in plist_bytes.decode("utf-8")
    print("安装器自检通过：配置权限和 LaunchAgent 生成逻辑正常。")
    return 0


def install() -> int:
    if sys.platform != "darwin":
        raise RuntimeError("此安装器当前只支持 macOS。")
    if sys.version_info < (3, 11):
        raise RuntimeError("需要 Python 3.11 或更高版本。")
    if (
        not MONITOR_SOURCE.is_file()
        or not HELP_DAILY_SOURCE.is_file()
        or not REQUIREMENTS_FILE.is_file()
    ):
        raise RuntimeError("Skill 文件不完整，缺少监控程序、日报脚本或依赖文件。")

    existing_config = read_env_file(DEFAULT_ENV_FILE)
    hermes_bin = locate_hermes(existing_config)
    if hermes_bin is None:
        raise RuntimeError("找不到 Hermes。请先安装 Hermes，并确认 hermes 命令可用。")

    print_plan()
    if not prompt_yes_no("确认创建运行文件、安装 Python 依赖并写入配置吗？"):
        print("已取消，未做任何修改。")
        return 0

    if existing_config and prompt_yes_no("检测到现有配置，保留现有凭据和路由设置吗？", True):
        config = dict(existing_config)
        config["HERMES_BIN"] = str(hermes_bin)
        config["DISCORD_MONITOR_STATE_DIR"] = str(DEFAULT_SERVICE_DIR)
        config["DISCORD_TICKET_ROUTES_FILE"] = str(
            DEFAULT_SERVICE_DIR / "ticket-routes.json"
        )
        config["DISCORD_TICKET_EVENT_FILE"] = str(
            DEFAULT_SERVICE_DIR / "data" / "ticket-events.jsonl"
        )
    else:
        config = collect_new_config(hermes_bin, DEFAULT_SERVICE_DIR)

    missing_keys = sorted(REQUIRED_CONFIG_KEYS - set(config))
    if missing_keys:
        raise RuntimeError(f"配置缺少必需键：{', '.join(missing_keys)}")

    DEFAULT_SERVICE_DIR.mkdir(parents=True, exist_ok=True)
    (DEFAULT_SERVICE_DIR / "logs").mkdir(parents=True, exist_ok=True)
    (DEFAULT_SERVICE_DIR / "data").mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    staging_venv = DEFAULT_SERVICE_DIR / f".venv-staging-{timestamp}"
    if staging_venv.exists():
        raise RuntimeError(f"临时虚拟环境已存在：{staging_venv}")

    print("正在创建独立 Python 环境并安装 aiohttp……")
    try:
        run_command([sys.executable, "-m", "venv", str(staging_venv)])
        run_command(
            [
                str(staging_venv / "bin" / "python"),
                "-m",
                "pip",
                "install",
                "--requirement",
                str(REQUIREMENTS_FILE),
            ]
        )
    except Exception:
        shutil.rmtree(staging_venv, ignore_errors=True)
        raise

    for path in (
        DEFAULT_ENV_FILE,
        DEFAULT_SERVICE_DIR / "monitor.py",
        DEFAULT_SERVICE_DIR / "help_daily_summary_source.py",
        DEFAULT_LAUNCH_AGENT,
    ):
        backup = backup_file(path)
        if backup:
            print(f"已备份：{backup}")

    venv_dir = DEFAULT_SERVICE_DIR / "venv"
    if venv_dir.exists():
        venv_backup = DEFAULT_SERVICE_DIR / f"venv.backup-{timestamp}"
        venv_dir.replace(venv_backup)
        print(f"已备份旧虚拟环境：{venv_backup}")
    staging_venv.replace(venv_dir)

    runtime_monitor = DEFAULT_SERVICE_DIR / "monitor.py"
    temp_monitor = DEFAULT_SERVICE_DIR / f".monitor.py.tmp-{os.getpid()}"
    shutil.copy2(MONITOR_SOURCE, temp_monitor)
    temp_monitor.chmod(0o700)
    temp_monitor.replace(runtime_monitor)

    runtime_help_daily = DEFAULT_SERVICE_DIR / "help_daily_summary_source.py"
    temp_help_daily = DEFAULT_SERVICE_DIR / f".help-daily.py.tmp-{os.getpid()}"
    shutil.copy2(HELP_DAILY_SOURCE, temp_help_daily)
    temp_help_daily.chmod(0o700)
    temp_help_daily.replace(runtime_help_daily)

    safe_write_text(DEFAULT_ENV_FILE, render_env(config), 0o600)
    routes_file = DEFAULT_SERVICE_DIR / "ticket-routes.json"
    if not routes_file.exists():
        safe_write_text(routes_file, "{}\n", 0o600)

    plist_bytes = render_launch_agent(
        venv_dir / "bin" / "python",
        runtime_monitor,
        DEFAULT_ENV_FILE,
        DEFAULT_SERVICE_DIR / "logs" / "stdout.log",
        DEFAULT_SERVICE_DIR / "logs" / "stderr.log",
    )
    safe_write_text(
        DEFAULT_LAUNCH_AGENT,
        plist_bytes.decode("utf-8"),
        0o600,
    )

    if prompt_yes_no("文件安装完成。现在加载并启动后台监控服务吗？"):
        domain = f"gui/{os.getuid()}"
        target = f"{domain}/{LAUNCH_AGENT_LABEL}"
        run_command(["launchctl", "bootout", target], check=False)
        result = run_command(
            ["launchctl", "bootstrap", domain, str(DEFAULT_LAUNCH_AGENT)],
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(
                "LaunchAgent 加载失败："
                + (result.stderr.strip() or f"退出码 {result.returncode}")
            )
        print("后台服务已加载。请运行 --check，并通过获批的真实消息确认端到端通知。")
    else:
        print("文件已安装，但服务尚未加载或启动。")
    return 0


def main() -> int:
    args = parse_args()
    if args.check:
        return check_installation()
    if args.dry_run:
        print_plan()
        return 0
    if args.self_test:
        return run_self_test()
    return install()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\n已取消。", file=sys.stderr)
        raise SystemExit(130)
    except Exception as exc:
        print(f"操作失败：{exc}", file=sys.stderr)
        raise SystemExit(1)
