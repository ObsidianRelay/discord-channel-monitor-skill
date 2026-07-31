"""Discord 监控的可选业务适配器接口。

公开核心只依赖本模块提供的通用分类和安全加载器。公司专用术语、字段
提取和模型顺序必须放在受保护的本机适配器中，不能写入公开 Skill。
"""

from __future__ import annotations

import hashlib
import importlib.util
import os
import re
import stat
from dataclasses import dataclass, field
from pathlib import Path
from types import ModuleType
from typing import Any


PROFILE_API_VERSION = 1
PROFILE_ENV_KEY = "HERMES_MONITOR_BUSINESS_PROFILE"
PROFILE_REQUIRED_ENV_KEY = "HERMES_MONITOR_BUSINESS_PROFILE_REQUIRED"

GENERIC_CATEGORY_LABELS = {
    "account_access": "账号/访问",
    "technical_issue": "技术问题",
    "service_request": "服务请求",
    "feedback": "意见反馈",
    "other": "其他",
}
GENERIC_CATEGORY_ALIASES = {
    **GENERIC_CATEGORY_LABELS,
    "account": "账号/访问",
    "access": "账号/访问",
    "login": "账号/访问",
    "technical": "技术问题",
    "bug": "技术问题",
    "issue": "技术问题",
    "request": "服务请求",
    "suggestion": "意见反馈",
}


class BusinessProfileError(RuntimeError):
    """业务适配器缺失、权限不安全或接口不兼容。"""


def _clean_text(value: Any, limit: int = 240) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())[:limit]


@dataclass(frozen=True)
class BusinessProfile:
    """公开核心可调用的最小、稳定业务接口。"""

    key: str = "generic"
    report_title: str = "Discord 用户反馈日报"
    category_labels: dict[str, str] = field(
        default_factory=lambda: dict(GENERIC_CATEGORY_LABELS)
    )
    category_aliases: dict[str, str] = field(
        default_factory=lambda: dict(GENERIC_CATEGORY_ALIASES)
    )
    classification_rules: tuple[str, ...] = (
        "按用户实际描述选择账号/访问、技术问题、服务请求、意见反馈或其他。",
        "证据不足时使用其他，不得猜测。",
    )
    model_chain: tuple[tuple[str, str, str], ...] = ()
    sensitive_field_kinds: frozenset[str] = frozenset()

    @property
    def api_version(self) -> int:
        return PROFILE_API_VERSION

    def normalize_category(self, value: Any) -> str:
        cleaned = _clean_text(value, 80)
        if not cleaned:
            return ""
        return self.category_aliases.get(
            cleaned,
            self.category_aliases.get(cleaned.casefold(), cleaned),
        )

    def category_guidance(self) -> str:
        labels = "、".join(dict.fromkeys(self.category_labels.values()))
        rules = "".join(f"{index}. {item}\n" for index, item in enumerate(
            self.classification_rules,
            start=1,
        ))
        return f"category 优先使用：{labels}。\n{rules}".strip()

    def extract_message_fields(self, content: str) -> list[dict[str, str]]:
        del content
        return []

    def extract_ocr_fields(
        self,
        accepted_lines: list[tuple[str, str, float]],
    ) -> list[dict[str, Any]]:
        del accepted_lines
        return []

    def ocr_category_hints(self, text: str) -> list[str]:
        normalized = _clean_text(text, 2000)
        hints: list[str] = []
        if re.search(
            r"(?i)\b(?:account|login|sign\s+in|password|access)\b|"
            r"(?:账号|账户|登录|密码|访问)",
            normalized,
        ):
            hints.append("account_access")
        if re.search(
            r"(?i)\b(?:error|failed?|unable|cannot|not\s+working|"
            r"timeout|bug|issue|problem)\b|"
            r"(?:错误|报错|失败|无法|超时|异常|问题)",
            normalized,
        ):
            hints.append("technical_issue")
        if re.search(
            r"(?i)\b(?:please|request|need\s+help|can\s+you)\b|"
            r"(?:请求|需要帮助|请问|能否)",
            normalized,
        ):
            hints.append("service_request")
        if re.search(
            r"(?i)\b(?:feedback|suggestion|idea|recommend)\b|"
            r"(?:反馈|建议|意见)",
            normalized,
        ):
            hints.append("feedback")
        return list(dict.fromkeys(hints))[:4] or ["other"]

    def prioritize_category(
        self,
        current_category: str,
        ocr_hints: list[str],
    ) -> str:
        """OCR 只在模型未给出已知分类时提供确定性兜底。"""
        if current_category in self.category_labels.values():
            return current_category
        for hint in ocr_hints:
            if hint in self.category_labels:
                return self.category_labels[hint]
        return self.category_labels["other"]

    def case_detail_lines(self, case: dict[str, Any]) -> list[str]:
        del case
        return []


GENERIC_PROFILE = BusinessProfile()


def parse_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def profile_digest(path: Path | None, profile: BusinessProfile) -> str:
    hasher = hashlib.sha256()
    hasher.update(f"api={PROFILE_API_VERSION};key={profile.key}".encode())
    if path is not None:
        hasher.update(path.read_bytes())
    return hasher.hexdigest()


def _load_module(path: Path) -> ModuleType:
    module_name = "hermes_monitor_private_business_profile"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise BusinessProfileError(f"无法加载业务适配器：{path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _validate_private_path(path: Path) -> None:
    if path.is_symlink():
        raise BusinessProfileError("业务适配器不能是符号链接。")
    if not path.is_file():
        raise BusinessProfileError(f"找不到业务适配器：{path}")
    info = path.stat()
    if info.st_uid != os.getuid():
        raise BusinessProfileError("业务适配器必须归当前用户所有。")
    if stat.S_IMODE(info.st_mode) & 0o077:
        raise BusinessProfileError("业务适配器权限必须是 600。")


def load_business_profile(
    env: dict[str, str] | None = None,
) -> tuple[BusinessProfile, str]:
    """加载受保护的本机适配器；未配置时返回可独立工作的通用实现。"""
    values = env or os.environ
    configured = str(values.get(PROFILE_ENV_KEY) or "").strip()
    required = parse_bool(values.get(PROFILE_REQUIRED_ENV_KEY), False)
    if not configured:
        if required:
            raise BusinessProfileError(
                f"{PROFILE_REQUIRED_ENV_KEY}=true，但没有配置 {PROFILE_ENV_KEY}。"
            )
        return GENERIC_PROFILE, profile_digest(None, GENERIC_PROFILE)

    path = Path(configured).expanduser()
    _validate_private_path(path)
    module = _load_module(path)
    module_api_version = getattr(module, "PROFILE_API_VERSION", None)
    if module_api_version != PROFILE_API_VERSION:
        raise BusinessProfileError(
            "业务适配器 API 版本不兼容："
            f"需要 {PROFILE_API_VERSION}，实际 {module_api_version!r}。"
        )
    factory = getattr(module, "create_profile", None)
    if not callable(factory):
        raise BusinessProfileError("业务适配器缺少 create_profile()。")
    profile = factory()
    if not isinstance(profile, BusinessProfile):
        raise BusinessProfileError(
            "create_profile() 必须返回 BusinessProfile 实例。"
        )
    if profile.api_version != PROFILE_API_VERSION:
        raise BusinessProfileError("业务适配器实例 API 版本不兼容。")
    if not re.fullmatch(r"[a-z0-9][a-z0-9_-]{0,63}", profile.key):
        raise BusinessProfileError("业务适配器 key 格式无效。")
    if not profile.category_labels or "other" not in profile.category_labels:
        raise BusinessProfileError("业务适配器必须包含 other 分类。")
    return profile, profile_digest(path, profile)


def self_test() -> None:
    profile, digest = load_business_profile({})
    assert profile.key == "generic"
    assert len(digest) == 64
    assert profile.normalize_category("technical") == "技术问题"
    assert profile.extract_message_fields("hello") == []
    assert "technical_issue" in profile.ocr_category_hints("page failed")


if __name__ == "__main__":
    self_test()
    print("通用业务适配器接口自检通过。")
