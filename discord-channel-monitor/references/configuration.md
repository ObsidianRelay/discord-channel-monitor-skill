# Discord Channel Monitor configuration

## Requirements

- macOS
- Python 3.11 or later
- Xcode Command Line Tools with `clang`
- Hermes installed and able to send to the intended Telegram chat or topic
- A dedicated Discord Bot with Message Content Intent enabled

Invite the Bot with only the `bot` OAuth scope. Grant `View Channels` and `Read Message History` only in the monitored channels or categories. Never grant Administrator or message-management permissions.

## Runtime locations

The Skill source contains no credentials or collected messages. The installer uses:

- Configuration: `~/.hermes/discord-channel-monitor.env`
- Runtime: `~/.hermes/services/discord-channel-monitor/`
- Ticket routes: `~/.hermes/services/discord-channel-monitor/ticket-routes.json`
- Ticket event metadata: `~/.hermes/services/discord-channel-monitor/data/ticket-events.jsonl`
- Ticket message index: `~/.hermes/services/discord-channel-monitor/data/ticket-message-state.json`
- Local OCR helper: `~/.hermes/services/discord-channel-monitor/bin/support_vision_ocr`
- Logs: `~/.hermes/services/discord-channel-monitor/logs/`
- LaunchAgent: `~/Library/LaunchAgents/local.discord-channel-monitor.plist`

The ticket event file stores only channel, category, owner, type, and time metadata. It does not store Discord message bodies.

## Configuration keys

| Key | Required | Purpose |
| --- | --- | --- |
| `DISCORD_MONITOR_BOT_TOKEN` | Yes | Dedicated read-only Discord Bot Token |
| `DISCORD_MONITOR_CHANNEL_ID` | Yes | One allowlisted message channel |
| `DISCORD_SUPPORT_CATEGORY_ID` | No | v1.1 compatibility key that creates one collected Support route |
| `HERMES_NOTIFY_TARGET` | Yes | Hermes destination, normally `telegram` or a Telegram chat/topic target |
| `HERMES_HELP_COLLECTION_TARGET` | No | Optional per-message Help collection target |
| `HERMES_HELP_DAILY_SUMMARY_TARGET` | No | Daily summary target; falls back to the legacy Help target |
| `HELP_COLLECTION_ENABLED` | No | Set `false` to stop per-message forwarding while retaining delayed alerts |
| `HERMES_TICKET_NOTIFY_TARGET` | No | Default ticket destination; falls back to the normal target |
| `DISCORD_MONITOR_ROLE_IDS` | No | Comma-separated role allowlist |
| `DISCORD_MONITOR_EXCLUDED_ROLE_IDS` | No | Team、Mod、BD 等不作为普通用户提醒的身份组 |
| `DISCORD_MONITOR_REPLY_ROLE_IDS` | No | 可以取消五分钟未回复提醒的 Team/Mod 身份组；不要包含 BD |
| `DISCORD_MESSAGE_NOTIFY_DELAY_SECONDS` | No | 未回复提醒等待时间，默认 `300` |
| `HELP_MESSAGE_RECONCILE_INTERVAL_SECONDS` | No | 休眠补漏检查间隔，默认 `30` |
| `NOTIFY_BOT_MESSAGES` | No | Defaults to `false` |
| `SEND_STARTUP_NOTICE` | No | Defaults to `true` |
| `HERMES_BIN` | No | Explicit Hermes executable path |
| `DISCORD_MONITOR_STATE_DIR` | No | Runtime data directory |
| `DISCORD_TICKET_ROUTES_FILE` | No | Ticket route JSON path |
| `DISCORD_TICKET_EVENT_FILE` | No | Minimum-metadata ticket event file path |
| `DISCORD_TICKET_MESSAGE_STATE_FILE` | No | Protected seven-day ticket message and OCR index |
| `DISCORD_SUPPORT_MESSAGE_STATE_FILE` | No | v1.1 legacy state path used only for migration |
| `SUPPORT_OCR_ENABLED` | No | Global switch for generic local ticket screenshot OCR |
| `SUPPORT_OCR_MAX_IMAGES` | No | Maximum eligible screenshots per message; maximum `3` |
| `SUPPORT_OCR_MAX_BYTES` | No | Maximum bytes per screenshot; default `8388608` |
| `SUPPORT_OCR_TIMEOUT_SECONDS` | No | Timeout per screenshot; default `20` |
| `SUPPORT_OCR_MIN_CONFIDENCE` | No | Minimum accepted Apple Vision confidence; default `0.45` |
| `HERMES_SUMMARY_MODEL_CHAIN_JSON` | No | JSON model fallback list; omit to use the Hermes default |
| `HERMES_SUMMARY_LANGUAGE` | No | Summary language, default `zh-CN` |
| `HERMES_HELP_SPAM_ALIASES` | No | Comma-separated strict spam aliases |
| `HERMES_HELP_RESTORE_ALIASES` | No | Comma-separated strict restore aliases |
| `HERMES_MONITOR_BUSINESS_PROFILE` | No | Optional local adapter path; file must be owned by the user and mode `600` |
| `HERMES_MONITOR_BUSINESS_PROFILE_REQUIRED` | No | Stop summary progress safely when a required adapter is unavailable |

## Help alerts and daily summary

- Help 新消息即时汇总只显示用户名和消息正文。
- 用户消息等待五分钟仍未得到配置的 Team/Mod 回复时发送提醒。
- BD 身份组不计为有效回复；Team/Mod 的回复、引用或提及可以取消提醒。
- 电脑休眠或监听器离线后，监听器补查 Help 消息和新工单。
- 日报脚本每五分钟可被 Hermes 静默调用，但仅在每天 10:00
  到期或完整唤醒稳定两分钟后执行实际工作。
- 最长补查七天；零条有效反馈时直接生成空日报，不调用 AI。
- 有消息时按配置的模型列表尝试；未配置时使用 Hermes 当前默认模型。
  每个模型最多60秒，全部失败后保留统计进度并等待下一次完整唤醒重试。
- Telegram 发送失败时缓存已生成日报，下次只重发，不重复调用模型。

## Generic ticket screenshot OCR

- Process ordinary-user screenshots only in routes whose `ocr_mode` is `generic`.
- Accept JPG, PNG, and WebP from Discord attachment hosts only.
- Process at most three screenshots per message and 8 MB per screenshot.
- Use Apple Vision locally; never send the source image or complete OCR text to
  an external model.
- Extract only page environment, error text, error codes, timestamps, and short
  issue evidence. Do not interpret domain-specific identifiers or workflow state.
- Remove names, addresses, phone numbers, email addresses, credentials, payment
  evidence, links, Discord mentions, and long platform IDs before model analysis.
- Delete temporary images after success or failure. Keep only sanitized evidence
  and OCR status for seven days.
- Mark low-confidence, unsupported, or failed screenshots for manual review.

```text
SUPPORT_OCR_ENABLED=true
SUPPORT_OCR_MAX_IMAGES=3
SUPPORT_OCR_MAX_BYTES=8388608
SUPPORT_OCR_TIMEOUT_SECONDS=20
SUPPORT_OCR_MIN_CONFIDENCE=0.45
```

The Discord Bot still needs only `View Channel` and `Read Message History` in
configured ticket routes. It must not receive permission to send or manage messages.

安装器只复制日报脚本，不会自动创建或修改 Hermes Cron。安排五分钟检查
属于独立的配置变更，必须先展示具体任务并获得确认。

Do not commit the real environment file. The installer reads it as plain `KEY=VALUE` data and never executes it as shell code.

## Ticket route example

Use clearly separated ticket categories and Telegram destinations:

```json
{
  "100000000000000010": {
    "label": "Support ticket",
    "target": "telegram:-1000000000000:2",
    "owner": "Support team",
    "collect_messages": true,
    "include_in_daily": true,
    "ocr_mode": "generic"
  },
  "100000000000000020": {
    "label": "Collaboration ticket",
    "target": "telegram:-1000000000000:3",
    "owner": "Partnership team",
    "collect_messages": false,
    "include_in_daily": false,
    "ocr_mode": "off"
  }
}
```

All IDs above are fictional examples.

`collect_messages`, `include_in_daily`, and `ocr_mode` are independent per
route. Daily inclusion or OCR requires `collect_messages=true`.

## Help spam exclusion

In the configured unanswered-alert topic, a paired operator can reply to the
bot alert with `垃圾消息` or `spam` to exclude the whole merged Help group.
Reply with `恢复消息` or `restore` to reverse the decision. The command must be
an exact reply in the configured topic; ordinary messages and unpaired users
cannot change the report state.

## Safe commands

Read-only prerequisite and service check:

```bash
python3 scripts/install_monitor.py --check
```

Preview installation without writing files or downloading packages:

```bash
python3 scripts/install_monitor.py --dry-run
```

Offline monitor test:

```bash
python3 scripts/monitor.py --self-test
```

Offline Help daily-summary test:

```bash
python3 scripts/help_daily_summary_source.py --self-test
```

Interactive installation:

```bash
python3 scripts/install_monitor.py
```

The interactive command must be run only after the user approves the listed filesystem, dependency, and LaunchAgent changes. Enter the Bot Token only in the hidden local prompt.

## Troubleshooting boundaries

- A passing self-test proves only local formatting and routing logic.
- The daily-summary self-test does not call Discord, Telegram, or any model.
- A healthy LaunchAgent proves only that the process is running.
- A real end-to-end result requires an approved message in the configured Discord channel and confirmed arrival at the configured Telegram destination.
- Never enable Discord write permissions to solve a read or connection problem.
- If a credential is exposed, revoke or rotate it before further testing.
