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
- Support message index: `~/.hermes/services/discord-channel-monitor/data/support-message-state.json`
- Local OCR helper: `~/.hermes/services/discord-channel-monitor/bin/support_vision_ocr`
- Logs: `~/.hermes/services/discord-channel-monitor/logs/`
- LaunchAgent: `~/Library/LaunchAgents/local.discord-channel-monitor.plist`

The ticket event file stores only channel, category, owner, type, and time metadata. It does not store Discord message bodies.

## Configuration keys

| Key | Required | Purpose |
| --- | --- | --- |
| `DISCORD_MONITOR_BOT_TOKEN` | Yes | Dedicated read-only Discord Bot Token |
| `DISCORD_MONITOR_CHANNEL_ID` | Yes | One allowlisted message channel |
| `DISCORD_SUPPORT_CATEGORY_ID` | Yes | Support Ticket Tool category read by the local message/OCR collector |
| `HERMES_NOTIFY_TARGET` | Yes | Hermes destination, normally `telegram` or a Telegram chat/topic target |
| `HERMES_HELP_COLLECTION_TARGET` | Yes | Help 即时汇总、未回复提醒和日报的 Telegram 目标 |
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
| `DISCORD_SUPPORT_MESSAGE_STATE_FILE` | No | Protected seven-day Support message and OCR index |
| `SUPPORT_OCR_ENABLED` | No | Enable local Support screenshot OCR; defaults to `false` unless configured |
| `SUPPORT_OCR_MAX_IMAGES` | No | Maximum eligible screenshots per message; maximum `3` |
| `SUPPORT_OCR_MAX_BYTES` | No | Maximum bytes per screenshot; default `8388608` |
| `SUPPORT_OCR_TIMEOUT_SECONDS` | No | Timeout per screenshot; default `20` |
| `SUPPORT_OCR_MIN_CONFIDENCE` | No | Minimum accepted Apple Vision confidence; default `0.45` |

## Help alerts and daily summary

- Help 新消息即时汇总只显示用户名和消息正文。
- 用户消息等待五分钟仍未得到配置的 Team/Mod 回复时发送提醒。
- BD 身份组不计为有效回复；Team/Mod 的回复、引用或提及可以取消提醒。
- 电脑休眠或监听器离线后，监听器补查 Help 消息和新工单。
- 日报脚本每五分钟可被 Hermes 静默调用，但仅在每天 10:00
  到期或完整唤醒稳定两分钟后执行实际工作。
- 最长补查七天；零条有效反馈时直接生成空日报，不调用 AI。
- 有消息时按 `OnlyRouter → GonkaRouter → DeepSeek` 尝试，每个模型最多
  60 秒。三个模型都失败后，保留统计进度并等待下一次完整唤醒重试。
- Telegram 发送失败时缓存已生成日报，下次只重发，不重复调用模型。

## Support screenshot OCR

- Process only ordinary-user screenshots in the configured Support category.
- Accept JPG, PNG, and WebP from Discord attachment hosts only.
- Process at most three screenshots per message and 8 MB per screenshot.
- Use Apple Vision locally. Do not install Tesseract and do not send the source
  image or complete OCR text to an external model.
- Extract only explicit order, product, platform, amount, carrier, tracking,
  shipping-status, refund, fee, and update-time fields.
- Remove names, addresses, telephone numbers, email addresses, credentials, and
  payment evidence. Replace order numbers, tracking numbers, and product links
  with placeholders before model analysis; restore allowed business fields
  locally when rendering the Telegram daily summary.
- Delete temporary images after success or failure. Keep only sanitized
  structured fields and OCR status for seven days.
- Mark low-confidence, unsupported, or failed screenshots for manual review.
  Do not infer a problem from product photos or other non-text images.

This public version intentionally does not include general screenshot problem
understanding, website/App bug classification, prompt extraction, or visual
reasoning. It only performs local structured OCR for order and logistics data.

```text
SUPPORT_OCR_ENABLED=true
SUPPORT_OCR_MAX_IMAGES=3
SUPPORT_OCR_MAX_BYTES=8388608
SUPPORT_OCR_TIMEOUT_SECONDS=20
SUPPORT_OCR_MIN_CONFIDENCE=0.45
```

The Discord Bot still needs only `View Channel` and `Read Message History` in
Support tickets. It must not receive permission to send or manage messages.

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
    "owner": "Support team"
  },
  "100000000000000020": {
    "label": "Collaboration ticket",
    "target": "telegram:-1000000000000:3",
    "owner": "Partnership team"
  }
}
```

All IDs above are fictional examples.

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
