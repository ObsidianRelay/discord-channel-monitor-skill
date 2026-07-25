# Discord Channel Monitor configuration

## Requirements

- macOS
- Python 3.11 or later
- Hermes installed and able to send to the intended Telegram chat or topic
- A dedicated Discord Bot with Message Content Intent enabled

Invite the Bot with only the `bot` OAuth scope. Grant `View Channels` and `Read Message History` only in the monitored channels or categories. Never grant Administrator or message-management permissions.

## Runtime locations

The Skill source contains no credentials or collected messages. The installer uses:

- Configuration: `~/.hermes/discord-channel-monitor.env`
- Runtime: `~/.hermes/services/discord-channel-monitor/`
- Ticket routes: `~/.hermes/services/discord-channel-monitor/ticket-routes.json`
- Ticket event metadata: `~/.hermes/services/discord-channel-monitor/data/ticket-events.jsonl`
- Logs: `~/.hermes/services/discord-channel-monitor/logs/`
- LaunchAgent: `~/Library/LaunchAgents/local.discord-channel-monitor.plist`

The ticket event file stores only channel, category, owner, type, and time metadata. It does not store Discord message bodies.

## Configuration keys

| Key | Required | Purpose |
| --- | --- | --- |
| `DISCORD_MONITOR_BOT_TOKEN` | Yes | Dedicated read-only Discord Bot Token |
| `DISCORD_MONITOR_CHANNEL_ID` | Yes | One allowlisted message channel |
| `HERMES_NOTIFY_TARGET` | Yes | Hermes destination, normally `telegram` or a Telegram chat/topic target |
| `HERMES_TICKET_NOTIFY_TARGET` | No | Default ticket destination; falls back to the normal target |
| `DISCORD_MONITOR_ROLE_IDS` | No | Comma-separated role allowlist |
| `NOTIFY_BOT_MESSAGES` | No | Defaults to `false` |
| `SEND_STARTUP_NOTICE` | No | Defaults to `true` |
| `HERMES_BIN` | No | Explicit Hermes executable path |
| `DISCORD_MONITOR_STATE_DIR` | No | Runtime data directory |
| `DISCORD_TICKET_ROUTES_FILE` | No | Ticket route JSON path |
| `DISCORD_TICKET_EVENT_FILE` | No | Minimum-metadata ticket event file path |

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

Interactive installation:

```bash
python3 scripts/install_monitor.py
```

The interactive command must be run only after the user approves the listed filesystem, dependency, and LaunchAgent changes. Enter the Bot Token only in the hidden local prompt.

## Troubleshooting boundaries

- A passing self-test proves only local formatting and routing logic.
- A healthy LaunchAgent proves only that the process is running.
- A real end-to-end result requires an approved message in the configured Discord channel and confirmed arrival at the configured Telegram destination.
- Never enable Discord write permissions to solve a read or connection problem.
- If a credential is exposed, revoke or rotate it before further testing.
