# Discord Channel Monitor Skill

A private-first, public-ready Codex Skill for operating a read-only Discord Gateway monitor on macOS. It forwards allowlisted channel messages and new ticket events through Hermes to Telegram.

## What is included

- A Codex Skill named `$discord-channel-monitor`
- A Discord Gateway listener for one allowlisted message channel
- Optional ticket-category routing to different Telegram destinations
- A safe interactive installer and macOS LaunchAgent
- Offline self-tests, read-only checks, and privacy-focused configuration guidance

## Privacy and permissions

The repository contains program logic and fictional examples only. It must never contain Bot Tokens, real Discord messages, logs, real routing files, or production configuration.

Use a dedicated Discord Bot with only:

- `bot` OAuth scope
- View Channels
- Read Message History
- Message Content Intent enabled

Do not grant Administrator, Send Messages, Manage Messages, or Manage Roles.

## Local validation

```bash
python3 discord-channel-monitor/scripts/monitor.py --self-test
python3 discord-channel-monitor/scripts/install_monitor.py --self-test
python3 discord-channel-monitor/scripts/install_monitor.py --dry-run
```

The interactive installer is intentionally separate and must be run only after reviewing the dry run:

```bash
python3 discord-channel-monitor/scripts/install_monitor.py
```

It stores credentials and runtime files under `~/.hermes/`, outside this repository.

## Codex installation

Install the `discord-channel-monitor` subdirectory from this private repository using the Codex Skill Installer and authenticated GitHub access. Restart Codex after installation so the new Skill is discovered.

## Future open-source release

Before changing this repository to public:

1. Scan the complete Git history for credentials and private IDs.
2. Confirm examples contain fictional data only.
3. Test installation on a clean Mac with a dedicated test Bot.
4. Add an open-source license and public contribution/security documentation.

This initial private version intentionally has no open-source license.
