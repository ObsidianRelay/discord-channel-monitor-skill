# Discord Channel Monitor Skill

A privacy-first Codex Skill for operating a read-only Discord Gateway monitor
on macOS. It forwards allowlisted Help messages and new ticket events through
Hermes to Telegram.

## What is included

- A Codex Skill named `$discord-channel-monitor`
- A Discord Gateway listener for one allowlisted message channel
- Role-based filtering with bot messages ignored by default
- Optional ticket-category routing to different Telegram destinations
- Help message collection and five-minute unanswered-message alerts
- Sleep recovery for missed Help messages and ticket creations
- A 10:00 Help feedback summary with the fixed
  `OnlyRouter → GonkaRouter → DeepSeek` fallback order
- Optional local Apple Vision OCR for structured order and logistics fields in
  ordinary-user Support screenshots
- Cached Telegram delivery without repeating successful model generation
- A safe interactive installer and macOS LaunchAgent
- Offline self-tests, read-only checks, and privacy-focused configuration guidance

## Privacy and permissions

This public repository contains program logic and fictional configuration
examples only.

Never commit any of the following:

- Discord, Telegram, Hermes, Feishu, or model-provider Tokens and Secrets
- `.env`, `config.yaml`, or production routing files
- Real Discord channel, role, user, group, or Telegram topic IDs
- Discord or Telegram message content
- Logs, runtime data, state files, cached notifications, or backups
- Personal usernames or absolute paths from a local computer

Credentials and production configuration must remain on the local computer
under `~/.hermes/` and must never be stored in this repository.

## Discord Bot permissions

Use a dedicated Discord Bot with only these permissions:

- `bot` OAuth scope
- View Channels
- Read Message History
- Message Content Intent enabled

Do not grant:

- Administrator
- Send Messages
- Manage Messages
- Manage Roles
- Webhook management

This Skill is designed for read-only monitoring. It does not provide Discord
message sending, deletion, banning, or role-management features.

Support OCR downloads only allowlisted Discord JPG, PNG, or WebP attachments,
uses Apple Vision locally, and deletes each temporary image after processing.
The public version extracts structured order and logistics fields only. It does
not include general screenshot problem understanding or website/App bug
classification.

## Local validation

Run all checks before installation or publishing an update:

```bash
python3 discord-channel-monitor/scripts/monitor.py --self-test
python3 discord-channel-monitor/scripts/help_daily_summary_source.py --self-test
python3 discord-channel-monitor/scripts/install_monitor.py --self-test
python3 discord-channel-monitor/scripts/install_monitor.py --dry-run
```

The interactive installer is intentionally separate. Run it only after
reviewing the dry-run result:

```bash
python3 discord-channel-monitor/scripts/install_monitor.py
```

The installer stores credentials and runtime files under `~/.hermes/`, outside
this repository. It copies the Help summary script but intentionally does not
modify Hermes Cron. Review and approve the separate five-minute schedule before
enabling it.

## Codex installation

Install the `discord-channel-monitor` subdirectory from this repository using
the Codex Skill Installer.

Before installation, review:

- `discord-channel-monitor/SKILL.md`
- The scripts under `discord-channel-monitor/scripts/`
- `discord-channel-monitor/references/configuration.md`
- The requested Discord Bot permissions

Restart Codex after installation so the new Skill can be discovered.

## Public release safety

Before publishing any update:

1. Run all local validation commands.
2. Scan changed files for Tokens, Secrets, Webhooks, private IDs, and local paths.
3. Confirm that all example IDs and configuration values are fictional.
4. Verify that no `.env`, configuration, log, state, cache, message-data, or
   backup files are included.
5. Review the final Git diff before committing and pushing.

Never include real credentials or private configuration in a public GitHub
Issue.

## License

This repository does not currently include an open-source license. Public
visibility allows the code to be viewed, but does not grant permission to
reuse, modify, or redistribute it.
