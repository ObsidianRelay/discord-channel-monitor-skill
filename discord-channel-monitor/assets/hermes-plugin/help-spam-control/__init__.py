"""Hermes 个人插件：通过 Telegram 回复标记或恢复 Help 消息。"""

from .controller import handle_pre_gateway_dispatch


def register(ctx):
    ctx.register_hook(
        "pre_gateway_dispatch",
        handle_pre_gateway_dispatch,
    )
