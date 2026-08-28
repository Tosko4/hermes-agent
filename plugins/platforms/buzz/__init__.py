"""Buzz plugin entry point.

The platform adapter remains deferred in CLI/TUI processes.  The small
``buzz_orchestrate`` client tool is pre-registered from ``tools.py`` so a Buzz
session can receive it without eagerly importing the adapter.
"""


def register(ctx) -> None:
    from .adapter import register as register_platform
    from .tools import register_tools

    register_tools(ctx)
    register_platform(ctx)


__all__ = ["register"]
