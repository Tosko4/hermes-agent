"""Buzz's client tool must load without materializing its gateway adapter."""

from __future__ import annotations

import sys


def test_buzz_orchestrate_preregisters_while_platform_stays_deferred() -> None:
    from hermes_cli.plugins import PluginManager
    from toolsets import resolve_toolset
    from tools.registry import registry

    manager = PluginManager()
    manager.discover_and_load()

    plugin = manager._plugins.get("buzz-platform")
    assert plugin is not None, "bundled Buzz platform plugin was not discovered"
    assert plugin.deferred is True
    assert "buzz_orchestrate" in set(resolve_toolset("buzz"))
    assert plugin.tools_registered == ["buzz_orchestrate"]

    entry = registry.get_entry("buzz_orchestrate")
    assert entry is not None and entry.check_fn is not None
    runtime_config = entry.check_fn.__globals__["_runtime_config"]
    entry.check_fn.__globals__["_runtime_config"] = lambda: {
        "gateway": {
            "platforms": {
                "buzz": {
                    "extra": {
                        "orchestration": {
                            "enabled": True,
                            "home_channel": "812dd8b8-ffd3-5619-8414-18df079fcce6",
                            "allowed_users": ["b" * 64],
                            "routes": {"research": {}},
                        }
                    }
                }
            }
        }
    }
    try:
        assert entry.check_fn() is True
    finally:
        entry.check_fn.__globals__["_runtime_config"] = runtime_config

    assert not any(
        name.endswith("buzz.adapter") or name.endswith("buzz_platform.adapter")
        for name in sys.modules
    )
