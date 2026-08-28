"""Contract checks for the optional Nabu Buzz orchestration skill."""

from __future__ import annotations

import re
from pathlib import Path


SKILL = (
    Path(__file__).resolve().parents[2]
    / "optional-skills"
    / "autonomous-ai-agents"
    / "nabu-buzz-orchestration"
    / "SKILL.md"
)
REQUIRED_SECTIONS = [
    "## When to Use",
    "## Prerequisites",
    "## How to Run",
    "## Quick Reference",
    "## Procedure",
    "## Pitfalls",
    "## Verification",
]


def _content() -> str:
    return SKILL.read_text(encoding="utf-8")


def _frontmatter_and_body() -> tuple[dict[str, str], str]:
    content = _content()
    match = re.match(r"^---\n(.*?)\n---\n(.*)$", content, re.DOTALL)
    assert match, "SKILL.md must contain closed YAML frontmatter"
    fields: dict[str, str] = {}
    for line in match.group(1).splitlines():
        field = re.match(r"^(\w[\w-]*):\s*(.*)$", line)
        if field:
            fields[field.group(1)] = field.group(2).strip().strip('"')
    return fields, match.group(2)


def test_frontmatter_and_placement_follow_skill_contract() -> None:
    fields, _ = _frontmatter_and_body()
    assert fields["name"] == "nabu-buzz-orchestration"
    for required in ("version", "author", "license", "platforms"):
        assert fields.get(required), f"missing frontmatter field: {required}"
    description = fields["description"]
    assert len(description) <= 60
    assert description.endswith(".")
    assert "Maikel" in fields["author"]
    assert "optional-skills" in SKILL.parts


def test_modern_sections_are_present_in_order() -> None:
    _, body = _frontmatter_and_body()
    positions = [body.find(section) for section in REQUIRED_SECTIONS]
    assert all(position >= 0 for position in positions)
    assert positions == sorted(positions)
    assert 80 <= len(_content().splitlines()) <= 180


def test_workflow_requires_real_semantic_handoff() -> None:
    _, body = _frontmatter_and_body()
    assert "`buzz_orchestrate`" in body
    assert "Nostr `p` tags" in body
    assert "buzz://message" in body
    assert "prose-only handoff" in body
    assert "at most three" in body


def test_safety_and_idempotency_guidance_are_explicit() -> None:
    _, body = _frontmatter_and_body()
    lowered = body.lower()
    assert "never invent or pass a channel uuid" in lowered
    assert "never retry blindly" in lowered
    assert "indeterminate" in lowered
    assert "no secret" in lowered
