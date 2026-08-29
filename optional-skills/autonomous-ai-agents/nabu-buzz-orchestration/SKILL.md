---
name: nabu-buzz-orchestration
description: "Route #nabu work to forums and specialist agents."
version: 1.1.0
author: Maikel (@Tosko4) + Hermes Agent
license: MIT
platforms: [linux]
metadata:
  hermes:
    tags: [buzz, nabu, orchestration, routing, multi-agent]
    category: autonomous-ai-agents
    related_skills: [hermes-agent]
---

# Nabu Buzz Orchestration Skill

Use `#nabu` as Maikel's single front door for concrete work. This skill creates
the real Buzz workplace and addresses the responsible specialists; it does not
pretend that a prose-only handoff is delegation.

## When to Use

Use this workflow for a concrete investigation, implementation, review,
security assessment, or operational change requested in `#nabu`.

Do not use it for casual conversation, a request already inside its correct
project topic, or a simple answer that needs no separate workplace.

## Prerequisites

- The request came from the configured owner in the configured Buzz
  coordinator channel.
- The Buzz platform plugin exposes `buzz_orchestrate`.
- Host configuration contains the route, channel, specialist pubkeys, relay,
  and signing identity. The model never supplies those sensitive values.
- Every selected specialist is already a member of the destination channel.

## How to Run

Interpret the request, choose one route, and normally let its host-owned primary
specialist handle it. Call `buzz_orchestrate` with `route`, `title`, and `task`.
Use `agent` only to override that primary deliberately. For genuinely parallel
work, use `assignments`; every entry must carry its own `agent`,
`responsibility`, and `acceptance` gate.

After success, answer in `#nabu` with the returned link, destination,
assignees, and the next expected evidence.

## Quick Reference

Specialist responsibilities:

| Specialist | Primary responsibility |
|---|---|
| Cosmo | Primary-source research and comparisons; no implementation. |
| Neo | Scoped implementation with tests and rollback evidence. |
| Loki | Threat modelling and safe red-team analysis; no live exploitation. |
| Looi | Independent read-only review and GO/NO-GO. |
| Dash | Live-state, reliability, deployment, and gated operations. |

Route by subject, not by requested method:

- Research and source validation go to `research`.
- Buzz, mobile, relay, and Buzz-Hermes integration go to `buzz-development`.
- Proxmox, Pangolin, Cloudflare, Home Assistant, Hindsight, DocVault,
  Mindly OS, NoiseGate, LCM-X, Hermes LCM, reMarkable, and Tesla news use
  their matching configured route.
- Use `topics` only when no named project or system fits.
- Alerts, updates, and direct-message channels are not work destinations.

## Procedure

1. Extract the requested outcome, constraints, and required proof.
2. Keep one coherent outcome in one root. Split only independent outcomes.
3. Choose the narrowest configured route for the subject.
4. Use the route's primary specialist by default. Override it only when another
   specialist clearly owns the work.
5. If several specialists are genuinely required, give each a distinct,
   non-overlapping responsibility and acceptance gate; use at most three.
6. Write a short, specific title without a conversational preamble.
7. Write a self-contained task that includes acceptance evidence and safety
   gates relevant to the work.
8. Call `buzz_orchestrate` exactly once for that request.
9. Confirm the returned destination, plain specialist names, and Buzz deep link.
10. Report the handoff in `#nabu` without visible `@Specialist` mentions; let
    execution continue in the new thread.

The tool publishes a signed forum or stream root, adds real Nostr `p` tags for
assignees, adds generic callback metadata for the signing coordinator, and
links back to the originating `#nabu` event. Those event tags, never visible
`@Name` text, are the semantic assignment and callback signals.

## Pitfalls

- Never invent or pass a channel UUID, pubkey, credential, shell command, or
  arbitrary destination. These are host-owned allow-lists.
- Never say that a specialist was assigned unless the tool returned success.
- Never retry blindly after an indeterminate network result; reconcile the
  target channel first to prevent duplicate roots.
- Do not duplicate the specialist's assigned work in the coordinator chat.
- Do not route by whichever agent name the user happened to mention when the
  subject clearly belongs elsewhere.
- Never use an undifferentiated multi-agent list. It makes every agent treat the
  whole root as its own job and creates duplicate or conflicting execution.

## Verification

Before completing the coordinator reply, verify all of the following:

- The result includes a `buzz://message` link to the configured destination.
- The title describes the actual outcome rather than the handoff mechanics.
- Every named assignee has a matching semantic mention in the published root.
- The task includes concrete acceptance evidence.
- No secret, signing key, raw channel ID, or raw pubkey appears in the reply.
- An error or indeterminate result is reported plainly and is not retried.
