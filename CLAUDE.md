## Active Skill Routing

Use the harness rules first.

Before invoking any skill, identify the work mode:

- /study for reasoning-critical work
- /produce for safe boilerplate and mechanical implementation
- Session capture / harness updates -> `/capture`

Skills may support the workflow, but they do not override the harness.

## Always-On Engineering Guardrail

Apply Karpathy-style engineering guidelines by default:

- think before coding
- keep changes simple
- prefer small vertical slices
- avoid clever abstractions too early
- verify success with concrete feedback
- do not broaden scope without reason

This is not a separate mode. It is always active.

## Core Routing

Use these skills when available:

- Product ideas / brainstorming -> /office-hours
- Strategy / scope review -> /plan-ceo-review
- Architecture review -> /plan-eng-review
- Design consultation -> /design-consultation
- Design plan review -> /plan-design-review
- Full plan review pipeline -> /autoplan
- Bugs / runtime errors / failures -> /investigate
- QA / site behavior testing -> /qa
- QA-only pass -> /qa-only
- Code review / diff review -> /review
- Visual polish -> /design-review
- Ship / deploy / PR -> /ship
- Land and deploy -> /land-and-deploy
- Save working context -> /context-save
- Restore working context -> /context-restore

## Engineering Skill Routing

Use these skills intentionally:

- tdd -> when implementing behavior that should be protected by tests
- diagnose -> when debugging failures, unclear errors, flaky behavior, or broken feedback loops
- grill-me -> when Pedro needs adversarial questioning before implementation
- grill-with-docs -> when the discussion must align with project docs, ADRs, OpenSpec, or domain language

## Planning Skill Routing

Use `ce-plan` or planning-oriented skills when the task needs structure before implementation.

Use planning to clarify:

- WHAT should change
- WHY it matters
- scope and non-goals
- risks
- tests
- verification gates

Do not let planning pre-solve the HOW for reasoning-critical components.

If the plan starts designing the implementation of a critical component, stop and move to `/study`.

## TDD Rules

When using tdd:

- prefer one vertical slice at a time
- write one failing test
- implement the minimum code to pass it
- refactor only after the test is green
- do not write a huge test suite before implementation
- do not use tests as decoration after the code is done

For reasoning-critical components, Pedro defines behavior, invariants, and edge cases first. Then the AI may help generate or refine tests.

## Diagnose Rules

When using diagnose:

- establish the feedback loop first
- reproduce the failure
- collect evidence before changing code
- change one variable at a time
- verify the fix with the same feedback loop

Do not patch randomly.

## Grill Rules

Use grill-me during /study when Pedro's plan is still vague.

Ask one question at a time.

Focus on:

- assumptions
- invariants
- edge cases
- failure modes
- testability
- operational risk

Do not turn grilling into implementation.

Use grill-with-docs when project language or decisions must match existing docs.

## Harness Priority

If a skill suggests implementation before Pedro has reasoned through a critical component, stop and return to /study.

If a skill suggests broad refactors, extra cleanup, or unrelated improvements, keep the current task scoped.

The harness is the operating system. Skills are tools inside it.

<!-- ai-memory:start -->

## Long-term memory (ai-memory)

This project uses [ai-memory](https://github.com/akitaonrails/ai-memory)
for cross-session continuity. **Lifecycle hooks already capture every
prompt + tool call automatically.** You never need to manually write
routine notes; the SessionStart hook auto-fetches pending handoffs and
the SessionEnd hook auto-consolidates. Only write a durable wiki page
when the user explicitly asks to remember or annotate something
permanently.

### When to reach for each tool

The user can express any of the intents below in plain English —
match the intent to the tool. They do not need to name the tool.

| User says / situation                                                                           | Tool                                                                                                              |
| ----------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| "have we discussed X?" / "search memory for Y" / before proposing architecture                  | `memory_query`                                                                                                    |
| "what's been going on" / "show recent activity" (light)                                         | `memory_recent`                                                                                                   |
| "is ai-memory healthy?" / "how big is the wiki?"                                                | `memory_status`                                                                                                   |
| "give me the stats" / structured snapshot for the agent to consume                              | `memory_briefing`                                                                                                 |
| "catch me up" / "I've been away" / "what's important right now?" / open-ended exploration       | `memory_explore`                                                                                                  |
| "where did we leave off?" — and you see a `📥 ai-memory: pending handoff` block in your context | already done — answer from that block; do NOT re-call `memory_handoff_accept`                                     |
| "where did we leave off?" — and no such block is visible                                        | `memory_handoff_accept` (rare; the SessionStart hook usually got there first)                                     |
| "save context for the next session" / wrapping up                                               | `memory_handoff_begin` (single-use handoff; terse summary; put detail in `open_questions` + `next_steps` bullets) |
| "consolidate this session" / "compile what we learned" (usually automatic)                      | `memory_consolidate`                                                                                              |
| "remember this permanently" / "save a note" / "add an annotation" / durable project knowledge   | `memory_write_page` (write a wiki page; do **not** use handoff for permanent notes)                               |
| "audit the wiki" / "find contradictions" / "what rules should we add?"                          | `memory_lint`                                                                                                     |
| "prune old pages" / "memory cleanup"                                                            | `memory_forget_sweep`                                                                                             |

`memory_explore` is the right default for the "I want to know what's
going on" use case — it returns a prose digest whose verbosity
scales automatically to how long it's been since the last activity
(< 1 h → one line; > 30 days → full catchup).

### When you write a project rule, write it here

If you're about to write a durable project rule ("always X", "never
Y", "all PRs must …"), this rules file (CLAUDE.md for Claude Code;
AGENTS.md for Codex / OpenCode / Cursor / Gemini CLI; whichever
convention your agent uses) is where it belongs. ai-memory's lint
pass surfaces the same hint automatically when a `kind: rule` page
lands in `_rules/`.

### Refreshing this snippet

This block is maintained by ai-memory. Two ways to refresh it with
the latest binary's recommended copy:

- **From the agent** (no terminal needed): ask "refresh the ai-memory
  routing in this project" — the agent calls
  `memory_install_self_routing`, picks the right filename for itself
  (Claude Code → `CLAUDE.md`; Codex / OpenCode / Cursor / Gemini →
  `AGENTS.md`), and uses its Write / Edit tool to land the block.
- **From the CLI**: `ai-memory install-instructions` (defaults to
  `CLAUDE.md`; pass `--target AGENTS.md` for non-Claude agents).

Both are idempotent: re-runs replace the block bracketed by
`<!-- ai-memory:start -->` / `<!-- ai-memory:end -->` markers
without disturbing the rest of the file.

<!-- ai-memory:end -->




<!-- >>> harness >>> -->
<!-- GENERATED by harness/apply.sh v1.0.0. Do not edit here. Edit the harness repo and run apply.sh again. -->

## Active Harness (v1.0.0)

Always-on rules (`.claude/rules/`, loaded every session):
- `core-invariants.md` — universal invariants (TS safety, scope, honesty, engineering judgment)
- `spec-discipline.md` — paired spec+tasks discipline
- `project.md`         — project-specific decisions (if present)

Explicit skills (load only on invocation):
- `/study`   → `.claude/skills/study/SKILL.md`  (mode rules + adversarial procedure)
- `/produce` → `.claude/skills/produce/SKILL.md` (mode rules + implementation procedure)
- `/capture` → `.claude/skills/capture/SKILL.md` (session capture procedure)

AI memory, when installed:
- use it as supporting context
- verify remembered implementation details against the current repo
- never let memory override harness rules, project rules, specs, or tests
- never store secrets or sensitive data

Source of truth: personal `harness/` repository
<!-- <<< harness <<< -->
