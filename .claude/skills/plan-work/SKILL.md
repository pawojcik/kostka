---
name: plan-work
description: Plan a significant change to kostka before writing code. Writes a proposed ADR (adr/NNNN-name.md) with one specific goal and its design, agrees it with the user, then opens one GitHub issue with the implementation plan and falsifiable exit criteria. Use when the user asks to plan, design, propose or spec a change, a feature or new behaviour, or before a change to how the engine works.
---

# Plan work

The result is a **proposed ADR** and, once the user approves it, **one GitHub issue**
with its implementation plan. Write no code here.

Both must be self-contained: someone who knows the codebase, but not this
conversation, can implement from them alone. Write them as AGENTS.md's Writing section
asks.

## 1. Understand the change

- Read `AGENTS.md`, the parts of `README.md` and the code the change touches, and the
  ADRs that concern those paths (`grep -l <path> adr/`).
- Ask the user what you can't settle from the code. Don't guess at intent.

## 2. One goal per ADR

An ADR has **one goal**: one specific feature or behaviour. If the request holds
several (say, "stream probes" and "pre-build hash tables"), say so, propose one ADR
for each, in an order in which each stands on its own, and plan one at a time.
Goals that only make sense together are one goal.

## 3. Write the proposed ADR

Copy `adr-template.md` to `adr/NNNN-short-name.md`: the next free number, and a
short, unique kebab-case name (check `ls adr/`). Fill in every section; write "None"
rather than drop one.

- **Goal:** what will be true afterwards, observable from outside the module.
- **Design:** the concepts, the flow and the interfaces that change, with Mermaid
  diagrams (a sequence diagram for messages, a flowchart for decisions, a diagram of
  modules for boundaries). Follow AGENTS.md's Principles; say where the change sits
  in the onion and what the new public interface is.
- **Exit criteria:** see below. They are copied to the issue.
- **Alternatives:** each with why not.

Status is `Proposed`. Show the user the ADR and iterate until they approve it. Only
then set it to `Accepted` (with the date). If it supersedes an ADR, mark that one
`Superseded by NNNN` in the same change.

## 4. Exit criteria

Each criterion is one sentence that a test or a run can show true or false, about one
behaviour, under stated conditions. Name how it is checked.

- Good: "The coordinator recovers and finishes a query after a worker is gone, while
  other workers with enough memory are up: the same rows as without the failure."
- Bad: "The coordinator can recover from any failure." (Not falsifiable: any failure?
  Recovered how?)

Cover the goal, what must not change (results, the default behaviour), failures and
limits (memory, a worker gone), and what the workbench shows, if anything.

## 5. Open the issue

When the ADR is accepted, show the user the issue text from `issue-template.md`,
then create it on their go-ahead:

```bash
gh issue create --title "NNNN: <ADR title>" --label enhancement --body-file <file>
```

Add the issue's URL to the ADR's header. One issue per ADR. The issue is implemented
as one change and tested as a whole, so its steps are an order of work, not separate
deliverables.

Commit the ADR only when the user asks.
