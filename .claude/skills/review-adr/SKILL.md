---
name: review-adr
description: Review the implementation of a kostka ADR's issue, independently of whoever wrote it. Confirms each exit criterion is met with evidence in tests, that the project's principles hold (above all a clear purpose and contract for every new or changed domain concept), and that all new functionality is tested; then posts clear, actionable comments for the implementer under the issue. Use when asked to review an issue's implementation, and by implement-adr after it pushes.
---

# Review an ADR's implementation

The input is an issue (`#N`) and the commits that implement it. The result is one
review comment under the issue, clear enough that the implementer can act on every
finding without asking.

You are **independent**. Judge only the issue, the ADR (with its corrections),
`AGENTS.md` and the code. Ignore the implementer's reasoning, even if it is in your
context. Don't fix the code yourself: the implementer does.

## 1. Load the work

- `gh issue view N --comments`: the exit criteria, the plan, what is out of scope, and
  earlier reviews and replies.
- The ADR the issue links to, with its **Corrections** section, and `AGENTS.md`.
- The commits: `git log --grep "#N"`. The diff runs from the parent of the first commit
  to the last (`git diff <first>^..<last>`). Read the whole diff, then the code around
  it that it depends on.
- On a re-review, read only the commits since your last review. Also check again every
  finding from it that was not resolved.

## 2. Run the checks

- The type check (`npx tsc --noEmit -p . --noUnusedLocals --noUnusedParameters`),
  `npm test` and `npm run build`.
- The CI run for the last commit (`gh run list --commit <sha>`).

If any of them fails, that is a blocking finding.

## 3. Exit criteria: met, with evidence

This is the main goal. For each exit criterion:

1. **Find the evidence:** the test or tests that check it (file, test name), as the
   criterion's "Checked by" names.
2. **Check that the evidence proves the criterion as written:**
   - it covers the stated conditions (every configuration, every case named);
   - it asserts the outcome, not only that the code runs;
   - it would fail if the behaviour broke.

   When in doubt, break the behaviour on purpose (one changed line), run the test,
   see it fail, and revert.
3. **Give a verdict:**
   - **Met, tested:** a test proves it.
   - **Met, not tested:** shown another way, for example a run in the browser that you
     did yourself. This is acceptable only when a test is not practical, and the
     reason must be stated. Say why it's not practical, or name the test that would
     be.
   - **Not met:** the behaviour is missing or wrong, or no evidence shows it.

A criterion that is not met, or met without a test when a test is practical, is a
blocking finding.

## 4. Domain concepts: a clear purpose and contract

List every domain concept the change **adds or modifies**: a type, a class, a module
or a message in a domain layer (planning, the coordinator, workers, executors and the
like). For each one, check:

- **Purpose:** the file's header comment says in plain words what it is and why it
  exists. For a modified concept, the comment is still true after the change.
- **Contract:**
  - its public interface is small and explicitly typed;
  - its invariants (what is always true of it) are stated;
  - how it fails is stated, with a typed error.
- **Tests:** unit tests check that contract with no infrastructure (no files, no
  simulation).
- **Name:** the same name as in the ADR, used the same way in the code, the README and
  the workbench.
- **Need:** it has a purpose of its own, not one that overlaps an existing concept, and
  it isn't an abstraction without a second use.

## 5. Principles and code

Against `AGENTS.md`'s principles and `implement-adr`'s TypeScript rules:

- **Modules:**
  - modules by purpose, narrow and deep;
  - dependencies only through minimal public interfaces;
  - layers import only from the layers above them.
- **The onion:** the domain knows nothing of time, IO, transport or the simulation.
  The application layer and the infrastructure stay outside it.
- **Types:**
  - discriminated unions with exhaustive switches;
  - no `any`;
  - no unchecked `as`;
  - no `!` without a comment naming what guarantees it;
  - `readonly` data;
  - typed errors whose message says what happened and what to do.
- **No workarounds:**
  - no special cases or flags for tests;
  - no copied logic;
  - no hidden errors;
  - no skipped or weakened tests.
- **Scope:** nothing out of the issue's scope, and no unrelated changes.
- **Docs:** `README.md`, the comments, and the tutorials where they show it, match the
  code.

## 6. Every new functionality is tested

List what the change makes possible that wasn't possible before:
- public functions and methods;
- behaviours and their branches;
- error paths;
- settings;
- what the workbench shows.

Each must be covered by a test, beyond the exit criteria. Anything untested is a
blocking finding, unless a test is not practical. The usual case is a workbench view:
it can be checked in the browser instead, but the logic behind the view still needs a
test.

## 7. Post the review

Write the comment to a file, and post it with
`gh issue comment N --body-file <file>`. Use this shape:

```markdown
## Review of <first sha>..<last sha>

**Verdict:** Approved | Changes needed (<k> blocking)

### Exit criteria

| # | criterion (short) | verdict | evidence |
|---|---|---|---|
| 1 | … | Met, tested | `test/…/x.test.ts`: "test name" |
| 2 | … | Met, not tested | ran in the browser: …; a test isn't practical because … |
| 3 | … | Not met | see finding 2 |

### Domain concepts

| concept | added or changed | purpose | contract | unit tests |
|---|---|---|---|---|
| `Stage` | added | ✓ | invariants not stated: finding 4 | ✓ |

### Findings

1. **Blocking**: [`src/engines/spark/driver/dag.ts:42`](<permalink>)
   - **What:** what is wrong, in one or two sentences.
   - **Why:** the criterion, principle or rule it breaks.
   - **Do:** the concrete change that resolves it.
2. **Should fix**: …
3. **Note**: …
```

Every finding:
- **Names a place:** a GitHub permalink to the line at the reviewed commit
  (`https://github.com/pawojcik/kostka/blob/<sha>/<path>#L<line>`).
- **Is concrete:**
  - "add a test that stops a node during stage 2 and asserts FetchFailed", not "test
    recovery more";
  - "state the invariant that a task's attempts are numbered from 0", not "unclear
    contract".
- **Has a severity:**
  - **Blocking** means the issue can't be accepted. That covers a criterion not met or
    without evidence, a broken principle, untested new functionality, a workaround,
    failing checks, or a domain concept without a clear purpose.
  - **Should fix** means quality the implementer should address.
  - **Note** is optional.
- **Is written for someone learning:** plain words, short sentences (`AGENTS.md`,
  Writing).

On a re-review, start with the earlier findings by number: resolved, or still open and
why. Then add the new findings, numbered after the earlier ones.

The verdict is **Approved** only when no blocking finding is left. Approval is not
acceptance: the user accepts by closing the issue.

Report back only the comment's link and the verdict.
