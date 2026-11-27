---
name: implement-adr
description: Implement an accepted kostka ADR from its open GitHub issue, as one change. First checks that the ADR's and the plan's assumptions still hold against the code (appending a correction to the ADR and updating the issue's plan if not; changes to exit criteria are discussed with the user), then designs the straightforward way to meet every exit criterion, implements it within the project's principles, verifies each criterion, commits and pushes it, has it reviewed independently (review-adr) until approved, and reports on the issue. Use when the user asks to implement, build or deliver an issue or an ADR.
---

# Implement an ADR

The input is an open issue (`#N`) and the ADR it implements. The result is one change
that meets every exit criterion, reviewed by someone who did not write it.

Read `AGENTS.md` first. Its principles, layout and Done rules apply throughout.

## 1. Load the work

- `gh issue view N`: the goal, context, plan, exit criteria and what is out of scope.
- The ADR it links to, every ADR that ADR names, and every ADR that concerns the
  paths in its **Affects** (`grep -l <path> adr/`).
- The parts of `README.md` and the code those paths cover.

## 2. Check the assumptions

The ADR and the plan were written against the code as it was then. Before designing
anything, check every claim they make that the work depends on, against the code at
`HEAD`:

- What the Context says is true today: paths, names, interfaces, behaviour, numbers.
- What the Design builds on: the modules, types and functions it changes or uses, and
  the behaviour it keeps.
- The plan: its order still works, and its paths exist.
- Later ADRs: none supersedes this one or changes what it builds on.

Write down each claim that no longer holds: what was assumed, what is true, and how
you know (a path and line, or a command and its output).

If everything holds, go on to step 4.

## 3. Correct the ADR and the plan

An accepted ADR is not rewritten. Append a section at its end, and leave the rest of
the ADR as it is:

```markdown
## Corrections

### YYYY-MM-DD: <what changed, in a few words>

- **Assumed:** what the ADR says.
- **True:** what the code shows (path, line).
- **So:** what changes in the design, and why it still meets the goal.
```

Then update the issue's Context and Plan to match: show the user the new text, and
edit the issue with `gh issue edit N --body-file <file>` on their go-ahead.

**Exit criteria are a contract. Do not change them on your own.** If a correction
means a criterion can't be met, needs a different check, or no longer matters, stop
and discuss it with the user: say what changed, and offer the new wording. Change the
criterion in the ADR (in the correction) and in the issue only once the user agrees.

## 4. Design the whole change

Look at the plan as a whole before you write code. For each exit criterion, decide
what makes it true and what test or run shows it. Then find the most straightforward
design that meets all of them at once. This is often simpler than doing the plan's
steps one after another.

- **No workarounds.** A workaround makes a criterion pass without the behaviour it
  describes, or hides a problem instead of solving it. Examples:
  - special cases for tests;
  - flags that only tests set;
  - logic copied instead of shared;
  - `any`, `as`, `!` or `@ts-ignore` to silence the type checker;
  - catching an error to hide it;
  - a test that is skipped or weakened.

  If the straightforward way is blocked, find out why. If the reason is in the ADR,
  it is a correction (step 3).
- **Keep the principles** (`AGENTS.md`):
  - modules by purpose, narrow and deep;
  - depend only on minimal public interfaces;
  - layers import only from the layers above them.
- **Separate the layers:**
  - The domain holds the concepts and rules. It knows nothing of time, IO, transport
    or the simulation.
  - The application layer (`api/`, the Session) puts the domain to use.
  - The infrastructure (`cluster/`, `storage/`, the simulation, the browser) sits
    behind interfaces the domain defines.
- **New domain concepts** are welcome when the design needs them. Each one has:
  - **a clear purpose:** one sentence in the file's header comment on what it is and
    why it exists;
  - **a contract:** its public interface, its invariants (what is always true of it),
    and how it fails;
  - **unit tests** of that contract, with no infrastructure;
  - **a name** taken from the ADR, used the same way in the code, the README and the
    workbench.

  Don't add an abstraction that has no second use, unless the ADR asks for it.

If the design departs from the ADR's (a different module boundary, interface or
concept), say so and agree it with the user, then record it as a correction.

## 5. Implement

Follow the design, in the plan's order where it matters. For example, record a
fixture before the code it guards changes.

TypeScript, beyond what `tsconfig.json` enforces (strict, `noUncheckedIndexedAccess`,
no enums, no parameter properties):

- **Model with types.** Use a discriminated union for each set of cases (a status, an
  event, a command), with an exhaustive `switch` whose `default` assigns to `never`.
  Don't use stringly-typed fields or boolean flags that only make sense together.
- **No escape hatches:**
  - no `any`;
  - no `as` casts except where untyped data enters (a parsed file, a message from a
    worker), and only after checking it;
  - no non-null `!` unless an invariant guarantees it, stated in a comment next to it.
- **Use `unknown` at the boundaries** and narrow it with type guards.
- **Make data `readonly`** unless it is meant to change. Domain objects share no
  mutable state.
- **Keep the public interface small and explicitly typed.** Exported functions state
  their return types.
- **Errors are typed:** a class per kind of failure the caller handles differently.
  The message says what happened and what to do (`AGENTS.md`, Writing).
- **Keep names consistent:** the same concept gets the same name in types, functions,
  steps, the README and the workbench.

Update `README.md`, and the tutorials if they show what changed, in the same change.

## 6. Verify

- Check each exit criterion as it is written, by the check it names. Record how each
  was shown, with test names or what the browser showed.
- Run the type check (`npx tsc --noEmit -p . --noUnusedLocals --noUnusedParameters`),
  `npm test` and `npm run build`.
- For workbench changes, check them in a browser: run `npm run dev`, and load data on
  the Tables tab first.

A criterion you can't show true is not done. Fix it, or discuss it with the user
(step 3). Don't report it as done.

## 7. Commit and push

Asking for an issue to be implemented is the request to commit and push it
(`AGENTS.md`, Workflow).

- Commit the change on `main` (unless the user named a branch), with `Refs #N` in the
  message. Don't write `Closes #N`: the user closes the issue.
- Push it, and check that the CI run passes. CI tests it and deploys the workbench.

## 8. Independent review

Start a fresh agent, not a fork of this conversation, and have it run the
`review-adr` skill on issue #N. It reviews the pushed commits and posts its review
under the issue.

For each finding in the review:
- **Blocking:** fix it.
- **Should fix:** fix it, unless you can explain why it does not apply.
- **Note:** your choice.

Fix them the same way as the rest of the work (design, implement, verify), commit
with `Refs #N`, and push. Then reply under the issue with each finding by number:
what you changed (with the commit), or why it does not apply.

Ask for a re-review (a fresh agent again, with `review-adr`). Repeat until the verdict
is **Approved**.

## 9. Deliver

Delivery goes to the issue, not to the chat. Comment on the issue
(`gh issue comment N --body-file <file>`) with:

- the commits;
- each exit criterion and how it was shown (a test name, or what the browser showed);
- the corrections made to the ADR, if any;
- a link to the approving review, and any findings left open, with why.

In the chat, give only the comment's link.

The user accepts the work by closing the issue. If they comment instead, read the
comments (`gh issue view N --comments`), make the changes they ask for the same way
(design, implement, verify, commit, review), and deliver again with a comment that
answers theirs.
