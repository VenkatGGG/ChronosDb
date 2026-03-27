# ChronosDB Repository Rules

These rules govern how Codex and other agents should modify this repository.

## Git Workflow

- Push commits directly to `main`.
- Do not open PRs unless the user explicitly asks for a PR.
- Keep commits incremental and scoped to one coherent change.
- Commit plan or protocol updates before code that depends on them.

## Planning and Scope Discipline

- `IMPLEMENTATION_PLAN.md` is the execution contract.
- `TODOS.md` is the task tracker and should be kept current.
- If a protocol or scope change is needed, update the plan docs first, then
  change code.
- Do not mix current-phase work with future-phase implementation in the same
  commit.
