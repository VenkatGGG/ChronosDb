# ChronosDB Repository Rules

These rules govern how Codex and other agents should modify this repository.

## Pull Request Workflow

- Do not push directly to `main`.
- Every distinct task or change set must use its own branch and its own pull
  request.
- Do not keep extending an already-open PR with new unrelated work.
- After opening a PR, only push additional commits to that PR if they are
  directly part of the same task or are follow-up changes responding to review.
- If a new user request starts a new task, start a new branch and open a new PR.
- Branch names should match the feature or change being shipped.

## Planning and Scope Discipline

- `IMPLEMENTATION_PLAN.md` is the execution contract.
- `TODOS.md` is the task tracker and should be kept current.
- If a protocol or scope change is needed, update the plan docs first, then
  change code.
- Do not mix current-phase work with future-phase implementation in the same
  commit.
