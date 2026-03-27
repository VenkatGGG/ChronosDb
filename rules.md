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
# Codex Rules

These rules are persistent repository instructions for Codex and other coding
agents working in this repo.

## Git Workflow

1. Never push agent-generated changes directly to `main`.
2. Agent-generated changes should be raised through pull requests.
3. Reusing an existing working branch is fine; a brand-new branch is not required
   for every task. Use the `codex/` prefix by default when creating a branch.
4. Once a PR is raised, work may continue without blocking on asynchronous review.
5. If a protocol or architecture change is needed, update docs first in the PR
   before any implementation code.

## Planning Discipline

1. `ARCHITECTURE.md` explains the target system and rationale.
2. `IMPLEMENTATION_PLAN.md` is the execution contract.
3. `TODOS.md` tracks near-term milestones.
4. If code would violate any written contract, update the docs in the same PR
   before changing the code.
