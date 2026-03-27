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
