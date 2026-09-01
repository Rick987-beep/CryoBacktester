# Solo workflow (CryoBacktester)

One person, one current version on **`main`**, private strategies in a submodule.

## Repos

| Repo | Role |
|------|------|
| **CryoBacktester** (public) | Engine, UI, blueprint — branch **`main`** |
| **CryoBacktester-workspace** (private) | Strategies, experiments, marketing — branch **`main`** |

## First clone (new machine)

```bash
git clone --recurse-submodules https://github.com/Rick987-beep/CryoBacktester.git
cd CryoBacktester
python3.12 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
```

If you forgot `--recurse-submodules`:

```bash
git submodule update --init workspace
```

## Daily work

**Product / engine changes** — commit in the main repo:

```bash
cd CryoBacktester
# edit backtester/…
python -m pytest tests/ -v
git add … && git commit -m "…"
git push origin main
```

**Strategy / marketing changes** — commit inside the submodule, then bump the pointer in the parent:

```bash
cd CryoBacktester/workspace
# edit strategies/… or marketing/…
python -m pytest tests/ -v          # from repo root: workspace/tests/
git add … && git commit -m "…"
git push origin main                # pushes private workspace repo

cd ..
git add workspace
git commit -m "Bump workspace submodule."
git push origin main
```

## Checkpoints

- **`main`** on both repos = your saved checkpoints (push after each logical step).
- **`wip/<topic>`** branches = optional experiments; merge to `main` when done, then delete the branch.
- **`analysis/`** and **`data/runs/`** stay local — not committed (see `.gitignore`).

## Worktrees (optional)

Use when you want two branches checked out at once (e.g. a long run on one branch while fixing something on another):

```bash
git worktree add ../CryoBacktester.wip-foo -b wip/foo
cd ../CryoBacktester.wip-foo
git submodule update --init workspace
```

Remove when finished:

```bash
git worktree remove ../CryoBacktester.wip-foo
git branch -d wip/foo
```

After a **history rewrite** on `main`, old worktrees may point at obsolete commits — remove and recreate them.

## Tests

```bash
python -m pytest tests/ -v                 # public product (always)
python -m pytest workspace/tests/ -v       # strategies (needs submodule)
```

## Submodule docs

See [workspace-submodule.md](workspace-submodule.md) for layout and security notes.
