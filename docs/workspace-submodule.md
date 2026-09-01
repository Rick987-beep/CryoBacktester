# Private workspace submodule

Strategy implementations, experiments, marketing artefacts, and strategy tests
are **not** part of the public repo. They live in a private submodule:

**https://github.com/Rick987-beep/CryoBacktester-workspace** (private)

## Clone (maintainer)

```bash
git clone --recurse-submodules https://github.com/Rick987-beep/CryoBacktester.git
cd CryoBacktester
```

If you already cloned without submodules:

```bash
git submodule update --init workspace
```

The sentinel file `workspace/.private` switches `backtester.catalog` to the full
private registry. Without it, only `blueprint_howto` is registered (public fallback).

## Public-only clone

```bash
git clone https://github.com/Rick987-beep/CryoBacktester.git
```

You get the backtester product plus `backtester/strategies/blueprint_howto.py`.
Run the blueprint with:

```bash
python -m backtester.run --strategy blueprint_howto
```

## Tests

```bash
# Product + public catalog
python -m pytest tests/ -v

# Full strategy suite (requires private submodule)
python -m pytest workspace/tests/ -v
```

## git-crypt (retired)

Older commits used git-crypt for `workspace/strategies/`. History was rewritten
when the submodule split landed. Do not re-enable git-crypt on the public repo.
