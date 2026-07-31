# Private strategies (git-crypt)

Strategy implementations are **version-controlled but encrypted** in this public repo.
`blueprint_howto.py` is the only strategy file stored in plaintext.

## First-time setup (repo maintainer)

```bash
brew install git-crypt
git crypt init
git crypt export-key ~/.cryo-backtester-git-crypt.key
chmod 600 ~/.cryo-backtester-git-crypt.key
```

Back up `~/.cryo-backtester-git-crypt.key` somewhere safe (password manager, encrypted
backup). If you lose it, encrypted strategy history cannot be recovered.

## Clone on a new machine

```bash
brew install git-crypt
git clone https://github.com/Rick987-beep/CryoBacktester.git
cd CryoBacktester
git crypt unlock ~/.cryo-backtester-git-crypt.key
```

After unlock, `workspace/strategies/**/*.py` (except `blueprint_howto.py`) are normal
plaintext files. Without the key, those paths show as binary blobs in git.

## Optional: unlock via environment variable

```bash
export GIT_CRYPT_KEY=~/.cryo-backtester-git-crypt.key
git crypt unlock
```

## What is encrypted

| Path | Encrypted? |
|------|------------|
| `workspace/strategies/**/*.py` | Yes (canonical strategy code) |
| `workspace/strategies/other/blueprint_howto.py` | No (public) |
| `workspace/strategies/**/__init__.py` | No |
| `workspace/tests/test_*.py` | Yes (strategy-specific) |
| `workspace/tests/test_strategy_base.py` | No |
| `workspace/tests/test_backtester_indicators.py` | No |
| `backtester/strategies/*.py` | No (compatibility shims only) |

## Note on history

Older commits encrypted files under `backtester/strategies/`. After the workspace
split, encrypt **`workspace/strategies/`** going forward (see `.gitattributes`).
`short_str_turb_dyn.py` existed in plaintext in older commits before git-crypt was
enabled. Rewriting history to remove it requires a separate force-push operation.
