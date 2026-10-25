#!/usr/bin/env python3
"""Build the scrubbed CryoBacktester-demo tree (+ optional zip).

Usage (from lab repo root):
  python -m scripts.demo_package build
  python -m scripts.demo_package build --zip
  python -m scripts.demo_package build --zip --publish-apps
"""
from __future__ import annotations

import argparse
import hashlib
import os
import re
import shutil
import subprocess
import sys
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

# Lab root = parents[2] from scripts/demo_package/build.py
LAB_ROOT = Path(__file__).resolve().parents[2]
TEMPLATES = Path(__file__).resolve().parent / "templates"
DIST_ROOT = LAB_ROOT / "dist"
DEMO_NAME = "CryoBacktester-demo"
DEMO_DIR = DIST_ROOT / DEMO_NAME

# Blueprint window inside the shipped ~90d slice
BLUEPRINT_DATE_RANGE = ("2026-10-06", "2026-10-09")
BLUEPRINT_ENTRY_DATE = "2026-10-07"

MARKET_DAYS = 90
APPS_HOST = "apps.aureas.xyz"
APPS_IP = "91.107.208.208"
FORBIDDEN_PROD_IP = "46.225.137.92"
FORBIDDEN_PROD_NAME = "TradeUbuHetz"
APPS_WEB_DIR = "/var/www/backtester-demo"
APPS_URL_BASE = f"https://{APPS_HOST}/backtester-demo"

FORBIDDEN_PATTERNS = [
    r"Starnberg",
    r"Lenbach",
    r"TradeUbuHetz",
    r"CryoBacktester-workspace",
    r"theta_engine",
    r"tudysho",
    r"Monopteros",
    r"46\.225\.137\.92",
    r"ccrelaunch",
]

# Paths relative to backtester/ to skip entirely
BACKTESTER_SKIP_DIRS = {
    "archive",
    "compare",
    "__pycache__",
    "data",
    "data_archive",
    "data_spot_rebuilt",
    "reports",
}

# Internal ops scripts under ingest/tardis (Storage Box provisioning, etc.)
TARDIS_SKIP_FILES = {
    "provision_storage_box.sh",
    "start_archive_bulk.sh",
    "start_archive_catchup.sh",
    "retry_missing.sh",
    "run_bulk.sh",
    "TARDIS_ARCHIVE_PLAN.md",
    "TARDIS_DATA_NOTES.md",
    "BULK_DOWNLOAD_PLAN.md",
}

# Test files / dirs to omit (private research fixtures)
TEST_SKIP_NAMES = {
    "tudysho_777.py",
    "test_tudysho_777_workers.py",
    "test_workspace_catalog.py",  # replaced by template
    "test_strategy_families.py",
}

TEXT_SUFFIXES = {
    ".py", ".md", ".txt", ".toml", ".yml", ".yaml", ".sh", ".json",
    ".css", ".html", ".example", ".gitignore", ".gitattributes",
}


def _git_rev() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=LAB_ROOT,
            text=True,
        ).strip()
    except Exception:
        return "unknown"


def _latest_market_day(market: Path) -> date:
    days: list[date] = []
    for p in market.glob("options_*.parquet"):
        try:
            days.append(date.fromisoformat(p.stem.split("_", 1)[1]))
        except ValueError:
            continue
    if not days:
        raise SystemExit(f"No options_*.parquet under {market}")
    return max(days)


def _copy_tree(
    src: Path,
    dst: Path,
    *,
    skip_dir_names: set[str] | None = None,
    skip_file_names: set[str] | None = None,
    skip_suffixes: set[str] | None = None,
) -> int:
    skip_dir_names = skip_dir_names or set()
    skip_file_names = skip_file_names or set()
    skip_suffixes = skip_suffixes or set()
    n = 0
    for root, dirs, files in os.walk(src):
        dirs[:] = [
            d for d in dirs
            if d not in skip_dir_names and not d.startswith(".")
        ]
        rel = Path(root).relative_to(src)
        out_dir = dst / rel
        out_dir.mkdir(parents=True, exist_ok=True)
        for name in files:
            if name in skip_file_names:
                continue
            if name.startswith(".") and name not in {".gitignore"}:
                continue
            if any(name.endswith(sfx) for sfx in skip_suffixes):
                continue
            if name.endswith((".pyc", ".pyo")) or name == ".DS_Store":
                continue
            shutil.copy2(Path(root) / name, out_dir / name)
            n += 1
    return n


def _write_templates(dest: Path) -> None:
    mapping = {
        "README.md": TEMPLATES / "README.md",
        "TRY_ME.md": TEMPLATES / "TRY_ME.md",
        "LICENSE": TEMPLATES / "LICENSE",
        "run_demo.sh": TEMPLATES / "run_demo.sh",
        ".env.example": TEMPLATES / "env.example",
        ".gitignore": TEMPLATES / "gitignore",
        "docs/strategy_howto.md": TEMPLATES / "strategy_howto.md",
        "tests/test_workspace_catalog.py": TEMPLATES / "test_workspace_catalog.py",
    }
    for rel, src in mapping.items():
        out = dest / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_bytes(src.read_bytes())
        if rel.endswith(".sh"):
            out.chmod(out.stat().st_mode | 0o111)


def _patch_blueprint(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    text = text.replace(
        "║  Location: workspace/strategies/other/blueprint_howto.py                     ║",
        "║  Location: backtester/strategies/blueprint_howto.py                           ║",
    )
    text = text.replace(
        "║  Register strategies in workspace/catalog.py (stable IDs; never rename).     ║",
        "║  Register strategies in backtester/public_catalog.py (stable IDs).           ║",
    )
    text = text.replace(
        "# Live consumer: workspace.strategies.theta_engine.v10\n",
        "# Demo package: blueprint only — no private strategy consumers.\n",
    )
    text = text.replace(
        "and no weekday filtering.  Those patterns are shown in short_str_turb_dyn.py.",
        "and no weekday filtering.  Extend this file when adding your own strategy.",
    )
    text = text.replace(
        "# To add an indicator: uncomment and follow the pattern in short_str_turb_dyn.py.",
        "# To add an indicator: see backtester/indicators/pipeline.py.",
    )
    text = text.replace(
        "# see docs/strategy_howto.md §5a and theta_engine_v10 (not wired here).",
        "# see docs/strategy_howto.md (portfolio risk helpers — not wired here).",
    )
    text = re.sub(
        r'DATE_RANGE = \("[^"]+", "[^"]+"\)',
        f'DATE_RANGE = ("{BLUEPRINT_DATE_RANGE[0]}", "{BLUEPRINT_DATE_RANGE[1]}")',
        text,
        count=1,
    )
    text = re.sub(
        r'"entry_date":\s*\[[^\]]+\]',
        f'"entry_date":      ["{BLUEPRINT_ENTRY_DATE}"]',
        text,
        count=1,
    )
    path.write_text(text, encoding="utf-8")


def _patch_text_file(path: Path, replacements: list[tuple[str, str]]) -> None:
    if not path.is_file():
        return
    text = path.read_text(encoding="utf-8")
    orig = text
    for a, b in replacements:
        text = text.replace(a, b)
    if text != orig:
        path.write_text(text, encoding="utf-8")


def _apply_product_patches(dest: Path) -> None:
    _patch_blueprint(dest / "backtester" / "strategies" / "blueprint_howto.py")
    _patch_text_file(
        dest / "backtester" / "strategies" / "__init__.py",
        [
            (
                "Private strategies live in the CryoBacktester-workspace submodule.",
                "Demo package: blueprint_howto only (see backtester/public_catalog.py).",
            ),
        ],
    )
    _patch_text_file(
        dest / "backtester" / "catalog.py",
        [
            (
                '"""True when the private CryoBacktester-workspace submodule is checked out."""',
                '"""True when a private workspace/.private sentinel is present (not in demo)."""',
            ),
        ],
    )
    # Scrub default prod recorder host from ingest helper
    sync = dest / "backtester" / "ingest" / "sync_vps.py"
    if sync.is_file():
        t = sync.read_text(encoding="utf-8")
        t = t.replace("root@46.225.137.92", "user@YOUR_RECORDER_HOST")
        t = t.replace("46.225.137.92", "YOUR_RECORDER_HOST")
        sync.write_text(t, encoding="utf-8")
    _patch_text_file(
        dest / "backtester" / "indicators" / "fomc_decision.py",
        [("baseline Monopteros", "baseline / never block")],
    )
    # Scrub Starnberg naming in calm_nights comments / docs (keep code working)
    for p in (dest / "backtester" / "indicators" / "calm_nights").rglob("*"):
        if p.suffix not in TEXT_SUFFIXES or not p.is_file():
            continue
        t = p.read_text(encoding="utf-8", errors="replace")
        t2 = (
            t.replace("Starnberg", "LocalTier")
            .replace("STARNBERG_", "LOCAL_TIER_")
            .replace("TuDySho", "strategy")
            .replace("Cadysho", "strategy")
        )
        if t2 != t:
            p.write_text(t2, encoding="utf-8")
    # Drop tardis planning docs (internal hosts)
    tardis = dest / "backtester" / "ingest" / "tardis"
    if tardis.is_dir():
        for md in tardis.glob("*.md"):
            md.unlink()
    # pyproject: tests only
    pyproject = dest / "pyproject.toml"
    if pyproject.is_file():
        t = pyproject.read_text(encoding="utf-8")
        t = re.sub(
            r'testpaths\s*=\s*\[[^\]]*\]',
            'testpaths = ["tests"]',
            t,
            count=1,
        )
        pyproject.write_text(t, encoding="utf-8")
    # Patch stray strategy id in UI test
    run_view = dest / "tests" / "ui" / "test_backtester_run_view.py"
    if run_view.is_file():
        t = run_view.read_text(encoding="utf-8")
        t = t.replace('strategy="tudysho"', 'strategy="blueprint_howto"')
        run_view.write_text(t, encoding="utf-8")
    # Drop shim sync test that requires workspace twin
    shims = dest / "tests" / "test_strategy_import_shims.py"
    if shims.is_file():
        t = shims.read_text(encoding="utf-8")
        # Replace workspace twin test with a no-op note
        t = re.sub(
            r"def test_blueprint_matches_workspace_copy_when_present\(\):.*?(?=\ndef |\Z)",
            (
                "def test_blueprint_matches_workspace_copy_when_present():\n"
                '    """Demo package has no workspace twin — skipped."""\n'
                "    return\n\n\n"
            ),
            t,
            count=1,
            flags=re.S,
        )
        shims.write_text(t, encoding="utf-8")


def _copy_market_slice(dest: Path, end: date, days: int) -> tuple[date, date, int]:
    src = LAB_ROOT / "data" / "market"
    out = dest / "data" / "market"
    out.mkdir(parents=True, exist_ok=True)
    start = end - timedelta(days=days - 1)
    n = 0
    for d in (start + timedelta(days=i) for i in range(days)):
        for kind in ("options", "spot_track"):
            name = f"{kind}_{d.isoformat()}.parquet"
            sp = src / name
            if sp.is_file():
                shutil.copy2(sp, out / name)
                n += 1
    (out / "README.txt").write_text(
        f"Demo market slice: {start.isoformat()} → {end.isoformat()} "
        f"({n} parquet files). Options + spot_track only.\n",
        encoding="utf-8",
    )
    return start, end, n


def _scrub_check(dest: Path) -> list[str]:
    hits: list[str] = []
    combined = re.compile("|".join(f"({p})" for p in FORBIDDEN_PATTERNS))
    for p in dest.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix not in TEXT_SUFFIXES and p.name not in {
            "LICENSE", "MANIFEST.txt", "run_demo.sh",
        }:
            continue
        # Allow MANIFEST to mention lab rev only
        try:
            text = p.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        if combined.search(text):
            hits.append(str(p.relative_to(dest)))
    return hits


def _write_manifest(
    dest: Path,
    *,
    market_start: date,
    market_end: date,
    market_files: int,
) -> None:
    lines = [
        f"package: {DEMO_NAME}",
        f"built_utc: {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}",
        f"lab_git_rev: {_git_rev()}",
        f"blueprint_date_range: {BLUEPRINT_DATE_RANGE[0]} → {BLUEPRINT_DATE_RANGE[1]}",
        f"blueprint_entry_date: {BLUEPRINT_ENTRY_DATE}",
        f"market_span: {market_start.isoformat()} → {market_end.isoformat()}",
        f"market_parquet_files: {market_files}",
        f"download_latest: {APPS_URL_BASE}/{DEMO_NAME}-latest.zip",
        f"github: https://github.com/Rick987-beep/{DEMO_NAME}",
    ]
    (dest / "MANIFEST.txt").write_text("\n".join(lines) + "\n", encoding="utf-8")


def build_tree() -> Path:
    if DEMO_DIR.exists():
        shutil.rmtree(DEMO_DIR)
    DEMO_DIR.mkdir(parents=True)

    # Product
    _copy_tree(
        LAB_ROOT / "backtester",
        DEMO_DIR / "backtester",
        skip_dir_names=BACKTESTER_SKIP_DIRS,
        skip_file_names=TARDIS_SKIP_FILES,
    )
    # Tests
    _copy_tree(
        LAB_ROOT / "tests",
        DEMO_DIR / "tests",
        skip_dir_names={"__pycache__"},
        skip_file_names=TEST_SKIP_NAMES,
    )
    # Root files
    for name in ("requirements.txt", "pyproject.toml", ".python-version"):
        src = LAB_ROOT / name
        if src.is_file():
            shutil.copy2(src, DEMO_DIR / name)

    _write_templates(DEMO_DIR)
    _apply_product_patches(DEMO_DIR)

    end = _latest_market_day(LAB_ROOT / "data" / "market")
    m_start, m_end, m_n = _copy_market_slice(DEMO_DIR, end, MARKET_DAYS)
    _write_manifest(
        DEMO_DIR,
        market_start=m_start,
        market_end=m_end,
        market_files=m_n,
    )

    hits = _scrub_check(DEMO_DIR)
    if hits:
        print("SCRUB FAILED — forbidden tokens in:", file=sys.stderr)
        for h in hits[:40]:
            print(f"  {h}", file=sys.stderr)
        if len(hits) > 40:
            print(f"  … and {len(hits) - 40} more", file=sys.stderr)
        raise SystemExit(1)

    print(f"Built {DEMO_DIR}")
    print(f"  market {m_start} → {m_end} ({m_n} files)")
    print(f"  blueprint entry {BLUEPRINT_ENTRY_DATE}")
    return DEMO_DIR


def _clean_runtime(demo_dir: Path) -> None:
    """Remove local venv / run artefacts before zip or git publish."""
    for name in (".venv", "venv", "data/runs", "data/jobs", "logs"):
        p = demo_dir / name
        if p.is_dir():
            shutil.rmtree(p)
        elif p.is_file():
            p.unlink()
    for p in demo_dir.rglob("__pycache__"):
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)


def _should_skip_zip_member(rel: Path) -> bool:
    parts = set(rel.parts)
    if parts & {".venv", "venv", "__pycache__", ".git"}:
        return True
    if rel.parts[:2] == ("data", "runs") or rel.parts[:2] == ("data", "jobs"):
        return True
    return False


def make_zip(demo_dir: Path) -> Path:
    _clean_runtime(demo_dir)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    zip_path = DIST_ROOT / f"{DEMO_NAME}-{stamp}.zip"
    if zip_path.exists():
        zip_path.unlink()
    print(f"Zipping → {zip_path} …")
    with zipfile.ZipFile(
        zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6
    ) as zf:
        for p in sorted(demo_dir.rglob("*")):
            if not p.is_file():
                continue
            rel = p.relative_to(demo_dir)
            if _should_skip_zip_member(rel):
                continue
            zf.write(p, arcname=str(Path(DEMO_NAME) / rel))
    digest = hashlib.sha256(zip_path.read_bytes()).hexdigest()
    sha_path = Path(str(zip_path) + ".sha256")
    sha_path.write_text(f"{digest}  {zip_path.name}\n", encoding="utf-8")
    # latest copies
    latest = DIST_ROOT / f"{DEMO_NAME}-latest.zip"
    latest_sha = DIST_ROOT / f"{DEMO_NAME}-latest.zip.sha256"
    shutil.copy2(zip_path, latest)
    latest_sha.write_text(f"{digest}  {latest.name}\n", encoding="utf-8")
    print(f"  sha256: {digest}")
    print(f"  latest: {latest}")
    return zip_path


def _resolve_apps_ok() -> str:
    """Return apps IP; abort if it resolves to production."""
    # Prefer getent / dig
    ip = ""
    for cmd in (
        ["dig", "+short", APPS_HOST],
        ["getent", "hosts", APPS_HOST],
    ):
        try:
            out = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL)
            for tok in out.replace("\n", " ").split():
                if re.match(r"^\d+\.\d+\.\d+\.\d+$", tok):
                    ip = tok
                    break
            if ip:
                break
        except Exception:
            continue
    if not ip:
        # dscacheutil on macOS
        try:
            out = subprocess.check_output(
                ["dscacheutil", "-q", "host", "-a", "name", APPS_HOST],
                text=True,
            )
            for line in out.splitlines():
                if "ip_address:" in line:
                    ip = line.split(":", 1)[1].strip()
                    break
        except Exception:
            pass
    if not ip:
        raise SystemExit(f"Could not resolve {APPS_HOST}")
    if ip == FORBIDDEN_PROD_IP:
        raise SystemExit(
            f"REFUSED: {APPS_HOST} resolved to production {FORBIDDEN_PROD_IP}"
        )
    # Confirm SSH hostname is not TradeUbuHetz
    try:
        hn = subprocess.check_output(
            [
                "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10",
                f"root@{ip}", "hostname",
            ],
            text=True,
        ).strip()
    except subprocess.CalledProcessError as e:
        raise SystemExit(f"SSH to root@{ip} failed: {e}") from e
    if hn == FORBIDDEN_PROD_NAME or FORBIDDEN_PROD_IP in hn:
        raise SystemExit(f"REFUSED: remote hostname looks like production ({hn})")
    if ip != APPS_IP:
        print(f"WARNING: {APPS_HOST} → {ip} (expected {APPS_IP}); continuing after hostname check ({hn})")
    print(f"Apps host OK: {APPS_HOST} → {ip} ({hn})")
    return ip


def publish_apps(zip_path: Path) -> list[str]:
    ip = _resolve_apps_ok()
    latest = DIST_ROOT / f"{DEMO_NAME}-latest.zip"
    files = [
        zip_path,
        Path(str(zip_path) + ".sha256"),
        latest,
        DIST_ROOT / f"{DEMO_NAME}-latest.zip.sha256",
    ]
    for f in files:
        if not f.is_file():
            raise SystemExit(f"Missing {f}")

    # Ensure remote dir + nginx location
    remote = f"root@{ip}"
    subprocess.check_call(
        ["ssh", "-o", "BatchMode=yes", remote, f"mkdir -p {APPS_WEB_DIR}"],
    )
    _ensure_nginx_location(remote)
    for f in files:
        subprocess.check_call(
            ["scp", "-o", "BatchMode=yes", str(f), f"{remote}:{APPS_WEB_DIR}/"],
        )
    urls = [
        f"{APPS_URL_BASE}/{zip_path.name}",
        f"{APPS_URL_BASE}/{DEMO_NAME}-latest.zip",
        f"{APPS_URL_BASE}/{DEMO_NAME}-latest.zip.sha256",
    ]
    print("Published:")
    for u in urls:
        print(f"  {u}")
    return urls


def _ensure_nginx_location(remote: str) -> None:
    """Insert location /backtester-demo/ into apps.aureas.xyz nginx if missing."""
    conf = "/etc/nginx/sites-enabled/buy-rent-calculator"
    check = subprocess.run(
        ["ssh", "-o", "BatchMode=yes", remote, f"grep -q 'location /backtester-demo/' {conf}"],
    )
    if check.returncode == 0:
        print("nginx location /backtester-demo/ already present")
        return
    patch_script = DIST_ROOT / "_nginx_patch_backtester_demo.py"
    patch_script.write_text(
        f"""\
from pathlib import Path
p = Path({conf!r})
t = p.read_text()
needle = "    # React SPA"
block = '''
    location /backtester-demo/ {{
        alias /var/www/backtester-demo/;
        autoindex on;
        types {{ application/zip zip; }}
        add_header Cache-Control "public, max-age=300";
    }}
'''
if "location /backtester-demo/" in t:
    print("already present")
    raise SystemExit(0)
if needle not in t:
    raise SystemExit("needle not found in nginx conf")
p.write_text(t.replace(needle, block + "\\n" + needle, 1))
print("nginx patched")
""",
        encoding="utf-8",
    )
    remote_py = "/tmp/_nginx_patch_backtester_demo.py"
    subprocess.check_call(
        ["scp", "-o", "BatchMode=yes", str(patch_script), f"{remote}:{remote_py}"],
    )
    subprocess.check_call(
        ["ssh", "-o", "BatchMode=yes", remote, f"python3 {remote_py} && rm -f {remote_py}"],
    )
    subprocess.check_call(
        ["ssh", "-o", "BatchMode=yes", remote, "nginx -t && systemctl reload nginx"],
    )
    print("nginx reloaded with /backtester-demo/")


def verify_download(url: str) -> None:
    import urllib.request

    req = urllib.request.Request(url, method="HEAD")
    with urllib.request.urlopen(req, timeout=30) as resp:
        code = resp.status
        length = resp.headers.get("Content-Length", "?")
        ctype = resp.headers.get("Content-Type", "?")
    if code != 200:
        raise SystemExit(f"Download check failed: {url} → HTTP {code}")
    print(f"Download OK: {url} (HTTP {code}, {length} bytes, {ctype})")


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="scripts.demo_package")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_build = sub.add_parser("build", help="Build demo tree under dist/")
    p_build.add_argument("--zip", action="store_true", help="Also create zip + sha256")
    p_build.add_argument(
        "--publish-apps",
        action="store_true",
        help="Upload zip to apps.aureas.xyz (implies --zip)",
    )
    p_build.add_argument(
        "--verify-download",
        action="store_true",
        help="HEAD-check the latest zip URL after publish",
    )

    sub.add_parser("publish-apps", help="Upload existing dist zip to apps.aureas.xyz")

    args = parser.parse_args(list(argv) if argv is not None else None)

    if args.cmd == "build":
        demo = build_tree()
        zip_path = None
        if args.zip or args.publish_apps:
            zip_path = make_zip(demo)
        if args.publish_apps:
            assert zip_path is not None
            urls = publish_apps(zip_path)
            if args.verify_download:
                verify_download(f"{APPS_URL_BASE}/{DEMO_NAME}-latest.zip")
            print(urls[1])
        return 0

    if args.cmd == "publish-apps":
        # Prefer dated zip from today, else latest
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
        zip_path = DIST_ROOT / f"{DEMO_NAME}-{stamp}.zip"
        if not zip_path.is_file():
            zip_path = DIST_ROOT / f"{DEMO_NAME}-latest.zip"
        if not zip_path.is_file():
            raise SystemExit("No zip in dist/ — run: python -m scripts.demo_package build --zip")
        urls = publish_apps(zip_path)
        verify_download(f"{APPS_URL_BASE}/{DEMO_NAME}-latest.zip")
        print(urls[1])
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
