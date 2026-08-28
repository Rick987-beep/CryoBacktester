"""Flexible HTML report from an audit JSON pack (section kit)."""
from __future__ import annotations

import html
from pathlib import Path
from typing import Any, Callable


SectionFn = Callable[[dict[str, Any]], str]


def _esc(x: Any) -> str:
    return html.escape("" if x is None else str(x))


def _fmt_pct(x: Any, digits: int = 1) -> str:
    if x is None:
        return "—"
    try:
        return f"{100 * float(x):.{digits}f}%"
    except (TypeError, ValueError):
        return _esc(x)


def _fmt_num(x: Any, digits: int = 2) -> str:
    if x is None:
        return "—"
    try:
        return f"{float(x):,.{digits}f}"
    except (TypeError, ValueError):
        return _esc(x)


def section_summary(pack: dict[str, Any]) -> str:
    m = pack.get("meta") or {}
    g = pack.get("grid_summary") or {}
    cf = ((pack.get("curve_fit") or {}).get("verdict") or {}).get("level", "?")
    return f"""
<section id="summary">
  <h1>Run audit — {_esc(m.get('strategy'))} · {_esc(m.get('bundle'))}</h1>
  <p class="meta">
    run_id={_esc(m.get('run_id'))} ·
    {_esc(m.get('date_from'))} → {_esc(m.get('date_to'))} ·
    {_esc(m.get('n_combos'))} combos ·
    account ${_esc(m.get('account_size'))}
  </p>
  <div class="stats">
    <div><strong>{_fmt_pct(g.get('pct_profit'))}</strong><span>profitable</span></div>
    <div><strong>{_fmt_num(g.get('median_sharpe'), 2)}</strong><span>median Sharpe</span></div>
    <div><strong>{_esc(g.get('n_unique_economic_outcomes'))}</strong><span>unique outcomes</span></div>
    <div><strong>{_esc(cf)}</strong><span>curve-fit level</span></div>
  </div>
</section>
"""


def section_influence(pack: dict[str, Any]) -> str:
    bars = pack.get("influence_bar") or []
    if not bars:
        return ""
    rows = "".join(
        f"<tr><td>{_esc(b['param'])}</td>"
        f"<td>{_fmt_num(b['eta_sharpe'], 3)}</td>"
        f"<td>{_fmt_num(b['eta_pnl'], 3)}</td>"
        f"<td>{_fmt_num(b['eta_dd'], 3)}</td></tr>"
        for b in bars
    )
    # Expand first param levels
    levels_html = ""
    influence = pack.get("influence") or []
    if influence:
        top = influence[0]
        lr = "".join(
            f"<tr><td>{_esc(L.get('level'))}</td>"
            f"<td>{_fmt_num(L.get('med_sharpe'), 2)}</td>"
            f"<td>{_fmt_num(L.get('med_pnl'), 0)}</td>"
            f"<td>{_fmt_num(L.get('med_dd'), 2)}</td>"
            f"<td>{_fmt_num(L.get('p95_dd'), 2)}</td>"
            f"<td>{_fmt_num(L.get('med_n'), 0)}</td></tr>"
            for L in (top.get("levels") or [])
        )
        levels_html = f"""
        <h3>Levels — {_esc(top['param'])}</h3>
        <table>
          <thead><tr><th>level</th><th>med Sharpe</th><th>med PnL</th>
          <th>med DD%</th><th>p95 DD%</th><th>med n</th></tr></thead>
          <tbody>{lr}</tbody>
        </table>
        """
    return f"""
<section id="influence">
  <h2>1. Parameter influence</h2>
  <p>One-way ANOVA η² — share of cross-cell variance explained by each varying param.</p>
  <table>
    <thead><tr><th>param</th><th>η² Sharpe</th><th>η² PnL</th><th>η² max DD</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
  {levels_html}
</section>
"""


def section_danger(pack: dict[str, Any]) -> str:
    v = pack.get("danger_verdict") or {}
    ranked = (pack.get("danger_rank") or [])[:8]
    rows = "".join(
        f"<tr><td>{_esc(r.get('param'))}</td><td>{_esc(r.get('level'))}</td>"
        f"<td>{_fmt_num(r.get('danger_score'), 2)}</td>"
        f"<td>{_fmt_num(r.get('med_sharpe'), 2)}</td>"
        f"<td>{_fmt_num(r.get('p95_dd'), 2)}</td>"
        f"<td>{_fmt_num(r.get('med_loss_win'), 1)}</td></tr>"
        for r in ranked
    )
    return f"""
<section id="danger">
  <h2>2. Most dangerous settings</h2>
  <p class="callout">{_esc(v.get('headline'))}</p>
  <table>
    <thead><tr><th>param</th><th>level</th><th>danger</th>
    <th>med Sharpe</th><th>p95 DD%</th><th>med |L|/W</th></tr></thead>
    <tbody>{rows}</tbody>
  </table>
</section>
"""


def section_curve_fit(pack: dict[str, Any]) -> str:
    g = pack.get("grid_summary") or {}
    cf = (pack.get("curve_fit") or {}).get("verdict") or {}
    bullets = "".join(f"<li>{_esc(e)}</li>" for e in (cf.get("evidence") or []))
    return f"""
<section id="curve-fit">
  <h2>3. Curve-fitting</h2>
  <p><strong>Level: {_esc(cf.get('level'))}</strong>
     (heuristic score { _esc(cf.get('score')) })</p>
  <ul>{bullets}</ul>
  <div class="stats">
    <div><strong>{_fmt_pct(g.get('effective_grid_shrink'))}</strong><span>duplicate shrink</span></div>
    <div><strong>{_fmt_num(g.get('spearman_h1_h2_pnl'), 2)}</strong><span>H1↔H2 Spearman</span></div>
    <div><strong>{_fmt_pct(g.get('top_decile_perfect_wr_share'))}</strong><span>top-decile perfect WR</span></div>
    <div><strong>{_fmt_pct(g.get('top100_h1_mean_decay_to_h2'))}</strong><span>top H1 PnL decay</span></div>
  </div>
</section>
"""


def section_live(pack: dict[str, Any]) -> str:
    live = pack.get("live_candidates") or {}
    picks = live.get("picks") or []
    if not picks:
        return f"""
<section id="live">
  <h2>4. Live candidates</h2>
  <p>No picks passed the honest filter (pool_size={_esc(live.get('pool_size'))}).
  Relax filters or inspect top_pnl_with_losses in the JSON.</p>
</section>
"""
    cards = []
    for p in picks:
        params = ", ".join(f"{k}={v}" for k, v in (p.get("params") or {}).items())
        cards.append(
            f"""
            <article class="card">
              <h3>{_esc(p.get('archetype'))} · <code>{_esc(p.get('combo_hash'))}</code></h3>
              <p class="params">{_esc(params)}</p>
              <ul>
                <li>PnL ${_fmt_num(p.get('total_pnl'), 0)} · Sharpe {_fmt_num(p.get('sharpe'), 2)}</li>
                <li>DD {_fmt_num(p.get('max_dd_pct'), 2)}% · WR {_fmt_pct(p.get('win_rate'))}
                    · losses { _esc(p.get('n_loss')) } / n { _esc(p.get('n')) }</li>
                <li>H1/H2 ${_fmt_num(p.get('pnl_h1'), 0)} / ${_fmt_num(p.get('pnl_h2'), 0)}</li>
              </ul>
            </article>
            """
        )
    return f"""
<section id="live">
  <h2>4. Live candidates</h2>
  <p>{_esc(live.get('note'))} Pool size: {_esc(live.get('pool_size'))}.</p>
  <div class="cards">{''.join(cards)}</div>
</section>
"""


DEFAULT_SECTIONS: list[tuple[str, SectionFn]] = [
    ("summary", section_summary),
    ("influence", section_influence),
    ("danger", section_danger),
    ("curve_fit", section_curve_fit),
    ("live", section_live),
]


_CSS = """
:root { --bg:#0f1419; --fg:#e7ecf1; --muted:#9aa7b5; --card:#1a2330; --accent:#c9a227; --line:#2a3544; }
* { box-sizing: border-box; }
body { margin:0; font: 15px/1.45 ui-sans-serif, system-ui, sans-serif; background:var(--bg); color:var(--fg); }
main { max-width: 980px; margin: 0 auto; padding: 32px 20px 64px; }
h1,h2,h3 { font-weight: 650; letter-spacing: -0.02em; }
h1 { font-size: 1.6rem; margin: 0 0 8px; }
h2 { font-size: 1.25rem; margin: 32px 0 12px; border-bottom: 1px solid var(--line); padding-bottom: 6px; }
.meta, .params { color: var(--muted); font-size: 0.92rem; }
.stats { display:grid; grid-template-columns: repeat(4,1fr); gap:12px; margin: 16px 0; }
.stats div { background: var(--card); padding: 12px 14px; border-radius: 8px; }
.stats strong { display:block; font-size: 1.25rem; color: var(--accent); }
.stats span { color: var(--muted); font-size: 0.8rem; }
table { width:100%; border-collapse: collapse; font-size: 0.9rem; margin: 12px 0 20px; }
th, td { text-align:left; padding: 6px 8px; border-bottom: 1px solid var(--line); }
th { color: var(--muted); font-weight: 600; }
.callout { background: var(--card); padding: 12px 14px; border-left: 3px solid var(--accent); }
.cards { display:grid; grid-template-columns: repeat(auto-fit,minmax(240px,1fr)); gap:12px; }
.card { background: var(--card); padding: 14px; border-radius: 8px; }
.card h3 { margin: 0 0 8px; font-size: 1rem; }
code { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 0.85em; }
ul { margin: 8px 0; padding-left: 1.2rem; }
@media (max-width: 720px) { .stats { grid-template-columns: 1fr 1fr; } }
"""


def render_html(
    pack: dict[str, Any],
    *,
    sections: list[tuple[str, SectionFn]] | None = None,
    extra_sections: list[tuple[str, SectionFn]] | None = None,
    title: str | None = None,
) -> str:
    """Assemble a self-contained HTML document from section functions.

    Pass ``extra_sections`` for strategy-specific appendices; omit a default
    section by supplying a custom ``sections`` list.
    """
    secs = list(sections if sections is not None else DEFAULT_SECTIONS)
    if extra_sections:
        secs.extend(extra_sections)
    parts = []
    for _, fn in secs:
        chunk = fn(pack)
        if chunk:
            parts.append(chunk)
    body = "".join(parts)
    m = pack.get("meta") or {}
    doc_title = title or f"Run audit — {m.get('strategy', 'run')} {m.get('bundle', '')}"
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>{_esc(doc_title)}</title>
<style>{_CSS}</style>
</head>
<body>
<main>
{body}
<p class="meta">Generated by backtester.research.run_audit · schema v{ _esc(pack.get('schema_version')) }</p>
</main>
</body>
</html>
"""


def write_html(pack: dict[str, Any], path: Path | str, **kwargs: Any) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_html(pack, **kwargs), encoding="utf-8")
    return path
