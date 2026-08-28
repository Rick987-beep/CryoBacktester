# Marketing reports

Investor-facing strategy HTML (Aureas / Defined Theta 7-chapter shell).

| Product | Report |
|---------|--------|
| **Monopteros** (TuDySho) | [`monopteros_strategy_report.html`](monopteros_strategy_report.html) |
| **Defined Theta** (v18) | [`defined_theta_strategy_report.html`](defined_theta_strategy_report.html) |

## Monopteros

- Strategy ID: `tudysho_monopteros`
- Favourite: run **750**, combo `812903ec6cc9`
- Rebuild from NAV/trades:

```bash
python analysis/marketing/monopteros_strategy_report/build_report.py
# copies into analysis/marketing/monopteros_strategy_report.html
```

Sources and pack live under `monopteros_strategy_report/` (`data/`, `stats.json`,
quality sidecar). Do not commit `data/runs/` bundles here — look them up with
`python -m backtester.inspect`.
