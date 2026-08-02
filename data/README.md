# Data plane

Market history, indicator caches, and backtest run artifacts live here — **not**
in the shippable `backtester/` product package.

| Subdir | Contents |
|---|---|
| `market/` | Deribit options/spot parquet snapshots (+ nested `macro/` calendars) |
| `macro/` | Synced macro series (e.g. `deribit/BTC_DVOL/` hive partitions) |
| `klines/` | Binance kline cache used by indicators |
| `runs/` | HTML reports + `*.bundle` UI run artifacts |
| `tardis_raw/` | Optional Tardis ingest output |
| `archive/` | Cold/legacy data blobs |

Override roots with env vars (see `backtester.core.paths`):

- `CRYOBT_MARKET_DATA`
- `CRYOBT_KLINE_DIR` (alias: `CRYOTRADER_KLINE_DIR`)
- `CRYOBT_RUNS`
- `CRYOBT_MACRO` / `CRYOBT_DVOL`
- `CRYOBT_MACRO_CALENDAR`
- `CRYOBT_TARDIS_RAW`

Refresh Deribit DVOL from CryoQuant (no runtime CQ import):

```bash
python -m backtester.ingest.sync_dvol
```

Do not commit parquet/bundle blobs. Transitional symlinks may exist under
`backtester/data`, `backtester/reports`, and `backtester/indicators/data`.
