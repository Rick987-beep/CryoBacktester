# Data plane

Market history, indicator caches, and backtest run artifacts live here — **not**
in the shippable `backtester/` product package.

| Subdir | Contents |
|---|---|
| `market/` | Deribit options/spot parquet snapshots + `macro/` |
| `klines/` | Binance kline cache used by indicators |
| `runs/` | HTML reports + `*.bundle` UI run artifacts |
| `tardis_raw/` | Optional Tardis ingest output |
| `archive/` | Cold/legacy data blobs |

Override roots with env vars (see `backtester.core.paths`):

- `CRYOBT_MARKET_DATA`
- `CRYOBT_KLINE_DIR` (alias: `CRYOTRADER_KLINE_DIR`)
- `CRYOBT_RUNS`
- `CRYOBT_MACRO_CALENDAR`
- `CRYOBT_TARDIS_RAW`

Do not commit parquet/bundle blobs. Transitional symlinks may exist under
`backtester/data`, `backtester/reports`, and `backtester/indicators/data`.
