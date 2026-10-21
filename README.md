# mt5-datahub

Python library for historical MetaTrader 5 ticks and candles.

## Status

**Experimental development branch:** caching and completeness semantics are
under development. Do not rely on this branch for production trading or
complete historical datasets.

## Requirements

Windows with a working MetaTrader 5 terminal, Python 3.13+, and uv.

## Install

```powershell
uv sync
```

## Planned API

```python
from datetime import UTC, datetime
from mt5_datahub import MarketData

with MarketData(source="broker-server-account") as data:
    ticks = data.get_ticks(
        "XAUUSD.s",
        datetime(2026, 10, 20, tzinfo=UTC),
        datetime(2026, 10, 21, tzinfo=UTC),
    )
```

`source` must uniquely identify the broker/account history; this value
is currently supplied manually.

## Limitations

- MT5 does not guarantee complete historical availability.
- Empty responses are not marked as covered.
- Partial responses may still contain unknown gaps.
- Concurrent writers and cache compaction are not implemented.
- Integration tests require Windows and a running MT5 terminal.

## Historical tick-quality audit

The on-demand audit for `XAUUSD.s` downloads (or reuses) historical data
and prints stored-request coverage, quote timestamp gaps and repeated timestamps:

```powershell
uv run python examples/audit_xauusd.py
```

The report is diagnostic: missing stored request ranges show that some
intervals have not been cached, while large inter-tick gaps *may* reflect
weekends, market closures, broker trading sessions or incomplete broker
history. Neither metric proves that the tick feed is complete.

To audit a custom existing cache without downloading anything:

```python
from datetime import UTC, datetime, timedelta
from mt5_datahub.quality import audit_ticks

report = audit_ticks(
    ".mt5-datahub/audit", "xauusd-s-history", "XAUUSD.s",
    datetime(2026, 10, 19, tzinfo=UTC),
    datetime(2026, 10, 21, tzinfo=UTC),
    gap_threshold=timedelta(minutes=5),
)
print(report.format())
```
