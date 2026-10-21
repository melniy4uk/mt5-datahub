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
