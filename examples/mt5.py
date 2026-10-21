import logging
from datetime import UTC, datetime, timedelta

import MetaTrader5 as mt5

from mt5_datahub.providers.mt5 import MT5Provider

SYMBOL = "XAUUSD.s"


def show_ticks(provider: MT5Provider) -> None:
    """Получить тики за последние 5 минут."""
    end = datetime.now(UTC)
    start = end - timedelta(minutes=5)

    ticks = provider.get_ticks(SYMBOL, start, end)

    print(f"\nТиков получено: {len(ticks)}")

    for tick in ticks[:10]:
        time = datetime.fromtimestamp(tick["time_msc"] / 1000, UTC)
        print(f"{time:%H:%M:%S.%f} Bid={tick['bid']:.2f} Ask={tick['ask']:.2f}")


def show_candles(provider: MT5Provider) -> None:
    """Получить последние 24 часа свечей M1."""
    end = datetime.now(UTC)
    start = end - timedelta(hours=24)

    candles = provider.get_candles(SYMBOL, mt5.TIMEFRAME_M1, start, end)

    print(f"\nСвечей получено: {len(candles)}")

    for candle in candles[-10:]:
        time = datetime.fromtimestamp(int(candle["time"]), UTC)
        print(
            f"{time:%Y-%m-%d %H:%M} "
            f"O={candle['open']:.2f} "
            f"H={candle['high']:.2f} "
            f"L={candle['low']:.2f} "
            f"C={candle['close']:.2f}"
        )


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )

    with MT5Provider() as provider:
        show_ticks(provider)
        show_candles(provider)


if __name__ == "__main__":
    main()
