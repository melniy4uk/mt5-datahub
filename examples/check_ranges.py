from datetime import UTC, datetime

from mt5_datahub.providers.mt5 import MT5Provider

SYMBOL = "XAUUSD.s"

start = datetime(2026, 10, 20, 10, 0, tzinfo=UTC)
middle = datetime(2026, 10, 20, 10, 5, tzinfo=UTC)
end = datetime(2026, 10, 20, 10, 10, tzinfo=UTC)

with MT5Provider() as provider:
    first = provider.get_ticks(SYMBOL, start, middle)
    second = provider.get_ticks(SYMBOL, middle, end)
    full = provider.get_ticks(SYMBOL, start, end)

    print("Первая часть:", len(first))
    print("Вторая часть:", len(second))
    print("Полный диапазон:", len(full))

    print("Сумма частей:", len(first) + len(second))

    print("Совпадают:", len(first) + len(second) == len(full))

    if len(first) and len(second):
        print("Первая часть заканчивается:", first[-1]["time_msc"])
        print("Вторая часть начинается:", second[0]["time_msc"])
