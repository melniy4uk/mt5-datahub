# mt5-datahub

Библиотека Python для загрузки и локального кэширования **тиков и свечей MetaTrader 5**. Данные возвращаются как структурированные массивы NumPy. Примеры настроены на **Bybit-Live-6 / XAUUSD.s**.

## Установка

Нужны Windows, Python 3.13+, [uv](https://docs.astral.sh/uv/) и установленный, запущенный MT5 с подключённым счётом.

```powershell
git clone https://github.com/melniy4uk/mt5-datahub.git
cd mt5-datahub
uv sync
```

## Получение данных

```python
from datetime import UTC, datetime
import MetaTrader5 as mt5
from mt5_datahub import MarketData

start = datetime(2026, 10, 20, 10, 0, tzinfo=UTC)
end = datetime(2026, 10, 20, 10, 10, tzinfo=UTC)

with MarketData(source="Bybit-Live-6", cache_dir=".mt5-datahub") as data:
    ticks = data.get_ticks("XAUUSD.s", start, end)
    candles = data.get_candles("XAUUSD.s", mt5.TIMEFRAME_M1, start, end)

print(len(ticks), len(candles))
print(ticks.dtype.names, candles.dtype.names)
```

- `get_ticks(symbol, start, end)` — тики (`time_msc`, `bid`, `ask`, `last`, `volume`, `flags` и др.).
- `get_candles(symbol, timeframe, start, end)` — **закрытые** свечи OHLC с объёмами и спредом.
- Времена должны иметь часовой пояс; внутри используются UTC и интервал **[start, end)**. Для свечей границы относятся ко **времени открытия**.
- `MarketData(..., tick_chunk=timedelta(hours=2), candle_chunk=timedelta(days=7))` позволяет настроить размер запросов. Повторные запросы к сохранённым непустым участкам читаются из кэша; незагруженные участки запрашиваются в MT5.
- `source` задаётся вручную и должен различать брокера, сервер и набор данных счёта. Для нескольких источников не переиспользуйте один и тот же `source`.

Для запросов напрямую к терминалу, **без кэширования**:

```python
from mt5_datahub.providers.mt5 import MT5Provider

with MT5Provider() as provider:
    ticks = provider.get_ticks("XAUUSD.s", start, end)
```

Ошибки: `MT5ConnectionError` (терминал/соединение), `MT5RequestError` (запрос), `StorageError` (кэш); все наследуются от `MT5Error` (`mt5_datahub.exceptions`). Некорректные аргументы вызывают `ValueError`.

## Кэш и аудит истории

Кэш хранит массивы в **Parquet**, индекс интервалов — в **SQLite** (`cache_dir`). Данные разделяются по источнику, символу, типу и таймфрейму. Пустые ответы не отмечаются как загруженные; после прерывания ранее сохранённые сегменты переиспользуются.

Аудит тиков проверяет непокрытые запросами интервалы, большие паузы между котировками, повторяющиеся миллисекундные метки и порядок времени:

```powershell
uv run python examples/audit_xauusd.py --sessions examples/bybit-live-6-xauusd-s.sessions.json
```

`audit_ticks(cache, source, symbol, start, end, gap_threshold=..., max_gaps=..., schedule=...)` из `mt5_datahub.quality` возвращает `TickQualityReport`, а `.format()` — текстовый отчёт.

Классификация разрывов: `session_break` — соответствует настроенному перерыву, `unexplained_gap` — существенная часть внутри сессии, `unverified_history` — расписание не передано. Расписание задаётся через `SessionSchedule` / `WeeklySession` (`mt5_datahub.sessions`) или JSON для примера аудита. Файл Bybit составлен по спецификации инструмента в MT5; **соответствие времени спецификации UTC необходимо подтвердить**. Праздники и исключительные закрытия не учитываются.

## Проверки

```powershell
uv run python -m unittest discover -s tests -v
uv run python examples/check_ranges.py
uv run python examples/check_integration.py
uv run python examples/stress_xauusd.py
```

- `tests/` — автоматические тесты SQLite/Parquet, сервиса, аудита и сессий без живого терминала.
- `check_ranges.py` — границы и разделение тиков; `check_integration.py` — кэш тиков и M1 на MT5; `stress_xauusd.py` — многодневная загрузка и восстановление после имитированного сбоя. Примеры с реальным MT5 используют `XAUUSD.s`.

## Ограничения

**Статус: экспериментальная версия.** Непустой ответ MT5 и отсутствие непокрытых интервалов в индексе **не гарантируют полноту истории брокера**. Большие паузы между тиками не доказывают потерю данных. Одновременная запись нескольких процессов в один кэш не поддерживается; для больших объёмов доступны чтение отдельными пакетами и явная компактизация, публичный MarketData.iter_ticks() выдаёт пакеты, а MarketData.get_ticks() по-прежнему возвращает весь результат в память. Библиотека не открывает сделки.

## Быстрое чтение Parquet

Для многомиллионных выборок можно читать только нужные столбцы и получать
небольшие пакеты NumPy, вместо загрузки всего диапазона в память:

```python
from mt5_datahub.storage.parquet import ParquetCache
from mt5_datahub.timeutils import to_us

cache = ParquetCache(".mt5-datahub/audit")
for batch in cache.iter_batches(
    "xauusd-s-history", "XAUUSD.s", "ticks", 0,
    to_us(start), to_us(end),
    columns=("time_msc", "bid", "ask"),
    batch_size=65_536,
):
    process(batch)
```

### Публичный потоковый API: `MarketData.iter_ticks()`

Метод загружает отсутствующие участки из MT5 в Parquet, а затем выдаёт
пакеты структурированных массивов NumPy. В отличие от `get_ticks()`,
не собирает весь результат в оперативной памяти.

```python
from datetime import UTC, datetime
from mt5_datahub import MarketData

start = datetime(2026, 10, 19, tzinfo=UTC)
end = datetime(2026, 10, 21, tzinfo=UTC)

with MarketData("Bybit-Live-6", cache_dir=".mt5-datahub") as data:
    total = 0
    for batch in data.iter_ticks(
        "XAUUSD.s", start, end,
        columns=("time_msc", "bid", "ask"),
        batch_size=65_536,
    ):
        total += len(batch)
    print("Processed ticks:", total)
```

Итератор ленивый: работа начинается при первом проходе по нему.
Перед выдачей первого пакета он дозагружает весь запрошенный диапазон
в кэш; каждый запрос к MT5 ограничен параметром `tick_chunk`. При
повторном вызове успешно кэшированные интервалы не скачиваются.
Пакеты ограничены `batch_size` строками; границы `[start, end)`
и проверка UTC такие же, как у `get_ticks()`. При наложении
сохранённых сегментов пакеты могут не иметь глобального порядка по времени.

`cache.read(..., columns=("time_msc", "bid", "ask"))` возвращает
выбранные столбцы единым массивом. `iter_batches()` экономит оперативную
память, но **не гарантирует глобальную сортировку пакетов при перекрывающихся
файлах**; для задач, требующих полного порядка, используйте `read()`.
Существующий `MarketData.get_ticks()` по-прежнему возвращает полный массив.

Запуск сравнительного теста на ранее сохранённой истории `XAUUSD.s`:

```powershell
uv run python examples/benchmark_cache.py
```

Явная компактизация смежных неперекрывающихся Parquet-сегментов
(изменяет содержимое кэша; перед запуском желательно сделать резервную копию):

```powershell
uv run python examples/benchmark_cache.py --compact
```

Компактизация ограничена 2 млн строк за вызов и предназначена для одного
процесса записи. Сложные перекрытия и отсутствующие интервалы объединять
нельзя: они должны оставаться видимыми для аудита истории.
