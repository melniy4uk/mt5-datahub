# Руководство по mt5-datahub

Практические сценарии для версии **0.2.0**. Полные сигнатуры — в [API.md](API.md). Библиотека **только читает исторические данные** и не выполняет торговых операций.

## 1. Перед запуском

1. **Windows x64** и **Python 3.13/3.14** (версия проекта в `.python-version` — 3.14); нужен [uv](https://docs.astral.sh/uv/).
2. Установите терминал **MetaTrader 5**, войдите в счёт и убедитесь, что нужный символ и история доступны.
3. В PowerShell выполните:

```powershell
git clone https://github.com/melniy4uk/mt5-datahub.git
cd mt5-datahub
uv sync --locked
uv run python examples/mt5.py
```

Последняя команда проверяет **прямой доступ** к MT5 без кэша. В примере используется `XAUUSD.s`; у вашего брокера имя может отличаться. Пакет пока не опубликован в PyPI.

## 2. Основной сценарий: загрузка → кэш → анализ

```python
from datetime import UTC, datetime, timedelta

import MetaTrader5 as mt5

from mt5_datahub import MarketData, inspect_cache
from mt5_datahub.quality import audit_ticks

SYMBOL = "XAUUSD.s"
START = datetime(2026, 10, 20, 10, tzinfo=UTC)
END = START + timedelta(minutes=10)

with MarketData(cache_dir=".mt5-datahub") as hub:
    source = hub.source  # Определён автоматически после подключения к MT5.
    print("Источник:", source)
    ticks = hub.get_ticks(SYMBOL, START, END)
    candles = hub.get_candles(SYMBOL, mt5.TIMEFRAME_M1, START, END)

    # Повторный запрос использует кэш для уже сохранённого интервала.
    again = hub.get_ticks(SYMBOL, START, END)
    print("Тики / повторно / M1:", len(ticks), len(again), len(candles))

status = inspect_cache(".mt5-datahub", source, SYMBOL, START, END)
print("Не запрошенные участки:", status.unrequested)
print("Повреждения Parquet:", status.corrupt_segments)

report = audit_ticks(".mt5-datahub", source, SYMBOL, START, END)
print(report.format())
```

Этот сценарий также доступен как исполняемый файл с параметрами:

```powershell
uv run python examples/workflow.py --symbol XAUUSD.s
uv run python examples/workflow.py --symbol XAUUSD.s --start 2026-10-20T10:00:00+00:00 --minutes 30
```

**Как это работает:** первый запрос скачивает доступную историю и сохраняет её в Parquet; SQLite индексирует сохранённые временные участки. Повторный запрос читает их локально. При запросе большего диапазона скачиваются недостающие участки. **Пустые ответы MT5 не считаются покрытием**, поэтому повторный запрос пустого участка может снова обращаться к терминалу.

### Источник данных и время

- **По умолчанию `source` не нужен.** После подключения к MT5 библиотека читает `server` и `login` и присваивает имя `mt5:<server>/<login>`. Например, `mt5:Bybit-Live-6/12345678` (номер счёта условный). `hub.source` доступен **после подключения**, а его значение можно передавать в offline-функции `inspect_cache`, `audit_ticks` и сверки.
- Когда использовать `source="my-history"`: для доступа к **уже существующему** кэшу под прежним названием или если требуется своё имя. Старые сегменты не переименовываются автоматически; привязка нового источника к счёту защищает от смешивания данных. При отсутствии информации о счёте **автоматический** режим вызывает `MT5ConnectionError` — используйте подключённый счёт или явный `source`. Номер счёта виден в локальном индексе и может попадать в логи; пароль не хранится.
- Если переключить счёт в том же экземпляре `MarketData()`, автоматический режим **откажет** даже при полностью заполненном кэше. Для другого счёта создайте новый `MarketData`.
- `start` и `end` — `datetime` с часовым поясом. Используйте `UTC`; диапазон **[start, end)**. Для свечей это время **открытия**.
- Библиотека не выдаёт **ещё формирующиеся свечи** и не индексирует их как готовые.
- `get_ticks()` возвращает полный массив `numpy.ndarray` в память, без удаления тиков с одинаковыми миллисекундными метками.

## 3. Большие диапазоны: две стратегии

**`iter_ticks()`** сначала дозагружает **весь** отсутствующий диапазон, затем читает его из Parquet пакетами. Подходит для повторяемого анализа архива.

```python
with MarketData() as hub:
    for batch in hub.iter_ticks(
        "XAUUSD.s", START, END,
        columns=("time_msc", "bid", "ask"),
        batch_size=10_000,
    ):
        # Обрабатывайте пакет здесь, без накопления всех тиков.
        print(len(batch), batch["bid"].mean())
```

**`stream_ticks()`** скачивает, сохраняет и выдаёт **очередной временной участок** по мере чтения. Если остановить цикл, ещё не пройденные интервалы **не скачиваются**. При новом запуске сохранённое используется повторно.

```python
with MarketData(
    tick_chunk=timedelta(minutes=5),
    max_tick_rows=100_000,
) as hub:
    for batch in hub.stream_ticks(
        "XAUUSD.s", START, END,
        columns=("time_msc", "bid"),
        batch_size=10_000,
    ):
        print(len(batch))
```

`batch_size` ограничивает **размер выдаваемого NumPy-пакета**, `max_tick_rows` — **один нативный запрос тиков MT5**, а `tick_chunk` — временной участок загрузки. Это **не** предел потребления памяти всего процесса. Если отдельная секунда содержит больше тиков, чем `max_tick_rows`, безопасная разбивка невозможна — получите ошибку без ложной записи полного покрытия.

## 4. Прямое чтение без кэша

```python
from mt5_datahub.providers.mt5 import MT5Provider

with MT5Provider() as provider:
    ticks = provider.get_ticks("XAUUSD.s", START, END)
    candles = provider.get_candles("XAUUSD.s", mt5.TIMEFRAME_M1, START, END)
```

`MT5Provider` работает с API терминала, но ничего не сохраняет. Для запросов тиков ограниченными страницами есть `iter_tick_pages(..., page_size=50_000)`; он возвращает тройки `(rows, left_us, right_us)`. Если передали `provider=` в `MarketData`, управлять закрытием этого **внешнего** провайдера должен вызывающий код.

## 5. Контроль качества истории

### Проверка локального кэша — без терминала

В примерах ниже переменная `source` — сохранённое значение `hub.source` из основного сценария. В offline-режиме MT5 подключать не нужно, **но автоматически определить источник без MT5 невозможно**.

```python
from mt5_datahub import inspect_cache

status = inspect_cache(
    ".mt5-datahub", source, SYMBOL, START, END,
    kind="ticks",
)
print(status.saved_request_coverage)  # Все ли интервалы запроса сохранены
print(status.files_readable)          # Нет ли обнаруженных повреждений
print(status.unrequested)            # Список [start, end)
print(status.corrupt_segments)       # Файл -> причина
```

Для свечей задайте `kind="candles", timeframe=mt5.TIMEFRAME_M1`. Эти флаги описывают **состояние запросов и файлов**, не полноту истории брокера.

### Аудит тиков

```python
from datetime import timedelta
from mt5_datahub.quality import audit_ticks

report = audit_ticks(
    ".mt5-datahub", source, SYMBOL, START, END,
    gap_threshold=timedelta(minutes=5),
    max_gaps=20,
)
print(report.format())
```

Аудит показывает количество тиков и сегментов, непокрытые интервалы, повторы `time_msc`, нарушения порядка и крупные паузы. Повторяющиеся метки времени **не равны** дублированным записям.

Можно передать `schedule=SessionSchedule((WeeklySession(...), ...))`, чтобы пометить паузы как `session_break` / `unexplained_gap`; без расписания — `unverified_history`. Расписание **нужно задать вручную в UTC и проверить у брокера**. Пример JSON: [sessions.example.json](../examples/sessions.example.json).

```powershell
uv run python examples/audit_xauusd.py
uv run python examples/audit_xauusd.py --sessions examples/sessions.example.json
```

### Сверка с новым ответом MT5

```python
from mt5_datahub.providers.mt5 import MT5Provider
from mt5_datahub.verification import compare_cached_ticks
from mt5_datahub.ohlc_check import compare_tick_ohlc_to_m1

with MT5Provider() as provider:
    ticks_check = compare_cached_ticks(
        ".mt5-datahub", source, SYMBOL, START, END, provider,
        window=timedelta(minutes=15),
    )
    print(ticks_check.format())

    m1_check = compare_tick_ohlc_to_m1(
        ".mt5-datahub", source, SYMBOL, START, END, provider,
        tolerance=0.01,
    )
    print(m1_check.format())
```

`compare_cached_ticks()` проверяет все поля тиков и **сохраняет количество записей с совпадающим `time_msc`**. `compare_tick_ohlc_to_m1()` восстанавливает минутный OHLC из **Bid** сохранённых тиков и сверяет с M1 брокера. Для M1 диапазон должен быть выровнен по минутам и полностью закрыт. Оба инструмента **не изменяют кэш**. Совпадение с **тем же брокером** не является независимым доказательством полноты истории.

Готовые проверки: `examples/compare_xauusd_history.py`, `examples/check_tick_ohlc_live.py`.

## 6. Исправление и обслуживание кэша

### Если брокер обновил историю

```python
with MarketData(cache_dir=".mt5-datahub") as hub:
    corrected = hub.refresh_ticks(
        SYMBOL, START, END,
        confirm=True,
        # allow_changes=True,  # Только если сознательно принимаете изменения
    )
```

Для свечей используйте `refresh_candles(SYMBOL, mt5.TIMEFRAME_M1, START, END, confirm=True)`. Обновление **сначала скачивает и проверяет** данные, затем заменяет затронутые индексированные сегменты. По умолчанию пропажа или изменение старых записей блокируется; `allow_changes=True` разрешает принять новую брокерскую историю. `max_rows` ограничивает размер одной операции. Старые Parquet-файлы могут остаться для восстановления.

### Если Parquet повреждён

```python
from mt5_datahub.storage.parquet import ParquetCache
from mt5_datahub.timeutils import to_us

cache = ParquetCache(".mt5-datahub")
a, b = to_us(START), to_us(END)
issues = cache.verify_segments(source, SYMBOL, "ticks", 0, a, b)
print(issues)

# После резервного копирования и проверки:
if issues:
    cache.invalidate_corrupt(
        source, SYMBOL, "ticks", 0, a, b, confirm=True,
    )
# Теперь MarketData может заново запросить исключённые участки.
```

`invalidate_corrupt` исключает сломанные записи из SQLite, **не удаляя файлы**. `compact(..., max_rows=...)` объединяет только **смежные и неперекрывающиеся** сегменты; это не восстановление отсутствующих тиков.

```python
orphans = cache.orphan_files()
print(orphans)
# Безвозвратное удаление — только после бэкапа и проверки:
# cache.prune_orphans(confirm=True)
```

**Важное правило:** для одного `cache_dir` допустим только **один процесс-писатель**. Не запускайте одновременно загрузку, refresh, compact или очистку. Существующие файлы не всегда можно восстановить из MT5, поэтому перед удалениями нужен бэкап.

## 7. Диагностика и готовые проверки

```powershell
uv run python -m unittest discover -s tests -v
uv run python examples/check_integration.py
uv run python examples/check_stream_live.py
uv run python examples/check_tick_pages_live.py
uv run python examples/benchmark_cache.py
uv run python examples/stress_xauusd.py
```

Offline-тесты из `tests/` не требуют живой MT5. Скрипты с запросами требуют терминала и истории `XAUUSD.s`, часто фиксированных дат и счёта конкретного брокера — при необходимости адаптируйте исходные параметры. **Ни один из примеров не выставляет ордера.**

Частые причины проблем: неверное имя символа; нет подключения к брокеру; время без `tzinfo`; история не загружена/не предоставляется брокером; лимит терминала **Max. bars in chart** (для свечей); смена счёта после определения автоматического `source`. При исключениях смотрите классы из [API.md](API.md#7-исключения-и-логирование).

**Ограничение качества:** успешное чтение из MT5, заполненный индекс кэша и повторная сверка с тем же сервером — это проверка **доступной истории**, а не гарантия всех когда-либо существовавших тиков.

[Основная страница](../README.md) · [Справочник API](API.md)
