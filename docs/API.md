# Справочник API — mt5-datahub 0.2.0

Краткая карта существующих интерфейсов; примеры работы — в [USAGE.md](USAGE.md). Импорт основного API: `from mt5_datahub import MarketData, inspect_cache, CacheInspection`. Методы с префиксом `_` считаются внутренними.

## 1. `MarketData` — загрузка + кэш

```python
from datetime import timedelta
from mt5_datahub import MarketData

hub = MarketData(
    source="broker/account",           # обязательный идентификатор набора
    cache_dir=".mt5-datahub",          # Parquet + SQLite
    provider=None,                     # по умолчанию свой MT5Provider
    tick_chunk=timedelta(hours=2),     # размер временного участка тиков
    candle_chunk=timedelta(days=7),    # размер участка свечей
    max_tick_rows=250_000,             # максимум тиков за нативный MT5-вызов
)
```

`with MarketData(...) as hub:` вызывает `connect()` / `disconnect()`. Если передан **внешний** `provider`, `MarketData` его не закрывает. Атрибуты: `hub.source`, `hub.cache` (`ParquetCache`).

| Метод | Результат | Когда использовать |
| --- | --- | --- |
| `get_ticks(symbol, start, end)` | `numpy.ndarray` | Все доступные тики диапазона **в памяти** |
| `get_candles(symbol, timeframe, start, end)` | `numpy.ndarray` | Только **закрытые** свечи |
| `iter_ticks(symbol, start, end, *, columns=None, batch_size=65536)` | Итератор `ndarray` | Сначала скачать пробелы **всего** диапазона, затем читать пакетами |
| `stream_ticks(symbol, start, end, *, columns=None, batch_size=65536)` | Итератор `ndarray` | Загружать/отдавать **очередной** временной участок по мере чтения |
| `refresh_ticks(symbol, start, end, *, confirm=False, allow_changes=False, max_rows=2_000_000)` | `ndarray` | Сверить заново и заменить сохранённую историю тиков |
| `refresh_candles(symbol, timeframe, start, end, *, confirm=False, allow_changes=False, max_rows=2_000_000)` | `ndarray` | Аналогично для свечей |
| `connect()`, `disconnect()` | `None` | Явное управление соединением без `with` |

`columns` — кортеж существующих полей тика (например, `("time_msc", "bid")`), без повторов. `batch_size >= 1`. Пустой ответ MT5 не записывается как готовый участок. Для `refresh_*` требуется `confirm=True`; изменение/удаление исторических строк допускается только с `allow_changes=True`. Операция refresh может включать **весь затронутый Parquet-сегмент**, а не только выбранные пользователем границы.

Все даты — `datetime` с `tzinfo`, приводятся к **UTC**; интервал **[start, end)**. `start < end`. Запросы к будущему ограничиваются текущим временем, запросы свечей — началом формирующегося бара. Пустой итоговый массив возможен и **не означает**, что брокер подтвердил отсутствие котировок.

## 2. `MT5Provider` — прямой MT5 без кэша

```python
from mt5_datahub.providers.mt5 import MT5Provider

with MT5Provider() as provider:
    ticks = provider.get_ticks(symbol, start, end)
    candles = provider.get_candles(symbol, timeframe, start, end)
    for rows, left_us, right_us in provider.iter_tick_pages(
        symbol, start, end, page_size=50_000
    ):
        ...
```

- `connect()` / `disconnect()` и контекстный менеджер; проверка соединения и **одна попытка восстановления** при подтверждённом разрыве.
- `get_ticks(...)` — MT5 `copy_ticks_range`, `COPY_TICKS_ALL`; `get_candles(...)` — `copy_rates_range`.
- `iter_tick_pages(..., page_size=50_000)` — порции с целочисленными границами `left_us` и `right_us` (микросекунды UTC). Сохраняет разные тики с одинаковой меткой времени и отказывает в небезопасной разбивке одной секунды.
- `source_identity() -> str | None` — идентификатор текущего `server/login`; `None`, если информация о счёте недоступна.
- Не хранит данные и **не управляет кэшем**. Не гарантирует полноту истории, которую отдаёт сам брокер.

Для свечей поддерживаются MT5-константы: `TIMEFRAME_M1/M2/M3/M4/M5/M6/M10/M12/M15/M20/M30`, `H1/H2/H3/H4/H6/H8/H12`, `D1`, `W1`, `MN1`. Константы берите из `import MetaTrader5 as mt5`.

## 3. Формат результатов

**Тики** — структурированный `numpy.ndarray` (не pandas DataFrame):

| Поле | Тип | Значение |
| --- | --- | --- |
| `time` | `int64` | Время Unix, секунды UTC |
| `time_msc` | `int64` | Время Unix, **миллисекунды** UTC |
| `bid`, `ask`, `last` | `float64` | Котировки |
| `volume` | `uint64` | Объём тика |
| `flags` | `uint32` | Флаги изменения тика MT5 |
| `volume_real` | `float64` | Реальный объём |

**Свечи**:

| Поле | Тип | Значение |
| --- | --- | --- |
| `time` | `int64` | **Открытие** свечи, секунды Unix UTC |
| `open`, `high`, `low`, `close` | `float64` | OHLC |
| `tick_volume`, `real_volume` | `int64` | Объёмы |
| `spread` | `int32` | Спред |

У свечей беззнаковые объёмы, полученные от MT5, преобразуются с **проверкой переполнения**. Равные `time_msc` у разных тиков **не удаляются**.

```python
print(ticks.dtype.names)
print(ticks[["time_msc", "bid", "ask"]][:5])
print(candles[["time", "close"]][-5:])
```

## 4. Состояние кэша и аудит

```python
from mt5_datahub import inspect_cache
from mt5_datahub.quality import audit_ticks
```

| Функция / тип | Параметры и назначение |
| --- | --- |
| `inspect_cache(cache, source, symbol, start, end, *, kind="ticks", timeframe=0)` | Без MT5 проверяет непокрытые запросами интервалы и повреждённые файлы |
| `CacheInspection` | `source`, `symbol`, `kind`, `timeframe`, `start`, `end`, `unrequested`, `corrupt_segments`; свойства `saved_request_coverage`, `files_readable` |
| `audit_ticks(cache, source, symbol, start, end, *, gap_threshold=timedelta(minutes=5), max_gaps=20, schedule=None)` | Без MT5 считает строки, паузы, повторы времени, нарушения порядка |
| `TickQualityReport` | `ticks`, `segments`, `uncovered`, `large_gaps`, `repeated_timestamps`, `out_of_order_timestamps`, `cached_request_coverage`; метод `format()` |
| `TickGap` | `before`, `after`, `seconds`, `classification` |

`cache` для диагностических функций — путь `str | Path` **или** объект `ParquetCache`. `max_gaps` ограничивает только количество выводимых самых длинных пауз, **не подсчёт тиков**.

### Расписание торговых сессий

```python
from datetime import time
from mt5_datahub.sessions import SessionSchedule, WeeklySession, classify_gap

schedule = SessionSchedule(
    sessions=(
        WeeklySession(weekday=0, opens=time(1, 0), closes=time(23, 58)),
    ),
    name="Пример, НЕ подтверждённый брокером",
)
```

`weekday`: понедельник=0 … воскресенье=6. Время — **UTC, без tzinfo**, интервалы могут пересекать полночь. `schedule.open_seconds_during(start, end)` считает секунды пересечения с заданными торговыми интервалами. `classify_gap(start, end, schedule)` возвращает `session_break`, `unexplained_gap` или `unverified_history` (если расписания нет). **Не используйте пример расписания как фактические часы торгов.**

## 5. Повторная сверка с терминалом (без изменения кэша)

| Функция | Параметры / результат |
| --- | --- |
| `compare_cached_ticks(cache, source, symbol, start, end, provider, *, window=timedelta(hours=1), max_rows_per_window=250_000, max_reported_issues=20)` | `TickComparison`: число окон, точных совпадений и расхождений; `matches`, `format()` |
| `compare_tick_ohlc_to_m1(cache, source, symbol, start, end, provider, *, tolerance=0.0, window=timedelta(hours=1), max_reported_issues=20)` | `CandleConsistency`: сравнение OHLC из Bid тиков со свечами M1 MT5; `consistent`, `format()` |

Импорт: `mt5_datahub.verification` и `mt5_datahub.ohlc_check`. `provider` должен быть **заранее подключён**. Сверка тиков учитывает **все поля и кратность одинаковых меток**; `max_rows_per_window` ограничивает сравнение окна, но не пиковую память нативного запроса MT5. Для OHLC даты должны быть **кратны минуте**; последний бар обязан быть закрыт, `window` — положительное целое число минут, `tolerance >= 0`.

Отчёты содержат `differences`/`issues`, `issue_count`, `source_binding`/`account_binding` (`matched`, `legacy-unbound`, `unknown-live-account`). Отсутствие расхождений с тем же MT5 — **проверка воспроизводимости, не абсолютной полноты**.

## 6. `ParquetCache` — низкоуровневое хранение

```python
from mt5_datahub.storage.parquet import ParquetCache
from mt5_datahub.timeutils import to_us, from_us

cache = ParquetCache(".mt5-datahub")
a, b = to_us(start), to_us(end)  # целые микросекунды UTC
```

Аргументы низкоуровневых методов обычно `(source, symbol, kind, timeframe, start, end)`: `kind="ticks"` + `timeframe=0` или `kind="candles"` + константа MT5; `start`/`end` — **числа микросекунд**, в отличие от пользовательского `MarketData`.

| Метод | Назначение |
| --- | --- |
| `missing(source, symbol, kind, timeframe, start, end)` | Список незакрытых **запросами** интервалов `(left_us, right_us)` |
| `read(..., *, columns=None)` | Собирает выбранные строки и колонки в `ndarray` |
| `iter_batches(..., *, columns=None, batch_size=65536)` | Читает отсортированные NumPy-пакеты из Parquet |
| `save(source, symbol, kind, timeframe, rows, start, end)` | Сохраняет новый непустой сегмент (структурированный массив) |
| `replace(..., rows, start, end, *, confirm=False, allow_changes=False)` | Защищённая атомарная замена полных затронутых сегментов |
| `verify_segments(...)` | `dict[путь, причина]` повреждений схемы, порядка, диапазона или файла |
| `invalidate_corrupt(..., *, confirm=False)` | При подтверждении **исключает** повреждённые записи из индекса; не удаляет Parquet |
| `compact(..., *, max_rows=2_000_000)` | Объединяет смежные неперекрывающиеся сегменты; возвращает количество объединённых файлов |
| `orphan_files()` | Список неиндексированных Parquet-файлов |
| `prune_orphans(*, confirm=False)` | **Безвозвратно** удаляет неиспользуемые файлы при `confirm=True` |
| `get_source_identity(source)` | Узнать сохранённую привязку к серверу/счёту |
| `bind_source(source, identity)` | Привязать **новый** источник к `server/login` (обычно делается `MarketData`) |

Хранилище: `<cache_dir>/index.sqlite3` и `<cache_dir>/segments/*.parquet`. Сжатие — Zstandard. Самостоятельная работа с `save`/`replace` требует корректной схемы `ndarray` и аккуратного управления индексом; обычно используйте `MarketData`.

**Один писатель на директорию**. Перед `refresh`, `invalidate_corrupt`, `compact` и особенно `prune_orphans` — резервная копия.

## 7. Исключения и логирование

Все специальные ошибки экспортируются из `mt5_datahub.exceptions`:

| Тип | Смысл |
| --- | --- |
| `MT5Error(RuntimeError)` | Базовая ошибка библиотеки |
| `MT5ConnectionError(MT5Error)` | Подключение терминала или брокера |
| `MT5RequestError(MT5Error)` | Запрос данных, символ, формат ответа |
| `MT5HistoryLimitError(MT5RequestError)` | Ответ свечей достиг `Max. bars in chart`; интервал может быть обрезан |
| `StorageError(MT5Error)` | Работа с кэшем, защитные проверки и операции записи |
| `ValueError` | Некорректные даты, `source`, символ, таймфрейм и параметры |

```python
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
```

Проект использует стандартный `logging`. Логи показывают запросы MT5, кэш-промахи, ошибки и восстановление подключения; библиотека не настраивает глобальный логгер за вас.

### Технические ограничения

Кэш фиксирует **успешно сохранённые запросы, но не доказанную полноту котировок**. Данные зависят от истории брокера и настройки MT5 `Max. bars in chart`. Для `get_ticks()` и `refresh_*` итоговые массивы могут быть велики. Одновременная запись несколькими процессами не поддерживается. Сессии и праздники могут объяснять длинные паузы без потери данных.

[Руководство](USAGE.md) · [README](../README.md)
