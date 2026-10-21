# mt5-datahub

Небольшая Python-библиотека для **исторических тиков и свечей MetaTrader 5**.
Данные возвращаются структурированными `numpy.ndarray`; локальный кэш —
**Parquet + SQLite**. Примеры используют терминал MT5 с сервером
`Bybit-Live-6` и символом `XAUUSD.s` (не прямой REST API Bybit).

**Статус:** экспериментальная библиотека, не система исполнения сделок.

## Установка

Windows, Python 3.13+, [uv](https://docs.astral.sh/uv/), установленный MT5
с подключённым брокерским счётом:

```powershell
git clone https://github.com/melniy4uk/mt5-datahub.git
cd mt5-datahub
uv sync
```

## Основной API

```python
from datetime import UTC, datetime, timedelta
import MetaTrader5 as mt5
from mt5_datahub import MarketData

start = datetime(2026, 10, 20, 10, tzinfo=UTC)
end = start + timedelta(minutes=10)

with MarketData(
    source="Bybit-Live-6/account-1",
    cache_dir=".mt5-datahub",
) as data:
    ticks = data.get_ticks("XAUUSD.s", start, end)
    candles = data.get_candles("XAUUSD.s", mt5.TIMEFRAME_M1, start, end)

print(len(ticks), len(candles))
```

- **`get_ticks(symbol,start,end)`** — все доступные тики в памяти.
- **`get_candles(symbol,timeframe,start,end)`** — только уже закрытые свечи.
- **`iter_ticks(...,columns=...,batch_size=65536)`** — сначала дозагружает
  отсутствующий диапазон, затем выдаёт NumPy-пакеты.
- **`stream_ticks(...,columns=...,batch_size=65536)`** — загружает и сохраняет
  очередной временной участок *по мере потребления*, без ожидания всего диапазона.
- **`refresh_ticks(...,confirm=True)`** и
  **`refresh_candles(...,confirm=True)`** — повторно запрашивают MT5 и атомарно
  заменяют индексированные Parquet-сегменты **после** успешной загрузки.
  По умолчанию обновление не разрешает пропажу/изменение старых записей.
  Для сознательного принятия исправленной брокером истории используйте
  `allow_changes=True`. `max_rows` ограничивает размер операции обновления.

`start` и `end` — timezone-aware `datetime`; используются UTC
и полуоткрытый диапазон **[start,end)**. Для свечей границы относятся
ко времени **открытия**. Участок, содержащий незакрытую свечу, не отмечается
как закэшированный: после закрытия он может быть запрошен заново.

Для отдельных запросов без кэша используйте `MT5Provider`:

```python
from mt5_datahub.providers.mt5 import MT5Provider

with MT5Provider() as provider:
    ticks = provider.get_ticks("XAUUSD.s", start, end)
```

Внешний `provider=` в `MarketData` остаётся собственностью вызывающего кода;
отключайте его самостоятельно. При неудаче MT5-соединения выполняется одна
попытка восстановления; запрос повторяется **только при подтверждённом разрыве**.

## Кэш и его проверка

Кэш разделяется по `source`, символу, виду данных и таймфрейму.
У **новых** источников запоминается привязка `source` к серверу и номеру
счёта MT5; несоответствие вызывает ошибку. У старых источников без привязки
появляется предупреждение — рекомендуем дать им новое имя после проверки.

Пустые ответы MT5 не записываются как покрытие запроса. Пакетные операции
и прогрессивная загрузка сохраняют ранее завершённые сегменты после сбоя.

```python
from mt5_datahub.storage.parquet import ParquetCache
from mt5_datahub.timeutils import to_us

cache = ParquetCache(".mt5-datahub")
missing = cache.missing(
    "Bybit-Live-6/account-1", "XAUUSD.s", "ticks", 0,
    to_us(start), to_us(end),
)
broken = cache.verify_segments(
    "Bybit-Live-6/account-1", "XAUUSD.s", "ticks", 0,
    to_us(start), to_us(end),
)
print("Unrequested ranges:", missing)
print("Broken segments:", broken)
```

- `verify_segments()` проверяет читаемость, типы полей, количество строк,
  интервалы и порядок меток времени, но **не полноту истории брокера**.
- `invalidate_corrupt(...,confirm=True)` исключает повреждённые сегменты
  из SQLite, **не удаляя их файлы**.
- `compact(...,max_rows=...)` объединяет только смежные неперекрывающиеся файлы.
- `orphan_files()` перечисляет неиспользуемые файлы;
  `prune_orphans(confirm=True)` **удаляет их безвозвратно**.
  Перед очисткой делайте резервную копию и обеспечьте единственного писателя.
- Потоковый `iter_batches` возвращает отсортированные по времени пакеты,
  даже если старые сегменты перекрываются; одинаковые временные метки
  **не дедуплицируются**.

## Аудит тиков и проверка

```powershell
uv run python -m unittest discover -s tests -v
uv run python examples/check_integration.py
uv run python examples/check_stream_live.py
uv run python examples/audit_xauusd.py --sessions examples/bybit-live-6-xauusd-s.sessions.json
uv run python examples/benchmark_cache.py
```

`audit_ticks(cache,source,symbol,start,end,...)` из `mt5_datahub.quality`
возвращает `TickQualityReport`: сохранённые тики, непокрытые запросами
диапазоны, повторы временных меток, нарушения порядка и самые длинные паузы.
Аудит обычно работает пакетно; для нарушенного физического порядка есть
резервная полная сортировка. По предоставленному `SessionSchedule` большие
паузы классифицируются как перерывы торгов или необъяснённые разрывы.
Проверяйте UTC-время сессий брокера самостоятельно.

## Важные ограничения

1. **Кэш ≠ доказанная полнота истории.** Непустой ответ MT5 может быть
   частичным; `copy_rates_range` зависит от объёма доступной истории
   терминала и его лимита **Max. bars in chart**; при достижении этого
   лимита провайдер записывает предупреждение в лог. Отсутствие пропусков
   в индексе говорит только об успешных запросах, а не о каждом тике.
2. **Память:** `batch_size` ограничивает выдаваемый NumPy-пакет, но
   **не размер исходного ответа `copy_ticks_range`**. Устанавливайте небольшой
   `tick_chunk` (например, `timedelta(minutes=15)`) при интенсивном потоке.
   `max_tick_rows=250_000` автоматически уменьшает длительность следующих
   запросов, если ответ слишком велик, но **не предотвращает первый пик
   памяти**, а интервал короче секунды дальше не делится.
   `get_ticks` всегда материализует итог целиком.
3. **Один писатель на `cache_dir`.** Параллельные процессы записи/компактизации
   не поддерживаются; не запускайте очистку во время загрузки.
4. Старые Parquet-файлы после `refresh_*` остаются как резервные orphan-файлы.
   Проверьте историю и сделайте backup перед `prune_orphans`.
5. Сессии, праздники, перерывы брокера и исторические изменения котировок
   могут выглядеть как разрывы. Нельзя автоматически объявлять их потерей данных.

## Техническая проверка

Windows CI запускает offline unit-тесты без терминала MT5. Live-проверки
в `examples/` запускаются отдельно только при подключённом терминале;
они **не открывают сделки**.
