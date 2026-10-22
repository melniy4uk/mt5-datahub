# Release 0.2.0 — проверка и выпуск

**Релизный кандидат:** `0.2.0`; Windows x64, Python 3.13/3.14.
**Публикация:** не настроена, секретов PyPI и автоматического выкладывания нет.
Изменения и известные ограничения описаны в [CHANGELOG](../CHANGELOG.md).

## Проверки перед тегом

- [x] Публичные точки входа и аргументы зафиксированы в `tests/test_public_contract.py`.
- [x] Регрессионные тесты: закрытые свечи, пограничные интервалы, восстановление MT5, повреждения Parquet, защищённая замена, одинаковые миллисекунды, типы native rates.
- [x] Реальные проверки XAUUSD.s (22.10.2026):
  - 5 382/5 382 тика совпали через `copy_ticks_from`/ `copy_ticks_range`, 11 страниц по максимум 512.
  - Кэш, повторная выдача, дозагрузка и потоковая выдача — PASS.
  - 1 265 132 тика совпали при повторном чтении 48 окон.
  - 2 756/2 756 BID OHLC M1 совпали при допуске 0.01.
- [x] Документированы ограничения: `legacy-unbound`, брокерская история, общий RSS, один writer, отсутствие торговых операций.
- [ ] Windows Python 3.13/3.14: `uv lock --check`, `uv sync --locked`, Ruff, BasedPyright, coverage, `uv build`, чистая установка wheel. **Подтвердить успешным CI релизного PR.**
- [ ] Проверить обновлённый (порционный) путь стресс-теста на отдельном подключённом MT5 после версии `0.2.0`: `uv run python examples/stress_xauusd.py`. Предыдущий live stress PASS выполнялся до перехода на count-pages.
- [ ] Принять решение о лицензии перед распространением пакета сторонним пользователям; в репозитории пока нет `LICENSE`.
- [ ] После выполнения пунктов и слияния релизного PR создать Git tag и GitHub Release.

## Локальные команды

```powershell
git pull --ff-only origin main
uv sync --locked
uv run python -m unittest discover -s tests -v
uv build
uv run python examples/check_integration.py
uv run python examples/check_stream_live.py
uv run python examples/check_tick_pages_live.py
uv run python examples/check_tick_ohlc_live.py --tolerance 0.01
uv run python examples/stress_xauusd.py
```

Никакие из этих примеров не отправляют торговые ордера. Live-команды требуют доступный терминал MT5.

## Публикация после проверки

Тег и GitHub Release создаются из **`main`** только после успешного CI и живого стресс-теста. Команды GitHub CLI:

```powershell
git pull --ff-only origin main
gh release create v0.2.0 --target main --title "mt5-datahub 0.2.0" --notes-file CHANGELOG.md
```

Перед публикацией проверьте, что версия действительно `0.2.0`, нет существующего тега и репозиторий содержит условия лицензирования, которые вы намерены предоставить. Публикация в PyPI — отдельное действие; не выполняется автоматически.
