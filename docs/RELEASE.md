# Release checklist

Library version: `0.2.0.dev0` is **experimental**.

Before a tagged version:

1. Ensure `main` and the release PR pass Windows 3.13 and 3.14 CI:
   `compileall`, Ruff, BasedPyright, offline unit tests/coverage, `uv build`.
2. On a dedicated live MT5 terminal, run:
   `uv run python examples/check_integration.py`,
   `uv run python examples/check_stream_live.py`,
   `uv run python examples/stress_xauusd.py`.
   Confirm the broker/server, UTC timestamps, and that the tests **do not trade**.
3. Test a fresh cache and a copied legacy cache, including repaired files
   and account/server identity warnings.
4. Review potential partial MT5 history, Max. bars in chart, holidays,
   duplicate millisecond timestamps and tick ordering.
5. Check backward compatibility of the public `MarketData` API and
   update README/examples for any changed signatures or limits.
6. Verify version, wheel/sdist metadata and installation in a clean
   Windows environment using `uv build` and `uv pip install`.
7. Only then tag a release; leave `0.2.0.dev0` for unreleased changes.

No automatic publish or package registry credentials are configured.
