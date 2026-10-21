class MT5Error(RuntimeError):
    """Base MetaTrader 5 integration error."""


class MT5ConnectionError(MT5Error):
    """Terminal or broker connection failed."""


class MT5RequestError(MT5Error):
    """Market data request failed."""


class MT5HistoryLimitError(MT5RequestError):
    """MT5 bar response touched Max. bars in chart and may be truncated."""


class StorageError(MT5Error):
    """Persistent market data storage failed."""
