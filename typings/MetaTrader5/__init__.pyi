from datetime import datetime
from typing import NamedTuple

from numpy import ndarray

COPY_TICKS_ALL: int

TIMEFRAME_M1: int
TIMEFRAME_M2: int
TIMEFRAME_M3: int
TIMEFRAME_M4: int
TIMEFRAME_M5: int
TIMEFRAME_M6: int
TIMEFRAME_M10: int
TIMEFRAME_M12: int
TIMEFRAME_M15: int
TIMEFRAME_M20: int
TIMEFRAME_M30: int
TIMEFRAME_H1: int
TIMEFRAME_H2: int
TIMEFRAME_H3: int
TIMEFRAME_H4: int
TIMEFRAME_H6: int
TIMEFRAME_H8: int
TIMEFRAME_H12: int
TIMEFRAME_D1: int
TIMEFRAME_W1: int
TIMEFRAME_MN1: int

class TerminalInfo(NamedTuple):
    connected: bool

class SymbolInfo(NamedTuple):
    visible: bool

class AccountInfo(NamedTuple):
    server: str
    login: int

def initialize() -> bool: ...
def shutdown() -> None: ...
def last_error() -> tuple[int, str]: ...
def terminal_info() -> TerminalInfo | None: ...
def symbol_info(symbol: str) -> SymbolInfo | None: ...
def account_info() -> AccountInfo | None: ...
def symbol_select(symbol: str, enable: bool) -> bool: ...
def copy_ticks_range(
    symbol: str,
    date_from: datetime,
    date_to: datetime,
    flags: int,
) -> ndarray | None: ...
def copy_rates_range(
    symbol: str,
    timeframe: int,
    date_from: datetime,
    date_to: datetime,
) -> ndarray | None: ...
