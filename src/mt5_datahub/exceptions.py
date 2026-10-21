class MT5Error(RuntimeError):
    """Базовая ошибка работы с MetaTrader 5."""


class MT5ConnectionError(MT5Error):
    """Ошибка соединения с терминалом или брокером."""


class MT5RequestError(MT5Error):
    """Ошибка выполнения запроса рыночных данных."""