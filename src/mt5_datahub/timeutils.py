"""UTC boundaries shared by the provider, cache and service."""
from datetime import UTC, datetime, timedelta

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def validate_range(start: datetime, end: datetime) -> tuple[datetime, datetime]:
    if start.utcoffset() is None or end.utcoffset() is None:
        raise ValueError("Dates must have a timezone")
    start, end = start.astimezone(UTC), end.astimezone(UTC)
    if start >= end:
        raise ValueError("Start must precede end")
    return start, end


def to_us(value: datetime) -> int:
    delta = value.astimezone(UTC) - EPOCH
    return (delta.days * 86400 + delta.seconds) * 1_000_000 + delta.microseconds


def from_us(value: int) -> datetime:
    return EPOCH + timedelta(microseconds=value)
