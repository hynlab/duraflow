from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal


@dataclass(frozen=True)
class Line:
    name: str
    amount: Decimal


@dataclass(frozen=True)
class Receipt:
    issued: datetime
    lines: list[Line]
    note: str | None


def sample() -> Receipt:
    return Receipt(datetime(2026, 1, 2, 3, 4, 5, tzinfo=timezone.utc),
                   [Line("sample", Decimal("12.340"))], None)
