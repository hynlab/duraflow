"""Retention policy is configured by the deployment, not by each archive caller."""

from __future__ import annotations

import math
from dataclasses import dataclass

from .contracts import Conflict

DAY = 86400
MESSAGE_HORIZON = 7 * DAY
DETAIL_RETENTION = 14 * DAY


def signal_expiry(epoch: int, now: float, horizon: float = MESSAGE_HORIZON) -> float:
    """Daily epochs admit at least one full retry horizon, then fail closed."""
    if type(epoch) is not int or epoch < 0 or epoch > int(now // DAY):
        raise Conflict("Invalid signal epoch")
    expires_at = (epoch + 1) * DAY + horizon
    if now >= expires_at:
        raise Conflict("Expired signal epoch")
    return expires_at


@dataclass(frozen=True)
class RetentionPolicy:
    minimum_retention: float = 0
    redelivery_horizon: float = 0
    message_horizon: float = MESSAGE_HORIZON
    detail_retention: float = DETAIL_RETENTION

    def __post_init__(self) -> None:
        for value in (self.minimum_retention, self.redelivery_horizon, self.message_horizon, self.detail_retention):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                raise ValueError("Retention bounds must be finite nonnegative numbers")
        if self.minimum_retention < self.redelivery_horizon:
            raise ValueError("Retention must cover the deployment redelivery horizon")
        if self.message_horizon <= 0 or self.detail_retention < self.message_horizon:
            raise ValueError("Detail retention must cover the positive message horizon")

    def validate(self, retention: float, safety_horizon: float) -> None:
        if retention < self.minimum_retention or safety_horizon < self.redelivery_horizon:
            raise Conflict("Archive request is below deployment retention policy")
        if retention < safety_horizon:
            raise ValueError("Retention does not cover the requested safety horizon")
