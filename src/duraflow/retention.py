"""Retention policy is configured by the deployment, not by each archive caller."""
from __future__ import annotations

import math
from dataclasses import dataclass

from .contracts import Conflict


@dataclass(frozen=True)
class RetentionPolicy:
    minimum_retention: float = 0
    redelivery_horizon: float = 0

    def __post_init__(self) -> None:
        for value in (self.minimum_retention, self.redelivery_horizon):
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
                raise ValueError("Retention bounds must be finite nonnegative numbers")
        if self.minimum_retention < self.redelivery_horizon:
            raise ValueError("Retention must cover the deployment redelivery horizon")

    def validate(self, retention: float, safety_horizon: float) -> None:
        if retention < self.minimum_retention or safety_horizon < self.redelivery_horizon:
            raise Conflict("Archive request is below deployment retention policy")
        if retention < safety_horizon:
            raise Conflict("Retention does not cover the requested safety horizon")
