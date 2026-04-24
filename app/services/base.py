"""
Provider adapter interface.
Every data source must implement this abstract base class.
To swap a data source, create a new class that extends BaseProvider
and plug it into the daily task runner — no other code changes needed.
"""
from abc import ABC, abstractmethod
from datetime import date
from typing import Any


class BaseProvider(ABC):
    """Abstract interface for all Axiom data providers."""

    @abstractmethod
    async def fetch(self, target_date: date) -> Any:
        """
        Fetch data for the given date.
        Returns provider-specific raw data that the task runner will map
        to PitcherFeatureSet fields.
        """
        ...

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable name shown in logs."""
        ...
