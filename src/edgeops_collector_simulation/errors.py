class CollectorError(Exception):
    """Base error exposed through the collector API."""


class ResourceNotFoundError(CollectorError):
    """A requested simulated resource does not exist."""


class RunConflictError(CollectorError):
    """A valid run overlaps the current active run."""
