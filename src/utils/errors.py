class SlowbotError(Exception):
    """Base application error."""


class ConfigError(SlowbotError):
    """Configuration related errors."""


class DataCollectionError(SlowbotError):
    """Collector related errors."""


class AIValidationError(SlowbotError):
    """AI output contract validation errors."""
