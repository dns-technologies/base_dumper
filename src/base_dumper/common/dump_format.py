from enum import Enum


class DumpFormat(Enum):
    """Enum for dump type format."""

    RAW = 0
    CSV = 1
    S3 = 2
