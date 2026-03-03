from abc import (
    ABC,
    abstractmethod,
)
from io import BufferedReader
from typing import (
    Any,
    Generator,
)

from pandas import DataFrame as PdFrame
from polars import DataFrame as PlFrame


class ExampleReader(ABC):
    """Example stream reader object.
    This class must have this five methods besides his own."""

    fileobj: BufferedReader

    @abstractmethod
    def to_rows(self) -> Generator[Any, None, None]:
        """Convert to python rows."""

    @abstractmethod
    def to_pandas(self) -> PdFrame:
        """Convert to pandas.DataFrame."""

    @abstractmethod
    def to_polars(self) -> PlFrame:
        """Convert to polars.DataFrame."""

    @abstractmethod
    def tell(self) -> int:
        """Return current position."""

    @abstractmethod
    def close(self) -> None:
        """Close reader object."""
