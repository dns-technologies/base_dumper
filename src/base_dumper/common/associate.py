from collections import OrderedDict
from collections.abc import (
    Iterable,
    Generator,
)
from platform import python_version
from typing import Any

from .diagram import DBMetadata


def repair_dtype_data(
    head_data: list[Any],
    iterator: Iterable[Any],
) -> Generator[Any, None, None]:
    """Yield saved rows first, then continue from iterator."""

    for row in head_data:
        yield row

    yield from iterator


def db_meta_from_iter(
    dtype_data: Iterable[Any],
    max_rows: int = 100,
) -> tuple[DBMetadata, Generator[Any, None, None]]:
    """Generate DBMetadata from iterable and return safe generator."""

    columns = OrderedDict()
    head_data = []
    iterator = iter(dtype_data)

    for _ in range(max_rows):
        try:
            row = next(iterator)
        except StopIteration:
            break

        head_data.append(row)
        non_associated = False

        for num, value in enumerate(row):
            type_name = value.__class__.__name__
            column = f"column_{num}"
            columns[column] = columns.get(column, type_name)

            if columns[column] == "NoneType":
                if columns[column] != type_name:
                    columns[column] = type_name
                else:
                    non_associated = True

        if not non_associated:
            break

    if not head_data:
        columns = OrderedDict([("column_0", "NoneType")])

    return (
        DBMetadata("Iterable Object", f"Python {python_version()}", columns),
        repair_dtype_data(head_data, iterator),
    )
