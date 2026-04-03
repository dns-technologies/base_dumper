from collections import OrderedDict
from logging import Logger
from typing import NamedTuple

from .mode_level import DumperMode


class DBMetadata(NamedTuple):
    """Database object."""

    name: str
    version: str
    columns: OrderedDict

    def to_bytes(self) -> None: ...
    def to_rows(self) -> None: ...
    def to_pandas(self) -> None: ...
    def to_polars(self) -> None: ...
    def close(self) -> None: ...
    def tell(self) -> int: return 0


def __format_table(
    metadata: DBMetadata,
    direction: str,
) -> list[str]:
    """Format single table as list of lines."""

    if metadata.columns:
        max_name_len = max(
            len(col_name) for col_name in metadata.columns.keys()
        )
        max_type_len = max(
            len(str(col_type)) for col_type in metadata.columns.values()
        )
    else:
        max_name_len = 0
        max_type_len = 0

    name_width = max(max_name_len, len("Column Name"))
    type_width = max(max_type_len, len("Data Type"))
    table_width = name_width + type_width + 7
    title = f"{direction} [{metadata.name} {metadata.version}]"
    title_width = len(title) + 4

    if title_width > table_width:
        extra = title_width - table_width
        name_width += extra // 2
        type_width += extra - extra // 2
        table_width = title_width

    name_header = f"{'Column Name':^{name_width}}"
    type_header = f"{'Data Type':^{type_width}}"

    lines = [
        f"┌{'─' * (table_width - 2)}┐",
        f"│ {title:^{table_width - 4}} │",
        f"╞{'═' * (name_width + 2)}╤{'═' * (type_width + 2)}╡",
        f"│ {name_header} │ {type_header} │",
        f"╞{'═' * (name_width + 2)}╪{'═' * (type_width + 2)}╡",
    ]

    for col_name, col_type in metadata.columns.items():
        name_cell = f"{col_name:<{name_width}}"
        type_cell = f"{str(col_type):>{type_width}}"
        lines.extend([
            f"│ {name_cell} │ {type_cell} │",
            f"├{'─' * (name_width + 2)}┼{'─' * (type_width + 2)}┤",
        ])

    lines[-1] = f"└{'─' * (name_width + 2)}┴{'─' * (type_width + 2)}┘"
    return lines


def transfer_table(source: DBMetadata, destination: DBMetadata) -> str:
    """Make transfer diagram with two tables and arrow."""

    src_lines = __format_table(source, "Source")
    dest_lines = __format_table(destination, "Destination")
    src_width = max(len(line) for line in src_lines) if src_lines else 0
    dest_width = max(len(line) for line in dest_lines) if dest_lines else 0
    max_lines = max(len(src_lines), len(dest_lines), 9)
    src_lines.extend([" " * src_width] * (max_lines - len(src_lines)))
    dest_lines.extend([" " * dest_width] * (max_lines - len(dest_lines)))
    middle_line = max_lines // 2

    arrow_config = [
        (middle_line - 3, " │╲   "),
        (middle_line - 2, " │ ╲  "),
        (middle_line - 1, "┌┘  ╲ "),
        (middle_line, "│    ╲"),
        (middle_line + 1, "│    ╱"),
        (middle_line + 2, "└┐  ╱ "),
        (middle_line + 3, " │ ╱  "),
        (middle_line + 4, " │╱   "),
    ]

    arrow_map = {line: arrow for line, arrow in arrow_config}
    return "Transfer data diagram:\n" + "\n".join(
        f"{src_lines[row]} {arrow_map.get(row, '      ')} {dest_lines[row]}"
        for row in range(max_lines)
    )


def single_table(metadata: DBMetadata) -> str:
    """Make diagram for sibgle table."""

    diagram = __format_table(metadata, "Summary")
    return "Result table diagram:\n" + "\n".join(diagram)


def log_table(
    logger: Logger,
    mode: DumperMode,
    source: DBMetadata,
    destination: DBMetadata = None,
) -> None:
    """Generate diagram log message."""

    if destination:
        diagram_message = transfer_table(source, destination)
    else:
        diagram_message = single_table(source)

    logger.info(diagram_message)

    if mode is DumperMode.TEST:
        logger.warning("Write functions are not available in TEST mode.")
