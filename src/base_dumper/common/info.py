INFO_TEMPLATE = """
┌───────────────────────────────────────┐
│           Query information           │
╞══════════════╤════════════════════════╡
│ Kind         │ {0:<22} │
├──────────────┼────────────────────────┤
│ Duration     │ {1:<22} │
├──────────────┼────────────────────────┤
│ Memory Usage │ {2:<22} │
├──────────────┼────────────────────────┤
│ Rows         │ {3:<22} │
├──────────────┼────────────────────────┤
│ Bytes        │ {4:<22} │
└──────────────┴────────────────────────┘
"""


def debug_info(
    kind: str,
    duration: int,
    memory_usage: int,
    rows: int,
    width: int,
) -> str:
    """Debug information to log."""

    duration_sec = f"{duration} seconds"
    memory = f"{memory_usage} bytes"

    return INFO_TEMPLATE.format(kind, duration_sec, memory, rows, width)
