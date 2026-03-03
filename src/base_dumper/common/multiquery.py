from re import split

from sqlparse import format as sql_format


PATTERN = r";(?=(?:[^']*'[^']*')*[^']*$)"


def query_formatter(query: str) -> str:
    """Reformat query."""

    return sql_format(sql=query, strip_comments=True).strip().strip(";")


def chunk_query(query: str | None) -> tuple[list[str]]:
    """Chunk multiquery to queryes."""

    if not query:
        return [], []

    parts = [
        part.strip().strip(";").strip()
        for part in split(PATTERN, query_formatter(query))
        if part.strip().strip(";").strip()
    ]

    if not parts:
        return [], []

    first_part: list[str] = []
    second_part: list[str] = []

    for i, part in enumerate(parts):
        first_part.append(part)

        if (
            i + 1 < len(parts) and parts[i + 1].lower().startswith(
            ("with", "select", "show"))
        ):
            second_part = parts[i + 1:]
            break

    if len(first_part) == 1 and not second_part:
        return second_part, first_part

    return first_part, second_part
