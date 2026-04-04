from re import (
    compile,
    IGNORECASE,
)

from sqlparse import (
    format as sql_format,
    parse,
)


EXECUTE_PATTERN = compile(r"^(with|select|show|grant|describe)\s", IGNORECASE)
FIRST_WORD_PATTERN = compile(r"^\s*([a-zA-Z]+)")
QUERY_PATTERN = compile(r";(?=(?:[^']*'[^']*')*[^']*$)")
STRIP_CHARS = "; \t\n\r"
UNKNOWN = "Unknown"


def get_query_kind(query: str) -> str:
    """Get kind of query."""

    try:
        query_kind = parse(query)[0].get_type().capitalize()

        if query_kind == UNKNOWN:
            _match = FIRST_WORD_PATTERN.match(query)

            if _match:
                return _match.group(1).capitalize()

        return query_kind
    except IndexError:
        return UNKNOWN


def query_formatter(queries: str) -> str:
    """Reformat query."""

    return sql_format(sql=queries, strip_comments=True).strip(STRIP_CHARS)


def chunk_query(queries: str | None) -> tuple[list[str], list[str]]:
    """Chunk multiquery to queryes."""

    if not queries:
        return [], []

    first_part: list[str] = [
        part.strip(STRIP_CHARS)
        for part in QUERY_PATTERN.split(query_formatter(queries))
        if part.strip(STRIP_CHARS)
    ]

    if not first_part:
        return [], []

    second_part: list[str] = []

    for _ in range(len(first_part)):
        query = first_part.pop()
        second_part.append(query)

        if EXECUTE_PATTERN.match(query):
            break

    second_part.reverse()
    return first_part, second_part
