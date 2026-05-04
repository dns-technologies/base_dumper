from re import (
    compile,
    IGNORECASE,
)
from sqlglot import (
    transpile,
    parse_one,
)
from sqlglot.errors import (
    ErrorLevel,
    ParseError,
)


EXECUTE_PATTERN = compile(r"^(with|select|show|grant|describe)\s", IGNORECASE)
FIRST_WORD_PATTERN = compile(r"^\s*([a-zA-Z]+)")
STRIP_CHARS = "; \t\n\r"
UNKNOWN = "Unknown"


def get_query_kind(query: str) -> str:
    """Get kind of query."""

    if not query:
        return UNKNOWN

    try:
        ast = parse_one(query, error_level=ErrorLevel.IGNORE)

        if not ast:
            match = FIRST_WORD_PATTERN.match(query)
            return match.group(1).capitalize() if match else UNKNOWN

        if ast.key == "block":
            return ast.expressions[0].key.capitalize()

        return ast.key.capitalize()
    except ParseError:
        return UNKNOWN


def query_formatter(queries: str) -> str:
    """Reformat query, removing comments."""

    statements = transpile(queries, comments=False)
    return ";\n".join(statements).strip(STRIP_CHARS)


def chunk_query(queries: str | None) -> tuple[list[str], list[str]]:
    """Chunk multiquery to queries."""

    if not queries:
        return [], []

    all_queries = [
        part.strip(STRIP_CHARS)
        for part in transpile(queries, comments=False)
        if part.strip(STRIP_CHARS)
    ]

    if not all_queries:
        return [], []

    first_part = all_queries.copy()
    second_part = []

    for _ in range(len(first_part)):
        query = first_part.pop()
        second_part.append(query)
        if EXECUTE_PATTERN.match(query):
            break

    second_part.reverse()
    return first_part, second_part
