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


DIALECT = {
    "athena": "athena",
    "bigquery": "bigquery",
    "clickhouse": "clickhouse",
    "databricks": "databricks",
    "denodo": "denodo",
    "doris": "doris",
    "drill": "drill",
    "duckdb": "duckdb",
    "greenplum": "postgres",
    "hive": "hive",
    "materialize": "materialize",
    "mysql": "mysql",
    "oracle": "oracle",
    "postgres": "postgres",
    "presto": "presto",
    "redshift": "redshift",
    "risingwave": "risingwave",
    "snowflake": "snowflake",
    "spark": "spark",
    "spark2": "spark2",
    "sqlite": "sqlite",
    "sqlserver": "tsql",
    "starrocks": "starrocks",
    "tableau": "tableau",
    "teradata": "teradata",
    "trino": "trino",
}
EXECUTE_PATTERN = compile(r"^(with|select|show|grant|describe)\s", IGNORECASE)
FIRST_WORD_PATTERN = compile(r"^\s*([a-zA-Z]+)")
STRIP_CHARS = "; \t\n\r"
UNKNOWN = "Unknown"


def get_query_kind(query: str, dialect: str = "postgres") -> str:
    """Get kind of query."""

    if not query:
        return UNKNOWN

    try:
        ast = parse_one(query, read=dialect, error_level=ErrorLevel.IGNORE)

        if not ast:
            match = FIRST_WORD_PATTERN.match(query)
            return match.group(1).capitalize() if match else UNKNOWN

        if ast.key == "block":
            return ast.expressions[0].key.capitalize()

        return ast.key.capitalize()
    except ParseError:
        return UNKNOWN


def query_formatter(queries: str, dialect: str = "postgres") -> str:
    """Reformat query, removing comments."""

    statements = transpile(queries, read=dialect, comments=False)
    return ";\n".join(statements).strip(STRIP_CHARS)


def chunk_query(
    queries: str | None,
    dialect: str = "postgres",
) -> tuple[list[str], list[str]]:
    """Chunk multiquery to queries."""

    if not queries:
        return [], []

    all_queries = [
        part.strip(STRIP_CHARS)
        for part in transpile(queries, read=dialect, comments=False)
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
