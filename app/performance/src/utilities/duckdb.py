"""A utility class for managing DuckDB connections and executing queries."""

import duckdb


class DuckDBConnection:
    """A context manager for managing a connection to a DuckDB database and executing SQL queries.
    Example usage:
    ```python
    with DuckDBConnection(database='my_database.db') as duckdb_conn:
        duckdb_conn.execute_multiple_queries([
            "CREATE TABLE IF NOT EXISTS my_table (id INTEGER, name STRING);",
            "INSERT INTO my_table VALUES (1, 'Alice'), (2, 'Bob');"
        ])
    ```
    In this example, a connection to the DuckDB database `my_database.db` is established,
    two SQL queries are executed to create a table and insert data,
    and the connection is automatically closed when exiting the context.

    Note: If no database file is specified, it will use an in-memory database by default.
    """

    def __init__(self, database=":memory:"):
        """Initializes the DuckDBConnection with the provided database name or path."""
        self.database = database
        self.conn: duckdb.DuckDBPyConnection

    def __enter__(self):
        """Establishes a connection to the DuckDB database using the provided database name or path."""
        self.conn = duckdb.connect(database=self.database)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Closes the connection to the DuckDB database when exiting the context."""
        self.close()

    def close(self):
        """Closes the connection to the DuckDB database."""
        self.conn.close()

    def execute_multiple_queries(self, queries) -> None:
        """Executes multiple SQL queries that doesnt return anything"""
        for query in queries:
            self.conn.execute(query)
