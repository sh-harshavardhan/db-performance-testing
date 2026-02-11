"""A utility class for managing DuckDB connections and executing queries."""

import duckdb
from typing import Union
from pathlib import Path


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

    def __init__(self, database: Union[str, Path] = ":memory:"):
        """Initializes the DuckDBConnection with the provided database name or path."""
        self.database = str(database)
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

    def export_table(self, table, **kwargs) -> None:
        """Exports a table from the DuckDB database to a specified format (Parquet or CSV) and target path."""
        export_type = kwargs.get("export_type", "csv")
        target_path = kwargs.get("target_path", "exported_data")
        file_prefix = kwargs.get("file_prefix", table)
        partition_column = kwargs.get("partition_column")

        # file_size = kwargs.get("file_size", 128)
        # table_size = self.conn.execute(
        #     f"""SELECT
        #             table_name,
        #             estimated_size AS rows,
        #             sum(estimated_size * 8) / 1024 / 1024 AS estimated_bytes -- Rough approximation in MB
        #         FROM duckdb_tables()
        #         WHERE table_name = '{table}'
        #         GROUP BY ALL;
        #     """).fetchone()[2]

        # table_size = self.conn.execute(
        #     f"""SELECT
        #         (
        #             COUNT(DISTINCT block_id) *
        #             (SELECT CAST(block_size AS BIGINT) FROM pragma_database_size())
        #         ) / 1024 / 1024
        #         AS size_in_mb
        #     FROM
        #         pragma_storage_info('{table}')
        #     WHERE
        #         persistent = TRUE and block_id IS NOT NULL
        #     GROUP BY
        #         ALL;
        #     """).fetchone()[0]
        #
        # num_of_partitions = min(max(1, int(table_size / file_size)), 1000)
        # print(f"Exporting: {table} of size "
        #       f"{table_size} to {num_of_partitions} partitions of size ~{file_size} MB each")
        # if num_of_partitions > 1:
        #     for part in range(num_of_partitions + 1):
        #         if export_type == "parquet":
        #             self.conn.execute(
        #                 f"""COPY (select * from {table} where rowid % {num_of_partitions + 1} = {part})
        #                             TO '{target_path}/{table}/{file_prefix}_{part}.parquet'
        #                             (FORMAT PARQUET)"""
        #             )
        #         elif export_type == "csv":
        #             self.conn.execute(
        #                 f"""COPY  (select * from {table} where rowid % {num_of_partitions + 1} = {part})
        #                             TO '{target_path}/{table}/{file_prefix}_{part}.csv'
        #                             (FORMAT CSV, HEADER)"""
        #             )

        if partition_column:
            if export_type == "parquet":
                self.conn.execute(
                    f"""COPY {table} TO '{target_path}/{table}/{file_prefix}'
                        (FORMAT PARQUET, PARTITION_BY ({partition_column}))"""
                )
            elif export_type == "csv":
                self.conn.execute(
                    f"""COPY {table}  TO '{target_path}/{table}/{file_prefix}'
                        (FORMAT CSV, HEADER, PARTITION_BY ({partition_column}))"""
                )
        else:
            if export_type == "parquet":
                self.conn.execute(f"""COPY {table} TO '{target_path}/{table}/{file_prefix}.parquet' (FORMAT PARQUET)""")
            elif export_type == "csv":
                self.conn.execute(f"""COPY {table} TO '{target_path}/{table}/{file_prefix}.csv' (FORMAT CSV, HEADER)""")
