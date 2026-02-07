"""Create TPC-DS tables using DuckDB's TPC-DS extension and export them to Parquet or CSV format."""

from pathlib import Path
from performance.src.utilities.duckdb import DuckDBConnection


def main(**kwargs) -> None:
    """Main function to create TPC-DS tables using DuckDB's TPC-DS extension
    The function creates the tables, exports them, and saves the TPC-DS queries for performance testing.
    For more details checkout : https://duckdb.org/docs/stable/core_extensions/tpcds
    """
    print(f"""Creating TPC-DS tables with scale factor {kwargs.get("scale_factor")} . . .""")
    Path(kwargs.get("sql_path")).mkdir(parents=True, exist_ok=True)

    with DuckDBConnection() as duckdb:
        duckdb.execute_multiple_queries(
            ["INSTALL tpcds;", "LOAD tpcds;", f"""CALL dsdgen(sf = {kwargs.get("scale_factor")});"""]
        )

        tables = duckdb.conn.execute("show tables;").fetchall()
        queries = duckdb.conn.execute("FROM tpcds_queries()").fetchall()

        for (table,) in tables:
            Path(f"""{kwargs.get("target_path")}/{table}""").mkdir(parents=True, exist_ok=True)

            if kwargs.get("export_type") == "parquet":
                duckdb.conn.execute(
                    f"""COPY {table} TO '{kwargs.get("target_path")}/{table}/part_0.parquet' (FORMAT PARQUET);"""
                )
            elif kwargs.get("export_type") == "csv":
                duckdb.conn.execute(
                    f"""COPY {table} TO '{kwargs.get("target_path")}/{table}/part_0.csv' (FORMAT CSV, HEADER);"""
                )

    for query in queries:
        with open(Path(kwargs.get("sql_path"), f"{query[0]}.sql"), "w") as f:
            f.write("*" * 20)
            f.write(f"\nQuery :: {query[0]}\n")
            f.write("*" * 20)
            f.write(f"\n{query[1]}")

    print(f"""Finished creating TPC-H tables with scale factor {kwargs.get("scale_factor")}.
            Use queries in {kwargs.get("sql_path")} to test the performance of your database.
            For more details checkout : https://duckdb.org/docs/stable/core_extensions/tpcds
    """)
