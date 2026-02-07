"""Create TPC-H tables using DuckDB's TPC-H extension and export them to Parquet or CSV format."""

from multiprocessing import Pool
from pathlib import Path
from performance.src.utilities.duckdb import DuckDBConnection


def create_tpc_h_tables(input_args):
    """Creates TPC-H tables using DuckDB's TPC-H extension and exports them to the specified format (Parquet or CSV).
    The function is designed to be run in parallel across multiple threads,
    with each thread generating a portion of the data based on the provided scale factor and run index.
    """
    run, kwargs = input_args

    with DuckDBConnection() as duckdb:
        duckdb.execute_multiple_queries(
            [
                "INSTALL tpch;",
                "LOAD tpch;",
                f"""CALL dbgen(sf ={kwargs.get("scale_factor")} , children = {kwargs.get("scale_factor")}, step = {run}, suffix = '_{run}' );""",
            ]
        )
        tables = duckdb.conn.execute("show tables;").fetchall()

        for table in tables:
            if kwargs.get("export_type") == "parquet":
                duckdb.conn.execute(
                    f"""COPY {table}_{run} TO '{kwargs.get("export_type")}/{table}/part_{run}.parquet' (FORMAT PARQUET);"""
                )
            elif kwargs.get("export_type") == "csv":
                duckdb.conn.execute(
                    f"""COPY {table}_{run} TO '{kwargs.get("export_type")}/{table}/part_{run}.csv' (FORMAT CSV, HEADER);"""
                )


def main(**kwargs) -> None:
    """Main function to create TPC-H tables in parallel using multiprocessing."""
    print(f"""Creating TPC-H tables with scale factor {kwargs.get("scale_factor")}...""")

    Path(kwargs.get("sql_path")).mkdir(parents=True, exist_ok=True)

    with Pool(processes=kwargs.get("num_of_threads")) as pool:
        pool.map(create_tpc_h_tables, range(0, kwargs.get("scale_factor")))

    with DuckDBConnection() as duckdb:
        tables = duckdb.conn.execute("show tables;").fetchall()
        for table in tables:
            Path(f"""{kwargs.get("export_type")}/{table}""").mkdir(parents=True, exist_ok=True)

        duckdb.execute_multiple_queries(["INSTALL tpch;", "LOAD tpch;"])

        queries = duckdb.conn.execute("FROM tpch_queries()").fetchall()

    for query in queries:
        with open(Path(kwargs.get("sql_path"), f"{query[0]}.sql"), "w") as f:
            f.write("*" * 20)
            f.write(f"\nQuery :: {query[0]}\n")
            f.write("*" * 20)
            f.write(f"\n{query[1]}")

    print(f"""Finished creating TPC-H tables with scale factor {kwargs.get("scale_factor")}.
              Use queries in {kwargs.get("sql_path")} to test the performance of your database.
              For more details checkout : https://duckdb.org/docs/stable/core_extensions/tpch
            """)
