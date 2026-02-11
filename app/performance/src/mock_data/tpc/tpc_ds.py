"""Create TPC-DS tables using DuckDB's TPC-DS extension and export them to Parquet or CSV format."""

from pathlib import Path
from performance.src.utilities.duckdb import DuckDBConnection


table_name_partition_column = {
    # 'catalog_returns': 'MOD(cr_item_sk+ cr_order_number, {partition_count})',
    # 'catalog_sales': 'MOD(cs_item_sk+ cs_order_number, {partition_count})',
    # 'customer': 'MOD(c_customer_sk, {partition_count})',
    # 'customer_address': 'MOD(ca_address_sk, {partition_count})',
    # 'customer_demographics': 'MOD(cd_demo_sk, {partition_count})',
    # 'date_dim': 'MOD(d_date_sk, {partition_count})',
    # 'inventory': 'MOD(inv_date_sk+ inv_item_sk+ inv_warehouse_sk, {partition_count})',
    # 'store_returns': 'MOD(sr_item_sk+ sr_ticket_number, {partition_count})',
    # 'store_sales': 'MOD(ss_item_sk+ ss_ticket_number, {partition_count})',
    # 'time_dim': 'MOD(t_time_sk, {partition_count})',
    # 'web_returns': 'MOD(wr_item_sk+ wr_order_number, {partition_count})',
    # 'web_sales': 'MOD(ws_item_sk+ ws_order_number, {partition_count})',
}


def main(**kwargs) -> None:
    """Main function to create TPC-DS tables using DuckDB's TPC-DS extension
    The function creates the tables, exports them, and saves the TPC-DS queries for performance testing.
    For more details checkout : https://duckdb.org/docs/stable/core_extensions/tpcds
    """
    print(f"""Creating TPC-DS tables with scale factor {kwargs.get("scale_factor")} . . .""")
    Path(kwargs.get("sql_path")).mkdir(parents=True, exist_ok=True)

    db_file_path = Path(".tmp/tpc_ds/tpc_ds.db")
    Path(db_file_path.parent).mkdir(parents=True, exist_ok=True)

    with DuckDBConnection(database=db_file_path) as duckdb:
        duckdb.execute_multiple_queries(
            ["INSTALL tpcds;", "LOAD tpcds;", f"""CALL dsdgen(sf = {kwargs.get("scale_factor")}, keys = true);"""]
        )

        tables = duckdb.conn.execute("show tables;").fetchall()
        queries = duckdb.conn.execute("FROM tpcds_queries()").fetchall()

        for (table,) in tables:
            Path(f"""{kwargs.get("target_path")}/{table}""").mkdir(parents=True, exist_ok=True)
            duckdb.export_table(
                table,
                export_type=kwargs.get("export_type"),
                target_path=kwargs.get("target_path"),
                partition_column=table_name_partition_column.get(table),
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

    db_file_path.unlink(missing_ok=True)
