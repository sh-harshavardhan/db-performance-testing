"""CLI for that generates mock data"""

import typer
from typing import Literal
from performance.src.utilities.common import timer

app = typer.Typer(help="Generate fake employee data")


@timer
@app.command(help="Create sample employee data")
def employee(
    scale_factor: int = typer.Option(100000, help="Number of fake employee records to generate"),
    num_of_threads: int = typer.Option(4, help="Number of parallel threads to use for data generation"),
    export_type: Literal["parquet", "csv"] = typer.Option("csv", help="Export format for the generated data"),
    target_path: str = typer.Option("fake_employee_data", help="Path where the data has to be exported"),
) -> None:
    """Command to generate fake employee data"""
    from performance.src.mock_data.employee.workday import main

    main(scale_factor=scale_factor, num_of_threads=num_of_threads, export_type=export_type, target_path=target_path)


@timer
@app.command(help="Create sample employee data in workday data model")
def workday(
    export_type: Literal["parquet", "csv"] = typer.Option("csv", help="Export format for the generated data"),
    target_path: str = typer.Option("fake_workday_data", help="Path where the data has to be exported"),
) -> None:
    """Command to generate fake employee data"""
    from performance.src.mock_data.employee.workday import main

    main(export_type=export_type, target_path=target_path)


@timer
@app.command("Create sample TPC-H data")
def tpc_h(
    scale_factor: Literal[1, 3, 10, 30, 100, 300, 1000, 3000] = typer.Option(
        3, help="Scale factor for TPC-H data generation"
    ),
    num_of_threads: int = typer.Option(4, help="Number of parallel threads to use for data generation"),
    export_type: Literal["parquet", "csv"] = typer.Option("csv", help="Export format for the generated data"),
    target_path: str = typer.Option("tpc_h_data_", help="Path where the data has to be exported"),
    sql_path: str = typer.Option("/sqls/tcp_h", help="Path where the TPC-H queries will be exported"),
) -> None:
    """Command to generate TPC-H data using DuckDB's TPC-H extension"""
    from performance.src.mock_data.tpc.tpc_h import main

    main(
        scale_factor=scale_factor,
        num_of_threads=num_of_threads,
        export_type=export_type,
        sql_path=sql_path,
        target_path=target_path,
    )


@timer
@app.command(help="Create sample TPC-DS data")
def tpc_ds(
    scale_factor: Literal[1, 10, 100, 999] = typer.Option(1, help="Scale factor for TPC-DS data generation"),
    export_type: Literal["parquet", "csv"] = typer.Option("csv", help="Export format for the generated data"),
    target_path: str = typer.Option("tpc_ds_data", help="Path where the data has to be exported"),
    sql_path: str = typer.Option("sqls/tcp_ds", help="Path where the TPC-DS queries will be exported"),
    file_size: int = typer.Option(128, help="Each file size in MB"),
) -> None:
    """Command to generate TPC-H data using DuckDB's TPC-DS extension"""
    from performance.src.mock_data.tpc.tpc_ds import main

    main(
        scale_factor=scale_factor,
        export_type=export_type,
        sql_path=sql_path,
        target_path=target_path,
        file_size=file_size,
    )
