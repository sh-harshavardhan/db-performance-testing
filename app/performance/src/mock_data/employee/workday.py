"""Using : https://github.com/fivetran/dbt_workday/tree/main/integration_tests/seeds"""

from tempfile import TemporaryDirectory
import git
import shutil
from performance.src.utilities.duckdb import DuckDBConnection
from pathlib import Path


def main(**kwargs) -> None:
    """Main function to generate fake workday data by cloning the dbt_workday repository"""
    # The URL of the GitHub repository
    git_url = "https://github.com/fivetran/dbt_workday.git"

    Path(kwargs.get("target_path")).mkdir(parents=True, exist_ok=True)

    # Clone the repository
    with TemporaryDirectory() as temp_dir:
        git.Repo.clone_from(git_url, temp_dir)
        print(f"Successfully cloned {git_url} to {temp_dir}")

        kwargs.get("target_path")
        if kwargs.get("export_type") == "csv":
            for file in Path(temp_dir, "integration_tests", "seeds").glob("*.csv"):
                shutil.copy(Path(temp_dir, "integration_tests", "seeds", file), Path(kwargs.get("target_path")))
        elif kwargs.get("export_type") == "parquet":
            with DuckDBConnection() as duckdb:
                for table in Path(temp_dir, "integration_tests", "seeds").glob("*.csv"):
                    duckdb.conn.execute(
                        f"COPY (SELECT * FROM read_csv_auto('{table}')) "
                        f"TO '{Path(kwargs.get('target_path'), table.stem)}.parquet' "
                        f"(FORMAT PARQUET);"
                    )

    print(f"Successfully generated fake workday data in {kwargs.get('target_path')}")
