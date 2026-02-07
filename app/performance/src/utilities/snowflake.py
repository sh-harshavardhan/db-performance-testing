"""Snowflake Utilities"""

import os
from snowflake.connector import connect, SnowflakeConnection as SnowConn
from pydantic import SecretStr


class SnowflakeConnection:
    """A context manager for managing Snowflake database connections."""

    def __init__(self, user, password, account, warehouse, database, schema):
        """Initializes the SnowflakeConnection with the provided parameters or environment variables."""
        self.user = user if user else os.getenv("SNOWFLAKE_USER")
        self.password = SecretStr(password) if password else SecretStr(os.getenv("SNOWFLAKE_PASSWORD"))
        self.account = account if account else os.getenv("SNOWFLAKE_ACCOUNT")
        self.warehouse = warehouse if warehouse else os.getenv("SNOWFLAKE_WAREHOUSE")
        self.database = database if database else os.getenv("SNOWFLAKE_DATABASE")
        self.schema = schema if schema else os.getenv("SNOWFLAKE_SCHEMA")
        self.conn: SnowConn

        if None in [self.user, self.password, self.account, self.warehouse, self.database]:
            raise ValueError(
                "Snowflake connection parameters are not fully provided. "
                "Please provide all parameters or set them as environment variables."
            )

    def __enter__(self):
        """Establishes a connection to the Snowflake database using the provided parameters.
        Returns the SnowflakeConnection instance for use within the context.
        """
        self.conn = connect(
            user=self.user,
            password=self.password.get_secret_value(),
            account=self.account,
            warehouse=self.warehouse,
            database=self.database,
            schema=self.schema,
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Closes the Snowflake database connection when exiting the context,
        This ensures that the connection is properly closed and resources are released.
        If an exception occurred, it will be propagated after closing the connection.
        """
        self.close()

    def close(self):
        """Closes the Snowflake database connection."""
        self.conn.close()

    def execute_multiple_queries(self, queries) -> None:
        """Executes multiple SQL queries that doesnt return anything"""
        for query in queries:
            self.conn.cursor().execute(query)


# warehouse_kind, warehouse-size, cluster_count
warehouse_sizes_to_test = [
    ("STANDARD_GEN_1", "X-SMALL", 1),
    ("STANDARD_GEN_1", "SMALL", 1),
    ("STANDARD_GEN_1", "MEDIUM", 1),
    ("STANDARD_GEN_1", "LARGE", 1),
    ("STANDARD_GEN_1", "X-LARGE", 1),
    ("STANDARD_GEN_1", "2X-LARGE", 1),
    ("STANDARD_GEN_1", "3X-LARGE", 1),
    ("STANDARD_GEN_1", "3X-LARGE", 5),
    ("STANDARD_GEN_1", "4X-LARGE", 1),
    ("STANDARD_GEN_1", "5X-LARGE", 5),
    ("STANDARD_GEN_1", "6X-LARGE", 1),
    ("STANDARD_GEN_2", "X-SMALL", 1),
    ("STANDARD_GEN_2", "SMALL", 1),
    ("STANDARD_GEN_2", "MEDIUM", 1),
    ("STANDARD_GEN_2", "LARGE", 1),
    ("STANDARD_GEN_2", "X-LARGE", 1),
    ("STANDARD_GEN_2", "2X-LARGE", 1),
    ("STANDARD_GEN_2", "3X-LARGE", 1),
    ("STANDARD_GEN_2", "4X-LARGE", 1),
    ("STANDARD_GEN_2", "4X-LARGE", 5),
]
