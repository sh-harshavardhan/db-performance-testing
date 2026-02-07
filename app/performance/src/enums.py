"""Enumeration used in the performance module."""

from enum import Enum


class DBType(str, Enum):
    """Enumeration for supported database types."""

    POSTGRESQL = "postgresql"
    DUCKDB = "duckdb"
    SNOWFLAKE = "snowflake"
    DATABRICKS = "databricks"
    BIGQUERY = "bigquery"
