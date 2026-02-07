"""Configuration settings for the API application."""

from typing import Optional, List, Dict, Any

from pydantic_settings import BaseSettings
from pydantic import field_validator, SecretStr, AnyHttpUrl, ValidationInfo, Field
from sqlalchemy.engine.url import URL
from src import enums


class Settings(BaseSettings):
    """Configuration settings for the API application."""

    BACKEND_CORS_ORIGINS: List[AnyHttpUrl] = []
    PROJECT_NAME: str = "API Application"
    DB_TYPE: enums.DBType = enums.DBType.DUCKDB
    DB_HOST: str
    DB_PORT: Optional[str]
    DB_USER: Optional[str]
    DB_PASSWORD: Optional[SecretStr]
    DB_NAME: Optional[str]
    DB_SCHEMA: Optional[str]

    DB_ADDITIONAL_PARAMS: Optional[Dict[str, Any]] = Field(default_factory=dict)

    DB_CONN_URL: Dict = None

    @field_validator("DB_CONN_URL", mode="before")
    @classmethod
    def generate_db_conn_url(cls, v: Optional[Dict], values: ValidationInfo) -> Dict:
        """Generates the database connection URL based on the provided database type and connection parameters."""
        db_type = values.data.get("DB_TYPE")
        if db_type == enums.DBType.DUCKDB:
            return {"url": URL.create(drivername="duckdb", database=v.get("HOSTNAME"))}
        elif db_type == enums.DBType.POSTGRESQL:
            check_mandate_fields(db_type, values.data, ["DB_USER"])
            return {
                "url": URL.create(
                    drivername="postgresql+psycopg2",
                    username=values.data.get("DB_USER"),
                    password=values.data.get("DB_PASSWORD").get_secret_value()
                    if values.data.get("DB_PASSWORD")
                    else None,
                    host=values.data.get("DB_HOST"),
                    port=values.data.get("DB_PORT", 5432),
                    database=values.data.get("DB_NAME", ""),
                    **values.data.get("ADDITIONAL_PARAMS", {}),
                ),
                "pool_recycle": 900,
            }
        elif db_type == enums.DBType.DATABRICKS:
            check_mandate_fields(db_type, values.data, ["DB_USER"])
            check_mandate_fields(
                db_type=db_type, data=values.data.get("ADDITIONAL_PARAMS", {}), required_fields=["DATABRICKS_HTTP_PATH"]
            )

            connect_args = {"_tls_verify_hostname": True, "_user_agent_entry": "API Application"}
            return {
                "url": URL.create(
                    drivername="databricks",
                    username=values.data.get("DB_USER"),
                    password=values.data.get("DB_PASSWORD").get_secret_value()
                    if values.data.get("DB_PASSWORD")
                    else None,
                    host=values.data.get("DB_HOST"),
                    port=443,
                    query={
                        "http_path": values.data.get("ADDITIONAL_PARAMS", {}).get("DATABRICKS_HTTP_PATH"),
                        "catalog": values.data.get("DB_NAME"),
                        "schema": values.data.get("DB_SCHEMA"),
                    },
                ),
                "connect_args": connect_args,
                "pool_recycle": 900,
            }

        elif db_type == enums.DBType.SNOWFLAKE:
            check_mandate_fields(db_type, values.data, ["DB_USER"])
            check_mandate_fields(
                db_type=db_type,
                data=values.data.get("ADDITIONAL_PARAMS", {}),
                required_fields=["SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_ROLE"],
            )
            connect_args = {
                "account": values.data.get("DB_HOST"),
                "warehouse": values.data.get("ADDITIONAL_PARAMS", {}).get("SNOWFLAKE_WAREHOUSE"),
                "role": values.data.get("ADDITIONAL_PARAMS", {}).get("SNOWFLAKE_ROLE"),
            }
            return {
                "url": URL.create(
                    drivername="snowflake",
                    username=values.data.get("DB_USER"),
                    password=values.data.get("DB_PASSWORD").get_secret_value()
                    if values.data.get("DB_PASSWORD")
                    else None,
                    host=values.data.get("DB_HOST"),
                    database=values.data.get("DB_NAME", ""),
                    query={
                        "schema": values.data.get("DB_SCHEMA", "PUBLIC"),
                        "warehouse": values.data.get("ADDITIONAL_PARAMS", {}).get("SNOWFLAKE_WAREHOUSE"),
                        "role": values.data.get("ADDITIONAL_PARAMS", {}).get("SNOWFLAKE_ROLE"),
                    },
                ),
                "connect_args": connect_args,
                "pool_recycle": 900,
            }

        elif db_type == enums.DBType.BIGQUERY:
            check_mandate_fields(
                db_type=db_type, data=values.data.get("ADDITIONAL_PARAMS", {}), required_fields=["BIGQUERY_PROJECT"]
            )
            return {
                "url": URL.create(
                    drivername="bigquery",
                    host=values.data.get("ADDITIONAL_PARAMS", {}).get("BIGQUERY_PROJECT"),
                    database=values.data.get("DB_NAME", ""),
                ),
                "pool_recycle": 900,
            }

        else:
            raise ValueError(f"Unsupported database type: {db_type}")


def check_mandate_fields(db_type, data, required_fields: List[str]):
    """Checks if the required fields are present in the provided data for a specific database type."""
    if not data:
        raise ValueError(f"{required_fields} are required for {db_type} database type.")
    for field in required_fields:
        if not data.get(field):
            raise ValueError(f"{field} is required for {db_type} database type.")


settings = Settings()
