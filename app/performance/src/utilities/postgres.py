"""This module provides a class for managing a connection to a PostgreSQL database and executing SQL queries."""

import psycopg2
from pydantic import SecretStr


class PostgresConnection:
    """A context manager for managing a connection to a PostgreSQL database and executing SQL queries."""

    def __init__(self, host, port, database, user, password, conn_args=None):
        """Initializes the PostgresConnection with the provided parameters."""
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = SecretStr(password)
        self.conn_args = conn_args if conn_args else {}

    def __enter__(self):
        """Returns the PostgresConnection instance for use within the context."""
        self.connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            database=self.database,
            user=self.user,
            password=self.password.get_secret_value(),
            **self.conn_args,
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Closes the connection to the PostgreSQL database when exiting the context,
        This ensures that the connection is properly closed and resources are released.
        If an exception occurred, it will be propagated after closing the connection.
        """
        self.close()

    def execute_query(self, query):
        """Executes a SQL query and returns the results as a list of tuples."""
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall()

    def close(self):
        """Closes the connection to the PostgreSQL database."""
        self.connection.close()

    def execute_multiple_queries(self, queries) -> None:
        """Executes multiple SQL queries that doesnt return anything"""
        with self.connection.cursor() as cursor:
            for query in queries:
                cursor.execute(query)
        self.connection.commit()
