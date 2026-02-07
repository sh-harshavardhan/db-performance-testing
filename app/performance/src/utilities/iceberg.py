"""Iceberg Utilities"""

from typing import Dict

import pyarrow as pa
from pyiceberg.catalog import load_catalog


class IcebergConnection:
    """Utility class for managing connections to an Iceberg catalog and writing data to Iceberg tables."""

    def __init__(self, config: Dict):
        """Initializes the IcebergConnection with the provided configuration for the catalog."""
        self.glue_catalog = load_catalog(**config)

    def write_to_iceberg(self, database: str, table_name: str, data: pa.Table, **kwargs):
        """Writes a PyArrow Table to an Iceberg table."""
        table_exists = self.glue_catalog.table_exists(identifier=(database, table_name))
        if not table_exists:
            if None in [kwargs.get("location")]:
                raise ValueError("Table does not exist. Please provide a `location` to create the table.")

            table = self.glue_catalog.create_table_if_not_exists(
                identifier=(database, table_name),
                schema=data.schema,
                location=kwargs.get("location"),
                partition_spec=kwargs.get("partition_spec"),
                sort_order=kwargs.get("sort_order"),
            )

        else:
            table = self.glue_catalog.load_table(identifier=(database, table_name))

        try:
            table.append(data)
        except Exception as e:
            print(f"Error writing to Iceberg table {table.name()}: {e}")
            raise e

        print(f"Successfully written {data.num_rows} records to Iceberg table {table.name()}")
