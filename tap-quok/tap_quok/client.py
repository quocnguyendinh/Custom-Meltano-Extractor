"""SQL client handling.

This includes QuokStream and QuokConnector.
"""

from __future__ import annotations

from typing import Any, Iterable

import sqlalchemy  # noqa: TCH002
from singer_sdk import SQLConnector, SQLStream

from singer_sdk import typing as th  # JSON schema typing helpers


class QuokConnector(SQLConnector):
    """Connects to the Quok SQL source."""

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Concatenate a SQLAlchemy URL for use in connecting to the source.

        Args:
            config: A dict with connection parameters

        Returns:
            SQLAlchemy connection string
        """
        # TODO: Replace this with a valid connection string for your source:
        return f"sqlite:///{config['database']}.db"

    @staticmethod
    def to_jsonschema_type(
        from_type: str
        | sqlalchemy.types.TypeEngine
        | type[sqlalchemy.types.TypeEngine],
    ) -> dict:
        """Returns a JSON Schema equivalent for the given SQL type.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            from_type: The SQL type as a string or as a TypeEngine. If a TypeEngine is
                provided, it may be provided as a class or a specific object instance.

        Returns:
            A compatible JSON Schema type definition.
        """
        # Optionally, add custom logic before calling the parent SQLConnector method.
        # You may delete this method if overrides are not needed.
        type_mapping = {
            "string": th.StringType(),
            "integer": th.IntegerType(),
            "float": th.NumberType(),
            "boolean": th.BooleanType(),
            "datetime": th.DateTimeType(),
            "date": th.DateType(),
            "time": th.TimeType(),
            "numeric": th.NumberType(),
            "largebinary": th.StringType(),
            "text": th.StringType(),
        }

        if isinstance(from_type, sqlalchemy.types.TypeEngine):
            sql_type = from_type.__class__.__name__
        else:
            sql_type = from_type

        if sql_type.lower() in type_mapping.keys():
            return type_mapping.get(sql_type.lower()).type_dict
        
        return type_mapping.get("string").type_dict
    
    def get_object_names(self, engine: sqlalchemy.Engine, inspected: sqlalchemy.Inspector, schema_name: str) -> list[tuple[str, bool]]:
        objects = self.config.get("filtered_objects")
        return [(obj["table"], obj.get("is_view", False)) for obj in objects]

class QuokStream(SQLStream):
    """Stream class for Quok streams."""

    connector_class = QuokConnector

    def get_records(self, partition: dict | None) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Developers may optionally add custom logic before calling the default
        implementation inherited from the base class.

        Args:
            partition: If provided, will read specifically from this data slice.

        Yields:
            One dict per record.
        """
        # Optionally, add custom logic instead of calling the super().
        # This is helpful if the source database provides batch-optimized record
        # retrieval.
        # If no overrides or optimizations are needed, you may delete this method.        
        selected_columns = self.get_selected_schema().get("properties").keys()
        table = self.connector.get_table(
            full_table_name=self.fully_qualified_name,
            column_names=selected_columns
        )
        query = table.select()
        if self.replication_key:
            replicated_key_col = table.columns[self.replication_key]
            query = query.order_by(replicated_key_col.asc())

            start_val = self.get_starting_replication_key_value(context=partition)
            if start_val:
                query = query.where(replicated_key_col >= start_val)
            
            with self.connector._connect() as conn:
                for record in conn.execute(query):
                    transformed_record = self.post_process(record)
                    if transformed_record is None:
                        continue
                    yield transformed_record
