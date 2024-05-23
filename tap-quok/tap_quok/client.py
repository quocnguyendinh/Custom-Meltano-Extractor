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
        return f"sqlite:///{config["database"]}.db"

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
            "largebinary": th.StringType()
        }

        if isinstance(from_type, sqlalchemy.types.TypeEngine):
            sql_type = from_type.__name__
        else:
            sql_type = from_type

        if sql_type.lower() in type_mapping.keys():
            return type_mapping.get(sql_type.lower())
        
        return type_mapping.get("string")

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
        yield from super().get_records(partition)
