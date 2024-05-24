"""Quok tap class."""

from __future__ import annotations

from singer_sdk import SQLTap
from singer_sdk import typing as th  # JSON schema typing helpers

from functools import cached_property

from tap_quok.client import QuokStream, QuokConnector


class TapQuok(SQLTap):
    """Quok tap class."""

    name = "tap-quok"
    default_stream_class = QuokStream

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "database",
            th.StringType,
            description="The name of the database to connect to",
        ),
        th.Property(
            "default_replication_method",
            th.StringType,
            default="FULL_TABLE",
            allowed_values=["FULL_TABLE", "INCREMENTAL"],
            description=(
                "Replication method to use if there is not a catalog entry to override "
                "this choice. One of `FULL_TABLE` or `INCREMENTAL`."
            ),
        ),
        th.Property(
            "filtered_objects",
            th.ArrayType(
                th.ObjectType(
                    th.Property(
                        "table",
                        th.StringType,
                        required=True,
                        description="The table name to be replicated",
                    ),
                    th.Property(
                        "is_view",
                        th.BooleanType,
                        required=False,
                        default=False,
                        description="The flag to indicate if the object is a view",
                    ),
                ),
            ),
            required=True,
            description="The list of objects to be replicated",
        )
    ).to_dict()

    @cached_property
    def connector(self) -> QuokConnector:
        return QuokConnector(
            config=dict(self.config),
        )

if __name__ == "__main__":
    TapQuok.cli()
