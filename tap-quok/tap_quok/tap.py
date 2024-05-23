"""Quok tap class."""

from __future__ import annotations

from singer_sdk import SQLTap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_quok.client import QuokStream


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
            "filtered_schema",
            th.StringType,
            required=False,
            description="The string of considered schemas (which is split by comma)",
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
    ).to_dict()


if __name__ == "__main__":
    TapQuok.cli()
