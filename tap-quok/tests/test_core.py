"""Tests standard tap features using the built-in SDK tests library."""

import datetime

from singer_sdk.testing import get_tap_test_class, tap_tests

from tap_quok.tap import TapQuok

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "database": "test",
    "filtered_objects": [
        {
            "table": "test_table",
            "is_view": False
        }
    ],
    "default_replication_method": "FULL_TABLE",
    # TODO: Initialize minimal tap config
}


# Run standard built-in tap tests from the SDK:
TestTapQuok = get_tap_test_class(
    tap_class=TapQuok,
    config=SAMPLE_CONFIG,
)


# TODO: Create additional tests as appropriate for your tap.
# class TapCLIPrintsTest(tap_tests.TapTestTemplate):
#     "Test that the tap is able to print standard metadata."
#     name = "cli_prints"

#     def test(self):
#         self.tap.print_version()
#         self.tap.print_about()
#         self.tap.print_about(format="json")

# my_custom_tap_tests = TestSuite(
#     kind="tap", tests=[TapCLIPrintsTest]
# )