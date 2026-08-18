"""
Custom Methods Module for Unisys Cost Center Import Integration

This module contains helper functions for data transformation and business logic
specific to the cost center import process.

Note: The categorization logic has been moved to SQL queries in the master DAG
for better performance with large datasets.

Functions:
    get_log_generation_conf: Generate log generation DAG configuration
    format_log_message: Format log messages for cost center operations

Design Reference:
    Based on cost_center_design.txt business logic requirements
"""

import rail




def format_logs_callable():
    """
    Format and aggregate logs from all DAG runs
    Calculates success, error, and exception counts for email reporting
    """
    final_log_records = []
    final_log_records.extend(rail.load_all_records(
        rail.result("create_processing_log")))
    print(final_log_records)
    # Set counters for email template
    rail.set_result(
        key="error_record_count",
        val=len(
            list(
                filter(
                    lambda x: x["properties"]["status"] == "Error",
                    final_log_records,
                )
            )
        ),
    )
    rail.set_result(
        key="success_record_count",
        val=len(
            list(
                filter(
                    lambda x: x["properties"]["status"] == "Success",
                    final_log_records,
                )
            )
        ),
    )
    rail.set_result(
        key="exception_record_count",
        val=len(
            list(
                filter(
                    lambda x: x["properties"]["status"] == "Exception",
                    final_log_records,
                )
            )
        ),
    )

    return rail.write_json_artifact(final_log_records)
