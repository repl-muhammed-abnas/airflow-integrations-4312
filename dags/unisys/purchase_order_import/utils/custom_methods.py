import rail


def format_logs_callable():
    final_log_records = []
    final_log_records.extend(rail.load_all_records(
        rail.result("create_processing_log")))
    # Set counters for email template
    rail.set_result(
        key="total_record_count",
        val=len(final_log_records),
    )
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

    return rail.write_json_artifact(final_log_records)
