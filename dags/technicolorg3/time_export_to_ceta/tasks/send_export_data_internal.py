import rail
from technicolorg3.time_export_to_ceta.utils import custom_methods


def create_send_export_data_internal(config, sub_erp):
    with rail.TaskGroup(group_id=f"send_export_data_internal_{sub_erp}", prefix_group_id=False) as send_export_data_internal:

        has_any_records = rail.IfOperator(
            task_id=f"has_any_{sub_erp}_records",
            test=lambda: bool(rail.result(f'process_{sub_erp}_data', key='length' if sub_erp == "skipped" else None)),
            yes_task=f"create_{sub_erp}_data_csv_file"
        )

        create_csv_file = rail.WriteCSVFileOperator(
            task_id=f"create_{sub_erp}_data_csv_file",
            source=lambda: rail.result(f'process_{sub_erp}_data'),
            header=lambda: custom_methods.get_csv_headers(sub_erp),
            row=lambda item:custom_methods.get_rows(item, sub_erp)
        )

        open_bracket = '{{'
        close_bracket = '}}'

        send_internal_email = rail.EmailOperator(
            task_id=f"send_internal_email_{sub_erp}",
            to=config.internal_logs_email,
            subject=f"Final Export Data {sub_erp} - {open_bracket} current_time() {close_bracket}",
            html_content=f"""<p>PFA Final Data to be exported for {sub_erp} - {open_bracket} current_time() {close_bracket}</p>"""
                         +("<p>Job run id: {{ run_id }} </p>" if sub_erp == "skipped" else ""),
            files=[
                (sub_erp + f"{open_bracket} result('get_required_details').file_name {close_bracket}" + ".csv",
                    f"{open_bracket} result('create_{sub_erp}_data_csv_file') {close_bracket}")
            ]
        )

        has_any_records >> rail.Label(
            "Yes") >> create_csv_file >> send_internal_email

    return send_export_data_internal
