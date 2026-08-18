import rail
from datetime import datetime as dt
from pendulum import  now

def approval_data_report_task(group_id, report_name, report_collection_name="datalake_time_extract_report"):

    with rail.TaskGroup(group_id=group_id) as tg:

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_report_details",
            report_name=report_name,
        )

        get_approval_date_filter_uri = rail.PythonOperator(
            task_id="get_approval_date_filter_uri",
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result(f"{group_id}.get_report_details")["filterConfiguration"]["enabledFilters"],
                "displayText",
                "ApprovalDateFilter",
                "uri",
            ),
        )

        get_report_date_range = rail.PythonOperator(
            task_id="get_report_date_range",
            python_callable=lambda dag_run: {
                "start_date": dag_run.conf["report_start_date"] if dag_run.conf.get("report_start_date") else now(tz="America/New_York").subtract(days=1).strftime("%Y/%m/%d"),
                "end_date": dag_run.conf["report_end_date"] if dag_run.conf.get("report_end_date") else now(tz="America/New_York").strftime("%Y/%m/%d"),
            },
        )

        generate_report = rail.run_report2(
            group_id="generate_report",
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result(f"{group_id}.get_report_details")["uri"],
                        "filterValues": [
                            {
                                "reportFilterUri": rail.result(f"{group_id}.get_approval_date_filter_uri"),
                                "value": None,
                            },
                            {
                                "reportFilterUri": rail.result(f"{group_id}.get_approval_date_filter_uri"),
                                "value": rail.result(f"{group_id}.get_report_date_range")["start_date"],
                            },
                            {
                                "reportFilterUri": rail.result(f"{group_id}.get_approval_date_filter_uri"),
                                "value": rail.result(f"{group_id}.get_report_date_range")["end_date"],
                            },
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                    }
                ]
            },
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id="load_report_data",
            document=f"{{{{ result('{group_id}.generate_report.get_report_result').reportGenerationResults[0].payload }}}}",
        )

        def _parse_report_date(raw):
            for fmt in ("%Y/%m/%d", "%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
                try:
                    return dt.strptime(raw, fmt).strftime("%m%d%Y")
                except (ValueError, AttributeError):
                    continue
            return ""

        def _parse_report_datetime(raw):
            for fmt in (
                "%Y/%m/%d %I:%M:%S %p", "%Y/%m/%d %H:%M:%S",
                "%m/%d/%Y %I:%M:%S %p", "%m/%d/%Y %H:%M:%S",
                "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S",
            ):
                try:
                    return dt.strptime(raw, fmt).strftime("%m%d%Y %H%M%S")
                except (ValueError, AttributeError):
                    continue
            return ""

        def add_unique_id(item):
            if not item:
                return item
            result = dict(item)
            employee_id = item.get("Employee ID", "")

            timesheet_period = item.get("Timesheet Period", "")
            start_date = ""
            end_date = ""
            if " - " in timesheet_period:
                parts = timesheet_period.split(" - ")
                start_date_raw = parts[0].strip()
                end_date_raw = parts[1].strip() if len(parts) > 1 else ""
                start_date = _parse_report_date(start_date_raw)
                end_date = _parse_report_date(end_date_raw)
            result["Unique ID"] = f"{employee_id}{start_date}"
            if start_date and end_date:
                result["Timesheet Period"] = f"{start_date} - {end_date}"

            submitted_on = (item.get("Submitted On") or "").strip()
            formatted_submitted = _parse_report_date(submitted_on.split(" ")[0]) if submitted_on else ""
            if formatted_submitted:
                result["Submitted On"] = formatted_submitted

            approval_dt = (item.get("Approval Date/Time") or "").strip()
            formatted_approval = _parse_report_datetime(approval_dt)
            if formatted_approval:
                result["Approval Date/Time"] = formatted_approval

            return result

        add_unique_id_task = rail.DataAdaptorOperator(
            task_id="add_unique_id_task",
            source=f"{{{{ result('{group_id}.load_report_data') }}}}",
            data=add_unique_id,
        )

        store_report_data = rail.CreateCollectionOperator(
            task_id="store_report_data",
            source=f"{{{{ result('{group_id}.add_unique_id_task') }}}}",
            name=report_collection_name,
            columns={
                "Unique ID": "unique_id",
                "Employee ID": "employee_id",
                "Timesheet Period": "timesheet_period",
                "Submitted On": "submitted_on",
                "Approval Date/Time": "approval_datetime",
                "Approval Status": "approval_status",
            },
        )

        get_report_details >> get_approval_date_filter_uri >> get_report_date_range >>\
        generate_report >> load_report_data >> add_unique_id_task >> store_report_data

    return tg