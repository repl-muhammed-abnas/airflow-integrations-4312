import rail
from datetime import datetime as dt
from pendulum import  now

def approval_data_report_task(group_id, report_name,country, report_collection_name="oef_time_extract_report"):

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
                "TimeEntryApprovalDateFilter",
                "uri",
            ),
        )

        get_service_center_filter_uri = rail.PythonOperator(
                    task_id="get_service_center_filter_uri",
                    python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                        rail.result(f"{group_id}.get_report_details")["filterConfiguration"]["enabledFilters"],
                        "displayText",
                        "ServiceCenterFilter",
                        "uri",
                    ),
        )

        
        get_service_center_uri = rail.PythonOperator(
            task_id="get_service_center_uri",
            python_callable=lambda: rail.result("get_enabled_service_centers").split(":")[-1],
        )

        get_report_date_range = rail.PythonOperator(
            task_id="get_report_date_range",
            python_callable=lambda dag_run: {
                "start_date": dag_run.conf["report_start_date"] if dag_run.conf.get("report_start_date") else now(tz="Etc/UTC").subtract(days=1).strftime("%Y/%m/%d"),
                "end_date": dag_run.conf["report_end_date"] if dag_run.conf.get("report_end_date") else now(tz="Etc/UTC").strftime("%Y/%m/%d"),
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
                            {
                                "reportFilterUri": rail.result(f"{group_id}.get_service_center_filter_uri"),
                                "value":  rail.result(f"{group_id}.get_service_center_uri")
                            },
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                    }
                ]
            }
        )

        is_report_failed = rail.IfOperator(
            task_id="is_report_failed",
            test=f'{{{{ result("{group_id}.generate_report.get_report_result").reportGenerationResults[0].error | is_truthy }}}}',
            yes_task=f"{group_id}.fail_report_generation",
            no_task=f"{group_id}.load_report_data",
        )

        fail_report_generation = rail.FailOperator(
            task_id="fail_report_generation",
            message="Report run failed with errors"
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id="load_report_data",
            document=f"{{{{ result('{group_id}.generate_report.get_report_result').reportGenerationResults[0].payload }}}}",
        )

        def translate_rows(row):
            if not row:
                return []
            entry_date = dt.strptime(row["Entry Date"], "%Y/%m/%d").strftime("%Y%m%d")
            return {
            "entry_date": entry_date,
            "employee_id": row["Employee ID"],
            "project_name": row["Project Name"],
            "task_name": row["Task Name"],
            "hours": row["Hours"],
            "select_spain": row["Select_Spain"],
            "select_an_option": row["Select an Option"],
            "select_type": row["Select Type"],
            "ro_select_type": row["RO_Select_Type"],
            "select_a_type": row["Select A Type"],
            "type_selection": row["Type Selection"],
            "select_clm": row["Select"],
            "nl_select_activity": row["NL Select Activity"],
            "ksa_select_type": row["KSA Select Type"],
            "belgium_activity_selection": row["Belgium Activity Selection"],
            "ireland_activity": row["Ireland Activity"],
            "at_select_activity_all_in": row["AT_Select_Activity_All_IN"],
            "ch_activity": row["CH Activity"],
            "uk_select_type": row["UK Select Type"],
            "uk_overtime": row["UK - Overtime"],
        }

        get_entry_date = rail.DataAdaptorOperator(
            task_id="get_entry_date",
            source=f"{{{{ result('{group_id}.load_report_data') }}}}",
            columns=[
                 "entry_date",
                 "employee_id",
                 "project_name",
                 "task_name",
                 "hours",
                 "select_spain",
                 "select_an_option",
                 "select_type",
                 "ro_select_type",
                 "select_a_type",
                 "type_selection",
                 "select_clm",
                 "nl_select_activity",
                 "ksa_select_type",
                 "belgium_activity_selection",
                 "ireland_activity",
                 "at_select_activity_all_in",
                 "ch_activity",
                 "uk_select_type",
                 "uk_overtime",
            ],
            data=lambda row: translate_rows(row)
        )

        store_report_data = rail.CreateCollectionOperator(
            task_id="store_report_data",
            source=f"{{{{ result('{group_id}.get_entry_date')}}}}",
            name=report_collection_name
        )

        dedup_report_data = rail.QueryCollectionOperator(
            task_id="dedup_report_data",
            query="""SELECT employee_id, entry_date, project_name, task_name,
                            SUM(CAST(hours AS DECIMAL)) as hours,
                            select_a_type, select_type, type_selection, ro_select_type,
                            select_clm, select_an_option, ksa_select_type, nl_select_activity,
                            select_spain, belgium_activity_selection, ireland_activity,
                            at_select_activity_all_in, ch_activity, uk_select_type, uk_overtime
                     FROM oef_time_extract_report
                     where nullif(project_name,'') is not null and nullif(task_name,'') is not null
                     GROUP BY employee_id, entry_date, project_name, task_name,
                              select_a_type, select_type, type_selection, ro_select_type,
                              select_clm, select_an_option, ksa_select_type, nl_select_activity,
                              select_spain, belgium_activity_selection, ireland_activity,
                              at_select_activity_all_in, ch_activity, uk_select_type, uk_overtime""",
            name="dedup_oef_report",
        )

        get_report_details >> get_approval_date_filter_uri >> get_service_center_filter_uri>> get_service_center_uri >>\
        get_report_date_range >>\
        generate_report >> is_report_failed >> rail.Label("No") >> load_report_data >> get_entry_date >> store_report_data >> dedup_report_data
        is_report_failed >> rail.Label("Yes") >> fail_report_generation
    return tg