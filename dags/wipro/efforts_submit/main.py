from datetime import timedelta
from pendulum import datetime,now
from wipro.efforts_submit.tasks.project_time_data import project_time_export
import rail
null = None
dag_created = []


def create_airflow_master_dag(config):
    for country in config.time_export_for_country:
        cnt=str(country).replace(" ","_")
        with rail.create_airflow_dag(
            dag_id=f"wipro_efforts_submission_process_project_time_master_{cnt}_{config.instance}",
            description=f"efforts submit to wipro master {country} {config.instance}",
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.master_max_active_runs,
            schedule_interval=config.schedule_interval,
            start_date=datetime(2023, 9, 28, tz=config.time_zone)
        ) as dag:

            process_start_time = rail.PythonOperator(
                task_id='process_start_time',
                python_callable=lambda: now(config.time_zone).strftime("%Y-%m-%d_%H:%M")
            )

            get_all_time_export_scripts = rail.RepliconServiceOperator(
                task_id="get_all_time_export_scripts",
                endpoint="/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts",
                data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                    response,
                    "displayText",
                    "Project Time export",
                    "uri"
                )
            )

            get_enabled_service_centers = rail.RepliconServiceOperator(
                task_id="get_enabled_service_centers",
                endpoint="/services/ServiceCenterService1.svc/GetEnabledServiceCenters",
                data_handler=lambda response, service_center=country: rail.find_first_by_attr_and_get_attr(
                    response,
                    "displayText",
                    service_center.strip(),
                    "uri"
                )
            )
            group_id = f"project_time_data_export_{cnt}"
            time_export_run = project_time_export(
                group_id=group_id, country_code=config.time_export_for_country_code[country])

            create_time_export_collection = rail.CreateCollectionOperator(
                task_id="create_time_export_collection",
                source='{{result("load_time_export_csv")}}',
                name=f"time_export_data_{cnt}",
                columns={'Employee ID': 'employee_id', 'Hours (Current)': 'hours_current',
                         'Project Name': 'project_name', 'Entry Date': 'entry_date',
                         'Timesheet Period': 'timesheet_period', 'Task Name': 'task_name',
                         'Task Code': 'task_code', 'Comments': 'comments',
                         'Project Export Type': 'project_export_type', 'Country Code': 'country_code', "Project Code": "project_code",
                         'Select a Type': 'select_a_type', "Select Type": "select_type", "Type":"type"}
            )

            if_no_export_data = rail.IfOperator(
                task_id="if_no_export_data",
                test='{{result("create_time_export_collection")|load_all_records|length < 1}}',
                yes_task="update_export_name_to_no_data",
                no_task="query_valid_time_export_collection"
            )

            update_export_name_to_no_data = rail.RepliconServiceOperator(
            task_id="update_export_name_to_no_data",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data=lambda country=country: {
                        "target": {
                            "uri": rail.result("get_time_export_batch_results"),
                            "name": null
                        },
                    "name": f"Time_export_{config.time_export_for_country_code[country]}_no_data_" + rail.result("process_start_time")
                }
            )

            log_to_sumo_no_export_data = rail.SendToSumoOperator(
                task_id="log_to_sumo_no_export_data",
                data={
                    "Country": country,
                    "JobStartTime": '{{result("process_start_time")}}',
                    "TimeExportName": f"Time_export_{config.time_export_for_country_code[country]}_no_data_" + '{{result("process_start_time")}}',
                    "TimeStamp": '{{ current_time_in_specified_tz("Etc/UTC", "%Y-%m-%dT%H:%M:%S") }}',
                    "Details":"No export data for the country"
                },
                sumo_conn_id=config.sumo_conn_id
            )

            query_valid_time_export_collection = rail.QueryCollectionOperator(
                task_id="query_valid_time_export_collection",
                query=f"""SELECT * FROM time_export_data_{cnt} WHERE
                        NULLIF("select_a_type","") IS NULL AND
                        NULLIF("select_type", "") IS NULL AND
                        NULLIF("type", "") IS NULL
                    """
            )

            if_valid_time_export_data = rail.IfOperator(
                task_id="if_valid_time_export_data",
                test='{{result("query_valid_time_export_collection", "length")>0}}',
                yes_task="query_unique_empid_time_sheet_period",
                no_task="get_export_name"
            )

            get_export_name = rail.PythonOperator(
                task_id="get_export_name",
                python_callable= lambda cntry=cnt:f"Time_export_{cntry}_oef_data_" + rail.result("process_start_time")
            )

            update_export_name_to_oef_data = rail.RepliconServiceOperator(
            task_id="update_export_name_to_oef_data",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data=lambda : {
                        "target": {
                            "uri": rail.result("get_time_export_batch_results"),
                            "name": null
                        },
                    "name": rail.result("get_export_name")
                }
            )

            log_to_sumo_oef_export_data = rail.SendToSumoOperator(
                task_id="log_to_sumo_oef_export_data",
                data={
                    "Country": country,
                    "JobStartTime": '{{result("process_start_time")}}',
                    "TimeExportName": '{{result("get_export_name")}}',
                    "TimeStamp": '{{ current_time_in_specified_tz("Etc/UTC", "%Y-%m-%dT%H:%M:%S") }}',
                    "Details":"No export entries.Only records with OEF entries."
                },
                sumo_conn_id=config.sumo_conn_id
            )

            query_unique_empid_time_sheet_period = rail.QueryCollectionOperator(
                task_id="query_unique_empid_time_sheet_period",
                query="""SELECT DISTINCT NULLIF(employee_id,"") as employee_id, timesheet_period
                            FROM query_valid_time_export_collection WHERE employee_id IS NOT NULL"""
            )

            log_to_sumo_export_data = rail.SendToSumoOperator(
                task_id="log_to_sumo_export_data",
                data={
                    "Country": country,
                    "JobStartTime": '{{result("process_start_time")}}',
                    "TimeExportName": f"Time_export_{config.time_export_for_country_code[country]}_" + '{{ result("process_start_time") }}',
                    "TimeStamp": '{{ current_time_in_specified_tz("Etc/UTC", "%Y-%m-%dT%H:%M:%S") }}',
                    "Details":"Export data for the country with "+'{{result("query_valid_time_export_collection")|load_all_records|length}}' + " records"
                },
                sumo_conn_id=config.sumo_conn_id
            )

            process_each_time_period_record = rail.trigger_parallel_dagrun(
                task_id="process_each_time_period_record",
                items='{{result("query_unique_empid_time_sheet_period")}}',
                trigger_dag_id=f"wipro_efforts_submission_process_project_time_master_child_process_period_{cnt}_{config.instance}",
                conf=lambda item:{
                    "employee_id": item["employee_id"],
                    "timesheet_period": item["timesheet_period"],
                    "time_export_data": rail.result("query_valid_time_export_collection"),
                    "time_export_name": rail.result("get_export_name")
                },
                parallel_count=config.max_active_parallel_runs,
                execution_timeout=timedelta(days=config.execution_timeout_days)
            )

            send_time_entry_submission_complete = rail.EmailOperator(
                task_id="send_time_entry_submission_complete",
                to=config.alert_mail,
                subject='{{ get_company_key() }} | Replicon time data extract completed for ' + country + ' - {{ result("process_start_time") }}',
                html_content="templates/email_export_complete.html",
                params={
                    "country":country
                }

            )
            log_to_sumo = rail.DagRunLogToSumoOperator(
                task_id="log_to_sumo",
                sumo_conn_id=config.sumo_conn_id
            )

            can_fail_dag = rail.IfOperator(
                task_id="can_fail_dag",
                test='{{get_error_message()|is_truthy}}',
                yes_task="fail_dagrun"
            )

            fail_dagrun = rail.FailOperator(
                task_id="fail_dagrun",
                message='{{get_error_message()}}'
            )

            process_start_time >> get_all_time_export_scripts >> get_enabled_service_centers >>\
                time_export_run >>\
                create_time_export_collection >> if_no_export_data >> rail.Label(
                    "Yes") >> update_export_name_to_no_data >> log_to_sumo_no_export_data >> log_to_sumo
            if_no_export_data >> rail.Label("No") >>\
            query_valid_time_export_collection>>\
            if_valid_time_export_data >> rail.Label("Yes") >>\
            query_unique_empid_time_sheet_period >>\
            log_to_sumo_export_data >>\
            process_each_time_period_record  >>\
            send_time_entry_submission_complete >>\
            log_to_sumo >> can_fail_dag >> fail_dagrun
            if_valid_time_export_data >> rail.Label("No") >> get_export_name >>\
            update_export_name_to_oef_data >> log_to_sumo_oef_export_data >>log_to_sumo
            dag_created.append(dag)
    return dag_created


rail.for_each_instance(create_airflow_master_dag)
