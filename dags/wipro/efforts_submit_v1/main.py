from datetime import timedelta
from pendulum import datetime,now
from wipro.efforts_submit_v1.tasks.project_time_data import project_time_export
from airflow.models import Variable
import rail
null = None
dag_created = []


def create_airflow_master_dag(config):
    for country in config.time_export_for_country:
        cnt=str(country).replace(" ","_")
        with rail.create_airflow_dag(
            dag_id=f"{config.master_dag}_{cnt}_{config.instance}_v1",
            description=f"efforts submit to wipro master {country} {config.instance}",
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.master_max_active_runs,
            schedule_interval=config.schedule_interval,
            start_date=datetime(2023, 9, 28, tz=config.time_zone)
        ) as dag:

            def get_currnt_country_callable(dag_run):
                _dag_id = dag_run.dag_id
                _country = _dag_id.split(f"{config.master_dag}_")[1]
                _country = _country.split(f"_{config.instance}_v1")[0]
                _country = " ".join(_country.split("_"))
                return _country

            get_currnt_country = rail.PythonOperator(
                # Note: DO NOT REMOVE THIS TASK
                # This is being added to take the country dynamically in the below trigger_parallel_dagrun task
                # as we are making use of for-loop to create multiple dags for different countries, in functions 
                # the very last value is taken to tackel this this task is added.
                task_id = "get_currnt_country",
                python_callable=get_currnt_country_callable 
            )

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

            if_service_center_enabled = rail.IfOperator(
                task_id="if_service_center_enabled",
                test='{{result("get_enabled_service_centers")|is_truthy}}',
                yes_task="time_export_start",
                no_task="log_to_sumo"
            )

            time_export_start = rail.EmptyOperator(
                task_id="time_export_start"
            )

            group_id = f"project_time_data_export_{cnt}"
            time_export_run = project_time_export(
                group_id=group_id, country_code=config.time_export_for_country_code[country], country=country)

            create_time_export_collection = rail.CreateCollectionOperator(
                task_id="create_time_export_collection",
                source='{{result("load_time_export_csv")}}',
                name=f"time_export_data_{cnt}",
                columns={'Employee ID': 'employee_id', 'Hours (Current)': 'hours_current',
                         'Project Name': 'project_name', 'Entry Date': 'entry_date',
                         'Timesheet Period': 'timesheet_period', 'Task Name': 'task_name',
                         'Task Code': 'task_code', 'Comments': 'comments',
                         'Project Export Type': 'project_export_type', 'Country Code': 'country_code', "Project Code": "project_code",
                         'Select A Type': 'select_a_type', "Select type": "select_type", "Type":"type",
                         'Select an Option': 'select_an_option', "Login Name": "login_name", "In Time":"in_time",
                         'Out Time': 'out_time', 'Type Selection': 'type_selection','RO_Select_Type': 'ro_select_type',
                         'RO_Select': 'ro_select', 'Select':'select_clm', 'KSA Select Type': 'ksa_select_type',
                         'Select_Spain': 'select_spain', 'NL Select Activity': 'nl_select_activity',
                         'Work Location KSA': 'work_location_ksa', 'Work Location': 'work_location',
                         'Belgium Activity Selection': 'belgium_activity_selection', 'Ireland Activity': 'ireland_activity',
                         'AT_Select_Activity_All_IN': 'at_select_activity_all_in', 'CH Select Activity': 'ch_select_activity',
                         'UK Select Type': 'uk_select_type', 'UK - Overtime': 'uk_overtime'
                        }
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
                        NULLIF("type", "") IS NULL AND
                        NULLIF("type_selection", "") IS NULL AND
                        NULLIF("ro_select_type", "") IS NULL AND
                        NULLIF("ro_select", "") IS NULL AND
                        NULLIF("select_clm", "") IS NULL AND
                        NULLIF("select_an_option", "") IS NULL AND
                        NULLIF("project_name","") IS NOT NULL AND
                        NULLIF("ksa_select_type", "") IS NULL AND
                        NULLIF("nl_select_activity", "") IS NULL AND
                        NULLIF("select_spain", "") IS NULL AND
                        NULLIF("belgium_activity_selection", "") IS NULL AND
                        NULLIF("ireland_activity", "") IS NULL AND
                        NULLIF("at_select_activity_all_in", "") IS NULL AND
                        NULLIF("ch_select_activity", "") IS NULL AND
                        NULLIF("uk_select_type", "") IS NULL AND
                        NULLIF("uk_overtime", "") IS NULL
                    """
            )

            if_valid_time_export_data = rail.IfOperator(
                task_id="if_valid_time_export_data",
                test=lambda:int(rail.render_template('{{result("query_valid_time_export_collection", "length")}}')) > 0 and \
                    Variable.get(rail.result("get_currnt_country"), "true").lower() == "true",
                yes_task="query_unique_empid_time_sheet_period",
                no_task="start_oef"
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
                trigger_dag_id=f"{config.process_time_period_child}_{cnt}_{config.instance}_v1",
                conf=lambda item:{
                    "employee_id": item["employee_id"],
                    "timesheet_period": item["timesheet_period"],
                    "time_export_data": rail.result("query_valid_time_export_collection"),
                    "time_export_name": rail.result("get_export_name")
                },
                parallel_count=config.max_active_parallel_runs,
                execution_timeout=timedelta(days=config.execution_timeout_days)
            )

            start_oef = rail.EmptyOperator(
                task_id="start_oef"
            )

            can_trigger_ot_oncall = rail.IfOperator(
                task_id="can_trigger_ot_oncall",
                test=lambda:Variable.get(
                    f"can_trigger_ot_oncall_{rail.result('get_currnt_country').replace(' ', '_')}",
                    "true").lower() == "true",
                yes_task="query_valid_ot_oncall_callout_time_export_collection",
                no_task="send_regular_time_entry_submission_complete"
            )

            query_valid_ot_oncall_callout_time_export_collection = rail.QueryCollectionOperator(
                task_id="query_valid_ot_oncall_callout_time_export_collection",
                query=f"""SELECT * FROM time_export_data_{cnt} WHERE
                        (NULLIF("project_code","") IS NOT NULL AND
                            (
                                NULLIF("select_a_type","") IS NOT NULL OR
                                NULLIF("select_type", "") IS NOT NULL OR
                                NULLIF("type", "") IS NOT NULL OR
                                NULLIF("type_selection", "") IS NOT NULL OR
                                NULLIF("ro_select_type", "") IS NOT NULL OR
                                NULLIF("ro_select", "") IS NOT NULL OR
                                NULLIF("select_clm", "") IS NOT NULL OR
                                NULLIF("select_an_option", "") IS NOT NULL OR 
                                NULLIF("ksa_select_type", "") IS NOT NULL OR 
                                NULLIF("nl_select_activity", "") IS NOT NULL OR 
                                NULLIF("select_spain", "") IS NOT NULL OR
                                NULLIF("belgium_activity_selection", "") IS NOT NULL OR
                                NULLIF("ireland_activity", "") IS NOT NULL OR
                                NULLIF("at_select_activity_all_in", "") IS NOT NULL OR
                                NULLIF("ch_select_activity", "") IS NOT NULL OR
                                NULLIF("uk_select_type", "") IS NOT NULL OR
                                NULLIF("uk_overtime", "") IS NOT NULL
                            )
                        )
                    """
            )

            if_valid_ot_oncall_callout_time_export_data = rail.IfOperator(
                task_id="if_valid_ot_oncall_callout_time_export_data",
                test='{{result("query_valid_ot_oncall_callout_time_export_collection", "length")>0}}',
                yes_task="if_valid_time_export_has_data",
                no_task="send_time_entry_submission_complete"
            )

            if_valid_time_export_has_data = rail.IfOperator(
                task_id="if_valid_time_export_has_data",
                test='{{result("query_valid_ot_oncall_callout_time_export_collection", "length") > 0}}',
                yes_task="query_unique_empid_time_sheet_period_for_ot_oncall_callout",
                no_task="get_export_name"
            )

            query_unique_empid_time_sheet_period_for_ot_oncall_callout = rail.QueryCollectionOperator(
                task_id="query_unique_empid_time_sheet_period_for_ot_oncall_callout",
                query="""SELECT DISTINCT NULLIF(employee_id,"") as employee_id, timesheet_period
                            FROM query_valid_ot_oncall_callout_time_export_collection WHERE employee_id IS NOT NULL"""
            )

            log_to_sumo_ot_oncall_callout_export_data = rail.SendToSumoOperator(
                task_id="log_to_sumo_ot_oncall_callout_export_data",
                data={
                    "Country": country,
                    "JobStartTime": '{{result("process_start_time")}}',
                    "TimeExportName": f"Time_export_{config.time_export_for_country_code[country]}_" + '{{ result("process_start_time") }}',
                    "TimeStamp": '{{ current_time_in_specified_tz("Etc/UTC", "%Y-%m-%dT%H:%M:%S") }}',
                    "Details":"Export data for the country with "+'{{result("query_valid_ot_oncall_callout_time_export_collection")|load_all_records|length}}' + " records"
                },
                sumo_conn_id=config.sumo_conn_id
            )

            process_each_time_period_record_ot_oncall_callout = rail.trigger_parallel_dagrun(
                task_id="process_each_time_period_record_ot_oncall_callout",
                items='{{result("query_unique_empid_time_sheet_period_for_ot_oncall_callout")}}',
                trigger_dag_id=f"{config.process_ot_time_period_child}_{cnt}_{config.instance}_v1",
                conf=lambda item:{
                    "cntry": rail.result('get_currnt_country'),
                    "employee_id": item["employee_id"],
                    "timesheet_period": item["timesheet_period"],
                    "time_export_data": rail.result("query_valid_ot_oncall_callout_time_export_collection"),
                    "time_export_name": rail.result("get_export_name")
                },
                parallel_count=config.max_active_parallel_runs,
                execution_timeout=timedelta(days=config.execution_timeout_days)
            )

            send_time_entry_submission_complete = rail.EmailOperator(
                task_id="send_time_entry_submission_complete",
                to=config.alert_mail,
                bcc=config.internal_logs_email,
                subject='{{ get_company_key() }} | Replicon time data extract completed for ' + country + ' - {{ result("process_start_time") }}',
                html_content="templates/email_export_complete.html",
                params={
                    "country":country
                }
            )

            send_regular_time_entry_submission_complete = rail.EmailOperator(
                task_id="send_regular_time_entry_submission_complete",
                to=config.alert_mail,
                bcc=config.internal_logs_email,
                subject='{{ get_company_key() }} | Replicon time data extract completed for ' + country + ' - {{ result("process_start_time") }}',
                html_content="templates/email_regular_export_complete.html",
                params=None
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

            get_currnt_country >> process_start_time >> get_all_time_export_scripts >> get_enabled_service_centers >>\
            if_service_center_enabled >> rail.Label("No") >> log_to_sumo
            if_service_center_enabled >> rail.Label("Yes") >> time_export_start >>\
                time_export_run >>\
                create_time_export_collection >> if_no_export_data >> rail.Label(
                    "Yes") >> update_export_name_to_no_data >> log_to_sumo_no_export_data >> log_to_sumo
            if_no_export_data >> rail.Label("No") >>\
            query_valid_time_export_collection>>\
            if_valid_time_export_data >> rail.Label("Yes") >>\
            query_unique_empid_time_sheet_period >>\
            log_to_sumo_export_data >>\
            process_each_time_period_record  >> start_oef >> can_trigger_ot_oncall
            if_valid_time_export_data >> rail.Label("No") >> start_oef
            can_trigger_ot_oncall >> rail.Label("Yes") >> query_valid_ot_oncall_callout_time_export_collection
            can_trigger_ot_oncall >> rail.Label("No") >> send_regular_time_entry_submission_complete >> log_to_sumo
            query_valid_ot_oncall_callout_time_export_collection>> if_valid_ot_oncall_callout_time_export_data
            if_valid_ot_oncall_callout_time_export_data >> rail.Label("Yes") >> if_valid_time_export_has_data
            if_valid_time_export_has_data >> rail.Label("Yes") >>\
            query_unique_empid_time_sheet_period_for_ot_oncall_callout >> \
            log_to_sumo_ot_oncall_callout_export_data >> process_each_time_period_record_ot_oncall_callout >>\
            send_time_entry_submission_complete >> log_to_sumo >> can_fail_dag >> fail_dagrun
            if_valid_time_export_has_data >> rail.Label("No") >> get_export_name
            get_export_name >>\
            update_export_name_to_oef_data >> log_to_sumo_oef_export_data >>query_unique_empid_time_sheet_period_for_ot_oncall_callout
            if_valid_ot_oncall_callout_time_export_data >> rail.Label("No") >> send_time_entry_submission_complete
            dag_created.append(dag)
    return dag_created


rail.for_each_instance(create_airflow_master_dag)
