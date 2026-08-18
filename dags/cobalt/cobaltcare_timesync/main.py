from pendulum import datetime
from cobalt.cobaltcare_timesync import custom_methods, request_payload
from airflow.models import Variable
import rail
null = None


def create_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.time_sync_master_dag_id,
        description="cobalt care time sync",
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 9, 13),
        max_active_runs=config.max_active_runs,
        company_key=config.company_key,

    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_redirect_to_workato = rail.IfOperator(
            task_id='can_redirect_to_workato',
            test=lambda: Variable.get(
                config.can_redirect_to_workato_var_name, default_var='true').lower() == 'true',
            yes_task='post_to_workato',
            no_task='cobaltcare_log_lookup_table',
        )

        post_to_workato = rail.SimpleHttpOperator(
            task_id='post_to_workato',
            method='POST',
            http_conn_id=config.workato_api_endpoint,
            headers={
                'Content-Type': 'application/json; charset=utf-8'
            },
            data='{{ dag_run.conf.webhook.data | to_json }}',
        )

        cobaltcare_log_lookup_table = rail.CreateLogOperator(
            task_id="cobaltcare_log_lookup_table"
        )

        if_required_fields_are_not_present = rail.IfOperator(
            task_id="if_required_fields_are_not_present",
            test=lambda dag_run: bool(
                custom_methods.get_exception_message(dag_run)),
            yes_task="log_required_field_exception",
            no_task="if_tickedid_present"
        )

        log_required_field_exception = rail.WriteLogOperator(
            task_id="log_required_field_exception",
            log='{{result("cobaltcare_log_lookup_table")}}',
            severity="Exception",
            message=custom_methods.get_exception_message,
            properties=lambda dag_run: {
                "Logtype": "Time",
                "Username": dag_run.conf["webhook"]["data"]["currentUser"],
                "Project": dag_run.conf["webhook"]["data"]["project"],
                "Task": dag_run.conf["webhook"]["data"]["ticketid"],
                "Time": dag_run.conf["webhook"]["data"]["starttime"],
                "Status": "Exception",
                "Reason": custom_methods.get_exception_message(dag_run),
                "Parentjobid": rail.render_template('{{ecid()}}'),
                "childjobid": rail.render_template('{{ecid()}}'),
                "Logsent?": "no"
            }
        )

        if_tickedid_present = rail.IfOperator(
            task_id="if_tickedid_present",
            test=lambda dag_run: bool(
                dag_run.conf["webhook"]["data"]["ticketid"]),
            yes_task="search_for_projects",
            no_task="fail_dagrun"
        )

        search_for_projects = rail.RepliconServiceOperator(
            task_id="search_for_projects",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=lambda dag_run: {
                    "projects": [
                        {
                            "uri": null,
                            "name": dag_run.conf["webhook"]["data"]["project"],
                            "code": null,
                            "parameterCorrelationId": null
                        }
                    ]
            },
            data_handler=lambda response: response[0]["projectDetails"][
                "uri"] if response[0]["projectDetails"] else null
        )

        if_project_uri_present = rail.IfOperator(
            task_id="if_project_uri_present",
            test='{{result("search_for_projects")| is_truthy}}',
            yes_task="search_for_users",
            no_task="log_project_not_found_exception"
        )

        log_project_not_found_exception = rail.WriteLogOperator(
            task_id="log_project_not_found_exception",
            log='{{result("cobaltcare_log_lookup_table")}}',
            severity="Exception",
            message="Project not found in Replicon",
            properties=lambda dag_run: {
                "Logtype": "Time",
                "Username": dag_run.conf["webhook"]["data"]["currentUser"],
                "Project": dag_run.conf["webhook"]["data"]["project"],
                "Task": dag_run.conf["webhook"]["data"]["ticketid"],
                "Time": dag_run.conf["webhook"]["data"]["starttime"],
                "Status": "Exception",
                "Reason": "Project not found in Replicon",
                "Parentjobid": rail.render_template('{{ecid()}}'),
                "childjobid": rail.render_template('{{ecid()}}'),
                "Logsent?": "no"
            }
        )

        search_for_users = rail.RepliconServiceOperator(
            task_id="search_for_users",
            endpoint="services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: {
                    "users": [
                        {
                            "uri": null,
                            "loginName": dag_run.conf["webhook"]["data"]["currentUser"],
                            "employeeId": null,
                            "parameterCorrelationId": null
                        }
                    ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0]["userDetails"][
                'uri'] if response and response[0]["userDetails"]['isEnabled'] else null
        )

        if_user_enabled_in_replicon = rail.IfOperator(
            task_id="if_user_enabled_in_replicon",
            test='{{result("search_for_users")| is_truthy}}',
            yes_task="get_report_details",
            no_task="log_user_not_found_exception"
        )

        log_user_not_found_exception = rail.WriteLogOperator(
            task_id="log_user_not_found_exception",
            log='{{result("cobaltcare_log_lookup_table")}}',
            severity="Exception",
            message="User not found in Replicon or is disabled",
            properties=lambda dag_run: {
                "Logtype": "Time",
                "Username": dag_run.conf["webhook"]["data"]["currentUser"],
                "Project": dag_run.conf["webhook"]["data"]["project"],
                "Task": dag_run.conf["webhook"]["data"]["ticketid"],
                "Time": dag_run.conf["webhook"]["data"]["starttime"],
                "Status": "Exception",
                "Reason": "User " + dag_run.conf["webhook"]["data"]["currentUser"]+" not found in Replicon or is disabled",
                "Parentjobid": rail.render_template('{{ecid()}}'),
                "childjobid": rail.render_template('{{ecid()}}'),
                "Logsent?": "no"
            }
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_report_details",
            report_name=config.project_task_ref_report_name,
        )

        run_report_entry, run_report_exit = rail.run_report(
            group_id="run_project_task_report",
            report_params=lambda dag_run: {
                "reportParameters": [
                    {
                        "reportUri": rail.result("get_report_details")["uri"],
                        "filterValues": [
                            {
                                "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                                    rail.result("get_report_details")[
                                        "filterConfiguration"]["enabledFilters"],
                                    "displayText", "ProjectFilter", "uri"),
                                "value": rail.result("search_for_projects").split(":")[-1]
                            }
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        if_report_has_data = rail.IfOperator(
            task_id="if_report_has_data",
            test='{{result("run_project_task_report.get_report_result","has_data")|is_truthy}}',
            yes_task="parse_project_report_csv",
            no_task="fail_dagrun"
        )

        parse_project_report_csv = rail.LoadCSVFileOperator(
            task_id="parse_project_report_csv",
            document='{{result("run_project_task_report.get_report_result").reportGenerationResults[0].payload}}'
        )

        create_existing_tasks_collection = rail.CreateCollectionOperator(
            task_id="create_existing_tasks_collection",
            source='{{result("parse_project_report_csv")}}',
            name="existingreplicontasks"
        )

        query_to_check_if_task_present = rail.QueryCollectionOperator(
            task_id="query_to_check_if_task_present",
            query="""SELECT * FROM existingreplicontasks WHERE task_name='{{dag_run.conf.webhook.data.ticketid}}' """
        )

        if_query_has_records = rail.IfOperator(
            task_id="if_query_has_records",
            test='{{result("query_to_check_if_task_present", "length") > 0}}',
            yes_task="update_time_and_expense_entry_for_task",
            no_task="create_task"
        )

        update_time_and_expense_entry_for_task = rail.RepliconServiceOperator(
            task_id="update_time_and_expense_entry_for_task",
            endpoint="/services/TaskService1.svc/UpdateTimeAndExpenseEntryType",
            data=lambda: {
                    "taskUri": rail.load_all_records(rail.result("query_to_check_if_task_present"))[0]["taskuri"],
                    "timeAndExpenseEntryType": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable"
            }
        )

        create_task = rail.RepliconServiceOperator(
            task_id="create_task",
            endpoint="/services/ProjectService1.svc/PutTask",
            data=request_payload.get_create_task_request,
            data_handler=lambda response:response["uri"] if response else null
        )

        if_start_time_less_than_end_time = rail.IfOperator(
            task_id="if_start_time_less_than_end_time",
            test=lambda dag_run: bool(
                custom_methods.compare_start_end_time(dag_run)),
            no_task="log_out_before_in_exception",
            yes_task="put_time_entry_against_task"
        )

        log_out_before_in_exception = rail.WriteLogOperator(
            task_id="log_out_before_in_exception",
            log='{{result("cobaltcare_log_lookup_table")}}',
            severity="Exception",
            message="Out time is before the In time",
            properties=lambda dag_run: {
                "Logtype": "Time",
                "Username": dag_run.conf["webhook"]["data"]["currentUser"],
                "Project": dag_run.conf["webhook"]["data"]["project"],
                "Task": dag_run.conf["webhook"]["data"]["ticketid"],
                "Time": dag_run.conf["webhook"]["data"]["starttime"],
                "Status": "Exception",
                "Reason": "Out time is before the In time",
                "Parentjobid": rail.render_template('{{ecid()}}'),
                "childjobid": rail.render_template('{{ecid()}}'),
                "Logsent?": "no"
            }
        )

        put_time_entry_against_task = rail.RepliconServiceOperator(
            task_id="put_time_entry_against_task",
            endpoint="/services/TimeEntryService3.svc/PutTimeEntry",
            data=request_payload.get_time_entry_request
        )

        log_time_entry_added = rail.WriteLogOperator(
            task_id="log_time_entry_added",
            log='{{result("cobaltcare_log_lookup_table")}}',
            severity="Success",
            message="Time entry added",
            properties=lambda dag_run: {
                "Logtype": "Time",
                "Username": dag_run.conf["webhook"]["data"]["currentUser"],
                "Project": dag_run.conf["webhook"]["data"]["project"],
                "Task": dag_run.conf["webhook"]["data"]["ticketid"],
                "Time": dag_run.conf["webhook"]["data"]["starttime"],
                "Status": "Success",
                "Reason": "Time entry added",
                "Parentjobid": rail.render_template('{{ecid()}}'),
                "childjobid": rail.render_template('{{ecid()}}'),
                "Logsent?": "no"
            }
        )

        log_tosumo = rail.DagRunLogToSumoOperator(
            task_id="log_tosumo",
            sumo_conn_id="sumologic-dagrunlogger",
            trigger_rule="all_done"
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{get_error_message()|is_truthy}}',
            yes_task="fail_dagrun"
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message="{{get_error_message()}}"
        )

        can_redirect_to_workato >> rail.Label("Yes") >> post_to_workato >> log_tosumo
        can_redirect_to_workato >> rail.Label("No") >> cobaltcare_log_lookup_table

        cobaltcare_log_lookup_table >>\
            if_required_fields_are_not_present >> rail.Label(
                "Yes") >> log_required_field_exception >> log_tosumo
        if_required_fields_are_not_present >> rail.Label("No") >>\
            if_tickedid_present >> rail.Label("Yes") >> search_for_projects >>\
            if_project_uri_present >> rail.Label("Yes") >> search_for_users >>\
            if_user_enabled_in_replicon >> rail.Label("Yes") >> get_report_details >>\
            run_report_entry >> run_report_exit >>\
            if_report_has_data >> rail.Label("Yes") >> parse_project_report_csv >>\
            create_existing_tasks_collection >> query_to_check_if_task_present >>\
            if_query_has_records >> rail.Label("Yes") >>\
            update_time_and_expense_entry_for_task >> if_start_time_less_than_end_time
        if_query_has_records >> rail.Label("No") >> create_task >>\
            if_start_time_less_than_end_time >> rail.Label("Yes") >>\
            put_time_entry_against_task >> log_time_entry_added >> log_tosumo
        if_start_time_less_than_end_time >> rail.Label(
            "No") >> log_out_before_in_exception >> log_tosumo
        if_report_has_data >> rail.Label("No") >> fail_dagrun
        if_user_enabled_in_replicon >> rail.Label(
            "No") >> log_user_not_found_exception >> log_tosumo
        if_project_uri_present >> rail.Label(
            "No") >> log_project_not_found_exception >> log_tosumo
        if_tickedid_present >> rail.Label("No") >> fail_dagrun
        log_tosumo >> can_fail_dag >> fail_dagrun

        return dag


rail.for_each_instance(create_master_dag)
