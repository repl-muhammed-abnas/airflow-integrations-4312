from datetime import timedelta
from pendulum import datetime as pdt
from wipro.user_import_global_dags.utils import custom_methods
import rail
null = None


def create_airflow_master_dag(config):
    cntry = config.country.lower().replace(" ", "_")
    with rail.create_airflow_dag(
        dag_id=f"wipro_disable_user_{cntry}_{config.instance}",
        description="netherlands disable user",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.disable_schedule_interval,
        start_date=pdt(2023, 12, 18, tz=config.time_zone),
        max_active_runs=config.master_max_active_run,
    ) as dag:

        create_netherlands_disable_user_log = rail.CreateLogOperator(
            task_id=f"create_{cntry}_disable_user_log"
        )

        get_country_uri = rail.RepliconServiceOperator(
            task_id="get_country_uri",
            endpoint="/services/ServiceCenterService1.svc/GetAllServiceCenters",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response,
                "displayText",
                config.country,
                "uri"
            )
        )

        get_user_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_user_report_details",
            report_name=config.user_report_details,
        )

        run_report_start, run_report_end = rail.run_report(
            group_id="disable_user_report",
            report_params=custom_methods.get_report_params
        )

        report_end = rail.EmptyOperator(task_id="report_end")

        if_error_in_report_run = rail.IfOperator(
            task_id="if_error_in_report_run",
            test="{{result('disable_user_report.get_report_result')['reportGenerationResults'][0].error | is_truthy}}",
            yes_task="fail_report_run",
            no_task="report_has_data"
        )

        fail_report_run = rail.FailOperator(
            task_id="fail_report_run",
            message="Base report run error"
        )

        report_has_data = rail.IfOperator(
            task_id = "report_has_data",
            test="{{ result('disable_user_report.get_report_result', 'has_data') }}",
            yes_task='report_has_expected_columns',
            no_task="fail_no_data"
        )

        fail_no_data = rail.FailOperator(
            task_id="fail_no_data",
            message="Base report has no data"
        )

        report_has_expected_columns = rail.IfOperator(
            task_id='report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{ result('disable_user_report.get_report_result').reportGenerationResults[0].payload | \
                starts_with('%s') }}" % config.expected_report_columns,
            yes_task='load_report_data',
            no_task='fail_no_expected_columns',
        )

        fail_no_expected_columns = rail.FailOperator(
            task_id='fail_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id = 'load_report_data',
            document = "{{ result('disable_user_report.get_report_result').reportGenerationResults[0].payload }}",
            headers=["user_name","user_first_name",
                     "user_last_name","user_end_date",
                     "local_hire_daydiff",
                     "employee_id","login_name",
                     "country","onsite_direct_recruit","onsite_end_date","assignee_daydiff","useruri","userstatus"]
        )

        create_all_user_collection = rail.CreateCollectionOperator(
            task_id="create_all_user_collection",
            source='{{result("load_report_data")}}',
            name="all_user_records"
        )

        query_users_with_daydiff_as_2 = rail.QueryCollectionOperator(
            task_id="query_users_with_daydiff_as_2",
            query="""SELECT * FROM all_user_records WHERE
            NULLIF(employee_id,"") IS NOT NULL AND
            userstatus = 'Enabled' AND ((local_hire_daydiff < CAST(-2 AS FLOAT) AND NULLIF(user_end_date,"") IS NOT NULL AND onsite_direct_recruit != "ASSIGNEE") OR
            (assignee_daydiff < CAST(-2 AS FLOAT) AND NULLIF(onsite_end_date,"") IS NOT NULL AND onsite_direct_recruit = "ASSIGNEE")) """,
            name="users_with_enddate"
        )

        query_all_assignee_users_with_enddate = rail.QueryCollectionOperator(
            task_id="query_all_assignee_users_with_enddate",
            query="""SELECT * FROM users_with_enddate WHERE LOWER(onsite_direct_recruit) ='assignee' """
        )

        query_all_local_hire_users_with_enddate = rail.QueryCollectionOperator(
            task_id="query_all_local_hire_users_with_enddate",
            query="""SELECT * FROM users_with_enddate WHERE LOWER(onsite_direct_recruit) ='local_hire' """
        )

        get_all_active_users_with_enddate = rail.PythonOperator(
            task_id="get_all_active_users_with_enddate",
            python_callable=custom_methods.get_all_users_with_enddate_data
        )

        is_user_with_enddate_present = rail.IfOperator(
            task_id="is_user_with_enddate_present",
            test=lambda:rail.result("get_all_active_users_with_enddate"),
            yes_task="for_each_user_delete_timeoff_start",
            no_task="end_disable_user"
        )

        for_each_user_delete_timeoff_start = rail.EmptyOperator(task_id="for_each_user_delete_timeoff_start")

        end_disable_user = rail.EmptyOperator(task_id="end_disable_user")

        for_each_user_delete_timeoff = rail.ForEachOperator(
            task_id="for_each_user_delete_timeoff",
            items='{{result("get_all_active_users_with_enddate")|to_json}}',
            start_task="get_data_forall_timeoff_after_the_enddate",
            end_task="end_each_user_delete_timeoff"
        )

        get_data_forall_timeoff_after_the_enddate = rail.RepliconServiceOperator(
            task_id='get_data_forall_timeoff_after_the_enddate',
            endpoint='/services/TimeOffListService1.svc/GetData',
            data=custom_methods.get_data_for_all_future_timeoff_after_the_enddate,
            response_filter=custom_methods.map_time_off_delete_uri
        )

        if_time_off_present = rail.IfOperator(
            task_id='if_time_off_present',
            test=lambda: bool(rail.result(
                'get_data_forall_timeoff_after_the_enddate')),
            yes_task='create_timeOff_delete_batch',
            no_task='disable_user_in_replicon'
        )

        create_timeOff_delete_batch = rail.RepliconServiceOperator(
            task_id="create_timeOff_delete_batch",
            endpoint="/services/TimeOffService1.svc/CreateTimeOffDeleteBatch",
            data=custom_methods.create_timeOff_delete_batch
        )

        execute_timeOff_delete_batch = rail.RepliconServiceOperator(
            task_id="execute_timeOff_delete_batch",
            endpoint="/services/TimeOffService1.svc/ExecuteTimeOffDeleteBatch",
            data=custom_methods.execute_timeOff_delete_batch
        )

        write_log_timeoff_deleted_for_user = rail.WriteLogOperator(
            task_id="write_timeoff_deleted_for_user",
            log='{{result("create_'+cntry+'_disable_user_log")}}',
            message="User time off deleted",
            properties={
                "employee_id": '{{result("for_each_user_delete_timeoff").employee_id}}',
                "enddate": '{{result("for_each_user_delete_timeoff").enddate}}',
                "employee_first_name": '{{result("for_each_user_delete_timeoff").first_name}}',
                "employee_last_name": '{{ result("for_each_user_delete_timeoff").last_name}}',
                "country": '{{result("for_each_user_delete_timeoff").country}}',
                "status": "Success",
                "details": "Time off deleted",
                "ecid": '{{dag_run_ecid()}}'
            }
        )

        disable_user_in_replicon = rail.RepliconServiceOperator(
            task_id='disable_user_in_replicon',
            endpoint='services/SecurityService1.svc/DisableLogin',
            data={
                'userUri': '{{ result("for_each_user_delete_timeoff").useruri }}'
            }
        )

        write_log_user_disabled_in_replicon = rail.WriteLogOperator(
            task_id="write_log_user_disabled_in_replicon",
            log='{{result("create_'+cntry+'_disable_user_log")}}',
            message="User disabled",
            trigger_rule="all_success",
            properties={
                "employee_id": '{{result("for_each_user_delete_timeoff").employee_id}}',
                "enddate": '{{result("for_each_user_delete_timeoff").enddate}}',
                "employee_first_name": '{{result("for_each_user_delete_timeoff").first_name}}',
                "employee_last_name": '{{ result("for_each_user_delete_timeoff").last_name}}',
                "country": '{{result("for_each_user_delete_timeoff").country}}',
                "status": "Success",
                "details": "User disabled",
                "ecid": '{{dag_run_ecid()}}'
            }
        )

        write_log_user_disabled_in_replicon_failed = rail.WriteLogOperator(
            task_id="write_log_user_disabled_in_replicon_failed",
            log='{{result("create_'+cntry+'_disable_user_log")}}',
            message="User disabled",
            trigger_rule="one_failed",
            severity="Error",
            properties=lambda: {
                "employee_id": rail.result("for_each_user_delete_timeoff")["employee_id"],
                "enddate": rail.result("for_each_user_delete_timeoff")["enddate"],
                "employee_first_name": '{{result("for_each_user_delete_timeoff").first_name}}',
                "employee_last_name": '{{ result("for_each_user_delete_timeoff").last_name}}',
                "country": '{{result("for_each_user_delete_timeoff").country}}',
                "status": "Failure",
                "details": "Disable user not processed" + rail.render_template('{{get_error_message()}}'),
                "ecid": rail.render_template('{{dag_run_ecid()}}')
            }
        )

        end_each_user_delete_timeoff = rail.EmptyOperator(
            task_id="end_each_user_delete_timeoff")

        process_logs = rail.TriggerDagRunOperator(
            task_id="process_logs",
            trigger_dag_id=f"wipro_user_import_logs_{cntry}_master_{config.instance}",
            wait_for_completion=True,
            execution_timeout=timedelta(config.execution_timeout),
            conf=lambda dag_run:{
                "parent_run_id":dag_run.id,
                "disable_user": True
            }
        )

        create_netherlands_disable_user_log >>\
        get_country_uri >> get_user_report_details >>\
        run_report_start >>\
        run_report_end>> report_end >>\
        if_error_in_report_run >> rail.Label("Yes") >> fail_report_run
        if_error_in_report_run >> rail.Label("No") >>\
        report_has_data >> rail.Label("No") >> fail_no_data
        report_has_data >> rail.Label("Yes") >>\
        report_has_expected_columns >> rail.Label("No") >>\
        fail_no_expected_columns
        report_has_expected_columns >> rail.Label("Yes") >>\
        load_report_data >> create_all_user_collection >>\
        query_users_with_daydiff_as_2 >> query_all_local_hire_users_with_enddate >>\
        query_all_assignee_users_with_enddate >>\
        get_all_active_users_with_enddate>>\
        is_user_with_enddate_present
        is_user_with_enddate_present >> rail.Label("No") >> end_disable_user
        is_user_with_enddate_present >> rail.Label("Yes")>>\
        for_each_user_delete_timeoff_start>>\
            for_each_user_delete_timeoff >> end_each_user_delete_timeoff
        for_each_user_delete_timeoff >> \
            get_data_forall_timeoff_after_the_enddate >>\
            if_time_off_present >> rail.Label("Yes") >> create_timeOff_delete_batch >>\
            execute_timeOff_delete_batch >> write_log_timeoff_deleted_for_user >>\
            disable_user_in_replicon
        if_time_off_present >> rail.Label("No") >> disable_user_in_replicon >>\
            write_log_user_disabled_in_replicon >> write_log_user_disabled_in_replicon_failed >>\
            end_each_user_delete_timeoff >> process_logs >> end_disable_user

    return dag


rail.for_each_instance(create_airflow_master_dag)
