
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'michaelkorstna_uk_user_import_disable_users_child_{config.instance}',
        description=f'MichaelKorsTnA_UK_disable users v1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_today_4'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_today_4',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_today_4 = rail.PythonOperator(
            task_id='log_today_4',
            python_callable=lambda: datetime.utcnow().strftime("%d/%m/%Y")
        )

        get_my_actual_user_identity_6 = rail.RepliconServiceOperator(
            task_id='get_my_actual_user_identity_6',
            endpoint="/services/UserAccessControlService1.svc/GetMyActualUserIdentity",
        )

        get_all_time_off_types_7 = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types_7',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
        )

        get_userlist_to_disable_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_userlist_to_disable_report_details',
            report_name=config.userlist_to_disable_report
        )

        if_log_get_user_listtodisablereport_9_blank_10 = rail.IfOperator(
            task_id='if_log_get_user_listtodisablereport_9_blank_10',
            test='''{{ result('get_userlist_to_disable_report_details') | is_falsy }}''',
            yes_task="stop_11",
            no_task="goto_run_report_task",
        )

        stop_11 = rail.FailOperator(
            task_id='stop_11',
            message='''***User List to disable*** report not available in Replicon'''
        )

        goto_run_report_task = rail.EmptyOperator(
            task_id = 'goto_run_report_task'
        )

        run_userlisttodisable_report = rail.run_report2(
            group_id='run_userlisttodisable_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_userlist_to_disable_report_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact'
        )

        if_first_error_present_15 = rail.IfOperator(
            task_id='if_first_error_present_15',
            test='''{{(result('run_userlisttodisable_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error | is_truthy}}''',
            yes_task="stop_16",
            no_task="if_first_payload_contains_nodata_17",
        )

        stop_16 = rail.FailOperator(
            task_id='stop_16',
            message='''{{ (result('run_userlisttodisable_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error }}'''
        )

        if_first_payload_contains_nodata_17 = rail.IfOperator(
            task_id='if_first_payload_contains_nodata_17',
            test="{{(result('run_userlisttodisable_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload | matches('No Data')}}",
            yes_task="log_to_sumo",
            no_task="load_csv_create_list_from_csv_21",
        )

        load_csv_create_list_from_csv_21 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_21",
            document="{{(result('run_userlisttodisable_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload}}",
        )

        create_collection_create_list_from_csv_21 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_21',
            source="{{ result('load_csv_create_list_from_csv_21') }}",
            name="enableduserdata",
            columns={
                'User Name': 'username',
                'Login Name': 'loginname',
                'User Status': 'status',
                'User End Date': 'enddate',
                'day diff': 'daydiff',
                'useruri': 'useruri',
                'today': 'today',
                'Country (Current)': 'country'
            }
        )

        create_list_of_timeofftermination_child = rail.SetVariableOperator(
            task_id='create_list_of_timeofftermination_child',
            name='termination_assignment_list',
            append=False,
            value=[]
        )

        query_list_getalltheuserswhoareenabledandhaveanenddateinpast_22 = rail.QueryCollectionOperator(
            task_id='query_list_getalltheuserswhoareenabledandhaveanenddateinpast_22',
            query="""SELECT * FROM  enableduserdata WHERE  enableduserdata.daydiff > 0 AND  enableduserdata.country = "United Kingdom" """,
        )

        if_first_username_blank_23 = rail.IfOperator(
            task_id='if_first_username_blank_23',
            test='''{{ result('query_list_getalltheuserswhoareenabledandhaveanenddateinpast_22','length') < 1 }}''',
            yes_task="log_to_sumo",
            no_task="foreach_enabled_user_with_past_enddate",
        )

        foreach_enabled_user_with_past_enddate = rail.ForEachOperator(
            task_id='foreach_enabled_user_with_past_enddate',
            items="{{ result('query_list_getalltheuserswhoareenabledandhaveanenddateinpast_22') }}",
            start_task='if_user_loginname_equal_actual_user_identity',
            end_task='foreach_enabled_user_with_past_enddate_end'
        )

        if_user_loginname_equal_actual_user_identity = rail.IfOperator(
            task_id='if_user_loginname_equal_actual_user_identity',
            test=lambda: rail.result('foreach_enabled_user_with_past_enddate')[
                'loginname'] != rail.result('get_my_actual_user_identity_6')['loginName'],
            yes_task="disable_login_28",
            no_task="foreach_enabled_user_with_past_enddate_end",
        )

        disable_login_28 = rail.RepliconServiceOperator(
            task_id='disable_login_28',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ result('foreach_enabled_user_with_past_enddate').useruri }}"
            }
        )

        if_error_in_disabling_user = rail.IfOperator(
            task_id='if_error_in_disabling_user',
            trigger_rule='all_done',
            test="{{get_task_state('disable_login_28') == 'failed' }}",
            yes_task='accumulate_list_items_30',
            no_task='trigger_child_timesheet_recalculation'
        )

        accumulate_list_items_30 = rail.SetVariableOperator(
            task_id='accumulate_list_items_30',
            name='error_while_disabling',
            append=True,
            value={
                "user": "{{ result('foreach_enabled_user_with_past_enddate').username }}",
                "loginname": "{{ result('foreach_enabled_user_with_past_enddate').loginname }}",
                "error": "{{get_error_message()}}"
            }
        )

        trigger_child_timesheet_recalculation = rail.TriggerDagRunOperator(
            task_id='trigger_child_timesheet_recalculation',
            retries=0,
            trigger_dag_id=f'michaelkorstna_uk_user_import_timesheet_recalculation_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": "{{ dag_run_ecid() }}",
                "userloginname": "{{ result('foreach_enabled_user_with_past_enddate').loginname }}",
                "useruri": "{{ result('foreach_enabled_user_with_past_enddate').useruri }}"
            }
        )

        foreach_enabled_user_with_past_enddate_end = rail.EmptyOperator(
            task_id='foreach_enabled_user_with_past_enddate_end',
        )

        if_first_user_present_36 = rail.IfOperator(
            task_id='if_first_user_present_36',
            test=lambda: (len(rail.get_dag_run_var('error_while_disabling')) > 0 if rail.result('accumulate_list_items_30') else False),
            yes_task="stop_37",
            no_task="log_to_sumo",
        )

        stop_37 = rail.FailOperator(
            task_id='stop_37',
            message='''Action to disable user failed for few users'''
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> log_today_4
        log_today_4 >> get_my_actual_user_identity_6
        get_my_actual_user_identity_6 >> get_all_time_off_types_7 >> get_userlist_to_disable_report_details >> if_log_get_user_listtodisablereport_9_blank_10
        if_log_get_user_listtodisablereport_9_blank_10 >> rail.Label(
            'Yes') >> stop_11 >> log_to_sumo
        if_log_get_user_listtodisablereport_9_blank_10 >> rail.Label(
            'No') >> goto_run_report_task >> run_userlisttodisable_report >> if_first_error_present_15
        if_first_error_present_15 >> rail.Label('Yes') >> stop_16 >> log_to_sumo
        if_first_error_present_15 >> rail.Label(
            'No') >> if_first_payload_contains_nodata_17
        if_first_payload_contains_nodata_17 >> rail.Label('Yes') >> log_to_sumo
        if_first_payload_contains_nodata_17 >> rail.Label(
            'No') >> load_csv_create_list_from_csv_21 >> create_collection_create_list_from_csv_21 >> create_list_of_timeofftermination_child
        create_list_of_timeofftermination_child >> query_list_getalltheuserswhoareenabledandhaveanenddateinpast_22 >> if_first_username_blank_23
        if_first_username_blank_23 >> rail.Label('Yes') >> log_to_sumo
        if_first_username_blank_23 >> rail.Label(
            'No') >> foreach_enabled_user_with_past_enddate
        foreach_enabled_user_with_past_enddate >> if_user_loginname_equal_actual_user_identity
        if_user_loginname_equal_actual_user_identity >> rail.Label(
            'Yes') >> disable_login_28 >> if_error_in_disabling_user
        if_error_in_disabling_user >> rail.Label(
            'Yes') >> accumulate_list_items_30 >> trigger_child_timesheet_recalculation
        if_error_in_disabling_user >> rail.Label(
            'No') >> trigger_child_timesheet_recalculation
        trigger_child_timesheet_recalculation >> foreach_enabled_user_with_past_enddate_end
        if_user_loginname_equal_actual_user_identity >> rail.Label(
            'No') >> foreach_enabled_user_with_past_enddate_end
        foreach_enabled_user_with_past_enddate >> foreach_enabled_user_with_past_enddate_end >> if_first_user_present_36
        if_first_user_present_36 >> rail.Label('Yes') >> stop_37 >> log_to_sumo
        if_first_user_present_36 >> rail.Label('No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
