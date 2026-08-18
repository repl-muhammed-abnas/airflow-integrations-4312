from datetime import timedelta,datetime
import rail
def create_airflow_master(config):
    with rail.create_airflow_dag(
        dag_id=config.user_import_fte_adhoc_run_master,
        description="pwcglobal adhoc fte initial update",
        start_date=datetime(2024,2,13),
        schedule_interval=config.schedule_interval,
        company_key=config.company_key,
        max_active_runs=config.max_active_runs_master,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            "sftp_conn_id":config.sftp_conn_id
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id="new_file_sensor",
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        was_new_file_found = rail.IfOperator(
            task_id="was_new_file_found",
            trigger_rule="all_done",
            test='{{get_task_state("new_file_sensor") == "success"}}',
            yes_task="archive_file",
            no_task="delete_dagrun"
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id="archive_file",
            new_filename=config.archive_filepath + '{{ dag_run_ecid()|replace(":","_")}}' + "{{result('new_file_sensor') | file_name }}",
            existing_filename='{{result("new_file_sensor")}}'
        )

        delete_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id="delete_dagrun"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id="download_file",
            remote_filepath='{{result("new_file_sensor")}}'
        )

        create_adhoc_pwcglobal_user_import_log = rail.CreateLogOperator(
            task_id="create_adhoc_pwcglobal_user_import_log",
        )

        load_user_records_csv = rail.LoadCSVFileOperator(
            task_id="load_user_records_csv",
            document='{{result("download_file")}}',
            headers=["user_login_name","user", "user_uri",  "user_start_date","FTE", "FTE_percent_effective_date" ]
        )

        create_user_records_collection = rail.CreateCollectionOperator(
            task_id="create_user_records_collection",
            source='{{result("load_user_records_csv")}}',
            name="user_records"
        )

        query_user_records_with_uri = rail.QueryCollectionOperator(
            task_id="query_user_records_with_uri",
            query="""SELECT * FROM user_records WHERE NULLIF("user_uri", "") IS NOT NULL"""
        )

        if_user_records = rail.IfOperator(
            task_id="if_user_records",
            test='{{result("query_user_records_with_uri","length") > 0}}',
            yes_task="get_all_custom_fields",
            no_task="log_to_sumo"
        )

        get_all_custom_fields = rail.RepliconServiceOperator(
            task_id="get_all_custom_fields",
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={'objectUri': 'urn:replicon:object-type:user'},
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                                            response, 'displayText', 'FTE Percent', 'uri'),
        )

        update_each_user = rail.trigger_parallel_dagrun(
            task_id="update_each_user",
            trigger_dag_id=f"pwcglobal_user_import_adhoc_fte_initial_update_child_{config.instance}",
            execution_timeout=timedelta(days=14),
            items=lambda:rail.result("query_user_records_with_uri"),
            parallel_count=config.trigger_parallel_run_update_each_user_count,
            conf= lambda item:{
                **item,
                "lookuptable": rail.result("create_adhoc_pwcglobal_user_import_log"),
                "customFieldUri":rail.result("get_all_custom_fields"),
                "ecid":rail.render_template('{{dag_run_ecid()}}')
            }
        )

        write_csv_logs = rail.WriteCSVFileOperator(
            task_id="write_csv_logs",
            source='{{result("create_adhoc_pwcglobal_user_import_log")}}',
            header=["User login name", "User uri", "FTE", "FTE_percent_effective_date", "Details" ,"Jobid"],
            row=[
                '{{item.properties | attr_or_default("user","")}}',
                '{{item.properties | attr_or_default("user_uri","")}}',
                '{{item.properties | attr_or_default("FTE","")}}',
                '{{item.properties | attr_or_default("FTE_percent_effective_date","")}}',
                '{{item.properties | attr_or_default("details","")}}',
                '{{item.properties | attr_or_default("jobid","")}}',
            ]
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger",
            trigger_rule="all_done"
        )

        can_faildag = rail.IfOperator(
            task_id="can_faildag",
            test='{{get_error_message()|is_truthy}}',
            yes_task="fail_dagrun"
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{get_error_message()}}'
        )

        new_file_sensor >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_dagrun
        new_file_sensor >> download_file >>\
        create_adhoc_pwcglobal_user_import_log >>\
        load_user_records_csv >>\
        create_user_records_collection >> query_user_records_with_uri>>\
        if_user_records >> rail.Label("No") >> log_to_sumo
        if_user_records >> rail.Label("Yes") >> get_all_custom_fields>>\
        update_each_user >>\
        write_csv_logs >>\
        log_to_sumo >> can_faildag >> fail_dagrun

        return dag

rail.for_each_instance(create_airflow_master)
