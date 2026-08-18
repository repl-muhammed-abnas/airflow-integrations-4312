
from datetime import timedelta, datetime, timezone
import urllib
import rail
from airflow.models import Variable
from arcticwolf.timeoff_import.utils import python_callable_methods


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'Arctic Wolf Master Timeoff Import {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.schedule_interval),
        max_active_runs=config.max_active_runs,
    ) as dag:


        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        log_starttime= rail.PythonOperator(
            task_id='log_starttime',
            python_callable=lambda: Variable.get(config.last_synced_endtime)
                                        if Variable.get(config.last_synced_endtime, default_var=False)
                                            else urllib.parse.quote((datetime.now(tz=timezone.utc)- timedelta(hours=6)).isoformat())
        )

        log_endtime = rail.PythonOperator(
            task_id='log_endtime',
            python_callable=lambda: urllib.parse.quote(datetime.now(tz=timezone.utc).isoformat())
        )

        get_csv_data_from_workday = rail.SimpleHttpOperator(
            task_id='get_csv_data_from_workday',
            method='GET',
            http_conn_id=config.workday_http_conn_id,
            # pylint: disable=line-too-long
            endpoint='/RPT_-_INT_-_S2_Time_Off_for_Replicon?datetimeStart={{dag_run.conf.datetimestart if dag_run.conf.get("datetimestart") else result("log_starttime")}}&datetimeEnd={{dag_run.conf.datetimeend if dag_run.conf.get("datetimeend") else result("log_endtime")}}&format=csv',
            headers={
                "Content-Type": "application/json"
            },
            extra_options={
                'verify': False
            }
        )

        set_synced_endtime_var = rail.PythonOperator(
            task_id='set_synced_endtime_var',
            python_callable=lambda dag_run: Variable.set(config.last_synced_endtime, value=rail.result("log_endtime"))
                                                if not dag_run.conf.get('csvdata') else None
        )

        load_csv = rail.LoadCSVFileOperator(
            task_id="load_csv",
            document="{{ dag_run.conf.csvdata if 'csvdata' in dag_run.conf else result('get_csv_data_from_workday') }}",
        )

        create_collection_from_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_csv',
            source="{{ result('load_csv') }}",
            name="input",
            columns={
                'idEmployee': 'employeeid',
                'catProcess': 'timeoffaction',
                'catTimeoff': 'timeofftype',
                'dateTimeoff': 'timeoffdate',
                'amount': 'amount',
                'unit': 'unit',
            }
        )

        if_csv_row_count_less_than_1 = rail.IfOperator(
            task_id='if_csv_row_count_less_than_1',
            test='''{{ result('create_collection_from_csv', 'length') < 1 }}''',
            yes_task="send_blank_payload_mail",
            no_task="create_timeoff_import_logs"
        )
        send_blank_payload_mail = rail.EmailOperator(
            task_id='send_blank_payload_mail',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Replicon timeoff import skipped -{{ current_time() }} ''',
            html_content="templates/emails/blank_payload_email.html"
        )

        create_timeoff_import_logs = rail.CreateLogOperator(
            task_id='create_timeoff_import_logs'
        )

        query_list_missingmandatoryvalues_ignored = rail.QueryCollectionOperator(
            task_id='query_list_missingmandatoryvalues_ignored',
            query="""SELECT * FROM input WHERE NULLIF(employeeid,'') IS NULL OR NULLIF(timeofftype,'') IS NULL
                    OR NULLIF(timeoffaction,'') IS NULL OR NULLIF(timeoffdate,'') IS NULL OR NULLIF(amount,'') IS NULL
                    OR NULLIF(unit,'') IS NULL"""
        )

        insert_missingmandatoryvalues_to_log = rail.WriteLogOperator(
            task_id='insert_missingmandatoryvalues_to_log',
            log="{{ result('create_timeoff_import_logs') }}",
            items="{{ result('query_list_missingmandatoryvalues_ignored') }}",
            message="One or more mandatory field is missing.",
            severity="Info",
            properties=lambda item:{
                "employeeid": item["employeeid"],
                "timeoffaction": item["timeoffaction"],
                "timeofftype": item["timeofftype"],
                "timeoffdate": item["timeoffdate"],
                "amount": item["amount"],
                "unit": item["unit"],
                "status": "Ignored",
                "details": python_callable_methods.get_missing_field_message(item),
            }
        )

        query_list_recordswithmandatoryvalues = rail.QueryCollectionOperator(
            task_id='query_list_recordswithmandatoryvalues',
            query="""SELECT * FROM input WHERE NULLIF(employeeid,'') IS NOT NULL AND NULLIF(timeofftype,'') IS NOT NULL
                    AND NULLIF(timeoffaction,'') IS NOT NULL AND NULLIF(timeoffdate,'') IS NOT NULL AND NULLIF(amount,'') IS NOT NULL
                    AND NULLIF(unit,'') IS NOT NULL"""
        )

        if_has_valid_records = rail.IfOperator(
            task_id='if_has_valid_records',
            test='''{{ result('query_list_recordswithmandatoryvalues', 'length') > 0 }}''',
            yes_task="get_enabled_time_off_types",
            no_task="format_logs",
        )

        get_enabled_time_off_types = rail.RepliconServiceOperator(
            task_id='get_enabled_time_off_types',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
        )

        trigger_dag_run_process_timeoff_records_async = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_process_timeoff_records_async',
            retries=0,
            items="{{ result('query_list_recordswithmandatoryvalues') }}",
            trigger_dag_id=config.process_timeoff_records_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "employeeid":  item['employeeid'],
                "timeoffuri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_time_off_types'), 'name', item['timeofftype'], 'uri'),
                "timeoffaction": item['timeoffaction'],
                "amount": item['amount'],
                "unit": item['unit'],
                "timeoffdate": item['timeoffdate'],
                "timeofftype": item['timeofftype'],
            }
        )

        wait_for_completion_dag_run_trigger_dag_run_process_timeoff_records_async = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_dag_run_trigger_dag_run_process_timeoff_records_async',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_process_timeoff_records_async") }}'
        )

        gather_timeoff_import_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_timeoff_import_child_logs',
            dag_runs="{{ result('trigger_dag_run_process_timeoff_records_async') }}",
            dagrun_task_id='create_timeoff_import_child_logs',
            flatten=True
        )


        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=python_callable_methods.do_format_logs
        )

        create_csv_lines = rail.WriteCSVFileOperator(
            task_id='create_csv_lines',
            source="{{ result('format_logs') | to_json }}",
            header=['timeoffaction',
                    'employeeid',
                    'timeofftype',
                    'timeoffdate',
                    'amount',
                    'unit',
                    'status',
                    'details',
                    'jobid'],
            row=[
                "{{ item.timeoffaction }}",
                "{{ item.employeeid }}",
                "{{ item.timeofftype }}",
                "{{ item.timeoffdate }}",
                "{{ item.amount }}",
                "{{ item.unit }}",
                "{{ item.status }}",
                "{{ item.details }}",
                "{{ item.jobid }}"
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_download_link",
            artifact_name='{{result("create_csv_lines")}}',
            output_file_name='timeoff_import_logs_{{ dag_run_ecid() | replace(":", "-") }}.csv',
            expires_in_seconds=config.log_file_download_link_expiry_in_sec
        )

        if_log_has_errors = rail.IfOperator(
            task_id='if_log_has_errors',
            test='''{{ result("format_logs", key="error_record_count") > 0 }}''',
            yes_task="send_mail_failed",
            no_task="send_mail_success",
        )

        send_mail_failed = rail.EmailOperator(
            task_id='send_mail_failed',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='''{{ get_company_key() }} | Replicon timeoff import completed with errors -{{ current_time() }} ''',
            html_content="templates/emails/failed_records_email.html",
            params={'log_file_path': config.log_filepath}
        )

        send_mail_success = rail.EmailOperator(
            task_id='send_mail_success',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Replicon timeoff import completed successfully -{{ current_time() }} ''',
            html_content="templates/emails/successful_email.html",
            params={'log_file_path': config.log_filepath}
        )


        finish = rail.EmptyOperator(
            task_id='finish'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        log_starttime >> log_endtime >> get_csv_data_from_workday >> set_synced_endtime_var >> load_csv \
        >> create_collection_from_csv >> if_csv_row_count_less_than_1
        if_csv_row_count_less_than_1 >> rail.Label('Yes') >> send_blank_payload_mail >> finish
        if_csv_row_count_less_than_1 >> rail.Label('No') >> create_timeoff_import_logs >> query_list_missingmandatoryvalues_ignored \
        >> insert_missingmandatoryvalues_to_log >> query_list_recordswithmandatoryvalues >> if_has_valid_records
        if_has_valid_records >> rail.Label('Yes') >> get_enabled_time_off_types >> trigger_dag_run_process_timeoff_records_async \
        >> wait_for_completion_dag_run_trigger_dag_run_process_timeoff_records_async \
        >> gather_timeoff_import_child_logs >> format_logs >> create_csv_lines >> generate_download_link >> if_log_has_errors
        if_log_has_errors >> rail.Label('Yes') >> send_mail_failed >> finish
        if_log_has_errors >> rail.Label('No') >> send_mail_success >> finish
        if_has_valid_records >> rail.Label('No') >> format_logs

        finish >> log_to_sumo >> can_fail_dag >> fail_dagrun

    return dag

rail.for_each_instance(create_dag)
