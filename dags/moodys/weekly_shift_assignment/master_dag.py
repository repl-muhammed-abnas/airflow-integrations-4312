
from datetime import timedelta, datetime
import pendulum
import rail
from moodys.weekly_shift_assignment.task.generate_report_batch import report_batch
from moodys.weekly_shift_assignment.utils import custom_methods
from moodys.weekly_shift_assignment.utils import request_payload

null = None


def create_moodys_weekly_shift_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'moodysemea_weeklyshiftassignment_master_{config.instance}',
        description=f'Moodysemea_Weekly shift assignment_master V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum.datetime(2023, 1, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        weekly_shift_log_csv_file_name = rail.PythonOperator(
            task_id='weekly_shift_log_csv_file_name',
            python_callable=lambda: datetime.now().strftime("%Y%m%dT%H%M%S") +
            '_moodys_weeklyshiftupdate.csv'
        )

        create_weekly_shift_log = rail.CreateLogOperator(
            task_id='create_weekly_shift_log'
        )

        generate_report = rail.EmptyOperator(task_id='generate_report')

        get_report_details, get_users_to_be_processed, get_users_to_be_ignored, fail_no_report_data = report_batch(
            config)

        log_ignored_users = rail.WriteLogOperator(
            task_id='log_ignored_users',
            log='{{ result("create_weekly_shift_log") }}',
            items='{{ result("get_users_to_be_ignored") }}',
            message='Shift schedule was not populated. User either is assigned to office schedule or "Regular/Shift User" is null.',
            properties={
                'parentjobid': '{{ ecid() }}',
                'childjobid': 'NA',
                'loginname': '{{ item.loginname }}',
                'shiftname': '{{ item.regularshiftuserudf }}',
                'status': 'Exception',
                'details': 'Shift schedule was not populated. User either is assigned to office schedule or "Regular/Shift User" is null.'
            }
        )

        process_weekly_shift_assignment_per_existing_user = rail.TriggerDagRunForEachItemOperator(
            task_id='process_weekly_shift_assignment_per_existing_user',
            retries=0,
            items=lambda: custom_methods.get_data_from_document(
                rail.result('get_users_to_be_processed')),
            trigger_dag_id=f'moodysemea_weeklyshiftassignment_perexistinguser_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=request_payload.get_request_conf
        )

        wait_for_process_weekly_shift_assignment_per_existing_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_weekly_shift_assignment_per_existing_user',
            dag_runs='{{ result("process_weekly_shift_assignment_per_existing_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_logs',
            dag_runs='{{ result("process_weekly_shift_assignment_per_existing_user") }}',
            dagrun_task_id='create_weekly_shift_per_user_log',
            flatten=True
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'moodysemea_weeklyshiftassignment_loggeneration_{config.instance}',
            conf=lambda: {
                'child_log': rail.result('gather_logs'),
                'log_filename': rail.result('weekly_shift_log_csv_file_name'),
                'weekly_shift_log': rail.result('create_weekly_shift_log')
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("create_weekly_shift_log") }}',
            trigger_rule='one_failed',
            severity='Error',
            message=config.error_template,
            properties={
                'parentjobid': '{{ ecid() }}',
                'childjobid': 'NA',
                'loginname': '',
                'shiftname': '',
                'status': 'Exception',
                'details': {config.error_template}
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'number_of_users': '{{ result("create_report_collection","length")}}',
                'users_processed': '{{ result("get_users_to_be_processed","length")}}'
            }
        )

        weekly_shift_log_csv_file_name >> create_weekly_shift_log >> generate_report >> get_report_details
        get_users_to_be_processed >> get_users_to_be_ignored >> log_ignored_users \
            >> process_weekly_shift_assignment_per_existing_user >> wait_for_process_weekly_shift_assignment_per_existing_user \
            >> gather_logs >> process_log_generation >> finish

        fail_no_report_data >> finish
        finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_moodys_weekly_shift_dag)
