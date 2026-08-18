import itertools
import rail
from datetime import timedelta
from airflow.models import Variable
from tsystems.timesheet_guessing_hours_update.utils.request_payload import report_config

null = None

def create_guessing_hours_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_orgstructure,
        description=f"T-Systems-Guessing Hours | {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='check_org_uri'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task='check_org_uri',
            end_task='batch_task_end',
        )

        check_org_uri = rail.IfOperator(
            task_id='check_org_uri',
            test='{{ dag_run.conf.input_data.org_uri | is_truthy }}',
            yes_task='get_report_details',
            no_task='log_org_uri_missing'
        )

        log_org_uri_missing = rail.WriteLogOperator(
            task_id='log_org_uri_missing',
            log='{{ dag_run.conf.org_log }}',
            severity='Exception',
            message='Org Structure not present in Replicon for code: {{ dag_run.conf.input_data.org_code }}',
            properties={
                'employee_id': '',
                'user_name': '',
                'entry_date': '',
                'original_hours': '',
                'task_name': '',
                'project_name': '',
                'org_structure_code': '{{ dag_run.conf.input_data.org_code }}',
                'status': 'Exception',
                'action': 'Validation',
                'details': 'Org Structure not present in Replicon for code: {{ dag_run.conf.input_data.org_code }}'
            }
        )
        
        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name
        )
            
        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params=report_config,
            target='artifact',
        )

        is_report_failed = rail.IfOperator(
            task_id='is_report_failed',
            test="{{ (result('run_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error | is_truthy }}",
            yes_task='fail_report_generation',
            no_task='report_has_data'
        )

        fail_report_generation = rail.FailOperator(
            task_id='fail_report_generation',
            message="{{ (result('run_report.get_report_result') | load_json_artifact).reportGenerationResults[0].error }}"
        )

        report_has_data = rail.IfOperator(
            task_id='report_has_data',
            test="{{result('run_report.get_report_result','has_data')}}",
            yes_task='is_report_has_expected_columns'
        )

        is_report_has_expected_columns = rail.IfOperator(
            task_id='is_report_has_expected_columns',
            test="{{ (result('run_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload | \
                starts_with('%s') }}" % config.expected_report_columns,
            yes_task='process_report_data',
            no_task='fail_no_expected_columns',
        )

        fail_no_expected_columns = rail.FailOperator(
            task_id='fail_no_expected_columns',
            message='Base report column order does not match expected.'
        )

        process_report_data = rail.EmptyOperator(task_id='process_report_data')

        load_csv = rail.LoadCSVFileOperator(
            task_id='load_csv',
            document="{{ (result('run_report.get_report_result') | load_json_artifact).reportGenerationResults[0].payload }}",
            delimiter=config.csv_separator
        )

        create_userstimeentries_collection = rail.CreateCollectionOperator(
            task_id='create_userstimeentries_collection',
            source='{{ result("load_csv") }}',
            name='userstimeentries',
            columns={
                'Employee ID': 'employee_id',
                'User Name': 'user_name',
                'User URI': 'user_uri',
                'Entry Date': 'entry_date',
                'Hours': 'hours',
                'Project Name': 'project_name',
                'Task Name': 'task_name',
                'Task URI': 'task_uri',
                'Org Structure Code (Current)': 'org_structure_code',
                'Org Structure (Current) (Full Path)': 'org_structure_full_path',
                'Timesheet Start Date': 'timesheet_start_date',
                'Entry ID': 'entry_id',
            }
        )

        filter_valid_guessing_hours = rail.QueryCollectionOperator(
            task_id='filter_valid_guessing_hours',
            query="SELECT * FROM userstimeentries WHERE LOWER(task_name) = 'guessing hours' AND hours NOT IN ('0.00', '0', '0,00', 0.00, 0)",
            name='validguessinghours'
        )

        unique_user_uris = rail.QueryCollectionOperator(
            task_id='unique_user_uris',
            query="SELECT DISTINCT user_uri, employee_id, user_name FROM validguessinghours",
            name='unique_user_uris'
        )

        batch_task_end = rail.EmptyOperator(task_id='batch_task_end')

        check_valid_guessing_hours = rail.IfOperator(
            task_id='check_valid_guessing_hours',
            test='{{ result("filter_valid_guessing_hours", "length") > 0 }}',
            yes_task='process_valid_guessing_hours',
            no_task='catch_and_log_errors'
        )

        process_valid_guessing_hours = rail.EmptyOperator(task_id='process_valid_guessing_hours')

        trigger_process_users = rail.trigger_parallel_dagrun(
            task_id='trigger_process_users',
            items='{{ result("unique_user_uris") }}',
            trigger_dag_id=config.process_users,
            parallel_count=config.trigger_process_users_parallel_dagrun_count,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                "input_data": {
                    "employee_id": item["employee_id"],
                    "user_name": item["user_name"],
                    "user_uri": item["user_uri"],
                    "timesheet_start_date": dag_run.conf["input_data"]["timesheet_start_date"],
                    "timesheet_end_date": dag_run.conf["input_data"]["timesheet_end_date"],
                }
            }
        )

        get_process_users_dag_ids = rail.PythonOperator(
            task_id='get_process_users_dag_ids',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: (rail.result(
                    f'trigger_process_users_{x+1}') if rail.result(
                    f'trigger_process_users_{x+1}') else []), range(config.trigger_process_users_parallel_dagrun_count))))),
            show_return_value_in_logs=False
        )

        gather_process_users_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_process_users_logs',
            dag_runs='{{ result("get_process_users_dag_ids") }}',
            dagrun_task_id='create_process_user_log',
            flatten=True
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.org_log }}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'employee_id': '',
                'user_name': '',
                'entry_date': '',
                'original_hours': '',
                'task_name': '',
                'project_name': '',
                'org_structure_code': '{{ dag_run.conf.input_data.org_code }}',
                'status': 'Error',
                'action': 'Validation',
                'details': '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> batch_task_end
        can_run_batch_task >> rail.Label('No') >> check_org_uri

        check_org_uri >> rail.Label('Yes') >> get_report_details >> run_report_group_entry
        check_org_uri >> rail.Label('No') >> log_org_uri_missing >> catch_and_log_errors
        run_report_group_exit >> is_report_failed
        is_report_failed >> rail.Label('No') >> report_has_data
        is_report_failed >> rail.Label('Yes') >> fail_report_generation
        report_has_data >> rail.Label('Yes') >> is_report_has_expected_columns
        is_report_has_expected_columns >> rail.Label('Yes') >> process_report_data
        is_report_has_expected_columns >> rail.Label('No') >> fail_no_expected_columns
        process_report_data >> load_csv >> create_userstimeentries_collection
        create_userstimeentries_collection >> filter_valid_guessing_hours >> unique_user_uris \
            >> batch_task_end >> check_valid_guessing_hours

        check_valid_guessing_hours >> rail.Label('Yes') >> process_valid_guessing_hours >> trigger_process_users
        check_valid_guessing_hours >> rail.Label('No') >> catch_and_log_errors

        trigger_process_users >> get_process_users_dag_ids >> gather_process_users_logs >> catch_and_log_errors

    return dag

rail.for_each_instance(create_guessing_hours_dag)
