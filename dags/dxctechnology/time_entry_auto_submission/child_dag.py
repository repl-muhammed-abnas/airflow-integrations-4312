import rail
from dxctechnology.time_entry_auto_submission.utils import python_callable_method
from dxctechnology.time_entry_auto_submission.utils import request_payload


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_time_entry_submission_child_{config.instance}',
        description=f'DxcTechnology Time Entry Submission Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.project_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        Get_TimeEntry_Revision_Groups_For_User_And_DateRange = rail.RepliconServiceOperator(
            task_id='Get_TimeEntry_Revision_Groups_For_User_And_DateRange',
            endpoint='/services/TimeEntryRevisionGroupService1.svc/BulkGetTimeEntryRevisionGroupsForUserAndDateRange',
            data=request_payload.get_time_entry_revision_data
        )

        add_multi_day_entries_data = rail.PythonOperator(
            task_id='add_multi_day_entries_data',
            python_callable=python_callable_method.add_data
        )

        has_add_multi_day_entries_data = rail.IfOperator(
            task_id='has_add_multi_day_entries_data',
            test=lambda: bool(rail.result("add_multi_day_entries_data")),
            yes_task='entry_per_user',
            no_task='log_user_validation_error'
        )

        entry_per_user = rail.PythonOperator(
            task_id='entry_per_user',
            python_callable=python_callable_method.get_entries_per_user
        )

        has_entry_per_user = rail.IfOperator(
            task_id='has_entry_per_user',
            test='{{ result("entry_per_user").entries | is_truthy }}',
            yes_task='create_submit_batch',
            no_task='log_completion'
        )

        log_completion = rail.WriteLogOperator(
            task_id='log_completion',
            message='Number Of Time Entry Submitted - {{ result("entry_per_user").length }}',
            properties={
                'employeeid': '{{ dag_run.conf.Employeeid }}',
                'username': '{{ dag_run.conf.Username }}',
                'timesheetperiod': '{{ dag_run.conf.Period }}',
                'status': 'Success',
                'details': 'Number Of Time Entry Submitted - {{ result("entry_per_user").length }}',
                'ecid': '{{ dag_run_ecid() }}',
                'country': '{{ dag_run.conf.Country }}'
            }
        )

        create_submit_batch = rail.RepliconServiceOperator(
            task_id='create_submit_batch',
            endpoint='/services/TimeEntryRevisionGroupApprovalService1.svc/CreateSubmitBatch',
            data=request_payload.get_submit_batch_data
        )

        (process_timedata_batch, wait_for_timedata_batch) = rail.batch_execution(
            group_id='process_timedata_batch',
            creation_task_id=create_submit_batch.task_id
        )

        log_user_validation_error = rail.WriteLogOperator(
            task_id='log_user_validation_error',
            message='Time Entries Not Found For User {{ dag_run.conf.Username }} ,Time sheet Period{{ dag_run.conf.Period }}',
            properties={
                'employeeid': '{{ dag_run.conf.Employeeid }}',
                'username': '{{ dag_run.conf.Username }}',
                'timesheetperiod': '{{ dag_run.conf.Period }}',
                'status': 'Exception',
                'details': 'Time Entries Not Found For User {{ dag_run.conf.Username }} ,Time sheet Period{{ dag_run.conf.Period }}',
                'ecid': '{{ dag_run_ecid() }}',
                'country': '{{ dag_run.conf.Country }}'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'employeeid': '{{ dag_run.conf.Employeeid }}',
                'username': '{{ dag_run.conf.Username }}',
                'timesheetperiod': '{{ dag_run.conf.Period }}',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
                'ecid': '{{ dag_run_ecid() }}',
                'country': '{{ dag_run.conf.Country }}'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'TimeEntry_Count': '{{ result("entry_per_user").length  if result("entry_per_user")}}',
                'Status': "{{ 'Error' if result('catch_and_log_errors') else 'Success' }}",
                'Processed': "{{ 'No' if result('catch_and_log_errors') else 'Yes' }}",
                'Details': '{{ dag_run.conf.Period }} | {{ dag_run.conf.User }}'
            }
        )

        Get_TimeEntry_Revision_Groups_For_User_And_DateRange >> add_multi_day_entries_data >> \
            has_add_multi_day_entries_data >> rail.Label(
                "Yes") >> entry_per_user >> has_entry_per_user

        has_add_multi_day_entries_data >> rail.Label(
            "No") >> log_user_validation_error

        has_entry_per_user >> rail.Label(
            "Yes") >> create_submit_batch >> process_timedata_batch

        has_entry_per_user >> rail.Label(
            "No") >> log_completion

        wait_for_timedata_batch >> log_completion >> rail.Label(
            "On error") >> catch_and_log_errors

        log_user_validation_error >> rail.Label(
            "On error") >> catch_and_log_errors >> log_to_sumo
    return dag


rail.for_each_instance(create_dag)
