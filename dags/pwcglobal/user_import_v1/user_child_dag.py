from datetime import timedelta
import rail
from pwcglobal.user_import_v1 import request_payload
from pwcglobal.user_import_v1.validate_field import validate_field, field_config_add, field_config_update
from pwcglobal.user_import_v1.task.search_user import get_search_user_task

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/user_import/config.py


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pwcglobal_user_import_user_child_{config.instance}_v1',
        description=f'PwCGlobal_User_Import Process User {config.instance} V1',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.user_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_log',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        search_user = get_search_user_task()

        has_user = rail.IfOperator(
            task_id='has_user',
            test="{{ result('get_user_uri') | is_truthy }}",
            yes_task='update_user_validate_fields',
            no_task='has_valid_status'
        )

        has_valid_status = rail.IfOperator(
            task_id='has_valid_status',
            test="{{ dag_run.conf.isloginenabled == 'Yes' }}",
            yes_task='add_user_validate_fields',
            no_task='log_invalid_status'
        )

        log_invalid_status = rail.WriteLogOperator(
            task_id='log_invalid_status',
            log="{{ result('create_log') }}",
            message='User not created since login status received {{ dag_run.conf.isloginenabled }}',
            severity='Exception',
            properties={
                'userpartyid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.firstname}} {{dag_run.conf.lastname}}',
                'legalentityid': '{{dag_run.conf.legalentity}}',
                'message': 'User not created since login status received {{ dag_run.conf.isloginenabled }}',
                'status': 'Exception',
            }
        )

        add_user_validate_fields = rail.PythonOperator(
            task_id='add_user_validate_fields',
            python_callable=lambda: validate_field(field_config_add)
        )

        add_user_has_exception_logs = rail.IfOperator(
            task_id='add_user_has_exception_logs',
            test=lambda: len(list(filter(
                lambda x: x['log_type'] == 'Exception', rail.result('add_user_validate_fields')))) > 0,
            no_task='add_user',
            yes_task='add_user_log_exception_logs',
        )

        add_user_log_exception_logs = rail.WriteLogOperator(
            task_id='add_user_log_exception_logs',
            log="{{ result('create_log') }}",
            message='User not processed due to following reason/s: {{ result("add_user_validate_fields") | map_to_attr("message") | join(", ") }}',
            severity='Exception',
            properties={
                'userpartyid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.firstname}} {{dag_run.conf.lastname}}',
                'legalentityid': '{{dag_run.conf.legalentity}}',
                'message': 'User not processed due to following reason/s: {{ result("add_user_validate_fields") | map_to_attr("message") | join(", ") }}',
                'status': 'Exception',
            }
        )

        add_user = rail.TriggerDagRunForEachItemOperator(
            task_id='add_user',
            retries=0,
            items=lambda: [request_payload.get_conf()],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'{config.user_add_dag_id}_{config.instance}_v1',
            conf=lambda item: {
                **item, **{
                    'validationlog': rail.result('add_user_validate_fields'),
                    'loginnameupdated': rail.result('get_user_uri', 'loginnameupdated'),
                    'log': rail.result('create_log'),
                }
            }
        )

        wait_for_add_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_add_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("add_user") }}',
        )

        update_user_validate_fields = rail.PythonOperator(
            task_id='update_user_validate_fields',
            python_callable=lambda: validate_field(field_config_update)
        )

        update_user_has_exception_logs = rail.IfOperator(
            task_id='update_user_has_exception_logs',
            test=lambda: len(list(filter(
                lambda x: x['log_type'] == 'Exception', rail.result('update_user_validate_fields')))) > 0,
            no_task='update_user',
            yes_task='update_user_log_exception_logs',
        )

        update_user_log_exception_logs = rail.WriteLogOperator(
            task_id='update_user_log_exception_logs',
            log="{{ result('create_log') }}",
            message='User not updated due to following reason/s: {{ result("update_user_validate_fields") | map_to_attr("message") | join(", ") }}',
            severity='Exception',
            properties={
                'userpartyid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.firstname}} {{dag_run.conf.lastname}}',
                'legalentityid': '{{dag_run.conf.legalentity}}',
                'message': 'User not updated due to following reason/s: {{ result("update_user_validate_fields") | map_to_attr("message") | join(", ") }}',
                'status': 'Exception',
            }
        )

        update_user = rail.TriggerDagRunForEachItemOperator(
            task_id='update_user',
            retries=0,
            items=lambda: [request_payload.get_conf()],
            trigger_dag_id=f'{config.user_update_dag_id}_{config.instance}_v1',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {**item, **{
                'useruri': rail.result('get_user_uri'),
                'validationlog': rail.result('update_user_validate_fields'),
                'loginnameupdated': rail.result('get_user_uri', 'loginnameupdated'),
                'log': rail.result('create_log'),
            }
            }
        )

        wait_for_update_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("update_user") }}',
        )

        get_supervisor_assignment = rail.GatherResultsFromDagRunsOperator(
            task_id='get_supervisor_assignment',
            dag_runs="{{ result('add_user') or result('update_user') }}",
            dagrun_task_id='queue_supervisor_assignment',
            flatten=True,
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ result('create_log') }}",
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'userpartyid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.firstname}} {{dag_run.conf.lastname}}',
                'legalentityid': '{{dag_run.conf.legalentity}}',
                'status': 'Error',
                'message': '{{ get_error_message() }}',

            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        batch_task >> create_log
        batch_task >> catch_and_log_errors
        create_log >> search_user >> has_user

        has_user >> rail.Label(
            'Yes') >> update_user_validate_fields >> update_user_has_exception_logs
        update_user_has_exception_logs >> rail.Label('no') >> update_user
        update_user_has_exception_logs >> rail.Label(
            'yes') >> update_user_log_exception_logs >> catch_and_log_errors
        update_user >> wait_for_update_user >> get_supervisor_assignment

        has_user >> rail.Label(
            'No') >> has_valid_status
        has_valid_status >> rail.Label(
            'Yes') >> add_user_validate_fields >> add_user_has_exception_logs
        add_user_has_exception_logs >> rail.Label('no') >> add_user
        add_user_has_exception_logs >> rail.Label(
            'yes') >> add_user_log_exception_logs >> catch_and_log_errors
        add_user >> wait_for_add_user >> get_supervisor_assignment
        has_valid_status >> rail.Label(
            'No') >> log_invalid_status >> catch_and_log_errors

        get_supervisor_assignment >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
