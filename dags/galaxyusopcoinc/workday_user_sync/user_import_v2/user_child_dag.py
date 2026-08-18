from datetime import timedelta
import rail
from galaxyusopcoinc.workday_user_sync.user_import_v2.utils import request_payload
from galaxyusopcoinc.workday_user_sync.user_import_v2.validate_field import validate_field, field_config_add, field_config_update
from galaxyusopcoinc.workday_user_sync.user_import_v2.tasks.search_user import get_search_user_task
from airflow.models import Variable


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.user_dag_id,
        description=f'VialtoPartners_User_Import Process User {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.user_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_user_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="create_user_log",
            end_task="catch_and_log_errors"
        )
        
        create_user_log = rail.CreateLogOperator(
            task_id="create_user_log"
        )

        search_user_start, search_user_end = get_search_user_task()

        has_user = rail.IfOperator(
            task_id='has_user',
            test="{{ result('get_user_uri') | is_truthy }}",
            yes_task='update_user_validate_fields',
            no_task='add_user_validate_fields'
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
            log="{{result('create_user_log')}}",
            message='User not processed due to following reason/s: {{ result("add_user_validate_fields") | map_to_attr("message") | join(", ") }}',
            severity='Exception',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.legalfirstname}} {{dag_run.conf.legallastname}}',
                'loginname': '{{dag_run.conf.workemail}}',
                'status': 'Exception',
                'action': 'Pre-Check',
                'message': 'User not processed due to following reason/s: {{ result("add_user_validate_fields") | map_to_attr("message") | join(", ") }}',
                "allowed_for_supervisor_dag": "False",
                "user_uri": "",
                "managerid": "{{dag_run.conf.managerid}}",
                "is_add_and_errored": "False"
            }
        )

        add_user = rail.TriggerDagRunForEachItemOperator(
            task_id='add_user',
            retries=0,
            items=lambda: [request_payload.get_conf()],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.user_add_dag_id,
            conf=lambda item: {
                **{
                    "action": "add"
                },
                **item,
                **{
                    'validationlog': rail.result('add_user_validate_fields'),
                    "work_week_uri": request_payload.get_work_week_uri('sunday'),
                    "create_user_log": rail.result("create_user_log")
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
            log="{{result('create_user_log')}}",
            message='User not updated due to following reason/s: {{ result("update_user_validate_fields") | map_to_attr("message") | join(", ") }}',
            severity='Exception',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.legalfirstname}} {{dag_run.conf.legallastname}}',
                'loginname': '{{dag_run.conf.workemail}}',
                'status': 'Exception',
                'action': 'Pre-Check',
                'message': 'User not updated due to following reason/s: {{ result("update_user_validate_fields") | map_to_attr("message") | join(", ") }}',
                "allowed_for_supervisor_dag": "False",
                "user_uri": "",
                "managerid": "{{dag_run.conf.managerid}}",
                "is_add_and_errored": "False"
            }
        )

        update_user = rail.TriggerDagRunForEachItemOperator(
            task_id='update_user',
            retries=0,
            items=lambda: [request_payload.get_conf()],
            trigger_dag_id=config.user_update_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **{
                    "action": "update"
                },
                **item,
                **{
                    'useruri': rail.result('get_user_uri'),
                    'validationlog': rail.result('update_user_validate_fields'),
                    "work_week_uri": request_payload.get_work_week_uri('sunday'),
                    "create_user_log": rail.result("create_user_log")
                }
            }
        )

        wait_for_update_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("update_user") }}',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{result('create_user_log')}}",
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.legalfirstname}} {{dag_run.conf.legallastname}}',
                'loginname': '{{dag_run.conf.workemail}}',
                'status': 'Error',
                'action': 'Pre-Check',
                'message': '{{ get_error_message() }}',
                "allowed_for_supervisor_dag": "False",
                "user_uri": "",
                "managerid": "{{dag_run.conf.managerid}}",
                "is_add_and_errored": "False"
            },
        )

        rail.run_report
        rail.run_report2
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> create_user_log

        create_user_log >> search_user_start
        search_user_end >> has_user

        has_user >> rail.Label(
            'Yes') >> update_user_validate_fields >> update_user_has_exception_logs
        update_user_has_exception_logs >> rail.Label(
            'no') >> update_user
        update_user_has_exception_logs >> rail.Label(
            'yes') >> update_user_log_exception_logs >> rail.Label("On error") >> catch_and_log_errors
        update_user >> wait_for_update_user >> rail.Label(
            "On error") >> catch_and_log_errors

        has_user >> rail.Label(
            'No') >> add_user_validate_fields >> add_user_has_exception_logs
        add_user_has_exception_logs >> rail.Label(
            'no') >> add_user
        add_user_has_exception_logs >> rail.Label(
            'yes') >> add_user_log_exception_logs >> rail.Label("On error") >> catch_and_log_errors
        add_user >> wait_for_add_user >> rail.Label(
            "On error") >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
