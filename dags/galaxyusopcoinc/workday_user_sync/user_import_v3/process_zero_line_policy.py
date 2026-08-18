from airflow.models import Variable
import rail
from galaxyusopcoinc.workday_user_sync.user_import_v3.utils import request_payload
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.add_zero_line_policy_dag_id,
        description=f'VialtoPartners_User_Import_location add V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_run_zero_line_policy_child,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_put_timeoff_zero_line_policy_payload'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="get_put_timeoff_zero_line_policy_payload",
            end_task="catch_and_log_errors"
        )

        get_put_timeoff_zero_line_policy_payload = rail.PythonOperator(
            task_id='get_put_timeoff_zero_line_policy_payload',
            python_callable=request_payload.get_put_timeoff_zero_line_policy_payload
        )

        put_zero_line_policy = rail.RepliconServiceOperator(
            task_id="put_zero_line_policy",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data="{{ result('get_put_timeoff_zero_line_policy_payload') | to_json }}"
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{dag_run.conf.create_user_log}}",
            message='{{ get_error_message() }}',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'username':  '{{dag_run.conf.username}}',
                'loginname': '{{dag_run.conf.workemail}}',
                'status': 'Error',
                'action': 'Update',
                'message': '{{ get_error_message() }}',
                "allowed_for_supervisor_dag": "False",
                "user_uri": "{{dag_run.conf.user_uri}}",
                "managerid": "{{dag_run.conf.managerid}}",
                "is_add_and_errored": "False"

            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_put_timeoff_zero_line_policy_payload

        get_put_timeoff_zero_line_policy_payload >> put_zero_line_policy >> rail.Label(
            "On Error") >> catch_and_log_errors

    return dag


rail.for_each_instance(create_dag)
