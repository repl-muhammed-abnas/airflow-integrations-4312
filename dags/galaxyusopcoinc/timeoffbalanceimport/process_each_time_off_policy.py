from datetime import timedelta
from airflow.models import Variable
import rail
from galaxyusopcoinc.timeoffbalanceimport.utils import request_payload


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'vialtopartners_timeoffbalance_import_process_time_off_policy_child_{config.instance}',
        description='Vialto Partners Timeoff Balance Import Process Time Off Policy',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_policy,
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_reference_id_available'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout),
            start_task='is_reference_id_available',
            end_task='catch_and_log_errors',
        )

        is_reference_id_available = rail.IfOperator(
            task_id="is_reference_id_available",
            test= lambda dag_run: bool(dag_run.conf['timeoffuri']),
            yes_task="put_user_time_off_policy_set_schedule",
            no_task="log_reference_id_not_available"
        )

        log_reference_id_not_available = rail.WriteLogOperator(
            task_id='log_reference_id_not_available',
            log = '{{dag_run.conf.create_employee_log }}',
            severity='Exception',
            message='No Time off Type with this Reference ID in Replicon instance',
            properties={
                'batchid': '{{ dag_run.conf.batchid }}',
                'employeeid': '{{ dag_run.conf.employeeid }}',
                'referenceid': '{{ dag_run.conf.referenceid }}',
                'status': 'Exception',
            },
        )

        put_user_time_off_policy_set_schedule = rail.RepliconServiceOperator(
            task_id="put_user_time_off_policy_set_schedule",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda: request_payload.put_user_time_off_policy_set_schedule_payload(
                config.script_name, config.script_description),
        )

        log_successfull = rail.WriteLogOperator(
            task_id='log_successfull',
            log = '{{dag_run.conf.create_employee_log }}',
            severity='Success',
            message='Time off balance updated Successfully',
            properties={
                'batchid': '{{ dag_run.conf.batchid }}',
                'employeeid': '{{ dag_run.conf.employeeid }}',
                'referenceid': '{{ dag_run.conf.referenceid }}',
                'status': 'Success',
            },
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log = '{{dag_run.conf.create_employee_log }}',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'batchid': '{{ dag_run.conf.batchid }}',
                'employeeid': '{{ dag_run.conf.employeeid }}',
                'referenceid': '{{ dag_run.conf.referenceid }}',
                'status': 'Error',
            },
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> is_reference_id_available

        is_reference_id_available >> rail.Label('Yes') >> put_user_time_off_policy_set_schedule
        is_reference_id_available >> rail.Label('No') >> log_reference_id_not_available >> catch_and_log_errors
        put_user_time_off_policy_set_schedule >> log_successfull >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag_wbs)
