from datetime import timedelta
import json
from airflow.models import Variable
import rail

from crl.user_import_usa_v10.utils import response_filter

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_timeoff_type_assignment_vacation_new_user_dagid,
        description='CRL User Import USA- Process TIme Off Type Vacation',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_vacation_new_user,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_default_time_off_policy_schedule'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_default_time_off_policy_schedule',
            end_task='finish',
        )

        get_default_time_off_policy_schedule = rail.RepliconServiceOperator(
            task_id="get_default_time_off_policy_schedule",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ dag_run.conf.placeholder_timeoff_type_uri }}"
            },
            data_handler=response_filter.get_policy_to_assign_for_vacation_add
        )

        is_policy_present = rail.IfOperator(
            task_id='is_policy_present',
            test=lambda: bool(rail.result(
                'get_default_time_off_policy_schedule')),
            yes_task='put_user_timeoff_policy',
            no_task='finish'
        )

        put_user_timeoff_policy = rail.RepliconServiceOperator(
            task_id="put_user_timeoff_policy",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run:{
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['timeoff_type_uri']
                },
                "policySetScheduleEntries": json.loads(rail.result('get_default_time_off_policy_schedule'))
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_default_time_off_policy_schedule

        get_default_time_off_policy_schedule >> is_policy_present

        is_policy_present >> rail.Label("Yes") >> put_user_timeoff_policy >> finish
        is_policy_present >> rail.Label("No") >> finish


    return dag

rail.for_each_instance(create_child_dag)
