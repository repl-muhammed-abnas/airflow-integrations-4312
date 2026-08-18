from datetime import timedelta
from airflow.models import Variable
import rail
from darkmattertechnologiesllc.user_sync_v1.utils import request_payload, python_callable
from darkmattertechnologiesllc.user_sync_v1.task.supervisor_assignment import supervisor_assignment

def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.assign_timeoff_newuser_child_dagid,
        description=config.assign_timeoff_newuser_child_dagid,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.assign_timeoff_newuser_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_enabled_timeoff_list'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_enabled_timeoff_list',
            end_task='empty_process_timeoff',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_enabled_timeoff_list = rail.RepliconServiceOperator(
            task_id='get_enabled_timeoff_list',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data_handler=lambda response : list(map(lambda x:x['uri'], response))
        )

        assign_req_timeofftypes = rail.RepliconServiceOperator(
            task_id='assign_req_timeofftypes',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'timeOffTypeUris': rail.result('get_enabled_timeoff_list')
            }
        )

        for_each_timeoff = rail.ForEachOperator(
            task_id = "for_each_timeoff",
            items=lambda: rail.result("get_enabled_timeoff_list"),
            start_task="get_default_timeoff_policy",
            end_task="empty_process_timeoff"
        )


        get_default_timeoff_policy = rail.RepliconServiceOperator(
            task_id = "get_default_timeoff_policy",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run: {"timeOffAccount":{
                "userUri" : dag_run.conf['useruri'],
                "timeOffTypeUri": rail.result("for_each_timeoff")
            }}
        )

        if_default_policy_present = rail.IfOperator(
            task_id='if_default_policy_present',
            test='''{{ result('get_default_timeoff_policy') | is_truthy }}''',
            yes_task="update_timeoff_policies",
            no_task="empty_process_timeoff",
        )

        update_timeoff_policies = rail.RepliconServiceOperator(
            task_id = "update_timeoff_policies",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=request_payload.get_update_timeoff_policies_payload
        )

        empty_process_timeoff = rail.EmptyOperator(
            task_id = "empty_process_timeoff"
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> empty_process_timeoff
        can_run_batch_task >> rail.Label('No') >> get_enabled_timeoff_list

        get_enabled_timeoff_list >> assign_req_timeofftypes >> for_each_timeoff

        for_each_timeoff >> rail.Label('Yes') >> get_default_timeoff_policy >> if_default_policy_present
        for_each_timeoff >> rail.Label('No') >> empty_process_timeoff
        
        if_default_policy_present >> rail.Label('Yes') >> update_timeoff_policies >> empty_process_timeoff
        if_default_policy_present >> rail.Label('No') >> empty_process_timeoff
        


    return dag

rail.for_each_instance(create_dag)
