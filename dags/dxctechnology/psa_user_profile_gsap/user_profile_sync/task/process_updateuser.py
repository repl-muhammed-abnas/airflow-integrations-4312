from datetime import timedelta
import rail
from dxctechnology.psa_user_profile_gsap.user_profile_sync.utils.request_payload import get_adduser_updateuser_conf


def process_updateuser_task_group(execution_timeout_days, instance, caller):
    with rail.TaskGroup(group_id=f'process_updateuser_child_task_{caller}', prefix_group_id=False):

        update_userprofile_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id=f'update_userprofile_child_dag_{caller}',
            retries=0,
            items=lambda dag_run: [dag_run.conf],
            execution_timeout=timedelta(
                days=execution_timeout_days),
            trigger_dag_id=f'dxctechnology_psa_userprofiles_update_child_gsap_{instance}',
            conf=get_adduser_updateuser_conf
        )

        wait_for_update_userprofile_child_dag = rail.WaitForDagRunsSensor(
            task_id=f'wait_for_update_userprofile_child_dag_{caller}',
            dag_runs='{{ result(\''+update_userprofile_child_dag.task_id+'\') }}',
            execution_timeout=timedelta(
                days=execution_timeout_days)
        )

        update_userprofile_child_dag >> wait_for_update_userprofile_child_dag

    return (update_userprofile_child_dag, wait_for_update_userprofile_child_dag)
