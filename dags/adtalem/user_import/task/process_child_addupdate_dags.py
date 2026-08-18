from datetime import timedelta
import rail
from adtalem.user_import.utils.request_payload import get_adduser_updateuser_conf


def process_child_addupdate_task_group(execution_timeout_days, instance, process_type='production'):
    with rail.TaskGroup(group_id=f'process_child_addupdate_task_{process_type}', prefix_group_id=False):

        add_userdag = f'adtalem_userimport_{process_type}_child_add_user_{instance}' if \
            process_type == 'caribbean' else f'adtalem_userimport_child_add_user_{process_type}_{instance}'
        update_userdag = f'adtalem_userimport_{process_type}_child_update_user_{instance}' if \
            process_type == 'caribbean' else f'adtalem_userimport_child_update_user_{process_type}_{instance}'

        is_user_not_present = rail.IfOperator(
            task_id=f'is_user_not_present_{process_type}',
            test="{{ result('get_required_useruri') | is_falsy }}",
            yes_task=f'trigger_adduser_{process_type}_child',
            no_task=f'trigger_updateuser_{process_type}_child',
        )

        trigger_adduser_child = rail.TriggerDagRunForEachItemOperator(
            task_id=f'trigger_adduser_{process_type}_child',
            retries=0,
            items=lambda dag_run: [dag_run.conf],
            trigger_dag_id=add_userdag,
            execution_timeout=timedelta(
                days=execution_timeout_days),
            conf=get_adduser_updateuser_conf
        )

        wait_for_adduser_child = rail.WaitForDagRunsSensor(
            task_id=f'wait_for_adduser_{process_type}_child',
            dag_runs="{{ result('trigger_adduser_" +
            process_type + "_child') }}",
            execution_timeout=timedelta(
                days=execution_timeout_days)
        )

        trigger_updateuser_child = rail.TriggerDagRunForEachItemOperator(
            task_id=f'trigger_updateuser_{process_type}_child',
            retries=0,
            items=lambda dag_run: [dag_run.conf],
            trigger_dag_id=update_userdag,
            execution_timeout=timedelta(
                days=execution_timeout_days),
            conf=get_adduser_updateuser_conf
        )

        wait_for_updateuser_child = rail.WaitForDagRunsSensor(
            task_id=f'wait_for_updateuser_{process_type}_child',
            dag_runs="{{ result('trigger_updateuser_" +
            process_type + "_child') }}",
            execution_timeout=timedelta(
                days=execution_timeout_days)
        )

        is_user_not_present >> rail.Label(
            'Yes') >> trigger_adduser_child >> wait_for_adduser_child

        is_user_not_present >> rail.Label(
            'No') >> trigger_updateuser_child >> wait_for_updateuser_child

        return (is_user_not_present, wait_for_adduser_child, wait_for_updateuser_child)
