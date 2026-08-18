from datetime import timedelta
import json
import rail

def add_default_tasks(tasks, bill_type, config, caller):
    with rail.TaskGroup(group_id=f'add_default_tasks_{caller}_{bill_type}', prefix_group_id=False) as add_default_task:

        assign_default_tasks = rail.TriggerDagRunForEachItemOperator(
            task_id=f'assign_default_tasks_{caller}_{bill_type}',
            items=lambda: tasks ,
            trigger_dag_id=f"rosterfy_hubspot_polaris_psa_integration_add_default_task_child_{config.instance}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
            conf=lambda item: {
                'billtype':bill_type,
                'task_name': item,
                'projectcode': json.loads(rail.result('get_details_of_deal'))['id'] + '-sales' if caller == 'presales' else json.loads(
                    rail.result('get_details_of_deal'))['id']
            }
        )

        assign_default_tasks

        return add_default_task
