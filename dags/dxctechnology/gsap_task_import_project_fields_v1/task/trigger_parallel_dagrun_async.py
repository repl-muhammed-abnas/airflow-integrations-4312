from rail import TriggerDagRunForEachItemOperator, TaskGroup

def trigger_parallel_dagrun_async(  # pylint: disable=too-many-arguments
    task_id,
    items,
    trigger_dag_id,
    parallel_count,
    execution_timeout,
    conf,
    batch_size = None
):

    with TaskGroup(group_id=task_id, prefix_group_id=False) as task_group:

        for i in range(parallel_count):
            item_task_id = f'{task_id}_{i+1}'

            TriggerDagRunForEachItemOperator(
                task_id=item_task_id,
                retries=0,
                items=items,
                trigger_dag_id=trigger_dag_id,
                execution_timeout=execution_timeout,
                conf=conf,
                batch_size=batch_size,
                parallel_count=parallel_count,
                parallel_index=i,
            )

        return task_group
