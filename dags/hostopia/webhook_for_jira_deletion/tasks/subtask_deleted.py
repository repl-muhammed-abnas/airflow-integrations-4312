import rail
from hostopia.webhook_for_jira_deletion.utils import custom_method
from hostopia.webhook_for_jira_deletion.utils import response_filter


def subtask_deleted():
    with rail.TaskGroup(group_id="subtask_deleted_in_jira", prefix_group_id=False) as subtask_deleted_in_jira:

        check_subtask_status = rail.IfOperator(
            task_id='check_subtask_status',
            test=lambda: rail.result("get_triggered_data")['subtaskstatus'],
            yes_task='get_project_in_replicon_for_subtask',
            no_task='finish_sub_task'
        )

        get_project_in_replicon_for_subtask = rail.RepliconServiceOperator(
            task_id='get_project_in_replicon_for_subtask',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={"projects": [
                {
                    "code": '{{ result("get_triggered_data")["parent"] }}'
                }]},
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                {"projectDetails": None}])[0]['projectDetails']
        )

        has_project_data_in_replicon_for_subtask = rail.IfOperator(
            task_id='has_project_data_in_replicon_for_subtask',
            test='{{ result("get_project_in_replicon_for_subtask") | is_truthy }}',
            yes_task='check_project_status_for_subtask',
            no_task='finish_sub_task'
        )

        check_project_status_for_subtask = rail.IfOperator(
            task_id='check_project_status_for_subtask',
            test=custom_method.check_status,
            yes_task='get_all_project_task_in_replicon_for_subtask',
            no_task='finish_sub_task'
        )

        get_all_project_task_in_replicon_for_subtask = rail.RepliconServiceOperator(
            task_id='get_all_project_task_in_replicon_for_subtask',
            endpoint='/services/TaskService1.svc/GetDescendantTaskDetails',
            data={
                "parentUri": '{{ result("get_project_in_replicon_for_subtask")["uri"] }}'
            },
            response_filter=response_filter.get_task_data
        )

        is_task_exist_in_replicon_for_subtask = rail.IfOperator(
            task_id='is_task_exist_in_replicon_for_subtask',
            test= '{{ result("get_all_project_task_in_replicon_for_subtask") | is_truthy }}',
            yes_task='update_task_status_for_subtask',
            no_task='finish_sub_task'
        )

        update_task_status_for_subtask = rail.RepliconServiceOperator(
            task_id='update_task_status_for_subtask',
            endpoint='/services/TaskService1.svc/Close',
            data={
                "taskUri": '{{ result("get_all_project_task_in_replicon_for_subtask")[0]["Taskuri"] }}'
            }
        )

        finish_sub_task = rail.EmptyOperator(
            task_id='finish_sub_task'
        )

        check_subtask_status >> rail.Label(
            "Yes") >> finish_sub_task

        check_subtask_status >> rail.Label(
            "No") >> get_project_in_replicon_for_subtask >> has_project_data_in_replicon_for_subtask

        has_project_data_in_replicon_for_subtask >> rail.Label(
            "No") >> finish_sub_task

        has_project_data_in_replicon_for_subtask >> rail.Label(
            "Yes") >> check_project_status_for_subtask

        check_project_status_for_subtask>> rail.Label(
            "Yes") >> get_all_project_task_in_replicon_for_subtask >> is_task_exist_in_replicon_for_subtask

        is_task_exist_in_replicon_for_subtask>> rail.Label(
            "Yes") >> update_task_status_for_subtask >> finish_sub_task

        is_task_exist_in_replicon_for_subtask >> rail.Label(
            "No") >> finish_sub_task

        check_project_status_for_subtask >> rail.Label(
            "No") >> finish_sub_task

    return subtask_deleted_in_jira
