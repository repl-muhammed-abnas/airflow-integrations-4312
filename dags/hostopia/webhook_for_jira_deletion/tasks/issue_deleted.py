from datetime import timedelta
import rail
from hostopia.jira_integration.utils import response_filter


def issue_deleted():
    with rail.TaskGroup(group_id="issue_deleted_in_jira", prefix_group_id=False) as issue_deleted_in_jira:

        check_issue_subtask_status = rail.IfOperator(
            task_id='check_issue_subtask_status',
            test=lambda: rail.result("get_triggered_data")['subtaskstatus'],
            yes_task='finish_task',
            no_task='get_project_in_replicon_for_issue'
        )

        get_project_in_replicon_for_issue = rail.RepliconServiceOperator(
            task_id='get_project_in_replicon_for_issue',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={"projects": [
                {
                    "code": '{{ result("get_triggered_data")["key"] }}'
                }]},
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                {"projectDetails": None}])[0]['projectDetails']
        )

        has_project_data_in_replicon_for_issue = rail.IfOperator(
            task_id='has_project_data_in_replicon_for_issue',
            test='{{ result("get_project_in_replicon_for_issue") | is_truthy }}',
            yes_task='update_project_status_for_task',
            no_task='finish_task'
        )

        update_project_status_for_task = rail.RepliconServiceOperator(
            task_id='update_project_status_for_task',
            endpoint='/services/ProjectService1.svc/UpdateStatus',
            data={
                "projectUri": '{{ result("get_project_in_replicon_for_issue")["uri"] }}',
                "projectStatusUri": "urn:replicon:project-status-type:completed"
            }
        )

        get_all_tasks_for_project= rail.RepliconServiceOperator(
            task_id='get_all_tasks_for_project',
            endpoint='/services/ProjectService1.svc/BulkGetTaskDetails',
            data={
                "pageIndex": "1",
                "pageSize": "10000",
                "projectUris": [
                    '{{ result("get_project_in_replicon_for_issue").uri }}'
                ]
            },
            response_filter=response_filter.get_task_uris
        )

        update_task_status = rail.RepliconServiceCallForEachItemOperator(
            task_id="update_task_status",
            endpoint='/services/TaskService1.svc/Close',
            items='{{ result("get_all_tasks_for_project") | to_json }}',
            execution_timeout=timedelta(days=14),
            flatten=True,
            data={
                "taskUri": '{{ item.uri }}'
            },
        )

        finish_task = rail.EmptyOperator(
            task_id='finish_task'
        )

        check_issue_subtask_status >> rail.Label(
            "Yes") >> get_project_in_replicon_for_issue >> has_project_data_in_replicon_for_issue

        has_project_data_in_replicon_for_issue >> rail.Label(
            "Yes") >> update_project_status_for_task >> get_all_tasks_for_project >> update_task_status >> finish_task

        has_project_data_in_replicon_for_issue >> rail.Label(
            "No") >> finish_task

        check_issue_subtask_status >> rail.Label(
            "No") >> finish_task

    return issue_deleted_in_jira
