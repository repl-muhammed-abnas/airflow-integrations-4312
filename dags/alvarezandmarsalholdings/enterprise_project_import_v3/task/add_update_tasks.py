import rail
from alvarezandmarsalholdings.enterprise_project_import_v3.utils import request_payload, response_filter, python_callable

null = None

def get_task_added_or_updated(group_id, level, existing_task_to_process = ''):

    with rail.TaskGroup(group_id=group_id, prefix_group_id=False) as add_update_task:

        dummy_add_update_task = rail.EmptyOperator(
            task_id=f"dummy_add_update_task_{level}{existing_task_to_process}"
        )

        task_level = f'task_level{level}' if not existing_task_to_process else existing_task_to_process

        get_all_task_to_add_update = rail.PythonOperator(
            task_id=f"get_all_task_to_add_update_{level}{existing_task_to_process}",
            python_callable=lambda dag_run: python_callable.get_task_to_add_update_skip(
                level,
                python_callable.get_all_data_from_json_artifact(rail.result('format_payload_tasks'))[0][task_level],
                dag_run.conf['Project'],
            )
        )

        has_tasks_to_add = rail.IfOperator(
            task_id = f'has_tasks_to_add_{level}{existing_task_to_process}',
            test=lambda: bool(rail.result(f"get_all_task_to_add_update_{level}{existing_task_to_process}")['tasks_to_add']),
            yes_task= f'add_task_{level}{existing_task_to_process}',
            no_task= f'has_tasks_to_update_{level}{existing_task_to_process}'
        )

        add_task = rail.RepliconServiceCallForEachItemOperator(
            task_id=f'add_task_{level}{existing_task_to_process}',
            endpoint="services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications",
            items=lambda: rail.result(f"get_all_task_to_add_update_{level}{existing_task_to_process}")['tasks_to_add'],
            data=lambda item, dag_run: request_payload.get_add_task_payload(
                dag_run, item, level
            ),
            data_handler=lambda response: response_filter.get_existing_tasks_updated(response, level),
            all_result_data_handler=lambda data_handler: response_filter.get_add_task_response(data_handler),
        )

        log_task_added_success_error = rail.WriteLogOperator(
            task_id=f"log_task_added_success_error_{level}{existing_task_to_process}",
            log="{{result('create_project_log')}}",
            message="{{ item.details }}",
            items=lambda: python_callable.map_task_success_error(
                f"get_all_task_to_add_update_{level}{existing_task_to_process}",
                f"add_task_{level}{existing_task_to_process}", "add","tasks_to_add"),
            properties=lambda item, dag_run: {
                'projectcode': dag_run.conf['Project'],
                'projectname': dag_run.conf['ProjectDescription'],
                'taskcode': item['taskcode'],
                'taskname': item['taskname'],
                "action": "Add",
                "status": item['status'],
                "details": item['details'],
            }
        )

        has_tasks_to_update = rail.IfOperator(
            task_id = f'has_tasks_to_update_{level}{existing_task_to_process}',
            test=lambda: bool(rail.result(f"get_all_task_to_add_update_{level}{existing_task_to_process}")['tasks_to_update']),
            yes_task= f'update_task_{level}{existing_task_to_process}',
            no_task= f'has_tasks_to_skip_{level}{existing_task_to_process}'
        )

        update_task = rail.RepliconServiceCallForEachItemOperator(
            task_id=f"update_task_{level}{existing_task_to_process}",
            endpoint="/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications",
            items=lambda: rail.result(f"get_all_task_to_add_update_{level}{existing_task_to_process}")['tasks_to_update'],
            data=lambda dag_run, item: request_payload.get_update_task_payload(
                dag_run, item, level
            ),
            data_handler=lambda response: {'response': response},
            all_result_data_handler=lambda data_handler: response_filter.get_update_task_response(data_handler),
        )

        log_task_updated_success_error = rail.WriteLogOperator(
            task_id=f"log_task_updated_success_error_{level}{existing_task_to_process}",
            log="{{result('create_project_log')}}",
            message="{{ item.details }}",
            items=lambda: python_callable.map_task_success_error(
                f"get_all_task_to_add_update_{level}{existing_task_to_process}",
                f"update_task_{level}{existing_task_to_process}", "updat","tasks_to_update"),
            properties=lambda dag_run, item: {
                'projectcode': dag_run.conf['Project'],
                'projectname': dag_run.conf['ProjectDescription'],
                'taskcode': item['taskcode'],
                'taskname': item['taskname'],
                "action": "Update",
                "status": item['status'],
                "details": item['details'],
            }
        )

        has_tasks_to_skip = rail.IfOperator(
            task_id = f'has_tasks_to_skip_{level}{existing_task_to_process}',
            test=lambda: bool(rail.result(f"get_all_task_to_add_update_{level}{existing_task_to_process}")['task_to_skip']),
            yes_task= f'log_task_skipped_{level}{existing_task_to_process}',
            no_task= f'dummy_end_task_group_{level}{existing_task_to_process}'
        )

        log_task_skipped = rail.WriteLogOperator(
            task_id=f"log_task_skipped_{level}{existing_task_to_process}",
            log="{{result('create_project_log')}}",
            severity="Exception",
            message="Skipped",
            items=lambda: rail.result(f"get_all_task_to_add_update_{level}{existing_task_to_process}")['task_to_skip'],
            properties=lambda dag_run, item: {
                'projectcode': dag_run.conf['Project'],
                'projectname': dag_run.conf['ProjectDescription'],
                'taskcode': item['task']['taskcode'],
                'taskname': item['task']['taskname'],
                "action": "Update",
                "status": 'Skipped',
                "details": item['message'],
            }
        )

        dummy_end_task_group = rail.EmptyOperator(
            task_id=f"dummy_end_task_group_{level}{existing_task_to_process}"
        )

        dummy_add_update_task >> get_all_task_to_add_update >> has_tasks_to_add >> rail.Label(
            "Yes") >> add_task >> log_task_added_success_error >> has_tasks_to_update
        has_tasks_to_add >> rail.Label(
            "No") >> has_tasks_to_update >> rail.Label(
            "Yes") >> update_task >> log_task_updated_success_error >> has_tasks_to_skip
        has_tasks_to_update >> rail.Label(
            "No") >> has_tasks_to_skip >> rail.Label(
            "Yes") >> log_task_skipped >> dummy_end_task_group
        has_tasks_to_skip >> rail.Label(
            "No") >> dummy_end_task_group


    return dummy_add_update_task, add_update_task
