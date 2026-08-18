import rail
from alvarezandmarsalholdings.customer_project_import_v1.utils import request_payload, response_filter, python_callable

null = None

def get_task_added_or_updated(group_id, level):

    with rail.TaskGroup(group_id=group_id, prefix_group_id=False) as add_update_task:

        dummy_add_update_task = rail.EmptyOperator(
            task_id=f"dummy_add_update_task_{level}"
        )

        def get_project_uri():
            return rail.result('update_project')['uri'] if request_payload.does_project_code_exist() else \
                    rail.result('create_project')['uri']

        get_all_tasks_for_project = rail.RepliconServiceOperator(
            task_id=f"get_all_tasks_for_project_{level}",
            endpoint='/services/TaskService1.svc/GetDescendantTaskDetails',
            data=lambda: {
                "parentUri": get_project_uri()
            },
            data_handler=response_filter.format_project_task_details
        )

        get_all_task_to_add_update = rail.PythonOperator(
            task_id=f"get_all_task_to_add_update_{level}",
            python_callable=lambda: python_callable.get_task_to_add_update_skip(
                rail.result(f'get_all_tasks_for_project_{level}'),
                rail.result('format_payload_tasks')[f'task_level{level}'],
            )
        )

        has_tasks_to_update = rail.IfOperator(
            task_id = f'has_tasks_to_update_{level}',
            test=lambda: bool(rail.result(f"get_all_task_to_add_update_{level}")['tasks_to_update']),
            yes_task= f'update_task_{level}',
            no_task= f'has_tasks_to_add_{level}'
        )

        update_task = rail.RepliconServiceCallForEachItemOperator(
            task_id=f"update_task_{level}",
            endpoint="/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications",
            items=lambda: rail.result(f"get_all_task_to_add_update_{level}")['tasks_to_update'],
            data=lambda dag_run, item: request_payload.get_update_task_payload(
                dag_run, item
            ),
            all_result_data_handler=response_filter.get_flatten_rows
        )

        log_task_updated_success_error = rail.WriteLogOperator(
            task_id=f"log_task_updated_success_error_{level}",
            log="{{result('create_project_log')}}",
            message="{{ item.details }}",
            items=lambda: python_callable.map_task_success_error(
                f"get_all_task_to_add_update_{level}",
                f"update_task_{level}", "updat","tasks_to_update"),
            properties=lambda dag_run, item: {
                'projectcode': dag_run.conf['ProjectID'],
                'projectname': dag_run.conf['ProjectName'],
                'taskcode': item['taskcode'],
                'taskname': item['taskname'],
                "action": "Update",
                "status": item['status'],
                "details": item['details'],
            }
        )

        has_tasks_to_add = rail.IfOperator(
            task_id = f'has_tasks_to_add_{level}',
            test=lambda: bool(rail.result(f"get_all_task_to_add_update_{level}")['tasks_to_add']),
            yes_task= f'add_task_{level}',
            no_task= f'has_tasks_to_skip_{level}'
        )

        add_task = rail.RepliconServiceCallForEachItemOperator(
            task_id=f'add_task_{level}',
            endpoint="services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications",
            items=lambda: rail.result(f"get_all_task_to_add_update_{level}")['tasks_to_add'],
            data=lambda dag_run, item: request_payload.get_add_task_payload(
                dag_run, item
            ),
            all_result_data_handler=response_filter.get_flatten_rows
        )

        log_task_added_success_error = rail.WriteLogOperator(
            task_id=f"log_task_added_success_error_{level}",
            log="{{result('create_project_log')}}",
            message="{{ item.details }}",
            items=lambda: python_callable.map_task_success_error(
                f"get_all_task_to_add_update_{level}",
                f"add_task_{level}", "add","tasks_to_add"),
            properties=lambda item, dag_run: {
                'projectcode': dag_run.conf['ProjectID'],
                'projectname': dag_run.conf['ProjectName'],
                'taskcode': item['taskcode'],
                'taskname': item['taskname'],
                "action": "Add",
                "status": item['status'],
                "details": item['details'],
            }
        )

        has_tasks_to_skip = rail.IfOperator(
            task_id = f'has_tasks_to_skip_{level}',
            test=lambda: bool(rail.result(f"get_all_task_to_add_update_{level}")['task_to_skip']),
            yes_task= f'log_task_skipped_{level}',
            no_task= f'dummy_end_task_group_{level}'
        )

        log_task_skipped = rail.WriteLogOperator(
            task_id=f"log_task_skipped_{level}",
            log="{{result('create_project_log')}}",
            severity="Exception",
            message="Skipped",
            items=lambda: rail.result(f"get_all_task_to_add_update_{level}")['task_to_skip'],
            properties=lambda dag_run, item: {
                'projectcode': dag_run.conf['ProjectID'],
                'projectname': dag_run.conf['ProjectName'],
                'taskcode': item['task']['taskcode'],
                'taskname': item['task']['taskname'],
                "action": "Update",
                "status": 'Skipped',
                "details": item['message'],
            }
        )

        dummy_end_task_group = rail.EmptyOperator(
            task_id=f"dummy_end_task_group_{level}"
        )

        dummy_add_update_task >> get_all_tasks_for_project >> get_all_task_to_add_update >> has_tasks_to_update
        has_tasks_to_update >> rail.Label(
            "Yes") >> update_task >> log_task_updated_success_error >> has_tasks_to_add
        has_tasks_to_update >> rail.Label(
            "No") >> has_tasks_to_add
        has_tasks_to_add >> rail.Label(
            "Yes") >> add_task >> log_task_added_success_error >> has_tasks_to_skip
        has_tasks_to_add >> rail.Label(
            "No") >> has_tasks_to_skip
        has_tasks_to_skip >> rail.Label(
            "Yes") >> log_task_skipped >> dummy_end_task_group
        has_tasks_to_skip >> rail.Label(
            "No") >> dummy_end_task_group


    return dummy_add_update_task, add_update_task
