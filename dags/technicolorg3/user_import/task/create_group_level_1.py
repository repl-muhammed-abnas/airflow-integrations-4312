import rail
from technicolorg3.user_import.utils.python_callable_method import get_group_parameters
from technicolorg3.user_import.utils.request_payload import create_group_level1_payload


def create_group_level1_task_group(dws_service):
    group_name = dws_service.lower()
    # pylint: disable=line-too-long
    end_point = f'services/{dws_service}GroupService1.svc/Create{dws_service}GroupOrApplyModification' if group_name == 'department' else f'services/{dws_service}Service1.svc/Create{dws_service}OrApplyModification'

    with rail.TaskGroup(group_id=f'create_{group_name}_level1_task', prefix_group_id=False):

        get_group_params = rail.PythonOperator(
            task_id=f'get_{group_name}_params',
            python_callable=get_group_parameters,
            op_args=[group_name]
        )

        is_group_hierarchy_exception = rail.IfOperator(
            task_id=f'is_{group_name}_hierarchy_exception',
            test=lambda dag_run: rail.result(f'get_{group_name}_params')[
                'required_level'] > dag_run.conf['hierarchy_threshold'],
            yes_task='catch_group_error',
            no_task=f'process_create_{group_name}'
        )

        process_create_group = rail.EmptyOperator(
            task_id=f'process_create_{group_name}'
        )

        is_group_hierarchy_level1 = rail.IfOperator(
            task_id=f'is_{group_name}_hierarchy_level1',
            test=lambda: rail.result(f'get_{group_name}_params')[
                'required_level'] == 1,
            yes_task=f'create_{group_name}_level1',
            no_task=f'process_other_levels_{group_name}'
        )

        create_group_level1 = rail.RepliconServiceOperator(
            task_id=f'create_{group_name}_level1',
            endpoint=end_point,
            data=lambda dag_run: create_group_level1_payload(
                dag_run, group_name)
        )

        def get_fullpath(group_name):
            if group_name == 'department':
                return f"Technicolor|{rail.result('get_department_params')['required_name']}"
            return rail.result(f'create_{group_name}_level1')['displayText']

        write_group_log = rail.WriteLogOperator(
            task_id=f'write_{group_name}_log',
            log="{{ dag_run.conf.gmbh_groups_log }}",
            message=f'add {group_name} level1 to gmbh log',
            properties=lambda: {
                'name': rail.result(f'create_{group_name}_level1')['displayText'],
                'uri': rail.result(f'create_{group_name}_level1')['uri'],
                'fullpath': get_fullpath(group_name),
                'type': group_name
            }
        )

        get_group_params >> is_group_hierarchy_exception

        is_group_hierarchy_exception >> rail.Label(
            'No') >> process_create_group >> is_group_hierarchy_level1

        is_group_hierarchy_level1 >> rail.Label(
            'Yes') >> create_group_level1 >> write_group_log

        return (get_group_params, is_group_hierarchy_exception, is_group_hierarchy_level1, write_group_log)
