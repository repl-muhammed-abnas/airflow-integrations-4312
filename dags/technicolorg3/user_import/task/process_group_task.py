from datetime import timedelta
import rail
from technicolorg3.user_import.utils.python_callable_method import get_query_for_raw_group, get_query_for_group_to_create
from technicolorg3.user_import.utils.request_payload import get_replicon_groups_list
from technicolorg3.user_import.utils.response_filter import page_handler, map_replicon_groups


def process_groups_task_group(dws_service, execution_timeout_days, instance):
    group = dws_service.lower()
    gmbh_log_groups = ('department', 'servicecenter', 'location')
    endpoint = f'/services/{dws_service}GroupListService1.svc/GetData' if group == 'department' else f'/services/{dws_service}ListService1.svc/GetData'

    with rail.TaskGroup(group_id=f'process_{group}_task', prefix_group_id=False):

        get_replicon_groups = rail.RepliconServicePageOperator(
            task_id=f'get_replicon_{group}',
            endpoint=endpoint,
            data=lambda: get_replicon_groups_list(group),
            page_handler=page_handler,
            all_result_data_handler=lambda response: map_replicon_groups(
                response, group)
        )

        replicon_groups_collection = rail.CreateCollectionOperator(
            task_id=f'replicon_{group}_collection',
            source=lambda: rail.result(f'get_replicon_{group}'),
            name=f'{group}data'
        )

        raw_groups_collection = rail.QueryCollectionOperator(
            task_id=f'raw_groups_{group}_collection',
            query=get_query_for_raw_group(group),
            name=f'{group}rawdata'
        )

        get_groups_not_in_replicon = rail.QueryCollectionOperator(
            task_id=f'get_{group}_not_in_replicon',
            query=get_query_for_group_to_create(group)
        )

        is_groups_to_create = rail.IfOperator(
            task_id=f'is_{group}_to_create',
            test=lambda: rail.result(
                f'get_{group}_not_in_replicon', 'length') > 0,
            yes_task=f'write_{group}_gmbh_log' if group in gmbh_log_groups else f'create_{group}',
            no_task=f'process_{group}_finish'
        )

        create_group = rail.TriggerDagRunForEachItemOperator(
            task_id=f'create_{group}',
            retries=0,
            items=lambda: rail.result(f'get_{group}_not_in_replicon'),
            execution_timeout=timedelta(
                days=execution_timeout_days),
            trigger_dag_id=f'technicolorg3_user_import_child_groups_add_{instance}',
            conf=lambda item: {
                **dict(item.items()),
                **{
                    'gmbh_groups_log': rail.result('create_gmbh_groups_log'),
                    'dws_service_name': dws_service,
                    'type': group,
                    'hierarchy_threshold': 7 if group == 'location' else 6,
                    'company_department_uri': rail.find_first_by_attr_and_get_attr(
                        rail.result(f'get_replicon_{group}'), 'fullpath', 'Technicolor', 'departmenturi') if group == 'department' else None
                }
            }
        )

        wait_for_create_group = rail.WaitForDagRunsSensor(
            task_id=f'wait_for_create_{group}',
            dag_runs="{{ result('create_" + group + "') }}",
            execution_timeout=timedelta(
                days=execution_timeout_days)
        )

        gather_groups_error = rail.GatherResultsFromDagRunsOperator(
            task_id=f'gather_{group}_error',
            dag_runs="{{ result('create_" + group + "') }}",
            dagrun_task_id='catch_group_error',
            flatten=True
        )

        process_group_finish = rail.EmptyOperator(
            task_id=f'process_{group}_finish'
        )

        get_replicon_groups >> replicon_groups_collection >> raw_groups_collection >> \
            get_groups_not_in_replicon >> is_groups_to_create

        if group in gmbh_log_groups:

            write_group_gmbh_log = rail.WriteLogOperator(
                task_id=f'write_{group}_gmbh_log',
                log="{{ result('create_gmbh_groups_log') }}",
                items="{{ result('replicon_" + group + "_collection') }}",
                message=f'add {group} to gmbh log',
                properties=lambda item: {
                    'name': item[f'{group}name'],
                    'uri': item[f'{group}uri'],
                    'fullpath': item['fullpath'],
                    'type': group
                }
            )

            is_groups_to_create >> rail.Label(
                'Yes') >> write_group_gmbh_log >> create_group

        else:

            is_groups_to_create >> rail.Label(
                'Yes') >> create_group

        create_group >> wait_for_create_group >> gather_groups_error >> process_group_finish

        is_groups_to_create >> rail.Label(
            'No') >> process_group_finish

    return (get_replicon_groups, process_group_finish)
