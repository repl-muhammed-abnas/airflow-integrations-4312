from datetime import timedelta
import uuid
import rail
from ce_replicon_integration.initial_setup.utils.python_callable_method import (
    parse_group_option,
    build_hierarchical_options,
    calculate_sync_operations
)
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.union_group_child_dag_id,
        description=f'{config.company_key} Syncs Union groups (with locals as hierarchy) from ComputerEase to Replicon',
        company_key=config.company_key,
        max_active_runs=config.child_dag_max_active_runs,
        multi_tenant=True
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_unions_from_ce',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_unions_from_ce = rail.ComputereaseAPIOperator(
            task_id='get_all_unions_from_ce',
            endpoint='/catalog/union',
            request_method='GET',
            computerease_conn_id='{{ dag_run.conf.computerease_conn_id }}',
            data_handler=lambda response: response.get('data', [])
        )

        def get_getdata_payload(dag_run):
            group_var = dag_run.conf['getdataservicevariable']
            return {
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    f"urn:replicon:{group_var}-list-column:name",
                    f"urn:replicon:{group_var}-list-column:code",
                    f"urn:replicon:{group_var}-list-column:{group_var}"
                ],
                "sort": [],
                "filterExpression": null
            }

        get_all_group_options_in_replicon = rail.RepliconServiceOperator(
            task_id='get_all_group_options_in_replicon',
            endpoint="{{dag_run.conf.optionsendpoint}}",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=get_getdata_payload,
            data_handler=lambda response: [parse_group_option(opt) for opt in response['rows']]
        )

        def get_options_delta():
            ce_unions = rail.result('get_all_unions_from_ce')
            existing_options = rail.result('get_all_group_options_in_replicon')

            root_dept_uri = rail.find_first_by_attr_and_get_attr(existing_options, 'name', config.root_department, 'uri')

            parent_options, child_options, _ = build_hierarchical_options(
                ce_unions, existing_options, root_dept_uri, config.root_department
            )

            ce_all_options = parent_options + child_options

            options_to_disable, options_to_add = calculate_sync_operations(
                ce_all_options, existing_options, config.root_department
            )

            parent_options_final = [opt for opt in options_to_add if opt['is_parent'] and not opt.get('uri')]
            child_options_final = [opt for opt in options_to_add if not opt['is_parent']]
            return {
                'options_to_disable': options_to_disable,
                'options_to_add': options_to_add,
                'parent_options': parent_options_final,
                'child_options': child_options_final
            }

        get_options_to_add_disable = rail.PythonOperator(
            task_id='get_options_to_add_disable',
            python_callable=get_options_delta
        )

        disable_options = rail.RepliconServiceCallForEachItemOperator(
            task_id='disable_options',
            items=lambda: rail.result('get_options_to_add_disable')['options_to_disable'],
            endpoint="{{dag_run.conf.disableendpoint}}",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda dag_run, item: {
                f"{dag_run.conf['grouptypevariable']}Uri": item['uri']
            }
        )

        def build_create_modify_payload(dag_run, item):
            group_payload = {
                "uri": item['uri'] or null,
                "parameterCorrelationId": null
            }
            if not item.get('uri') and item.get('parent_uri'):
                group_payload["parent"] = {"uri": item['parent_uri']}

            return {
                dag_run.conf['grouptypevariable']: group_payload,
                "modifications": {
                    "name": item['name'],
                    "codeToApply": {"value": item['code']},
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4())
            }

        if_parent_options_to_create = rail.IfOperator(
            task_id='if_parent_options_to_create',
            test=lambda: len(rail.result('get_options_to_add_disable')['parent_options']) > 0,
            yes_task='create_modify_parent_unions',
            no_task='get_updated_group_options'
        )

        create_modify_parent_unions = rail.RepliconServiceCallForEachItemOperator(
            task_id='create_modify_parent_unions',
            items=lambda: rail.result('get_options_to_add_disable')['parent_options'],
            endpoint="{{dag_run.conf.creategroupendpoint}}",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=build_create_modify_payload
        )

        get_updated_group_options = rail.RepliconServiceOperator(
            task_id='get_updated_group_options',
            endpoint="{{dag_run.conf.optionsendpoint}}",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=get_getdata_payload,
            data_handler=lambda response: [parse_group_option(opt) for opt in response['rows']]
        )

        def update_child_options_with_parent_uris():
            child_options = rail.result('get_options_to_add_disable')['child_options']
            updated_options = rail.result('get_updated_group_options')

            filtered_child_options = []
            for child in child_options:
                if not child.get('uri') and child.get('parent_code'):
                    parent_uri = rail.find_first_by_attr_and_get_attr(
                        updated_options, 'code', child['parent_code'], 'uri'
                    )
                    if parent_uri:
                        child['parent_uri'] = parent_uri
                        filtered_child_options.append(child)
                elif child.get('uri'):
                    child['parent_uri'] = None
                    filtered_child_options.append(child)

            return filtered_child_options

        update_child_options = rail.PythonOperator(
            task_id='update_child_options',
            python_callable=update_child_options_with_parent_uris
        )

        if_child_options_to_create = rail.IfOperator(
            task_id='if_child_options_to_create',
            test=lambda: len(rail.result('update_child_options')) > 0,
            yes_task='create_modify_child_locals',
            no_task='catch_error'
        )

        create_modify_child_locals = rail.RepliconServiceCallForEachItemOperator(
            task_id='create_modify_child_locals',
            items=lambda: rail.result('update_child_options'),
            endpoint="{{dag_run.conf.creategroupendpoint}}",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=build_create_modify_payload
        )

        def get_downstreamtasks_error(error_message):
            return {
                'error': f'Error in union group sync - {error_message}'
            }

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ get_error_message() }}']
        )

        batch_task >> catch_error
        batch_task >> get_all_unions_from_ce >> get_all_group_options_in_replicon >> get_options_to_add_disable >> disable_options >> if_parent_options_to_create
        if_parent_options_to_create >> rail.Label('Yes') >> create_modify_parent_unions >> get_updated_group_options
        if_parent_options_to_create >> rail.Label('No') >> get_updated_group_options
        get_updated_group_options >> update_child_options >> if_child_options_to_create
        if_child_options_to_create >> rail.Label('Yes') >> create_modify_child_locals >> catch_error
        if_child_options_to_create >> rail.Label('No') >> catch_error

        return dag


rail.for_each_instance(create_dag)
