from datetime import timedelta
import itertools
import rail

from deltek_vantagepoint_v2.user_sync.utils.python_callable_method import page_handler
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.tag_oef_options_update_child_dag_id,
        description=f'{config.company_key} Syncs the list of options for a tag OEF into Replicon',
        company_key=config.company_key,
        max_active_runs=config.child_dag_max_active_runs,
        multi_tenant=True
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_existing_tags',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def compose_tag_details(response):
            flatten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            return list(map(lambda tag: {
                'name': tag['cells'][0]['textValue'] if 'textValue' in tag['cells'][0] else None,
                'code': tag['cells'][1]['textValue'] if 'textValue' in tag['cells'][1] else None,
                'enabled': tag['cells'][2]['textValue'] if 'textValue' in tag['cells'][2] else None,
                'uri': tag['cells'][3]['uri'] if 'uri' in tag['cells'][3] else None
            }, flatten_rows))

        get_existing_tags = rail.RepliconServicePageOperator(
            task_id='get_existing_tags',
            endpoint='services/ObjectExtensionTagListService1.svc/GetData',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:object-extension-tag-list-column:name",
                    "urn:replicon:object-extension-tag-list-column:code",
                    "urn:replicon:object-extension-tag-list-column:enabled",
                    "urn:replicon:object-extension-tag-list-column:object-extension-tag"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:definition"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "value": {
                            "uri": dag_run.conf['definition_uri']
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=compose_tag_details
        )

        def get_oef_name(tag, existing_tags, required_tags):
            is_tag_present_by_name = rail.find_first_by_attr_and_get_attr(
                existing_tags, 'name', tag['name'])
            tags_to_create_by_this_name = list(
                filter(lambda option: option['Description'] == tag['name'], required_tags))
            if is_tag_present_by_name or (len(tags_to_create_by_this_name) > 1):
                return f"{tag['name']} - {tag['code']}"
            return tag['name']

        def parse_options(dag_run):
            oef_type = dag_run.conf.get('oef_id', '')
            parsed_options = []
            if oef_type == 'laborcode':
                parsed_options = list(map(lambda tag: {
                    'name': f"{tag['Description']}/{tag['Code']}",
                    'code': str(tag['Code'])
                }, dag_run.conf['options']))
            else:
                parsed_options = list(map(lambda tag: {
                    'name': tag['Description'],
                    'code': str(tag['Category'])
                }, dag_run.conf['options']))
            return parsed_options

        def get_tags_to_create_and_enable(dag_run):
            existing_tags = rail.result('get_existing_tags')
            required_tags = parse_options(dag_run)
            tags_to_enable = []
            tags_to_create = []
            tags_to_update = []

            for tag in required_tags:
                tag_found_by_code = rail.find_first_by_attr_and_get_attr(
                    existing_tags, 'code', tag['code'])

                tag_found_by_name = None
                if not tag_found_by_code:
                    tag_found_by_name = rail.find_first_by_attr_and_get_attr(
                        existing_tags, 'name', tag['name'])

                tag_found = tag_found_by_code or tag_found_by_name

                if tag_found:
                    if tag_found['name'] != tag['name'] or tag_found['code'] != tag['code']:
                        tags_to_update.append({
                            'uri': tag_found['uri'],
                            'newname': get_oef_name(tag, existing_tags, dag_run.conf['options']),
                            'newcode': tag['code']
                        })
                    tags_to_enable.append(tag_found)
                else:
                    tags_to_create.append({
                        **tag,
                        'name': get_oef_name(tag, existing_tags, dag_run.conf['options'])
                    })

            matched_uris = {tag['uri'] for tag in tags_to_enable}
            tags_to_disable = [tag for tag in existing_tags if tag['uri'] not in matched_uris]

            return {
                'tags_to_enable': tags_to_enable,
                'tags_to_create': tags_to_create,
                'tags_to_update': tags_to_update,
                'tags_to_disable': tags_to_disable
            }

        get_tags_to_create_and_enabled = rail.PythonOperator(
            task_id='get_tags_to_create_and_enabled',
            python_callable=get_tags_to_create_and_enable
        )

        created_tags_list = rail.SetVariableOperator(
            task_id='created_tags_list',
            append=False,
            name='created_tags_list',
            value=[]
        )

        foreach_tag_to_create = rail.ForEachOperator(
            task_id='foreach_tag_to_create',
            items="{{result('get_tags_to_create_and_enabled').tags_to_create | to_json}}",
            start_task='create_tag_draft',
            end_task='foreach_tag_to_create_end'
        )

        create_tag_draft = rail.RepliconServiceOperator(
            task_id='create_tag_draft',
            endpoint="services/ObjectExtensionTagService1.svc/CreateNewDraft",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda dag_run: {
                "objectExtensionTagDefinitionUri": dag_run.conf['definition_uri']
            }
        )

        update_tag_name = rail.RepliconServiceOperator(
            task_id="update_tag_name",
            endpoint="services/ObjectExtensionTagService1.svc/UpdateName",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_tag_draft'),
                "name": rail.result('foreach_tag_to_create')['name']
            },
        )

        update_tag_code = rail.RepliconServiceOperator(
            task_id="update_tag_code",
            endpoint="services/ObjectExtensionTagService1.svc/UpdateCode",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_tag_draft'),
                "code": rail.result('foreach_tag_to_create')['code']
            },
        )

        publish_tag_draft = rail.RepliconServiceOperator(
            task_id="publish_tag_draft",
            endpoint="services/ObjectExtensionTagService1.svc/PublishDraft",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_tag_draft')
            },
        )

        insert_tag_to_list = rail.SetVariableOperator(
            task_id='insert_tag_to_list',
            append=True,
            name='created_tags_list',
            value=lambda: {
                "target": {
                    "uri": rail.result('publish_tag_draft')['uri']
                },
                "name": rail.result('foreach_tag_to_create')['name'],
                "code": rail.result('foreach_tag_to_create')['code'],
                "description": null,
                "isEnabled": "true"
            }
        )

        foreach_tag_to_create_end = rail.EmptyOperator(
            task_id='foreach_tag_to_create_end'
        )

        if_tags_to_create = rail.IfOperator(
            task_id='if_tags_to_create',
            test=lambda: len(rail.result('get_tags_to_create_and_enabled')[
                             'tags_to_create']) > 0,
            yes_task='put_oef_tags',
            no_task='if_tags_to_update'
        )

        put_oef_tags = rail.RepliconServiceOperator(
            task_id='put_oef_tags',
            endpoint='services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda dag_run: {
                "objectExtensionTagDefinition": {
                    "uri": dag_run.conf['definition_uri']
                },
                "objectExtensionTags": rail.get_dag_run_var('created_tags_list')
            }
        )

        if_tags_to_update = rail.IfOperator(
            task_id='if_tags_to_update',
            test=lambda: len(rail.result('get_tags_to_create_and_enabled')['tags_to_update']) > 0,
            yes_task='foreach_tag_to_update',
            no_task='enable_tags'
        )

        foreach_tag_to_update = rail.ForEachOperator(
            task_id='foreach_tag_to_update',
            items="{{result('get_tags_to_create_and_enabled').tags_to_update | to_json}}",
            start_task='if_name_changed',
            end_task='foreach_tag_to_update_end'
        )

        if_name_changed = rail.IfOperator(
            task_id='if_name_changed',
            test=lambda: 'newname' in rail.result('foreach_tag_to_update'),
            yes_task='update_tag_name_for_existing',
            no_task='if_code_changed'
        )

        update_tag_name_for_existing = rail.RepliconServiceOperator(
            task_id='update_tag_name_for_existing',
            endpoint='services/ObjectExtensionTagService1.svc/UpdateName',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda: {
                "objectExtensionTagUri": rail.result('foreach_tag_to_update')['uri'],
                "name": rail.result('foreach_tag_to_update')['newname']
            }
        )

        if_code_changed = rail.IfOperator(
            task_id='if_code_changed',
            test=lambda: 'newcode' in rail.result('foreach_tag_to_update'),
            yes_task='update_tag_code_for_existing',
            no_task='foreach_tag_to_update_end'
        )

        update_tag_code_for_existing = rail.RepliconServiceOperator(
            task_id='update_tag_code_for_existing',
            endpoint='services/ObjectExtensionTagService1.svc/UpdateCode',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda: {
                "objectExtensionTagUri": rail.result('foreach_tag_to_update')['uri'],
                "code": rail.result('foreach_tag_to_update')['newcode']
            }
        )

        foreach_tag_to_update_end = rail.EmptyOperator(
            task_id='foreach_tag_to_update_end'
        )

        enable_tags = rail.RepliconServiceCallForEachItemOperator(
            task_id='enable_tags',
            items=lambda: rail.result('get_tags_to_create_and_enabled')[
                'tags_to_enable'],
            endpoint='services/ObjectExtensionTagService1.svc/Enable',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda item: {
                "objectExtensionTagUri": item['uri']
            }
        )

        disable_tags = rail.RepliconServiceCallForEachItemOperator(
            task_id='disable_tags',
            items=lambda: rail.result('get_tags_to_create_and_enabled')[
                'tags_to_disable'],
            endpoint='services/ObjectExtensionTagService1.svc/Disable',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda item: {
                "objectExtensionTagUri": item['uri']
            }
        )

        def get_downstreamtasks_error(error_message):
            return {
                'error': f'Error in tag options update - {error_message}'
            }

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ get_error_message() }}']
        )

        batch_task >> catch_error
        batch_task >> get_existing_tags >> get_tags_to_create_and_enabled >> created_tags_list >> foreach_tag_to_create >> create_tag_draft
        create_tag_draft >> update_tag_name >> update_tag_code >> publish_tag_draft >> insert_tag_to_list >> foreach_tag_to_create_end
        foreach_tag_to_create >> foreach_tag_to_create_end
        foreach_tag_to_create_end >> if_tags_to_create >> rail.Label(
            'Yes') >> put_oef_tags >> if_tags_to_update
        if_tags_to_create >> rail.Label('No') >> if_tags_to_update

        if_tags_to_update >> rail.Label('Yes') >> foreach_tag_to_update >> if_name_changed
        if_name_changed >> rail.Label('Yes') >> update_tag_name_for_existing >> if_code_changed
        if_name_changed >> rail.Label('No') >> if_code_changed
        if_code_changed >> rail.Label('Yes') >> update_tag_code_for_existing >> foreach_tag_to_update_end
        if_code_changed >> rail.Label('No') >> foreach_tag_to_update_end
        foreach_tag_to_update >> foreach_tag_to_update_end
        foreach_tag_to_update_end >> enable_tags

        if_tags_to_update >> rail.Label('No') >> enable_tags >> disable_tags >> catch_error

        return dag

rail.for_each_instance(create_dag)
