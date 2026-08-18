from datetime import timedelta
import itertools
from airflow.models import Variable
import rail

from deltek_vantagepoint.user_sync.utils.python_callable_method import page_handler
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_vantagepoint_tag_oef_options_update_child_{config.instance}',
        description='Syncs the list of options for a tag OEF into Replicon',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'vp_conn_id': config.deltek_vantagepoint_conn_id
        }
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_existing_tags'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_existing_tags',
            end_task='log_to_sumo',
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
                            "uri": dag_run.conf['definition']
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
            oef_type = dag_run.conf['type']
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
            tags_to_disable = list(filter(lambda tag: (not rail.find_first_by_attr_and_get_attr(
                required_tags, 'code', tag['code'])), existing_tags))
            for tag in required_tags:
                tag_found = rail.find_first_by_attr_and_get_attr(
                    existing_tags, 'code', tag['code'])
                if tag_found:
                    if tag_found['name'] != tag['name']:
                        tags_to_update.append({
                            'uri': tag_found['uri'],
                            'newname': get_oef_name(tag, existing_tags, dag_run.conf['options'])
                        })
                    tags_to_enable.append(tag_found)
                else:
                    tags_to_create.append({
                        **tag,
                        'name': get_oef_name(tag, existing_tags, dag_run.conf['options'])
                    })
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
            data=lambda dag_run: {
                "objectExtensionTagDefinitionUri": dag_run.conf['definition']
            }
        )

        update_tag_name = rail.RepliconServiceOperator(
            task_id="update_tag_name",
            endpoint="services/ObjectExtensionTagService1.svc/UpdateName",
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_tag_draft'),
                "name": rail.result('foreach_tag_to_create')['name']
            },
        )

        update_tag_code = rail.RepliconServiceOperator(
            task_id="update_tag_code",
            endpoint="services/ObjectExtensionTagService1.svc/UpdateCode",
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_tag_draft'),
                "code": rail.result('foreach_tag_to_create')['code']
            },
        )

        publish_tag_draft = rail.RepliconServiceOperator(
            task_id="publish_tag_draft",
            endpoint="services/ObjectExtensionTagService1.svc/PublishDraft",
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
            no_task='update_tags'
        )

        put_oef_tags = rail.RepliconServiceOperator(
            task_id='put_oef_tags',
            endpoint='services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags',
            data=lambda dag_run: {
                "objectExtensionTagDefinition": {
                    "uri": dag_run.conf['definition']
                },
                "objectExtensionTags": rail.get_dag_run_var('created_tags_list')
            }
        )

        update_tags = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_tags',
            items=lambda: rail.result('get_tags_to_create_and_enabled')[
                'tags_to_update'],
            endpoint='services/ObjectExtensionTagService1.svc/UpdateName',
            data=lambda item: {
                "objectExtensionTagUri": item['uri'],
                "name": item['newname']
            }
        )

        enable_tags = rail.RepliconServiceCallForEachItemOperator(
            task_id='enable_tags',
            items=lambda: rail.result('get_tags_to_create_and_enabled')[
                'tags_to_enable'],
            endpoint='services/ObjectExtensionTagService1.svc/Enable',
            data=lambda item: {
                "objectExtensionTagUri": item['uri']
            }
        )

        disable_tags = rail.RepliconServiceCallForEachItemOperator(
            task_id='disable_tags',
            items=lambda: rail.result('get_tags_to_create_and_enabled')[
                'tags_to_disable'],
            endpoint='services/ObjectExtensionTagService1.svc/Disable',
            data=lambda item: {
                "objectExtensionTagUri": item['uri']
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_existing_tags >> get_tags_to_create_and_enabled >> created_tags_list >> foreach_tag_to_create >> create_tag_draft
        create_tag_draft >> update_tag_name >> update_tag_code >> publish_tag_draft >> insert_tag_to_list >> foreach_tag_to_create_end
        foreach_tag_to_create >> foreach_tag_to_create_end
        foreach_tag_to_create_end >> if_tags_to_create >> rail.Label(
            'Yes') >> put_oef_tags >> update_tags
        if_tags_to_create >> rail.Label(
            'No') >> update_tags >> enable_tags >> disable_tags >> log_to_sumo

        return dag

rail.for_each_instance(create_dag)
