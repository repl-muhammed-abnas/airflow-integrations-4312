from datetime import timedelta
from airflow.models import Variable
import rail

# pylint:disable = too-many-statements
# pylint:disable = unsubscriptable-object
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_oef_tag_update_child_{config.instance}',
        description=f'deltek_costpoint_oef_tag_update_child_{config.instance}',
        company_key=config.company_key,
        max_active_runs=config.child_dag_max_active_runs,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'deltek_costpoint_conn_id': config.deltek_cospoint_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_oefs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_user_oefs',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_oef_details(task_name, name, code, description):
            company_oef_obj = rail.result(
                task_name) if rail.result(task_name) else None
            modified_oefs = []
            for company_oefs in company_oef_obj:
                if company_oefs['document']['rows']:
                    for oef in company_oefs['document']['rows']:
                        if (len(modified_oefs) == 0 or
                                rail.find_first_by_attr_and_get_attr(modified_oefs,
                                                                     "group_code", oef['row']['data'].get(code), "group_name", None) is None):
                            if rail.find_first_by_attr_and_get_attr(modified_oefs,
                                                                    "group_name", oef['row']['data'].get(name), "group_name", None):
                                modified_oefs.append({
                                    "group_name": oef['row']['data'].get(name)+"_" + oef['row']['data'].get(code),
                                    "group_code": oef['row']['data'].get(code),
                                    "group_description": oef['row']['data'].get(description)
                                })
                            else:
                                modified_oefs.append({
                                    "group_name": oef['row']['data'].get(name),
                                    "group_code": oef['row']['data'].get(code),
                                    "group_description": oef['row']['data'].get(description)
                                })

            return modified_oefs

        def get_existing_tag_details(costpoint_modified_oef, tag):
            existing_entity = list(
                filter(lambda x: x['group_code'] == tag['code'], costpoint_modified_oef))
            return existing_entity[0] if existing_entity else None

        def is_tag_present(existing_tag_task_name, modified_tag):
            existing_tags = rail.result(existing_tag_task_name)
            return bool(rail.find_first_by_attr_and_get_attr(existing_tags, 'code', modified_tag['group_code'], 'uri', None))

        def get_formatted_data(response):
            tag_info = list(map(lambda row: {
                "name": row['cells'][0]['textValue'],
                "code": row['cells'][1].get('textValue'),
                "uri": row['cells'][3]['uri'],
                "is_enabled": row['cells'][4]['textValue']
            }, response['rows']))
            return tag_info if tag_info else None

        def get_unique_oef_name(current_oef_task_name, modifed_oef, current_oef_name):
            if modifed_oef:
                return get_new_off_name(current_oef_task_name, modifed_oef['group_name'], modifed_oef['group_code'])
            return current_oef_name

        def get_new_off_name(existing_oef_task, new_oef_name, new_oef_code):
            existing_oefs = rail.result(existing_oef_task) if rail.result(
                existing_oef_task) else []
            existing_oefs_by_name = list(filter(
                lambda x: x['name'] == new_oef_name and x['code'] != new_oef_code, existing_oefs))
            return new_oef_name + "_" + new_oef_code if existing_oefs_by_name \
                and len(existing_oefs_by_name) > 0 else new_oef_name

        get_user_oefs = rail.RepliconServiceOperator(
            task_id="get_user_oefs",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda oefs: {
                'generallabourcategories': rail.find_first_by_attr_and_get_attr(oefs, 'name', config.glc, 'uri'),
                'paytype': rail.find_first_by_attr_and_get_attr(oefs, 'name', config.paytype, 'uri'),
                'oeftaxableentity': rail.find_first_by_attr_and_get_attr(oefs, 'name', config.taxableentity, 'uri'),
                'oefemployeeclass': rail.find_first_by_attr_and_get_attr(oefs, 'name', config.empclass, 'uri'),
                'oefflsaexempt': rail.find_first_by_attr_and_get_attr(oefs, 'name', config.flsaexempt, 'uri'),
                'projectlaborcategory': rail.find_first_by_attr_and_get_attr(oefs, 'name', config.plc, 'uri')                
            },
        )

        get_modified_glc_from_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='get_modified_glc_from_costpoint',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_cospoint_company_ids,
            data=lambda dag_run: {
                "filter": {
                    "id": "polaris_exp_glc",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "LDMGLC_GENLLABCAT_HDR",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "LDMGLC_GENLLABCAT_HDR_LAST_MODIFIED",
                                                "relation": "gt=",
                                                "value": dag_run.conf['last_modified'],
                                            }
                                        ]
                                    }
                                ],
                                "children": [
                                ]
                            }
                        }
                    ]
                }
            }
        )

        get_modified_glc = rail.PythonOperator(
            task_id='get_modified_glc',
            python_callable=lambda: get_oef_details(
                'get_modified_glc_from_costpoint', 'GENL_LAB_CAT_DESC', 'GENL_LAB_CAT_CD', 'GENL_LAB_CAT_DESC')
        )

        if_costpoint_glc_present = rail.IfOperator(
            task_id='if_costpoint_glc_present',
            test="{{result('get_modified_glc') | length > 0 }}",
            yes_task="get_oef_tags_for_glc",
            no_task="get_modified_plc_from_costpoint",
        )

        get_oef_tags_for_glc = rail.RepliconServiceOperator(
            task_id="get_oef_tags_for_glc",
            endpoint="services/ObjectExtensionTagListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:object-extension-tag-list-column:name",
                    "urn:replicon:object-extension-tag-list-column:code",
                    "urn:replicon:object-extension-tag-list-column:description",
                    "urn:replicon:object-extension-tag-list-column:object-extension-tag",
                    "urn:replicon:object-extension-tag-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:definition"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": "{{ result('get_user_oefs').generallabourcategories }}",
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null,
                            "numberRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            data_handler=get_formatted_data
        )

        declare_list_1 = rail.SetVariableOperator(
            task_id='declare_list_1',
            append=False,
            name='glc_tags',
            value=[]
        )

        foreach_glc_oef_flow = rail.ForEachOperator(
            task_id='foreach_glc_oef_flow',
            items="{{ result('get_modified_glc') | to_json }}",
            start_task='if_glc_tag_present',
            end_task='foreach_glc_oef_flow_end'
        )

        if_glc_tag_present = rail.IfOperator(
            task_id='if_glc_tag_present',
            test=lambda: is_tag_present(
                'get_oef_tags_for_glc', rail.result('foreach_glc_oef_flow')),
            yes_task="foreach_glc_oef_flow_end",
            no_task="create_new_draft",
        )

        create_new_draft = rail.RepliconServiceOperator(
            task_id="create_new_draft",
            endpoint="services/ObjectExtensionTagService1.svc/CreateNewDraft",
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.result('get_user_oefs')['generallabourcategories']
            },
        )

        update_task_name = rail.RepliconServiceOperator(
            task_id="update_task_name",
            endpoint="services/ObjectExtensionTagService1.svc/UpdateName",
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_new_draft'),
                "name": get_new_off_name('get_oef_tags_for_glc',
                                         rail.result('foreach_glc_oef_flow')[
                                             'group_name'],
                                         rail.result('foreach_glc_oef_flow')['group_code'])
            },
        )

        update_task_code = rail.RepliconServiceOperator(
            task_id="update_task_code",
            endpoint="services/ObjectExtensionTagService1.svc/UpdateCode",
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_new_draft'),
                "code": rail.result('foreach_glc_oef_flow')['group_code']
            },
        )

        publish_draft = rail.RepliconServiceOperator(
            task_id="publish_draft",
            endpoint="services/ObjectExtensionTagService1.svc/PublishDraft",
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_new_draft')
            },
        )

        insert_to_list_1 = rail.SetVariableOperator(
            task_id='insert_to_list_1',
            append=True,
            name='{{ result("declare_list_1").name }}',
            value=lambda: {
                "code": rail.result('foreach_glc_oef_flow')['group_code'],
                "name": rail.result('publish_draft')['displayText'],
                "uri": rail.result('publish_draft')['uri']
            }
        )

        foreach_glc_oef_flow_end = rail.EmptyOperator(
            task_id='foreach_glc_oef_flow_end',
        )

        def get_oeftag_request():
            objectExtensionTags = []
            new_glc_tags = rail.get_dag_run_var(
                rail.result('declare_list_1')['name'])
            existing_tags = rail.result('get_oef_tags_for_glc') if rail.result(
                'get_oef_tags_for_glc') else []
            costpoint_modified_glc = rail.result(
                'get_modified_glc')
            existing_entity = []
            for tag in existing_tags:
                existing_entity = get_existing_tag_details(
                    costpoint_modified_glc, tag)
                objectExtensionTags.append({
                    "target": {
                        "uri": tag['uri']
                    },
                    "name": get_unique_oef_name('get_oef_tags_for_glc', existing_entity, tag['name']),
                    "code": existing_entity['group_code'] if existing_entity else tag['code'],
                    "description": null,
                    "isEnabled": 'true' if existing_entity else tag['is_enabled'].lower()
                })
            for tag in new_glc_tags:
                objectExtensionTags.append({
                    "target": {
                        "uri": tag['uri']
                    },
                    "name": tag['name'],
                    "code": tag['code'],
                    "description": null,
                    "isEnabled": "true"
                })

            return {
                "objectExtensionTagDefinition": {
                    "uri": rail.result('get_user_oefs')['generallabourcategories']
                },
                "objectExtensionTags": objectExtensionTags
            }

        add_new_glc_oef_tags = rail.RepliconServiceOperator(
            task_id="add_new_glc_oef_tags",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags",
            data=get_oeftag_request,
        )

        get_modified_plc_from_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='get_modified_plc_from_costpoint',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_cospoint_company_ids,
            data=lambda dag_run: {
                "filter": {
                    "id": "polaris_exp_plc",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "PJMPLC_BILLLABCAT_PLC",
                                "conditions": [
                                        {
                                            "joinWithParent": "N",
                                            "relations": [
                                                {
                                                    "name": "PJMPLC_BILLLABCAT_PLC_LAST_MODIFIED",
                                                    "relation": "gt=",
                                                    "value": dag_run.conf['last_modified'],
                                                }
                                            ]
                                        }
                                ],
                                "children": [
                                ]
                            }
                        }
                    ]
                }
            }
        )

        get_modified_plc = rail.PythonOperator(
            task_id='get_modified_plc',
            python_callable=lambda: get_oef_details(
                'get_modified_plc_from_costpoint', 'BILL_LAB_CAT_DESC', 'BILL_LAB_CAT_CD', 'BILL_LAB_CAT_DESC')
        )

        if_costpoint_plc_present = rail.IfOperator(
            task_id='if_costpoint_plc_present',
            test="{{result('get_modified_plc') | length > 0 }}",
            yes_task="get_oef_tags_for_plc",
            no_task="get_modified_empclass_from_costpoint",
        )

        get_oef_tags_for_plc = rail.RepliconServiceOperator(
            task_id="get_oef_tags_for_plc",
            endpoint="services/ObjectExtensionTagListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:object-extension-tag-list-column:name",
                    "urn:replicon:object-extension-tag-list-column:code",
                    "urn:replicon:object-extension-tag-list-column:description",
                    "urn:replicon:object-extension-tag-list-column:object-extension-tag",
                    "urn:replicon:object-extension-tag-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:definition"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": "{{ result('get_user_oefs').projectlaborcategory }}",
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null,
                            "numberRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            data_handler=get_formatted_data
        )

        declare_list_2 = rail.SetVariableOperator(
            task_id='declare_list_2',
            append=False,
            name='plc_tags',
            value=[]
        )

        foreach_plc_oef_flow = rail.ForEachOperator(
            task_id='foreach_plc_oef_flow',
            items="{{ result('get_modified_plc') | to_json }}",
            start_task='if_plc_tag_present',
            end_task='foreach_plc_oef_flow_end'
        )

        if_plc_tag_present = rail.IfOperator(
            task_id='if_plc_tag_present',
            test=lambda: is_tag_present(
                'get_oef_tags_for_plc', rail.result('foreach_plc_oef_flow')),
            yes_task="foreach_plc_oef_flow_end",
            no_task="create_new_draft_plc",
        )

        create_new_draft_plc = rail.RepliconServiceOperator(
            task_id="create_new_draft_plc",
            endpoint="services/ObjectExtensionTagService1.svc/CreateNewDraft",
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.result('get_user_oefs')['projectlaborcategory']
            },
        )

        update_task_name_plc = rail.RepliconServiceOperator(
            task_id="update_task_name_plc",
            endpoint="services/ObjectExtensionTagService1.svc/UpdateName",
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_new_draft_plc'),
                "name": get_new_off_name('get_oef_tags_for_plc',
                                         rail.result('foreach_plc_oef_flow')[
                                             'group_name'],
                                         rail.result('foreach_plc_oef_flow')['group_code'])
            },
        )

        update_task_code_glc = rail.RepliconServiceOperator(
            task_id="update_task_code_glc",
            endpoint="services/ObjectExtensionTagService1.svc/UpdateCode",
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_new_draft_plc'),
                "code": rail.result('foreach_plc_oef_flow')['group_code']
            },
        )

        publish_draft_plc = rail.RepliconServiceOperator(
            task_id="publish_draft_plc",
            endpoint="services/ObjectExtensionTagService1.svc/PublishDraft",
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_new_draft_plc')
            },
        )

        insert_to_list_2 = rail.SetVariableOperator(
            task_id='insert_to_list_2',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value=lambda: {
                "code": rail.result('foreach_plc_oef_flow')['group_code'],
                "name": rail.result('publish_draft_plc')['displayText'],
                "uri": rail.result('publish_draft_plc')['uri']
            }
        )

        foreach_plc_oef_flow_end = rail.EmptyOperator(
            task_id='foreach_plc_oef_flow_end',
        )

        def get_oeftag_plc_request():
            objectExtensionTags = []
            new_tags = rail.get_dag_run_var(
                rail.result('declare_list_2')['name'])
            existing_tags = rail.result('get_oef_tags_for_plc') if rail.result(
                'get_oef_tags_for_plc') else []
            costpoint_modified_plc = rail.result(
                'get_modified_plc')
            existing_entity = []
            for tag in existing_tags:
                existing_entity = get_existing_tag_details(
                    costpoint_modified_plc, tag)
                objectExtensionTags.append({
                    "target": {
                        "uri": tag['uri']
                    },
                    "name": get_unique_oef_name('get_oef_tags_for_plc', existing_entity, tag['name']),
                    "code": existing_entity['group_code'] if existing_entity else tag['code'],
                    "description": null,
                    "isEnabled": 'true' if existing_entity else tag['is_enabled'].lower()
                })
            for tag in new_tags:
                objectExtensionTags.append({
                    "target": {
                        "uri": tag['uri']
                    },
                    "name": tag['name'],
                    "code": tag['code'],
                    "description": null,
                    "isEnabled": "true"
                })

            return {
                "objectExtensionTagDefinition": {
                    "uri": rail.result('get_user_oefs')['projectlaborcategory']
                },
                "objectExtensionTags": objectExtensionTags
            }

        add_new_plc_oef_tags = rail.RepliconServiceOperator(
            task_id="add_new_plc_oef_tags",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags",
            data=get_oeftag_plc_request,
        )

        get_modified_empclass_from_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='get_modified_empclass_from_costpoint',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_cospoint_company_ids,
            data=lambda dag_run: {
                "filter": {
                    "id": "polaris_exp_empclass",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "LDMCLASS_EMPLCLASS_HDR",
                                "conditions": [
                                        {
                                            "joinWithParent": "N",
                                            "relations": [
                                                {
                                                    "name": "LDMCLASS_EMPLCLASS_HDR_LAST_MODIFIED",
                                                    "relation": "gt=",
                                                    "value": dag_run.conf['last_modified'],
                                                }
                                            ]
                                        }
                                ],
                                "children": [
                                ]
                            }
                        }
                    ]
                }
            }
        )

        get_modified_empclass = rail.PythonOperator(
            task_id='get_modified_empclass',
            python_callable=lambda: get_oef_details(
                'get_modified_empclass_from_costpoint', 'EMPL_CLASS_DESC', 'EMPL_CLASS_CD', 'EMPL_CLASS_DESC')
        )

        if_costpoint_empclas_present = rail.IfOperator(
            task_id='if_costpoint_empclas_present',
            test="{{result('get_modified_empclass') | length > 0 }}",
            yes_task="get_oef_tags_for_empclas",
            no_task="get_modified_paytype_from_costpoint",
        )

        get_oef_tags_for_empclas = rail.RepliconServiceOperator(
            task_id="get_oef_tags_for_empclas",
            endpoint="services/ObjectExtensionTagListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:object-extension-tag-list-column:name",
                    "urn:replicon:object-extension-tag-list-column:code",
                    "urn:replicon:object-extension-tag-list-column:description",
                    "urn:replicon:object-extension-tag-list-column:object-extension-tag",
                    "urn:replicon:object-extension-tag-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:definition"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": "{{ result('get_user_oefs').oefemployeeclass }}",
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null,
                            "numberRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            data_handler=get_formatted_data
        )

        declare_list_3 = rail.SetVariableOperator(
            task_id='declare_list_3',
            append=False,
            name='empclass_tags',
            value=[]
        )

        foreach_empclas_oef_flow = rail.ForEachOperator(
            task_id='foreach_empclas_oef_flow',
            items="{{ result('get_modified_empclass') | to_json }}",
            start_task='if_empclas_tag_present',
            end_task='foreach_empclas_oef_flow_end'
        )

        if_empclas_tag_present = rail.IfOperator(
            task_id='if_empclas_tag_present',
            test=lambda: is_tag_present(
                'get_oef_tags_for_empclas', rail.result('foreach_empclas_oef_flow')),
            yes_task="foreach_empclas_oef_flow_end",
            no_task="create_new_draft_empclas",
        )

        create_new_draft_empclas = rail.RepliconServiceOperator(
            task_id="create_new_draft_empclas",
            endpoint="services/ObjectExtensionTagService1.svc/CreateNewDraft",
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.result('get_user_oefs')['oefemployeeclass']
            },
        )

        update_task_name_empclas = rail.RepliconServiceOperator(
            task_id="update_task_name_empclas",
            endpoint="services/ObjectExtensionTagService1.svc/UpdateName",
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_new_draft_empclas'),
                "name": get_new_off_name('get_oef_tags_for_empclas',
                                         rail.result('foreach_empclas_oef_flow')[
                                             'group_name'],
                                         rail.result('foreach_empclas_oef_flow')['group_code'])
            },
        )

        update_task_code_empclas = rail.RepliconServiceOperator(
            task_id="update_task_code_empclas",
            endpoint="services/ObjectExtensionTagService1.svc/UpdateCode",
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_new_draft_empclas'),
                "code": rail.result('foreach_empclas_oef_flow')['group_code']
            },
        )

        publish_draft_empclas = rail.RepliconServiceOperator(
            task_id="publish_draft_empclas",
            endpoint="services/ObjectExtensionTagService1.svc/PublishDraft",
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_new_draft_empclas')
            },
        )

        insert_to_list_3 = rail.SetVariableOperator(
            task_id='insert_to_list_3',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value=lambda: {
                "code": rail.result('foreach_empclas_oef_flow')['group_code'],
                "name": rail.result('publish_draft_empclas')['displayText'],
                "uri": rail.result('publish_draft_empclas')['uri']
            }
        )

        foreach_empclas_oef_flow_end = rail.EmptyOperator(
            task_id='foreach_empclas_oef_flow_end',
        )

        def get_oeftag_empclass_request():
            objectExtensionTags = []
            new_empclass_tags = rail.get_dag_run_var(
                rail.result('declare_list_3')['name'])
            existing_empclass_tags = rail.result(
                'get_oef_tags_for_empclas') if rail.result('get_oef_tags_for_empclas') else []
            costpoint_modified_empclass = rail.result(
                'get_modified_empclass')
            existing_entity = []
            for tag in existing_empclass_tags:
                existing_entity = get_existing_tag_details(
                    costpoint_modified_empclass, tag)
                objectExtensionTags.append({
                    "target": {
                        "uri": tag['uri']
                    },
                    "name": get_unique_oef_name('get_oef_tags_for_empclas', existing_entity, tag['name']),
                    "code": existing_entity['group_code'] if existing_entity else tag['code'],
                    "description": null,
                    "isEnabled": 'true' if existing_entity else tag['is_enabled'].lower()
                })
            for tag in new_empclass_tags:
                objectExtensionTags.append({
                    "target": {
                        "uri": tag['uri']
                    },
                    "name": tag['name'],
                    "code": tag['code'],
                    "description": null,
                    "isEnabled": "true"
                })

            return {
                "objectExtensionTagDefinition": {
                    "uri": rail.result('get_user_oefs')['oefemployeeclass']
                },
                "objectExtensionTags": objectExtensionTags
            }

        add_new_empclas_oef_tags = rail.RepliconServiceOperator(
            task_id="add_new_empclas_oef_tags",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags",
            data=get_oeftag_empclass_request,
        )

        get_modified_paytype_from_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='get_modified_paytype_from_costpoint',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_cospoint_company_ids,
            data=lambda dag_run: {
                "filter": {
                    "id": "polaris_exp_paytype",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "LDMPAYTP_PAYTYPE",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "LDMPAYTP_PAYTYPE_LAST_MODIFIED",
                                                "relation": "gt=",
                                                "value": dag_run.conf['last_modified'],
                                            }
                                        ]
                                    }
                                ],
                                "children": [
                                ]
                            }
                        }
                    ]
                }
            }
        )

        def get_paytype_details(name, code, description, rate):
            company_oef_obj = rail.result(
                'get_modified_paytype_from_costpoint') if rail.result('get_modified_paytype_from_costpoint') else None
            modified_oefs = []
            for company_oefs in company_oef_obj:
                if company_oefs['document']['rows']:
                    for oef in company_oefs['document']['rows']:
                        if (len(modified_oefs) == 0 or
                                rail.find_first_by_attr_and_get_attr(modified_oefs,
                                                                     "group_code", oef['row']['data'].get(code), "group_name", None) is None):
                            modified_oefs.append({
                                "group_name": oef['row']['data'].get(name),
                                "group_code": oef['row']['data'].get(code),
                                "group_description": oef['row']['data'].get(description),
                                "group_rate": oef['row']['data'].get(rate),
                                "group_active": 'true'
                            })

            return modified_oefs

        get_modified_paytype = rail.PythonOperator(
            task_id='get_modified_paytype',
            python_callable=lambda: get_paytype_details(
                'PAY_TYPE_DESC', 'PAY_TYPE', 'PAY_TYPE_DESC', 'PAY_TYPE_FCTR_QTY')
        )

        if_costpoint_paytype_present = rail.IfOperator(
            task_id='if_costpoint_paytype_present',
            test="{{result('get_modified_paytype') | length > 0 }}",
            yes_task="get_oef_tags_for_paytype",
            no_task="get_modified_taxableentity_from_costpoint",
        )

        get_oef_tags_for_paytype = rail.RepliconServiceOperator(
            task_id="get_oef_tags_for_paytype",
            endpoint="services/ObjectExtensionTagListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:object-extension-tag-list-column:name",
                    "urn:replicon:object-extension-tag-list-column:code",
                    "urn:replicon:object-extension-tag-list-column:description",
                    "urn:replicon:object-extension-tag-list-column:object-extension-tag",
                    "urn:replicon:object-extension-tag-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:definition"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": "{{ result('get_user_oefs').paytype }}",
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null,
                            "numberRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            data_handler=get_formatted_data
        )

        declare_list_4 = rail.SetVariableOperator(
            task_id='declare_list_4',
            append=False,
            name='paytype_tags',
            value=[]
        )

        foreach_paytype_oef_flow = rail.ForEachOperator(
            task_id='foreach_paytype_oef_flow',
            items="{{ result('get_modified_paytype') | to_json }}",
            start_task='if_paytype_tag_present',
            end_task='foreach_paytype_oef_flow_end'
        )

        if_paytype_tag_present = rail.IfOperator(
            task_id='if_paytype_tag_present',
            test=lambda: is_tag_present(
                'get_oef_tags_for_paytype', rail.result('foreach_paytype_oef_flow')),
            yes_task="foreach_paytype_oef_flow_end",
            no_task="create_new_draft_paytype",
        )

        create_new_draft_paytype = rail.RepliconServiceOperator(
            task_id="create_new_draft_paytype",
            endpoint="services/ObjectExtensionTagService1.svc/CreateNewDraft",
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.result('get_user_oefs')['paytype']
            },
        )

        update_task_name_paytype = rail.RepliconServiceOperator(
            task_id="update_task_name_paytype",
            endpoint="services/ObjectExtensionTagService1.svc/UpdateName",
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_new_draft_paytype'),
                "name": get_new_off_name('get_oef_tags_for_paytype',
                                         rail.result('foreach_paytype_oef_flow')[
                                             'group_name'],
                                         rail.result('foreach_paytype_oef_flow')['group_code'])
            },
        )

        update_task_code_paytype = rail.RepliconServiceOperator(
            task_id="update_task_code_paytype",
            endpoint="services/ObjectExtensionTagService1.svc/UpdateCode",
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_new_draft_paytype'),
                "code": rail.result('foreach_paytype_oef_flow')['group_code']
            },
        )

        publish_draft_paytype = rail.RepliconServiceOperator(
            task_id="publish_draft_paytype",
            endpoint="services/ObjectExtensionTagService1.svc/PublishDraft",
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_new_draft_paytype')
            },
        )

        insert_to_list_4 = rail.SetVariableOperator(
            task_id='insert_to_list_4',
            append=True,
            name='{{ result("declare_list_4").name }}',
            value=lambda: {
                "code": rail.result('foreach_paytype_oef_flow')['group_code'],
                "name": rail.result('publish_draft_paytype')['displayText'],
                "uri": rail.result('publish_draft_paytype')['uri']
            }
        )

        foreach_paytype_oef_flow_end = rail.EmptyOperator(
            task_id='foreach_paytype_oef_flow_end',
        )

        def get_paytype_oeftag_request():
            objectExtensionTags = []
            new_tags = rail.get_dag_run_var(
                rail.result('declare_list_4')['name'])
            existing_tags = rail.result('get_oef_tags_for_paytype') if rail.result(
                'get_oef_tags_for_paytype') else []
            costpoint_modified_paytype = rail.result(
                'get_modified_paytype')
            existing_entity = []
            for tag in existing_tags:
                existing_entity = get_existing_tag_details(
                    costpoint_modified_paytype, tag)
                objectExtensionTags.append({
                    "target": {
                        "uri": tag['uri']
                    },
                    "name": get_unique_oef_name('get_oef_tags_for_paytype', existing_entity, tag['name']),
                    "code": existing_entity['group_code'] if existing_entity else tag['code'],
                    "description": null,
                    "isEnabled": 'true' if existing_entity else tag['is_enabled'].lower()
                })
            for tag in new_tags:
                objectExtensionTags.append({
                    "target": {
                        "uri": tag['uri']
                    },
                    "name": tag['name'],
                    "code": tag['code'],
                    "description": null,
                    "isEnabled": "true"
                })

            return {
                "objectExtensionTagDefinition": {
                    "uri": rail.result('get_user_oefs')['paytype']
                },
                "objectExtensionTags": objectExtensionTags
            }

        add_new_paytype_oef_tags = rail.RepliconServiceOperator(
            task_id="add_new_paytype_oef_tags",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags",
            data=get_paytype_oeftag_request,
        )

        get_modified_taxableentity_from_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='get_modified_taxableentity_from_costpoint',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_cospoint_company_ids,
            data=lambda dag_run: {
                "filter": {
                    "id": "polaris_exp_taxable",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "GLMCOMP_TAXBLEENTITY",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "GLMCOMP_TAXBLEENTITY_LAST_MODIFIED",
                                                "relation": "gt=",
                                                "value": dag_run.conf['last_modified'],
                                            }
                                        ]
                                    }
                                ],
                                "children": [
                                ]
                            }
                        }
                    ]
                }
            }
        )

        get_modified_taxableentity = rail.PythonOperator(
            task_id='get_modified_taxableentity',
            python_callable=lambda: get_oef_details(
                'get_modified_taxableentity_from_costpoint', 'TAXBLE_ENTITY_NAME', 'TAXBLE_ENTITY_ID', 'TAXBLE_ENTITY_NAME')
        )

        if_costpoint_taxableentity_present = rail.IfOperator(
            task_id='if_costpoint_taxableentity_present',
            test="{{result('get_modified_taxableentity') | length > 0 }}",
            yes_task="get_oef_tags_for_taxableentity",
            no_task="catch_and_log_error",
        )

        get_oef_tags_for_taxableentity = rail.RepliconServiceOperator(
            task_id="get_oef_tags_for_taxableentity",
            endpoint="services/ObjectExtensionTagListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:object-extension-tag-list-column:name",
                    "urn:replicon:object-extension-tag-list-column:code",
                    "urn:replicon:object-extension-tag-list-column:description",
                    "urn:replicon:object-extension-tag-list-column:object-extension-tag",
                    "urn:replicon:object-extension-tag-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:definition"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": "{{ result('get_user_oefs').oeftaxableentity }}",
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null,
                            "numberRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            data_handler=get_formatted_data
        )

        declare_list_5 = rail.SetVariableOperator(
            task_id='declare_list_5',
            append=False,
            name='taxableentity_tags',
            value=[]
        )

        foreach_taxableentity_oef_flow = rail.ForEachOperator(
            task_id='foreach_taxableentity_oef_flow',
            items="{{ result('get_modified_taxableentity') | to_json }}",
            start_task='if_taxableentity_tag_present',
            end_task='foreach_taxableentity_oef_flow_end'
        )

        if_taxableentity_tag_present = rail.IfOperator(
            task_id='if_taxableentity_tag_present',
            test=lambda: is_tag_present(
                'get_oef_tags_for_taxableentity', rail.result('foreach_taxableentity_oef_flow')),
            yes_task="foreach_taxableentity_oef_flow_end",
            no_task="create_new_draft_taxableentity",
        )

        create_new_draft_taxableentity = rail.RepliconServiceOperator(
            task_id="create_new_draft_taxableentity",
            endpoint="services/ObjectExtensionTagService1.svc/CreateNewDraft",
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.result('get_user_oefs')['oeftaxableentity']
            },
        )

        update_task_name_taxableentity = rail.RepliconServiceOperator(
            task_id="update_task_name_taxableentity",
            endpoint="services/ObjectExtensionTagService1.svc/UpdateName",
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_new_draft_taxableentity'),
                "name": get_new_off_name('get_oef_tags_for_taxableentity',
                                         rail.result('foreach_taxableentity_oef_flow')[
                                             'group_name'],
                                         rail.result('foreach_taxableentity_oef_flow')['group_code'])
            },
        )

        update_task_code_taxableentity = rail.RepliconServiceOperator(
            task_id="update_task_code_taxableentity",
            endpoint="services/ObjectExtensionTagService1.svc/UpdateCode",
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_new_draft_taxableentity'),
                "code": rail.result('foreach_taxableentity_oef_flow')['group_code']
            },
        )

        publish_draft_taxableentity = rail.RepliconServiceOperator(
            task_id="publish_draft_taxableentity",
            endpoint="services/ObjectExtensionTagService1.svc/PublishDraft",
            data=lambda: {
                "objectExtensionTagUri": rail.result('create_new_draft_taxableentity')
            },
        )

        insert_to_list_5 = rail.SetVariableOperator(
            task_id='insert_to_list_5',
            append=True,
            name='{{ result("declare_list_5").name }}',
            value=lambda: {
                "code": rail.result('foreach_taxableentity_oef_flow')['group_code'],
                "name": rail.result('publish_draft_taxableentity')['displayText'],
                "uri": rail.result('publish_draft_taxableentity')['uri']
            }
        )

        foreach_taxableentity_oef_flow_end = rail.EmptyOperator(
            task_id='foreach_taxableentity_oef_flow_end',
        )

        def get_taxableentity_oeftag_request():
            objectExtensionTags = []
            new_tags = rail.get_dag_run_var(
                rail.result('declare_list_5')['name'])
            existing_tags = rail.result('get_oef_tags_for_taxableentity') if rail.result(
                'get_oef_tags_for_taxableentity') else []
            costpoint_modified_taxableentity = rail.result(
                'get_modified_taxableentity')
            existing_entity = []
            for tag in existing_tags:
                existing_entity = get_existing_tag_details(
                    costpoint_modified_taxableentity, tag)
                objectExtensionTags.append({
                    "target": {
                        "uri": tag['uri']
                    },
                    "name": get_unique_oef_name('get_oef_tags_for_taxableentity', existing_entity, tag['name']),
                    "code": existing_entity['group_code'] if existing_entity else tag['code'],
                    "description": null,
                    "isEnabled": 'true' if existing_entity else tag['is_enabled'].lower()
                })
            for tag in new_tags:
                objectExtensionTags.append({
                    "target": {
                        "uri": tag['uri']
                    },
                    "name": tag['name'],
                    "code": tag['code'],
                    "description": null,
                    "isEnabled": "true"
                })

            return {
                "objectExtensionTagDefinition": {
                    "uri": rail.result('get_user_oefs')['oeftaxableentity']
                },
                "objectExtensionTags": objectExtensionTags
            }

        add_new_taxableentity_oef_tags = rail.RepliconServiceOperator(
            task_id="add_new_taxableentity_oef_tags",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/PutObjectExtensionTags",
            data=get_taxableentity_oeftag_request,
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "entity": "OEFTag",
                "action": "Add / Update",
                "status": "Error",
                "reason": "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_user_oefs >> get_modified_glc_from_costpoint >> get_modified_glc >> if_costpoint_glc_present
        if_costpoint_glc_present >> rail.Label(
            'No') >> get_modified_plc_from_costpoint
        if_costpoint_glc_present >> rail.Label('Yes') >> get_oef_tags_for_glc >> \
            declare_list_1 >> foreach_glc_oef_flow >> if_glc_tag_present
        if_glc_tag_present >> rail.Label('Yes') >> foreach_glc_oef_flow_end
        if_glc_tag_present >> rail.Label('No') >> create_new_draft >> update_task_name >> \
            update_task_code >> publish_draft >> insert_to_list_1 >> foreach_glc_oef_flow_end
        foreach_glc_oef_flow >> foreach_glc_oef_flow_end >> \
            add_new_glc_oef_tags >> get_modified_plc_from_costpoint >> get_modified_plc >> if_costpoint_plc_present
        if_costpoint_plc_present >> rail.Label(
            'No') >> get_modified_empclass_from_costpoint
        if_costpoint_plc_present >> rail.Label('Yes') >> get_oef_tags_for_plc >> \
            declare_list_2 >> foreach_plc_oef_flow >> if_plc_tag_present
        if_plc_tag_present >> rail.Label('Yes') >> foreach_plc_oef_flow_end
        if_plc_tag_present >> rail.Label('No') >> create_new_draft_plc >> update_task_name_plc >> \
            update_task_code_glc >> publish_draft_plc >> insert_to_list_2 >> foreach_plc_oef_flow_end
        foreach_plc_oef_flow >> foreach_plc_oef_flow_end >> \
            add_new_plc_oef_tags >> get_modified_empclass_from_costpoint >> get_modified_empclass >> if_costpoint_empclas_present
        if_costpoint_empclas_present >> rail.Label(
            'No') >> get_modified_paytype_from_costpoint
        if_costpoint_empclas_present >> rail.Label('Yes') >> get_oef_tags_for_empclas >> \
            declare_list_3 >> foreach_empclas_oef_flow >> if_empclas_tag_present
        if_empclas_tag_present >> rail.Label(
            'Yes') >> foreach_empclas_oef_flow_end
        if_empclas_tag_present >> rail.Label('No') >> create_new_draft_empclas >> update_task_name_empclas >> \
            update_task_code_empclas >> publish_draft_empclas >> insert_to_list_3 >> foreach_empclas_oef_flow_end
        foreach_empclas_oef_flow >> foreach_empclas_oef_flow_end >> \
            add_new_empclas_oef_tags >> get_modified_paytype_from_costpoint >> get_modified_paytype >> if_costpoint_paytype_present
        if_costpoint_paytype_present >> rail.Label(
            'No') >> get_modified_taxableentity_from_costpoint
        if_costpoint_paytype_present >> rail.Label('Yes') >> get_oef_tags_for_paytype >> \
            declare_list_4 >> foreach_paytype_oef_flow >> if_paytype_tag_present
        if_paytype_tag_present >> rail.Label(
            'Yes') >> foreach_paytype_oef_flow_end
        if_paytype_tag_present >> rail.Label('No') >> create_new_draft_paytype >> update_task_name_paytype >> \
            update_task_code_paytype >> publish_draft_paytype >> insert_to_list_4 >> foreach_paytype_oef_flow_end
        foreach_paytype_oef_flow >> foreach_paytype_oef_flow_end >> \
            add_new_paytype_oef_tags >> get_modified_taxableentity_from_costpoint >> get_modified_taxableentity >> if_costpoint_taxableentity_present
        if_costpoint_taxableentity_present >> rail.Label(
            'No') >> catch_and_log_error
        if_costpoint_taxableentity_present >> rail.Label('Yes') >> get_oef_tags_for_taxableentity >> \
            declare_list_5 >> foreach_taxableentity_oef_flow >> if_taxableentity_tag_present
        if_taxableentity_tag_present >> rail.Label(
            'Yes') >> foreach_taxableentity_oef_flow_end
        if_taxableentity_tag_present >> rail.Label('No') >> create_new_draft_taxableentity >> update_task_name_taxableentity >> \
            update_task_code_taxableentity >> publish_draft_taxableentity >> insert_to_list_5 >> foreach_taxableentity_oef_flow_end
        foreach_taxableentity_oef_flow >> foreach_taxableentity_oef_flow_end >> \
            add_new_taxableentity_oef_tags >> catch_and_log_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
