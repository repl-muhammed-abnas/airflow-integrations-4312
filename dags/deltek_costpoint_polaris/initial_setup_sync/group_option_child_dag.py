from datetime import timedelta
import uuid
from airflow.models import Variable
import rail

# pylint:disable = too-many-statements
# pylint:disable = unsubscriptable-object
# pylint:disable = cell-var-from-loop
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_group_option_update_child_{config.instance}',
        description=f'deltek_costpoint_group_option_update_child_{config.instance}',
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
            no_task='if_run_servicecenter_sync'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_run_servicecenter_sync',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_run_servicecenter_sync = rail.IfOperator(
            task_id='if_run_servicecenter_sync',
            test=lambda: Variable.get(
                config.run_servicecenter_sync_var_name, default_var='true').lower() == 'true',
            yes_task='get_modified_servicecenter_countries_from_costpoint',
            no_task='if_run_labor_location_sync'
        )

        def get_value(data, index, pluck_key):
            return data['cells'][index].get(pluck_key)

        def filter_group_data(res):
            return list(
                map(lambda item:
                    {
                        'name': get_value(item, 0, 'textValue'),
                        'uri': get_value(item, 0, 'uri'),
                        'code': get_value(item, 1, 'textValue'),
                    }, res['rows'])
            )

        def get_group_details(task_name, name, code, description, activeflag=None, lvl_no=None, taxable_entity_name=None):
            company_countries_obj = rail.result(
                task_name) if rail.result(task_name) else None
            modified_groups = []
            for company_countries in company_countries_obj:
                if company_countries['document']['rows']:
                    for grp in company_countries['document']['rows']:
                        if config.group_hierarchy_sync == "HIERARCHICAL" and lvl_no and taxable_entity_name:
                            modified_groups.append({
                                    "group_name": grp['row']['data'].get(name),
                                    "group_code": grp['row']['data'].get(code),
                                    "group_description": grp['row']['data'].get(description),
                                    "group_active": grp['row']['data'].get(activeflag) if activeflag else "Y",
                                    "group_taxable_entity_code": company_countries['_company'],
                                    "group_lvl_no": grp['row']['data'].get(lvl_no) if lvl_no else None,
                                    "group_taxable_entity_name": grp['row']['data'].get(taxable_entity_name) if taxable_entity_name else None
                                })
                        elif (len(modified_groups) == 0 or
                                rail.find_first_by_attr_and_get_attr(modified_groups,
                                                                     "group_code", grp['row']['data'].get(code), "group_name", None) is None):
                            if rail.find_first_by_attr_and_get_attr(modified_groups,
                                                                    "group_name", grp['row']['data'].get(name), "group_name", None):
                                modified_groups.append({
                                    "group_name": grp['row']['data'].get(name)+"_" + grp['row']['data'].get(code),
                                    "group_code": grp['row']['data'].get(code),
                                    "group_description": grp['row']['data'].get(description),
                                    "group_active": grp['row']['data'].get(activeflag) if activeflag else "Y"
                                })
                            else:
                                modified_groups.append({
                                    "group_name": grp['row']['data'].get(name),
                                    "group_code": grp['row']['data'].get(code),
                                    "group_description": grp['row']['data'].get(description),
                                    "group_active": grp['row']['data'].get(activeflag) if activeflag else "Y"
                                })

            return modified_groups

        get_modified_servicecenter_countries_from_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='get_modified_servicecenter_countries_from_costpoint',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_cospoint_company_ids,
            data=lambda dag_run: {
                "filter": {
                    "id": "polaris_exp_country",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "SYMCNTRY_COUNTRY",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "SYMCNTRY_COUNTRY_LAST_MODIFIED",
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

        get_modified_servicecenter_countries = rail.PythonOperator(
            task_id='get_modified_servicecenter_countries',
            python_callable=lambda: get_group_details(
                'get_modified_servicecenter_countries_from_costpoint', 'COUNTRY_NAME', 'COUNTRY_CD', 'COUNTRY_NAME')
        )

        if_costpoint_servicecente_present = rail.IfOperator(
            task_id='if_costpoint_servicecente_present',
            test="{{result('get_modified_servicecenter_countries') | length > 0 }}",
            yes_task="get_all_service_centers",
            no_task="if_run_labor_location_sync",
        )

        if_run_labor_location_sync = rail.IfOperator(
            task_id='if_run_labor_location_sync',
            test=lambda: Variable.get(
                config.run_labor_location_sync_var_name, default_var='true').lower() == 'true',
            yes_task='get_modified_labor_location_from_costpoint',
            no_task='if_run_division_sync'
        )

        get_all_service_centers = rail.RepliconServiceOperator(
            task_id="get_all_service_centers",
            endpoint="services/ServiceCenterListService1.svc/GetData",
            data={
                    "page": "1",
                    "pagesize": "100000",
                    "columnUris": [
                        "urn:replicon:service-center-list-column:service-center",
                        "urn:replicon:service-center-list-column:code"
                    ]
            },
            data_handler=filter_group_data
        )

        def get_groups_request(costpoint_gropu_task, replicon_group_exsiting_task, department=False, supportsHierarchy=False):
            if config.group_hierarchy_sync == "HIERARCHICAL" and supportsHierarchy:
                return get_groups_request_with_hierarchy(costpoint_gropu_task, replicon_group_exsiting_task)
            target = null if department is False else {
                "parent": {"name": config.root_deperment}}
            groups_history = []
            for groups in rail.result(costpoint_gropu_task):
                existing_uri = rail.find_first_by_attr_and_get_attr(rail.result(
                    replicon_group_exsiting_task), 'code', groups['group_code'], 'uri')
                existing_groups = list(filter(
                    lambda x: x['name'] == groups['group_name']
                    and x['code'] != groups['group_code'], rail.result(replicon_group_exsiting_task)))

                groups_history.append(
                    {
                        "target": {"uri": existing_uri} if existing_uri else target,
                        "parameterCorrelationId": null,
                        "modificationToApply": {
                            "name": groups['group_name'] + "_" + groups['group_code']
                            if existing_groups and len(existing_groups) > 0 else groups['group_name'],
                            "codeToApply": {
                                "value": groups['group_code']
                            },
                            "descriptionToApply": {
                                "value": groups['group_description']
                            },
                            "isEnabled": "true" if groups['group_active'] == "Y" else "false"
                        }
                    }
                )
            return groups_history

        def get_groups_request_with_hierarchy(costpoint_gropu_task, replicon_group_exsiting_task):
            target = null
            groups_history = []
            seen_codes = set()

            replicon_groups = rail.result(replicon_group_exsiting_task)

            def is_already_added(code):
                return code in seen_codes

            def find_name_for_code(partial_code, sorted_by_lvl):
                matched = next(
                    (g for g in sorted_by_lvl if g['group_code'] == partial_code),
                    None
                )
                if matched:
                    return matched['group_name']

                matched_replicon = next(
                    (r for r in replicon_groups if r['code'] == partial_code),
                    None
                )
                if matched_replicon:
                    return matched_replicon['name']

                return None

            def build_target(target_name, split_codes, current_entity_name, sorted_by_lvl):
                if not split_codes:
                    return {"name": target_name}

                parent_names = []
                for i, _ in enumerate(split_codes):
                    if i == 0:
                        parent_names.append(current_entity_name)
                    else:
                        partial_code = ".".join(split_codes[:i + 1])
                        parent_names.append(find_name_for_code(partial_code, sorted_by_lvl))

                nested = None
                for name in parent_names:
                    if nested is None:
                        nested = {"name": name}
                    else:
                        nested = {"name": name, "parent": nested}

                return {"name": target_name, "parent": nested}

            sorted_by_entity = sorted(
                rail.result(costpoint_gropu_task),
                key=lambda x: (x['group_taxable_entity_name'] or "")
            )

            entity_groups = {}
            for group in sorted_by_entity:
                entity_name = group['group_taxable_entity_name']
                if entity_name not in entity_groups:
                    entity_groups[entity_name] = []
                entity_groups[entity_name].append(group)

            for entity_name, entity_group_list in entity_groups.items():
                sorted_by_lvl = sorted(entity_group_list, key=lambda x: (x['group_lvl_no'] or 0))

                for index, groups in enumerate(sorted_by_lvl):
                    if index == 0:
                        matching_entity = next(
                            (r for r in replicon_groups if r['code'] == groups['group_taxable_entity_code']),
                            None
                        )

                        if not is_already_added(groups['group_taxable_entity_code']):
                            seen_codes.add(groups['group_taxable_entity_code'])
                            groups_history.append({
                                "target": {"uri": matching_entity['uri']} if matching_entity else target,
                                "parameterCorrelationId": None,
                                "modificationToApply": {
                                    "name": groups['group_taxable_entity_name'],
                                    "codeToApply": {
                                        "value": groups['group_taxable_entity_code']
                                    },
                                    "descriptionToApply": {
                                        "value": groups['group_taxable_entity_name']
                                    },
                                    "isEnabled": "true"
                                }
                            })

                    split_codes = groups['group_code'].split('.')
                    parent_split_codes = split_codes[:-1]

                    matching_group = next(
                        (r for r in replicon_groups if r['code'] == groups['group_code']),
                        None
                    )

                    target_name = matching_group['name'] if matching_group else None
                    nested_target = build_target(target_name, parent_split_codes, entity_name, sorted_by_lvl)

                    if not is_already_added(groups['group_code']):
                        seen_codes.add(groups['group_code'])
                        groups_history.append({
                            "target": nested_target,
                            "parameterCorrelationId": None,
                            "modificationToApply": {
                                "name": groups['group_name'],
                                "codeToApply": {
                                    "value": groups['group_code']
                                },
                                "descriptionToApply": {
                                    "value": groups['group_description']
                                },
                                "isEnabled": "true" if groups['group_active'] == "Y" else "false"
                            }
                        })

            return groups_history

        create_servicecenter_or_applymodifications = rail.RepliconServiceOperator(
            task_id='create_servicecenter_or_applymodifications',
            endpoint='services/ServiceCenterService1.svc/CreateServiceCenterHierarchyOrApplyModifications',
            data=lambda: {
                "hierarchy": get_groups_request('get_modified_servicecenter_countries', 'get_all_service_centers'),
                "modificationOptionUri": null,
                "unitOfWorkId": rail.result('get_modified_servicecenter_countries')[0]['group_name'] + str(uuid.uuid4())
            }
        )

        get_modified_labor_location_from_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='get_modified_labor_location_from_costpoint',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_cospoint_company_ids,
            data=lambda dag_run: {
                "filter": {
                    "id": "polaris_exp_labor_location",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "LDMLLOC_LABLOCATION",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "LDMLLOC_LABLOCATION_LAST_MODIFIED",
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

        get_modified_labor_location = rail.PythonOperator(
            task_id='get_modified_labor_location',
            python_callable=lambda: get_group_details(
                'get_modified_labor_location_from_costpoint', 'LAB_LOC_DESC', 'LAB_LOC_CD', 'LAB_LOC_DESC')
        )

        if_costpoint_location_present = rail.IfOperator(
            task_id='if_costpoint_location_present',
            test="{{result('get_modified_labor_location') | length > 0 }}",
            yes_task="get_all_locations",
            no_task="if_run_division_sync",
        )

        if_run_division_sync = rail.IfOperator(
            task_id='if_run_division_sync',
            test=lambda: Variable.get(
                config.run_division_sync_var_name, default_var='true').lower() == 'true',
            yes_task='get_modified_division_organisations_from_costpoint',
            no_task='if_run_costcenter_sync'
        )

        get_all_locations = rail.RepliconServiceOperator(
            task_id='get_all_locations',
            endpoint='/services/LocationListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:location-list-column:location",
                    "urn:replicon:location-list-column:code"
                ]
            },
            data_handler=filter_group_data
        )

        create_location_or_applymodifications = rail.RepliconServiceOperator(
            task_id='create_location_or_applymodifications',
            endpoint='services/LocationService1.svc/CreateLocationHierarchyOrApplyModifications',
            data=lambda: {
                "hierarchy": get_groups_request('get_modified_labor_location', 'get_all_locations'),
                "modificationOptionUri": null,
                "unitOfWorkId": rail.result('get_modified_labor_location')[0]['group_name'] + str(uuid.uuid4())
            }
        )

        get_modified_division_organisations_from_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='get_modified_division_organisations_from_costpoint',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_cospoint_company_ids,
            data=lambda dag_run: {
                "filter": {
                    "id": "polaris_exp_org",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "GLMORMNT_ORG_PARENT",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "GLMORMNT_ORG_PARENT_LAST_MODIFIED",
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

        get_modified_division_organisations = rail.PythonOperator(
            task_id='get_modified_division_organisations',
            python_callable=lambda: get_group_details(
                'get_modified_division_organisations_from_costpoint', 'ORG_NAME', 'ORG_ID', 'ORG_NAME', 'ACTIVE_FL', 'LVL_NO', 'TAXBLE_ENTITY_NAME')
        )

        if_costpoint_division_present = rail.IfOperator(
            task_id='if_costpoint_division_present',
            test="{{result('get_modified_division_organisations') | length > 0 }}",
            yes_task="get_all_divisions",
            no_task="if_run_costcenter_sync",
        )

        if_run_costcenter_sync = rail.IfOperator(
            task_id='if_run_costcenter_sync',
            test=lambda: Variable.get(
                config.run_costcenter_sync_var_name, default_var='true').lower() == 'true',
            yes_task='get_modified_costcenter_accounts_from_costpoint',
            no_task='if_run_dept_glc_sync'
        )

        get_all_divisions = rail.RepliconServiceOperator(
            task_id="get_all_divisions",
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:division-list-column:division",
                    "urn:replicon:division-list-column:code"
                ]
            },
            data_handler=filter_group_data
        )

        create_division_or_applymodifications = rail.RepliconServiceOperator(
            task_id='create_division_or_applymodifications',
            endpoint='services/DivisionService1.svc/CreateDivisionHierarchyOrApplyModifications',
            data=lambda: {
                "hierarchy": get_groups_request('get_modified_division_organisations', 'get_all_divisions', False, True),
                "modificationOptionUri": null,
                "unitOfWorkId": rail.result('get_modified_division_organisations')[0]['group_name'] + str(uuid.uuid4())
            }
        )

        if_division_sync_error = rail.IfOperator(
            task_id='if_division_sync_error',
            test=lambda: any(entry.get('error') for entry in rail.result('create_division_or_applymodifications')),
            yes_task='log_division_sync_error',
            no_task='if_run_costcenter_sync'
        )

        log_division_sync_error = rail.WriteLogOperator(
            task_id='log_division_sync_error',
            message="{{ result('create_division_or_applymodifications') | selectattr('error') | list }}",
            severity="Error",
            properties={
                "entity": "divisions",
                "action": "Add / Update",
                "status": "Error",
                "reason": "One or more division hierarchy modifications failed"
            }
        )

        get_modified_costcenter_accounts_from_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='get_modified_costcenter_accounts_from_costpoint',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_cospoint_company_ids,
            data=lambda dag_run: {
                "filter": {
                    "id": "polaris_exp_act",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "GLMACT_ACCT_HDR",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "GLMACT_ACCT_HDR_LAST_MODIFIED",
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

        get_modified_costcenter_accounts = rail.PythonOperator(
            task_id='get_modified_costcenter_accounts',
            python_callable=lambda: get_group_details(
                'get_modified_costcenter_accounts_from_costpoint', 'ACCT_NAME', 'ACCT_ID', 'ACCT_NAME', 'ACTIVE_FL')
        )

        if_costpoint_costcenter_present = rail.IfOperator(
            task_id='if_costpoint_costcenter_present',
            test="{{result('get_modified_costcenter_accounts') | length > 0 }}",
            yes_task="get_all_cost_centers",
            no_task="if_run_dept_glc_sync",
        )

        if_run_dept_glc_sync = rail.IfOperator(
            task_id='if_run_dept_glc_sync',
            test=lambda: Variable.get(
                config.run_dept_glc_sync_var_name, default_var='true').lower() == 'true',
            yes_task='get_modified_dept_glc_from_costpoint',
            no_task='catch_and_log_error'
        )

        get_all_cost_centers = rail.RepliconServiceOperator(
            task_id="get_all_cost_centers",
            endpoint="services/CostCenterListService1.svc/GetData",
            data={
                    "page": "1",
                    "pagesize": "100000",
                    "columnUris": [
                        "urn:replicon:cost-center-list-column:cost-center",
                        "urn:replicon:cost-center-list-column:code"
                    ]
            },
            data_handler=filter_group_data
        )

        create_costcenter_or_applymodifications = rail.RepliconServiceCallForEachItemOperator(
            task_id='create_costcenter_or_applymodifications',
            endpoint='services/CostCenterService1.svc/CreateCostCenterHierarchyOrApplyModifications',
            items=lambda: [
                h[i:i + int(Variable.get(config.batch_size_var_name, default_var=400))]
                for h in [get_groups_request('get_modified_costcenter_accounts', 'get_all_cost_centers')]
                for i in range(0, len(h), int(Variable.get(config.batch_size_var_name, default_var=400)))
            ],
            data=lambda item: {
                "hierarchy": item,
                "modificationOptionUri": null,
                "unitOfWorkId": rail.result('get_modified_costcenter_accounts')[0]['group_name'] + str(uuid.uuid4())
            }
        )

        get_modified_dept_glc_from_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='get_modified_dept_glc_from_costpoint',
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

        get_modified_dept_glc = rail.PythonOperator(
            task_id='get_modified_dept_glc',
            python_callable=lambda: get_group_details(
                'get_modified_dept_glc_from_costpoint', 'GENL_LAB_CAT_DESC', 'GENL_LAB_CAT_CD', 'GENL_LAB_CAT_DESC')
        )

        if_costpoint_department_present = rail.IfOperator(
            task_id='if_costpoint_department_present',
            test="{{result('get_modified_dept_glc') | length > 0 }}",
            yes_task="get_all_departments",
            no_task="catch_and_log_error",
        )

        get_all_departments = rail.RepliconServiceOperator(
            task_id='get_all_departments',
            endpoint='/services/DepartmentGroupListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:code"
                ]
            },
            data_handler=filter_group_data
        )

        create_department_or_applymodifications = rail.RepliconServiceOperator(
            task_id='create_department_or_applymodifications',
            endpoint='services/DepartmentGroupService1.svc/CreateDepartmentGroupHierarchyOrApplyModifications',
            data=lambda: {
                "hierarchy": get_groups_request('get_modified_dept_glc', 'get_all_departments', True),
                "modificationOptionUri": null,
                "unitOfWorkId": rail.result('get_modified_dept_glc')[0]['group_name'] + str(uuid.uuid4())
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "entity": "groups",
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
            'No') >> if_run_servicecenter_sync
        if_run_servicecenter_sync >> rail.Label(
            'No') >> if_run_labor_location_sync
        if_run_servicecenter_sync >> rail.Label('Yes') >> get_modified_servicecenter_countries_from_costpoint >> \
            get_modified_servicecenter_countries >> if_costpoint_servicecente_present
        if_costpoint_servicecente_present >> rail.Label(
            'No') >> if_run_labor_location_sync
        if_costpoint_servicecente_present >> rail.Label('Yes') >> get_all_service_centers >> create_servicecenter_or_applymodifications >> \
            if_run_labor_location_sync
        if_run_labor_location_sync >> rail.Label(
            'No') >> if_run_division_sync
        if_run_labor_location_sync >> rail.Label('Yes') >> get_modified_labor_location_from_costpoint >> \
            get_modified_labor_location >> if_costpoint_location_present
        if_costpoint_location_present >> rail.Label(
            'No') >> if_run_division_sync
        if_costpoint_location_present >> rail.Label('Yes') >> get_all_locations >> create_location_or_applymodifications >> \
            if_run_division_sync
        if_run_division_sync >> rail.Label(
            'No') >> if_run_costcenter_sync
        if_run_division_sync >> rail.Label('Yes') >> get_modified_division_organisations_from_costpoint >> \
            get_modified_division_organisations >> if_costpoint_division_present
        if_costpoint_division_present >> rail.Label(
            'No') >> if_run_costcenter_sync
        if_costpoint_division_present >> rail.Label('Yes') >> get_all_divisions >> create_division_or_applymodifications >> \
            if_division_sync_error
        if_division_sync_error >> rail.Label('Yes') >> log_division_sync_error >> if_run_costcenter_sync
        if_division_sync_error >> rail.Label('No') >> if_run_costcenter_sync
        if_run_costcenter_sync >> rail.Label(
            'No') >> if_run_dept_glc_sync
        if_run_costcenter_sync >> rail.Label('Yes') >> get_modified_costcenter_accounts_from_costpoint >> \
            get_modified_costcenter_accounts >> if_costpoint_costcenter_present
        if_costpoint_costcenter_present >> rail.Label(
            'No') >> if_run_dept_glc_sync
        if_costpoint_costcenter_present >> rail.Label('Yes') >> get_all_cost_centers >> create_costcenter_or_applymodifications >> \
            if_run_dept_glc_sync
        if_run_dept_glc_sync >> rail.Label(
            'No') >> catch_and_log_error
        if_run_dept_glc_sync >> rail.Label('Yes') >> get_modified_dept_glc_from_costpoint >> \
            get_modified_dept_glc >> if_costpoint_department_present
        if_costpoint_department_present >> rail.Label(
            'No') >> catch_and_log_error
        if_costpoint_department_present >> rail.Label('Yes') >> get_all_departments >> create_department_or_applymodifications >> \
            catch_and_log_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
