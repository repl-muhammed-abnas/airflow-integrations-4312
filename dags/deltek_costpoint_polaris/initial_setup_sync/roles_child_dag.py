from datetime import timedelta
from airflow.models import Variable
import rail

# pylint:disable = too-many-statements
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_roles_child_{config.instance}',
        description=f'deltek_costpoint_roles_child_{config.instance}',
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
            no_task='get_modified_plc_from_costpoint'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_modified_plc_from_costpoint',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
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

        def get_plc_details(task_name, name, code, description):
            company_oef_obj = rail.result(
                task_name) if rail.result(task_name) else None
            modified_oefs = []
            for company_oefs in company_oef_obj:
                if company_oefs['document']['rows']:
                    for oef in company_oefs['document']['rows']:
                        if (len(modified_oefs) == 0 or
                                rail.find_first_by_attr_and_get_attr(modified_oefs,
                                                                     "plc_code", oef['row']['data'].get(code), "plc_name", None) is None):
                            if rail.find_first_by_attr_and_get_attr(modified_oefs,
                                                                    "plc_name", oef['row']['data'].get(name), "plc_name", None):
                                modified_oefs.append({
                                    "plc_name": oef['row']['data'].get(name)+"_" + oef['row']['data'].get(code),
                                    "plc_code": oef['row']['data'].get(code),
                                    "plc_description": oef['row']['data'].get(description)
                                })
                            else:
                                modified_oefs.append({
                                    "plc_name": oef['row']['data'].get(name),
                                    "plc_code": oef['row']['data'].get(code),
                                    "plc_description": oef['row']['data'].get(description)
                                })

            return modified_oefs

        get_modified_plc = rail.PythonOperator(
            task_id='get_modified_plc',
            python_callable=lambda: get_plc_details(
                'get_modified_plc_from_costpoint', 'BILL_LAB_CAT_DESC', 'BILL_LAB_CAT_CD', 'BILL_LAB_CAT_DESC')
        )

        if_costpoint_plc_present = rail.IfOperator(
            task_id='if_costpoint_plc_present',
            test="{{result('get_modified_plc') | length > 0 }}",
            yes_task="get_all_roles",
            no_task="catch_and_log_error",
        )

        def project_role_list_input(response):
            rows = response.json()['d']['rows']
            return list(map(lambda row: {
                "name": row['cells'][0].get('textValue'),
                "code": row['cells'][1].get('textValue'),
                "uri": row['cells'][2].get('uri')
            }, rows)) if rows else []

        get_all_roles = rail.RepliconServiceOperator(
            task_id='get_all_roles',
            endpoint="/services/ProjectRoleListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000000",
                "columnUris":  [
                    "urn:replicon:project-role-list-column:name",
                    "urn:replicon:project-role-list-column:description",
                    "urn:replicon:project-role-list-column:project-role"
                ],
                "sort": [],
                "filterExpression": null
            },
            response_filter=project_role_list_input
        )

        def get_role_name(role_name, role_code):
            existing_roles = rail.result('get_all_roles')
            roles_by_name = list(
                filter(lambda x: x['name'].lower() == role_name.lower() and
                       x['code'] != role_code, existing_roles))
            return role_name + "_" + role_code \
                if roles_by_name and len(roles_by_name) > 0 else role_name

        def get_missing_roles_callable():
            costpoint_roles = rail.result('get_modified_plc') or []
            polaris_roles = rail.result('get_all_roles') or []
            polaris_codes = {r['code'] for r in polaris_roles}
            return [r for r in costpoint_roles if r['plc_code'] not in polaris_codes]

        get_missing_roles = rail.PythonOperator(
            task_id='get_missing_roles',
            python_callable=get_missing_roles_callable
        )

        foreach_all_roles_flow = rail.ForEachOperator(
            task_id='foreach_all_roles_flow',
            items="{{ result('get_missing_roles') | to_json }}",
            start_task='add_role',
            end_task='foreach_all_roles_flow_end'
        )

        def get_target_request():
            existing_roles = rail.result('get_all_roles')
            existing_role_uri = rail.find_first_by_attr_and_get_attr(
                existing_roles, "code", rail.result('foreach_all_roles_flow')['plc_code'], "uri", None)
            if existing_role_uri:
                return {
                    "uri": existing_role_uri,
                    "name": None
                }
            return {
                "uri": null,
                "name": get_role_name(rail.result('foreach_all_roles_flow')['plc_name'], rail.result('foreach_all_roles_flow')['plc_code'])
            }

        add_role = rail.RepliconServiceOperator(
            task_id='add_role',
            endpoint="/services/ProjectRoleService1.svc/PutProjectRole",
            data=lambda: {
                "projectRoleUri": {
                    "target": get_target_request(),
                    "name": get_role_name(rail.result('foreach_all_roles_flow')['plc_name'], rail.result('foreach_all_roles_flow')['plc_code']),
                    "description": rail.result('foreach_all_roles_flow')['plc_code'],
                    "isArchived": "false",
                    "isBillable": "true",
                    "rateSchedule": null
                }
            }
        )

        def get_action():
            existing_roles = rail.result('get_all_roles')
            role_info = list(
                filter(lambda x: x['code'] == rail.result('foreach_all_roles_flow')['plc_code'], existing_roles))
            return "Update" if role_info else "Add"

        roles_logs_add_entry = rail.WriteLogOperator(
            task_id='roles_logs_add_entry',
            message="na",
            severity="Success",
            properties=lambda: {
                "rolename": rail.result('foreach_all_roles_flow')['plc_name'],
                "action": get_action(),
                "status": "Succeeded",
                "reason": ""
            }
        )

        foreach_all_roles_flow_end = rail.EmptyOperator(
            task_id='foreach_all_roles_flow_end',
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "entity": "Roles",
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
            'No') >> get_modified_plc_from_costpoint >> get_modified_plc >> if_costpoint_plc_present
        if_costpoint_plc_present >> get_all_roles >> get_missing_roles >> foreach_all_roles_flow >> add_role >> foreach_all_roles_flow_end
        foreach_all_roles_flow >> foreach_all_roles_flow_end >> roles_logs_add_entry >> \
            catch_and_log_error >> log_to_sumo
        if_costpoint_plc_present >> catch_and_log_error
        return dag


rail.for_each_instance(create_dag)
