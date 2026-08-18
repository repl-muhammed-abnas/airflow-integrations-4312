from datetime import timedelta
import uuid
from airflow.models import Variable
import rail

# pylint:disable = too-many-statements
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_paycode_type_child_{config.instance}',
        description=f'deltek_costpoint_paycode_type_child_{config.instance}',
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
            no_task='get_modified_paycodes_from_costpoint'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_modified_paycodes_from_costpoint',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_paycode_from_costpoint(task_name):
            company_paycodes_obj = rail.result(
                task_name) if rail.result(task_name) else None
            modified_paycodes = []
            for company_paycodes in company_paycodes_obj:
                if company_paycodes['document']['rows']:
                    for paycode in company_paycodes['document']['rows']:
                        if (len(modified_paycodes) == 0 or
                                rail.find_first_by_attr_and_get_attr(modified_paycodes,
                                                                     "paycode_code", paycode['row']['data'].get('PAY_TYPE'), "paycode_name", None) is None):
                            if rail.find_first_by_attr_and_get_attr(modified_paycodes,
                                                                    "paycode_name", paycode['row']['data'].get('PAY_TYPE_DESC'), "paycode_name", None):

                                modified_paycodes.append({
                                    "paycode_name": paycode['row']['data'].get('PAY_TYPE_DESC') + "_" + paycode['row']['data'].get('PAY_TYPE'),
                                    "paycode_code": paycode['row']['data'].get('PAY_TYPE'),
                                    "paycode_multiplier": paycode['row']['data'].get('PAY_TYPE_FCTR_QTY')
                                })
                            else:
                                modified_paycodes.append({
                                    "paycode_name": paycode['row']['data'].get('PAY_TYPE_DESC'),
                                    "paycode_code": paycode['row']['data'].get('PAY_TYPE'),
                                    "paycode_multiplier": paycode['row']['data'].get('PAY_TYPE_FCTR_QTY')
                                })

            return modified_paycodes

        get_modified_paycodes_from_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='get_modified_paycodes_from_costpoint',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_cospoint_company_ids,
            data=lambda dag_run: {
                "filter": {
                    "id": "replicon_exp_paytype",
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

        get_modified_paycodes = rail.PythonOperator(
            task_id='get_modified_paycodes',
            python_callable=lambda: get_paycode_from_costpoint(
                'get_modified_paycodes_from_costpoint')
        )

        def filter_paycode_list(response):
            return list(map(lambda row:
                            {
                                "uri": row["cells"][1]["uri"],
                                "code": row["cells"][0].get('textValue'),
                                "name": row["cells"][1].get('textValue'),
                                "multiplier": row["cells"][2].get('textValue'),
                                "description": row['cells'][3].get('textValue')
                            }, response.json()["d"]["rows"]))

        get_all_paycodelist = rail.RepliconServiceOperator(
            task_id='get_all_paycodelist',
            endpoint="/services/PayCodeListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:pay-code-list-column:code",
                    "urn:replicon:pay-code-list-column:name",
                    "urn:replicon:pay-code-list-column:multiplier",
                    "urn:replicon:pay-code-list-column:description"
                ],
                "sort": [],
                "filterExpression": null
            },
            response_filter=filter_paycode_list
        )

        foreach_paycode_flow = rail.ForEachOperator(
            task_id='foreach_paycode_flow',
            items="{{ result('get_modified_paycodes') | to_json }}",
            start_task='create_paycode_or_apply_modifications',
            end_task='foreach_paycode_flow_end'
        )

        def get_paycode_target():
            all_paycodes = rail.result('get_all_paycodelist')
            paycode_info = list(
                filter(lambda x: x['code'] == rail.result('foreach_paycode_flow')['paycode_code'], all_paycodes))
            return {
                "uri": paycode_info[0]['uri'],
                "name": null
            } if paycode_info and len(paycode_info) > 0 else None

        def get_paycode_name(paycode_name, paycode_code):
            existing_paycodes = rail.result('get_all_paycodelist') if rail.result(
                'get_all_paycodelist') else []
            existing_paycodes_by_name = list(
                filter(lambda x: x['name'] == paycode_name and x['code'] != paycode_code, existing_paycodes))
            return paycode_name + "_" + paycode_code if existing_paycodes_by_name \
                and len(existing_paycodes_by_name) > 0 else paycode_name

        def get_paycode_request():
            return {
                "target": get_paycode_target(),
                "modifications": {
                    "nameToApply": {
                        "value": get_paycode_name(rail.result('foreach_paycode_flow')['paycode_name'],
                                                  rail.result('foreach_paycode_flow')['paycode_code'])
                    },
                    "codeToApply": {
                        "value": rail.result('foreach_paycode_flow')['paycode_code']
                    },
                    "descriptionToApply": {
                        "value": rail.result('foreach_paycode_flow')['paycode_name']
                    },
                    "multiplierToApply": rail.result('foreach_paycode_flow')['paycode_multiplier'],
                    "payCodeTypeUriToApply": "urn:replicon:pay-code-type:none"
                },
                "payCodeModificationOptionUri": "urn:replicon:paycode-modification-option:save",
                "unitOfWorkId": rail.result('foreach_paycode_flow')['paycode_name'] + str(uuid.uuid4())
            }

        create_paycode_or_apply_modifications = rail.RepliconServiceOperator(
            task_id='create_paycode_or_apply_modifications',
            endpoint='/services/PayCodeService1.svc/CreatePayCodeOrApplyModifications',
            data=get_paycode_request
        )

        paycode_logs_add_entry = rail.WriteLogOperator(
            task_id='paycode_logs_add_entry',
            message="na",
            severity="Success",
            properties=lambda: {
                "paycode": rail.result('foreach_paycode_flow')['paycode_name'],
                "action": "Add / Update",
                "status": "Succeeded",
                "reason": ""
            }
        )

        foreach_paycode_flow_end = rail.EmptyOperator(
            task_id='foreach_paycode_flow_end',
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "entity": "paycode",
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
            'No') >> get_modified_paycodes_from_costpoint >> get_modified_paycodes >> get_all_paycodelist >> \
            foreach_paycode_flow >> create_paycode_or_apply_modifications >> \
            paycode_logs_add_entry >> foreach_paycode_flow_end
        foreach_paycode_flow >> foreach_paycode_flow_end >> catch_and_log_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
