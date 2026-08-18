
from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'mpmq_disable_users_momentivequartz_put_remaining_balance_for_payout_{config.instance}',
        description=f'MomentiveQuartz _Put remaining balance for payout {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='declare_variable_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_variable_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_variable_3 = rail.SetVariableOperator(
            task_id='declare_variable_3',
            append=False,
            name='balance_amount',
            value=lambda: round(float(rail.get_dag_run_conf()['balance']), 2)
        )

        update_variable_5 = rail.SetVariableOperator(
            task_id='update_variable_5',
            append=False,
            name='{{ result("declare_variable_3").name }}',
            value=lambda: rail.result('declare_variable_3')['value'] if rail.result('declare_variable_3')[
                'value'] % 0.5 == 0 else rail.result('declare_variable_3')['value'] + 1
            if int(str(rail.result('declare_variable_3')['value']).rsplit('.', maxsplit=1)[-1]) > 50 else float(str(rail.result(
                'declare_variable_3')['value']).rsplit('.', maxsplit=1)[0] + '.50')
        )

        invoke_custom_ruby_code_6 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_6',
            python_callable=lambda: rail.get_dag_run_conf()['terminationdate']
        )

        getassignedpolicyforthetimeofftype_7 = rail.RepliconServiceOperator(
            task_id='getassignedpolicyforthetimeofftype_7',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        parse_json_9 = rail.PythonOperator(
            task_id='parse_json_9',
            python_callable=lambda: list(map(lambda item: item['policySetSchedule'], filter(lambda item: item["timeOffType"]['uri'] == rail.get_dag_run_conf()[
                                         'timeoffuri'], rail.result('getassignedpolicyforthetimeofftype_7')['policiesByTimeOffType'])))
        )

        if_first_description_present_10 = rail.IfOperator(
            task_id='if_first_description_present_10',
            test='''{{ result('parse_json_9') | is_truthy }}''',
            yes_task="foreach_document_11",
            no_task="finish",
        )

        foreach_document_11 = rail.ForEachOperator(
            task_id='foreach_document_11',
            items=lambda: rail.result('parse_json_9'),
            start_task='foreach_foreach_document_11_12',
            end_task='foreach_document_11_end'
        )

        foreach_foreach_document_11_12 = rail.ForEachOperator(
            task_id='foreach_foreach_document_11_12',
            items=lambda: rail.result('foreach_document_11'),
            start_task='log_effectivedate_13',
            end_task='foreach_foreach_document_11_12_end'
        )

        log_effectivedate_13 = rail.PythonOperator(
            task_id='log_effectivedate_13',
            python_callable=lambda:  rail.result(
                'foreach_foreach_document_11_12')['effectiveDate']
        )

        if_to_dateformatmmddyyyy_to_time_less_than_dataworkato_service0fafa311requestterminationdateto_dateto_time_14 = rail.IfOperator(
            task_id='if_to_dateformatmmddyyyy_to_time_less_than_dataworkato_service0fafa311requestterminationdateto_dateto_time_14',
            test=lambda: datetime(**rail.result('log_effectivedate_13')
                                  ) < datetime(**rail.get_dag_run_conf()['terminationdate']),
            yes_task="accumulate_list_items_15",
            no_task="foreach_foreach_document_11_12_end",
        )

        accumulate_list_items_15 = rail.SetVariableOperator(
            task_id='accumulate_list_items_15',
            name='count',
            append=True,
            value={
                "count": "{{ result('foreach_foreach_document_11_12').description }}"
            }
        )

        foreach_foreach_document_11_12_end = rail.EmptyOperator(
            task_id='foreach_foreach_document_11_12_end',
        )

        foreach_document_11_end = rail.EmptyOperator(
            task_id='foreach_document_11_end',
        )

        log_repeatcount_16 = rail.PythonOperator(
            task_id='log_repeatcount_16',
            python_callable=lambda:  len((rail.result(
                'accumulate_list_items_15') or {}).get('value', []))
        )

        log_policysetfromthepast_17 = rail.PythonOperator(
            task_id='log_policysetfromthepast_17',
            python_callable=lambda: json.loads(json.dumps(rail.result('foreach_document_11')[0:rail.result(
                'log_repeatcount_16')]).replace('null', '"effective"').replace('"script"', '"scriptTarget"'))
        )

        log_policysetfromthepast_18 = rail.PythonOperator(
            task_id='log_policysetfromthepast_18',
            python_callable=lambda:  [
                rail.result('log_policysetfromthepast_17')]
        )

        log_policysetfromthepast_19 = rail.PythonOperator(
            task_id='log_policysetfromthepast_19',
            python_callable=lambda:  rail.result('log_policysetfromthepast_18')
        )

        if_to_s_contains_urn_20 = rail.IfOperator(
            task_id='if_to_s_contains_urn_20',
            test='''{{ result('log_policysetfromthepast_19')|to_json | matches('urn') }}''',
            yes_task="put_time_offpolicywithinitialbalanceasremainingbalance_21",
            no_task="log_forlogsmonitoringpurpose_23",
        )

        put_time_offpolicywithinitialbalanceasremainingbalance_21 = rail.RepliconServiceOperator(
            task_id='put_time_offpolicywithinitialbalanceasremainingbalance_21',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda: {
                "timeOffAccount": {
                    "userUri": rail.render_template("{{ dag_run.conf.useruri }}"),
                    "timeOffTypeUri": rail.render_template("{{ dag_run.conf.timeoffuri }}")
                },
                "policySetScheduleEntries": [
                    rail.result('log_policysetfromthepast_19')[0][0],
                    {
                        "effectiveDate": {
                            "year": rail.render_template("{{result('invoke_custom_ruby_code_6').year}}"),
                            "month": rail.render_template("{{result('invoke_custom_ruby_code_6').month}}"),
                            "day": rail.render_template("{{result('invoke_custom_ruby_code_6').day}}")
                        },
                        "description": rail.render_template("Effective on {{ result('invoke_custom_ruby_code_6').month }}/{{ result('invoke_custom_ruby_code_6').day }}/{{ result('invoke_custom_ruby_code_6').year }}"),
                        "policySet": {
                            "timeOffBalanceEventScripts": [
                                {
                                    "scriptTarget": {
                                        "uri": rail.render_template("{{ dag_run.conf.startingbalancesettouri }}"),
                                        "slug": null,
                                        "name": null
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:amount",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": rail.render_template("{{ result('declare_variable_3').value }}"),
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                                            "value": {
                                                "uri": null,
                                                "slug": null,
                                                "bool": null,
                                                "date": null,
                                                "number": "20",
                                                "text": null,
                                                "time": null,
                                                "calendarDayDurationValue": null,
                                                "workdayDurationValue": null,
                                                "dateRange": null,
                                                "collection": []
                                            }
                                        }
                                    ]
                                }
                            ],
                            "timeOffValidationScripts": []
                        }
                    }
                ]
            }
        )

        log_forlogsmonitoringpurpose_23 = rail.PythonOperator(
            task_id='log_forlogsmonitoringpurpose_23',
            python_callable=lambda:  "No policy, hence no 0 balance required"
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> declare_variable_3
        declare_variable_3 >> update_variable_5 >> invoke_custom_ruby_code_6 >> getassignedpolicyforthetimeofftype_7 >> parse_json_9 >> if_first_description_present_10
        if_first_description_present_10 >> rail.Label(
            'Yes') >> foreach_document_11 >> foreach_foreach_document_11_12 >> log_effectivedate_13 >> if_to_dateformatmmddyyyy_to_time_less_than_dataworkato_service0fafa311requestterminationdateto_dateto_time_14
        if_to_dateformatmmddyyyy_to_time_less_than_dataworkato_service0fafa311requestterminationdateto_dateto_time_14 >> rail.Label(
            'Yes') >> accumulate_list_items_15 >> foreach_foreach_document_11_12_end
        if_to_dateformatmmddyyyy_to_time_less_than_dataworkato_service0fafa311requestterminationdateto_dateto_time_14 >> rail.Label(
            'No') >> foreach_foreach_document_11_12_end
        foreach_foreach_document_11_12 >> foreach_foreach_document_11_12_end >> foreach_document_11_end
        foreach_document_11 >> foreach_document_11_end >> log_repeatcount_16 >> log_policysetfromthepast_17 >> log_policysetfromthepast_18 >> log_policysetfromthepast_19 >> if_to_s_contains_urn_20
        if_to_s_contains_urn_20 >> rail.Label(
            'Yes') >> put_time_offpolicywithinitialbalanceasremainingbalance_21 >> finish
        if_to_s_contains_urn_20 >> rail.Label(
            'No') >> log_forlogsmonitoringpurpose_23 >> finish
        if_first_description_present_10 >> rail.Label(
            'No') >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
