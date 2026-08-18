from datetime import timedelta, datetime
import json
import re
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'momentive_annual_leave_policy_update_south_korea_child_dag_{config.instance}',
        description=f'Momentive Anual Leave Policy Update Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_timeoff_policies_schedule_list'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_timeoff_policies_schedule_list',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_timeoff_policies_schedule_list=rail.SetVariableOperator(
            task_id='create_timeoff_policies_schedule_list',
            append=False,
            name='timeoff polices schedule',
            value=[]
        )

        get_user_time_off_type_policy_summary=rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
              "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response,dag_run: list(filter
                          (lambda x: x['timeOffType']['uri'] == dag_run.conf['timeoffuri'], response['policiesByTimeOffType']))
        )

        foreach_item_in_policies_by_timeoff_type=rail.ForEachOperator(
            task_id='foreach_item_in_policies_by_timeoff_type',
            items=lambda: rail.result('get_user_time_off_type_policy_summary'),
            start_task = 'for_each_policy_set_schedule',
            end_task = 'foreach_item_in_policies_by_timeoff_type_end'
        )

        for_each_policy_set_schedule=rail.ForEachOperator(
            task_id='for_each_policy_set_schedule',
            items=lambda: rail.result('foreach_item_in_policies_by_timeoff_type')['policySetSchedule'],
            start_task = 'log_effective_date',
            end_task = 'for_each_policy_set_schedule_end'
        )

        def get_first_date_of_next_year():
            return datetime(((datetime.today() - timedelta(days=1)) + relativedelta(months = 12)).year,1,1).date()

        log_effective_date=rail.PythonOperator(
            task_id='log_effective_date',
            python_callable= lambda: str(rail.result('for_each_policy_set_schedule')['effectiveDate']['day']) + "/" +
                              str(rail.result('for_each_policy_set_schedule')['effectiveDate']['month']) + "/" +
                              str(rail.result('for_each_policy_set_schedule')['effectiveDate']['year'])
        )

        if_effective_date_not_equal_beginning_of_next_year=rail.IfOperator(
            task_id='if_effective_date_not_equal_beginning_of_next_year',
            test=lambda: bool(datetime.strptime(rail.result('log_effective_date'),"%d/%m/%Y") != datetime.strptime(
                          get_first_date_of_next_year().strftime('%d-%m-%Y'),'%d-%m-%Y')),
            yes_task="insert_to_timeoff_policies_schedule_list",
            no_task="for_each_policy_set_schedule_end",
        )

        insert_to_timeoff_policies_schedule_list=rail.SetVariableOperator(
            task_id='insert_to_timeoff_policies_schedule_list',
            append=True,
            name='{{ result("create_timeoff_policies_schedule_list").name }}',
            value=lambda: {
                "description": rail.result('for_each_policy_set_schedule')['description'],
                "policySet": rail.result('for_each_policy_set_schedule')['policySet'],
                "effectiveDate": {
                    "day": rail.result('for_each_policy_set_schedule')['effectiveDate']['day'],
                    "month": rail.result('for_each_policy_set_schedule')['effectiveDate']['month'],
                    "year": rail.result('for_each_policy_set_schedule')['effectiveDate']['year']
                }
            }
        )

        for_each_policy_set_schedule_end=rail.EmptyOperator(
            task_id='for_each_policy_set_schedule_end',
        )

        foreach_item_in_policies_by_timeoff_type_end=rail.EmptyOperator(
            task_id='foreach_item_in_policies_by_timeoff_type_end',
        )

        log_entitlement_calculation=rail.PythonOperator(
            task_id='log_entitlement_calculation',
            python_callable= lambda dag_run: 15 + int(float(dag_run.conf['tenure'])) - 2
        )

        add_to_timeoff_policies_schedule_list=rail.SetVariableOperator(
            task_id='add_to_timeoff_policies_schedule_list',
            append=True,
            name='{{ result("create_timeoff_policies_schedule_list").name }}',
            value=lambda: {
                "description": "Added by yearly run on " + (datetime.now() - timedelta(days=1)).strftime("%d/%m/%Y"),
                "policySet": {
                  "timeOffBalanceEventScripts": [
                    {
                      "additionalParameters": [
                        {
                          "keyUri": "urn:replicon:script-key:parameter:accrual-annual-amount",
                          "value": {
                            "number": rail.result('log_entitlement_calculation')
                          }
                        },
                        {
                          "keyUri": "urn:replicon:script-key:parameter:accrue-on-day-of-month",
                          "value": {
                            "uri": "urn:replicon:monthly-frequency-start-day-option:1st"
                          }
                        },
                        {
                          "keyUri": "urn:replicon:script-key:parameter:accrue-on-month",
                          "value": {
                            "uri": "urn:replicon:month:january"
                          }
                        },
                        {
                          "keyUri": "urn:replicon:script-key:parameter:precedence",
                          "value": {
                            "number": 30
                          }
                        },
                        {
                          "keyUri": "urn:replicon:script-key:parameter:proration-option",
                          "value": {
                            "uri": "urn:replicon:time-off-policy-proration-option:do-not-prorate"
                          }
                        }
                      ],
                      "script": {
                        "description": "Accrues time once per year.",
                        "name": "Yearly Accrual",
                        "uri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:script:80c105a0-a894-4c34-9cb8-27a828e81a96"
                      }
                    },
                    {
                      "additionalParameters": [
                        {
                          "keyUri": "urn:replicon:script-key:parameter:periodic-reset-option",
                          "value": {
                            "uri": "urn:replicon:time-off-policy-reset-option:reset-balance-to-specific-value"
                          }
                        },
                        {
                          "keyUri": "urn:replicon:script-key:parameter:precedence",
                          "value": {
                            "number": 20
                          }
                        },
                        {
                          "keyUri": "urn:replicon:script-key:parameter:reset-balance-amount",
                          "value": {
                            "number": 0
                          }
                        },
                        {
                          "keyUri": "urn:replicon:script-key:parameter:reset-on-day-of-month",
                          "value": {
                            "uri": "urn:replicon:monthly-frequency-start-day-option:1st"
                          }
                        },
                        {
                          "keyUri": "urn:replicon:script-key:parameter:reset-on-month",
                          "value": {
                            "uri": "urn:replicon:month:january"
                          }
                        }
                      ],
                      "script": {
                        "description": "Reset balance once a year",
                        "name": "Yearly Reset",
                        "uri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:script:18d3348d-d882-4ee9-8512-7241958d6829"
                      }
                    }
                  ],
                  "timeOffValidationScripts": []
                },
                "effectiveDate": {
                    "day": get_first_date_of_next_year().day,
                    "month": get_first_date_of_next_year().month,
                    "year": get_first_date_of_next_year().year
                }
            }
        )

        if_timeoff_policies_schedule_list_has_data=rail.IfOperator(
            task_id='if_timeoff_policies_schedule_list_has_data',
            test=lambda: rail.get_dag_run_var('timeoff polices schedule'),
            yes_task="log_final_policytobeassigned",
            no_task="log_to_sumo",
        )

        log_final_policytobeassigned=rail.PythonOperator(
            task_id='log_final_policytobeassigned',
            python_callable= lambda: re.sub(r"\"script\"","\"scriptTarget\"",json.dumps(rail.get_dag_run_var('timeoff polices schedule')))
        )

        put_user_time_off_account_policy_set_schedule=rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
              "timeOffAccount": {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUri": dag_run.conf['timeoffuri']
              },
              "policySetScheduleEntries": json.loads(rail.result('log_final_policytobeassigned'))
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> create_timeoff_policies_schedule_list
        create_timeoff_policies_schedule_list >> get_user_time_off_type_policy_summary >> foreach_item_in_policies_by_timeoff_type
        foreach_item_in_policies_by_timeoff_type >> for_each_policy_set_schedule
        for_each_policy_set_schedule >> log_effective_date >> if_effective_date_not_equal_beginning_of_next_year
        if_effective_date_not_equal_beginning_of_next_year >> rail.Label('Yes')  >> insert_to_timeoff_policies_schedule_list >> for_each_policy_set_schedule_end
        if_effective_date_not_equal_beginning_of_next_year >> rail.Label('No') >> for_each_policy_set_schedule_end
        for_each_policy_set_schedule >> for_each_policy_set_schedule_end >> foreach_item_in_policies_by_timeoff_type_end >> log_entitlement_calculation
        foreach_item_in_policies_by_timeoff_type >> foreach_item_in_policies_by_timeoff_type_end >> log_entitlement_calculation
        log_entitlement_calculation >> add_to_timeoff_policies_schedule_list >> if_timeoff_policies_schedule_list_has_data
        if_timeoff_policies_schedule_list_has_data >> rail.Label(
            'Yes')  >> log_final_policytobeassigned >> put_user_time_off_account_policy_set_schedule >> log_to_sumo
        if_timeoff_policies_schedule_list_has_data >> rail.Label('No') >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
