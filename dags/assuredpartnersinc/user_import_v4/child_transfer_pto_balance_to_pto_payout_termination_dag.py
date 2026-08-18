from datetime import timedelta
from airflow.models import Variable
import rail
from assuredpartnersinc.user_import_v4.utils import python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_transfer_pto_balance_to_pto_payout_termination_dag_id,
        description=f'Assured Partners User Import Workflow to transfer pto balance to payout (Termination) {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_effective_date_derived'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_effective_date_derived',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_effective_date_derived = rail.PythonOperator(
            task_id='log_effective_date_derived',
            python_callable=lambda dag_run:  python_callable.get_split_date(
                dag_run.conf['enddate'], 'int')
        )

        log_get_required_timeoff_validation_script = rail.RepliconServiceOperator(
            task_id='log_get_required_timeoff_validation_script',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: {
                'max_overdraw': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Prevent balance overdraw', 'uri'),
                'restrict_booking_duration': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Restrict booking duration', 'uri')
            }
        )

        assign_time_offpolicy_8 = rail.RepliconServiceOperator(
            task_id='assign_time_offpolicy_8',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": dag_run.conf['ptopayouturi']
                },
                "policySetScheduleEntries": [
                    {
                        "effectiveDate": rail.result('log_effective_date_derived'),
                        "description": "Effective On " + str(rail.result('log_effective_date_derived')['year']) + "-" + str(
                            rail.result('log_effective_date_derived')['month']) + "-" + str(rail.result('log_effective_date_derived')['day']),
                        "policySet": {
                            "timeOffBalanceEventScripts": [
                                {
                                    "scriptTarget": {
                                        "uri": dag_run.conf['starting_balance_set_to_uri']
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:amount",
                                            "value": {
                                                "number": dag_run.conf['previousbalance']
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:precedence",
                                            "value": {
                                                "number": "20"
                                            }
                                        }
                                    ]
                                }
                            ],
                            "timeOffValidationScripts": [
                                {
                                    "scriptTarget": {
                                        "uri": rail.result('log_get_required_timeoff_validation_script')['max_overdraw']
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:maximum-overdraw",
                                            "value": {
                                                "number": "0"
                                            }
                                        }
                                    ]
                                },
                                {
                                    "scriptTarget": {
                                        "uri": rail.result('log_get_required_timeoff_validation_script')['restrict_booking_duration']
                                    },
                                    "additionalParameters": [
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:booking-duration-option",
                                            "value": {
                                                "uri": "urn:replicon:booking-duration-option:exact-duration"
                                            }
                                        },
                                        {
                                            "keyUri": "urn:replicon:script-key:parameter:booking-duration",
                                            "value": {
                                                "number": "9999999999"
                                            }
                                        }
                                    ]
                                }
                            ]
                        }
                    }
                ]
            }
        )

        catch_and_log_error = rail.PythonOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in Workflow to transfer pto balance to Pto payout (termination) : {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result(
                "catch_and_log_error") or "Success"
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> log_effective_date_derived

        log_effective_date_derived >> log_get_required_timeoff_validation_script >> assign_time_offpolicy_8 >> catch_and_log_error >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
