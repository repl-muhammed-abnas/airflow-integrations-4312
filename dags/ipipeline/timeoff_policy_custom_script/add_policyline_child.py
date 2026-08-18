from datetime import timedelta
from airflow.models import Variable
import rail
from ipipeline.timeoff_policy_custom_script.utils import python_callable

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.add_policyline_child,
        description=f'iPipeline | YEAR END POLICY LINE - CUSTOM SCRIPT - Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_details_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_details_logs',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_details_logs = rail.CreateLogOperator(
            task_id='create_details_logs'
        )

        def get_user_data(response):
            if not response: return []
            oefs = {
                "seniority_years": float(rail.find_first_by_attr_and_get_attr(
                    response[0]['userDetails']['extensionFieldValues'], 'definition.displayText', 'Seniority Years', 'textValue', 0
                )),
                "fte": float(rail.find_first_by_attr_and_get_attr(
                    response[0]['userDetails']['extensionFieldValues'], 'definition.displayText', 'FTE', 'textValue', 1
                )),
                "scheduled_hours": float(rail.find_first_by_attr_and_get_attr(
                    response[0]['userDetails']['extensionFieldValues'], 'definition.displayText', 'Scheduled Hours', 'textValue', 1
                ))
            }
            return {
                "useruri": response[0]['userDetails']['uri'],
                "enabled": response[0]['userDetails']['isEnabled'],
                "oefs": oefs,
                "timeoffpolicies": response[0]['timeOffTypePolicySummary']['policiesByTimeOffType']
            }

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": null,
                        "loginName": "{{dag_run.conf.login_name}}",
                        "employeeId": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=get_user_data
        )

        get_user_timeoff_policysetschedule = rail.PythonOperator(
            task_id='get_user_timeoff_policysetschedule',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                "get_user_details")["timeoffpolicies"], 'timeOffType.uri',
                dag_run.conf['timeoff_type_uri'], 'policySetSchedule',[])
        )

        get_historical_policies = rail.PythonOperator(
            task_id='get_historical_policies',
            python_callable=lambda dag_run: python_callable.get_relevant_historical_policies(
                rail.result('get_user_timeoff_policysetschedule'), dag_run.conf['effective_date_for_new_policyset'])
        )

        final_policyset_schedule_for_timeoff = rail.PythonOperator(
            task_id='final_policyset_schedule_for_timeoff',
            python_callable=lambda dag_run: python_callable.get_final_policyset_schedule(
                rail.result('get_historical_policies'),
                dag_run.conf['effective_date_for_new_policyset'],
                dag_run.conf['get_default_policy'],
                rail.result("get_user_details")["oefs"],
                dag_run.conf['timeoff_type'],config)
        )

        update_policy = rail.RepliconServiceOperator(
            task_id="update_policy",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": rail.result("get_user_details")["useruri"],
                    "timeOffTypeUri": dag_run.conf['timeoff_type_uri']
                },
                "policySetScheduleEntries": rail.result('final_policyset_schedule_for_timeoff')
            }
        )

        log_policy_update = rail.WriteLogOperator(
            task_id='log_policy_update',
            log="{{ result('create_details_logs') }}",
            message='na',
            severity='Success',
            properties=lambda dag_run: {
                "login_name": dag_run.conf['login_name'],
                "timeoff_type": dag_run.conf['timeoff_type'],
                "status": "Success",
                "details": f"Year End Policy Line Added for {dag_run.conf['timeoff_type']}"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{dag_run.conf.user_log}}",
            trigger_rule='one_failed',
            message='na',
            severity='Error',
            properties=lambda dag_run: {
                "login_name": dag_run.conf['login_name'],
                "timeoff_type": dag_run.conf['timeoff_type'],
                "status": "Error",
                "details": rail.render_template("Error Year End Policy Line Addition : {{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> create_details_logs

        create_details_logs >> get_user_details >> get_user_timeoff_policysetschedule >> get_historical_policies >> \
        final_policyset_schedule_for_timeoff >> update_policy >> log_policy_update >> catch_and_log_error

        return dag


rail.for_each_instance(create_dag)
