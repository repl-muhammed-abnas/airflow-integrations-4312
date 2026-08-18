from datetime import timedelta
from airflow.models import Variable
import rail

from cohnreznick.user_sync_v1.utils import request_payload, response_filter
from cohnreznick.user_sync_v1.utils.python_callable_methods import get_log_status_or_message

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_update_users,
        description='Cohnreznick User Sync - Process Update Users',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_update_users,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_info'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_user_info',
            end_task='catch_and_log_errors',
        )

        get_user_info = rail.RepliconServiceOperator(
            task_id='get_user_info',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data={
                "users": [
                    {
                        "uri": '{{ dag_run.conf.useruri }}',
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:fail-if-insufficient-data-access-permission"
            },
            response_filter=lambda res: res.json()['d'][0]
        )

        is_rehire_user = rail.IfOperator(
            task_id="is_rehire_user",
            test=lambda dag_run: not rail.result('get_user_info')['userDetails']['isEnabled'] and
                dag_run.conf['status'] == 'Enabled',
            yes_task="enable_login",
            no_task="get_current_udf_values"
        )

        enable_login = rail.RepliconServiceOperator(
            task_id='enable_login',
            endpoint='/services/securityservice1.svc/EnableLogin',
            data={
                "userUri": '{{ dag_run.conf.useruri }}'
            }
        )

        get_current_udf_values = rail.PythonOperator(
            task_id='get_current_udf_values',
            python_callable=lambda: rail.result('get_user_info')[
                'userDetails']['customFieldValues']
        )

        get_current_oef_values = rail.PythonOperator(
            task_id='get_current_oef_values',
            python_callable=lambda: rail.result('get_user_info')[
                'userDetails']['extensionFieldValues']
        )

        get_effective_user_groupmembership = rail.RepliconServiceOperator(
            task_id='get_effective_user_groupmembership',
            endpoint='/services/UserGroupService1.svc/GetEffectiveUserGroupMembership',
            data={
                "userUri": "{{dag_run.conf.useruri}}",
                "dateRange": null
            },
            data_handler=response_filter.get_effective_user_groupmembership_filter
        )

        def get_filtered_assigned_policy_to_user(response):
            result = list(filter(lambda x: x["policyUri"] == "urn:replicon:policy:time-punch", response))
            if result:
                return result[0]
            return null

        get_assigned_punch_policy_to_user = rail.RepliconServiceOperator(
            task_id='get_assigned_punch_policy_to_user',
            endpoint='/services/PolicySetService1.svc/GetAssignedPolicySetsForUser',
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri']
            },
            data_handler=lambda response: get_filtered_assigned_policy_to_user(response)
        )

        apply_user_modifications = rail.RepliconServiceOperator(
            task_id='apply_user_modifications',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=request_payload.apply_user_modifications_payload,
        )

        is_user_update_failed = rail.IfOperator(
            task_id = "is_user_update_failed",
            test="{{ result('apply_user_modifications').errors | is_truthy }}",
            yes_task="log_update_user_failed",
            no_task="log_update_user_success"
        )

        log_update_user_failed = rail.WriteLogOperator(
            task_id='log_update_user_failed',
            log = '{{ dag_run.conf.user_log }}',
            message="{{ result('apply_user_modifications').errors }}",
            severity='Error',
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "employeenumber": dag_run.conf['employeenumber'],
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                "action": "Update",
                'status': 'Error',
            }
        )

        log_update_user_success = rail.WriteLogOperator(
            task_id='log_update_user_success',
            log = '{{ dag_run.conf.user_log }}',
            message=lambda : get_log_status_or_message("msg", "Updated", apply_user_modifications.task_id),
            severity='Success',
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "employeenumber": dag_run.conf['employeenumber'],
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                "action": "Update",
                'status': get_log_status_or_message("status", "Updated", apply_user_modifications.task_id),
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log = '{{ dag_run.conf.user_log }}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "employeenumber": dag_run.conf['employeenumber'],
                "firstname": dag_run.conf['firstname'],
                "lastname": dag_run.conf['lastname'],
                "action": "Update",
                'status': 'Error',
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> get_user_info

        get_user_info >> is_rehire_user >> rail.Label('Yes') >> enable_login
        enable_login >> get_current_udf_values
        is_rehire_user >> rail.Label('Yes') >> get_current_udf_values >> get_current_oef_values >> get_effective_user_groupmembership

        get_effective_user_groupmembership >> get_assigned_punch_policy_to_user >> apply_user_modifications >> is_user_update_failed
        is_user_update_failed >> rail.Label('Yes') >> log_update_user_failed >> catch_and_log_errors
        is_user_update_failed >> rail.Label('No') >> log_update_user_success
        log_update_user_success >> catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_child_dag)
