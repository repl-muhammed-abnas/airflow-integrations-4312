from datetime import timedelta
from airflow.models import Variable
import rail

from lanter_delivery_systems.user_import.user_import_integration.utils import request_payload, response_filter
from lanter_delivery_systems.user_import.user_import_integration.utils.python_callable_methods import get_product_licenses_uris
from lanter_delivery_systems.user_import.user_import_integration.tasks.process_supervisor import process_supervisor_assignment_task_group

null= None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_update_users_dagid,
        description='Lanter Delivery Systems User Import - Process Update Users',
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

        is_user_disabled =  rail.IfOperator(
            task_id="is_user_disabled",
            test=lambda dag_run : not bool(rail.result('get_user_info')['userDetails']['isEnabled']) and dag_run.conf['enabled'] =='Yes',
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

        get_effective_user_groupmembership = rail.RepliconServiceOperator(
            task_id='get_effective_user_groupmembership',
            endpoint='/services/UserGroupService1.svc/GetEffectiveUserGroupMembership',
            data={
                "userUri": "{{dag_run.conf.useruri}}",
                "dateRange": null
            },
            data_handler=response_filter.get_effective_user_groupmembership_filter
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
            no_task="get_assigned_product_licenses"
        )

        log_update_user_failed = rail.WriteLogOperator(
            task_id='log_update_user_failed',
            log = '{{ dag_run.conf.user_log }}',
            message="{{ result('apply_user_modifications').errors }}",
            severity='Error',
            properties=lambda dag_run: {
                "loginname": dag_run.conf['loginname'],
                "lastname": dag_run.conf['lastname'],
                "firstname": dag_run.conf['firstname'],
                "action": "Update",
                'status': 'Error',
                'details': rail.result('apply_user_modifications')['errors']
            }
        )

        get_assigned_product_licenses = rail.RepliconServiceOperator(
            task_id='get_assigned_product_licenses',
            endpoint='/services/AccountManagementService1.svc/GetProductAssignmentsForUser',
            data={
                "userUri": "{{dag_run.conf.useruri}}",
            },
            data_handler=response_filter.filter_product_license_description
        )

        get_product_licenses_to_assign = rail.PythonOperator(
            task_id='get_product_licenses_to_assign',
            python_callable=get_product_licenses_uris
        )

        is_product_license_changed_for_user = rail.IfOperator(
            task_id='is_product_license_changed_for_user',
            test=lambda: bool(rail.result('get_product_licenses_to_assign')),
            yes_task='update_product_licences',
            no_task='is_supervisor_in_feed_file'
        )

        update_product_licences = rail.RepliconServiceOperator(
            task_id='update_product_licences',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data=lambda dag_run:{
                "userUri": dag_run.conf['useruri'],
                "productUris": rail.result('get_product_licenses_to_assign')
            }
        )

        is_supervisor_in_feed_file = rail.IfOperator(
            task_id='is_supervisor_in_feed_file',
            test=lambda dag_run: bool(dag_run.conf['supervisorusername']),
            yes_task='search_supervisor_in_replicon',
            no_task='log_user_completion'
        )

        process_supervisor_entry,  process_supervisor_exit= process_supervisor_assignment_task_group(
            'useruri', 'update_user')

        log_user_completion = rail.WriteLogOperator(
            task_id='log_user_completion',
            log = '{{ dag_run.conf.user_log }}',
            message=request_payload.get_update_user_message,
            severity=request_payload.get_update_user_severity,
            properties=lambda dag_run: {
                "loginname": dag_run.conf['loginname'],
                "lastname": dag_run.conf['lastname'],
                "firstname": dag_run.conf['firstname'],
                "action": "Update",
                "status": request_payload.get_update_user_severity(),
                'details': request_payload.get_update_user_message()
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log = '{{ dag_run.conf.user_log }}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "lastname": "{{dag_run.conf.lastname}}",
                "firstname": "{{dag_run.conf.firstname}}",
                "action": "Update",
                'status': 'Error',
                'details': "{{ get_error_message() }}"
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

        get_user_info >> is_user_disabled >> rail.Label('Yes') >> enable_login >> get_current_udf_values
        is_user_disabled >> rail.Label('No') >> get_current_udf_values

        get_current_udf_values >> get_effective_user_groupmembership
        get_effective_user_groupmembership >> apply_user_modifications >> is_user_update_failed

        is_user_update_failed >> rail.Label('Yes') >> log_update_user_failed >> catch_and_log_errors
        is_user_update_failed >> rail.Label('No') >> get_assigned_product_licenses >> get_product_licenses_to_assign >> is_product_license_changed_for_user
        is_product_license_changed_for_user >> rail.Label('Yes') >> update_product_licences >> is_supervisor_in_feed_file
        is_product_license_changed_for_user >> rail.Label('No') >> is_supervisor_in_feed_file

        is_supervisor_in_feed_file >> rail.Label('No') >> log_user_completion >> catch_and_log_errors
        is_supervisor_in_feed_file >> rail.Label('Yes') >> process_supervisor_entry
        process_supervisor_exit >> log_user_completion

        catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_child_dag)
