from datetime import timedelta
from airflow.models import Variable
import rail
import json

from mercury_systems_inc.user_import_v1.utils import request_payload, custom_methods
from mercury_systems_inc.user_import_v1.task_groups.process_supervisor import process_supervisor_assignment_task_group

null = None


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.process_update_user_dagid,
        description='MercurySystemsInc User Import Process Update Users',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_new_update_users,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_user_details',
            end_task='catch_and_log_errors',
        )

        get_user_details = rail.PythonOperator(
            task_id='get_user_details',
            python_callable=lambda dag_run: json.loads(
                rail.read_artifact(dag_run.conf['user_details_artifact']))
        )

        process_supervisor_entry,  process_supervisor_exit = process_supervisor_assignment_task_group(
            "update_user")

        validate_replicon_fields = rail.PythonOperator(
            task_id='validate_replicon_fields',
            python_callable=custom_methods.validate_replicon_field_names_uris
        )

        is_validation_successful = rail.IfOperator(
            task_id='is_validation_successful',
            test=lambda: rail.result("validate_replicon_fields")["is_valid"],
            yes_task='get_effective_user_groupmembership',
            no_task='log_validation_errors'
        )

        log_validation_errors = rail.WriteLogOperator(
            task_id='log_validation_errors',
            log='{{ dag_run.conf.user_log }}',
            message=lambda: '; '.join(rail.result(
                'validate_replicon_fields')['missing_fields']),
            severity='Exception',
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['Employee_ID'],
                'first_name': dag_run.conf['First_Name'],
                'last_name': dag_run.conf['Last_Name'],
                'action': 'Validation',
                'status': 'Exception',
                'details': "User not updated ; " + ' ; '.join(rail.result('validate_replicon_fields')['missing_fields'])
            }
        )

        get_effective_user_groupmembership = rail.RepliconServiceOperator(
            task_id='get_effective_user_groupmembership',
            endpoint='/services/UserGroupService1.svc/GetEffectiveUserGroupMembership',
            data={
                "userUri": "{{dag_run.conf.user_uri}}",
                "dateRange": null
            },
            data_handler=lambda res: {
                'current_departmentgroup_name': (res['departments'][0]['department']['department']['displayText'] if res['departments'][0]['department'] else '') if res['departments'] else '',
                'current_departmentgroup_uri': (res['departments'][0]['department']['department']['uri'] if res['departments'][0]['department'] else '') if res['departments'] else '',
                'current_employeetype_name': (res['employeeTypes'][0]['employeeType']['employeeType']['displayText'] if res['employeeTypes'][0]['employeeType'] else '') if res['employeeTypes'] else '',
                'current_employeetype_uri': (res['employeeTypes'][0]['employeeType']['employeeType']['uri'] if res['employeeTypes'][0]['employeeType'] else '') if res['employeeTypes'] else '',
                'current_location_name': (res['locations'][0]['location']['location']['displayText'] if res['locations'][0]['location'] else '') if res['locations'] else '',
                'current_location_uri': (res['locations'][0]['location']['location']['uri'] if res['locations'][0]['location'] else '') if res['locations'] else '',
            }
        )

        log_existing_timeoff_policies_for_user = rail.RepliconServiceOperator(
            task_id='log_existing_timeoff_policies_for_user',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}"
            },
            data_handler=lambda res: res["policiesByTimeOffType"] if (
                res["policiesByTimeOffType"]) else []
        )

        get_new_and_existing_timeoff_types_for_user = rail.PythonOperator(
            task_id='get_new_and_existing_timeoff_types_for_user',
            python_callable=custom_methods.get_timeoffs_for_update_rehire_user
        )

        log_update_user_payload = rail.PythonOperator(
            task_id='log_update_user_payload',
            python_callable=lambda dag_run: request_payload.get_update_rehire_user_payload(
                dag_run, config)
        )

        update_user = rail.RepliconServiceOperator(
            task_id="update_user",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda: rail.result(
                'log_update_user_payload')['payload']
        )

        if_remaining_modifications_to_apply = rail.IfOperator(
            task_id='if_remaining_modifications_to_apply',
            test=lambda: rail.result(
                'log_update_user_payload')['applyusermodifications3_payload_for_modifications'],
            yes_task='apply_remaining_modifications',
            no_task='if_non_eligible_timeoff_types'
        )

        apply_remaining_modifications = rail.RepliconServiceOperator(
            task_id='apply_remaining_modifications',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['user_uri']
                },
                "modifications": rail.result('log_update_user_payload')['applyusermodifications3_payload_for_modifications'],
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_non_eligible_timeoff_types = rail.IfOperator(
            task_id='if_non_eligible_timeoff_types',
            test=lambda: rail.result('get_new_and_existing_timeoff_types_for_user')[
                'timeoff_for_stopping_accrual'],
            yes_task='dummy_process_stop_accruals_for_non_eligible_timeoff_types',
            no_task='log_user_completion'
        )

        dummy_process_stop_accruals_for_non_eligible_timeoff_types = rail.EmptyOperator(
            task_id='dummy_process_stop_accruals_for_non_eligible_timeoff_types'
        )

        stop_accruals_for_non_eligible_timeoff_types = rail.TriggerDagRunForEachItemOperator(
            task_id='stop_accruals_for_non_eligible_timeoff_types',
            items=lambda: rail.result('get_new_and_existing_timeoff_types_for_user')[
                'timeoff_for_stopping_accrual'],
            trigger_dag_id=config.process_stop_accrual_for_timeoff_types,
            conf=lambda item, dag_run: {
                'timeoff_uri_for_stopping_accrual': item,
                'existing_policyset_schedule_for_timeoff': rail.find_first_by_attr_and_get_attr(rail.result(
                    'log_existing_timeoff_policies_for_user'), 'timeOffType.uri', item, 'policySetSchedule'),
                "starting_balance_set_to_script_uri": dag_run.conf["starting_balance_set_to_script_uri"],
                "prevent_balance_overdraw_script_uri": dag_run.conf["prevent_balance_overdraw_script_uri"],
                'user_uri': dag_run.conf["user_uri"],
                'effective_date': dag_run.conf["Effective_Date"] or dag_run.conf["integration_run_date"],
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_for_stop_accruals_for_non_eligible_timeoff_types = rail.WaitForDagRunsSensor(
            task_id='wait_for_stop_accruals_for_non_eligible_timeoff_types',
            dag_runs="{{ result('stop_accruals_for_non_eligible_timeoff_types') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_errors_from_child = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_errors_from_child',
            dag_runs='{{ result("stop_accruals_for_non_eligible_timeoff_types") }}',
            dagrun_task_id='catch_errors',
            execution_timeout=timedelta(
                hours=config.gather_errors_from_child_timeout_hours),
            flatten=True
        )

        if_error_in_child = rail.IfOperator(
            task_id='if_error_in_child',
            test=lambda: 'Error' in json.dumps(
                rail.result('gather_errors_from_child')),
            yes_task='failure_while_stoppping_accrual_for_timeoffs',
            no_task='log_user_completion'
        )

        failure_while_stoppping_accrual_for_timeoffs = rail.FailOperator(
            task_id='failure_while_stoppping_accrual_for_timeoffs',
            message="Failed while stopping accrual for Non-Eligible time off type",
        )

        log_user_completion = rail.WriteLogOperator(
            task_id='log_user_completion',
            log='{{ dag_run.conf.user_log }}',
            message="User Updated Successfully",
            severity="Success",
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['Employee_ID'],
                'first_name': dag_run.conf['First_Name'],
                'last_name': dag_run.conf['Last_Name'],
                'action': 'Update',
                "status": "Success",
                'details': "User Updated Successfully",
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.user_log }}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'employee_id': '{{dag_run.conf.Employee_ID}}',
                "first_name": "{{dag_run.conf.First_Name}}",
                "last_name": "{{dag_run.conf.Last_Name}}",
                'action': 'Update',
                'status': 'Error',
                'details': "{{ get_error_message() }}"
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> get_user_details >> process_supervisor_entry

        process_supervisor_exit >> validate_replicon_fields

        validate_replicon_fields >> is_validation_successful
        is_validation_successful >> rail.Label(
            'Yes') >> get_effective_user_groupmembership
        is_validation_successful >> rail.Label(
            'No') >> log_validation_errors >> catch_and_log_errors

        get_effective_user_groupmembership >> log_existing_timeoff_policies_for_user >>\
            get_new_and_existing_timeoff_types_for_user

        get_new_and_existing_timeoff_types_for_user >> log_update_user_payload

        log_update_user_payload >> update_user >> if_remaining_modifications_to_apply

        if_remaining_modifications_to_apply >> rail.Label(
            'Yes') >> apply_remaining_modifications >> if_non_eligible_timeoff_types
        if_remaining_modifications_to_apply >> rail.Label(
            'No') >> if_non_eligible_timeoff_types

        if_non_eligible_timeoff_types >> rail.Label(
            'Yes') >> dummy_process_stop_accruals_for_non_eligible_timeoff_types
        if_non_eligible_timeoff_types >> rail.Label(
            'No') >> log_user_completion

        dummy_process_stop_accruals_for_non_eligible_timeoff_types >> stop_accruals_for_non_eligible_timeoff_types >>\
            wait_for_stop_accruals_for_non_eligible_timeoff_types >> gather_errors_from_child >> if_error_in_child

        if_error_in_child >> rail.Label('No') >> log_user_completion
        if_error_in_child >> rail.Label(
            'Yes') >> failure_while_stoppping_accrual_for_timeoffs >> log_user_completion

        log_user_completion >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
