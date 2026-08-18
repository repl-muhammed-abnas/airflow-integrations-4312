import rail
from galaxyusopcoinc.workday_user_sync.user_schedule_v4.utils import request_payload
from galaxyusopcoinc.workday_user_sync.user_schedule_v4.utils import python_callable_method
from airflow.models import Variable


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.process_user_schedule_child_dag,
        description=f'Vialto Partners User Schedule Child V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_process_user_schedule_runs,
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test = lambda: Variable.get(config.can_run_batch_task_var_name, deserialize_json=True, default_var={}).get('process_user', True),
            yes_task="batch_task",
            no_task="get_user_info_from_user_service"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="get_user_info_from_user_service",
            end_task="catch_and_log_errors"
        )

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_user_info_from_user_service = rail.RepliconServiceOperator(
            task_id='get_user_info_from_user_service',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=request_payload.get_user_details3_payload
        )

        is_user_present = rail.IfOperator(
            task_id="is_user_present",
            test="{{ result('get_user_info_from_user_service') | length > 0}}",
            yes_task="is_user_enable",
            no_task="log_user_not_present",
        )

        is_user_enable = rail.IfOperator(
            task_id="is_user_enable",
            # to be updated
            test=lambda: bool(rail.result('get_user_info_from_user_service')[0]['userDetails']['isEnabled']),
            yes_task="is_user_new_employee",
            no_task="log_user_disable",
        )

        log_user_not_present = rail.WriteLogOperator(
            task_id='log_user_not_present',
            message='User not present in Replicon',
            severity='Exception',
            properties={
                'schedulename': "{{dag_run.conf.replicon_schedule_type}}",
                'employeeid': "{{dag_run.conf.employee_id}}",
                'status': 'Exception',
                'message': "User not present in Replicon"
            }
        )

        log_user_disable = rail.WriteLogOperator(
            task_id='log_user_disable',
            message='User is disable in Replicon',
            severity='Exception',
            properties={
                'schedulename': "{{dag_run.conf.replicon_schedule_type}}",
                'employeeid': "{{dag_run.conf.employee_id}}",
                'status': 'Exception',
                'message': "User is disable in Replicon"
            }
        )

        is_user_new_employee = rail.IfOperator(
            task_id = "is_user_new_employee",
            test=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result("get_user_info_from_user_service")[0]['userDetails']['customFieldValues'],
                "customField.displayText", "New Employee" , "text", "No").lower() == "yes",
            yes_task="update_schedule_as_initial",
            no_task="is_payroll_type_udf_present"
        )

        update_schedule_as_initial = rail.RepliconServiceOperator(
            task_id='update_schedule_as_initial',
            endpoint='/services/ImportService1.svc/ApplyUserModifications3',
            data=request_payload.get_update_schedule_as_initial
        )

        update_new_employee_as_no = rail.RepliconServiceOperator(
            task_id='update_new_employee_as_no',
            endpoint='/services/ImportService1.svc/ApplyUserModifications3',
            data=request_payload.get_update_new_employee_as_no
        )

        log_success_assigned_schedule_as_initial = rail.WriteLogOperator(
            task_id='log_success_assigned_schedule_as_initial',
            message='Initial Schedule is assigned to user',
            severity='Success',
            properties={
                'schedulename': "{{ dag_run.conf.replicon_schedule_type}}",
                'employeeid': "{{ dag_run.conf.employee_id}}",
                'status': 'Success',
                'message': "Schedule is assigned to user as Initial Schedule as New Employee value set to Yes"
            }
        )

        def is_payroll_type_udf_present_test():
            user_details = rail.result("get_user_info_from_user_service")[0]['userDetails']['extensionFieldValues']
            payroll_type = rail.find_first_by_attr_and_get_attr(
                user_details,
                'definition.displayText',
                'Payroll Type',
                'textValue'
            )

            if payroll_type is None:
                return False

            if payroll_type not in ['Weekly', 'Monthly']:
                return False

            return True

        is_payroll_type_udf_present = rail.IfOperator(
            task_id = "is_payroll_type_udf_present",
            test = is_payroll_type_udf_present_test,
            yes_task="is_schedule_all_blank_zero_empty",
            no_task="log_invalid_payroll_type"
        )

        log_invalid_payroll_type = rail.WriteLogOperator(
            task_id='log_invalid_payroll_type',
            message='Payroll type is not valid, skipping schedule assignment for user',
            severity='Exception',
            properties=lambda dag_run:{
                'schedulename': dag_run.conf['replicon_schedule_type'],
                'employeeid': dag_run.conf['employee_id'],
                'status': 'Exception',
                'message':f"""Payroll type assigned to user (`{rail.find_first_by_attr_and_get_attr(
                                                rail.result("get_user_info_from_user_service")[0]['userDetails']['extensionFieldValues'],
                                                'definition.displayText',
                                                'Payroll Type',
                                                'textValue',
                                                default=""
            )}`) is not valid, skipping schedule assignment for user"""
            }
        )

        is_schedule_all_blank_zero_empty = rail.EmptyOperator(
            task_id = "is_schedule_all_blank_zero_empty"
        )

        is_schedule_all_blank_zero = rail.IfOperator(
            task_id='is_schedule_all_blank_zero',
            test=lambda dag_run: python_callable_method.schedule_all_blank_zero(
                dag_run.conf['replicon_schedule_type']),
            yes_task='update_office_schedule_to_zero_schedule',
            no_task='update_user_office_schedule'
        )

        update_office_schedule_to_zero_schedule = rail.RepliconServiceOperator(
            task_id='update_office_schedule_to_zero_schedule',
            endpoint='/services/ImportService1.svc/ApplyUserModifications3',
            # uri to be updated
            data=lambda dag_run: request_payload.apply_user_modification_payload(dag_run, False)
        )

        log_success_updated_office_schedule_to_zero_schedule = rail.WriteLogOperator(
            task_id='log_success_updated_office_schedule_to_zero_schedule',
            message='`0|0|0|0|0|0|0` Schedule is assigned to user',
            severity='Success',
            properties={
                'schedulename': "{{ dag_run.conf.replicon_schedule_type}}",
                'employeeid': "{{ dag_run.conf.employee_id}}",
                'status': 'Success',
                'message': "`0|0|0|0|0|0|0` Schedule is assigned to user"
            }
        )

        update_user_office_schedule = rail.RepliconServiceOperator(
            task_id='update_user_office_schedule',
            endpoint='/services/ImportService1.svc/ApplyUserModifications3',
            data=lambda dag_run: request_payload.apply_user_modification_payload(dag_run, False)
        )

        log_success_schedule = rail.WriteLogOperator(
            task_id='log_success_schedule',
            message='Schedule is assigned to user',
            severity='Success',
            properties={
                'schedulename': "{{ dag_run.conf.replicon_schedule_type}}",
                'employeeid': "{{ dag_run.conf.employee_id}}",
                'status': 'Success',
                'message': "Schedule is assigned to user"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'schedulename': "{{ dag_run.conf.replicon_schedule_type}}",
                'employeeid': "{{ dag_run.conf.employee_id}}",
                'status': 'Error',
                'message': '{{ get_error_message() }}',

            },
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_user_info_from_user_service

        get_user_info_from_user_service >> is_user_present >> rail.Label(
            "NO") >> log_user_not_present >> catch_and_log_errors
        is_user_present >> rail.Label("YES") >> is_user_enable
        is_user_enable >> rail.Label("YES") >> is_user_new_employee
        is_user_enable >> rail.Label("NO") >> log_user_disable >> catch_and_log_errors
        is_schedule_all_blank_zero >> rail.Label(
            "YES") >> update_office_schedule_to_zero_schedule >> log_success_updated_office_schedule_to_zero_schedule >> catch_and_log_errors
        is_schedule_all_blank_zero >> rail.Label(
            "NO") >> update_user_office_schedule
        update_user_office_schedule >> log_success_schedule >> catch_and_log_errors

        is_user_new_employee >> rail.Label("Yes") >> update_schedule_as_initial >> update_new_employee_as_no >> log_success_assigned_schedule_as_initial\
            >> catch_and_log_errors
        is_user_new_employee >> rail.Label("no") >> is_payroll_type_udf_present
        is_payroll_type_udf_present >> rail.Label("No") >> log_invalid_payroll_type >> rail.Label("On Error") >> catch_and_log_errors
        is_payroll_type_udf_present >> rail.Label("Yes") >> is_schedule_all_blank_zero_empty >> is_schedule_all_blank_zero
        return dag


rail.for_each_instance(create_child_dag)
