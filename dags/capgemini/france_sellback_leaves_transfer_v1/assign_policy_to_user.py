from datetime import timedelta
from capgemini.france_sellback_leaves_transfer_v1.utils import request_payload
import rail
from airflow.models import Variable

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.assign_policy_child_dagid,
        description=f'Capgemini France Sellback Leaves Transfer Assign Policy to User Child {config.instance} V1',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_child_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_log',
            end_task='catch_and_log_errors',
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        get_credit_to_timeoff = rail.RepliconServiceOperator(
            task_id='get_credit_to_timeoff',
            endpoint='/services/TimeOffService1.svc/GetEnabledTimeOffTypes',
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, "displayText",
                dag_run.conf["transfer_bal_to_timeoff"], "uri")
        )

        is_france_credit_to_timeoff_present = rail.IfOperator(
            task_id='is_france_credit_to_timeoff_present',
            test='{{ result("get_credit_to_timeoff") | is_truthy }}',
            yes_task="get_existingpolicy_schedule_for_timeoff",
            no_task="log_timeofftype_not_present_or_disabled"
        )

        log_timeofftype_not_present_or_disabled = rail.WriteLogOperator(
            task_id='log_timeofftype_not_present_or_disabled',
            log='{{ result("create_log") }}',
            message="Time Off Type '{{ dag_run.conf.transfer_bal_to_timeoff }}' is not present or disabled in Replicon",
            severity='Exception',
            properties=lambda dag_run: {
                "username": dag_run.conf["sellback_leaves_details"]["username"],
                "employee_id": dag_run.conf["sellback_leaves_details"]["employeeid"],
                "sellback_source_timeoff_type": dag_run.conf["sellback_leaves_details"]["timeofftype"],
                "sellback_amount": abs(float(dag_run.conf["sellback_leaves_details"]["amount"])),
                "sellback_dest_timeoff_type": dag_run.conf["transfer_bal_to_timeoff"],
                "status": "Exception",
                "comments": "Time Off Type '{{ dag_run.conf.transfer_bal_to_timeoff }}' is not present or disabled in Replicon"
            }
        )

        get_existingpolicy_schedule_for_timeoff = rail.RepliconServiceOperator(
            task_id='get_existingpolicy_schedule_for_timeoff',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.sellback_leaves_details.useruri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response['policiesByTimeOffType'], 'timeOffType.uri', rail.result("get_credit_to_timeoff"))
        )

        is_france_credit_to_timeoff_assigned_to_user = rail.IfOperator(
            task_id='is_france_credit_to_timeoff_assigned_to_user',
            test='{{ result("get_existingpolicy_schedule_for_timeoff") | is_truthy }}',
            yes_task='put_timeoffpolicy_entry',
            no_task='log_timeoff_not_assigned_to_user'
        )

        log_timeoff_not_assigned_to_user = rail.WriteLogOperator(
            task_id='log_timeoff_not_assigned_to_user',
            log='{{ result("create_log") }}',
            message="Timeoff Type '{{ dag_run.conf.transfer_bal_to_timeoff }}' is not available to user in Replicon",
            severity='Exception',
            properties=lambda dag_run: {
                "username": dag_run.conf["sellback_leaves_details"]["username"],
                "employee_id": dag_run.conf["sellback_leaves_details"]["employeeid"],
                "sellback_source_timeoff_type": dag_run.conf["sellback_leaves_details"]["timeofftype"],
                "sellback_amount": abs(float(dag_run.conf["sellback_leaves_details"]["amount"])),
                "sellback_dest_timeoff_type": dag_run.conf["transfer_bal_to_timeoff"],
                "status": "Exception",
                "comments": "Timeoff Type '{{ dag_run.conf.transfer_bal_to_timeoff }}' is not available to user in Replicon"
            }
        )

        put_timeoffpolicy_entry = rail.RepliconServiceOperator(
            task_id='put_timeoffpolicy_entry',
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=request_payload.get_put_timeoffpolicyentry
        )

        log_sellback_leaves_transferred_success = rail.WriteLogOperator(
            task_id='log_sellback_leaves_transferred_success',
            log='{{ result("create_log") }}',
            message="'{{ dag_run.conf.sellback_leaves_details.timeofftype }}' Sell Back Leaves transferred to '{{ dag_run.conf.transfer_bal_to_timeoff }}' successfully",
            severity='Success',
            properties=lambda dag_run: {
                "username": dag_run.conf["sellback_leaves_details"]["username"],
                "employee_id": dag_run.conf["sellback_leaves_details"]["employeeid"],
                "sellback_source_timeoff_type": dag_run.conf["sellback_leaves_details"]["timeofftype"],
                "sellback_amount": abs(float(dag_run.conf["sellback_leaves_details"]["amount"])),
                "sellback_dest_timeoff_type": dag_run.conf["transfer_bal_to_timeoff"],
                "status": "Success",
                "comments": "'{{ dag_run.conf.sellback_leaves_details.timeofftype }}' Sell Back Leaves transferred to '{{ dag_run.conf.transfer_bal_to_timeoff }}' successfully"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("create_log") }}',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error',
            properties=lambda dag_run: {
                "username": dag_run.conf["sellback_leaves_details"]["username"],
                "employee_id": dag_run.conf["sellback_leaves_details"]["employeeid"],
                "sellback_source_timeoff_type": dag_run.conf["sellback_leaves_details"]["timeofftype"],
                "sellback_amount": abs(float(dag_run.conf["sellback_leaves_details"]["amount"])),
                "sellback_dest_timeoff_type": dag_run.conf["transfer_bal_to_timeoff"],
                "status": "Error",
                "comments": '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> create_log

        create_log >> get_credit_to_timeoff >> is_france_credit_to_timeoff_present
        is_france_credit_to_timeoff_present >> rail.Label("Yes") >> get_existingpolicy_schedule_for_timeoff \
            >> is_france_credit_to_timeoff_assigned_to_user
        is_france_credit_to_timeoff_present >> rail.Label("No") >> log_timeofftype_not_present_or_disabled >> catch_and_log_errors
        is_france_credit_to_timeoff_assigned_to_user >> rail.Label("Yes") \
            >> put_timeoffpolicy_entry >> log_sellback_leaves_transferred_success >> catch_and_log_errors
        is_france_credit_to_timeoff_assigned_to_user >> rail.Label("No") >> log_timeoff_not_assigned_to_user >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
