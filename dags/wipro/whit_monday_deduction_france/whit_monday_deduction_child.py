from datetime import timedelta
from airflow.models import Variable
import rail
from wipro.whit_monday_deduction_france.utils import request_payload
from wipro.whit_monday_deduction_france import config

null = None


def create_dag(instance_config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=instance_config.child_dag,
        description=f'WIPRO | France Whit Monday Deduction | Child {instance_config.instance}',
        company_key=instance_config.company_key,
        replicon_conn_id=instance_config.replicon_conn_id,
        max_active_runs=instance_config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                instance_config.can_run_batch_task_var_name,
                default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_user_details',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(days=instance_config.execution_timeout_days),
        )

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
                "dataLoadOptionUri": "urn:replicon:data-load-option:fail-if-insufficient-data-access-permission"
            },
            data_handler=lambda response: {
                "useruri": response[0]['userDetails']['uri'],
                "timeoffpolicies": response[0]['timeOffTypePolicySummary']['policiesByTimeOffType']
            }
        )

        get_user_timeoff_policysetschedule = rail.PythonOperator(
            task_id='get_user_timeoff_policysetschedule',
            python_callable=lambda dag_run: {
                "_".join(key.split("_")[1:-1]): rail.find_first_by_attr_and_get_attr(
                    rail.result("get_user_details")["timeoffpolicies"],
                    'timeOffType.uri',
                    uri,
                    'policySetSchedule'
                )
                for key, uri in dag_run.conf['all_timeoff_type_uris'].items()
                if uri is not None
            }
        )

        if_rtt_carried_over_assigned = rail.IfOperator(
            task_id='if_rtt_carried_over_assigned',
            test=lambda: rail.result("get_user_timeoff_policysetschedule").get('annual_leave_rtt_carried_over') is not None,
            yes_task='deduct_rtt_carried_over_transaction',
            no_task='if_rtt_for_forfait_jours_co_assigned'
        )

        deduct_rtt_carried_over_transaction = rail.RepliconServiceOperator(
            task_id='deduct_rtt_carried_over_transaction',
            endpoint="/services/TimeOffService2.svc/PutTransaction",
            data=lambda dag_run: request_payload.build_whit_monday_deduction_payload(
                dag_run,
                timeoff_uri=dag_run.conf['all_timeoff_type_uris']['timeoff_annual_leave_rtt_carried_over_uri']
            )
        )

        log_rtt_carried_over_deduction = rail.WriteLogOperator(
            task_id='log_rtt_carried_over_deduction',
            log="{{ dag_run.conf.deduction_log }}",
            message='na',
            severity='Successful',
            properties=lambda dag_run: {
                "login_name": dag_run.conf['login_name'],
                "status": "Successful",
                "details": f"Whit Monday deduction applied to {config.ANNUAL_LEAVE_RTT_CARRIED_OVER}"
            }
        )

        if_rtt_for_forfait_jours_co_assigned = rail.IfOperator(
            task_id='if_rtt_for_forfait_jours_co_assigned',
            test=lambda: rail.result("get_user_timeoff_policysetschedule").get('annual_leave_rtt_for_forfait_jours_carried_over') is not None,
            yes_task='deduct_rtt_for_forfait_jours_co_transaction',
            no_task='finish'
        )

        deduct_rtt_for_forfait_jours_co_transaction = rail.RepliconServiceOperator(
            task_id='deduct_rtt_for_forfait_jours_co_transaction',
            endpoint="/services/TimeOffService2.svc/PutTransaction",
            data=lambda dag_run: request_payload.build_whit_monday_deduction_payload(
                dag_run,
                timeoff_uri=dag_run.conf['all_timeoff_type_uris']['timeoff_annual_leave_rtt_for_forfait_jours_carried_over_uri']
            )
        )

        log_rtt_for_forfait_jours_co_deduction = rail.WriteLogOperator(
            task_id='log_rtt_for_forfait_jours_co_deduction',
            log="{{ dag_run.conf.deduction_log }}",
            message='na',
            severity='Successful',
            properties=lambda dag_run: {
                "login_name": dag_run.conf['login_name'],
                "status": "Successful",
                "details": f"Whit Monday deduction applied to {config.ANNUAL_LEAVE_RTT_FOR_FORFAIT_JOURS_CARRIED_OVER}"
            }
        )

        finish = rail.EmptyOperator(task_id='finish')

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.deduction_log }}",
            trigger_rule='one_failed',
            message='na',
            severity='Error',
            properties=lambda dag_run: {
                "login_name": dag_run.conf['login_name'],
                "status": "Error",
                "details": rail.render_template("Error in Whit Monday deduction : {{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> get_user_details

        get_user_details >> get_user_timeoff_policysetschedule >> if_rtt_carried_over_assigned

        if_rtt_carried_over_assigned >> rail.Label("Yes") >> deduct_rtt_carried_over_transaction >> \
            log_rtt_carried_over_deduction >> if_rtt_for_forfait_jours_co_assigned
        if_rtt_carried_over_assigned >> rail.Label("No") >> if_rtt_for_forfait_jours_co_assigned

        if_rtt_for_forfait_jours_co_assigned >> rail.Label("Yes") >> deduct_rtt_for_forfait_jours_co_transaction >> \
            log_rtt_for_forfait_jours_co_deduction >> finish
        if_rtt_for_forfait_jours_co_assigned >> rail.Label("No") >> finish

        finish >> catch_and_log_error

        return dag


rail.for_each_instance(create_dag)
