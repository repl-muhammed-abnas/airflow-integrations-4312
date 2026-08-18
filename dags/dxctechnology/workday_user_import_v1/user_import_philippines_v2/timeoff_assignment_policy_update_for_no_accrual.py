from pendulum import datetime
import rail
from airflow.models import Variable

from dxctechnology.workday_user_import_v1.user_import_philippines_v2.utils.custom_methods import (
    get_timeoff_polices_to_assign_callable,
    format_timeoff_polices_to_assign_callable
)

from dxctechnology.workday_user_import_v1.user_import_philippines_v2.utils.request_payload import (
    get_update_policy_payload,
    get_user_timeoff_balance_summary_payload
)
from datetime import timedelta


def create_dag(config):
    _dags = []
    for batch_index in range(1, config.DAG_BATCH_COUNT + 1):
        prefix = f"_{batch_index}"
        if batch_index == 1:
            prefix = ""
        with rail.create_airflow_dag(
            dag_id=f"{config.process_time_off_accrual}{prefix}",
            description="dxctechnology workday user sync timeoff assignment policy update for no accrual child",
            replicon_conn_id=config.replicon_conn_id,
            company_key=config.company_key,
            start_date=datetime(2023, 9, 26),
            max_active_runs=config.max_active_run_process_timeoff_no_accrual
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

            can_run_batch_task = rail.IfOperator(
                task_id = "can_run_batch_task",
                test=lambda: Variable.get(
                config.can_run_batch_task_var_name_philippines, default_var='true').lower() == 'true',
                yes_task="batch_task",
                no_task="is_end_date_present"
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id = "batch_task",
                start_task="is_end_date_present",
                end_task="update_policy",
                execution_timeout=timedelta(days=14)
            )

            is_end_date_present = rail.IfOperator(
                task_id="is_end_date_present",
                test=lambda dag_run: bool(
                    dag_run.conf['user_end_date_json'].get('year', False)),
                yes_task="get_timeoff_details"
            )

            get_timeoff_details = rail.RepliconServiceOperator(
                task_id="get_timeoff_details",
                endpoint="/services/TimeOffService1.svc/BulkGetTimeOffTypes",
                data={
                    "timeOffTypeUris": [
                        "{{dag_run.conf.timeoff_type_uri}}"
                    ]
                }
            )

            get_users_effective_group_membership = rail.RepliconServiceOperator(
                task_id="get_users_effective_group_membership",
                endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
                data={
                    "userUri": "{{dag_run.conf.user_uri}}",
                    "dateRange": None
                }
            )

            get_user_timeoff_balance_summary = rail.RepliconServiceOperator(
                task_id="get_user_timeoff_balance_summary",
                endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
                data=get_user_timeoff_balance_summary_payload
            )

            get_timeoff_polices_to_assign = rail.PythonOperator(
                task_id="get_timeoff_polices_to_assign",
                python_callable=get_timeoff_polices_to_assign_callable
            )

            format_timeoff_polices_to_assign = rail.PythonOperator(
                task_id="format_timeoff_polices_to_assign",
                python_callable=format_timeoff_polices_to_assign_callable
            )

            update_policy = rail.RepliconServiceOperator(
                task_id="update_policy",
                endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
                data=get_update_policy_payload
            )

            catch_errors = rail.PythonOperator(
                task_id="catch_errors",
                trigger_rule="one_failed",
                python_callable=lambda: rail.render_template(
                    "{{ get_error_message() }}")
            )

            can_run_batch_task >> rail.Label("Yes") >> batch_task >> update_policy >> rail.Label("On Error") >> catch_errors
            can_run_batch_task >> rail.Label("No") >> is_end_date_present

            is_end_date_present >> rail.Label("Yes") >> get_timeoff_details >> get_users_effective_group_membership \
                >> get_user_timeoff_balance_summary >> get_timeoff_polices_to_assign >> format_timeoff_polices_to_assign >> update_policy

        _dags.append(dag)

    return _dags

rail.for_each_instance(create_dag)
