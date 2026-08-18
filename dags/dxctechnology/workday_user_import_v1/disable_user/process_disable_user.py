# TimeOff policy assignment dag is available in the user_import\generic_dags
from datetime import timedelta, datetime as dt
from json import dumps
from pendulum import datetime
import rail
from dxctechnology.workday_user_import_v1.disable_user.utils.data_handler import get_user_timeoff_type_policy_summary_data_handler
from dxctechnology.workday_user_import_v1.disable_user.utils.request_payload import REPORT_DATE_FORMAT
from dxctechnology.workday_user_import_v1.disable_user.utils.custom_methods import INPUT_DATE_FORMAT
from airflow.models import Variable


def create_child_dag(config):

    all_dags = []
    # To Create `config.process_disable_user_dag_count`  batches of the dag
    for idx in range(config.process_disable_user_dag_count):

        with rail.create_airflow_dag(
            dag_id=f"{config.disable_user_process_each_user_dag_id}_batch_{idx}",
            description=f"dxctechnology workday user sync disable users process user batch {idx}",
            replicon_conn_id=config.replicon_conn_id,
            company_key=config.company_key,
            start_date=datetime(2023, 9, 26),
            max_active_runs=config.max_active_run_master
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

            can_run_batch_task = rail.IfOperator(
                task_id = "can_run_batch_task",
                test=lambda: Variable.get(
                config.can_run_batch_task_var_name_disable_user, default_var='true').lower() == 'true',
                yes_task="batch_task",
                no_task="validate_employee_type"
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id = "batch_task",
                start_task="validate_employee_type",
                end_task="gather_failures",
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
            )

            validate_employee_type = rail.IfOperator(
                task_id="validate_employee_type",
                test="{{dag_run.conf.employee_type_check}}",
                yes_task="trigger_delete_future_entries"
            )

            # Trigger child DAG to delete future time entries and time offs
            trigger_delete_future_entries = rail.TriggerDagRunForEachItemOperator(
                task_id="trigger_delete_future_entries",
                trigger_dag_id=config.delete_future_entries_child_dag_id,
                items=[1],
                execution_timeout=timedelta(days=config.execution_timeout_days),
                retries=0,
                conf =lambda dag_run: {
                    'user_uri': dag_run.conf['user_uri'],
                    'end_date': dt.strftime(dt.strptime(dag_run.conf['end_date'], REPORT_DATE_FORMAT), INPUT_DATE_FORMAT),
                }
            )

            wait_for_trigger_delete_future_entries = rail.WaitForDagRunsSensor(
                task_id="wait_for_trigger_delete_future_entries",
                dag_runs="{{result('trigger_delete_future_entries')}}",
                execution_timeout=timedelta(days=config.execution_timeout_days),
            )

            disable_user_login = rail.RepliconServiceOperator(
                task_id="disable_user_login",
                endpoint="/services/SecurityService1.svc/DisableLogin",
                data={
                    "userUri": "{{dag_run.conf.user_uri}}"
                }
            )

            is_disabled_required = rail.IfOperator(
                task_id="is_disabled_required",
                test="{{ dag_run.conf.disable_required == 'Yes'}}",
                yes_task="get_user_timeoff_type_policy_summary"
            )

            get_user_timeoff_type_policy_summary = rail.RepliconServiceOperator(
                task_id="get_user_timeoff_type_policy_summary",
                endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
                data={
                    "userUri": "{{dag_run.conf.user_uri}}"
                },
                data_handler=get_user_timeoff_type_policy_summary_data_handler
            )

            #! This dag code is available under dxctechnology\workday_user_import_v1\user_import\generic_dags
            process_timeoff_no_accrual = rail.TriggerDagRunForEachItemOperator(
                task_id="process_timeoff_no_accrual",
                items=lambda: rail.result(
                    "get_user_timeoff_type_policy_summary"),
                trigger_dag_id=config.process_time_off_accrual,
                conf=lambda dag_run, item: {
                    **dag_run.conf,
                    **{
                        "timeoff_type_uri": item['timeOffType']['uri'],
                        "policy_set": dumps(item['policySetSchedule']).replace("[[{", "[{").replace("}]]", "}]")
                    }
                },
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                retries=0
            )

            wait_for_process_timeoff_no_accrual = rail.WaitForDagRunsSensor(
                task_id="wait_for_process_timeoff_no_accrual",
                dag_runs="{{result('process_timeoff_no_accrual')}}",
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                retries=0
            )

            gather_failures = rail.GatherResultsFromDagRunsOperator(
                task_id="gather_failures",
                dag_runs="{{result('process_timeoff_no_accrual')}}",
                dagrun_task_id="catch_errors",
                flatten=True
            )

            catch_errors = rail.PythonOperator(
                task_id="catch_errors",
                trigger_rule="one_failed",
                python_callable=lambda: rail.render_template(
                    "{{ get_error_message() }}")
            )

            can_run_batch_task >> rail.Label("Yes") >> batch_task >> gather_failures
            can_run_batch_task >> rail.Label("No") >> validate_employee_type

            # Main workflow - trigger deletion child DAG then proceed with disable user flow
            validate_employee_type >> rail.Label("Process disable") >> trigger_delete_future_entries >> wait_for_trigger_delete_future_entries >> disable_user_login \
                >> is_disabled_required >> rail.Label("Yes") >> get_user_timeoff_type_policy_summary\
                >> process_timeoff_no_accrual >> wait_for_process_timeoff_no_accrual >> gather_failures\
                >> rail.Label("On Error") >> catch_errors

        all_dags.append(dag)

    return all_dags


rail.for_each_instance(create_child_dag)
