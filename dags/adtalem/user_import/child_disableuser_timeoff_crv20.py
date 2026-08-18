from datetime import timedelta
from airflow.models import Variable
import rail
from adtalem.user_import.utils.request_payload import get_datetime_obj
from adtalem.user_import.utils.response_filter import get_assigned_timeoffuris


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


def create_disableuser_timeoff_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_disable_user_timeoff_crv2.0_{config.instance}',
        description=f'Adtalem Disable User - Time Off_Production CRV2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_assigned_timeoffuri_list'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_assigned_timeoffuri_list',
            end_task='dagrun_log_to_sumo',
        )

        get_assigned_timeoffuri_list = rail.RepliconServiceOperator(
            task_id='get_assigned_timeoffuri_list',
            endpoint="/services/TimeOffService1.svc/BulkGetTimeOffTypeAssignmentsForUsers",
            data={
                "userUris": [
                    "{{ dag_run.conf.useruri }}"
                ]
            },
            data_handler=get_assigned_timeoffuris
        )

        is_assignedtimeoffs = rail.IfOperator(
            task_id='is_assignedtimeoffs',
            test="{{ result('get_assigned_timeoffuri_list') | length > 0 }}",
            yes_task='trigger_child_put_0_balance_15days_terminationdate_crv20',
            no_task='is_terminationdate_present'
        )

        trigger_child_put_0_balance_15days_terminationdate_crv20 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_put_0_balance_15days_terminationdate_crv20',
            retries=0,
            items=lambda: rail.result('get_assigned_timeoffuri_list'),
            trigger_dag_id=f'adtalem_userimport_put_0_balance_15days_terminationdate_crv2.0_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                "timeoffuri": item,
                "useruri": dag_run.conf['useruri'],
                "terminationdate": dag_run.conf['terminationdate']
            }
        )

        wait_for_completion_trigger_child_put_0_balance_15days_terminationdate_crv20 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_child_put_0_balance_15days_terminationdate_crv20',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_put_0_balance_15days_terminationdate_crv20") }}'
        )

        is_terminationdate_present = rail.IfOperator(
            task_id='is_terminationdate_present',
            test=lambda dag_run: bool(dag_run.conf['terminationdate']) and dag_run.conf[
                'terminationdate'] != dag_run.conf.get('userenddatefrominstance'),
            yes_task="update_enddate",
            no_task="dagrun_log_to_sumo",
        )

        update_enddate = rail.RepliconServiceOperator(
            task_id='update_enddate',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": get_datetime_obj(dag_run.conf['startdate']),
                    "endDate": get_datetime_obj(dag_run.conf['terminationdate'])
                }
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> get_assigned_timeoffuri_list

        get_assigned_timeoffuri_list >> is_assignedtimeoffs

        is_assignedtimeoffs >> rail.Label(
            'Yes') >> trigger_child_put_0_balance_15days_terminationdate_crv20 >> \
            wait_for_completion_trigger_child_put_0_balance_15days_terminationdate_crv20 >> \
            is_terminationdate_present
        is_assignedtimeoffs >> rail.Label(
            'No') >> is_terminationdate_present

        is_terminationdate_present >> rail.Label(
            'Yes') >> update_enddate >> dagrun_log_to_sumo
        is_terminationdate_present >> rail.Label(
            'No') >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_disableuser_timeoff_child_dag)
