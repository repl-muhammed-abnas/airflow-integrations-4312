from datetime import timedelta
from airflow.models import Variable
import rail
from terraconconsultants.user_import.utils.request_payload import get_holidaybookings_delete_payload
from terraconconsultants.user_import.utils.response_filter import get_timeoff_uris, get_timeofftype_list, page_handler


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/terraconconsultants/user_import/config.py


def create_delete_holiday_booking_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'terraconconsultants_userimport_child_delete_holiday_bookings_{config.instance}',
        description=f'TerraconConsultants User Import Child - Delete Holiday Bookings {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_timeofftype_policy_summary'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_timeofftype_policy_summary',
            end_task='dagrun_log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_timeofftype_policy_summary = rail.RepliconServiceOperator(
            task_id='get_timeofftype_policy_summary',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=get_timeofftype_list
        )

        put_timeofftype_assignments_user = rail.RepliconServiceOperator(
            task_id='put_timeofftype_assignments_user',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": [x['uri'] for x in rail.result('get_timeofftype_policy_summary') if x['uri']]
            }
        )

        get_holiday_timeoff_uri = rail.RepliconServiceOperator(
            task_id='get_holiday_timeoff_uri',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response,
                                                                               'displayText', 'Holiday', 'uri', '')
        )

        get_holidaybookings_to_delete = rail.RepliconServicePageOperator(
            task_id='get_holidaybookings_to_delete',
            endpoint="/services/TimeOffListService1.svc/GetData",
            data=get_holidaybookings_delete_payload,
            page_handler=page_handler,
            all_result_data_handler=get_timeoff_uris
        )

        is_holidaybookings_to_delete = rail.IfOperator(
            task_id='is_holidaybookings_to_delete',
            test="{{ result('get_holidaybookings_to_delete') | length > 0 }}",
            yes_task="create_timeoff_delete_batch",
            no_task="dagrun_log_to_sumo",
        )

        create_timeoff_delete_batch = rail.RepliconServiceOperator(
            task_id='create_timeoff_delete_batch',
            endpoint="/services/TimeOffService1.svc/CreateTimeOffDeleteBatch",
            data=lambda: {
                "timeOffUris": rail.result('get_holidaybookings_to_delete')
            }
        )

        execute_timeoff_delete_batch = rail.RepliconServiceOperator(
            task_id='execute_timeoff_delete_batch',
            endpoint="/services/TimeOffService1.svc/ExecuteTimeOffDeleteBatch",
            data={
                "timeOffDeleteBatchUri": "{{ result('create_timeoff_delete_batch') }}"
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_timeofftype_policy_summary
        get_timeofftype_policy_summary >> put_timeofftype_assignments_user >> \
            get_holiday_timeoff_uri >> get_holidaybookings_to_delete >> is_holidaybookings_to_delete
        is_holidaybookings_to_delete >> rail.Label(
            'Yes') >> create_timeoff_delete_batch >> execute_timeoff_delete_batch >> dagrun_log_to_sumo
        is_holidaybookings_to_delete >> rail.Label(
            'No') >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_delete_holiday_booking_dag)
