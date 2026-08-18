"""
Process Supervisor Check - GuestTek Talent User Import Child DAG

Validates and processes supervisor assignments for users after initial processing.
"""
from datetime import timedelta
from airflow.models import Variable
import rail
from guesttekinteractive.talent_user_import.utils import request_payload, response_filters
from guesttekinteractive.talent_user_import import config as base_config

null = None


def create_child_dag_wbs(config):
    """Create child DAG for processing and validating supervisor assignments."""
    with rail.create_airflow_dag(
        dag_id=config.processs_supervisor,
        description='GuestTek Talent User Import - Process Supervisor Check',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_supervisor,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        filter_user_logs = rail.FilterLogEntriesOperator(
            task_id='filter_user_logs',
            log='{{ dag_run.conf.user_log }}',
            properties={'action': ['Add User', 'Update User']},
            severity=['Success']
        )

        has_users_to_process = rail.IfOperator(
            task_id='has_users_to_process',
            test="{{ result('filter_user_logs','length') > 0 }}",
            yes_task='search_supervisor',
            no_task='finish'
        )

        search_supervisor = rail.RepliconServiceOperator(
            task_id='search_supervisor',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_supervisor_data_payload,
            data_handler=response_filters.get_filtered_user_data
        )

        has_supervisor = rail.IfOperator(
            task_id='has_supervisor',
            test="{{ result('search_supervisor') | is_truthy }}",
            yes_task='check_supervisor_permission',
            no_task='finish'
        )

        check_supervisor_permission = rail.IfOperator(
            task_id='check_supervisor_permission',
            test=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('search_supervisor')[0].get('permissionSets', []),
                'displayText',
                base_config.SUPERVISOR_PERMISSION,
                'uri'),
            yes_task='assign_supervisor',
            no_task='update_supervisor_permission'
        )

        update_supervisor_permission = rail.RepliconServiceOperator(
            task_id='update_supervisor_permission',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: request_payload.get_update_supervisor_permission_payload(
                dag_run, supervisor_result_task_id='search_supervisor')
        )

        assign_supervisor = rail.RepliconServiceOperator(
            task_id='assign_supervisor',
            endpoint='/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange',
            data=lambda dag_run: request_payload.get_supervisor_assignment_payload(
                dag_run, rail.result('search_supervisor')[0]['userDetails']['uri']
            )
        )

        finish = rail.EmptyOperator(task_id='finish')

        filter_user_logs >> has_users_to_process >> [search_supervisor, finish]
        search_supervisor >> has_supervisor >> [check_supervisor_permission, finish]
        check_supervisor_permission >> [assign_supervisor, update_supervisor_permission]
        update_supervisor_permission >> assign_supervisor
        assign_supervisor >> finish

    return dag


rail.for_each_instance(create_child_dag_wbs)
