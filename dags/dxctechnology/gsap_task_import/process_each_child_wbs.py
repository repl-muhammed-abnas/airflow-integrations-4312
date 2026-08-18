from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.gsap_task_import.utils import request_payload
from dxctechnology.gsap_task_import.utils import custom_methods
from dxctechnology.gsap_task_import.utils import response_filters

# config: https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/gsap_task_import/config.py


# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_child_wbs_dagid,
        description=f"DXCTechnology GSAP task import process each GSAP child WBS {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_process_wbs_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run")

        can_run_batch_task = rail.IfOperator(
            task_id= "can_run_batch_task",
            test= lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='false').lower() == 'true',
            yes_task="batch_task",
            no_task= "get_parent_task_details_from_feed"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_parent_task_details_from_feed',
            end_task="empty_task_end",
        )

        empty_task_end = rail.EmptyOperator(task_id="empty_task_end")

        get_parent_task_details_from_feed = rail.QueryCollectionOperator(
            task_id="get_parent_task_details_from_feed",
            query="SELECT * FROM valid_input_data WHERE wbs = :WBS",
            query_params={
                "WBS": "{{dag_run.conf.wbs}}"
            },
            name = "get_parent_task_details_from_feed"
        )

        has_input_tasks_for_project = rail.IfOperator(
            task_id="has_input_tasks_for_project",
            test="{{result('get_parent_task_details_from_feed', 'length') > 0}}",
            yes_task='get_project_details',
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=lambda dag_run: request_payload.get_project_detail_payload(
                dag_run, wbs_type="child"),
            response_filter=lambda response: (response.json()['d'][0:1] or [
                {"projectDetails": None}])[0]['projectDetails']
        )

        is_timeentry_allowed_against_project = rail.IfOperator(
            task_id="is_timeentry_allowed_against_project",
            test=lambda: rail.result("get_project_details")[
                'isTimeEntryAllowed'],
            yes_task="remove_timeentry_against_project",
            no_task="get_all_billing_keys_from_replicon"
        )

        remove_timeentry_against_project = rail.RepliconServiceOperator(
            task_id="remove_timeentry_against_project",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=request_payload.get_remove_timeentry_payload
        )

        get_project_team_member_details = rail.RepliconServiceOperator(
            task_id="get_project_team_member_details",
            endpoint="/services/ProjectService1.svc/GetAllProjectTeamMemberDetails",
            data=lambda dag_run: request_payload.get_project_team_member_payload(
                dag_run, wbs_type="gsap"),
            data_handler=lambda data: list(
                map(lambda assignment: assignment['resource']['uri'], data))
        )

        get_all_billing_keys_from_replicon = rail.RepliconServiceOperator(
            task_id="get_all_billing_keys_from_replicon",
            endpoint="/services/TaskService1.svc/GetDescendantTaskDetails",
            data=lambda: request_payload.get_project_tasks_payload(
                wbs_type="gsap"),
            response_filter=response_filters.get_descendant_task_details_filter
        )

        valid_task_records_for_child_project = rail.QueryCollectionOperator(
            task_id="valid_task_records_for_child_project",
            query="""SELECT * FROM get_parent_task_details_from_feed
                WHERE (NULLIF(task_start_date, '') IS NOT NULL AND NULLIF(task_end_date, '') IS NOT NULL) """,
            name="valid_task_records_for_child_project"
        )

        has_valid_task_records_for_child_project = rail.IfOperator(
            task_id="has_valid_task_records_for_child_project",
            test="{{result('valid_task_records_for_child_project','length') > 0}}",
            yes_task="is_timeentry_allowed_against_project",
        )

        project_has_any_task = rail.IfOperator(
            task_id="project_has_any_task",
            test=lambda: rail.result(
                "get_all_billing_keys_from_replicon") != [],
            yes_task="process_each_billing_task",
            no_task="finish"
        )

        process_each_billing_task = rail.TriggerDagRunForEachItemOperator(
            task_id="process_each_billing_task",
            items="{{result('get_all_billing_keys_from_replicon') | to_json }}",
            trigger_dag_id=config.process_each_child_wbs_billing_key_dagid,
            conf=lambda item, dag_run: custom_methods.get_trigger_conf(item, dag_run, True),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        wait_for_process_each_billing_task = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_billing_task',
            dag_runs='{{ result("process_each_billing_task") }}',
            execution_timeout=timedelta(
                    days=config.execution_timeout_days),
        )

        finish = rail.EmptyOperator(task_id = "finish")

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> empty_task_end >> log_to_sumo
        can_run_batch_task >> rail.Label("No") >> get_parent_task_details_from_feed

        get_parent_task_details_from_feed >> has_input_tasks_for_project
        has_input_tasks_for_project >> rail.Label("Yes") >> get_project_details
        has_input_tasks_for_project >> rail.Label("No") >> finish

        get_project_details >> valid_task_records_for_child_project

        valid_task_records_for_child_project >> has_valid_task_records_for_child_project >> rail.Label("Yes") >> \
            is_timeentry_allowed_against_project >> rail.Label(
                "Yes") >> remove_timeentry_against_project
        has_valid_task_records_for_child_project >> rail.Label("No") >> finish
        remove_timeentry_against_project >> get_all_billing_keys_from_replicon >> get_project_team_member_details \
            >> project_has_any_task

        is_timeentry_allowed_against_project >> rail.Label(
            "No") >> get_all_billing_keys_from_replicon

        project_has_any_task >> rail.Label("Yes") >> process_each_billing_task
        project_has_any_task >> rail.Label("No") >> finish >> empty_task_end
        process_each_billing_task >> wait_for_process_each_billing_task >> empty_task_end >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
