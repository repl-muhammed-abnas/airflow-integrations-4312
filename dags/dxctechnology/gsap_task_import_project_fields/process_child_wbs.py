from datetime import timedelta, datetime as dt
from pendulum import datetime
import rail
from dxctechnology.gsap_task_import_project_fields.utils import request_payload
from dxctechnology.gsap_task_import_project_fields.utils import response_filter


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsap_project_field_task_import_sync_child_wbs_{config.instance}',
        description=f'Sync Child WBS GASP Task {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_wbs_dag_sync_gsap_task_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        start_of_year = datetime(year=dt.now().year, month=1, day=1).strftime("%d.%m.%Y")
        # ALL valid tasks for parent
        #pylint: disable=line-too-long
        query_project_attribute_entires = rail.QueryCollectionOperator(
            task_id="query_project_attribute_entires",
            query=f"""SELECT * FROM valid_input_records id
            WHERE wbs = :WBS
            AND date(substr(task_end_date, 7, 4) || '-' || substr(task_end_date, 4, 2) || '-' || substr(task_end_date, 1, 2), 'start of day') > date(substr('{start_of_year}', 7, 4) || '-' || substr('{start_of_year}', 4, 2) || '-' || substr('{start_of_year}', 1, 2), 'start of day')""",
            query_params={
                "WBS": "{{dag_run.conf.wbs}}",
            }
        )

        has_any_task_data = rail.IfOperator(
            task_id = "has_any_task_data",
            test="{{result('query_project_attribute_entires', 'length') > 0 }}",
            yes_task= "get_project_details"
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id="get_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=request_payload.get_child_project_details,
            response_filter=response_filter.map_get_project_details
        )

        is_wbs_present = rail.IfOperator(
            task_id="is_wbs_present",
            test="{{ result('get_project_details') | length > 0}}",
            yes_task="is_wbs_start_date_empty",
            no_task="fail_wbs_not_present",
        )

        fail_wbs_not_present = rail.FailOperator(
            task_id='fail_wbs_not_present',
            message="Project {{  dag_run.conf.childWbs }} Not found in Replicon"
        )

        is_wbs_start_date_empty = rail.IfOperator(
            task_id="is_wbs_start_date_empty",
            test=lambda: bool(rail.result('get_project_details')[
                              0]['start_date_year']),
            yes_task="is_wbs_in_progress",
            no_task="finish_wbs_start_date_empty",
        )

        finish_wbs_start_date_empty = rail.EmptyOperator(
            task_id='finish_wbs_start_date_empty',
        )

        is_wbs_in_progress = rail.IfOperator(
            task_id="is_wbs_in_progress",
            test=lambda: rail.result('get_project_details')[
                0]['status'] == "In Progress",
            yes_task="dummy_sync_each_gsap_task_project_level",
            no_task="finish_wbs_not_in_progress",
        )

        finish_wbs_not_in_progress = rail.EmptyOperator(
            task_id='finish_wbs_not_in_progress',
        )

        dummy_sync_each_gsap_task_project_level = rail.EmptyOperator(
            task_id = "dummy_sync_each_gsap_task_project_level"
        )

        sync_each_gsap_task_project_level = rail.trigger_parallel_dagrun(
            task_id='sync_each_gsap_task_project_level',
            parallel_count=config.parallel_dag_run_count,
            items="{{ result('query_project_attribute_entires') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_gsap_project_field_task_import_sync_each_child_wbs_gsap_task_child_{config.instance}',
            conf=lambda item, dag_run: {
                'WBS': dag_run.conf['childWbs'],
                'wbs_type': "child_wbs",
                'parent_wbs': dag_run.conf['wbs'],
                'task_name': item['task_name'],
                'task_code': item['task_code'] if item['task_code'] else "",
                'task_start_date': item['task_start_date'],
                'task_end_date': item['task_end_date'],
                'gsap_task_uri': dag_run.conf['gsap_task_uri'],
                'start_date_year': rail.result('get_project_details')[0]['start_date_year'],
                'start_date_month': rail.result('get_project_details')[0]['start_date_month'],
                'start_date_day': rail.result('get_project_details')[0]['start_date_day'],
                'end_date_year': rail.result('get_project_details')[0]['end_date_year'],
                'end_date_month': rail.result('get_project_details')[0]['end_date_month'],
                'end_date_day': rail.result('get_project_details')[0]['end_date_day'],
            }
        )

        finish_batch_task = rail.EmptyOperator(
            task_id = "finish_batch_task"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_fail_dag = rail.IfOperator(
            task_id = "can_fail_dag",
            test="{{ get_failed_upstream_task_ids() | is_truthy }}",
            yes_task="fail_dag"
        )

        fail_dag = rail.FailOperator(
            task_id = "fail_dag",
             message="{{ get_error_message() }}"
        )

        query_project_attribute_entires >> has_any_task_data >> rail.Label("Yes") >> get_project_details
        get_project_details >> is_wbs_present
        is_wbs_present >> rail.Label(
            "NO") >> fail_wbs_not_present >> finish_batch_task
        is_wbs_present >> rail.Label("YES") >> is_wbs_start_date_empty
        is_wbs_start_date_empty >> rail.Label("YES") >> is_wbs_in_progress
        is_wbs_start_date_empty >> rail.Label(
            "NO") >> finish_wbs_start_date_empty >> finish_batch_task
        is_wbs_in_progress >> rail.Label(
            "NO") >> finish_wbs_not_in_progress >> finish_batch_task
        is_wbs_in_progress >> rail.Label(
            "YES") >> dummy_sync_each_gsap_task_project_level >> sync_each_gsap_task_project_level
        sync_each_gsap_task_project_level >> finish_batch_task >> log_to_sumo >> can_fail_dag >> fail_dag

    return dag


rail.for_each_instance(create_dag)
