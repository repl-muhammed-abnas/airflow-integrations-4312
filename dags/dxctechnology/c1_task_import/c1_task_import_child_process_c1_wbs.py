from datetime import timedelta
import rail
from dxctechnology.c1_task_import import request_payload
from dxctechnology.c1_task_import import custom_method

# config: https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_task_import/config.py


# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_c1_task_import_child_c1_wbs_{config.instance}",
        description=f"DXCTechnology C1 task import child WBS {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
        max_active_runs=config.child_dag_process_wbs_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run")

        get_input_tasks_for_project = rail.QueryCollectionOperator(
            task_id="get_input_tasks_for_project",
            query="SELECT * FROM valid_input_taskdata_collection WHERE wbs = :WBS",
            query_params={
                "WBS": "{{dag_run.conf.wbs}}"
            }
        )

        has_input_tasks_for_project = rail.IfOperator(
            task_id="has_input_tasks_for_project",
            test="{{result('get_input_tasks_for_project', 'length') > 0}}",
            yes_task='get_project_details',
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=lambda dag_run: request_payload.get_project_detail_payload(
                dag_run, wbs_type="c1"),
            response_filter=lambda response: (response.json()['d'][0:1] or [
                {"projectDetails": None}])[0]['projectDetails']
        )

        does_project_exist = rail.IfOperator(
            task_id="does_project_exist",
            test="{{ result('get_project_details') is not none }}",
            yes_task="get_child_wbs_details",
            no_task="log_project_doesnt_exist",
        )

        log_project_doesnt_exist = rail.WriteLogOperator(
            task_id="log_project_doesnt_exist",
            message='{{dag_run.conf.wbs}} is not present in Replicon',
            items='{{ result("get_input_tasks_for_project") }}',
            severity='Exception',
            properties={
                'wbs': "{{dag_run.conf.wbs}}",
                'task': '{{ item.taskname }}',
                'status': 'Exception',
                'details': 'WBS Element not present in Replicon'
            }
        )

        get_child_wbs_details = rail.RepliconServiceOperator(
            task_id="get_child_wbs_details",
            endpoint="/services/ProjectListService1.svc/GetData",
            data=request_payload.get_getdata_payload,
            response_filter=custom_method.get_valid_wbs
        )

        has_any_child_wbs = rail.IfOperator(
            task_id="has_any_child_wbs",
            test="{{result('get_child_wbs_details') | is_truthy}}",
            yes_task="process_each_child_wbs",
            no_task=["valid_task_records_for_project",
                     "invalid_task_records_for_project"]
        )

        process_each_child_wbs = rail.TriggerDagRunForEachItemOperator(
            task_id="process_each_child_wbs",
            items="{{result('get_child_wbs_details') | to_json}}",
            trigger_dag_id=f"dxctechnology_c1_task_import_child_compass_wbs_{config.instance}",
            conf=lambda item, dag_run: {
                "file_name": dag_run.conf['file_name'],
                "child_wbs": item['child_wbs_name'].split(" - ")[0],
                "child_wbs_uri": item['child_wbs_uri'],
                "wbs": dag_run.conf['wbs'],
                "parent_wbs_uri": rail.result("get_project_details")['uri']
            }
        )

        is_timeentry_allowed_against_project = rail.IfOperator(
            task_id="is_timeentry_allowed_against_project",
            test=lambda: rail.result("get_project_details")[
                'isTimeEntryAllowed'],
            yes_task="remove_timeentry_against_project",
            no_task=["get_project_team_member_details",
                     "get_all_project_tasks_from_replicon"]
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
                dag_run, wbs_type="c1"),
            data_handler=lambda data: list(
                map(lambda assignment: assignment['resource']['uri'], data))
        )

        get_all_project_tasks_from_replicon = rail.RepliconServiceOperator(
            task_id="get_all_project_tasks_from_replicon",
            endpoint="/services/TaskService1.svc/GetDescendantTaskDetails",
            data=lambda: request_payload.get_project_tasks_payload(
                wbs_type="c1"),
        )

        valid_task_records_for_project = rail.QueryCollectionOperator(
            task_id="valid_task_records_for_project",
            query="""SELECT * FROM get_input_tasks_for_project
                WHERE (NULLIF(startdate, '') IS NOT NULL AND NULLIF(enddate, '') IS NOT NULL) """
        )

        has_valid_task_records_for_project = rail.IfOperator(
            task_id="has_valid_task_records_for_project",
            test="{{result('valid_task_records_for_project','length') > 0}}",
            yes_task="is_timeentry_allowed_against_project",
        )

        invalid_task_records_for_project = rail.QueryCollectionOperator(
            task_id="invalid_task_records_for_project",
            query="""SELECT * FROM get_input_tasks_for_project WHERE NULLIF(enddate, '') IS NULL OR NULLIF(startdate,'') IS NULL"""
        )

        has_invalid_task_records_for_project = rail.IfOperator(
            task_id="has_invalid_task_records_for_project",
            test="{{result('invalid_task_records_for_project','length') > 0}}",
            yes_task="log_invalid_task_records_for_project",
        )

        log_invalid_task_records_for_project = rail.WriteLogOperator(
            task_id='log_invalid_task_records_for_project',
            items="{{result('invalid_task_records_for_project')}}",
            message=custom_method.get_log_missing_required_fields_msg,
            severity="Exception",
            properties=custom_method.get_log_invalid_task_records_for_project,
        )

        project_has_any_task = rail.IfOperator(
            task_id="project_has_any_task",
            test=lambda: rail.result(
                "get_all_project_tasks_from_replicon") != [],
            yes_task="process_each_task",
            no_task="create_tasks_to_project"
        )

        create_tasks_to_project = rail.TriggerDagRunForEachItemOperator(
            task_id="create_tasks_to_project",
            items="{{result('valid_task_records_for_project')}}",
            trigger_dag_id=f"dxctechnology_c1_task_import_child_create_c1_task_{config.instance}",
            conf=lambda item, dag_run: {
                "file_name": dag_run.conf['file_name'],
                "project_name": dag_run.conf['wbs'],
                "project_uri": rail.result('get_project_details')['uri'],
                "project_startdate": rail.result("get_project_details")['timeEntryDateRange']['startDate'],
                "project_enddate": rail.result("get_project_details")['timeEntryDateRange']['endDate'],
                "task_name": item['taskname'],
                "task_code": item['taskcode'] if item['taskcode'] else None,
                "start_date": item['startdate'] if item['startdate'] else None,
                "end_date": item['enddate'] if item['enddate'] else None,
                "user_list": rail.result("get_project_team_member_details"),
                "existing_tasks": None,
            },
            execution_timeout=timedelta(days=14),
        )

        wait_for_create_tasks_to_project = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_tasks_to_project',
            dag_runs='{{ result("create_tasks_to_project") }}',
            execution_timeout=timedelta(
                    days=config.execution_timeout_days),
        )

        process_each_task = rail.TriggerDagRunForEachItemOperator(
            task_id="process_each_task",
            items="{{result('valid_task_records_for_project')}}",
            trigger_dag_id=f"dxctechnology_c1_task_import_child_update_c1_task_{config.instance}",
            conf=lambda item, dag_run: {
                "file_name": dag_run.conf['file_name'],
                "project_name": dag_run.conf['wbs'],
                "project_uri": rail.result('get_project_details')['uri'],
                "project_startdate": rail.result("get_project_details")['timeEntryDateRange']['startDate'],
                "project_enddate": rail.result("get_project_details")['timeEntryDateRange']['endDate'],
                "task_name": item['taskname'],
                "task_code": item['taskcode'] if item['taskcode'] else None,
                "start_date": item['startdate'] if item['startdate'] else None,
                "end_date": item['enddate'] if item['enddate'] else None,
                "user_list": rail.result("get_project_team_member_details"),
                'existing_tasks': rail.find_first_by_attr_and_get_attr(rail.result("get_all_project_tasks_from_replicon"),
                                                                       'task.name', item['taskname'], 'task'),
            },
            execution_timeout=timedelta(days=14),
        )

        wait_for_process_each_task = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_task',
            dag_runs='{{ result("process_each_task") }}',
            execution_timeout=timedelta(
                    days=config.execution_timeout_days),
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            items='{{ result("get_input_tasks_for_project") }}',
            message='{{ get_error_message() }}',
            properties={
                'wbs': '{{ dag_run.conf.wbs }}',
                'task': '{{item.taskname}}',
                'status': "Error",
                'details': '{{ get_error_message() }}'
            },
        )

        get_input_tasks_for_project >> has_input_tasks_for_project
        has_input_tasks_for_project >> rail.Label("Yes") >> get_project_details

        get_project_details >> does_project_exist >> rail.Label("Yes") >> get_child_wbs_details \
            >> has_any_child_wbs >> rail.Label("Yes") >> process_each_child_wbs >> valid_task_records_for_project
        process_each_child_wbs >> invalid_task_records_for_project
        does_project_exist >> rail.Label("No") >> log_project_doesnt_exist
        has_any_child_wbs >> rail.Label(
            "No") >> [valid_task_records_for_project, invalid_task_records_for_project]

        valid_task_records_for_project >> has_valid_task_records_for_project >> rail.Label("Yes") >> \
            is_timeentry_allowed_against_project >> rail.Label(
                "Yes") >> remove_timeentry_against_project
        remove_timeentry_against_project >> [
            get_project_team_member_details, get_all_project_tasks_from_replicon] >> project_has_any_task

        invalid_task_records_for_project >> has_invalid_task_records_for_project >> rail.Label("Yes") >> log_invalid_task_records_for_project >> rail.Label(
            "On error") >> catch_and_log_errors
        is_timeentry_allowed_against_project >> rail.Label(
            "No") >> [get_project_team_member_details, get_all_project_tasks_from_replicon]

        project_has_any_task >> rail.Label("Yes") >> process_each_task
        process_each_task >> wait_for_process_each_task >> rail.Label(
            "On error") >> catch_and_log_errors

        project_has_any_task >> rail.Label("No") >> create_tasks_to_project
        create_tasks_to_project >> wait_for_create_tasks_to_project >> rail.Label(
            "On error") >> catch_and_log_errors
        log_project_doesnt_exist >> rail.Label(
            "On error") >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
