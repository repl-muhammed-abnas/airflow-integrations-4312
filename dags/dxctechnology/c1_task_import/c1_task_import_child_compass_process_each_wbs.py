from datetime import timedelta
import ast
import rail
from airflow.models import Variable
from dxctechnology.c1_task_import import request_payload
from dxctechnology.c1_task_import import custom_method

# config: https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_task_import/config.py


# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_c1_task_import_child_compass_wbs_{config.instance}",
        description=f"DXCTechnology C1 task import child Compass WBS {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
        max_active_runs=config.child_dag_process_wbs_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run")

        get_input_tasks_for_compass_project = rail.QueryCollectionOperator(
            task_id="get_input_tasks_for_compass_project",
            query="SELECT * FROM valid_input_taskdata_collection WHERE wbs = :WBS",
            query_params={
                "WBS": "{{dag_run.conf.wbs}}"
            }
        )

        get_compass_project_details = rail.RepliconServiceOperator(
            task_id='get_compass_project_details',
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=lambda dag_run: request_payload.get_project_detail_payload(
                dag_run, wbs_type="compass"),
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                {"projectDetails": None}])[0]['projectDetails']
        )

        does_project_exist = rail.IfOperator(
            task_id="does_project_exist",
            test="{{ result('get_compass_project_details') is not none }}",
            yes_task="is_division_present",
        )

        is_division_present = rail.IfOperator(
            task_id="is_division_present",
            test="{{result('get_compass_project_details').division | is_truthy}}",
            yes_task="get_division"
        )

        get_division = rail.RepliconServiceOperator(
            task_id="get_division",
            endpoint="/services/DivisionService1.svc/GetDivisionDetails",
            data={
                "divisionUri": "{{result('get_compass_project_details').division.uri}}"
            },
            response_filter=custom_method.map_division_name_or_code
        )

        def get_airflow_var():

            value = Variable.get(config.division_variable)
            value = ast.literal_eval(value)
            if not isinstance(value, list):
                # pylint: disable=line-too-long
                raise Exception(f"The variable `{config.division_variable}` is not in correct format. Excepted `list` got {type(value)}. Found Variable value: `{value}`")
            if len(value) <= 0:
                raise Exception(f"Variable {config.division_variable} does not have any values present")
            return value

        get_division_list_variable = rail.PythonOperator(
          task_id = "get_division_list_variable",
          python_callable = get_airflow_var
        )

        is_division_compass = rail.IfOperator(
          task_id="is_division_compass",
          test=lambda: rail.result('get_division') in rail.result('get_division_list_variable'),
          yes_task="valid_task_records_for_compass_project",
        )

        valid_task_records_for_compass_project = rail.QueryCollectionOperator(
            task_id="valid_task_records_for_compass_project",
            query="""SELECT * FROM get_input_tasks_for_compass_project
                WHERE NULLIF(startdate, '') IS NOT NULL AND NULLIF(enddate, '') IS NOT NULL"""
        )

        has_valid_task_records_for_compass_project = rail.IfOperator(
            task_id="has_valid_task_records_for_compass_project",
            test="{{result('valid_task_records_for_compass_project','length') > 0}}",
            yes_task=["get_all_project_tasks_from_replicon", "get_child_project_team_member_details"],
        )

        get_child_project_team_member_details = rail.RepliconServiceOperator(
            task_id="get_child_project_team_member_details",
            endpoint="/services/ProjectService1.svc/GetAllProjectTeamMemberDetails",
            data=lambda dag_run: request_payload.get_project_team_member_payload(
                dag_run, wbs_type="compass"),
            data_handler=lambda data: list(
                map(lambda assignment: assignment['resource']['uri'], data))
        )

        get_all_project_tasks_from_replicon = rail.RepliconServiceOperator(
            task_id="get_all_project_tasks_from_replicon",
            endpoint="/services/TaskService1.svc/GetDescendantTaskDetails",
            data=lambda: request_payload.get_project_tasks_payload(
                wbs_type="compass"),
        )

        is_timeentry_allowed_against_project = rail.IfOperator(
            task_id="is_timeentry_allowed_against_project",
            test=lambda: rail.result("get_compass_project_details")[
                'isTimeEntryAllowed'],
            yes_task="remove_timeentry_against_project",
            no_task="project_has_any_task"
        )

        remove_timeentry_against_project = rail.RepliconServiceOperator(
            task_id="remove_timeentry_against_project",
            endpoint="/services/ProjectService1.svc/UpdateAllowTimeEntryAgainstTasksOnly",
            data= {
                "projectUri": "{{result('get_compass_project_details').uri}}",
                "allowTimeEntryAgainstTasksOnly": "true"
            }
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
            items="{{result('valid_task_records_for_compass_project')}}",
            trigger_dag_id=f"dxctechnology_c1_task_import_child_create_compass_task_{config.instance}",
            conf=lambda item, dag_run: {
                "file_name": dag_run.conf['file_name'],
                "project_name": dag_run.conf['child_wbs'],
                "project_uri": rail.result('get_compass_project_details')['uri'],
                "project_startdate": rail.result("get_compass_project_details")['timeEntryDateRange']['startDate'],
                "project_enddate": rail.result("get_compass_project_details")['timeEntryDateRange']['endDate'],
                "task_name": item['taskname'],
                "task_code": item['taskcode'] if item['taskcode'] else None,
                "start_date": item['startdate'] if item['startdate'] else None,
                "end_date": item['enddate'] if item['enddate'] else None,
                "user_list": rail.result("get_child_project_team_member_details"),
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
            items="{{result('valid_task_records_for_compass_project')}}",
            trigger_dag_id=f"dxctechnology_c1_task_import_child_update_compass_task_{config.instance}",
            conf=lambda item, dag_run: {
                "file_name": dag_run.conf['file_name'],
                "project_name": dag_run.conf['child_wbs'],
                "project_uri": rail.result('get_compass_project_details')['uri'],
                "project_startdate": rail.result("get_compass_project_details")['timeEntryDateRange']['startDate'],
                "project_enddate": rail.result("get_compass_project_details")['timeEntryDateRange']['endDate'],
                "task_name": item['taskname'],
                "task_code": item['taskcode'] if item['taskcode'] else None,
                "start_date": item['startdate'] if item['startdate'] else None,
                "end_date": item['enddate'] if item['enddate'] else None,
                "user_list": rail.result("get_child_project_team_member_details"),
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

        [get_input_tasks_for_compass_project, get_compass_project_details] >> does_project_exist >> rail.Label(
            "Yes") >> is_division_present >> rail.Label("Yes") >> get_division >> get_division_list_variable >> is_division_compass \
            >> rail.Label("Yes") >> valid_task_records_for_compass_project

        valid_task_records_for_compass_project >> has_valid_task_records_for_compass_project >> rail.Label("Yes") >> \
            [get_child_project_team_member_details,
                get_all_project_tasks_from_replicon] >> is_timeentry_allowed_against_project >> rail.Label("No") >> project_has_any_task
        is_timeentry_allowed_against_project >> rail.Label("Yes") >> remove_timeentry_against_project >> project_has_any_task
        project_has_any_task >> rail.Label("Yes") >> process_each_task
        process_each_task >> wait_for_process_each_task

        project_has_any_task >> rail.Label("No") >> create_tasks_to_project
        create_tasks_to_project >> wait_for_create_tasks_to_project

    return dag


rail.for_each_instance(create_child_dag)
