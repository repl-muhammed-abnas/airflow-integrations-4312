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
        dag_id=config.process_each_gsap_wbs_dagid,
        description=f"DXCTechnology GSAP task import child WBS {config.instance}",
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
            no_task= "get_input_tasks_for_project"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_input_tasks_for_project',
            end_task="catch_and_log_errors",
        )

        get_input_tasks_for_project = rail.QueryCollectionOperator(
            task_id="get_input_tasks_for_project",
            query="SELECT * FROM valid_input_data WHERE wbs = :WBS",
            query_params={
                "WBS": "{{dag_run.conf.wbs}}"
            },
            name= "get_input_tasks_for_project"
        )

        has_input_tasks_for_project = rail.IfOperator(
            task_id="has_input_tasks_for_project",
            test="{{result('get_input_tasks_for_project', 'length') > 0}}",
            yes_task='get_project_details',
            no_task="finish_no_task_for_projects"
        )

        finish_no_task_for_projects = rail.EmptyOperator(
            task_id = "finish_no_task_for_projects"
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=lambda dag_run: request_payload.get_project_detail_payload(
                dag_run, wbs_type="gsap"),
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
                'task': '{{ item.task_name }}',
                'status': 'Exception',
                'details': 'WBS Element not present in Replicon'
            }
        )

        get_child_wbs_details = rail.RepliconServiceOperator(
            task_id="get_child_wbs_details",
            endpoint="/services/ProjectListService1.svc/GetData",
            data=request_payload.get_getdata_payload,
            response_filter=response_filters.get_valid_wbs
        )

        has_any_child_wbs = rail.IfOperator(
            task_id="has_any_child_wbs",
            test="{{result('get_child_wbs_details') | is_truthy}}",
            yes_task="process_each_child_wbs",
            no_task="invalid_task_records_for_project"
        )

        process_each_child_wbs = rail.TriggerDagRunForEachItemOperator(
            task_id="process_each_child_wbs",
            items="{{result('get_child_wbs_details') | to_json}}",
            trigger_dag_id=config.process_each_child_wbs_dagid,
            conf=lambda item, dag_run: {
                "file_name": dag_run.conf['file_name'],
                "child_wbs": item['child_wbs_name'].split(" - ")[0],
                "child_wbs_uri": item['child_wbs_uri'],
                "wbs": dag_run.conf['wbs'],
                "parent_wbs_uri": rail.result("get_project_details")['uri'],
                "task_type_oef_uri": dag_run.conf['task_type_oef_uri'],
                "gsap_task_option_uri": dag_run.conf['gsap_task_option_uri']
            }
        )

        valid_task_records_for_project = rail.QueryCollectionOperator(
            task_id="valid_task_records_for_project",
            query="""SELECT * FROM get_input_tasks_for_project
                WHERE (NULLIF(task_start_date, '') IS NOT NULL AND NULLIF(task_end_date, '') IS NOT NULL) """,
            name= "valid_task_records_for_project"
        )

        has_valid_task_records_for_project = rail.IfOperator(
            task_id="has_valid_task_records_for_project",
            test="{{result('valid_task_records_for_project','length') > 0}}",
            yes_task="is_timeentry_allowed_against_project",
            no_task="finish_no_task_for_projects"
        )

        invalid_task_records_for_project = rail.QueryCollectionOperator(
            task_id="invalid_task_records_for_project",
            query="""SELECT * FROM get_input_tasks_for_project WHERE NULLIF(task_end_date, '') IS NULL OR NULLIF(task_start_date,'') IS NULL""",
            name= "invalid_task_records_for_project"
        )

        has_invalid_task_records_for_project = rail.IfOperator(
            task_id="has_invalid_task_records_for_project",
            test="{{result('invalid_task_records_for_project','length') > 0}}",
            yes_task="log_invalid_task_records_for_project",
            no_task="valid_task_records_for_project"
        )

        log_invalid_task_records_for_project = rail.WriteLogOperator(
            task_id='log_invalid_task_records_for_project',
            items="{{result('invalid_task_records_for_project')}}",
            message=custom_methods.get_log_missing_required_fields_msg,
            severity="Exception",
            properties=custom_methods.get_log_invalid_task_records_for_project,
        )

        is_timeentry_allowed_against_project = rail.IfOperator(
            task_id="is_timeentry_allowed_against_project",
            test=lambda: rail.result("get_project_details")[
                'isTimeEntryAllowed'],
            yes_task="remove_timeentry_against_project",
            no_task="get_all_billing_keys_from_replicon",
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

        # we will process for each billing key
        get_all_billing_keys_from_replicon = rail.RepliconServiceOperator(
            task_id="get_all_billing_keys_from_replicon",
            endpoint="/services/TaskService1.svc/GetDescendantTaskDetails",
            data=lambda: request_payload.get_project_tasks_payload(
                wbs_type="gsap"),
            response_filter=response_filters.get_descendant_task_details_filter
        )

        project_has_any_task = rail.IfOperator(
            task_id="project_has_any_task",
            test=lambda: rail.result(
                "get_all_billing_keys_from_replicon") != [],
            yes_task="process_each_billing_task",
            no_task="log_no_billing_key_present"
        )

        log_no_billing_key_present = rail.WriteLogOperator(
            task_id="log_no_billing_key_present",
            message='{{dag_run.conf.wbs}} No billing key present',
            items='{{ result("get_input_tasks_for_project") }}',
            severity='Exception',
            properties={
                'wbs': "{{dag_run.conf.wbs}}",
                'task': '{{ item.task_name }}',
                'status': 'Exception',
                'details': 'No billing key present'
            }
        )

        process_each_billing_task = rail.TriggerDagRunForEachItemOperator(
            task_id="process_each_billing_task",
            items="{{result('get_all_billing_keys_from_replicon') | to_json }}",
            trigger_dag_id=config.process_each_gsap_wbs_billing_key_dagid,
            conf=custom_methods.get_trigger_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        wait_for_process_each_billing_task = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_billing_task',
            dag_runs='{{ result("process_each_billing_task") }}',
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
                'task': '{{item.task_name}}',
                'status': "Error",
                'details': '{{ get_error_message() }}'
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_input_tasks_for_project

        get_input_tasks_for_project >> has_input_tasks_for_project
        has_input_tasks_for_project >> rail.Label("Yes") >> get_project_details
        has_input_tasks_for_project >> rail.Label("No") >> finish_no_task_for_projects >> rail.Label("On Error") >> catch_and_log_errors
        get_project_details >> does_project_exist >> rail.Label("Yes") >> get_child_wbs_details \
            >> has_any_child_wbs >> rail.Label("Yes") >> process_each_child_wbs >> valid_task_records_for_project
        does_project_exist >> rail.Label("No") >> log_project_doesnt_exist
        has_any_child_wbs >> rail.Label(
            "No") >> invalid_task_records_for_project

        valid_task_records_for_project >> has_valid_task_records_for_project >> rail.Label("Yes") >> \
            is_timeentry_allowed_against_project >> rail.Label(
                "Yes") >> remove_timeentry_against_project
        has_valid_task_records_for_project >> rail.Label("No") >> finish_no_task_for_projects
        remove_timeentry_against_project >> get_all_billing_keys_from_replicon\
            >> get_project_team_member_details >> project_has_any_task

        invalid_task_records_for_project >> has_invalid_task_records_for_project >> rail.Label("Yes") \
            >> log_invalid_task_records_for_project >> valid_task_records_for_project

        has_invalid_task_records_for_project >> rail.Label("No") >> valid_task_records_for_project
        is_timeentry_allowed_against_project >> rail.Label(
            "No") >> get_all_billing_keys_from_replicon

        project_has_any_task >> rail.Label("Yes") >> process_each_billing_task
        process_each_billing_task >> wait_for_process_each_billing_task >> rail.Label(
            "On error") >> catch_and_log_errors

        project_has_any_task >> rail.Label("No") >> log_no_billing_key_present >> rail.Label(
            "On error") >> catch_and_log_errors
        log_project_doesnt_exist >> rail.Label(
            "On error") >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
