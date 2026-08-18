from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.gsap_wbs_import_v3.utils import request_payload
from dxctechnology.gsap_wbs_import_v3.utils import response_filter
from dxctechnology.gsap_wbs_import_v3.utils import python_callable_methods


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.process_child_projects_dagid,
        description='DXC_GSAP_WBS_Automation Process Child WBS',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_child_projects,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        column_names = ['taskname', 'uri', 'enabled', 'task_fullpath', 'parent_present',
                        'parent_task_name', 'parent_task_uri', 'levels', 'code', 'start_date', 'end_date']

        parentwbs = '{{ dag_run.conf.parentwbsname }}'
        childwbs = '{{ dag_run.conf.wbsname }}'

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_child_project_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_child_project_details',
            end_task='finish',
        )

        get_child_project_details = rail.RepliconServiceOperator(
            task_id='get_child_project_details',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={
                "projects": [
                    {
                        "name": childwbs
                    }
                ]
            },
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                {"projectDetails": None}])[0]['projectDetails']
        )

        get_parent_project_details = rail.RepliconServiceOperator(
            task_id='get_parent_project_details',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={
                "projects": [
                    {
                        "name": parentwbs
                    }
                ]
            },
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                {"projectDetails": None}])[0]['projectDetails']
        )

        does_project_exist = rail.IfOperator(
            task_id='does_project_exist',
            test='{{ result("get_child_project_details") is not none  }}',
            yes_task='is_division_gsap',
            no_task='finish',
        )

        is_division_gsap = rail.IfOperator(
            task_id='is_division_gsap',
            test=request_payload.test_division,
            yes_task='get_all_parent_oefs',
            no_task='update_oef_fields_c1_compass',
        )

        update_oef_fields_c1_compass = rail.RepliconServiceOperator(
            task_id='update_oef_fields_c1_compass',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=request_payload.update_oef_fields_c1_compass,
        )

        get_all_parent_oefs = rail.PythonOperator(
            task_id='get_all_parent_oefs',
            python_callable=python_callable_methods.get_all_oef_payload
        )

        update_oef_fields = rail.RepliconServiceOperator(
            task_id='update_oef_fields',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=request_payload.get_update_oef_payload,
        )

        get_all_tasks_of_parent_project = rail.RepliconServiceOperator(
            task_id='get_all_tasks_of_parent_project',
            endpoint='/services/TaskListService1.svc/GetData',
            data=lambda: request_payload.get_all_project_tasks_payload(
                rail.result("get_parent_project_details")['uri']),
            response_filter=response_filter.all_task_response_filter
        )

        does_parent_project_tasks_exist = rail.IfOperator(
            task_id='does_parent_project_tasks_exist',
            test=lambda: len(rail.result(
                'get_all_tasks_of_parent_project')) > 0,
            yes_task='parent_project_task_collection',
            no_task='finish',
        )

        parent_project_task_collection = rail.CreateCollectionOperator(
            task_id='parent_project_task_collection',
            source='{{result("get_all_tasks_of_parent_project") | to_json }}',
            columns=column_names,
            name='newtasklist'
        )

        get_all_tasks_of_child_project = rail.RepliconServiceOperator(
            task_id='get_all_tasks_of_child_project',
            endpoint='/services/TaskListService1.svc/GetData',
            data=lambda: request_payload.get_all_project_tasks_payload(
                rail.result("get_child_project_details")['uri']),
            response_filter=response_filter.all_task_response_filter
        )

        child_project_task_collection = rail.CreateCollectionOperator(
            task_id='child_project_task_collection',
            source='{{result("get_all_tasks_of_child_project") | to_json }}',
            columns=column_names,
            name='existingtasklist'
        )

        query_tasks_not_present_in_child_project = rail.QueryCollectionOperator(
            task_id='query_tasks_not_present_in_child_project',
            name = 'tasknotpresentchild',
            query="""SELECT * FROM newtasklist WHERE task_fullpath NOT IN (SELECT DISTINCT task_fullpath FROM existingtasklist)"""
        )

        does_tasks_list_to_be_created = rail.IfOperator(
            task_id='does_tasks_list_to_be_created',
            test='{{ result("query_tasks_not_present_in_child_project", "length") > 0}}',
            yes_task='get_all_unique_levels',
            no_task='finish'
        )

        get_all_unique_levels = rail.QueryCollectionOperator(
            task_id="get_all_unique_levels",
            query="SELECT DISTINCT levels FROM tasknotpresentchild ORDER BY levels ASC"
        )

        process_task_by_level = rail.TriggerDagRunForEachItemOperator(
            task_id="process_task_by_level",
            trigger_dag_id=config.process_tasks_by_level_dagid,
            items="{{result('get_all_unique_levels')}}",
            conf=lambda item, dag_run: {
                "level": item['levels'],
                "parent_wbs": dag_run.conf['parentwbsname'],
                "parent_wbs_uri": rail.result('get_parent_project_details')["uri"],
                "processing_wbs": dag_run.conf["wbsname"],
                "processing_wbs_uri": rail.result('get_child_project_details')["uri"],
                "task_type": dag_run.conf['tasktypeuri']
            },
            retries=0,
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout_days)
        )

        wait_for_process_tasks_by_level = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_tasks_by_level",
            dag_runs="{{result('process_task_by_level')}}",
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout_days)
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_fail_dag = rail.IfOperator(
            task_id = "can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task= "fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id = "fail_dagrun",
            message='{{ get_error_message() }}'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_child_project_details

        get_child_project_details >> get_parent_project_details >> does_project_exist >> rail.Label(
            'No') >> finish
        does_project_exist >> rail.Label(
            'Yes') >> is_division_gsap >> rail.Label('No') >> update_oef_fields_c1_compass >> finish
        is_division_gsap >> rail.Label('Yes') >> get_all_parent_oefs >> update_oef_fields
        update_oef_fields >> get_all_tasks_of_parent_project >> does_parent_project_tasks_exist >> rail.Label(
            'No') >> finish
        does_parent_project_tasks_exist >> rail.Label(
            'Yes') >> parent_project_task_collection >> get_all_tasks_of_child_project
        get_all_tasks_of_child_project >> child_project_task_collection >> query_tasks_not_present_in_child_project >> does_tasks_list_to_be_created
        does_tasks_list_to_be_created >> rail.Label('No') >> finish
        does_tasks_list_to_be_created >> rail.Label(
            'Yes') >> get_all_unique_levels >> process_task_by_level >> wait_for_process_tasks_by_level >> finish >> log_to_sumo >> can_fail_dag

        can_fail_dag >> rail.Label('Yes') >> fail_dagrun

    return dag


rail.for_each_instance(create_child_dag_wbs)
