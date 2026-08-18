from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.gsap_wbs_import_v3.utils import request_payload


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.process_iwo_element_dagid,
        description='DXC_GSAP_WBS_Automation Process IWO Element',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_iwo_element,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_project_info_based_on_parent'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_project_info_based_on_parent',
            end_task='finish',
        )

        get_project_info_based_on_parent = rail.RepliconServiceOperator(
            task_id='get_project_info_based_on_parent',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={
                "projects": [
                    {
                        "name": "{{ dag_run.conf.parentwbs }}"
                    }
                ]
            }
        )

        is_project_exist = rail.IfOperator(
            task_id="is_project_exist",
            test=request_payload.is_iwo_project_exist,
            yes_task="query_all_childs",
            no_task="finish",
        )

        query_all_childs = rail.QueryCollectionOperator(
            task_id='query_all_childs',
            query="""SELECT DISTINCT WBS_Name FROM gsaprecordscollection
            WHERE (Parent_Project = '{{dag_run.conf.parentwbs }}' or WBS_Parent_Project = '{{dag_run.conf.parentwbs }}')"""
        )

        update_iwo_wbs_oef = rail.RepliconServiceOperator(
            task_id='update_iwo_wbs_oef',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.update_iwo_wbs_oef
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
        can_run_batch_task >> rail.Label('No') >> get_project_info_based_on_parent

        get_project_info_based_on_parent >> is_project_exist >> rail.Label(
            "Yes") >> query_all_childs >> update_iwo_wbs_oef >> finish

        is_project_exist >> rail.Label(
            "No") >> finish >> log_to_sumo >> can_fail_dag

        can_fail_dag >> rail.Label('Yes') >> fail_dagrun

    return dag


rail.for_each_instance(create_child_dag_wbs)
