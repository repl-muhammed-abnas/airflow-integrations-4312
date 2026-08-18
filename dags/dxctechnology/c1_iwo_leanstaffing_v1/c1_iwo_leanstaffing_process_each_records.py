from datetime import timedelta
from airflow.models import Variable
import rail
from dxctechnology.c1_iwo_leanstaffing_v1.utils import response_filter
from dxctechnology.c1_iwo_leanstaffing_v1.utils import request_payload
null = None
# pylint: disable=too-many-statements


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_records_dag_id,
        description=f'DXC_C1_Lean Staffing Process Each Record V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_leanstaffing_automation_child_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_log',
            end_task='catch_and_log_errors',
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        is_user_present = rail.IfOperator(
            task_id="is_user_present",
            test=lambda: bool(
                request_payload.get_dag_run_conf()['companycode']),
            yes_task='is_c1_user',
            no_task='log_no_user_present_in_replicon',
        )

        log_no_user_present_in_replicon = rail.WriteLogOperator(
            task_id="log_no_user_present_in_replicon",
             log='{{ result("create_log") }}',
            message="Required user \"'{{dag_run.conf.personnelnumber}}'\" not available in replicon",
            properties={
                'employeeid': '{{dag_run.conf.personnelnumber}}',
                'wbs': '{{dag_run.conf.wbselement}}',
                'billingtype': null,
                'status': 'Exception',
                'childwbs': ""
            }
        )

        is_c1_user = rail.IfOperator(
            task_id="is_c1_user",
            test='{{ dag_run.conf.companycode | matches(["C1"]) }}',
            yes_task='c1_lean_staffing_c1_automation_child',
            no_task='get_project_info_based_on_wbs_element',
        )

        c1_lean_staffing_c1_automation_child = rail.TriggerDagRunForEachItemOperator(
            task_id='c1_lean_staffing_c1_automation_child',
            retries=0,
            items=["one_run"],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_each_child,
            conf=request_payload.get_project_dag_c1_confg
        )

        wait_for_c1_lean_staffing_c1_automation_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_c1_lean_staffing_automation_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("c1_lean_staffing_c1_automation_child") }}',
        )

        get_project_info_based_on_wbs_element = rail.RepliconServiceOperator(
            task_id='get_project_info_based_on_wbs_element',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={
                "projects": [
                    {
                        "uri": null,
                        "name": "{{ dag_run.conf.wbselement }}",
                        "code": null,
                        "parameterCorrelationId": null
                    }
                ]
            }
        )

        project_exist_validation = rail.IfOperator(
            task_id="project_exist_validation",
            test=lambda: bool(rail.result(
                'get_project_info_based_on_wbs_element')[0]['error']),
            yes_task="log_project_validation",
            no_task="get_all_filter_defination",
        )

        log_project_validation = rail.WriteLogOperator(
            task_id='log_project_validation',
            log='{{ result("create_log") }}',
            message="Required WBS \"'{{dag_run.conf.wbselement}}'\" is not available in Replicon",
            properties={
                'employeeid': '{{dag_run.conf.personnelnumber}}',
                'wbs': '{{dag_run.conf.wbselement}}',
                'billingtype': null,
                'status': 'Exception',
                'childwbs': ""
            }
        )

        get_all_filter_defination = rail.RepliconServiceOperator(
            task_id="get_all_filter_defination",
            endpoint="services/ProjectListService1.svc/GetAllFilterDefinitions",
            data={},
            response_filter=response_filter.map_parent_wbs_oef_uri
        )

        get_all_columns = rail.RepliconServiceOperator(
            task_id="get_all_columns",
            endpoint="services/ProjectListService1.svc/GetAllColumns",
            data={},
            response_filter=response_filter.map_parent_column_uri
        )

        get_all_child_wbs_details = rail.RepliconServiceOperator(
            task_id="get_all_child_wbs_details",
            endpoint="services/ProjectListService1.svc/GetData",
            data=request_payload.get_child_wbs_payload,
            response_filter=response_filter.map_child_wbs
        )

        is_child_wbs_present = rail.IfOperator(
            task_id="is_child_wbs_present",
            test='{{ result("get_all_child_wbs_details") | length > 0 }}',
            yes_task='c1_lean_staffing_compass_automation_child',
            no_task='log_no_child_project_validation',
        )

        log_no_child_project_validation = rail.WriteLogOperator(
            task_id='log_no_child_project_validation',
            log='{{ result("create_log") }}',
            message="Required WBS \"'{{dag_run.conf.wbselement}}'\" don't have any child WBS to process",
            properties={
                'employeeid': '{{dag_run.conf.personnelnumber}}',
                'wbs': '{{dag_run.conf.wbselement}}',
                'billingtype': null,
                'status': 'Exception',
                'childwbs': ""
            }
        )

        c1_lean_staffing_compass_automation_child = rail.TriggerDagRunForEachItemOperator(
            task_id='c1_lean_staffing_compass_automation_child',
            retries=0,
            items=lambda: rail.result('get_all_child_wbs_details'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_each_child,
            conf=request_payload.get_project_dag_compass_confg
        )

        wait_for_c1_lean_staffing_compass_automation_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_c1_lean_staffing_compass_automation_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("c1_lean_staffing_compass_automation_child") }}',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("create_log") }}',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            properties={
                'employeeid': '{{dag_run.conf.personnelnumber}}',
                'wbs': '{{dag_run.conf.wbselement}}',
                'billingtype': null,
                'status': 'Error',
                'childwbs': ""
            })

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_log >> is_user_present

        is_user_present >> rail.Label("YES") >> is_c1_user
        is_user_present >> rail.Label(
            "NO") >> log_no_user_present_in_replicon >> catch_and_log_errors
        is_c1_user >> rail.Label(
            "NO") >> get_project_info_based_on_wbs_element
        c1_lean_staffing_c1_automation_child >> wait_for_c1_lean_staffing_c1_automation_child >> catch_and_log_errors
        is_c1_user >> rail.Label(
            "YES") >> c1_lean_staffing_c1_automation_child
        get_project_info_based_on_wbs_element >> project_exist_validation
        project_exist_validation >> rail.Label(
            "YES") >> log_project_validation >> catch_and_log_errors
        project_exist_validation >> rail.Label(
            "NO") >> get_all_filter_defination
        get_all_filter_defination >> get_all_columns >> get_all_child_wbs_details
        get_all_child_wbs_details >> is_child_wbs_present
        is_child_wbs_present >> rail.Label(
            "NO") >> log_no_child_project_validation >> catch_and_log_errors
        is_child_wbs_present >> rail.Label(
            "YES") >> c1_lean_staffing_compass_automation_child >> wait_for_c1_lean_staffing_compass_automation_child >> catch_and_log_errors

    return dag


rail.for_each_instance(create_dag)
