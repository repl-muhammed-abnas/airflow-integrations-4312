from datetime import timedelta
import rail
import itertools
from dxctechnology.wf39_psa_resource_assignment_compass_v2.utils import python_callable_method
from dxctechnology.wf39_psa_resource_assignment_compass_v2.utils import response_filter
from dxctechnology.wf39_psa_resource_assignment_compass_v2.utils import request_payload
from airflow.models import Variable

# pylint: disable=too-many-statements


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_wf39_psa_resource_assignment_compass_process_distinct_wbs_item_child_{config.instance}_v2',
        description=f'DXC_WF39 Resource Assignment Automation Child V2 - B1 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_process_wbs_max_active_runs,
    ) as dag:
        
        project_name = "{{ dag_run.conf.wbs }}"
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        query_billing_rates_for_wbs = rail.QueryCollectionOperator(
            task_id="query_billing_rates_for_wbs",
            query="""SELECT DISTINCT wbs, employeeid FROM inputcombineddata WHERE wbs=:wbs""",
            query_params={
                "wbs": project_name
            }
        )

        get_assignable_billing_rates = rail.PythonOperator(
            task_id="get_assignable_billing_rates",
            python_callable=python_callable_method.get_assignable_billing_rates
        )

        assignable_billing_rates_for_wbs = rail.CreateCollectionOperator(
            task_id="assignable_billing_rates_for_wbs",
            source=lambda: rail.result('get_assignable_billing_rates'),
            name="assignablebillingrates"
        )

        get_project_info_from_project_service = rail.RepliconServiceOperator(
            task_id='get_project_info_from_project_service',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=request_payload.get_project_payload,
            response_filter=response_filter.map_project_response
        )

        is_project_not_exists = rail.IfOperator(
            task_id="is_project_not_exists",
            test=lambda: not bool(rail.result(
                'get_project_info_from_project_service')),
            yes_task="log_project_doesnt_exist",
            no_task="get_division_detail",
        )

        log_project_doesnt_exist = rail.WriteLogOperator(
            task_id="log_project_doesnt_exist",
            log='{{ result("create_log") }}',
            message='WBS Element is not present in Replicon',
            items='{{ result("query_all_records_for_wbs") }}',
            severity='Exception',
            properties={
                'wbs': project_name,
                'role': '{{ item.role }}',
                'billingrate': '',
                'status': 'Exception',
                'action': 'Validation',
                'employeeid': '{{ item.employeeid }}'
            }
        )

        get_division_detail = rail.RepliconServiceOperator(
            task_id='get_division_detail',
            endpoint='/services/DivisionService1.svc/GetDivisionDetails',
            data=request_payload.get_division_detail
        )

        is_project_compass = rail.IfOperator(
            task_id="is_project_compass",
            test=python_callable_method.is_project_compass,
            yes_task="is_psa_flag_x",
            no_task="log_project_not_compass",
        )

        is_psa_flag_x = rail.IfOperator(
            task_id="is_psa_flag_x",
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(rail.result('get_project_info_from_project_service')[
                'extensionFieldValues'], "definition.displayText", "PSA Flag", "tag", False)),
            yes_task="is_project_iwo",
            no_task="log_project_not_psa",
        )

        is_project_iwo = rail.IfOperator(
            task_id="is_project_iwo",
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(rail.result('get_project_info_from_project_service')[
                'extensionFieldValues'], "definition.displayText", "Parent WBS", "textValue", False)),
            yes_task="log_project_iwo",
            no_task="get_active_user",
        )

        log_project_iwo = rail.WriteLogOperator(
            task_id="log_project_iwo",
            log='{{ result("create_log") }}',
            message='WBS Element is IWO',
            items='{{ result("query_all_records_for_wbs") }}',
            severity='Exception',
            properties={
                'wbs': project_name,
                'role': '{{ item.role }}',
                'status': 'Exception',
                'action': 'Validation',
                'employeeid': '{{ item.employeeid }}'
            }
        )

        log_project_not_compass = rail.WriteLogOperator(
            task_id="log_project_not_compass",
            log='{{ result("create_log") }}',
            message='WBS Element is not Compass',
            items='{{ result("query_all_records_for_wbs") }}',
            severity='Exception',
            properties={
                'wbs': project_name,
                'role': '{{ item.role }}',
                'status': 'Exception',
                'action': 'Validation',
                'employeeid': '{{ item.employeeid }}'
            }
        )

        log_project_not_psa = rail.WriteLogOperator(
            task_id="log_project_not_psa",
            log='{{ result("create_log") }}',
            message='WBS Element is not PSA',
            items='{{ result("query_all_records_for_wbs") }}',
            severity='Exception',
            properties={
                'wbs': project_name,
                'role': '{{ item.role }}',
                'status': 'Exception',
                'action': 'Validation',
                'employeeid': '{{ item.employeeid }}'
            }
        )

        query_all_records_for_wbs = rail.QueryCollectionOperator(
            task_id="query_all_records_for_wbs",
            query="""SELECT * FROM inputcombineddata WHERE wbs=:wbs""",
            query_params={
                "wbs": project_name
            }
        )

        get_active_user = rail.PythonOperator(
            task_id="get_active_user",
            python_callable=python_callable_method.active_user
        )

        process_each_wbs_item = rail.trigger_parallel_dagrun(
            task_id='process_each_wbs_item',
            parallel_count=config.parallel_count,
            items=lambda: rail.result('query_all_records_for_wbs'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_wf39_psa_resource_assignment_compass_process_each_record_child_{config.instance}_v2',
            conf=request_payload.get_project_dag_confg
        )

        get_process_each_wbs_item_dag_ids =rail.PythonOperator(
            task_id= 'get_process_each_wbs_item_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(filter(None, map(lambda x: rail.result(
                    f'process_each_wbs_item_{x+1}'), range(config.parallel_count)))))),
            show_return_value_in_logs= False
        )

        gather_all_wbs_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_all_wbs_logs',
            dag_runs='{{ result("get_process_each_wbs_item_dag_ids") }}',
            dagrun_task_id='create_log',
            execution_timeout=timedelta(
                hours=config.gather_logs_timeout_hours),
            flatten=True
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("create_log") }}',
            trigger_rule='one_failed',
            items='{{ result("query_all_records_for_wbs") }}',
            message='{{ get_error_message() }}',
            properties={
                'wbs': project_name,
                'role': '{{ item.role }}',
                'billingrate': '',
                'status': 'Error',
                'action': 'Error',
                'employeeid': '{{ item.employeeid }}'
            })

        create_log >> query_billing_rates_for_wbs >> get_assignable_billing_rates >> assignable_billing_rates_for_wbs
        assignable_billing_rates_for_wbs >> get_project_info_from_project_service >> query_all_records_for_wbs >> is_project_not_exists
        is_project_not_exists >> rail.Label(
            "Yes") >> log_project_doesnt_exist >> catch_and_log_errors
        is_project_not_exists >> rail.Label(
            "No") >> get_division_detail >> is_project_compass
        is_project_compass >> rail.Label(
            "Yes") >> is_psa_flag_x
        is_project_compass >> rail.Label(
            "No") >> log_project_not_compass >> catch_and_log_errors
        is_psa_flag_x >> rail.Label(
            "No") >> log_project_not_psa >> catch_and_log_errors
        is_psa_flag_x >> rail.Label(
            "Yes") >> is_project_iwo
        is_project_iwo >> rail.Label(
            "No") >> get_active_user >> process_each_wbs_item
        process_each_wbs_item >> get_process_each_wbs_item_dag_ids >> gather_all_wbs_logs >> catch_and_log_errors
        is_project_iwo >> rail.Label(
            "Yes") >> log_project_iwo >> catch_and_log_errors

        return dag


rail.for_each_instance(create_child_dag)
