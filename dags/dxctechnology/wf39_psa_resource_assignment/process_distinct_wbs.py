from datetime import timedelta
import rail
from dxctechnology.wf39_psa_resource_assignment.utils import python_callable_method
from dxctechnology.wf39_psa_resource_assignment.utils import response_filter
from dxctechnology.wf39_psa_resource_assignment.utils import request_payload


# pylint: disable=too-many-statements


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_wf39_psa_resource_assignment_process_distinct_wbs_item_child_{config.instance}',
        description=f'DXC_WF39 Resource Assignment Automation Child V2.0 - B1 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_process_wbs_max_active_runs,
    ) as dag:

        project_name = "{{ dag_run.conf.wbs }}"
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

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
            message='WBS Element is not present in Replicon',
            items='{{ result("query_all_records_for_wbs") }}',
            severity='Exception',
            properties={
                'wbs': project_name,
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

        is_project_c1 = rail.IfOperator(
            task_id="is_project_c1",
            test=python_callable_method.is_project_c1,
            yes_task="is_project_iwo",
            no_task="log_project_not_c1",
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
            message='WBS Element is IWO',
            items='{{ result("query_all_records_for_wbs") }}',
            severity='Exception',
            properties={
                'wbs': project_name,
                'status': 'Exception',
                'action': 'Validation',
                'employeeid': '{{ item.employeeid }}'
            }
        )

        log_project_not_c1 = rail.WriteLogOperator(
            task_id="log_project_not_c1",
            message='WBS Element is not C1',
            items='{{ result("query_all_records_for_wbs") }}',
            severity='Exception',
            properties={
                'wbs': project_name,
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

        process_each_wbs_item = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_wbs_item',
            retries=0,
            items=lambda: rail.result('query_all_records_for_wbs'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_wf39_psa_resource_assignment_process_each_record_child_{config.instance}',
            conf=request_payload.get_project_dag_confg
        )

        wait_for_process_each_wbs_item = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_wbs_item',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_each_wbs_item") }}',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            properties={
                'wbs': project_name,
                'billingrate': '',
                'status': 'Error',
                'action': '',
                'employeeid': ''
            })

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        query_billing_rates_for_wbs >> get_assignable_billing_rates >> assignable_billing_rates_for_wbs
        assignable_billing_rates_for_wbs >> get_project_info_from_project_service >> query_all_records_for_wbs >> is_project_not_exists
        is_project_not_exists >> rail.Label(
            "Yes") >> log_project_doesnt_exist >> catch_and_log_errors >> log_to_sumo
        is_project_not_exists >> rail.Label(
            "No") >> get_division_detail >> is_project_c1 >> rail.Label("Yes") >> is_project_iwo >> rail.Label("No") >> get_active_user >> process_each_wbs_item
        is_project_c1 >> rail.Label(
            "No") >> log_project_not_c1 >> catch_and_log_errors
        process_each_wbs_item >> wait_for_process_each_wbs_item >> catch_and_log_errors
        is_project_iwo >> rail.Label(
            "Yes") >> log_project_iwo >> catch_and_log_errors

        return dag


rail.for_each_instance(create_child_dag)
