from datetime import timedelta
import rail

from rail.lib.ecid import get_dagrun_ecid
from pimco.create_new_task_consultant.utils import custom_methods
from pimco.create_new_task_consultant.utils import request_payload
from pimco.create_new_task_consultant.utils import response_filter

# pylint: disable=too-many-statements


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pimco_consultant_process_task_levels_child_dag_{config.instance}",
        description=f"PIMCO consultant Process task levels child dag {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        query_task_level = rail.QueryCollectionOperator(
            task_id='query_task_level',
            query="SELECT * FROM task_data_and_task_level WHERE tasklevel=:task_level",
            query_params={
                "task_level": '{{dag_run.conf.task_level}}'
            }
        )

        check_task_data_size = rail.IfOperator(
            task_id='check_task_data_size',
            test='{{ result("query_task_level", "length") > 0 }}',
            yes_task='get_in_progress_project_details',
            no_task='catch_and_log_errors'
        )

        get_in_progress_project_details = rail.RepliconReportDetailsOperator(
            task_id='get_in_progress_project_details',
            report_name=config.extract_in_progress_project_report_name,
        )

        report_group_entry, report_group_exit = rail.run_report(
            group_id='in_progress_project_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_in_progress_project_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        project_report_has_error = rail.IfOperator(
            task_id="project_report_has_error",
            test="{{ result('in_progress_project_report.get_report_result').reportGenerationResults[0].error | is_truthy }}",
            yes_task='fail_error_project_report_data',
            no_task='project_report_has_data',
        )

        project_report_has_data = rail.IfOperator(
            task_id="project_report_has_data",
            test="{{ result('in_progress_project_report.get_report_result', 'has_data') }}",
            yes_task='load_project_report_data',
            no_task='fail_no_project_report_data',
        )

        fail_no_project_report_data = rail.FailOperator(
            task_id="fail_no_project_report_data",
            message="Report \"In-Progress Project report\" execution failed",
        )

        fail_error_project_report_data = rail.FailOperator(
            task_id="fail_error_project_report_data",
            message="{{result('in_progress_project_report.get_report_result').reportGenerationResults[0].error}}",
        )

        load_project_report_data = rail.LoadCSVFileOperator(
            task_id='load_project_report_data',
            document="{{ result('in_progress_project_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        project_report_data_collection = rail.CreateCollectionOperator(
            task_id='project_report_data_collection',
            source="{{ result('load_project_report_data') }}",
            name='project_report_data',
            columns={
                'Fund/Deal/Entity Name': 'projectname',
                'Fund/Deal/Entity Code': 'projectcode',
                'Project URI': 'projecturi',
            }
        )

        query_distinct_projects = rail.QueryCollectionOperator(
            task_id='query_distinct_projects',
            query="SELECT DISTINCT projectname, projecturi FROM project_report_data"
        )

        model_task_details = rail.RepliconServiceOperator(
            task_id='model_task_details',
            endpoint='/services/TaskService1.svc/BulkGetTaskDetails',
            data=request_payload.get_model_task_payload
        )

        resource_assignments = rail.RepliconServiceOperator(
            task_id='resource_assignments',
            endpoint='/services/TaskService1.svc/BulkGetResourceAssignments',
            data=request_payload.get_resource_assignment_payload,
            response_filter=response_filter.get_task_resource
        )

        custom_fields = rail.RepliconServiceOperator(
            task_id='custom_fields',
            endpoint='/services/TaskCustomFieldListService1.svc/GetData',
            data=request_payload.get_custom_fields_payload
        )

        get_all_currencies = rail.RepliconServiceOperator(
            task_id="get_all_currencies",
            endpoint="/services/CurrencyService2.svc/GetAllCurrencies",
        )

        create_projects_tasks_list = rail.PythonOperator(
            task_id='create_projects_tasks_list',
            python_callable=custom_methods.create_projects_tasks_list,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        create_task_from_model_to_all_projects = rail.TriggerDagRunForEachItemOperator(
            task_id='create_task_from_model_to_all_projects',
            retries=0,
            items='{{ result("create_projects_tasks_list") | to_json}}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'pimco_create_task_from_model_to_all_projects_child_dag_{config.instance}',
            conf=lambda item, dag_run: {
                "dag_run_ecid": get_dagrun_ecid(dag_run),
                "project_task_data": item
            }
        )

        wait_for_create_tasks = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_tasks',
            dag_runs='{{ result("create_task_from_model_to_all_projects") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'projectname': '',
                'runid': '{{dag_run.run_id}}',
                'status': 'Error',
            },
        )

        query_task_level >> check_task_data_size
        check_task_data_size >> rail.Label(
            "Yes") >> get_in_progress_project_details >> report_group_entry
        report_group_exit >> project_report_has_error
        check_task_data_size >> rail.Label("No") >> catch_and_log_errors

        project_report_has_error >> rail.Label(
            "Yes") >> fail_error_project_report_data
        project_report_has_error >> rail.Label("No") >> project_report_has_data

        project_report_has_data >> rail.Label(
            "Yes") >> load_project_report_data
        project_report_has_data >> rail.Label(
            "No") >> fail_no_project_report_data

        load_project_report_data >> project_report_data_collection >> query_distinct_projects \
            >> model_task_details >> resource_assignments >> custom_fields >> get_all_currencies >> create_projects_tasks_list \
            >> create_task_from_model_to_all_projects >> wait_for_create_tasks >> catch_and_log_errors
    return dag


rail.for_each_instance(create_child_dag)
