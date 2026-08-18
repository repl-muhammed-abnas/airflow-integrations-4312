
from datetime import datetime, timedelta
from airflow.utils.edgemodifier import Label
import rail
from rail.task_groups.batch_execution import batch_execution
from dxctechnology.ppmc_project_and_tasks_import import request_payload

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/ppmc_project_and_tasks_import/config.py

# pylint: disable=too-many-statements
def create_child_project_process_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_ppmc_project_task_import_child_project_process{dag_id_postfix}',
        description=f'DXC PPMC Project and Tasks - Child_process WBS V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        max_active_runs=config.dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        start_date=datetime(2022, 1, 1)
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        null = None

        create_projectlist_collection = rail.CreateCollectionOperator(
            task_id="create_projectlist_collection",
            name="projectlist",
            source=lambda: request_payload.get_dag_run_conf()['ppmcprojects']
        )

        create_tasklist_collection = rail.CreateCollectionOperator(
            task_id="create_tasklist_collection",
            name="tasklist",
            source=lambda: request_payload.get_dag_run_conf()['task']
        )

        query_distinct_project = rail.QueryCollectionOperator(
            task_id="query_distinct_project",
            name="query_distinct_project",
            query="""SELECT * FROM
                            projectlist
                        GROUP BY
                            taskcode
                    """
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={
                "projects": [
                    {
                        "uri": null,
                        "name": "{{ dag_run.conf.wbsname }}",
                        "code": null,
                        "parameterCorrelationId": null
                    }
                ]
            }
        )

        is_valid_project = rail.IfOperator(
            task_id='is_valid_project',
            test=lambda: rail.result('get_project_details')[0] and rail.result(
                'get_project_details')[0]['projectDetails']
            and rail.result('get_project_details')[0]['projectDetails']['uri'],
            yes_task='get_task_base_report_details',
            no_task='log_invalid_project'
        )

        log_invalid_project = rail.WriteLogOperator(
            task_id='log_invalid_project',
            message='The recived WBS is not available in Replicon.',
            severity='Exception',
            properties={
                'wbs': '{{ dag_run.conf.wbsname }}',
                'status': 'Exception',
            }
        )

        get_task_base_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_task_base_report_details",
            report_name='Replicon_Integration_PPMCtask_basereport'
        )

        create_task_base_report_generation_batch = rail.RepliconServiceOperator(
            task_id="create_task_base_report_generation_batch",
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=request_payload.get_task_base_report_generation_batch_param
        )

        batchuri = "{{ result('create_task_base_report_generation_batch') }}"

        process_report_batch = batch_execution(
            group_id='execute_report_generation_batch',
            creation_task_id=create_task_base_report_generation_batch.task_id
        )

        payload = {
            "reportGenerationBatchUri": batchuri
        }

        get_report_batch_result = rail.RepliconServiceOperator(
            task_id="get_report_batch_result",
            endpoint="/services/ReportService1.svc/GetReportGenerationBatchResults",
            data=payload
        )

        has_valid_report_data = rail.IfOperator(
            task_id='has_valid_report_data',
            test=lambda: rail.result('get_report_batch_result')['reportGenerationResults'][0]['payload'].startswith(
                'Task Name,Task Type,Attribute2,Attribute1,TaskUri,Task Name (Full Path)') and
            rail.result('get_report_batch_result')[
                'reportGenerationResults'][0]['payload'] != 'No Data',
            yes_task='put_eligible_teammember_access',
            no_task='raise_invalid_report_data_error',
        )

        def raise_exception(ex):
            raise ex

        raise_invalid_report_data_error = rail.PythonOperator(
            task_id='raise_invalid_report_data_error',
            python_callable=lambda: raise_exception(
                Exception('invalid report column configuration or no data'))
        )

        put_eligible_teammember_access = rail.RepliconServiceOperator(
            task_id="put_eligible_teammember_access",
            endpoint="/services/ProjectService1.svc/PutEligibleProjectTeamMemberDataAccessScopesForProject",
            data=request_payload.get_put_eligible_teammember_access_param()
        )

        is_ppmc_task_required = rail.IfOperator(
            task_id='is_ppmc_task_required',
            test=request_payload.is_ppmc_task_required,
            yes_task='can_manually_assign_task',
            no_task='update_ppmc_required_oef',
        )

        update_ppmc_required_oef = rail.RepliconServiceOperator(
            task_id="update_ppmc_required_oef",
            endpoint="/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue",
            data=request_payload.get_update_ppmc_required_oef_param()
        )

        can_manually_assign_task = rail.IfOperator(
            task_id='can_manually_assign_task',
            test=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_project_details')[
                    0]['projectDetails']['keyValues'],
                'keyUri',
                "urn:replicon:project-key-value-key:project-team-member-assignment-type"
            )['value']['uri'] != 'urn:replicon:project-team-member-assignment-type:manually-assign-task',
            yes_task='put_key_value_for_project',
            no_task='load_csv_payload_from_report',
        )

        put_key_value_for_project = rail.RepliconServiceOperator(
            task_id="put_key_value_for_project",
            endpoint="/services/ProjectService1.svc/PutKeyValueForProject",
            data=request_payload.get_put_key_value_for_project_param()
        )

        load_csv_payload_from_report = rail.LoadCSVFileOperator(
            task_id="load_csv_payload_from_report",
            document="{{ result('get_report_batch_result').reportGenerationResults[0].payload }}"
        )

        create_draft_parenttask_collection = rail.CreateCollectionOperator(
            task_id="create_draft_parenttask_collection",
            name="temp_projectlist",
            source="{{ result('load_csv_payload_from_report') }}"
        )

        create_parenttask_collection = rail.CreateCollectionOperator(
            task_id="create_parenttask_collection",
            name="parenttask",
            source=request_payload.get_create_parenttask_collection_source
        )

        query_all_task = rail.QueryCollectionOperator(
            task_id="query_all_task",
            name="merge_task",
            query="""SELECT
                        Task_name,
                        Task_type,
                        Taskuri
                    FROM
                        parenttask
                """
        )

        query_merge_task = rail.QueryCollectionOperator(
            task_id="query_merge_task",
            name="tasks",
            query="""SELECT
                        Task_name,
                        Taskuri
                    FROM
                        parenttask
                    WHERE
                        Attribute2= 'Yes' AND
                        Task_name != "" AND
                        Task_name IS NOT NULL
                    UNION
                    SELECT
                        Task_name,
                        Taskuri
                    FROM
                        parenttask
                    WHERE
                        Attribute1='Yes' AND
                        Attribute1_name NOT IN(SELECT
                                                    Attribute1_name
                                                FROM
                                                    parenttask
                                                WHERE
                                                    Attribute2 = 'Yes') AND
                        Task_name != "" AND
                        Task_name IS NOT NULL
            """
        )

        is_time_entry_allowed = rail.IfOperator(
            task_id='is_time_entry_allowed',
            test=lambda: (len(request_payload.get_dag_run_conf()['ppmcprojects']) > 0 or
                          len(request_payload.get_dag_run_conf()['task']) > 0) and
            rail.result('get_project_details')[
                0]['projectDetails']['isTimeEntryAllowed'],
            yes_task='update_allow_timeentry_tasksonly',
            no_task='process_task',
        )

        update_allow_timeentry_tasksonly = rail.RepliconServiceOperator(
            task_id="update_allow_timeentry_tasksonly",
            endpoint="/services/ProjectService1.svc/UpdateAllowTimeEntryAgainstTasksOnly",
            data={
                "projectUri": "{{ result('get_project_details')[0].projectDetails.uri }}",
                "allowTimeEntryAgainstTasksOnly": "true"
            }
        )

        process_task = rail.TriggerDagRunForEachItemOperator(
            task_id='process_task',
            retries=0,
            items="{{ result('query_distinct_project') }}",
            trigger_dag_id=f'dxctechnology_ppmc_project_task_import_child_task_process{dag_id_postfix}',
            execution_timeout=timedelta(days=7),
            conf=request_payload.get_call_task_import_child_dag_confg
        )

        wait_for_process_task = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_task',
            dag_runs='{{ result("process_task") }}',
            execution_timeout=timedelta(days=7),
        )

        [create_projectlist_collection, create_tasklist_collection] >> query_distinct_project >> \
            get_project_details >> is_valid_project
        is_valid_project >> rail.Label('No') >> log_invalid_project
        is_valid_project >> rail.Label('Yes') >> get_task_base_report_details >> create_task_base_report_generation_batch >> \
            process_report_batch >> get_report_batch_result >> has_valid_report_data

        has_valid_report_data >> Label(
            "Yes") >> put_eligible_teammember_access >> is_ppmc_task_required
        has_valid_report_data >> Label("No") >> raise_invalid_report_data_error

        is_ppmc_task_required >> Label(
            "Yes") >> update_ppmc_required_oef >> can_manually_assign_task
        is_ppmc_task_required >> Label("No") >> can_manually_assign_task

        can_manually_assign_task >> Label(
            "Yes") >> put_key_value_for_project >> load_csv_payload_from_report
        can_manually_assign_task >> Label("No") >> load_csv_payload_from_report

        load_csv_payload_from_report >> create_draft_parenttask_collection >> create_parenttask_collection >> query_all_task >> \
            query_merge_task >> is_time_entry_allowed

        is_time_entry_allowed >> Label(
            "Yes") >> update_allow_timeentry_tasksonly >> process_task >> wait_for_process_task
        is_time_entry_allowed >> Label(
            "No") >> process_task >> wait_for_process_task

    return dag


rail.for_each_instance(create_child_project_process_dag)
