from datetime import timedelta, datetime, timezone
from pendulum import datetime as dt
import rail
from airflow.models import Variable
from pimco.task_resource_update.utils import python_callable_method

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pimco_task_resource_update_on_all_active_projects_master_{config.instance}',
        description=f'PIMCO Task Resource Update on All Active Projects_master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        start_date=dt(2022, 4, 1, tz=config.pst_timezone),
        max_active_runs=config.max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_master, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_task_status_and_resource_update_lookup_table'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_task_status_and_resource_update_lookup_table',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_task_status_and_resource_update_lookup_table = rail.CreateLogOperator(
            task_id="get_task_status_and_resource_update_lookup_table",
            tenant_wide_name="task_status_and_resource_update_lookup_table",
            existing_log_mode="append",
        )

        get_date_oneday_back = rail.PythonOperator(
            task_id = 'get_date_oneday_back',
            python_callable=lambda: (datetime.now(timezone.utc)-timedelta(days=1)).strftime('%d/%m/%Y')
        )

        search_entries_task_status_and_resource_update_lookup= rail.FilterLogEntriesOperator(
            task_id = 'search_entries_task_status_and_resource_update_lookup',
            log= "{{ result('get_task_status_and_resource_update_lookup_table') }}",
            properties={
                'type': 'resource',
                'date': "{{result('get_date_oneday_back')}}",
                'project_type': "FTE"
            }
        )

        if_entries_not_present=rail.IfOperator(
            task_id='if_entries_not_present',
            test='''{{ result('search_entries_task_status_and_resource_update_lookup',"length") == 0 }}''',
            yes_task="finish",
            no_task="get_resource_assignment",
        )

        get_resource_assignment=rail.PythonOperator(
            task_id='get_resource_assignment',
            python_callable= python_callable_method.create_resource_assignment
        )

        bulk_get_resource_assignments=rail.RepliconServiceOperator(
            task_id='bulk_get_resource_assignments',
            endpoint="/services/TaskService1.svc/BulkGetResourceAssignments",
            data=python_callable_method.get_payload_bulk_resource_assignment
        )

        get_resourceassignment=rail.PythonOperator(
            task_id='get_resourceassignment',
            python_callable= python_callable_method.create_resourceassignment
        )

        get_project_uri = rail.RepliconServiceOperator(
            task_id = 'get_project_uri',
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data= {
                "projects": [
                    {
                    "uri": null,
                    "name": config.project_name,
                    "code": null,
                    "parameterCorrelationId": null
                    }
                ]
            }
        )

        get_initial_team_data=rail.RepliconServiceOperator(
            task_id='get_initial_team_data',
            endpoint="/services/ProjectService1.svc/BulkGetAllProjectTeamMembers2",
            data=lambda: {
                "projectUris": [
                    rail.result('get_project_uri')[0]['projectDetails']['uri']
                ]
            }
        )

        getresource_assignment=rail.PythonOperator(
            task_id='getresource_assignment',
            python_callable= python_callable_method.createresource_assignment
        )

        get_enabled_departments=rail.RepliconServiceOperator(
            task_id='get_enabled_departments',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartments",
            data={}
        )

        get_inprogress_project_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_inprogress_project_report_details',
            report_name=config.inprogress_project_report
        )

        load_inprogress_project_report = rail.run_report2(
            group_id='load_inprogress_project_report',
            report_params={
                "reportParameters": [
                    {
                    "reportUri": "{{result('get_inprogress_project_report_details').uri}}",
                    "filterValues": [],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        parse_report_to_csv = rail.LoadCSVFileOperator(
            task_id='parse_report_to_csv',
            document="{{result('load_inprogress_project_report.get_report_result').reportGenerationResults[0].payload}}",
            delimiter = ',',
            headers=['Fund/Deal/Entity Name','Fund/Deal/Entity Code','Project URI']
        )

        create_projecttaskuri_collection = rail.CreateCollectionOperator(
            task_id="create_projecttaskuri_collection",
            name="projecttaskuri",
            source="{{result('parse_report_to_csv')}}",
            columns={
                'Fund/Deal/Entity Name': 'projectname',
                'Fund/Deal/Entity Code': 'projectcode',
                'Project URI': 'projecturi'
            }
        )

        query_distinct_projects=rail.QueryCollectionOperator(
            task_id='query_distinct_projects',
            query="""SELECT DISTINCT projectname, projecturi FROM  projecttaskuri""",
        )

        foreach_distinct_project = rail.ForEachOperator(
            task_id='foreach_distinct_project',
            items="{{ result('query_distinct_projects') }}",
            start_task='trigger_dag_run_resource_assignment_update',
            end_task='foreach_distinct_project_end'
        )

        trigger_dag_run_resource_assignment_update = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_resource_assignment_update',
            trigger_dag_id=config.child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda : python_callable_method.get_payload_for_child(rail.result('foreach_distinct_project'))
        )

        foreach_distinct_project_end = rail.EmptyOperator(
            task_id = 'foreach_distinct_project_end'
        )

        wait_for_dag_run_resource_assignment_update = rail.WaitForDagRunsSensor(
            task_id='wait_for_dag_run_resource_assignment_update',
            dag_runs='{{ result("trigger_dag_run_resource_assignment_update") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        delete_entries_task_status_and_resource_update= rail.FilterLogEntriesOperator(
            task_id = 'delete_entries_task_status_and_resource_update',
            log= "{{ result('get_task_status_and_resource_update_lookup_table') }}",
            properties= {
                'type': 'resource',
                'date': "{{result('get_date_oneday_back')}}"
            },
            remove_filtered_entries=True
        )

        send_mail = rail.EmailOperator(
            task_id='send_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject= config.company_key + " | " + " Updating Task resource from base project to all in-progress projects completed successfully at " + "{{current_time() }}",
            html_content="templates/email.html",
        )

        on_error = rail.EmptyOperator(
            task_id='on_error',
            trigger_rule='one_failed'
        )

        send_failure_mail = rail.EmailOperator(
            task_id='send_failure_mail',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            subject= config.company_key + " | " + "Updating market rate from base project to all in-progress projects Failed at " + "{{current_time() }}",
            html_content="templates/failure_mail.html",
            params={
                'dag_id': f'pimco_task_resource_update_on_all_active_projects_{config.instance}'
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_task_status_and_resource_update_lookup_table >> get_date_oneday_back
        get_date_oneday_back >> search_entries_task_status_and_resource_update_lookup >> if_entries_not_present
        if_entries_not_present >> rail.Label('Yes')  >> finish
        if_entries_not_present >> rail.Label('No') >> get_resource_assignment >> bulk_get_resource_assignments >> get_resourceassignment
        get_resourceassignment >> get_project_uri >> get_initial_team_data >> getresource_assignment
        getresource_assignment >> get_enabled_departments >> get_inprogress_project_report_details
        get_inprogress_project_report_details >> load_inprogress_project_report >> parse_report_to_csv >> create_projecttaskuri_collection
        create_projecttaskuri_collection >> query_distinct_projects >> foreach_distinct_project
        foreach_distinct_project >> trigger_dag_run_resource_assignment_update >> foreach_distinct_project_end
        foreach_distinct_project_end >> wait_for_dag_run_resource_assignment_update>> delete_entries_task_status_and_resource_update
        delete_entries_task_status_and_resource_update >> send_mail >> on_error >> send_failure_mail >> log_to_sumo >> finish
        foreach_distinct_project >> foreach_distinct_project_end
    return dag

rail.for_each_instance(create_dag)
