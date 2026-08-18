
from datetime import timedelta, datetime
from pendulum import datetime as dt
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'hexagonap_project_task_resource_assignment_master_{config.instance}',
        description=f'hexagonap_assigntaskresource - Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2022, 1, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_job_start_time'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_job_start_time',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_job_start_time = rail.PythonOperator(
            task_id = 'log_job_start_time',
            python_callable=lambda: datetime.now().strftime("%Y%m%dT%H%M%S")
        )

        get_project_task_list_report_details=rail.RepliconReportDetailsOperator(
            task_id='get_project_task_list_report_details',
            report_name=config.project_task_list_report
        )

        run_project_task_list_report=rail.run_report2(
            group_id='run_project_task_list_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_project_task_list_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact'
        )

        load_csv_from_project_task_list_report_result=rail.LoadCSVFileOperator(
            task_id="load_csv_from_project_task_list_report_result",
            document="{{(result('run_project_task_list_report.get_report_result')| load_json_artifact).reportGenerationResults[0].payload }}",
        )

        create_collection_projecttasklist = rail.CreateCollectionOperator(
            task_id='create_collection_projecttasklist',
            source = "{{ result('load_csv_from_project_task_list_report_result') }}",
            name = "projecttasklist",
            columns = {
                'Project Name':'projectname', 
                'Task Name':'taskname', 
                'ProjectUri':'projecturi', 
                'TaskUri':'taskuri'
            }
        )

        get_project_task_assignment_report_details=rail.RepliconReportDetailsOperator(
            task_id='get_project_task_assignment_report_details',
            report_name=config.project_task_assignment
        )

        run_project_task_assignment_report=rail.run_report2(
            group_id='run_project_task_assignment_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_project_task_assignment_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact'
        )

        load_csv_from_project_task_assignment_report_result=rail.LoadCSVFileOperator(
            task_id='load_csv_from_project_task_assignment_report_result',
            document="{{(result('run_project_task_assignment_report.get_report_result')| load_json_artifact).reportGenerationResults[0].payload}}"
        )

        create_collection_projecttaskassignment = rail.CreateCollectionOperator(
            task_id='create_collection_projecttaskassignment',
            source = "{{ result('load_csv_from_project_task_assignment_report_result') }}",
            name = "projecttaskassignment",
            columns = {
                'Project Name':'projectname', 
                'Task Name':'taskname', 
                'ProjectUri':'projecturi', 
                'TaskUri':'taskuri'
            }
        )

        query_unique_projects_from_projecttasklist=rail.QueryCollectionOperator(
            task_id='query_unique_projects_from_projecttasklist',
            query="""SELECT DISTINCT  projecttasklist.projectname,  projecttasklist.projecturi FROM  projecttasklist""",
        )

        create_task_resource_assignment_logs_lookuptable = rail.CreateLogOperator(
            task_id = 'create_task_resource_assignment_logs_lookuptable'
        )

        create_child_dag_runs_list = rail.SetVariableOperator(
            task_id = 'create_child_dag_runs_list',
            name = 'childdagruns',
            append=False,
            value=[]
        )

        create_taskcount_list = rail.SetVariableOperator(
            task_id = 'create_taskcount_list',
            name = 'tasks',
            append=False,
            value=[]
        )

        foreach_unique_project_from_projectlist=rail.ForEachOperator(
            task_id='foreach_unique_project_from_projectlist',
            items="{{result('query_unique_projects_from_projecttasklist')}}",
            start_task = 'query_tasks_without_team_assignments',
            end_task = 'foreach_unique_project_from_projectlist_end'
        )

        query_tasks_without_team_assignments=rail.QueryCollectionOperator(
            task_id='query_tasks_without_team_assignments',
            query="""SELECT  * FROM  projecttasklist WHERE  projecttasklist.projecturi='{{ result('foreach_unique_project_from_projectlist').projecturi }}' AND
                    projecttasklist.taskuri NOT IN  ( SELECT  projecttaskassignment.taskuri FROM  projecttaskassignment WHERE
                    projecttaskassignment.projecturi='{{ result('foreach_unique_project_from_projectlist').projecturi }}')""",
        )

        foreach_task_without_team_assignments=rail.ForEachOperator(
            task_id='foreach_task_without_team_assignments',
            items=lambda: rail.load_all_records(rail.result('query_tasks_without_team_assignments')),
            start_task = 'insert_to_task_count_list',
            end_task = 'foreach_task_without_team_assignments_end'
        )

        insert_to_task_count_list=rail.SetVariableOperator(
            task_id='insert_to_task_count_list',
            name='tasks',
            append=True,
            value=lambda:{
                "taskid": ((rail.result('foreach_task_without_team_assignments')['taskuri']).split(':'))[-1]
            }
        )

        trigger_assign_task_resource_child_dag=rail.TriggerDagRunOperator(
            task_id='trigger_assign_task_resource_child_dag',
            retries=0,
            trigger_dag_id=f'hexagonap_project_task_resource_assignment_assign_task_resource_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "projectname": "{{ result('foreach_task_without_team_assignments').projectname }}",
                "taskname": "{{ result('foreach_task_without_team_assignments').taskname }}",
                "projecturi": "{{ result('foreach_task_without_team_assignments').projecturi }}",
                "taskuri": "{{ result('foreach_task_without_team_assignments').taskuri }}",
                "logslookuptable": "{{result('create_task_resource_assignment_logs_lookuptable')}}",
                "callerjobid": "{{dag_run_ecid()}}"
            }
        )

        insert_to_child_dag_runs_list = rail.SetVariableOperator(
            task_id = 'insert_to_child_dag_runs_list',
            append=True,
            name ="{{result('create_child_dag_runs_list').name}}",
            value="{{result('trigger_assign_task_resource_child_dag')}}"
        )

        foreach_task_without_team_assignments_end=rail.EmptyOperator(
            task_id='foreach_task_without_team_assignments_end',
        )

        foreach_unique_project_from_projectlist_end=rail.EmptyOperator(
            task_id='foreach_unique_project_from_projectlist_end',
        )

        if_assign_task_resource_child_triggered = rail.IfOperator(
            task_id = 'if_assign_task_resource_child_triggered',
            test="{{ result('insert_to_child_dag_runs_list') | is_truthy }}",
            yes_task='wait_for_assign_task_resource_child_dag',
            no_task='log_tasks_count'
        )

        wait_for_assign_task_resource_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_assign_task_resource_child_dag',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("insert_to_child_dag_runs_list").value | to_json }}'
        )

        log_tasks_count=rail.PythonOperator(
            task_id='log_tasks_count',
            python_callable= lambda: len(rail.get_dag_run_var('tasks'))
        )

        if_tasks_tobe_assigned_present=rail.IfOperator(
            task_id='if_tasks_tobe_assigned_present',
            test='''{{ result('log_tasks_count') > 0 }}''',
            yes_task="search_logs_in_lookuptable",
            no_task="finish",
        )

        search_logs_in_lookuptable = rail.FilterLogEntriesOperator(
            task_id = 'search_logs_in_lookuptable',
            log="{{result('create_task_resource_assignment_logs_lookuptable')}}",
            properties={
                'jobid': "{{dag_run_ecid()}}"
            }
        )

        log_file_name = rail.PythonOperator(
            task_id = 'log_file_name',
            python_callable=lambda: rail.render_template("{{get_company_key()}}") + "_taskresourcelog_" + rail.result('log_job_start_time') + ".csv"
        )

        compose_logs_csv = rail.WriteCSVFileOperator(
            task_id = 'compose_logs_csv',
            source="{{result('search_logs_in_lookuptable')}}",
            header=['projectname',
                    'taskname',
                    'taskuri',
                    'status',
                    'details',
                    'jobid'],
            row= [
                    "{{ item.properties.projectname }}",
                    "{{ item.properties.taskname }}",
                    "{{ item.properties.taskuri }}",
                    "{{ item.properties.status }}",
                    "{{ item.properties.details }}",
                    "{{ item.properties.jobid }}|{{ item.properties.childjob }}"
                ],
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('compose_logs_csv')}}",
            output_file_name="{{ result('log_file_name')}}",
            expires_in_seconds=7*24*60*60,
        )

        def check_error_logs():
            logs = rail.load_all_records(rail.result('search_logs_in_lookuptable'))
            return rail.find_first_by_attr_and_get_attr(logs,'properties.status','Failed','properties.status','')

        check_for_error_logs = rail.PythonOperator(
            task_id = 'check_for_error_logs',
            python_callable=check_error_logs
        )

        if_error_logs_not_present = rail.IfOperator(
            task_id = 'if_error_logs_not_present',
            test=lambda: not bool(rail.result('check_for_error_logs')),
            yes_task='send_mail_completed_successfully',
            no_task='send_mail_completed_with_failed_records'
        )

        send_mail_completed_successfully=rail.EmailOperator(
            task_id='send_mail_completed_successfully',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Project task resource assignment Completed Successfully {{ result('log_job_start_time') }} ''',
            html_content= '''templates/completed_successfully_mail.html''',
        )

        send_mail_completed_with_failed_records=rail.EmailOperator(
            task_id='send_mail_completed_with_failed_records',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key()}} | Project task resource assignment Completed with failed records {{result('log_job_start_time')}} ''',
            html_content= '''templates/completed_with_failed_records_mail.html''',
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> log_job_start_time
        log_job_start_time >> get_project_task_list_report_details >> run_project_task_list_report >> load_csv_from_project_task_list_report_result
        load_csv_from_project_task_list_report_result >> create_collection_projecttasklist >> get_project_task_assignment_report_details
        get_project_task_assignment_report_details >> run_project_task_assignment_report >> load_csv_from_project_task_assignment_report_result
        load_csv_from_project_task_assignment_report_result >> create_collection_projecttaskassignment >> query_unique_projects_from_projecttasklist
        query_unique_projects_from_projecttasklist >> create_task_resource_assignment_logs_lookuptable >> create_child_dag_runs_list >> create_taskcount_list
        create_taskcount_list >> foreach_unique_project_from_projectlist >> query_tasks_without_team_assignments
        query_tasks_without_team_assignments >> foreach_task_without_team_assignments >> insert_to_task_count_list >> trigger_assign_task_resource_child_dag
        trigger_assign_task_resource_child_dag >> insert_to_child_dag_runs_list >> foreach_task_without_team_assignments_end
        foreach_task_without_team_assignments >> foreach_task_without_team_assignments_end >> foreach_unique_project_from_projectlist_end
        foreach_unique_project_from_projectlist_end >> if_assign_task_resource_child_triggered
        if_assign_task_resource_child_triggered >> rail.Label('Yes') >> wait_for_assign_task_resource_child_dag >> log_tasks_count
        if_assign_task_resource_child_triggered >> rail.Label('') >> log_tasks_count
        foreach_unique_project_from_projectlist >> foreach_unique_project_from_projectlist_end >> if_assign_task_resource_child_triggered
        log_tasks_count >> if_tasks_tobe_assigned_present
        if_tasks_tobe_assigned_present >> rail.Label('Yes') >> search_logs_in_lookuptable >> log_file_name >> compose_logs_csv >> generate_download_link
        generate_download_link >> check_for_error_logs >> if_error_logs_not_present
        if_error_logs_not_present >> rail.Label('Yes') >> send_mail_completed_successfully >> finish
        if_error_logs_not_present >> rail.Label('No') >> send_mail_completed_with_failed_records >> finish
        if_tasks_tobe_assigned_present >> rail.Label('No') >> finish >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
