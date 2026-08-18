from datetime import timedelta, datetime, timezone
from pendulum import datetime as dt
from pimco.task_status_update.utils import request_payload, python_callable_method

from airflow.models import Variable
import rail

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pimco_task_status_update_on_all_active_projects_master_{config.instance}',
        description=f'PIMCO Task Status Update on All Active Projects master dag {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2023, 1, 1, tz=config.pst_timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_master,
    ) as dag:


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

        get_yesterday_date = rail.PythonOperator(
            task_id = 'get_yesterday_date',
            python_callable=lambda: (datetime.now(timezone.utc)-timedelta(days=1)).strftime('%d/%m/%Y')
        )

        search_entries_task_status_and_resource_update_lookup = rail.FilterLogEntriesOperator(
            task_id = 'search_entries_task_status_and_resource_update_lookup',
            log= "{{ result('get_task_status_and_resource_update_lookup_table') }}",
            properties={
                'type': 'status',
                'date': "{{result('get_yesterday_date')}}",
                'project_type': "FTE"
            }
        )

        if_entries_not_present=rail.IfOperator(
            task_id='if_entries_not_present',
            test='''{{ result('search_entries_task_status_and_resource_update_lookup',"length") == 0 }}''',
            yes_task="finish",
            no_task="get_all_project_task_report_details",
        )

        get_all_project_task_report_details=rail.RepliconReportDetailsOperator(
            task_id='get_all_project_task_report_details',
            report_name=config.all_project_task_report
        )

        load_all_project_task_report = rail.run_report2(
            group_id='load_all_project_task_report',
            report_params=request_payload.get_payload_all_project_task_report_generation
        )

        load_csv_from_report_result=rail.LoadCSVFileOperator(
            task_id="load_csv_from_report_result",
            document="{{result('load_all_project_task_report.get_report_result').reportGenerationResults[0].payload}}",
            delimiter = ',',
            headers=['Fund/Deal/Entity Name','Project URI','Project - Task','Task URI','Billing Activity-Task Code']
        )

        create_collection_from_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_csv',
            source = "{{ result('load_csv_from_report_result') }}",
            name = "projecttaskuri",
            columns = {
                'Fund/Deal/Entity Name':'projectname',
                'Project URI':'projecturi',
                'Project - Task':'taskfullpath',
                'Task URI':'taskuri',
                'Billing Activity-Task Code':'taskcode'
            }
        )

        query_all_projects=rail.QueryCollectionOperator(
            task_id='query_all_projects',
            query="""SELECT * FROM  projecttaskuri""",
        )

        query_distinct_projects=rail.QueryCollectionOperator(
            task_id='query_distinct_projects',
            query="""SELECT DISTINCT  projecttaskuri.projectname,  projecttaskuri.projecturi FROM  projecttaskuri""",
        )

        foreach_distinct_project = rail.ForEachOperator(
            task_id='foreach_distinct_project',
            items="{{ result('query_distinct_projects') }}",
            start_task='trigger_dag_run_task_update',
            end_task='foreach_distinct_project_end'
        )

        trigger_dag_run_task_update = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_task_update',
            trigger_dag_id=config.child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda : python_callable_method.get_payload_for_child(rail.result('foreach_distinct_project'))
        )

        foreach_distinct_project_end = rail.EmptyOperator(
            task_id = 'foreach_distinct_project_end'
        )

        delete_entries_task_status_and_resource_update_lookup=rail.FilterLogEntriesOperator(
            task_id = 'delete_entries_task_status_and_resource_update_lookup',
            log= "{{ result('get_task_status_and_resource_update_lookup_table') }}",
            properties={
                'type': 'status',
                'date': "{{result('get_yesterday_date')}}"
            },
            remove_filtered_entries=True
        )

        send_mail=rail.EmailOperator(
            task_id='send_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject= '{{ get_company_key() }}' + " | " + "Tasks status update from base project to all in-progress projects completed successfully at " + "{{ current_time()}}",
            html_content='templates/success_mail.html',
        )

        on_error = rail.EmptyOperator(
            task_id='on_error',
            trigger_rule='one_failed'
        )

        send_failure_mail = rail.EmailOperator(
            task_id='send_failure_mail',
            to=config.alert_email,
            bcc=config.internal_logs_email,
            subject=  '{{ get_company_key() }}' + " | " + "  Tasks status update to all project master recipe Failed at" + "{{current_time() }}",
            html_content="templates/failure_mail.html",
            params={
                'dag_id': f'pimco_task_status_update_child_{config.instance}'
            }
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_task_status_and_resource_update_lookup_table
        get_task_status_and_resource_update_lookup_table >> get_yesterday_date >> search_entries_task_status_and_resource_update_lookup
        search_entries_task_status_and_resource_update_lookup >> if_entries_not_present
        if_entries_not_present >> rail.Label('Yes')  >> finish
        if_entries_not_present >> rail.Label(
            'No') >> get_all_project_task_report_details >> load_all_project_task_report >> load_csv_from_report_result >> create_collection_from_csv
        create_collection_from_csv >> query_all_projects >> query_distinct_projects
        query_distinct_projects >> foreach_distinct_project >> trigger_dag_run_task_update >> foreach_distinct_project_end
        foreach_distinct_project_end >> delete_entries_task_status_and_resource_update_lookup >> send_mail >> on_error >> send_failure_mail >> finish
        foreach_distinct_project >> foreach_distinct_project_end
    return dag

rail.for_each_instance(create_dag)
