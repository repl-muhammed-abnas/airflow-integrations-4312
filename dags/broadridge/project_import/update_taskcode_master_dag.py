
from datetime import timedelta
from airflow.models import Variable
from pendulum import datetime as dt
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'broadridge_project_import_update_taskcode_master_{config.instance}',
        description=f'Broadridge_project_import_update_taskcode_master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval1,
        start_date=dt(2023, 1, 1, tz=config.time_zone),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_reportdetails'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_reportdetails',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_reportdetails = rail.RepliconReportDetailsOperator(
            task_id='get_reportdetails',
            report_name=config.task_report_name,
        )

        generate_report_data = rail.run_report2(
            group_id="generate_report_data",
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_reportdetails')['uri'],
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "persistedReportName": null
                    }
                ]
            }
        )

        load_csv_data = rail.LoadCSVFileOperator(
            task_id="load_csv_data",
            document="{{result('generate_report_data.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_list_5 = rail.CreateCollectionOperator(
            task_id='create_list_5',
            source="{{ result('load_csv_data') }}",
            name="base_report",
            columns={
                'Project Code(DO NOT EDIT)': 'projectcode',
                'Metis_ProjectUID': 'metisprojectuid',
                'Task Code(DO NOT EDIT)': 'taskcode',
                'Metis_TaskUID': 'metistaskuid',
                'taskcomparison': 'taskcomparison',
                'projectcomparison': 'projectcomparison',
                'projecturi': 'projecturi',
                'taskuri': 'taskuri',
                'Project Name': 'projectname',
                'Task Name': 'taskname'
            }
        )

        query_list_for_task_12 = rail.QueryCollectionOperator(
            task_id='query_list_for_task_12',
            query="""SELECT * FROM  base_report WHERE  base_report.taskcomparison="2.00" OR  base_report.taskcomparison IS NULL""",
        )

        query_list_for_project_13 = rail.QueryCollectionOperator(
            task_id='query_list_for_project_13',
            query="""SELECT DISTINCT  base_report.projectcode, base_report.metisprojectuid, base_report.projectcomparison, base_report.projecturi, base_report.projectname FROM  base_report WHERE  base_report.projectcomparison = "2.00" OR  base_report.projectcomparison IS NULL""",
        )

        taskcode_lookup_table = rail.CreateLogOperator(
            task_id='taskcode_lookup_table'
        )

        if_taskmetisid_present = rail.IfOperator(
            task_id='if_taskmetisid_present',
            test="{{result('query_list_for_task_12','length') > 0}}",
            yes_task='process_taskcode_child1',
            no_task='if_projectmetisid_present'
        )

        process_taskcode_child1 = rail.TriggerDagRunForEachItemOperator(
            task_id='process_taskcode_child1',
            retries=0,
            items="{{ result('query_list_for_task_12') }}",
            trigger_dag_id=f'broadridge_project_import_update_project_and_task_code_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "task_items": item,
                "object": "task",
                "job_id": rail.render_template("{{dag_run_ecid()}}"),
                "task_lookuptable": rail.result('taskcode_lookup_table')
            }
        )

        wait_for_process_taskcode_child1 = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_taskcode_child1',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_taskcode_child1") }}'
        )

        if_projectmetisid_present = rail.IfOperator(
            task_id='if_projectmetisid_present',
            test="{{result('query_list_for_project_13','length') > 0}}",
            yes_task='process_taskcode_child2',
            no_task='search_entries_in_lookup_table'
        )

        process_taskcode_child2 = rail.TriggerDagRunForEachItemOperator(
            task_id='process_taskcode_child2',
            retries=0,
            items="{{ result('query_list_for_project_13') }}",
            trigger_dag_id=f'broadridge_project_import_update_project_and_task_code_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "task_items": item,
                "object": "project",
                "job_id": rail.render_template("{{dag_run_ecid()}}"),
                "taskuri": null,
                "taskmetisuid": null,
                "previoustaskcode": null,
                "task_lookuptable": rail.result('taskcode_lookup_table')
            }
        )

        wait_for_process_taskcode_child2 = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_taskcode_child2',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_taskcode_child2") }}'
        )

        search_entries_in_lookup_table = rail.FilterLogEntriesOperator(
            task_id='search_entries_in_lookup_table',
            log="{{ result('taskcode_lookup_table') }}",
            severity='Success',

        )

        if_entry_present = rail.IfOperator(
            task_id='if_entry_present',
            test='''{{ result('search_entries_in_lookup_table','length') > 0 }}''',
            yes_task="create_csv_lines_24",
            no_task="send_mail_28",
        )

        create_csv_lines_24 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_24',
            source="{{ result('search_entries_in_lookup_table')}}",
            header=['Project Name',
                    'Previous Project code',
                    'New Project code',
                    'Task Name',
                    'Previous Task code',
                    'New Task code'],
            row=lambda item: [
                item['properties']['projectname'],
                item['properties']['previouscode'],
                item['properties']['newcode'],
                item['properties']['taskname'],
                item['properties']['previoustaskcode'],
                item['properties']['newtaskcode']
            ],
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_lines_24')}}",
            output_file_name='project_task_codelogs_{{ current_time() }}.csv',
            expires_in_seconds=7*24*60*60,
        )

        send_mail_25 = rail.EmailOperator(
            task_id='send_mail_25',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''Broadridge | Project and Task Code Update  - Completed Successfully on {{ current_time() }} ''',
            html_content="templates/emails/update_successful_mail.html"
        )

        send_mail_28 = rail.EmailOperator(
            task_id='send_mail_28',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''Broadridge | Sandbox_Project and Task Code Update  - Completed Successfully on {{ current_time() }} ''',
            html_content="templates/emails/update_notsuccessful_mail.html"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_reportdetails
        get_reportdetails >> generate_report_data >> load_csv_data >> create_list_5
        create_list_5 >> query_list_for_task_12 >> query_list_for_project_13 >> taskcode_lookup_table
        taskcode_lookup_table >> if_taskmetisid_present >> rail.Label(
            'Yes') >> process_taskcode_child1 >> wait_for_process_taskcode_child1 >> if_projectmetisid_present
        if_taskmetisid_present >> rail.Label(
            'No') >> if_projectmetisid_present >> rail.Label(
            'Yes') >> process_taskcode_child2 >> wait_for_process_taskcode_child2 >> search_entries_in_lookup_table
        if_projectmetisid_present >> rail.Label(
            'No') >> search_entries_in_lookup_table
        search_entries_in_lookup_table >> if_entry_present >> rail.Label(
            'Yes') >> create_csv_lines_24 >> generate_download_link >> send_mail_25 >> log_to_sumo
        if_entry_present >> rail.Label(
            'No') >> send_mail_28 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
