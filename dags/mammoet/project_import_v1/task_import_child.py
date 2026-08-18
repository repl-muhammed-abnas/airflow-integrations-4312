from datetime import timedelta,datetime
import rail
from mammoet.project_import_v1.utils import request_payload
from rail import load_all_records
from airflow.models import Variable

def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.task_child_dag_id,
        description='Mammoet Task Import Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_second_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_task_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_task_log',
            end_task='send_import_complete_email',
        )

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_task_log = rail.CreateLogOperator(
             task_id='create_task_log'
        )

        create_collection_input_data = rail.CreateCollectionOperator(
            task_id = 'create_collection_input_data',
            source=lambda dag_run: dag_run.conf['task_data']['task'],
            name= 'inputdata',
            columns= {
                "projectcode": "projectcode",
                "taskcode": "taskcode",
                "taskname": "taskname",
                "taskstartdate": "taskstartdate",
                "taskenddate": "taskenddate",
                "taskstatus": "taskstatus"
            }
        )

        has_collection_data = rail.IfOperator(
            task_id='has_collection_data',
            test="{{ result('create_collection_input_data', 'length') > 0 }}",
            yes_task='query_any_task_blank_mandatory_check',
        )

        query_any_task_blank_mandatory_check = rail.QueryCollectionOperator(
            task_id='query_any_task_blank_mandatory_check',
            query="""SELECT * FROM inputdata WHERE NULLIF(projectcode,'') IS NULL OR NULLIF(taskcode,'') IS NULL OR
                NULLIF(taskname,'') IS NULL OR NULLIF(taskstatus,'') IS NULL"""
        )

        has_any_task_blank_mandatory_field = rail.IfOperator(
            task_id='has_any_task_blank_mandatory_field',
            test="{{ result('query_any_task_blank_mandatory_check', 'length') > 0 }}",
            yes_task='write_task_blank_mandatory_field_log',
            no_task='query_valid_task_data'
        )

        write_task_blank_mandatory_field_log = rail.WriteLogOperator(
            task_id="write_task_blank_mandatory_field_log",
            items="{{result('query_any_task_blank_mandatory_check')}}",
            log= "{{ result('create_task_log') }}",
            severity="Skipped",
            message="mandatory field is not present",
            properties=request_payload.get_invalid_task_logs
        )

        query_valid_task_data = rail.QueryCollectionOperator(
            task_id='query_valid_task_data',
            name='validtaskdata',
            query="""SELECT * FROM inputdata WHERE NULLIF(projectcode,'') IS NOT NULL AND NULLIF(taskcode,'') IS NOT NULL AND
                NULLIF(taskname,'') IS NOT NULL AND NULLIF(taskstatus,'') IS NOT NULL"""
        )

        has_valid_task_data = rail.IfOperator(
            task_id='has_valid_task_data',
            test="{{ result('query_valid_task_data', 'length') > 0 }}",
            yes_task='query_distinct_projects',
            no_task='render_logs_csv'
        )

        query_distinct_projects = rail.QueryCollectionOperator(
            task_id='query_distinct_projects',
            name='distinctprojects',
            query="""SELECT DISTINCT projectcode FROM validtaskdata"""
        )

        process_tasks = rail.TriggerDagRunForEachItemOperator(
            task_id = 'process_tasks',
            items= '{{ result("query_distinct_projects") }}',
            trigger_dag_id= config.process_each_task_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf= {
                'projectcode': '{{ item.projectcode }}',
                'log': '{{ result("create_task_log") }}'
            }
        )

        wait_for_process_tasks = rail.WaitForDagRunsSensor(
            task_id = 'wait_for_process_tasks',
            dag_runs= '{{ result("process_tasks") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('create_task_log'),
            header=[
                'projectcode',
                'taskcode',
                'taskname',
                'action',
                'details',
                'status',
                'ecid'
            ],
            row=[
                "{{item.properties.projectcode}}",
                "{{item.properties.taskcode}}",
                "{{item.properties.taskname}}",
                "{{item.properties.action}}",
                "{{item.properties.details}}",
                "{{item.properties.status}}",
                "{{item.ecid}}"
            ]
        )

        get_log_file_name = rail.PythonOperator(
            task_id = 'get_log_file_name',
            python_callable= lambda: 'logs_' + datetime.now().strftime('%m%d%YT%H%M%S')
        )

        upload_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_logs_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.task_log_filepath +
            '/{{ result("get_log_file_name") }}.csv',
        )

        get_errored_logs = rail.PythonOperator(
            task_id='get_errored_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == "Error", load_all_records(rail.result('render_logs_csv'))))), 'length')
        )

        generate_downloadable_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_downloadable_link",
            artifact_name="{{result('render_logs_csv')}}",
            output_file_name="Log_file_"+'{{result("get_log_file_name")}}'+".csv",
            expires_in_seconds=7*24*60*60
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Task import - " }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed successfully  \
                {%- endif -%} \
                {{ " - " + current_time("%Y/%m/%d/%H:%M:%S") }}',
            html_content='templates/import_complete.html',
            params= {
                'type': 'Task'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger'
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> send_import_complete_email
        can_run_batch_task >> rail.Label("No") >> create_task_log

        create_task_log >> create_collection_input_data >> has_collection_data

        has_collection_data >> rail.Label(
            "Yes") >> query_any_task_blank_mandatory_check >> has_any_task_blank_mandatory_field

        has_any_task_blank_mandatory_field >> rail.Label(
            "Yes") >> write_task_blank_mandatory_field_log >> query_valid_task_data

        has_any_task_blank_mandatory_field >> rail.Label(
            "No") >> query_valid_task_data >> has_valid_task_data

        has_valid_task_data >> rail.Label(
            "Yes") >> query_distinct_projects >> process_tasks >> wait_for_process_tasks >> render_logs_csv

        has_valid_task_data >> rail.Label(
            "No") >> render_logs_csv >> get_log_file_name >> upload_logs_to_sftp >> get_errored_logs >>\
                generate_downloadable_link >> send_import_complete_email >> log_to_sumo


    return dag


rail.for_each_instance(create_child_dag_wbs)
