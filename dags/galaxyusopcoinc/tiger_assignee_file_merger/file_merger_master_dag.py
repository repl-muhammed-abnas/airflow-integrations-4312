from datetime import timedelta
from airflow.models import Variable
import math
import pendulum
from pendulum import datetime
import rail

from galaxyusopcoinc.tiger_assignee_file_merger.utils import custom_methods
null = None

# pylint: disable=too-many-statements
def create_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'vialtopartners_tiger_assignee_file_merger_master_{config.instance}',
        description='Vialto Partners Tiger Assignee file merger master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 4, 1, tz=config.utc_timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='list_import_files'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='list_import_files',
            end_task='finish',
        )

        list_import_files = rail.SFTPListFilesOperator(
            task_id="list_import_files",
            paths=[config.input_filepath],
        )

        has_any_files = rail.IfOperator(
            task_id="has_any_files",
            test=lambda: custom_methods.has_any_file(
                result_task_id="list_import_files", input_file_path=config.input_filepath),
            yes_task="get_sorted_files",
            no_task="finish"
        )

        get_sorted_files = rail.PythonOperator(
            task_id='get_sorted_files',
            python_callable=lambda: custom_methods.get_sorted_files(
                config)
        )

        process_reading_files = rail.TriggerDagRunForEachItemOperator(
            task_id='process_reading_files',
            retries=0,
            items=lambda: rail.result("get_sorted_files"),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'vialtopartners_tiger_assignee_file_merger_child_{config.instance}',
            conf={
                "file_name": "{{item.file_name}}",
                "file_path": config.input_filepath,
                "file_index": "{{item.file_index}}",
                "file_date_time": "{{item.modify}}"
            }
        )

        wait_for_reading_process = rail.WaitForDagRunsSensor(
            task_id='wait_for_reading_process',
            dag_runs='{{ result("process_reading_files") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_raw_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_raw_data',
            dag_runs="{{ result('process_reading_files')}}",
            dagrun_task_id='query_data',
            flatten=True
        )

        merge_all_files = rail.PythonOperator(
            task_id='merge_all_files',
            python_callable=custom_methods.get_merged_data,
            execution_timeout=timedelta(hours=config.execution_timeout_hours),
        )

        create_assignee_collection = rail.CreateCollectionOperator(
            task_id='create_assignee_collection',
            source=lambda: rail.result('merge_all_files'),
            name='combined_data'
        )

        query_assignee_data = rail.QueryCollectionOperator(
            task_id='query_assignee_data',
            name="query_assignee_data",
            query='''SELECT * FROM combined_data INNER JOIN (SELECT assigneeid as assigneeid_1, clientshortname as clientshortname_1,
                    MAX(CAST (sequence_number AS UNSIGNED)) as seq_number
                    FROM combined_data GROUP BY assigneeid, clientshortname) as combined_data_1 on
                    combined_data.assigneeid = combined_data_1.assigneeid_1 AND
                    combined_data.clientshortname = combined_data_1.clientshortname_1 AND
                    CAST (combined_data.sequence_number AS UNSIGNED)= combined_data_1.seq_number ORDER BY combined_data.clientshortname'''
        )

        assignee_data_with_index = rail.QueryCollectionOperator(
            task_id="assignee_data_with_index",
            name="assignee_data_with_index",
            query="""SELECT ROW_NUMBER() OVER (ORDER BY query_assignee_data.clientshortname) AS ROW_NUM, * FROM query_assignee_data"""
        )

        get_batch_list = rail.PythonOperator(
            task_id="get_batch_list",
            python_callable=lambda: list(
                range(0, math.ceil(rail.result('assignee_data_with_index', 'length')/config.BATCH_SIZE)))
        )

        get_time_for_file = rail.PythonOperator(
            task_id='get_time_for_file',
            python_callable=lambda: pendulum.now().strftime('%Y%m%dT%H%M%S')
        )

        create_batch_log = rail.CreateLogOperator(
            task_id='create_batch_log'
        )

        process_split_csv_batch = rail.TriggerDagRunForEachItemOperator(
            task_id='process_split_csv_batch',
            retries=0,
            items=lambda: rail.result("get_batch_list"),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'vialtopartners_tiger_assignee_file_merger_split_csv_batch_child_{config.instance}',
            conf=lambda item, index:{
                "record_start_index": (item*config.BATCH_SIZE)+1,
                "record_end_index": (item+1)*config.BATCH_SIZE,
                "index": index,
                "get_time_for_file": rail.result('get_time_for_file'),
                "actual_file_name" : f"Tiger_Assignee_Mergeddata_{rail.result('get_time_for_file') }",
                'batch_log':  rail.result('create_batch_log')
            }
        )

        wait_for_process_split_csv_batch = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_split_csv_batch',
            dag_runs='{{ result("process_split_csv_batch") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        render_batch_logs = rail.WriteCSVFileOperator(
            task_id='render_batch_logs',
            source="{{ result('create_batch_log') }}",
            header=[
                'Actual File Name',
                'File Name Processed',
                'Job ID'
            ],
            row=[
                '{{ item.properties.actual_file_name }}',
                '{{ item.properties.file_name_processed }}',
                '{{ item.ecid }}'
            ]
        )

        upload_batch_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_batch_logs_to_sftp',
            content="{{ result('render_batch_logs') }}",
            remote_filepath=config.batch_log_filepath +
            '/Batchlog_Tiger_Assignee_Mergeddata_{{ result("get_time_for_file") }}.csv'
        )

        compare_assignee_files = rail.QueryCollectionOperator(
            task_id='compare_assignee_files',
            query="""SELECT * , "No" as "ignored" FROM combined_data cd WHERE cd.md5 IN (SELECT qad.md5 from query_assignee_data qad) UNION
                    SELECT * , "Yes" as "ignored" FROM combined_data cd WHERE cd.md5 NOT IN (SELECT qad.md5 from query_assignee_data qad)"""
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('compare_assignee_files'),
            header=[
                'Tiger client long name',
                'Tiger short name',
                'assignee ID',
                'First Name',
                'Last Name',
                'Status',
                'filedatetime',
                'sourcefilename',
                'sourcefilerecordcount',
                'sequenceno',
                'md5',
                'ignored',
                'jobid',
                'mergedfilename'
            ],
            row=custom_methods.translate_row
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.merge_log_filepath +
            '/' + "Mergelog_Tiger_Assignee_Mergeddata_{{ result('get_time_for_file') }}.csv")

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                "Mergedecords": '{{ result("create_assignee_collection", "length") }}',
                "Recordtoprocess": '{{ result("query_assignee_data", "length") }}',
                "Mergeddatafilename": "Tiger_Assignee_Mergeddata_{{result('get_time_for_file') }}.csv",
                "Job Status": "Processed"
            }
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> list_import_files

        list_import_files >> has_any_files
        has_any_files >> rail.Label("Yes") >> get_sorted_files >> process_reading_files >> wait_for_reading_process >> gather_raw_data >> \
            merge_all_files >> create_assignee_collection >> query_assignee_data >> assignee_data_with_index >> get_batch_list

        get_batch_list >> get_time_for_file >> create_batch_log >> process_split_csv_batch >> wait_for_process_split_csv_batch

        wait_for_process_split_csv_batch >> render_batch_logs >> upload_batch_logs_to_sftp >> compare_assignee_files

        compare_assignee_files >> render_logs_csv >> \
            upload_log_to_sftp >> finish >> log_to_sumo
        has_any_files >> rail.Label("No") >> finish

        log_to_sumo >> can_fail_dag >> rail.Label('Yes') >> fail_dagrun

    return dag

rail.for_each_instance(create_dag)
