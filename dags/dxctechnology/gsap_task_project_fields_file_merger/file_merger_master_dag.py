from datetime import timedelta
import pendulum
from pendulum import datetime
import rail
from dxctechnology.gsap_task_project_fields_file_merger.utils import python_callable_method
from dxctechnology.gsap_task_project_fields_file_merger.utils import custom_method


null = None

# pylint: disable=too-many-statements
def create_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsap_task_project_fields_file_merger_master_{config.instance}',
        description='DXCtechnology GSAP Tasks Project Fields file merger master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2023, 4, 1, tz=config.utc_timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        list_import_files = rail.SFTPListFilesOperator(
            task_id="list_import_files",
            paths=[config.input_filepath],
        )

        has_any_tasks_files = rail.IfOperator(
            task_id="has_any_tasks_files",
            test=lambda: custom_method.has_any_file(
                result_task_id="list_import_files", input_file_path=config.input_filepath),
            yes_task="get_sorted_files",
            no_task="finish"
        )

        get_sorted_files = rail.PythonOperator(
            task_id='get_sorted_files',
            python_callable=lambda: python_callable_method.get_sorted_files(
                config)
        )

        process_reading_files = rail.TriggerDagRunForEachItemOperator(
            task_id='process_reading_files',
            retries=0,
            items=lambda: rail.result("get_sorted_files"),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'dxctechnology_gsap_task_project_fields_file_merger_child_{config.instance}',
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

        gather_tasks_raw_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_tasks_raw_data',
            dag_runs="{{ result('process_reading_files')}}",
            dagrun_task_id='query_tasks_data',
            flatten=True
        )

        merge_all_tasks_files = rail.PythonOperator(
            task_id='merge_all_tasks_files',
            python_callable=python_callable_method.get_merged_tasks_data
        )

        create_tasks_collection = rail.CreateCollectionOperator(
            task_id='create_tasks_collection',
            source=lambda: rail.result('merge_all_tasks_files'),
            name='tasks_combined_data'
        )

        query_tasks_data = rail.QueryCollectionOperator(
            task_id='query_tasks_data',
            query='''SELECT * FROM tasks_combined_data INNER JOIN (SELECT WBS_Name as wbs_1, Task_Name as taskname_1,
                    MAX(CAST (sequance_number AS UNSIGNED)) as seq_number
                    FROM tasks_combined_data GROUP BY WBS_Name, Task_Name) as tasks_combined_data_1 on
                    tasks_combined_data.WBS_Name = tasks_combined_data_1.wbs_1 AND
                    tasks_combined_data.Task_Name = tasks_combined_data_1.taskname_1 AND
                    CAST (tasks_combined_data.sequance_number AS UNSIGNED) = tasks_combined_data_1.seq_number'''
        )

        write_xml_file = rail.RenderTemplateOperator(
            task_id='write_xml_file',
            target='artifact',
            template_file='templates/output/output_template.xml',
            dataset=python_callable_method.get_tasks_dataset,
        )

        get_time_for_file = rail.PythonOperator(
            task_id='get_time_for_file',
            python_callable=lambda: pendulum.now().strftime('%Y%m%dT%H%M%S')
        )

        upload_xml_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_xml_to_sftp',
            content="{{ result('write_xml_file') }}",
            remote_filepath=config.processing_filepath +
            '/' +
            "GSAP_Tasks_Mergeddata_{{ result('get_time_for_file') }}.xml",
        )

        compare_tasks_files = rail.PythonOperator(
            task_id='compare_tasks_files',
            python_callable=python_callable_method.get_data_from_compared_tasks_files
        )

        update_tasks_logs = rail.WriteLogOperator(
            task_id="update_tasks_logs",
            items=lambda: rail.result('compare_tasks_files'),
            severity="Success",
            message="Successfully Completed",
            properties=custom_method.get_tasks_status
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=[
                'WBS_Name',
                'Task_Name',
                'Task_Code',
                'Task_Start_Date',
                'Task_End_Date',
                'filedatetime',
                'sourcefilename',
                'sourcefilerecordcount',
                'sequenceno',
                'md5',
                'ignored',
                'jobid',
                'mergedfilename'
            ],
            row=[
                '{{ item.properties | attr_or_default("WBS_Name", "") }}',
                '{{ item.properties | attr_or_default("Task_Name", "") }}',
                '{{ item.properties | attr_or_default("Task_Code", "") }}',
                '{{ item.properties | attr_or_default("Task_Start_Date", "") }}',
                '{{ item.properties | attr_or_default("Task_End_Date", "") }}',
                '{{ item.properties | attr_or_default("filedatetime", "") }}',
                '{{ item.properties | attr_or_default("sourcefilename", "") }}',
                '{{ item.properties | attr_or_default("sourcefilerecordcount", "") }}',
                '{{ item.properties | attr_or_default("sequenceno", "") }}',
                '{{ item.properties | attr_or_default("md5", "") }}',
                '{{ item.properties | attr_or_default("ignored", "") }}',
                '{{ item.properties | attr_or_default("jobid", "") }}',
                '{{ item.properties | attr_or_default("mergedfilename", "") }}'
            ]
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/' + "MergeLog_{{ result('get_time_for_file') }}.csv")

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                "Mergedecords": '{{ result("create_tasks_collection", "length") }}',
                "Recordtoprocess": '{{ result("query_tasks_data", "length") }}',
                "Mergeddatafilename": "GSAP_Tasks_Mergeddata_{{result('get_time_for_file')}}.xml",
                "Job Status": "Processed"
            }
        )

        list_import_files >> has_any_tasks_files
        has_any_tasks_files >> rail.Label("Yes") >> get_sorted_files >> process_reading_files >>\
            wait_for_reading_process >> gather_tasks_raw_data >> \
            merge_all_tasks_files >> create_tasks_collection >> query_tasks_data >> write_xml_file >> get_time_for_file >> \
            upload_xml_to_sftp >> compare_tasks_files >> update_tasks_logs >> render_logs_csv >> \
            upload_log_to_sftp >> finish >> log_to_sumo
        has_any_tasks_files >> rail.Label("No") >> finish
    return dag


rail.for_each_instance(create_dag)
