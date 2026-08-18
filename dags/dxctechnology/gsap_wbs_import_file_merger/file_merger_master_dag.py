from datetime import timedelta
import pendulum
from pendulum import datetime
import rail
from dxctechnology.gsap_wbs_import_file_merger.utils import python_callable_method
from dxctechnology.gsap_wbs_import_file_merger.utils import custom_method


null = None

# pylint: disable=too-many-statements
def create_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsap_wbs_file_merger_master_{config.instance}',
        description='DXCtechnology GSAP WBS file merger master',
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

        has_any_wbs_files = rail.IfOperator(
            task_id="has_any_wbs_files",
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
            trigger_dag_id=f'dxctechnology_gsap_wbs_file_merger_child_{config.instance}',
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

        gather_wbs_raw_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_wbs_raw_data',
            dag_runs="{{ result('process_reading_files')}}",
            dagrun_task_id='query_wbs_data',
            flatten=True
        )

        merge_all_wbs_files = rail.PythonOperator(
            task_id='merge_all_wbs_files',
            python_callable=python_callable_method.get_merged_wbs_data
        )

        create_wbs_collection = rail.CreateCollectionOperator(
            task_id='create_wbs_collection',
            source=lambda: rail.result('merge_all_wbs_files'),
            name='wbs_combined_data'
        )

        query_wbs_data = rail.QueryCollectionOperator(
            task_id='query_wbs_data',
            query='''SELECT * FROM wbs_combined_data INNER JOIN (SELECT WBS_Name as wbs_1,
                    MAX(CAST (sequance_number AS UNSIGNED)) as seq_number
                    FROM wbs_combined_data GROUP BY WBS_Name) as wbs_combined_data_1 on
                    wbs_combined_data.WBS_Name = wbs_combined_data_1.wbs_1 AND
                    CAST (wbs_combined_data.sequance_number AS UNSIGNED)= wbs_combined_data_1.seq_number'''
        )

        write_xml_file = rail.RenderTemplateOperator(
            task_id='write_xml_file',
            target='artifact',
            template_file='templates/output/output_template.xml',
            dataset=python_callable_method.get_wbs_dataset,
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
            "GSAP_WBS_Mergeddata_{{ result('get_time_for_file') }}.xml",
        )

        compare_wbs_files = rail.PythonOperator(
            task_id='compare_wbs_files',
            python_callable=python_callable_method.get_data_from_compared_wbs_files
        )

        update_wbs_logs = rail.WriteLogOperator(
            task_id="update_wbs_logs",
            items=lambda: rail.result('compare_wbs_files'),
            severity="Success",
            message="Successfully Completed",
            properties=custom_method.get_wbs_status
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            header=[
                'WBS_Name',
                'WBS_Code',
                'CompanyCode',
                'Project_Type',
                'Profit_Centre',
                'Task_Indicator',
                'Project_Start',
                'Project_End',
                'Primary_Project_Manager_ID',
                'Primary_Project_Manager_Name',
                'WBS_Currency',
                'Parent_Project',
                'WBS_Parent_Project',
                'Customer_Name',
                'PSA_Flag',
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
                '{{ item.properties | attr_or_default("WBS_Code", "") }}',
                '{{ item.properties | attr_or_default("Company_Code", "") }}',
                '{{ item.properties | attr_or_default("Project_Type", "")}}',
                '{{ item.properties | attr_or_default("Profit_Centre", "") }}',
                '{{ item.properties | attr_or_default("Task_Indicator", "") }}',
                '{{ item.properties | attr_or_default("Project_Start", "") }}',
                '{{ item.properties | attr_or_default("Project_End", "") }}',
                '{{ item.properties | attr_or_default("Primary_Project_Manager_ID", "") }}',
                '{{ item.properties | attr_or_default("Primary_Project_Manager_Name", "") }}',
                '{{ item.properties | attr_or_default("WBS_Currency", "") }}',
                '{{ item.properties | attr_or_default("Parent_Project", "") }}',
                '{{ item.properties | attr_or_default("WBS_Parent_Project", "") }}',
                '{{ item.properties | attr_or_default("Customer_Name", "") }}',
                '{{ item.properties | attr_or_default("PSA_Flag", "") }}',
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
                "Mergedecords": '{{ result("create_wbs_collection", "length") }}',
                "Recordtoprocess": '{{ result("query_wbs_data", "length") }}',
                "Mergeddatafilename": "GSAP_WBS_Mergeddata_{{current_time('%Y%m%dT%H%M%S')}}.xml",
                "Job Status": "Processed"
            }
        )

        list_import_files >> has_any_wbs_files
        has_any_wbs_files >> rail.Label("Yes") >> get_sorted_files >> process_reading_files >> wait_for_reading_process >> gather_wbs_raw_data >> \
            merge_all_wbs_files >> create_wbs_collection >> query_wbs_data >> write_xml_file >> get_time_for_file >> \
            upload_xml_to_sftp >> compare_wbs_files >> update_wbs_logs >> render_logs_csv >> \
            upload_log_to_sftp >> finish >> log_to_sumo
        has_any_wbs_files >> rail.Label("No") >> finish
    return dag


rail.for_each_instance(create_dag)
