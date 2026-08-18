from datetime import timedelta
import pendulum
import rail
from dxctechnology.compass_wbs_file_merger.utils import python_callable_method
from dxctechnology.compass_wbs_file_merger.utils import custom_method


null = None

# pylint: disable=too-many-statements


def create_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_wbs_file_merger_master_{config.dag_id_postfix}',
        description=f'DXCtechnology Compass Wbs file merger master V1.0 {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval_daily,
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
            trigger_dag_id=f'dxctechnology_compass_wbs_file_merger_child_{config.dag_id_postfix}',
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
            query='SELECT * FROM wbs_combined_data INNER JOIN (SELECT WBS as wbs_1, MAX(sequance_number) as \
                seq_number FROM wbs_combined_data GROUP BY WBS) as wbs_combined_data_1 on \
                    wbs_combined_data.WBS = wbs_combined_data_1.wbs_1 AND wbs_combined_data.sequance_number = wbs_combined_data_1.seq_number'
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
            remote_filepath=config.processing_file_directory +
            '/' +
            "Compass_WBS_Mergeddata_{{ result('get_time_for_file') }}.xml",
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
                'WBS',
                'Description',
                'Status',
                'CompanyCode',
                'ProjectType',
                'ProjectStart',
                'ProjectEnd',
                'PersonResponsible1',
                'PersonResponsible2',
                'TimeTrackingRequiredAttribute',
                'GlobalWBSIndicator',
                'IWOWBSIndicator',
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
                '{{ item.properties | attr_or_default("WBS", "") }}',
                '{{ item.properties | attr_or_default("Description", "") }}',
                '{{ item.properties | attr_or_default("Status", "") }}',
                '{{ item.properties | attr_or_default("CompanyCode", "")}}',
                '{{ item.properties | attr_or_default("ProjectType", "") }}',
                '{{ item.properties | attr_or_default("ProjectStart", "") }}',
                '{{ item.properties | attr_or_default("ProjectEnd", "") }}',
                '{{ item.properties | attr_or_default("PersonResponsible1", "") }}',
                '{{ item.properties | attr_or_default("PersonResponsible2", "") }}',
                '{{ item.properties | attr_or_default("TimeTrackingRequiredAttribute", "") }}',
                '{{ item.properties | attr_or_default("GlobalWBSIndicator", "") }}',
                '{{ item.properties | attr_or_default("IWOWBSIndicator", "") }}',
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

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message=config.error_template,
            properties={
                'WBS': "",
                'Description': "",
                # pylint: disable=line-too-long
                'Status': {config.error_template},
                'CompanyCode': "",
                'ProjectType': "",
                'ProjectStart': "",
                'ProjectEnd': "",
                'PersonResponsible1': "",
                'PersonResponsible2': "",
                'TimeTrackingRequiredAttribute': "",
                'GlobalWBSIndicator': "",
                'IWOWBSIndicator': "",
                'filedatetime': "",
                'sourcefilename': "",
                'sourcefilerecordcount': "",
                'sequenceno': "",
                'md5': "",
                'ignored': "",
                'jobid': "",
                'mergedfilename': ""
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                "Mergedecords": '{{ result("create_wbs_collection", "length") }}',
                "Recordtoprocess": '{{ result("query_wbs_data", "length") }}',
                "Mergeddatafilename": "Compass_WBS_Mergeddata_{{current_time('%Y%m%dT%H%M%S')}}.xml",
                "Job Status": "Processed"
            }
        )

        list_import_files >> has_any_wbs_files
        has_any_wbs_files >> rail.Label("Yes") >> get_sorted_files >> process_reading_files >> wait_for_reading_process >> gather_wbs_raw_data >> \
            merge_all_wbs_files >> create_wbs_collection >> query_wbs_data >> write_xml_file >> get_time_for_file >> \
            upload_xml_to_sftp >> compare_wbs_files >> update_wbs_logs >> render_logs_csv >> \
            upload_log_to_sftp >> finish >> catch_and_log_errors >> log_to_sumo
        has_any_wbs_files >> rail.Label("No") >> finish
    return dag


rail.for_each_instance(create_dag)
