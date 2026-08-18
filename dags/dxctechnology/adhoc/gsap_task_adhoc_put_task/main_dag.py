from datetime import timedelta
import rail

def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id = f'dxctechnology_gsap_task_adhoc_put_task_{config.instance}',
        description = 'DXC_GSAP_Task ADHOC',
        company_key = config.company_key,
        replicon_conn_id = config.replicon_conn_id,
        schedule_interval = timedelta(seconds=30),
        max_active_runs = 1,
        default_args = {
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id = 'new_file_sensor',
            path = config.input_filepath,
            soft_fail_timeout= timedelta(minutes=10),
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id = 'download_file',
            remote_filepath = "{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id = 'was_new_file_found',
            trigger_rule = 'all_done',
            test = '{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task = 'archive_file',
            no_task = 'delete_this_dagrun'
        )

        archive_file = rail.SFTPMoveFileOperator(
                task_id = 'archive_file',
                trigger_rule='all_done',
                existing_filename = '{{ result("new_file_sensor") }}',
                new_filename = config.archive_filepath + "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        load_data = rail.LoadCSVFileOperator(
            task_id='load_data',
            document="{{ result('download_file') }}"
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source="{{ result('load_data') }}",
            name="inputdatacollection",
            columns={
                'Name': 'name',
                'Code': 'code',
                'WSB / SO': 'wbs',
                'Start Date': 'startdate',
                'End Date': 'enddate',
                'Time And Expense Entry Type': 'entrytype',
                'Allow Time Entry':'allowentry',
                'Is Closed':'isclosed',
                'Task Type':'tasktype',
            }
        )


        query_valid_records = rail.QueryCollectionOperator(
            task_id = "query_valid_records",
            query="SELECT * FROM inputdatacollection WHERE LENGTH(code) <= 50"
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id = "query_invalid_records",
            query="SELECT * FROM inputdatacollection WHERE LENGTH(code) > 50"
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id = "log_invalid_records",
            severity="Skipped",
            items="{{result('query_invalid_records')}}",
            message="Task code value is more than 50 char in length",
            properties=lambda item: {
                'task1':item['name'].split('|')[0],
                'task2':item['name'].split('|')[1],
                'wbs':item['wbs'],
                'code': item['code'],
                'status': 'Skipped'
            }
        )

        process_tasks = rail.trigger_parallel_dagrun(
            task_id='process_tasks',
            items=lambda: rail.result('query_valid_records'),
            parallel_count=config.trigger_parallel_dagrun_count_tasks,
            trigger_dag_id=f'dxctechnology_gsap_task_adhoc_put_task_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item:{
                'task1':item['name'].split('|')[0],
                'task2':item['name'].split('|')[1],
                'wbs':item['wbs'],
                'code': item['code'],
                'startdate':item['startdate'],
                'enddate':item['enddate'],
            }
        )

        add_blank_record = rail.WriteLogOperator(
            task_id='add_blank_record',
            message='Processed',
            severity='Success',
            properties={
                'wbs': '',
                'task1': '',
                'task2': '',
                'status': 'Success'
            }
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ get_master_log() }}",
            # pylint: disable=line-too-long
            header=['WBS','TaskLevel1','TaskLevel2','Status','Details'],
            row=['{{ item.properties | attr_or_default("wbs", "") }}',
                 '{{ item.properties | attr_or_default("task1", "") }}',
                 '{{ item.properties | attr_or_default("task2", "") }}',
                 '{{ item.properties | attr_or_default("status", "") }}',
                 '{{ item.message }}'
                 ]
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv',
        )


        new_file_sensor >> download_file >> rail.Label('Always') >> was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        was_new_file_found >> rail.Label('Yes') >> archive_file
        download_file >> load_data >> create_input_data_collection >> [query_valid_records, query_invalid_records]
        query_valid_records >> process_tasks >> add_blank_record >> render_logs_csv >> upload_log_to_sftp
        query_invalid_records >> log_invalid_records >> render_logs_csv


    return dag

rail.for_each_instance(create_main_airflow_dag)
