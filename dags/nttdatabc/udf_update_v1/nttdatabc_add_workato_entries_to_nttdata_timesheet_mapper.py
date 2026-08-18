
from datetime import timedelta, datetime
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'nttdatabc_add_workato_entries_to_nttdata_timesheet_mapper_master_{config.instance}_v1',
        description=f'NTTDATABC Add Previous Entries From Workato To NTTDATA Mapper {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(days=config.manual_master_dag_interval),
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
                path=config.input_filepath,
                sftp_conn_id=config.sftp_conn_id,
                soft_fail_timeout=timedelta(minutes=10)
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        load_csv = rail.LoadCSVFileOperator(
            task_id = 'load_csv',
            document="{{result('download_file')}}",
            delimiter=',',
        )

        write_csv = rail.WriteCSVFileOperator(
            task_id = 'write_csv',
            source="{{result('load_csv')}}",
            delimiter=',',
            header = [
                'jobid',
                'loginname',
                'username',
                'timesheetperiod',
                'earlierudfvalue',
                'totalduration',
                'finalvalue',
                'date',
                'cleardate',
                'today'
            ],
            row= lambda item:[
                rail.render_template('{{dag_run_ecid()}}'),
                item['Loginname'],
                item['Username'],
                item['Timesheetperiod'],
                item['Earlierudfvalue'],
                item['Totalduration'],
                item['Finalvalue'],
                datetime.strptime(item['Date'],'%Y-%m-%dT%H:%M:%S.%f%z').strftime("%Y-%m-%d %H:%M:%S.%f") if item['Date'] else '',
                item['Check'],
                datetime.now().strftime('%Y-%m-%d')
            ]

        )

        create_collection = rail.CreateCollectionOperator(
            task_id = 'create_collection',
            source = "{{ result('write_csv') }}",
            name = "workatomapper",
        )

        get_today_date = rail.PythonOperator(
            task_id ='get_today_date',
            python_callable= lambda: datetime.now().strftime('%Y-%m-%d')
        )

        query_entries_to_be_kept = rail.QueryCollectionOperator(
            task_id = 'query_entries_to_be_kept',
            query="""SELECT * from "workatomapper" WHERE "cleardate" >= "today" """
        )

        get_timesheet_mapper = rail.CreateLogOperator(
            task_id = 'get_timesheet_mapper',
            tenant_wide_name="ntt_timesheet_mapper",
            existing_log_mode="append",
        )

        write_to_the_mapper = rail.WriteLogOperator(
            task_id='write_to_the_mapper',
            log="{{ result('get_timesheet_mapper') }}",
            items="{{result('query_entries_to_be_kept')}}",
            message='na',
            properties=lambda item: {
                "jobid": item['jobid'],
                "loginname": item['loginname'],
                "username": item['username'],
                "timesheetperiod": item['timesheetperiod'],
                "earlierudfvalue": item['earlierudfvalue'],
                "totalduration": item['totalduration'],
                "finalvalue": item['finalvalue'],
                "date": item['date'],
                "check": item['cleardate']
            }
        )


    new_file_sensor >> download_file >> load_csv >> write_csv >> create_collection
    create_collection >> get_today_date >> query_entries_to_be_kept >> get_timesheet_mapper >> write_to_the_mapper

    return dag

rail.for_each_instance(create_dag)
