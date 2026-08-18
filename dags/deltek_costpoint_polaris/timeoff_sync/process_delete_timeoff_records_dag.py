from datetime import timedelta
import uuid
import pendulum
from datetime import datetime
from airflow.models import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import rail


null = None

# pylint: disable=too-many-statements


def create_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'process_delete_timeoff_records_main_{config.instance}',
        description=f'process_delete_timeoff_records_main_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_delete_dag_interval),
        default_args={
        },
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        def do_get_last_run_date():
            today = pendulum.now()
            timeoff_start_date = today - \
                timedelta(days=config.number_of_days_past)
            timeoff_end_date = today + \
                timedelta(days=config.number_of_days_future)

            return {
                "start_date": timeoff_start_date.strftime("%Y-%m-%d %H:%M:%S"),
                "end_date": timeoff_end_date.strftime("%Y-%m-%d %H:%M:%S")
            }

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_last_run_date'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_last_run_date',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_last_run_date = rail.PythonOperator(
            task_id='get_last_run_date',
            python_callable=do_get_last_run_date
        )

        get_sql_timeoff_full_bookings_info = rail.MsSqlEncryptedOperator(
            task_id='get_sql_timeoff_full_bookings_info',
            mssql_conn_id=config.deltek_cospoint_sql_conn_id,
            sql=config.sql_delete_query,
        )

        choose_data_source = rail.IfOperator(
            task_id='choose_data_source',
            test=lambda: config.isFromSql,
            yes_task='get_sql_timeoff_full_bookings_info',
            no_task='get_cp_timeoff_bookings_for_periods',
        )

        get_cp_timeoff_bookings_for_periods = rail.DeltekCostPointODBCOperator(
            task_id='get_cp_timeoff_bookings_for_periods',
            deltek_costpoint_odbc_conn_id=config.odbc_conn_id,
            query=config.delete_odbc_query,
            query_params=[
                "{{result('get_last_run_date').start_date}}",
                "{{result('get_last_run_date').end_date}}",
                "{{result('get_last_run_date').start_date}}",
                "{{result('get_last_run_date').end_date}}",
                "{{result('get_last_run_date').start_date}}",
                "{{result('get_last_run_date').end_date}}"]
        )

        def get_timeoff_bookings_info():
            source_task = 'get_sql_timeoff_full_bookings_info' if config.isFromSql else 'get_cp_timeoff_bookings_for_periods'
            costpoint_timeoffbookings = rail.result(source_task)
            timeoffbookings = []
            for timeoff_info in costpoint_timeoffbookings:
                if config.isFromSql:
                    def _fmt(dt):
                        return dt.strftime('%Y-%m-%d %H:%M:%S') if hasattr(dt, 'strftime') else str(dt)
                    def _fmt_hours(h):
                        return float(h) if h is not None else 0.0
                    timeoffbookings.append({
                        "emp_id": timeoff_info[0],
                        "timeoff_type": timeoff_info[1],
                        "timeoff_start_date": _fmt(timeoff_info[3]),
                        "timeoff_end_date": _fmt(timeoff_info[4]),
                        "timeoff_hours": _fmt_hours(timeoff_info[5]),
                        "timeoff_modification_status": timeoff_info[7],
                        "timeoff_status": timeoff_info[8]
                    })
                else:
                    timeoffbookings.append({
                        "emp_id": timeoff_info['USER_ID'],
                        "timeoff_type": timeoff_info['TIME_OFF_TYPE'],
                        "timeoff_start_date": timeoff_info['TIME_OFF_STARTDATE'],
                        "timeoff_end_date": timeoff_info['TIME_OFF_ENDDATE'],
                        "timeoff_hours": timeoff_info['TIME_OFF_HOURS'],
                        "timeoff_modification_status": timeoff_info['ROWVERSION'],
                        "timeoff_status": timeoff_info['TIME_OFF_STATUS']
                    })
            return timeoffbookings

        get_current_cp_timeoff_bookings = rail.PythonOperator(
            task_id="get_current_cp_timeoff_bookings",
            python_callable=get_timeoff_bookings_info,
        )

        list_timeoff_file_from_s3 = rail.S3ListKeysOperator(
            task_id='list_timeoff_file_from_s3',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            prefix=config.s3_location_for_cp_timeoff
        )

        is_timeoff_file_available = rail.IfOperator(
            task_id='is_timeoff_file_available',
            test=lambda: (config.s3_location_for_cp_timeoff + "/" + config.cp_timeoff_file_name) in rail.result('list_timeoff_file_from_s3'),
            yes_task="download_timeoff_data_from_s3",
            no_task="deleted_timeoff_bookings",
        )

        download_timeoff_data_from_s3 = rail.S3DownloadFileOperator(
            task_id='download_timeoff_data_from_s3',
            bucket_name=config.s3_bucket_name,
            key_name=config.s3_location_for_cp_timeoff + "/" + config.cp_timeoff_file_name,
            aws_conn_id=config.aws_conn_id
        )

        load_timeoff_csv_file = rail.LoadCSVFileOperator(
            task_id="load_timeoff_csv_file",
            document="{{result('download_timeoff_data_from_s3')}}",
        )

        def get_deleted_timeoff_bookings():
            if rail.result('load_timeoff_csv_file'):
                previous_day_timeoff_records = rail.load_all_records(
                    rail.result('load_timeoff_csv_file'))
                current_timeoff_records = rail.result(
                    'get_current_cp_timeoff_bookings')
                if len(previous_day_timeoff_records) > 0:
                    deleted_bookings = []
                    for deleted_to in previous_day_timeoff_records:
                        timeoff_present = list(
                            filter(lambda x: x['emp_id'] == deleted_to['emp_id']
                                   and x['timeoff_start_date'] == deleted_to['timeoff_start_date'], current_timeoff_records))
                        if len(timeoff_present) == 0:
                            deleted_bookings.append(deleted_to)
                    return deleted_bookings
            return rail.result(
                'get_current_cp_timeoff_bookings')

        deleted_timeoff_bookings = rail.PythonOperator(
            task_id="deleted_timeoff_bookings",
            python_callable=get_deleted_timeoff_bookings,
        )

        render_timeoff_csv = rail.WriteCSVFileOperator(
            task_id='render_timeoff_csv',
            source="{{result('get_current_cp_timeoff_bookings')  | to_json }}",
            header=['emp_id', 'timeoff_type', 'timeoff_start_date',
                    'timeoff_end_date', 'timeoff_hours', 'timeoff_modification_status', 'timeoff_status'],
            row=['{{ item.emp_id }}', '{{  item.timeoff_type }}',
                 '{{ item.timeoff_start_date }}', '{{ item.timeoff_end_date }}',
                 '{{ item.timeoff_hours }}', '{{ item.timeoff_modification_status }}',
                 '{{ item.timeoff_status }}']
        )

        upload_timeoff_data_to_s3 = rail.S3UploadFileOperator(
            task_id='upload_timeoff_data_to_s3',
            source="{{ result('render_timeoff_csv') }}",
            key_name=config.s3_location_for_cp_timeoff + "/" + config.cp_timeoff_file_name,
            bucket_name=config.s3_bucket_name,
            aws_conn_id=config.aws_conn_id,
            replace=True
        )

        is_timeoff_deletion_available = rail.IfOperator(
            task_id='is_timeoff_deletion_available',
            test='''{{ result('deleted_timeoff_bookings') | length > 0 }}''',
            yes_task="get_user_details",
            no_task="finish",
        )

        def get_users_to_delete():
            user_for_fully_deleted_timeoff = rail.result(
                'deleted_timeoff_bookings')
            deleted_employee = []
            for user in user_for_fully_deleted_timeoff:
                if rail.find_first_by_attr_and_get_attr(deleted_employee, 'employeeId', user["emp_id"], "employeeId", None) is None:
                    deleted_employee.append({
                        "uri": null,
                        "loginName": null,
                        "employeeId": user["emp_id"],
                        "parameterCorrelationId": null
                    })
            return {
                "users": deleted_employee
            }

        def getemployeeinfo(response):
            emp_info = []
            response = response.json()['d']
            if not response:
                return []
            for usr in response:
                if usr:
                    emp_info.append(usr['uri'])
            return emp_info

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint='/services/UserService1.svc/BulkGetUsers2',
            data=get_users_to_delete,
            response_filter=getemployeeinfo
        )

        def getChunks(arrayof_obj):
            chunks_number = config.chunk_size
            chunks = [arrayof_obj[i:i + chunks_number]
                      for i in range(0, len(arrayof_obj), chunks_number)]
            return chunks

        def get_timeoff_array():
            all_timeoff_uris = rail.result('delete_timeoff_uris')
            chnkUris = getChunks(all_timeoff_uris)
            print("getChunks", chnkUris)
            return getChunks(all_timeoff_uris)

        foreach_get_user_timeoff = rail.ForEachOperator(
            task_id='foreach_get_user_timeoff',
            items=lambda: rail.result('get_user_details'),
            start_task='get_polaris_timeoff_details',
            end_task='foreach_get_user_timeoff_end'
        )

        def get_to_details_request(dag_run):
            today = pendulum.now()
            timeoff_start_date = today - \
                timedelta(days=config.number_of_days_past)
            timeoff_end_date = today + \
                timedelta(days=config.number_of_days_future)
            return {
                "userUri": rail.result('foreach_get_user_timeoff'),
                "dateRange": {
                    "startDate": {
                        "year": timeoff_start_date.year,
                        "month": timeoff_start_date.month,
                        "day": timeoff_start_date.day,
                    },
                    "endDate": {
                        "year": timeoff_end_date.year,
                        "month": timeoff_end_date.month,
                        "day": timeoff_end_date.day,
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }

        get_polaris_timeoff_details = rail.RepliconServiceOperator(
            task_id='get_polaris_timeoff_details',
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetailsForUserAndDateRange2",
            data=lambda dag_run: get_to_details_request(dag_run)
        )

        def get_user_timeoff_to_process(dag_run):
            deleting_timeoff_uri = []
            costpoint_timeoffs = rail.result('get_current_cp_timeoff_bookings')
            polaris_timeoffs = rail.result('get_polaris_timeoff_details')
            for toinfo in polaris_timeoffs:
                timeoff_start_date = datetime.strptime(str(toinfo['startDateDetails']['date']['month'])+'/'+str(
                    toinfo['startDateDetails']['date']['day'])+'/'+str(toinfo['startDateDetails']['date']['year']), config.replicon_timeoff_date_format)
                timeoff_end_date = datetime.strptime(str(toinfo['endDateDetails']['date']['month'])+'/'+str(
                    toinfo['endDateDetails']['date']['day'])+'/'+str(toinfo['endDateDetails']['date']['year']), config.replicon_timeoff_date_format)
                owner_loginname = toinfo['owner']['loginName']

                existing_timeoff = list(filter(lambda x:
                                               timeoff_start_date.date() == datetime.strptime(x['timeoff_start_date'], config.costpoint_to_date_format).date(
                                               ) and timeoff_end_date.date() == datetime.strptime(x['timeoff_end_date'], config.costpoint_to_date_format).date()
                                               and x['timeoff_type'].lower() != 'holiday'
                                               and x['emp_id'] == owner_loginname, costpoint_timeoffs))

                if len(existing_timeoff) == 0:
                    deleting_timeoff_uri.append(toinfo['uri'])
            return deleting_timeoff_uri

        delete_timeoff_uris = rail.PythonOperator(
            task_id='delete_timeoff_uris',
            python_callable=get_user_timeoff_to_process
        )

        if_timeoff_present_to_delete = rail.IfOperator(
            task_id='if_timeoff_present_to_delete',
            test='''{{ result('delete_timeoff_uris') | length > 0 }}''',
            yes_task="foreach_delete_user_timeoff",
            no_task="foreach_get_user_timeoff_end",
        )

        foreach_delete_user_timeoff = rail.ForEachOperator(
            task_id='foreach_delete_user_timeoff',
            items=lambda: get_timeoff_array(),
            start_task='create_timeOff_delete_batch',
            end_task='foreach_delete_user_timeoff_end'
        )

        create_timeOff_delete_batch = rail.RepliconServiceOperator(
            task_id="create_timeOff_delete_batch",
            endpoint="/services/TimeOffService1.svc/CreateTimeOffDeleteBatch",
            data=lambda: {
                "timeOffUris": rail.result('foreach_delete_user_timeoff')
            }
        )

        execute_timeOff_delete_batch = rail.RepliconServiceOperator(
            task_id="execute_timeOff_delete_batch",
            endpoint="/services/TimeOffService1.svc/ExecuteTimeOffDeleteBatch",
            data=lambda: {
                "timeOffDeleteBatchUri": rail.result("create_timeOff_delete_batch")
            }
        )

        foreach_delete_user_timeoff_end = rail.EmptyOperator(
            task_id='foreach_delete_user_timeoff_end',
        )

        foreach_get_user_timeoff_end = rail.EmptyOperator(
            task_id='foreach_get_user_timeoff_end',
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> get_last_run_date
        get_last_run_date >> choose_data_source
        choose_data_source >> rail.Label('Yes') >> get_sql_timeoff_full_bookings_info >> get_current_cp_timeoff_bookings
        choose_data_source >> rail.Label('No') >> get_cp_timeoff_bookings_for_periods >> get_current_cp_timeoff_bookings
        get_current_cp_timeoff_bookings >> \
            list_timeoff_file_from_s3 >> is_timeoff_file_available
        is_timeoff_file_available >> rail.Label(
            'No') >> deleted_timeoff_bookings
        is_timeoff_file_available >> rail.Label('Yes') >> download_timeoff_data_from_s3 >> load_timeoff_csv_file >> \
            deleted_timeoff_bookings >> render_timeoff_csv >> upload_timeoff_data_to_s3 >> is_timeoff_deletion_available
        is_timeoff_deletion_available >> rail.Label('No') >> finish
        is_timeoff_deletion_available >> rail.Label('Yes') >> get_user_details >> \
            foreach_get_user_timeoff >> get_polaris_timeoff_details >> \
            delete_timeoff_uris >> if_timeoff_present_to_delete
        if_timeoff_present_to_delete >> rail.Label(
            'No') >> foreach_get_user_timeoff_end
        if_timeoff_present_to_delete >> rail.Label(
            'Yes') >> foreach_delete_user_timeoff
        foreach_delete_user_timeoff >> create_timeOff_delete_batch >> \
            execute_timeOff_delete_batch >> foreach_delete_user_timeoff_end
        foreach_delete_user_timeoff >> foreach_delete_user_timeoff_end >> foreach_get_user_timeoff_end
        foreach_get_user_timeoff >> foreach_get_user_timeoff_end >> finish

    return dag


rail.for_each_instance(create_dag)
