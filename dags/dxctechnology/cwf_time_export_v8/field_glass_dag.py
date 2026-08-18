from datetime import datetime
import json
import pendulum
import rail
from dxctechnology.cwf_time_export_v8.utils import request_payload, python_callable_method
from dxctechnology.cwf_time_export_v8.mapper.location import location_mapper
from dxctechnology.cwf_time_export_v8.task.field_glass_compass_task import get_compass_task
from dxctechnology.cwf_time_export_v8.task.c1_task import get_c1_task
from dxctechnology.cwf_time_export_v8.task.gsap_task import get_gsap_task


null = None
header = [
    'Work_Order_Id',
    'Last_Name',
    'First_Name',
    'Date',
    'Week_Start_Date',
    'Cost_Center_Code',
    'Task_Code',
    'Rate_Category_Code',
    'UOM',
    'Sat_Hrs',
    'Sun_Hrs',
    'Mon_Hrs',
    'Tue_Hrs',
    'Wed_Hrs',
    'Thu_Hrs',
    'Fri_Hrs',
    '[c] CATW'
]


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_cwf_time_export_field_glass_{config.instance}_v8',
        description=f'DXCTechnology_CWF Time - Fieldglass report export v8 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        max_active_tasks=config.dag_max_active_tasks,
        schedule_interval=config.field_glass_schedule_interval,
        start_date= pendulum.datetime(2023, 4, 1, tz=config.est_timezone),
        default_args={
            'sftp_conn_id': config.field_glass_sftp_conn_id,
        },
    ) as dag:

        get_field_glass_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_field_glass_report_details',
            report_name=config.field_glass_report_name,
        )

        get_report_start_date = rail.PythonOperator(
            task_id='get_report_start_date',
            python_callable=config.field_glass_date_filter['report_start_date'],
        )

        get_report_end_date = rail.PythonOperator(
            task_id='get_report_end_date',
            python_callable=config.field_glass_date_filter['report_end_date'],
        )

        get_timesheet_start_date = rail.PythonOperator(
            task_id='get_timesheet_start_date',
            python_callable=config.field_glass_date_filter['timesheet_start_date'],
        )

        generate_field_glass_report_batch = rail.RepliconServiceOperator(
            task_id='generate_field_glass_report_batch',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_field_glass_report_details')['uri'],
                        "filterValues": [
                            {
                                "reportFilterUri":
                                rail.find_first_by_attr_and_get_attr(
                                    rail.result('get_field_glass_report_details')[
                                        'filterConfiguration']
                                    ['enabledFilters'], 'displayText', 'ApprovalDateFilter', 'uri'),
                                "value": None,
                            },
                            {
                                "reportFilterUri":
                                rail.find_first_by_attr_and_get_attr(
                                    rail.result('get_field_glass_report_details')[
                                        'filterConfiguration']
                                    ['enabledFilters'], 'displayText', 'ApprovalDateFilter', 'uri'),
                                "value": rail.result('get_report_start_date'),
                            },
                            {
                                "reportFilterUri":
                                rail.find_first_by_attr_and_get_attr(
                                    rail.result('get_field_glass_report_details')[
                                        'filterConfiguration']
                                    ['enabledFilters'], 'displayText', 'ApprovalDateFilter', 'uri'),
                                "value": rail.result('get_report_end_date'),
                            }
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]
            }
        )

        execute_report_batch = rail.batch_execution(
            group_id='execute_report_batch',
            creation_task_id=generate_field_glass_report_batch.task_id,
        )

        get_field_glass_report_batch_results = rail.RepliconServiceOperator(
            task_id="get_field_glass_report_batch_results",
            endpoint="/services/ReportService1.svc/GetReportGenerationBatchResults",
            data={
                'reportGenerationBatchUri': "{{result('generate_field_glass_report_batch')}}"},
        )

        has_empty_report_data = rail.IfOperator(
            task_id='has_empty_report_data',
            test=lambda: rail.result("get_field_glass_report_batch_results")[
                'reportGenerationResults'][0]['payload'].startswith("No Data"),
            yes_task="get_timesheet_period_report_details",
            no_task="has_invalid_report_field",
        )

        send_mail_nodata = rail.EmailOperator(
            task_id='send_mail_nodata',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Replicon CWF time export for Fieldglass - No data -  {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br />
            The Replicon  CWF time export for FIELDGLASS job is Completed and there was no data to be exported.
        <br />
        <br />
        Regards,<br />
        Deltek Inc.
        </p> ''',
            params=None,
        )

        # pylint: disable=line-too-long
        has_invalid_report_field = rail.IfOperator(
            task_id='has_invalid_report_field',
            test=lambda: not rail.result("get_field_glass_report_batch_results")['reportGenerationResults'][0]['payload'].startswith(
                'User Last Name,User First Name,Entry Date,Timesheet Period,Timesheet Start Date,Attendance Type Code,Rate Type,Total Hrs,Company Code Code,Login Name,Location,Cost Center,UserUri,Company Code (Current)'),
            yes_task="fail_for_invalid_report_field",
            no_task="load_report_csv",
        )

        fail_for_invalid_report_field = rail.FailOperator(
            task_id='fail_for_invalid_report_field',
            message="Base report columns modified.",
        )

        load_report_csv = rail.LoadCSVFileOperator(
            task_id="load_report_csv",
            document="{{ result('get_field_glass_report_batch_results')['reportGenerationResults'][0]['payload'] }}",
        )

        create_report_collection = rail.CreateCollectionOperator(
            task_id='create_report_collection',
            source="{{ result('load_report_csv') }}",
            name="rawdata",
            columns={
                'User Last Name': 'userlastname',
                'User First Name': 'userfirstname',
                'Entry Date': 'entrydate',
                'Timesheet Period': 'timesheetperiod',
                'Timesheet Start Date': 'timesheetstartdate',
                'Attendance Type Code': 'attendancetypecode',
                'Rate Type': 'ratetype',
                'Total Hrs': 'totalhrs',
                'Company Code Code': 'companycodecode',
                'Login Name': 'loginname',
                'Location': 'location',
                'Cost Center': 'costcenter',
                'UserUri': 'useruri',
                'Company Code (Current)': 'company_code_current',
                'Employee ID': 'employeeid'
            }
        )

        get_timesheet_period_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_timesheet_period_report_details',
            report_name=config.field_glass_timesheet_period_report_name
        )

        run_timesheet_period_report_group = rail.run_report2(
            group_id='run_report',
            report_params=request_payload.get_approved_timesheets_payload
        )

        is_timesheet_period_report_failed = rail.IfOperator(
            task_id='is_timesheet_period_report_failed',
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].error | is_truthy }}",
            yes_task='fail_timesheet_period_report_generation',
            no_task='timesheet_period_report_has_data'
        )

        fail_timesheet_period_report_generation = rail.FailOperator(
            task_id='fail_timesheet_period_report_generation',
            message="{{ result('run_report.get_report_result').reportGenerationResults[0].error }}"
        )

        timesheet_period_report_has_data = rail.IfOperator(
            task_id='timesheet_period_report_has_data',
            test="{{ result('run_report.get_report_result','has_data') }}",
            yes_task='is_timesheet_period_report_has_expected_columns',
            no_task='is_data_exists'
        )

        is_timesheet_period_report_has_expected_columns = rail.IfOperator(
            task_id='is_timesheet_period_report_has_expected_columns',
            # pylint: disable=consider-using-f-string
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | \
                starts_with('%s') }}" % config.expected_timesheet_period_report_columns,
            yes_task='load_timesheet_period_report_csv',
            no_task='fail_timesheet_period_no_expected_columns',
        )

        fail_timesheet_period_no_expected_columns = rail.FailOperator(
            task_id='fail_timesheet_period_no_expected_columns',
            message='''Base report column order doesn't match'''
        )

        load_timesheet_period_report_csv = rail.LoadCSVFileOperator(
            task_id="load_timesheet_period_report_csv",
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}"
        )

        create_timesheet_period_report_collection = rail.CreateCollectionOperator(
            task_id='create_timesheet_period_report_collection',
            source="{{ result('load_timesheet_period_report_csv') }}",
            name="rawdata2",
            columns={
                'User Last Name': 'userlastname',
                'User First Name': 'userfirstname',
                'Timesheet Period': 'timesheetperiod',
                'Timesheet Start Date': 'timesheetstartdate',
                'Timesheet End Date': 'timesheetenddate',
                'Total Hrs (In Period)': 'totalhrs',
                'Company Code Code (Current)': 'companycodecode',
                'Login Name': 'loginname',
                'Location (Current)': 'location',
                'Cost Center (Current)': 'costcenter',
                'UserUri': 'useruri',
                'Company Code (Current)': 'company_code_current',
                'Employee ID': 'employeeid',
                'Scheduled Hrs (In Period)': 'scheduledhrs'
            }
        )

        query_zero_hours_timesheets = rail.QueryCollectionOperator(
            task_id='query_zero_hours_timesheets',
            query='''SELECT * FROM rawdata2 WHERE CAST(totalhrs AS FLOAT) = 0 AND CAST(scheduledhrs AS FLOAT) > 0''',
        )

        if_zero_hours_timesheets_exists = rail.IfOperator(
            task_id='if_zero_hours_timesheets_exists',
            test='{{ result("query_zero_hours_timesheets", "length") > 0 }}',
            yes_task='generate_zero_hours_timesheets',
            no_task='is_data_exists'
        )

        generate_zero_hours_timesheets = rail.CreateCollectionOperator(
            task_id='generate_zero_hours_timesheets',
            source=lambda: python_callable_method.get_zero_hours_timesheets(config.input_date_format, config.rate_types_list),
            columns=[
                'userlastname',
                'userfirstname',
                'entrydate',
                'timesheetperiod',
                'timesheetstartdate',
                'attendancetypecode',
                'ratetype',
                'totalhrs',
                'companycodecode',
                'loginname',
                'location',
                'costcenter',
                'useruri',
                'company_code_current',
                'employeeid'
            ],
            name="zero_hrs_generated_records"
        )

        is_data_exists = rail.IfOperator(
            task_id='is_data_exists',
            test=lambda: rail.result("create_report_collection") or rail.result("generate_zero_hours_timesheets"),
            yes_task='query_to_merge_data',
            no_task='send_mail_nodata'
        )

        query_to_merge_data = rail.PythonOperator(
            task_id='query_to_merge_data',
            python_callable=python_callable_method.get_query_to_merge_data
        )

        merge_all_records = rail.QueryCollectionOperator(
            task_id='merge_all_records',
            query='{{ result("query_to_merge_data") }}'
        )

        query_list_c1userdata = rail.CreateCollectionOperator(
            task_id='query_list_c1userdata',
            source=lambda: list(filter(lambda item: item['companycodecode'] == 'C1' and
                                       datetime.strptime(item['timesheetstartdate'], config.input_date_format) >= datetime.strptime(
                                           rail.result('get_timesheet_start_date'), config.output_date_format),
                                       rail.load_all_records(rail.result("merge_all_records")))),
            columns=['userlastname',
                     'userfirstname',
                     'entrydate',
                     'timesheetperiod',
                     'timesheetstartdate',
                     'attendancetypecode',
                     'ratetype',
                     'totalhrs',
                     'companycodecode',
                     'loginname',
                     'location',
                     'costcenter',
                     'useruri',
                     'company_code_current',
                     'employeeid' ]
            # can not parse the date in sql lite and hence the python logic
            # '''SELECT * FROM rawdata WHERE companycodecode='C1' AND timesheetstartdate >= {{result('get_timesheet_start_date')}}''',
        )

        c1_task = get_c1_task(config=config, header=header)

        query_list_compassusersdata = rail.CreateCollectionOperator(
            task_id='query_list_compassusersdata',
            source=lambda: list(filter(lambda item:
                                       item['companycodecode'] == 'COMPASS' and
                                       datetime.strptime(item['timesheetstartdate'], config.input_date_format) >= datetime.strptime(
                                           rail.result('get_timesheet_start_date'), config.output_date_format),
                                       rail.load_all_records(rail.result("merge_all_records")))),
            columns=['userlastname',
                     'userfirstname',
                     'entrydate',
                     'timesheetperiod',
                     'timesheetstartdate',
                     'attendancetypecode',
                     'ratetype',
                     'totalhrs',
                     'companycodecode',
                     'loginname',
                     'location',
                     'costcenter',
                     'useruri',
                     'company_code_current',
                     'employeeid' ]
        )

        has_compassuserdata = rail.IfOperator(
            task_id='has_compassuserdata',
            test=lambda: rail.result(
                'query_list_compassusersdata', 'length') > 0,
            yes_task="query_list_compass_uniqueusers",
            no_task= 'query_list_gsap_userdata'
        )

        query_list_compass_uniqueusers = rail.QueryCollectionOperator(
            task_id='query_list_compass_uniqueusers',
            query='''SELECT DISTINCT loginname FROM query_list_compassusersdata''',
        )

        getkeyvalue_compass_rates = rail.RepliconServiceCallForEachItemOperator(
            task_id='getkeyvalue_compass_rates',
            endpoint="/services/GenericKeyValueStoreService1.svc/GetKeyValue",
            items='{{ result("query_list_compass_uniqueusers") }}',
            flatten=True,
            data={
                "keyNamespace": "DXC_WorkOrderRateTypeRates",
                "key": "{{item.loginname}}"
            },
            all_result_data_handler=lambda data: list(
                map(lambda item:  {'key': item['key'], 'jsonValue': json.loads(item['jsonValue'])},
                    filter(lambda item: item, data))),
        )

        def map_location_list(resp):
            data = resp.json()['d']
            return list(
                map(lambda row:
                    {
                        "locationcode": row['cells'][2].get('textValue'),
                        "description": row['cells'][4].get('textValue'),
                        "locationname": row['cells'][0].get('textValue'),
                        "fullpath": '/'.join(list(map(lambda x: x.get('textValue', ''), row['cells'][1]['cellCollection']))),
                        "parent": '/'.join(list(map(lambda x: x['textValue'], row['cells'][1]['cellCollection']))).split("/")[-2]
                        if len('/'.join(list(map(lambda x: x['textValue'], row['cells'][1]['cellCollection']))).split("/")) > 1 else None,
                        "status": row['cells'][3].get('textValue'),
                    },
                    data['rows']))

        getdata_locationlist = rail.RepliconServiceOperator(
            task_id='getdata_locationlist',
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:location-list-column:location",
                    "urn:replicon:location-list-column:full-path",
                    "urn:replicon:location-list-column:code",
                    "urn:replicon:location-list-column:effectively-enabled",
                    "urn:replicon:location-list-column:effectively-enabled"
                ],
                "sort": [],
                "filterExpression": null
            },
            response_filter=map_location_list,
        )

        def get_region_from_mapper(row):
            filtered_location_mapper = list(filter(
                lambda x: x['Currently  included in  COMPAS s  H r  load?'] == 'Y' and x['Allowed'] == 'yes', location_mapper))
            region_by_parent_location = rail.find_first_by_attr_and_get_attr(
                filtered_location_mapper, 'name',
                rail.find_first_by_attr_and_get_attr(
                    rail.result('getdata_locationlist'), 'locationname', row['location'], 'parent'),
                'compassregion')
            return region_by_parent_location if region_by_parent_location else rail.find_first_by_attr_and_get_attr(
                filtered_location_mapper, 'name', row['location'], 'compassregion')

        create_csv_lines_composedatawithlocations = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_composedatawithlocations',
            source="{{ result('query_list_compassusersdata')}}",
            header=[
                'lastname',
                'firstname',
                'entrydate',
                'timesheetperiod',
                'timesheetstartdate',
                'attendencetypecode',
                'ratetype',
                'totalhrs',
                'companycodecode',
                'loginname',
                'location',
                'region'],
            row=lambda row: {
                "column_0": row['userlastname'],
                "column_1": row['userfirstname'],
                "column_2": row['entrydate'],
                "column_3": row['timesheetperiod'],
                "column_4": row['timesheetstartdate'],
                "column_5": row['attendancetypecode'],
                "column_6": row['ratetype'],
                "column_7": row['totalhrs'],
                "column_8": row['companycodecode'],
                "column_9": row['loginname'],
                "column_10": row['location'],
                "column_11": get_region_from_mapper(row)
            }.values(),
        )

        load_csv_datawithlocation = rail.LoadCSVFileOperator(
            task_id="load_csv_datawithlocation",
            document="{{ result('create_csv_lines_composedatawithlocations') }}",
        )

        create_collection_datawithlocation = rail.CreateCollectionOperator(
            task_id='create_collection_datawithlocation',
            source="{{ result('load_csv_datawithlocation') }}",
            name="compassdatatodivide"
        )

        emea_task = get_compass_task(
            config=config, task_type='emea', region='EMEA', header=header, output_filename='RepTS_Compass_EMEA')

        amer_task = get_compass_task(
            config=config, task_type='amer', region='AMER', header=header, output_filename='RepTS_Compass_AMS')

        apac_task = get_compass_task(
            config=config, task_type='apac', region='APAC', header=header, output_filename='RepTS_Compass_APJ')

        query_list_gsap_userdata = rail.CreateCollectionOperator(
            task_id='query_list_gsap_userdata',
            source=lambda: list(filter(lambda item: item['company_code_current'] in ['3001', '3124', '1602', '3118'] and
                                       datetime.strptime(item['timesheetstartdate'], config.input_date_format) >= datetime.strptime(
                                           rail.result('get_timesheet_start_date'), config.output_date_format),
                                       rail.load_all_records(rail.result("merge_all_records")))),
            columns=['userlastname',
                     'userfirstname',
                     'entrydate',
                     'timesheetperiod',
                     'timesheetstartdate',
                     'attendancetypecode',
                     'ratetype',
                     'totalhrs',
                     'companycodecode',
                     'loginname',
                     'location',
                     'costcenter',
                     'useruri',
                     'company_code_current',
                     'employeeid' ]
        )

        gsap_task = get_gsap_task(config=config, header=header)

        get_field_glass_report_details >> get_report_start_date >> get_report_end_date >> get_timesheet_start_date >> generate_field_glass_report_batch >> \
            execute_report_batch >> get_field_glass_report_batch_results >> has_empty_report_data
        has_empty_report_data >> rail.Label('Yes') >> get_timesheet_period_report_details
        has_empty_report_data >> rail.Label('No') >> has_invalid_report_field
        has_invalid_report_field >> rail.Label(
            'Yes') >> fail_for_invalid_report_field
        has_invalid_report_field >> rail.Label(
            'No') >> load_report_csv >> create_report_collection >> get_timesheet_period_report_details
        get_timesheet_period_report_details >> run_timesheet_period_report_group
        run_timesheet_period_report_group >> is_timesheet_period_report_failed
        is_timesheet_period_report_failed >> rail.Label('Yes') >> fail_timesheet_period_report_generation
        is_timesheet_period_report_failed >> rail.Label('No') >> timesheet_period_report_has_data
        timesheet_period_report_has_data >> rail.Label('Yes') >> is_timesheet_period_report_has_expected_columns
        timesheet_period_report_has_data >> rail.Label('No') >> is_data_exists
        is_timesheet_period_report_has_expected_columns >> rail.Label('No') >> fail_timesheet_period_no_expected_columns
        is_timesheet_period_report_has_expected_columns >> rail.Label('Yes') >> load_timesheet_period_report_csv >> create_timesheet_period_report_collection \
            >> query_zero_hours_timesheets >> if_zero_hours_timesheets_exists
        if_zero_hours_timesheets_exists >> rail.Label("No") >> is_data_exists
        if_zero_hours_timesheets_exists >> rail.Label("Yes") >> generate_zero_hours_timesheets >> is_data_exists
        is_data_exists >> rail.Label('Yes') >> query_to_merge_data >> merge_all_records \
            >> query_list_c1userdata >> c1_task >> query_list_compassusersdata >> has_compassuserdata
        is_data_exists >> rail.Label('No') >> send_mail_nodata
        has_compassuserdata >> rail.Label(
            'Yes') >> query_list_compass_uniqueusers >> getkeyvalue_compass_rates >> getdata_locationlist >> create_csv_lines_composedatawithlocations >> \
            load_csv_datawithlocation >> create_collection_datawithlocation >> emea_task >> amer_task >> apac_task >> query_list_gsap_userdata >> gsap_task
        has_compassuserdata >> rail.Label(
            'No') >> query_list_gsap_userdata
        return dag


rail.for_each_instance(create_dag)
