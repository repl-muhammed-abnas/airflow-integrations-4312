from datetime import datetime
import json
import rail
from dxctechnology.cwf_time_export_v3.mapper.location import location_mapper
from dxctechnology.cwf_time_export_v3.task.compass_task import get_compass_task

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_cwf_time_export_compass_child_{config.instance}_v3',
        description=f'DXCTechnology_CWF Time export - Compass V3 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.compass_sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_last_time_export_details = rail.RepliconServiceOperator(
            task_id='get_last_time_export_details',
            endpoint='/services/TimeDataExportService1.svc/GetTimeDataExportDetails',
            data={
                "target": {
                    "uri": '{{ dag_run.conf.last_twb_uri }}',
                }
            }
        )

        get_current_time_export_details = rail.RepliconServiceOperator(
            task_id='get_current_time_export_details',
            endpoint='/services/TimeDataExportService1.svc/GetTimeDataExportDetails',
            data={
                "target": {
                    "uri": '{{ dag_run.conf.timeexporturi }}',
                }
            }
        )

        create_time_data_download_batch_compass = rail.RepliconServiceOperator(
            task_id='create_time_data_download_batch_compass',
            endpoint="/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch",
            data={
                "columnUris": [],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:time-data-export-filter:time-data-export"
                    },
                    "operatorUri": "urn:replicon:filter-operator:in",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": ["{{ dag_run.conf.timeexporturi }}"],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                },
                "fileFormatScriptUri": "{{ dag_run.conf.fileformaturi }}"
            }
        )

        batch_management_async_compass = rail.batch_execution(
            group_id='execute_batch_management_async_compass',
            creation_task_id=create_time_data_download_batch_compass.task_id,
        )

        get_time_data_download_batch_results = rail.RepliconServiceOperator(
            task_id='get_time_data_download_batch_results',
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults",
            data={
                "timeDataDownloadBatchUri": "{{ result('create_time_data_download_batch_compass') }}"
            }
        )

        download_timedata_file = rail.HTTPDownloadFileOperator(
            task_id='download_timedata_file',
            url="{{ result('get_time_data_download_batch_results').downloadUrl }}",
        )

        load_csv_create_list_from_csv_finaltimedata = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_finaltimedata",
            document="{{ result('download_timedata_file') }}",
        )

        create_collection_create_list_from_csv_finaltimedata = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_finaltimedata',
            source="{{ result('load_csv_create_list_from_csv_finaltimedata') }}",
            name="finaltimedata",
            columns={
                'Company Code Code': 'companycodecode',
                'Employee ID': 'employeeid',
                'PERNER': 'perner',
                'Approval Status': 'approvalstatus',
                'Entry Date': 'entrydate',
                'WBS / SO Name': 'projectname',
                'Cost Center Name': 'costcentercode',
                'Labor Type Name': 'labortype',
                'Job Activity Type': 'jobactivitytype',
                'Task Name': 'taskname',
                'Time Type': 'timetype',
                'Attendance Type Code': 'attendancetypecode',
                'Billable Indicator': 'billableindicator',
                'Hours (Current)': 'hours',
                'Rate Type': 'ratetype',
                'Short Time Entry ID': 'timeentryid',
                'Time Off Booking ID': 'timeoffbookingid',
                'Comments': 'comments',
                'WBS Type': 'wbstype',
                'Task Task Type': 'tasktype',
                'New Remaining Work': 'newremainningwork',
                'Customer 1': 'customer1',
                'Customer 2': 'customer2',
                'Customer 3': 'customer3',
                'GSAP Billable Flag': 'gsapbillableflag',
                'Time Off Type Description': 'timeofftypedescription',
                'Master WBS (SO, WO)': 'masterwbs',
                'Project Type': 'projecttype',
                'IWO Indicator': 'iwoindicator',
                'Parent WBS': 'parentwbs',
                'Company Code Name': 'companycodename',
                'Task Name (Full Path)': 'taskfullpath',
                'Time Entry ID': 'timentryid2',
                'Employee Type Name': 'employeetypename',
                'Timesheet Period': 'timesheetperiod',
                'Location Name': 'locationname',
                'Login Name': 'loginname',
                'User': 'user',
                'IWO WBS Element': 'iwowbselement',
                'Work Order ID': 'workorderid',
                'Parent Service Order': 'parentserviceorder',
                'CWF C1 alternate ID': 'c1cwfalternateid',
                'Parent Project': 'parentproject',
                'Attribute 1 (Code)': 'attributecode1',
                'Attribute 2 (Code)': 'attributecode2',
            }
        )

        query_list_filtered_data_estype = rail.QueryCollectionOperator(
            task_id='query_list_filtered_data_estype',
            query='''SELECT *
                    FROM
                        finaltimedata
                    WHERE
                        employeetypename LIKE '%Contractor%' AND companycodecode='COMPASS' OR
                         (employeetypename LIKE '%Contractor%' AND projecttype='ES' AND projectname LIKE 'E-%') OR
                         (employeetypename LIKE '%Contractor%' AND projecttype='CP')
                    ORDER BY CAST(hours as DECIMAL) ASC
                    ''',
        )

        get_data_division_list_service = rail.RepliconServiceOperator(
            task_id='get_data_division_list_service',
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:division-list-column:division",
                    "urn:replicon:division-list-column:full-path",
                    "urn:replicon:division-list-column:code",
                    "urn:replicon:division-list-column:effectively-enabled",
                    "urn:replicon:division-list-column:description"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:division-list-filter:text"
                        },
                        "operatorUri": "urn:replicon:filter-operator:text-search",
                        "rightExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": {
                                "uri": null,
                                "uris": [],
                                "bool": null,
                                "date": null,
                                "money": null,
                                "number": null,
                                "text": "COMPASS",
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "dateTimeUtc": null,
                                "dateTimeUtcRange": null
                            },
                            "filterDefinitionUri": null
                        },
                        "value": null,
                        "filterDefinitionUri": null
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:division-list-filter:effectively-enabled"
                        },
                        "operatorUri": "urn:replicon:filter-operator:equal",
                        "rightExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": {
                                "uri": null,
                                "uris": [],
                                "bool": "true",
                                "date": null,
                                "money": null,
                                "number": null,
                                "text": null,
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "dateTimeUtc": null,
                                "dateTimeUtcRange": null
                            },
                            "filterDefinitionUri": null
                        },
                        "value": null,
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
        )

        map_division_list = rail.PythonOperator(
            task_id='map_division_list',
            python_callable=lambda: list(map(lambda row:  {
                    "companycode": row['cells'][2].get('textValue'),
                    "description": row['cells'][4].get('textValue'),
                    "companycodename": row['cells'][0].get('textValue'),
                    "fullpath": "/".join(list(map(lambda x: x.get('textValue'), row['cells'][1]['cellCollection']))),
                    "parent": '/'.join(list(map(lambda x: x['textValue'], row['cells'][1]['cellCollection']))).split("/")[-2]
                if len('/'.join(list(map(lambda x: x['textValue'], row['cells'][1]['cellCollection']))).split("/")) > 1 else None,
                    "startus": row['cells'][3].get('textValue'),
            }, rail.result('get_data_division_list_service')['rows']))
        )

        get_data_location_list_service = rail.RepliconServiceOperator(
            task_id='get_data_location_list_service',
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
            }
        )

        map_location_list_data = rail.PythonOperator(
            task_id='map_location_list_data',
            python_callable=lambda: list(map(lambda row:  {
                    "locationcode": row['cells'][2].get('textValue'),
                    "description": row['cells'][4].get('textValue'),
                    "locationname": row['cells'][0].get('textValue'),
                "fullpath": "/".join(list(map(lambda x: x.get('textValue'), row['cells'][1]['cellCollection']))),
                    "parent": '/'.join(list(map(lambda x: x['textValue'], row['cells'][1]['cellCollection']))).split("/")[-2]
                if len('/'.join(list(map(lambda x: x['textValue'], row['cells'][1]['cellCollection']))).split("/")) > 1 else None,
                    "startus": row['cells'][3].get('textValue'),

            }, rail.result('get_data_location_list_service')['rows']))

        )

        search_entries_location_map_compass = rail.PythonOperator(
            task_id='search_entries_location_map_compass',
            python_callable=lambda: list(filter(
                lambda x: x["Currently  included in  COMPAS s  H r  load?"] == "Y"
                and x['Allowed'] == 'yes', location_mapper))
        )

        def get_region_from_mapper(item):
            if item['projecttype'] == "ES" and item['projectname'].startswith("E-"):
                return "AMER"
            region_by_parent_location = rail.find_first_by_attr_and_get_attr(
                rail.result('search_entries_location_map_compass'), 'name',
                rail.find_first_by_attr_and_get_attr(
                    rail.result('map_location_list_data'), 'locationname', item['locationname'], 'parent'),
                'compassregion')
            return region_by_parent_location if region_by_parent_location else rail.find_first_by_attr_and_get_attr(
                rail.result('search_entries_location_map_compass'), 'name', item['locationname'], 'compassregion')

        create_csv_lines_estype = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_estype',
            source="{{ result('query_list_filtered_data_estype') }}",
            header=[
                'companycodecode',
                'employeeid',
                'perner',
                'approvalstatus',
                'entrydate',
                'projectname',
                'costcentercode',
                'labortype',
                'jobactivitytype',
                'taskname',
                'timetype',
                'attendancetypecode',
                'billableindicator',
                'hours',
                'ratetype',
                'timeentryid',
                'timeoffbookingid',
                'comments',
                'wbstype',
                'tasktype',
                'newremainningwork',
                'customer1',
                'customer2',
                'customer3',
                'gsapbillableflag',
                'timeofftypedescription',
                'masterwbs',
                'projecttype',
                'iwoindicator',
                'parentwbs',
                'companycodename',
                'companycodedesc',
                'taskfullpath',
                'length',
                'timentryid2',
                'employeetype',
                'timesheetperiod',
                'locationname',
                'parentlocation',
                'loginname',
                'username',
                'iwowbselement',
                'workorderid',
                'parentserviceorder',
                'c1cwfalternateid',
                'region',
                'attributecode1',
                'attributecode2'],
            row=lambda item: {
                "column_0": item['companycodecode'],
                "column_1": item['employeeid'],
                "column_2": item['perner'],
                "column_3": item['approvalstatus'],
                "column_4": item['entrydate'],
                "column_5": item['projectname'],
                "column_6": item['costcentercode'],
                "column_7": item['labortype'],
                "column_8": item['jobactivitytype'],
                "column_9": item['taskname'],
                "column_10": item['timetype'],
                "column_11": item['attendancetypecode'],
                "column_12": item['billableindicator'],
                "column_13": item['hours'],
                "column_14": item['ratetype'],
                "column_15": item['timeentryid'],
                "column_16": datetime.strptime(item['entrydate'], config.entry_date_format).strftime("%Y%m%d") + item['timeoffbookingid']
                if item['timeoffbookingid'] else '',
                "column_17": item['comments'],
                "column_18": item['wbstype'],
                "column_19": item['tasktype'],
                "column_20": item['newremainningwork'],
                "column_21": item['customer1'],
                "column_22": item['customer2'],
                "column_23": item['customer3'],
                "column_24": item['gsapbillableflag'],
                "column_25": item['timeofftypedescription'],
                "column_26": item['masterwbs'],
                "column_27": item['projecttype'],
                "column_28": item['iwoindicator'],
                "column_29": item['parentwbs'],
                "column_30": item['companycodename'],
                "column_31": rail.find_first_by_attr_and_get_attr(rail.result('map_division_list'), 'companycodename', item['companycodename'], 'parent'),
                "column_32": item['taskfullpath'],
                "column_33": len(item['taskfullpath'].split(" / ")),
                "column_34": item['timentryid2'],
                "column_35": item['employeetypename'],
                "column_36": item['timesheetperiod'],
                "column_37": item['locationname'],
                "column_38": rail.find_first_by_attr_and_get_attr(rail.result('map_location_list_data'), 'locationname', item['locationname'], 'parent'),
                "column_39": item['loginname'],
                "column_40": item['user'],
                "column_41": item['iwowbselement'],
                "column_42": item['workorderid'],
                "column_43": item['parentserviceorder'],
                "column_44": item['c1cwfalternateid'],
                "column_45": get_region_from_mapper(item),
                "column_46": item['attributecode1'],
                "column_47": item['attributecode2'],
            }.values()
        )

        load_csv_create_list_from_estype = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_estype",
            document="{{ result('create_csv_lines_estype') }}",
        )

        create_collection_create_list_from_csv_estype = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_estype',
            source="{{ result('load_csv_create_list_from_estype') }}",
            name="datatodivide"
        )

        query_list_distinct_login_estype = rail.QueryCollectionOperator(
            task_id='query_list_distinct_login_estype',
            query='''SELECT DISTINCT loginname FROM datatodivide''',
        )

        get_key_value_workorder_rate = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_key_value_workorder_rate',
            endpoint="/services/GenericKeyValueStoreService1.svc/GetKeyValue",
            items="{{ result('query_list_distinct_login_estype') }}",
            flatten=True,
            data={
                "keyNamespace": "DXC_WorkOrderRateTypeRates",
                "key": "{{ item.loginname }}"
            },
            all_result_data_handler=lambda data: list(
                map(lambda item:  {'key': item['key'], 'jsonValue': json.loads(item['jsonValue'])},
                    filter(lambda item: item, data))),
        )

        emea_task = get_compass_task(
            config=config, region='EMEA', task_type='emea', output_filename='P01_ReplicontoCOMPASS', compass_oef_name="Compass_P01/NT2_Payload_Processed",
            internal_oef_name='COMPASS_P01_sent', unique_id= 'payload_identifier_replicon_uniqueid_p01')

        amer_task = get_compass_task(
            config=config, region='AMER', task_type='amer', output_filename='PN1_ReplicontoCOMPASS', compass_oef_name="Compass_PN1/NT1_Payload_Processed",
            internal_oef_name='COMPASS_PN1_sent', unique_id= 'payload_identifier_replicon_uniqueid_pn1')

        apac_task = get_compass_task(
            config=config, region='APAC', task_type='apac', output_filename='PJ1_ReplicontoCOMPASS', compass_oef_name="Compass_PJ1/NT3_Payload_Processed",
            internal_oef_name='COMPASS_PJ1_sent', unique_id= 'payload_identifier_replicon_uniqueid_pj1')

        get_last_time_export_details >> get_current_time_export_details >> create_time_data_download_batch_compass
        create_time_data_download_batch_compass >> batch_management_async_compass >> get_time_data_download_batch_results >> download_timedata_file >> \
            load_csv_create_list_from_csv_finaltimedata >> create_collection_create_list_from_csv_finaltimedata >> \
            query_list_filtered_data_estype >> get_data_division_list_service >> map_division_list >> get_data_location_list_service >> \
            map_location_list_data >> search_entries_location_map_compass >> create_csv_lines_estype >> \
            load_csv_create_list_from_estype >> create_collection_create_list_from_csv_estype >> query_list_distinct_login_estype >> \
            get_key_value_workorder_rate >> emea_task >> amer_task >> apac_task

    return dag


rail.for_each_instance(create_dag)
