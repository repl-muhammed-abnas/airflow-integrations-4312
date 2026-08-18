from datetime import timedelta
from dxctechnology.time_export_v1.compass_outbound.utils import request_payload, response_filters, custom_methods
from dxctechnology.time_export_v1.compass_outbound.tasks.iwo_time_data_for_divisions import get_iwo_time_data_for_divisions
from dxctechnology.time_export_v1.compass_outbound.tasks.upload_time_data import get_upload_time_data
from airflow.models import Variable
import rail

null = None
def create_child_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.compass_iwo_create_time_export_child_dagid,
        description=f"DXC - Compass IWO Time Export Create Compass IWO Time export child - {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_child_dag_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='data_existence_var'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='data_existence_var',
            end_task='batch_end',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        data_existence_var = rail.SetVariableOperator(
            task_id='data_existence_var',
            name='data_existence',
            value=[]
        )

        start_download_exports = rail.EmptyOperator(
            task_id='start_download_exports'
        )

        get_last_time_export_details = rail.RepliconServiceOperator(
            task_id='get_last_time_export_details',
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataExportDetails",
            data={
                "target": {
                    "uri": "{{ dag_run.conf.lasttwburi }}",
                    "name": null
                }
            }
        )

        get_current_time_export_details = rail.RepliconServiceOperator(
            task_id='get_current_time_export_details',
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataExportDetails",
            data={
                "target": {
                    "uri": "{{ dag_run.conf.timeexporturi }}",
                    "name": null
                }
            }
        )

        create_download_batch = rail.RepliconServiceOperator(
            task_id='create_download_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch',
            data=lambda dag_run: request_payload.get_create_download_batch(dag_run.conf["timeexporturi"], dag_run.conf["fileformaturi"])
        )

        execute_download_batch, wait_for_download_batch = rail.batch_execution(
            group_id='execute_download_batch',
            creation_task_id=create_download_batch.task_id
        )

        get_download_url = rail.RepliconServiceOperator(
            task_id='get_download_url',
            endpoint='/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults',
            data={
                "timeDataDownloadBatchUri": "{{ result('create_download_batch') }}"
            },
            data_handler=lambda response: response['downloadUrl']
        )

        download_export = rail.HTTPDownloadFileOperator(
            task_id='download_export',
            url="{{ result('get_download_url') }}",
        )

        load_export = rail.LoadCSVFileOperator(
            task_id='load_export',
            document="{{ result('download_export') }}"
        )

        create_final_time_data_collection = rail.CreateCollectionOperator(
            task_id='create_final_time_data_collection',
            source='{{ result("load_export") }}',
            columns={
                "Company Code Code": "companycodecode",
                "Employee ID": "employeeid",
                "PERNER": "perner",
                "Approval Status": "approvalstatus",
                "Entry Date": "entrydate",
                "WBS / SO Name": "projectname",
                "Cost Center Name": "costcentercode",
                "Labor Type Name": "labortype",
                "Job Activity Type": "jobactivitytype",
                "Task Name": "taskname",
                "Time Type US": "timetype",
                "Attendance Type Code": "attendancetypecode",
                "Billable Indicator": "billableindicator",
                "Hours (Current)": "hours",
                "Rate Type": "ratetype",
                "Short Time Entry ID": "timeentryid",
                "Time Off Booking ID": "timeoffbookingid",
                "Comments": "comments",
                "WBS Type": "wbstype",
                "Task Task Type": "tasktype",
                "New Remaining Work": "newremainningwork",
                "Customer 1": "customer1",
                "Customer 2": "customer2",
                "Customer 3": "customer3",
                "GSAP Billable Flag": "gsapbillableflag",
                "Time Off Type Description": "timeofftypedescription",
                "Master WBS (SO, WO)": "masterwbs",
                "Project Type": "projecttype",
                "IWO Indicator": "iwoindicator",
                "Parent WBS": "parentwbs",
                "Company Code Name": "companycodename",
                "Task Name (Full Path)": "taskfullpath",
                "Time Entry ID": "timeentryid2",
                "Parent Service Order": "parentserviceorder",
                "International Assignee": "internationalassignee",
                "IA PERNER ID": "iapernerid",
                "IWO WBS Element": "iwowbselement",
                "Beeper Pay": "beeperpay",
                "Parent Project": "parentproject",
                "Oncall/Standby": "oncallstandby",
                "Time Type US 2": "timetype2",
                "Name": "breaktypename",
                "Time Off Type Name": "timeofftypename",
                "Oncall / Standby": "oncallstandby2",
                "Attribute 1 (Code)": "attributecode1",
                "Attribute 2 (Code)": "attributecode2",
                "Actual Employee ID": "actualempid",
                "GSAP Reference Number": "gsapreferencenumber",
                "Personnel Area Code": "personnelareacode",
                "Time Type (AUS) (Code)": "timetypeauscode",
                "Stand by (AUS)": "standbyauscode",
                "Parent WBS Code": "parentwbscode",
                "PSA Flag": "psaflag",
                "GSAP Task": "gsaptask",
                "GSAP Task (Code)": "gsaptaskcode",
                "Employee Type Name": "employeetype",
                "Employee Type Code": "employeetypecode",
                "Organizational Unit Name": "organizationalunitname",
                "Time Type BFI": "timetypebfi",
                "Supplemental Pay": "supplementalpay",
                "PROF Supplemental Pay": "profsupplementalpay",
                "Time Type UK - Callout": "time_type_uk_callout",
                "Time Type UK - CallOut|Standby|OT": "time_type_uk_callout_standby_ot",
                "Time Type UK - EON": "time_type_uk_eon",
                "Time Type UK - Olympus": "time_type_uk_olympus",
                "Time Type UK - AT&T": "time_type_uk_att",
                "Time Type UK - Paybands": "time_type_uk_paybands",
                "Time Type IRL - CO|SD|OT": "time_type_irl_co_sd_ot",
                "Time Type IRL - CO & SB": "time_type_irl_co_sb",
                "Time Type 1 - UK FDS": "time_type_1_uk_fds",
                "Time Type 2 - UK FDS": "time_type_2_uk_fds",
                "Time Type 3 - UK FDS": "time_type_3_uk_fds",
                "Time Type 6- UK FDS": "time_type_6_uk_fds",
                "Time Type 8- UK FDS": "time_type_8_uk_fds",
                "Time Type 9- UK FDS": "time_type_9_uk_fds",
                "Time Type 11 - UK FDS": "time_type_11_uk_fds",
                "Time Type 13 - UK FDS": "time_type_13_uk_fds",
                "Time Type 18 - UK FDS": "time_type_18_uk_fds",
                "Time Type 19 - UK FDS": "time_type_19_uk_fds",
                "Time Type 1 - IRL FDS": "time_type_1_irl_fds",
                "Time Type 2 - IRL FDS": "time_type_2_irl_fds",
                "Time Type 3 - IRL FDS": "time_type_3_irl_fds",
                "Time Type - UK FCA": "time_type_uk_fca",
                "Location Name (Full Path)": "locationfullpath"
            },
            # Keep the above Time Type columns as per the mapper - ( time_type_oef_name: time_type_oef_attr ) as per the mapper
            name='finaltimedata'
        )

        create_download_batch_with_hours = rail.RepliconServiceOperator(
            task_id='create_download_batch_with_hours',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch',
            data=lambda dag_run: request_payload.get_create_download_batch(dag_run.conf["timeexporturi"], dag_run.conf["hoursfileformaturi"])
        )

        execute_download_batch_with_hours, wait_for_download_batch_with_hours = rail.batch_execution(
            group_id='execute_download_batch_with_hours',
            creation_task_id=create_download_batch_with_hours.task_id
        )

        get_download_url_with_hours = rail.RepliconServiceOperator(
            task_id='get_download_url_with_hours',
            endpoint='/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults',
            data={
                "timeDataDownloadBatchUri": "{{ result('create_download_batch_with_hours') }}"
            },
            data_handler=lambda response: response['downloadUrl']
        )

        download_export_with_hours = rail.HTTPDownloadFileOperator(
            task_id='download_export_with_hours',
            url="{{ result('get_download_url_with_hours') }}",
        )

        load_export_with_hours = rail.LoadCSVFileOperator(
            task_id='load_export_with_hours',
            document="{{ result('download_export_with_hours') }}"
        )

        create_final_time_data_collection_with_hours = rail.CreateCollectionOperator(
            task_id='create_final_time_data_collection_with_hours',
            source='{{ result("load_export_with_hours") }}',
            columns={
                "Company Code Code": "companycodecode",
                "Employee ID": "employeeid",
                "PERNER": "perner",
                "Approval Status": "approvalstatus",
                "Entry Date": "entrydate",
                "WBS / SO Name": "projectname",
                "Cost Center Name": "costcentercode",
                "Labor Type Name": "labortype",
                "Job Activity Type": "jobactivitytype",
                "Task Name": "taskname",
                "Time Type US": "timetype",
                "Attendance Type Code": "attendancetypecode",
                "Billable Indicator": "billableindicator",
                "Hours": "hours",
                "Rate Type": "ratetype",
                "Short Time Entry ID": "timeentryid",
                "Time Off Booking ID": "timeoffbookingid",
                "Comments": "comments",
                "WBS Type": "wbstype",
                "Task Task Type": "tasktype",
                "New Remaining Work": "newremainningwork",
                "Customer 1": "customer1",
                "Customer 2": "customer2",
                "Customer 3": "customer3",
                "GSAP Billable Flag": "gsapbillableflag",
                "Time Off Type Description": "timeofftypedescription",
                "Master WBS (SO, WO)": "masterwbs",
                "Project Type": "projecttype",
                "IWO Indicator": "iwoindicator",
                "Parent WBS": "parentwbs",
                "Company Code Name": "companycodename",
                "Task Name (Full Path)": "taskfullpath",
                "Time Entry ID": "timeentryid2",
                "Parent Service Order": "parentserviceorder",
                "International Assignee": "internationalassignee",
                "IA PERNER ID": "iapernerid",
                "IWO WBS Element": "iwowbselement",
                "Beeper Pay": "beeperpay",
                "Parent Project": "parentproject",
                "Oncall/Standby": "oncallstandby",
                "Time Type US 2": "timetype2",
                "Name": "breaktypename",
                "Time Off Type Name": "timeofftypename",
                "Oncall / Standby": "oncallstandby2",
                "Attribute 1 (Code)": "attributecode1",
                "Attribute 2 (Code)": "attributecode2",
                "Actual Employee ID": "actualempid",
                "GSAP Reference Number": "gsapreferencenumber",
                "Personnel Area Code": "personnelareacode",
                "Time Type (AUS) (Code)": "timetypeauscode",
                "Stand by (AUS)": "standbyauscode",
                "Parent WBS Code": "parentwbscode",
                "PSA Flag": "psaflag",
                "GSAP Task": "gsaptask",
                "GSAP Task (Code)": "gsaptaskcode",
                "Employee Type Name": "employeetype",
                "Employee Type Code": "employeetypecode",
                "Organizational Unit Name": "organizationalunitname",
                "Time Type BFI": "timetypebfi",
                "Supplemental Pay": "supplementalpay",
                "PROF Supplemental Pay": "profsupplementalpay",
                "Time Type UK - Callout": "time_type_uk_callout",
                "Time Type UK - CallOut|Standby|OT": "time_type_uk_callout_standby_ot",
                "Time Type UK - EON": "time_type_uk_eon",
                "Time Type UK - Olympus": "time_type_uk_olympus",
                "Time Type UK - AT&T": "time_type_uk_att",
                "Time Type UK - Paybands": "time_type_uk_paybands",
                "Time Type IRL - CO|SD|OT": "time_type_irl_co_sd_ot",
                "Time Type IRL - CO & SB": "time_type_irl_co_sb",
                "Time Type 1 - UK FDS": "time_type_1_uk_fds",
                "Time Type 2 - UK FDS": "time_type_2_uk_fds",
                "Time Type 3 - UK FDS": "time_type_3_uk_fds",
                "Time Type 6- UK FDS": "time_type_6_uk_fds",
                "Time Type 8- UK FDS": "time_type_8_uk_fds",
                "Time Type 9- UK FDS": "time_type_9_uk_fds",
                "Time Type 11 - UK FDS": "time_type_11_uk_fds",
                "Time Type 13 - UK FDS": "time_type_13_uk_fds",
                "Time Type 18 - UK FDS": "time_type_18_uk_fds",
                "Time Type 19 - UK FDS": "time_type_19_uk_fds",
                "Time Type 1 - IRL FDS": "time_type_1_irl_fds",
                "Time Type 2 - IRL FDS": "time_type_2_irl_fds",
                "Time Type 3 - IRL FDS": "time_type_3_irl_fds",
                "Time Type - UK FCA": "time_type_uk_fca",
                "Location Name (Full Path)": "locationfullpath"
            },
            # Keep the above Time Type columns as per the mapper - ( time_type_oef_name: time_type_oef_attr ) as per the mapper
            name='finaltimedata_with_hours'
        )

        if_collections_have_data = rail.IfOperator(
            task_id='if_collections_have_data',
            test='{{ result("create_final_time_data_collection", "length") > 0 or result("create_final_time_data_collection_with_hours", "length") > 0 }}',
            yes_task='get_effectively_enabled_compass_divisions',
            no_task='empty_time_data'
        )

        empty_time_data = rail.EmptyOperator(
            task_id='empty_time_data'
        )

        get_effectively_enabled_compass_divisions = rail.RepliconServiceOperator(
            task_id="get_effectively_enabled_compass_divisions",
            endpoint="/services/DivisionListService1.svc/GetData",
            data=request_payload.get_effectively_enabled_compass_divisions_payload,
            data_handler=response_filters.filter_effectively_enabled_compass_division_data
        )

        get_all_divisions_with_description = rail.RepliconServiceOperator(
            task_id="get_all_divisions_with_description",
            endpoint="/services/DivisionListService1.svc/GetData",
            data=request_payload.get_all_divisions_with_description_payload,
            data_handler=response_filters.filter_divisions_with_description
        )

        get_timeoff_types_to_exclude_in_export = rail.PythonOperator(
            task_id='get_timeoff_types_to_exclude_in_export',
            python_callable=lambda: '("' + "\",\"".join(list(map(lambda timeoff_type_data: timeoff_type_data["timeoff_type_name"],
                Variable.get(config.timeoff_types_to_exclude, deserialize_json=True)))) + '")'
        )

        get_time_types_oefs_to_exclude_in_export = rail.PythonOperator(
            task_id='get_time_types_oefs_to_exclude_in_export',
            python_callable=custom_methods.get_timetype_oef_query_to_exclude,
            op_args=[config.timetype_standby_units_to_exclude]
        )

        query_filter_time_export_data = rail.QueryCollectionOperator(
            task_id='query_filter_time_export_data',
            query="""SELECT * FROM finaltimedata
                WHERE (
                (
                    companycodecode = 'COMPASS'
                    AND attendancetypecode NOT IN ('499', '999', '779')
                    AND breaktypename NOT IN ('Meal', 'Rest')
                    AND timeofftypename NOT IN {{ result('get_timeoff_types_to_exclude_in_export') }}
                )
                OR (
                    projecttype = 'ES' AND projectname LIKE 'E-%'
                    AND attendancetypecode NOT IN ('499', '999', '779')
                    AND breaktypename NOT IN ('Meal', 'Rest')
                    AND (beeperpay IS NULL OR beeperpay = '')
                    AND (oncallstandby2 IS NULL OR oncallstandby2 = '')
                )
                OR (
                    companycodecode = 'GSAP' AND projecttype = 'CP'
                    AND timetypebfi <> 'Stand by'
                    AND supplementalpay <> 'Stand by'
                    AND standbyauscode <> "Stand by"
                    AND profsupplementalpay <> "Stand by"
                ))
                AND {{ result('get_time_types_oefs_to_exclude_in_export') }} ORDER BY CAST(hours AS FLOAT) ASC""",
            name='filtered_time_export_data'
        )

        query_ineligible_and_reversals = rail.QueryCollectionOperator(
            task_id='query_ineligible_and_reversals',
            query="""SELECT * FROM finaltimedata ftd WHERE ftd.timeentryid
                NOT IN (SELECT DISTINCT filtd.timeentryid FROM filtered_time_export_data filtd)
                AND CAST(ftd.hours AS FLOAT) = 0""",
            name='ineligible_and_reversals'
        )

        query_iwo_reversals = rail.QueryCollectionOperator(
            task_id='query_iwo_reversals',
            query="""SELECT * FROM finaltimedata_with_hours ftdh 
                WHERE ftdh.timeentryid NOT IN (
                    SELECT DISTINCT ftd.timeentryid FROM finaltimedata ftd
                )
                AND CAST(ftdh.hours AS FLOAT) < 0""",
            name='iwo_reversals'
        )

        final_export_data = rail.QueryCollectionOperator(
            task_id='final_export_data',
            query="SELECT * FROM filtered_time_export_data UNION ALL SELECT * FROM ineligible_and_reversals UNION ALL SELECT * FROM iwo_reversals",
            name='final_export_data'
        )

        if_filtered_timedata_has_data = rail.IfOperator(
            task_id='if_filtered_timedata_has_data',
            test='{{ result("final_export_data", "length") > 0 }}',
            yes_task='initial_data_for_processing',
            no_task='empty_filtered_data'
        )

        empty_filtered_data = rail.EmptyOperator(
            task_id='empty_filtered_data'
        )

        log_no_data_var = rail.SetVariableOperator(
            task_id='log_no_data_var',
            name='data_existence',
            value=[
                {
                    "name": "P01",
                    "type": "No Data",
                    "count": "0",
                },
                {
                    "name": "PN1",
                    "type": "No Data",
                    "count": "0",
                },
                {
                    "name": "PJ1",
                    "type": "No Data",
                    "count": "0",
                }
            ]
        )

        initial_data_for_processing = rail.CreateCollectionOperator(
            task_id='initial_data_for_processing',
            source=custom_methods.get_initial_data_for_processing,
            columns=["companycodecode","employeeid","perner","approvalstatus","entrydate","projectname","costcentercode",
                    "labortype","jobactivitytype","taskname","timetype","attendancetypecode","billableindicator",
                    "hours","ratetype","timeentryid","timeoffbookingid","comments","wbstype","tasktype",
                    "newremainningwork","customer1","customer2","customer3","gsapbillableflag","timeofftypedescription","timeofftypename",
                    "masterwbs","projecttype","iwoindicator","parentwbs","companycodename","companycodedesc","companycodedesc2",
                    "taskfullpath","length","timeentryid2","parentserviceorder","internationalassignee","iapernerid",
                    "iwowbselement","attributecode1","attributecode2"],
            name='initial_data_for_processing'
        )

        query_p01_nt2_data = rail.QueryCollectionOperator(
            task_id='query_p01_nt2_data',
            query="SELECT * from initial_data_for_processing WHERE companycodedesc = 'P01' OR companycodedesc2 = 'P01' ORDER BY CAST(hours AS FLOAT) ASC",
            name='p01_nt2_data'
        )

        query_pn1_nt1_data = rail.QueryCollectionOperator(
            task_id='query_pn1_nt1_data',
            query="SELECT * from initial_data_for_processing WHERE companycodedesc = 'PN1' OR companycodedesc2 = 'PN1' ORDER BY CAST(hours AS FLOAT) ASC",
            name='pn1_nt1_data'
        )

        query_pj1_nt3_data = rail.QueryCollectionOperator(
            task_id='query_pj1_nt3_data',
            query="SELECT * from initial_data_for_processing WHERE companycodedesc = 'PJ1' OR companycodedesc2 = 'PJ1' ORDER BY CAST(hours AS FLOAT) ASC",
            name='pj1_nt3_data'
        )

        create_p01_nt2_data = get_iwo_time_data_for_divisions(code='P01', task_type='p01_nt2')

        create_pn1_nt1_data = get_iwo_time_data_for_divisions(code='PN1', task_type='pn1_nt1')

        create_pj1_nt3_data = get_iwo_time_data_for_divisions(code='PJ1', task_type='pj1_nt3')

        get_data_existence_var = rail.GetVariableOperator(
            task_id='get_data_existence_var',
            name='data_existence'
        )

        p01_nt2_upload_data_task = get_upload_time_data(
            config=config, region='EMEA', code_1='P01', code_2='NT2', task_type='p01_nt2', compass_oef_name_attr="oefname_P01",
            internal_oef_name='COMPASS_P01_sent', internal_oef_uri_attr="P01_sent_oef", unique_id_attr= 'payload_identifier_replicon_uniqueid_P01',
            last_unique_id_attr='lasttwbuniqueindentifier_P01', division_final_data='p01_nt2_final_data', export_type='iwo')

        pn1_nt1_upload_data_task = get_upload_time_data(
            config=config, region='AMER', code_1='PN1', code_2='NT1', task_type='pn1_nt1', compass_oef_name_attr="oefname_PN1",
            internal_oef_name='COMPASS_PN1_sent', internal_oef_uri_attr="PN1_sent_oef", unique_id_attr= 'payload_identifier_replicon_uniqueid_PN1',
            last_unique_id_attr='lasttwbuniqueindentifier_PN1', division_final_data='pn1_nt1_final_data', export_type='iwo')

        pj1_nt3_upload_data_task = get_upload_time_data(
            config=config, region='APAC', code_1='PJ1', code_2='NT3', task_type='pj1_nt3', compass_oef_name_attr="oefname_PJ1",
            internal_oef_name='COMPASS_PJ1_sent', internal_oef_uri_attr="PJ1_sent_oef", unique_id_attr= 'payload_identifier_replicon_uniqueid_PJ1',
            last_unique_id_attr='lasttwbuniqueindentifier_PJ1', division_final_data='pj1_nt3_final_data', export_type='iwo')

        batch_end = rail.EmptyOperator(
            task_id='batch_end'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> batch_end
        can_run_batch_task >> rail.Label('No') >> data_existence_var >> start_download_exports

        start_download_exports >> get_last_time_export_details >> get_current_time_export_details >> create_download_batch \
            >> execute_download_batch >> wait_for_download_batch >> get_download_url \
            >> download_export >> load_export >> create_final_time_data_collection \
            >> create_download_batch_with_hours >> execute_download_batch_with_hours >> wait_for_download_batch_with_hours >> get_download_url_with_hours \
            >> download_export_with_hours >> load_export_with_hours >> create_final_time_data_collection_with_hours \
            >> if_collections_have_data

        if_collections_have_data >> rail.Label("Yes") >> get_effectively_enabled_compass_divisions >> get_all_divisions_with_description >> get_timeoff_types_to_exclude_in_export \
            >> get_time_types_oefs_to_exclude_in_export >> query_filter_time_export_data >> query_ineligible_and_reversals \
                >> query_iwo_reversals >> final_export_data >> if_filtered_timedata_has_data
        if_collections_have_data >> rail.Label("No") >> empty_time_data >> log_no_data_var
        if_filtered_timedata_has_data >> rail.Label("No") >> empty_filtered_data >> log_no_data_var

        log_no_data_var >> get_data_existence_var >> p01_nt2_upload_data_task >> pn1_nt1_upload_data_task \
            >> pj1_nt3_upload_data_task >> batch_end

        if_filtered_timedata_has_data >> rail.Label("Yes") >> initial_data_for_processing \
            >> query_p01_nt2_data >> query_pn1_nt1_data >> query_pj1_nt3_data >> create_p01_nt2_data >> create_pn1_nt1_data \
                >> create_pj1_nt3_data >> get_data_existence_var

    return dag

rail.for_each_instance(create_child_dag)
