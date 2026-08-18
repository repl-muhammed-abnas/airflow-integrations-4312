from datetime import timedelta
from dxctechnology.time_export.compass_outbound.utils import request_payload, response_filters, custom_methods
from dxctechnology.time_export.compass_outbound.tasks.reg_time_data_for_divisions import get_reg_time_data_for_divisions
from dxctechnology.time_export.compass_outbound.tasks.upload_time_data import get_upload_time_data
from airflow.models import Variable
import rail

null = None
def create_child_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.compass_regular_create_time_export_child_dagid,
        description=f"DXC - Compass Regular Time Export Create Compass Regular Time export child - {config.instance}",
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
            no_task='get_last_time_export_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_last_time_export_details',
            end_task='batch_end',
            execution_timeout=timedelta(days=config.execution_timeout_days)
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

        data_existence_var = rail.SetVariableOperator(
            task_id='data_existence_var',
            name='data_existence',
            value=[]
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
                "Time Type": "timetype2",
                "Name": "breaktypename",
                "Time Off Type Name": "timeofftypename",
                "Oncall / Standby": "oncallstandby2",
                "Attribute 1 (Code)": "attributecode1",
                "Attribute 2 (Code)": "attributecode2",
                "Actual Employee ID": "actualempid",
                "Stand by (AUS)": "standbyauscode",
                "On Leave": "onleave",
                "User Status": "userstatus"
            },
            name='finaltimedata'
        )

        if_final_timedata_has_data = rail.IfOperator(
            task_id='if_final_timedata_has_data',
            test='{{ result("create_final_time_data_collection", "length") > 0 }}',
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

        get_timeoff_types_to_exclude_in_export = rail.PythonOperator(
            task_id='get_timeoff_types_to_exclude_in_export',
            python_callable=lambda: '("' + "\",\"".join(list(map(lambda timeoff_type_data: timeoff_type_data["timeoff_type_name"],
                Variable.get(config.timeoff_types_to_exclude, deserialize_json=True)))) + '")'
        )

        query_filter_time_export_data = rail.QueryCollectionOperator(
            task_id='query_filter_time_export_data',
            query="""SELECT * FROM finaltimedata
                WHERE (companycodecode = 'COMPASS' AND
                    attendancetypecode NOT IN ('499', '999', '779')
                    AND breaktypename NOT IN ('Meal', 'Rest')
                    AND timeofftypename NOT IN {{ result('get_timeoff_types_to_exclude_in_export') }})
                OR (projecttype = 'ES' AND projectname LIKE 'E-%'
                    AND attendancetypecode NOT IN ('499', '999', '779')
                    AND breaktypename NOT IN ('Meal', 'Rest'))
                OR (companycodecode = 'COMPASS' AND timeofftypename = "[IND] Leave without pay"
                    AND onleave = "0")
                AND standbyauscode <> "Stand by" ORDER BY CAST(hours AS FLOAT) ASC""",
            name='filtered_time_export_data'
        )

        if_filtered_timedata_has_data = rail.IfOperator(
            task_id='if_filtered_timedata_has_data',
            test='{{ result("query_filter_time_export_data", "length") > 0 }}',
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
                    "newremainningwork","customer1","customer2","customer3","gsapbillableflag","timeofftypedescription",
                    "masterwbs","projecttype","iwoindicator","parentwbs","companycodename","companycodedesc",
                    "taskfullpath","length","timeentryid2","parentserviceorder","internationalassignee","iapernerid",
                    "iwowbselement","attributecode1","attributecode2"],
            name='initial_data_for_processing'
        )

        query_p01_nt2_data = rail.QueryCollectionOperator(
            task_id='query_p01_nt2_data',
            query="SELECT * from initial_data_for_processing WHERE companycodedesc = 'P01' ORDER BY CAST(hours AS FLOAT) ASC",
            name='p01_nt2_data'
        )

        query_pn1_nt1_data = rail.QueryCollectionOperator(
            task_id='query_pn1_nt1_data',
            query="""SELECT * from initial_data_for_processing
                WHERE companycodedesc = 'PN1'
                OR (projecttype = 'ES' AND projectname LIKE 'E-%')
                ORDER BY CAST(hours AS FLOAT) ASC""",
            name='pn1_nt1_data'
        )

        query_pj1_nt3_data = rail.QueryCollectionOperator(
            task_id='query_pj1_nt3_data',
            query="SELECT * from initial_data_for_processing WHERE companycodedesc = 'PJ1' ORDER BY CAST(hours AS FLOAT) ASC",
            name='pj1_nt3_data'
        )

        create_p01_nt2_data = get_reg_time_data_for_divisions(code='P01', task_type='p01_nt2')

        create_pn1_nt1_data = get_reg_time_data_for_divisions(code='PN1', task_type='pn1_nt1')

        create_pj1_nt3_data = get_reg_time_data_for_divisions(code='PJ1', task_type='pj1_nt3')

        get_data_existence_var = rail.GetVariableOperator(
            task_id='get_data_existence_var',
            name='data_existence'
        )

        p01_nt2_upload_data_task = get_upload_time_data(
            config=config, region='EMEA', code_1='P01', code_2='NT2', task_type='p01_nt2', compass_oef_name_attr="oefname_P01",
            internal_oef_name='COMPASS_P01_sent', internal_oef_uri_attr="P01_sent_oef", unique_id_attr= 'payload_identifier_replicon_uniqueid_P01',
            last_unique_id_attr='lasttwbuniqueindentifier_P01', division_final_data='p01_nt2_final_data', export_type='reg')

        pn1_nt1_upload_data_task = get_upload_time_data(
            config=config, region='AMER', code_1='PN1', code_2='NT1', task_type='pn1_nt1', compass_oef_name_attr="oefname_PN1",
            internal_oef_name='COMPASS_PN1_sent', internal_oef_uri_attr="PN1_sent_oef", unique_id_attr= 'payload_identifier_replicon_uniqueid_PN1',
            last_unique_id_attr='lasttwbuniqueindentifier_PN1', division_final_data='pn1_nt1_final_data', export_type='reg')

        pj1_nt3_upload_data_task = get_upload_time_data(
            config=config, region='APAC', code_1='PJ1', code_2='NT3', task_type='pj1_nt3', compass_oef_name_attr="oefname_PJ1",
            internal_oef_name='COMPASS_PJ1_sent', internal_oef_uri_attr="PJ1_sent_oef", unique_id_attr= 'payload_identifier_replicon_uniqueid_PJ1',
            last_unique_id_attr='lasttwbuniqueindentifier_PJ1', division_final_data='pj1_nt3_final_data', export_type='reg')

        batch_end = rail.EmptyOperator(
            task_id='batch_end'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> batch_end
        can_run_batch_task >> rail.Label('No') >> get_last_time_export_details

        get_last_time_export_details >> get_current_time_export_details >> create_download_batch \
            >> execute_download_batch >> wait_for_download_batch >> get_download_url \
                >> download_export >> load_export >> data_existence_var \
                    >> create_final_time_data_collection >> if_final_timedata_has_data

        if_final_timedata_has_data >> rail.Label("Yes") >> get_effectively_enabled_compass_divisions >> get_timeoff_types_to_exclude_in_export \
            >> query_filter_time_export_data >> if_filtered_timedata_has_data
        if_final_timedata_has_data >> rail.Label("No") >> empty_time_data >> log_no_data_var
        if_filtered_timedata_has_data >> rail.Label("No") >> empty_filtered_data >> log_no_data_var

        log_no_data_var >> get_data_existence_var

        if_filtered_timedata_has_data >> rail.Label("Yes") >> initial_data_for_processing \
            >> query_p01_nt2_data >> query_pn1_nt1_data >> query_pj1_nt3_data >> create_p01_nt2_data >> create_pn1_nt1_data \
                >> create_pj1_nt3_data >> get_data_existence_var >> p01_nt2_upload_data_task >> pn1_nt1_upload_data_task \
                    >> pj1_nt3_upload_data_task >> batch_end

    return dag

rail.for_each_instance(create_child_dag)
