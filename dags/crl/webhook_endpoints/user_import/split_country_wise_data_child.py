from datetime import timedelta
from airflow.models import Variable
import rail

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_split_country_wise_data_dagid,
        description='CRL - User Import - Split Country Wise data',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_split_country_wise_data,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source=lambda dag_run: dag_run.conf['payload'],
            name="input_data_collection_payload",
            columns={
                "Empl_ID": "Empl_ID",
                "First_Name": "First_Name",
                "Last_Name": "Last_Name",
                "Work_Email": "Work_Email",
                "User_Name": "User_Name",
                "Empl_Status": "Empl_Status",
                "Is_Contingent": "Is_Contingent",
                "Title": "Title",
                "Bus_Seg_Unit": "Bus_Seg_Unit",
                "Bus_Unit_Label": "Bus_Unit_Label",
                "Functional_Segment": "Functional_Segment",
                "Company": "Company",
                "Location": "Location",
                "Reg_Temp": "Reg_Temp",
                "Full_Part": "Full_Part",
                "Std_Hours": "Std_Hours",
                "Supv_Empl_ID": "Supv_Empl_ID",
                "Hire_Date": "Hire_Date",
                "Adj_Hire_Date": "Adj_Hire_Date",
                "is_HRBP": "is_HRBP",
                "Job_Code": "Job_Code",
                "Pay_Group": "Pay_Group",
                "Pay_Type": "Pay_Type",
                "US_FLSA_Status": "US_FLSA_Status",
                "Cost_Center_Business_Area": "Cost_Center_Business_Area",
                "Cost_Center_Label": "Cost_Center_Label",
                "Profit_Center": "Profit_Center",
                "Activity_Type": "Activity_Type",
                "Last_Worked_Day": "Last_Worked_Day",
                "Vacation_Exception": "Vacation_Exception",
                "US_Veterans_Status": "US_Veterans_Status",
                "SAP_Work_Schedule":"SAP_Work_Schedule",
                "Remote_Worker": "Remote_Worker",
                "Change_Effective_Date": "Change_Effective_Date",
                "Event": "Event",
                "Event_Reason_Code":"Event_Reason_Code",
                "department":"department",
                "name":"name",
                "holidayCalendarCode":"holidayCalendarCode",
                "Home_Location": "Home_Location",
                "Staff_Category":"Staff_Category",
                "Functional_Sub_Segment":"Functional_Sub_Segment",
                "Time_Off_Schedule":"Time_Off_Schedule",
                "Pay_Scale_Group": "Pay_Scale_Group",
                "Job_Level": "Job_Level",
                "OT_Eligible": "OT_Eligible"
                }
        )

        query_non_live_locations_records  = rail.QueryCollectionOperator(
            task_id="query_non_live_locations_records",
            name='non_live_location_records',
            query=f"""SELECT * FROM input_data_collection_payload WHERE NULLIF(Location, '') IS NOT NULL
                    AND {config.NON_LIVE_COUNTRIES_QUERY} """
        )

        has_non_live_location_records = rail.IfOperator(
            task_id='has_non_live_location_records',
            test="{{ result('query_non_live_locations_records','length') > 0 }}",
            yes_task='process_non_live_locations_records',
            no_task='query_canada_location_records'
        )

        process_non_live_locations_records = rail.TriggerDagRunOperator(
            task_id="process_non_live_locations_records",
            trigger_dag_id=config.process_non_live_location_records_dagid,
            conf=lambda dag_run: {
                "payload": rail.result("query_non_live_locations_records"),
                "log_filename": dag_run.conf['log_filename']+'_others.csv',
                "uploaded_payload_filename": dag_run.conf['uploaded_payload_filename']
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_process_non_live_locations_record = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_non_live_locations_record',
            dag_runs='{{ result("process_non_live_locations_records") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        query_canada_location_records  = rail.QueryCollectionOperator(
            task_id="query_canada_location_records",
            name='canada_location_records',
            query="""SELECT * FROM input_data_collection_payload WHERE NULLIF(Location, '') IS NOT NULL
                    AND Location LIKE 'CAN%'"""
        )

        has_canada_location_records = rail.IfOperator(
            task_id='has_canada_location_records',
            test="{{ result('query_canada_location_records','length') > 0 }}",
            yes_task='process_canada_locations_records',
            no_task='query_usa_locations_records'
        )

        process_canada_locations_records = rail.TriggerDagRunOperator(
            task_id="process_canada_locations_records",
            trigger_dag_id=config.process_canada_location_records_dagid,
            conf=lambda dag_run: {
                "payload": rail.result("query_canada_location_records"),
                "log_filename": dag_run.conf['log_filename']+'_canada.csv',
                "uploaded_payload_filename": dag_run.conf['uploaded_payload_filename']
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_process_canada_locations_records = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_canada_locations_records',
            dag_runs='{{ result("process_canada_locations_records") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        query_usa_locations_records  = rail.QueryCollectionOperator(
            task_id="query_usa_locations_records",
            name='usa_location_records',
            query="""SELECT * FROM input_data_collection_payload WHERE NULLIF(Location, '') IS NOT NULL
                    AND Location LIKE 'USA%'"""
        )

        has_usa_location_records = rail.IfOperator(
            task_id='has_usa_location_records',
            test="{{ result('query_usa_locations_records','length') > 0 }}",
            yes_task='process_usa_locations_records',
            no_task='can_process_mauritius_location'
        )

        process_usa_locations_records = rail.TriggerDagRunOperator(
            task_id="process_usa_locations_records",
            trigger_dag_id=config.process_usa_location_records_dagid,
            conf=lambda dag_run: {
                "payload": rail.result("query_usa_locations_records"),
                "log_filename": dag_run.conf['log_filename']+'_usa.csv',
                "uploaded_payload_filename": dag_run.conf['uploaded_payload_filename']
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_process_usa_locations_records = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_usa_locations_records',
            dag_runs='{{ result("process_usa_locations_records") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        can_process_mauritius_location = rail.IfOperator(
            task_id='can_process_mauritius_location',
            test=lambda: Variable.get(
                config.can_process_mauritius_location_var, default_var='true').lower() == 'true',
            yes_task='query_mauritius_location_records',
            no_task='can_process_ireland_location'
        )

        query_mauritius_location_records  = rail.QueryCollectionOperator(
            task_id="query_mauritius_location_records",
            name='mauritius_location_records',
            query="""SELECT * FROM input_data_collection_payload WHERE NULLIF(Location, '') IS NOT NULL
                    AND Location LIKE 'Mauritius%'"""
        )

        has_mauritius_location_records = rail.IfOperator(
            task_id='has_mauritius_location_records',
            test="{{ result('query_mauritius_location_records','length') > 0 }}",
            yes_task='process_mauritius_locations_records',
            no_task='can_process_ireland_location'
        )

        process_mauritius_locations_records = rail.TriggerDagRunOperator(
            task_id="process_mauritius_locations_records",
            trigger_dag_id=config.process_mauritius_location_records_dagid,
            conf=lambda dag_run: {
                "payload": rail.result("query_mauritius_location_records"),
                "log_filename": dag_run.conf['log_filename']+'_mauritius.csv',
                "uploaded_payload_filename": dag_run.conf['uploaded_payload_filename']
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        can_process_ireland_location = rail.IfOperator(
            task_id='can_process_ireland_location',
            test=lambda: Variable.get(
                config.can_process_ireland_location_var, default_var='true').lower() == 'true',
            yes_task='query_ireland_location_records',
            no_task='can_process_uk_location'
        )

        query_ireland_location_records  = rail.QueryCollectionOperator(
            task_id="query_ireland_location_records",
            name='ireland_location_records',
            query="""SELECT * FROM input_data_collection_payload WHERE NULLIF(Location, '') IS NOT NULL
                    AND Location LIKE 'IRL%'"""
        )

        has_ireland_location_records = rail.IfOperator(
            task_id='has_ireland_location_records',
            test="{{ result('query_ireland_location_records','length') > 0 }}",
            yes_task='process_ireland_locations_records',
            no_task='can_process_uk_location'
        )

        process_ireland_locations_records = rail.TriggerDagRunOperator(
            task_id="process_ireland_locations_records",
            trigger_dag_id=config.process_ireland_location_records_dagid,
            conf=lambda dag_run: {
                "payload": rail.result("query_ireland_location_records"),
                "log_filename": dag_run.conf['log_filename']+'_ireland.csv',
                "uploaded_payload_filename": dag_run.conf['uploaded_payload_filename']
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        can_process_uk_location = rail.IfOperator(
            task_id='can_process_uk_location',
            test=lambda: Variable.get(
                config.can_process_uk_location_var, default_var='true').lower() == 'true',
            yes_task='query_uk_location_records',
            no_task='can_process_brazil_location'
        )

        query_uk_location_records  = rail.QueryCollectionOperator(
            task_id="query_uk_location_records",
            name='uk_location_records',
            query="""SELECT * FROM input_data_collection_payload WHERE NULLIF(Location, '') IS NOT NULL
                    AND Location LIKE 'GBR%'"""
        )

        has_uk_location_records = rail.IfOperator(
            task_id='has_uk_location_records',
            test="{{ result('query_uk_location_records','length') > 0 }}",
            yes_task='process_uk_locations_records',
            no_task='can_process_brazil_location'
        )

        process_uk_locations_records = rail.TriggerDagRunOperator(
            task_id="process_uk_locations_records",
            trigger_dag_id=config.process_uk_location_records_dagid,
            conf=lambda dag_run: {
                "payload": rail.result("query_uk_location_records"),
                "log_filename": dag_run.conf['log_filename']+'_uk.csv',
                "uploaded_payload_filename": dag_run.conf['uploaded_payload_filename']
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        can_process_brazil_location = rail.IfOperator(
            task_id='can_process_brazil_location',
            test=lambda: Variable.get(
                config.can_process_brazil_location_var, default_var='true').lower() == 'true',
            yes_task='query_brazil_location_records',
            no_task='can_process_israel_location'
        )

        query_brazil_location_records  = rail.QueryCollectionOperator(
            task_id="query_brazil_location_records",
            name='brazil_location_records',
            query="""SELECT * FROM input_data_collection_payload WHERE NULLIF(Location, '') IS NOT NULL
                    AND Location LIKE 'BRA%'"""
        )

        has_brazil_location_records = rail.IfOperator(
            task_id='has_brazil_location_records',
            test="{{ result('query_brazil_location_records','length') > 0 }}",
            yes_task='process_brazil_locations_records',
            no_task='can_process_israel_location'
        )

        process_brazil_locations_records = rail.TriggerDagRunOperator(
            task_id="process_brazil_locations_records",
            trigger_dag_id=config.process_brazil_location_records_dagid,
            conf=lambda dag_run: {
                "payload": rail.result("query_brazil_location_records"),
                "log_filename": dag_run.conf['log_filename']+'_brazil.csv',
                "uploaded_payload_filename": dag_run.conf['uploaded_payload_filename']
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        can_process_israel_location = rail.IfOperator(
            task_id='can_process_israel_location',
            test=lambda: Variable.get(
                config.can_process_israel_location_var, default_var='true').lower() == 'true',
            yes_task='query_israel_location_records',
            no_task='can_process_switzerland_location'
        )

        query_israel_location_records = rail.QueryCollectionOperator(
            task_id="query_israel_location_records",
            name='israel_location_records',
            query="""SELECT * FROM input_data_collection_payload WHERE NULLIF(Location, '') IS NOT NULL
                    AND Location LIKE 'ISR%'"""
        )

        has_israel_location_records = rail.IfOperator(
            task_id='has_israel_location_records',
            test="{{ result('query_israel_location_records','length') > 0 }}",
            yes_task='process_israel_locations_records',
            no_task='can_process_switzerland_location'
        )

        process_israel_locations_records = rail.TriggerDagRunOperator(
            task_id="process_israel_locations_records",
            trigger_dag_id=config.process_israel_location_records_dagid,
            conf=lambda dag_run: {
                "payload": rail.result("query_israel_location_records"),
                "log_filename": dag_run.conf['log_filename']+'_israel.csv',
                "uploaded_payload_filename": dag_run.conf['uploaded_payload_filename']
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        can_process_switzerland_location = rail.IfOperator(
            task_id='can_process_switzerland_location',
            test=lambda: Variable.get(
                config.can_process_switzerland_location_var, default_var='true').lower() == 'true',
            yes_task='query_switzerland_location_records',
            no_task='finish'
        )

        query_switzerland_location_records = rail.QueryCollectionOperator(
            task_id="query_switzerland_location_records",
            name='switzerland_location_records',
            query="""SELECT * FROM input_data_collection_payload WHERE NULLIF(Location, '') IS NOT NULL
                    AND Location LIKE 'CHE%'"""
        )

        has_switzerland_location_records = rail.IfOperator(
            task_id='has_switzerland_location_records',
            test="{{ result('query_switzerland_location_records','length') > 0 }}",
            yes_task='process_switzerland_locations_records',
            no_task='finish'
        )

        process_switzerland_locations_records = rail.TriggerDagRunOperator(
            task_id="process_switzerland_locations_records",
            trigger_dag_id=config.process_switzerland_location_records_dagid,
            conf=lambda dag_run: {
                "payload": rail.result("query_switzerland_location_records"),
                "log_filename": dag_run.conf['log_filename']+'_switzerland.csv',
                "uploaded_payload_filename": dag_run.conf['uploaded_payload_filename']
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )


        create_input_data_collection >> query_non_live_locations_records >> has_non_live_location_records

        has_non_live_location_records >> rail.Label('Yes') >> process_non_live_locations_records
        has_non_live_location_records >> rail.Label('No') >> query_canada_location_records

        process_non_live_locations_records >> wait_for_process_non_live_locations_record

        wait_for_process_non_live_locations_record >> query_canada_location_records
        query_canada_location_records >> has_canada_location_records

        has_canada_location_records >> rail.Label('Yes') >> process_canada_locations_records
        has_canada_location_records >> rail.Label('No') >> query_usa_locations_records

        process_canada_locations_records >> wait_for_process_canada_locations_records

        wait_for_process_canada_locations_records >> query_usa_locations_records >> has_usa_location_records

        has_usa_location_records >> rail.Label('Yes') >> process_usa_locations_records
        has_usa_location_records >> rail.Label('No') >> can_process_mauritius_location

        process_usa_locations_records >> wait_for_process_usa_locations_records >> can_process_mauritius_location

        can_process_mauritius_location >> rail.Label('Yes') >> query_mauritius_location_records
        can_process_mauritius_location >> rail.Label('No') >> can_process_ireland_location
        can_process_ireland_location >> rail.Label('No') >> can_process_uk_location
        can_process_ireland_location >> rail.Label('Yes') >> query_ireland_location_records
        can_process_uk_location >> rail.Label("No") >> can_process_brazil_location
        query_mauritius_location_records >> has_mauritius_location_records

        has_mauritius_location_records >> rail.Label('Yes') >> process_mauritius_locations_records >> can_process_ireland_location
        has_mauritius_location_records >> rail.Label('No') >> can_process_ireland_location

        query_ireland_location_records >> has_ireland_location_records >> rail.Label('Yes') >> process_ireland_locations_records >> can_process_uk_location
        has_ireland_location_records >> rail.Label('No') >> can_process_uk_location
        can_process_uk_location >> rail.Label('Yes') >> query_uk_location_records
        can_process_uk_location >> rail.Label('No') >> can_process_brazil_location
        can_process_brazil_location >> rail.Label('Yes') >> query_brazil_location_records
        can_process_uk_location >> rail.Label('No') >> can_process_brazil_location
        query_uk_location_records >> has_uk_location_records >> rail.Label('Yes') >> process_uk_locations_records >> can_process_brazil_location
        has_uk_location_records >> rail.Label('No') >> can_process_brazil_location
        can_process_brazil_location >> rail.Label('No') >> can_process_israel_location
        query_brazil_location_records >> has_brazil_location_records >> rail.Label('Yes') >> process_brazil_locations_records >> can_process_israel_location
        has_brazil_location_records >> rail.Label('No') >> can_process_israel_location
        can_process_israel_location >> rail.Label('Yes') >> query_israel_location_records
        can_process_israel_location >> rail.Label('No') >> can_process_switzerland_location
        query_israel_location_records >> has_israel_location_records >> rail.Label('Yes') >> process_israel_locations_records >> can_process_switzerland_location
        has_israel_location_records >> rail.Label('No') >> can_process_switzerland_location
        can_process_switzerland_location >> rail.Label('Yes') >> query_switzerland_location_records
        can_process_switzerland_location >> rail.Label('No') >> finish
        query_switzerland_location_records >> has_switzerland_location_records >> rail.Label('Yes') >> process_switzerland_locations_records >> finish
        has_switzerland_location_records >> rail.Label('No') >> finish
    return dag

rail.for_each_instance(create_child_dag)
