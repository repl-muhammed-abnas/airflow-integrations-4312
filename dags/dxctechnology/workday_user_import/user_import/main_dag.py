from datetime import timedelta
from functools import lru_cache
from os import path
from pendulum import datetime
import rail
from airflow.models import Variable
from dxctechnology.workday_user_import.user_import.common_utils import request_payload
from dxctechnology.workday_user_import.user_import.common_utils import response_filter



def create_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_main_dag,
        description="dxctechnology workday user sync Master",
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=30),
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.max_active_run_master,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id = "new_file_sensor",
            path=config.input_file_path,
            soft_fail_timeout=timedelta(minutes=10)
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email'
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon user import for workday  - Incorrect File Format - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/bad_file_format.html"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            trigger_rule='all_done',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_file_path +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_decrypt_file = rail.IfOperator(
            task_id ="can_decrypt_file",
            test=lambda: Variable.get(config.can_decrypt_file_var_name, default_var='false').lower() == 'true',
            yes_task='decrypt_file',
            no_task='dummy_load_data'
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            source='{{ result("download_file") }}',
            pgp_conn_id=config.pgp_conn_id
        )

        upload_decrypted_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_decrypted_file_to_sftp',
            content="{{ result('decrypt_file') }}",
            remote_filepath=config.archive_file_path +  "/Decrypted_{{result('new_file_sensor') | file_name | replace('.csv.pgp','') | replace('.csv','') }}" + ".csv"
        )

        dummy_load_data = rail.PythonOperator(
            task_id= "dummy_load_data",
            python_callable= lambda: rail.result('decrypt_file') if Variable.get(
                config.can_decrypt_file_var_name, default_var='false').lower()== 'true' else  rail.result('download_file'),
            show_return_value_in_logs= False
        )

        load_data = rail.LoadCSVFileOperator(
            task_id='load_data',
            document="{{ result('dummy_load_data') }}",
            encoding="utf-8-sig"
        )

        create_log = rail.CreateLogOperator(
            task_id = "create_log"
        )

        create_supervisor_log = rail.CreateLogOperator(
            task_id = "create_supervisor_log"
        )

        create_input_collection = rail.CreateCollectionOperator(
            task_id = "create_input_collection",
            source="{{result('load_data')}}",
            name = "file_raw_data",
            columns={
                "emp_id": "empid",
                "pernr_id": "pernerid",
                "email": "email",
                "fname": "firstname",
                "lname": "lastname",
                "location_country": "country",
                "location_state_province": "state",
                "exempt": "exempt",
                "exempt_eff_date": "exempteffectivedate",
                "employee_type": "employeetype",
                "hire_date": "hiredate",
                "gender": "gender",
                "service_date": "servicedate",
                "term_date": "termdate",
                "status": "status",
                "on_leave": "onleave",
                "company_code": "companycode",
                "company_name": "companyname",
                "personnel_area_code": "areacode",
                "personnel_area_name": "areaname",
                "personnel_subarea_code": "subareacode",
                "emp_group_code": "empgroupcode",
                "emp_group_name": "empgroupname",
                "emp_Subgroup_code": "empsubgroupcode",
                "emp_Subgroup_Name": "empsubgroupname",
                "supervisor_id": "supervisorid",
                "supervisor_eff_date": "supervisordate",
                "supervisor_fname": "supervisorfname",
                "supervisor_lname": "supervisorlname",
                "supervisor_email": "supervisoremail",
                "Pay_Group": "paygroup",
                "location_eff_date": "locationeffectivedate",
                "home_country": "homecountry",
                "cost_center": "costcenter",
                "cost_center_name": "costcentername",
                "cost_center_eff_date": "costcentereffectivedate",
                "org_code": "orgcode",
                "org_unit_name": "orgname",
                "work_shift": "workshift",
                "work_shift_eff_date": "workshifteffectivedate",
                "job_level": "joblevel",
                "Job_change_effective_date": "jobchangeeffectivedate",
                "fte": "fte",
                "fte_pct": "ftepct",
                "is_ia": "isia",
                "ia_start_date": "iastartdate",
                "ia_end_date": "iaenddate",
                "RUT": "rut",
                "Middle_Name": "middlename",
                "Time_Type": "timetype",
                "DOB": "dob",
                "Mgmt_Lvl": "managementlvl",
                "AUS_JC": "ausjc",
                "Terms_Conditions": "termsconditions",
                "Industrial_Instrument_Classification": "industrialinstrumentclassification",
                "Additional_Data_Effective_Date": "additionaldataeffectivedate",
                "Termination_Reason": "terminationreason",
                "Scheduled_weekly_Hours": "scheduledweeklyhours",
                "assignment_type": "assignment_type",
                "Home_State": "home_state",
                "Work_City":"workcity",
                "Marital_Status_Ind":"marital_status_ind",
                "Marital_Status_efft_dt": "marital_status_efft_dt",
                'Additional_Job_Classifications': 'Additional_Job_Classifications',
                'Holiday_Schedule_Calendar': 'Holiday_Schedule_Calendar',
                'Employee_representative_indicator': 'Employee_representative_indicator',
                'Employee_Representative_Effective_Date': 'Employee_Representative_Effective_Date',
                "Default_Weekly_Hours": 'Default_Weekly_Hours'
            }
        )

        def _get_parent_company_code(company_code):
            # Placeholder implementation - replace with actual logic to fetch parent company code
            parent_company = list(filter(
                lambda m: m["Type"] == "Company Code" and m['URI'] == company_code, config.DXC_WORKDAY_USER_SYNC_USER_MAPPER
            ))
            return parent_company[0]["Source"] if parent_company else ""

        def get_updated_country_state_based_on_ia(item):
            if not item:
                return []
            country_to_use = item['country']
            state_to_use = item['state']

            if item['isia']:
                if item['isia'] in [1,'1'] and _get_parent_company_code(item['companycode']) == "COMPASS" and item['country'] in (
                    "United States of America", "Puerto Rico", "India", "Costa Rica", 'Australia', 'Portugal'):
                    if  "home pay" in item['assignment_type'].lower():
                        country_to_use = item['homecountry']
                        state_to_use = item['state'] #item['home_state']

            return {
                'empid': item['empid'],
                'pernerid': item['pernerid'],
                'email': item['email'],
                'firstname': item['firstname'],
                'lastname': item['lastname'],
                'country': item['country'],
                'state': item['state'],
                'exempt': item['exempt'],
                'exempteffectivedate': item['exempteffectivedate'],
                'employeetype': item['employeetype'],
                'hiredate': item['hiredate'],
                'gender': item['gender'],
                'servicedate': item['servicedate'],
                'termdate': item['termdate'],
                'status': item['status'],
                'onleave': item['onleave'],
                'companycode': item['companycode'],
                'companyname': item['companyname'],
                'areacode': item['areacode'], 
                'areaname': item['areaname'], 
                'subareacode': item['subareacode'],
                'empgroupcode': item['empgroupcode'], 
                'empgroupname': item['empgroupname'], 
                'empsubgroupcode': item['empsubgroupcode'], 
                'empsubgroupname': item['empsubgroupname'], 
                'supervisorid': item['supervisorid'], 
                'supervisordate': item['supervisordate'], 
                'supervisorfname': item['supervisorfname'], 
                'supervisorlname': item['supervisorlname'], 
                'supervisoremail': item['supervisoremail'], 
                'paygroup': item['paygroup'], 
                'locationeffectivedate': item['locationeffectivedate'], 
                'homecountry': item['homecountry'], 
                'costcenter': item['costcenter'], 
                'costcentername': item['costcentername'], 
                'costcentereffectivedate': item['costcentereffectivedate'],
                'orgcode': item['orgcode'],
                'orgname': item['orgname'], 
                'workshift': item['workshift'], 
                'workshifteffectivedate': item['workshifteffectivedate'], 
                'joblevel': item['joblevel'], 
                'jobchangeeffectivedate': item['jobchangeeffectivedate'], 
                'fte': item['fte'], 
                'ftepct': item['ftepct'], 
                'isia': item['isia'], 
                'iastartdate': item['iastartdate'], 
                'iaenddate': item['iaenddate'], 
                'rut': item['rut'], 
                'middlename': item['middlename'], 
                'timetype': item['timetype'], 
                'dob': item['dob'], 
                'managementlvl': item['managementlvl'], 
                'ausjc': item['ausjc'], 
                'termsconditions': item['termsconditions'], 
                'industrialinstrumentclassification': item['industrialinstrumentclassification'], 
                'additionaldataeffectivedate': item['additionaldataeffectivedate'], 
                'terminationreason': item['terminationreason'], 
                'scheduledweeklyhours': item['scheduledweeklyhours'], 
                'assignment_type': item['assignment_type'], 
                'home_state': item['home_state'], 
                '_actual_country': item['country'], 
                '_actual_state': item['state'],
                '_actual_home_country': item['homecountry'],
                '_actual_home_state': item['home_state'],
                '_country_to_use_for_query': country_to_use,
                '_state_to_use_for_query': state_to_use,
                'workcity': item['workcity'],
                'marital_status_ind': item['marital_status_ind'],
                'marital_status_efft_dt': item['marital_status_efft_dt'],
                'Additional_Job_Classifications': item['Additional_Job_Classifications'],
                'Holiday_Schedule_Calendar': item['Holiday_Schedule_Calendar'],
                'Employee_representative_indicator': item['Employee_representative_indicator'],
                'Employee_Representative_Effective_Date': item['Employee_Representative_Effective_Date'],
                'Default_Weekly_Hours': item['Default_Weekly_Hours']
            }

        updated_country_state_based_on_ia = rail.DataAdaptorOperator(
            task_id = "updated_country_state_based_on_ia",
            source="{{result('create_input_collection')}}",
            columns=['empid', 'pernerid', 'email',
                     'firstname', 'lastname', 'country', 'state', 'exempt', 'exempteffectivedate',
                     'employeetype', 'hiredate', 'gender', 'servicedate', 'termdate', 'status', 'onleave',
                     'companycode', 'companyname', 'areacode', 'areaname', 'subareacode', 'empgroupcode',
                     'empgroupname', 'empsubgroupcode', 'empsubgroupname', 'supervisorid', 'supervisordate', 'supervisorfname',
                     'supervisorlname', 'supervisoremail', 'paygroup', 'locationeffectivedate', 'homecountry', 'costcenter', 'costcentername',
                     'costcentereffectivedate', 'orgcode', 'orgname', 'workshift', 'workshifteffectivedate', 'joblevel', 'jobchangeeffectivedate',
                     'fte', 'ftepct', 'isia', 'iastartdate', 'iaenddate', 'rut', 'middlename', 'timetype', 'dob', 'managementlvl', 'ausjc',
                     'termsconditions', 'industrialinstrumentclassification', 'additionaldataeffectivedate', 'terminationreason', 'scheduledweeklyhours',
                     'assignment_type', 'home_state', '_actual_country' ,'_actual_state', '_country_to_use_for_query', '_state_to_use_for_query','workcity','marital_status_ind','marital_status_efft_dt',
                     'Additional_Job_Classifications','Holiday_Schedule_Calendar','Employee_representative_indicator','Employee_Representative_Effective_Date', 'Default_Weekly_Hours'],
            data=get_updated_country_state_based_on_ia
        )

        create_input_collection2 = rail.CreateCollectionOperator(
            task_id = "create_input_collection2",
            source="{{result('updated_country_state_based_on_ia')}}",
            name = "raw_data",
        )
        

        get_users_for_allowed_locations = rail.QueryCollectionOperator(
            task_id = "get_users_for_allowed_locations",
            name = "valid_raw_data",
            #! Can `in` be used?
            query="""SELECT * FROM raw_data rd WHERE
                        rd.country="United States of America" OR
                        rd.country="Canada" OR
                        rd.country="Argentina" OR
                        rd.country="Australia" OR
                        rd.country="Austria" OR
                        rd.country="South Africa" OR
                        rd.country="Italy" OR
                        rd.country="Japan" OR
                        rd.country="Korea- Republic of" OR
                        rd.country="Luxembourg" OR
                        rd.country="Morocco" OR
                        rd.country="Mexico" OR
                        rd.country="Malaysia" OR
                        rd.country="Netherlands" OR
                        rd.country="New Zealand" OR
                        rd.country="Philippines" OR
                        rd.country="Poland" OR
                        rd.country="Taiwan" OR
                        rd.country="Sweden" OR
                        rd.country="Singapore" OR
                        rd.country="Saudi Arabia" OR
                        rd.country="Russian Federation" OR
                        rd.country="Qatar" OR
                        rd.country="Portugal" OR
                        rd.country="Korea, Democratic People's Republic of" OR
                        rd.country="Israel" OR
                        rd.country="Ireland" OR
                        rd.country="India" OR
                        rd.country="Hungary" OR
                        rd.country="Hong Kong" OR
                        rd.country="Greece" OR
                        rd.country="United Kingdom" OR
                        rd.country="France" OR
                        rd.country="Finland" OR
                        rd.country="Spain" OR
                        rd.country="Egypt" OR
                        rd.country="Denmark" OR
                        rd.country="Germany" OR
                        rd.country="Costa Rica" OR
                        rd.country="China" OR
                        rd.country="Chile" OR
                        rd.country="Switzerland" OR
                        rd.country="Brazil" OR
                        rd.country="Belgium" OR
                        rd.country="United Arab Emirates" OR
                        rd.country="Colombia" OR
                        rd.country="Czechia" OR
                        rd.country="Indonesia" OR
                        rd.country="Lithuania" OR
                        rd.country="Norway" OR
                        rd.country="Puerto Rico" OR
                        rd.country="Romania" OR
                        rd.country="Slovakia" OR
                        rd.country="Turkey" OR
                        rd.country="Panama" OR
                        rd.country="Peru" OR
                        rd.country="Thailand" OR
                        rd.country="Ukraine" OR
                        rd.country="Bulgaria" OR
                        rd.country="Croatia" OR
                        rd.country="Jordan" OR
                        rd.country="Serbia" OR
                        rd.country="Nigeria" OR
                        rd.country="Tunisia" OR
                        rd.country="Vietnam" OR
                        rd.country="Kazakhstan" OR
                        rd.country="Brunei" OR
                        rd.country="Fiji"
                    """
        )

        has_any_data = rail.IfOperator(
            task_id = "has_any_data",
            test = "{{result('get_users_for_allowed_locations', 'length') > 0 }}",
            yes_task = "query_invalid_records",
            no_task = "send_blank_file_email"
        )

        send_blank_file_email = rail.EmptyOperator(
            task_id = "send_blank_file_email"
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id = "query_invalid_records",
            name = "invalid_raw_data",
            query = """SELECT * FROM valid_raw_data vrd WHERE
                    NULLIF(empid, '') IS NULL OR
                    NULLIF(firstname, '') IS NULL OR
                    NULLIF(lastname, '') IS NULL OR
                    NULLIF(email, '') IS NULL OR
                    NULLIF(country, '') IS NULL OR
                    NULLIF(hiredate, '') IS NULL OR
                    NULLIF(status, '') IS NULL OR
                    NULLIF(companycode, '') IS NULL OR
                    NULLIF(supervisorid, '') IS NULL OR
                    NULLIF(costcenter, '') IS NULL  OR
                    NULLIF(workshift, '') IS NULL
                """,
        )

        def get_exception_message(item):
            msg = []
            for key, log_msg in request_payload.USER_IMPORT_MANDATORY_FIELDS.items():
                if not item[key]:
                    msg.append(f"{log_msg} not available")
            return rail.smartjoin_by_delim(msg, ',')


        log_invalid_records = rail.WriteLogOperator(
            task_id = "log_invalid_records",
            log="{{result('create_log')}}",
            items="{{result('query_invalid_records')}}",
            severity="Skipped",
            message="Mandatory Fields are missing",
            properties = lambda item: {
                "Userid": item['empid'],
                "Email": None,
                "Action": "Validation",
                "Status": "Skipped",
                "Details": get_exception_message(item)
            }
        )

        create_mapper_collection = rail.CreateCollectionOperator(
            task_id = "create_mapper_collection",
            source=config.DXC_WORKDAY_USER_SYNC_USER_MAPPER,
            name = "mapper"
        )

        # Mapper data is loaded here used while trigger the process users
        query_valid_records = rail.QueryCollectionOperator(
            task_id = "query_valid_records",
            query="""SELECT
                        vrd.*,
                        CASE 
                            WHEN vrd.fte is not null
                                THEN CASE
                                    WHEN CAST(vrd.ftepct AS FLOAT) < 100
                                        THEN "Part Time"
                                    ELSE "Full Time"
                                END
                            ELSE
                                "Full Time"
                        END	AS fulltimeparttime,
                        (SELECT m."Source" FROM mapper m WHERE
                            m."Type" = "Company Code" AND
                            m.uri == vrd.companycode LIMIT 1) AS _parent_company_code
                    FROM valid_raw_data vrd WHERE
                        NULLIF(empid, '') IS NOT NULL AND
                        NULLIF(firstname, '') IS NOT NULL AND
                        NULLIF(lastname, '') IS NOT NULL AND
                        NULLIF(email, '') IS NOT NULL AND
                        NULLIF(country, '') IS NOT NULL AND
                        NULLIF(hiredate, '') IS NOT NULL AND
                        NULLIF(status, '') IS NOT NULL AND
                        NULLIF(companycode, '') IS NOT NULL AND
                        NULLIF(supervisorid, '') IS NOT NULL AND
                        NULLIF(costcenter, '') IS NOT NULL  AND
                        NULLIF(workshift, '') IS NOT NULL""",
            name="raw_valid_records"
        )

        has_any_valid_data = rail.IfOperator(
            task_id = "has_any_valid_data",
            test="{{result('query_valid_records', 'length') > 0}}",
            yes_task="start_pre-check_processing",
            no_task="gather_logs" # to be added here
        )

        start_pre_check_processing = rail.EmptyOperator(
            task_id = "start_pre-check_processing"
        )

        def get_all_user_custom_fields_data_handler(response):
            UDF_FIELDS = config.UDFs.copy()
            res = {}
            rail.set_result(key= "response", val = response)
            # doing in for loop to avoid multiple iter of response while using rail.find_first_by_attr_and_get_attr
            for udf in response:
                if not UDF_FIELDS:
                    break
                if udf['displayText'] in UDF_FIELDS:
                    res[udf['displayText'].replace(
                        ".", "").replace(" ", "_").lower()] = {"name": udf['displayText'], "uri": udf['uri']}
                    UDF_FIELDS.remove(udf['displayText'])
            rail.set_result(key = "udfs_not_found", val=UDF_FIELDS)
            return res

        get_all_user_custom_fields = rail.RepliconServiceOperator(
            task_id = "get_all_user_custom_fields",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=get_all_user_custom_fields_data_handler
        )

        trigger_pre_chec_dag = rail.TriggerDagRunOperator(
            task_id = "trigger_pre_chec_dag",
            trigger_dag_id=config.workday_user_import_process_groups_udfs_dag,
            conf = lambda: {
                "file_name": path.split(rail.result("new_file_sensor"))[1],
                "employee_group": rail.result("get_all_user_custom_fields").get("employee_group", {}),
                "employee_sub_group": rail.result("get_all_user_custom_fields").get("employee_sub_group", {})
            }
        )

        wait_trigger_pre_chec_dag = rail.WaitForDagRunsSensor(
            task_id = "wait_trigger_pre_chec_dag",
            dag_runs="{{result('trigger_pre_chec_dag')}}",
            retries = 0,
            execution_timeout = timedelta(days=14)
        )

        get_philipines_data = rail.QueryCollectionOperator(
            task_id = "get_philipines_data",
            query=f"""SELECT * FROM raw_valid_records vr
                        WHERE vr.country = 'Philippines' and vr.companycode IN ("PHES", "PHET")
            """
        )

        has_any_philipines_data = rail.IfOperator(
            task_id = "has_any_philipines_data",
            test="{{result('get_philipines_data', 'length') > 0}}",
            yes_task="create_phl_data_csv_file",
            no_task="get_hungary_data"
        )
        
        create_phl_data_csv_file = rail.WriteCSVFileOperator(
            task_id = "create_phl_data_csv_file",
            source="{{result('get_philipines_data')}}",
            header=['empid', 'pernerid', 'email', 'firstname', 'lastname', 'country', 'state', 'exempt', 'exempteffectivedate', 'employeetype', 'hiredate', 'gender', 'servicedate', 'termdate', 'status', 'onleave', 'companycode', 'companyname', 'areacode', 'areaname', 'subareacode', 'empgroupcode', 'empgroupname', 'empsubgroupcode', 'empsubgroupname', 'supervisorid', 'supervisordate', 'supervisorfname', 'supervisorlname', 'supervisoremail', 'paygroup', 'locationeffectivedate', 'homecountry', 'costcenter', 'costcentername', 'costcentereffectivedate', 'orgcode', 'orgname', 'workshift', 'workshifteffectivedate', 'joblevel', 'jobchangeeffectivedate', 'fte', 'ftepct', 'isia', 'iastartdate', 'iaenddate', 'rut', 'middlename', 'timetype', 'dob', 'managementlvl', 'ausjc', 'termsconditions', 'industrialinstrumentclassification', 'additionaldataeffectivedate', 'terminationreason', 'scheduledweeklyhours', 'assignmenttype', 'homestate', '_actual_country', '_actual_state', 'countrytouse', 'statetouse', 'Work_City', 'Marital_Status_Ind', 'Marital_Status_efft_dt'],
            row=lambda item: [
                item["empid"], item["pernerid"], item["email"], item["firstname"], item["lastname"], item["country"], item["state"],
                item["exempt"], item["exempteffectivedate"], item["employeetype"], item["hiredate"], item["gender"], item["servicedate"],
                item["termdate"], item["status"], item["onleave"], item["companycode"], item["companyname"], item["areacode"],
                item["areaname"], item["subareacode"], item["empgroupcode"], item["empgroupname"], item["empsubgroupcode"],
                item["empsubgroupname"], item["supervisorid"], item["supervisordate"], item["supervisorfname"], item["supervisorlname"],
                item["supervisoremail"], item["paygroup"], item["locationeffectivedate"], item["homecountry"], item["costcenter"],
                item["costcentername"], item["costcentereffectivedate"], item["orgcode"], item["orgname"], item["workshift"],
                item["workshifteffectivedate"], item["joblevel"], item["jobchangeeffectivedate"], item["fte"], item["ftepct"],
                item["isia"], item["iastartdate"], item["iaenddate"], item["rut"], item["middlename"], item["timetype"], item["dob"],
                item["managementlvl"], item["ausjc"], item["termsconditions"], item["industrialinstrumentclassification"],
                item["additionaldataeffectivedate"], item["terminationreason"], item["scheduledweeklyhours"], item["assignment_type"],
                item["home_state"], item["_actual_country"], item["_actual_state"], item["_country_to_use_for_query"],
                item["_state_to_use_for_query"], item["workcity"], item["marital_status_ind"], item["marital_status_efft_dt"]
            ]
        )

        upload_phl_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id = "upload_phl_data_to_sftp",
            content="{{result('create_phl_data_csv_file')}}",
            remote_filepath=config.philippines_file_path + "/PHL_{{ result('new_file_sensor') | file_name }}.csv"
        )

        get_hungary_data = rail.QueryCollectionOperator(
            task_id = "get_hungary_data",
            query=f"""SELECT * FROM raw_valid_records vr
                        WHERE vr.country = 'Hungary' and vr.companycode IN ("HU00", "HU00")
            """
        )

        has_any_hungary_data = rail.IfOperator(
            task_id = "has_any_hungary_data",
            test="{{result('get_hungary_data', 'length') > 0}}",
            yes_task="create_hun_data_csv_file",
            no_task="get_uki_csc_data"
        )
        
        create_hun_data_csv_file = rail.WriteCSVFileOperator(
            task_id = "create_hun_data_csv_file",
            source="{{result('get_hungary_data')}}",
            header=['empid', 'pernerid', 'email', 'firstname', 'lastname', 'country', 'state', 'exempt', 'exempteffectivedate', 'employeetype', 'hiredate', 'gender', 
                    'servicedate', 'termdate', 'status', 'onleave', 'companycode', 'companyname', 'areacode', 'areaname', 'subareacode', 'empgroupcode', 'empgroupname', 
                    'empsubgroupcode', 'empsubgroupname', 'supervisorid', 'supervisordate', 'supervisorfname', 'supervisorlname', 'supervisoremail', 'paygroup', 'locationeffectivedate', 
                    'homecountry', 'costcenter', 'costcentername', 'costcentereffectivedate', 'orgcode', 'orgname', 'workshift', 'workshifteffectivedate', 'joblevel', 'jobchangeeffectivedate', 
                    'fte', 'ftepct', 'isia', 'iastartdate', 'iaenddate', 'rut', 'middlename', 'timetype', 'dob', 'managementlvl', 'ausjc', 'termsconditions', 'industrialinstrumentclassification', 
                    'additionaldataeffectivedate', 'terminationreason', 'scheduledweeklyhours', 'assignmenttype', 'homestate', '_actual_country', '_actual_state', 'countrytouse', 'statetouse', 'Work_City', 'Marital_Status_Ind', 
                    'Marital_Status_efft_dt', 'Additional_Job_Classifications', 'Holiday_Schedule_Calendar', 'Employee_Representative_Status', 'Employee_Representative_Effective_Date'],
            row=lambda item: [
                item["empid"], item["pernerid"], item["email"], item["firstname"], item["lastname"], item["country"], item["state"],
                item["exempt"], item["exempteffectivedate"], item["employeetype"], item["hiredate"], item["gender"], item["servicedate"],
                item["termdate"], item["status"], item["onleave"], item["companycode"], item["companyname"], item["areacode"],
                item["areaname"], item["subareacode"], item["empgroupcode"], item["empgroupname"], item["empsubgroupcode"],
                item["empsubgroupname"], item["supervisorid"], item["supervisordate"], item["supervisorfname"], item["supervisorlname"],
                item["supervisoremail"], item["paygroup"], item["locationeffectivedate"], item["homecountry"], item["costcenter"],
                item["costcentername"], item["costcentereffectivedate"], item["orgcode"], item["orgname"], item["workshift"],
                item["workshifteffectivedate"], item["joblevel"], item["jobchangeeffectivedate"], item["fte"], item["ftepct"],
                item["isia"], item["iastartdate"], item["iaenddate"], item["rut"], item["middlename"], item["timetype"], item["dob"],
                item["managementlvl"], item["ausjc"], item["termsconditions"], item["industrialinstrumentclassification"],
                item["additionaldataeffectivedate"], item["terminationreason"], item["scheduledweeklyhours"], item["assignment_type"],
                item["home_state"], item["_actual_country"], item["_actual_state"], item["_country_to_use_for_query"],
                item["_state_to_use_for_query"], item["workcity"], item["marital_status_ind"], item["marital_status_efft_dt"], 
                item["Additional_Job_Classifications"], item["Holiday_Schedule_Calendar"], item["Employee_representative_indicator"], item["Employee_Representative_Effective_Date"]
            ]
        )

        upload_hun_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id = "upload_hun_data_to_sftp",
            content="{{result('create_hun_data_csv_file')}}",
            remote_filepath=config.hungary_file_path + "/HUN_{{ result('new_file_sensor') | file_name }}.csv"
        )

        get_uki_csc_data = rail.QueryCollectionOperator(
            task_id = "get_uki_csc_data",
            query=f"""SELECT * FROM raw_valid_records vr
                        WHERE vr.country IN ("United Kingdom", "Ireland") AND vr.companycode IN ("0201", "0290", "1627", "0250", "1629", "1639", "1631", "1630", "1628", "0237")
            """
        )

        has_any_uki_csc_data = rail.IfOperator(
            task_id = "has_any_uki_csc_data",
            test="{{result('get_uki_csc_data', 'length') > 0}}",
            yes_task="create_uki_csc_data_csv_file",
            no_task="get_uki_es_data"
        )

        create_uki_csc_data_csv_file = rail.WriteCSVFileOperator(
            task_id = "create_uki_csc_data_csv_file",
            source="{{result('get_uki_csc_data')}}",
            header=['empid', 'pernerid', 'email', 'firstname', 'lastname', 'country', 'state', 'exempt', 'exempteffectivedate', 'employeetype', 'hiredate', 'gender',
                    'servicedate', 'termdate', 'status', 'onleave', 'companycode', 'companyname', 'areacode', 'areaname', 'subareacode', 'empgroupcode', 'empgroupname',
                    'empsubgroupcode', 'empsubgroupname', 'supervisorid', 'supervisordate', 'supervisorfname', 'supervisorlname', 'supervisoremail', 'paygroup', 'locationeffectivedate',
                    'homecountry', 'costcenter', 'costcentername', 'costcentereffectivedate', 'orgcode', 'orgname', 'workshift', 'workshifteffectivedate', 'joblevel', 'jobchangeeffectivedate',
                    'fte', 'ftepct', 'isia', 'iastartdate', 'iaenddate', 'rut', 'middlename', 'timetype', 'dob', 'managementlvl', 'ausjc', 'termsconditions', 'industrialinstrumentclassification',
                    'additionaldataeffectivedate', 'terminationreason', 'scheduledweeklyhours', 'assignmenttype', 'homestate', '_actual_country', '_actual_state', 'countrytouse', 'statetouse', 'Work_City', 'Marital_Status_Ind',
                    'Marital_Status_efft_dt', 'Additional_Job_Classifications', 'Holiday_Schedule_Calendar', 'Employee_Representative_Status', 'Employee_Representative_Effective_Date', 'Default_Weekly_Hours'],
            row=lambda item: [
                item["empid"], item["pernerid"], item["email"], item["firstname"], item["lastname"], item["country"], item["state"],
                item["exempt"], item["exempteffectivedate"], item["employeetype"], item["hiredate"], item["gender"], item["servicedate"],
                item["termdate"], item["status"], item["onleave"], item["companycode"], item["companyname"], item["areacode"],
                item["areaname"], item["subareacode"], item["empgroupcode"], item["empgroupname"], item["empsubgroupcode"],
                item["empsubgroupname"], item["supervisorid"], item["supervisordate"], item["supervisorfname"], item["supervisorlname"],
                item["supervisoremail"], item["paygroup"], item["locationeffectivedate"], item["homecountry"], item["costcenter"],
                item["costcentername"], item["costcentereffectivedate"], item["orgcode"], item["orgname"], item["workshift"],
                item["workshifteffectivedate"], item["joblevel"], item["jobchangeeffectivedate"], item["fte"], item["ftepct"],
                item["isia"], item["iastartdate"], item["iaenddate"], item["rut"], item["middlename"], item["timetype"], item["dob"],
                item["managementlvl"], item["ausjc"], item["termsconditions"], item["industrialinstrumentclassification"],
                item["additionaldataeffectivedate"], item["terminationreason"], item["scheduledweeklyhours"], item["assignment_type"],
                item["home_state"], item["_actual_country"], item["_actual_state"], item["_country_to_use_for_query"],
                item["_state_to_use_for_query"], item["workcity"], item["marital_status_ind"], item["marital_status_efft_dt"],
                item["Additional_Job_Classifications"], item["Holiday_Schedule_Calendar"], item["Employee_representative_indicator"], item["Employee_Representative_Effective_Date"], item['Default_Weekly_Hours']
            ]
        )

        upload_uki_csc_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id = "upload_uki_csc_data_to_sftp",
            content="{{result('create_uki_csc_data_csv_file')}}",
            remote_filepath=config.uki_csc_file_path + "/UK_CSC_{{ result('new_file_sensor') | file_name }}.csv"
        )

        get_uki_es_data = rail.QueryCollectionOperator(
            task_id = "get_uki_es_data",
            query=f"""SELECT * FROM raw_valid_records vr
                        WHERE vr.country IN ("United Kingdom", "Ireland") and vr.companycode IN ("GBC5", "GBA5", "IEEU", "IEES")
            """
        )

        has_any_uki_es_data = rail.IfOperator(
            task_id = "has_any_uki_es_data",
            test="{{result('get_uki_es_data', 'length') > 0}}",
            yes_task="create_uki_es_data_csv_file",
            no_task="get_other_than_philipines_hungary_uki_data"
        )

        create_uki_es_data_csv_file = rail.WriteCSVFileOperator(
            task_id = "create_uki_es_data_csv_file",
            source="{{result('get_uki_es_data')}}",
            header=['empid', 'pernerid', 'email', 'firstname', 'lastname', 'country', 'state', 'exempt', 'exempteffectivedate', 'employeetype', 'hiredate', 'gender',
                    'servicedate', 'termdate', 'status', 'onleave', 'companycode', 'companyname', 'areacode', 'areaname', 'subareacode', 'empgroupcode', 'empgroupname',
                    'empsubgroupcode', 'empsubgroupname', 'supervisorid', 'supervisordate', 'supervisorfname', 'supervisorlname', 'supervisoremail', 'paygroup', 'locationeffectivedate',
                    'homecountry', 'costcenter', 'costcentername', 'costcentereffectivedate', 'orgcode', 'orgname', 'workshift', 'workshifteffectivedate', 'joblevel', 'jobchangeeffectivedate',
                    'fte', 'ftepct', 'isia', 'iastartdate', 'iaenddate', 'rut', 'middlename', 'timetype', 'dob', 'managementlvl', 'ausjc', 'termsconditions', 'industrialinstrumentclassification',
                    'additionaldataeffectivedate', 'terminationreason', 'scheduledweeklyhours', 'assignmenttype', 'homestate', '_actual_country', '_actual_state', 'countrytouse', 'statetouse', 'Work_City', 'Marital_Status_Ind',
                    'Marital_Status_efft_dt', 'Additional_Job_Classifications', 'Holiday_Schedule_Calendar', 'Employee_Representative_Status', 'Employee_Representative_Effective_Date', 'Default_Weekly_Hours'],
            row=lambda item: [
                item["empid"], item["pernerid"], item["email"], item["firstname"], item["lastname"], item["country"], item["state"],
                item["exempt"], item["exempteffectivedate"], item["employeetype"], item["hiredate"], item["gender"], item["servicedate"],
                item["termdate"], item["status"], item["onleave"], item["companycode"], item["companyname"], item["areacode"],
                item["areaname"], item["subareacode"], item["empgroupcode"], item["empgroupname"], item["empsubgroupcode"],
                item["empsubgroupname"], item["supervisorid"], item["supervisordate"], item["supervisorfname"], item["supervisorlname"],
                item["supervisoremail"], item["paygroup"], item["locationeffectivedate"], item["homecountry"], item["costcenter"],
                item["costcentername"], item["costcentereffectivedate"], item["orgcode"], item["orgname"], item["workshift"],
                item["workshifteffectivedate"], item["joblevel"], item["jobchangeeffectivedate"], item["fte"], item["ftepct"],
                item["isia"], item["iastartdate"], item["iaenddate"], item["rut"], item["middlename"], item["timetype"], item["dob"],
                item["managementlvl"], item["ausjc"], item["termsconditions"], item["industrialinstrumentclassification"],
                item["additionaldataeffectivedate"], item["terminationreason"], item["scheduledweeklyhours"], item["assignment_type"],
                item["home_state"], item["_actual_country"], item["_actual_state"], item["_country_to_use_for_query"],
                item["_state_to_use_for_query"], item["workcity"], item["marital_status_ind"], item["marital_status_efft_dt"],
                item["Additional_Job_Classifications"], item["Holiday_Schedule_Calendar"], item["Employee_representative_indicator"], item["Employee_Representative_Effective_Date"], item['Default_Weekly_Hours']
            ]
        )

        upload_uki_es_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id = "upload_uki_es_data_to_sftp",
            content="{{result('create_uki_es_data_csv_file')}}",
            remote_filepath=config.uki_es_file_path + "/UK_ES_{{ result('new_file_sensor') | file_name }}.csv"
        )

        get_other_than_philipines_hungary_uki_data = rail.QueryCollectionOperator(
            task_id = "get_other_than_philipines_hungary_uki_data",
            query=f"""SELECT * FROM raw_valid_records vr
                       WHERE NOT (vr.country = 'Philippines' and vr.companycode IN ("PHES", "PHET")) AND
                              NOT (vr.country = 'Hungary' and vr.companycode IN ("HU00", "HU00")) AND
                              NOT (vr.country IN ("United Kingdom", "Ireland") AND vr.companycode IN ("0201", "0290", "1627", "0250", "1629", "1639", "1631", "1630", "1628", "0237")) AND
                              NOT (vr.country IN ("United Kingdom", "Ireland") and vr.companycode IN ("GBC5", "GBA5", "IEEU", "IEES"))
            """,
            name="valid_records"
        )


        get_starting_balance_script = rail.RepliconServiceOperator(
            task_id="get_starting_balance_script",
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data_handler=response_filter.get_starting_balance_script_data_handler
        )

        get_prevent_balance_overdraw_script = rail.RepliconServiceOperator(
            task_id="get_prevent_balance_overdraw_script",
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",
            data_handler=response_filter.get_prevent_balance_overdraw_script_data_handler
        )
        
        def _get_all_company_codes_from_mapper():
            mapper_company_code_data = list(filter(lambda row: row['Type'] == "Company Code" and row['Function'] == "Workday User Sync",
                         config.DXC_WORKDAY_USER_SYNC_USER_MAPPER))
            # for any new region a new key needs to be added in below
            final_data = {
                "c1" : [],
                "compass": [],
                "gsap":[],
                "ftp": [],
                "non_live": []
            }

            for item in mapper_company_code_data:
                parent = item['Source'].lower()
                if parent not in final_data:
                    parent = "non_live"
                final_data[parent] = final_data[parent] + [item['URI']]

            rail.set_result(key = "final_data", val = final_data)
            rail.set_result(key = "c1_company_code_data", val = str(tuple(final_data['c1'])))
            rail.set_result(key = "compass_company_code_data", val = str(tuple(final_data['compass'])))
            rail.set_result(key = "gsap_company_code_data", val = str(tuple(final_data['gsap'])))
            rail.set_result(key = "ftp_company_code_data", val = str(tuple(final_data['ftp'])))
            rail.set_result(key = "non_live_company_code_data", val = str(tuple(final_data['non_live'])))
            rail.set_result(key = "all_allowed_company_code_data", val = str(tuple(final_data["ftp"]+final_data["c1"]+final_data["compass"]+final_data["gsap"])))
            return mapper_company_code_data

        get_all_company_codes_from_mapper = rail.PythonOperator(
            task_id = "get_all_company_codes_from_mapper",
            python_callable=_get_all_company_codes_from_mapper
        )

        get_gbl_data_new = rail.QueryCollectionOperator(
            task_id = "get_gbl_data_new",
            query=f"""SELECT * FROM valid_records vr
                        WHERE
                        (LOWER(COALESCE(vr._parent_company_code, '')) == 'ftp')
                        OR
                        (LOWER(vr._country_to_use_for_query) NOT IN ('united states of america', 'puerto rico', 'india', 'portugal', 'costa rica', 'australia', 'canada', 'philippines', 'hungary', 'united kingdom', 'ireland'))
                        OR
                        (LOWER(COALESCE(vr._country_to_use_for_query, '')) == 'philippines' AND COALESCE(vr.companycode, '') NOT IN ("PHES", "PHET"))
                        OR
                        (LOWER(COALESCE(vr._country_to_use_for_query, '')) == 'hungary' AND COALESCE(vr.companycode, '') NOT IN ("HU00"))
                        OR
                        (COALESCE(vr._country_to_use_for_query, '') IN ("United Kingdom", "Ireland") AND COALESCE(vr.companycode, '') NOT IN ("0201", "0290", "1627", "0250", "1629", "1639", "1631", "1630", "1628", "0237", "GBC5", "GBA5", "IEEU", "IEES"))
                        OR
                        (LOWER(vr._country_to_use_for_query) == 'australia' AND LOWER(COALESCE(vr._parent_company_code, '')) == 'gsap' AND vr.companycode NOT IN {config.gsap_company_codes_lcsc})
                        OR
                        (LOWER(vr._country_to_use_for_query) == 'australia' AND LOWER(COALESCE(vr._parent_company_code, '')) NOT IN ('gsap', 'compass'))
                        OR
                        (LOWER(vr._country_to_use_for_query) == 'united states of america' AND LOWER(COALESCE(vr._parent_company_code, '')) NOT IN ('c1', 'compass'))
                        OR
                        (LOWER(vr._country_to_use_for_query) == 'puerto rico' AND LOWER(COALESCE(vr._parent_company_code, '')) != 'c1')
                        OR
                        (LOWER(vr._country_to_use_for_query) == 'canada' AND LOWER(COALESCE(vr._parent_company_code, '')) != 'c1')
                        OR
                        (LOWER(vr._country_to_use_for_query) == 'costa rica' AND LOWER(COALESCE(vr._parent_company_code, '')) != 'compass')
                        OR
                        (LOWER(vr._country_to_use_for_query) == 'india' AND LOWER(COALESCE(vr._parent_company_code, '')) != 'compass')
                        OR
                        (LOWER(vr._country_to_use_for_query) == 'portugal' AND LOWER(COALESCE(vr._parent_company_code, '')) != 'compass')
                        """,
            name = "splitter_gbl_union_global_data_new"
        )

        has_any_data_to_trigger_for_gbl = rail.IfOperator(
            task_id = "has_any_data_to_trigger_for_gbl",
            test="{{result('get_gbl_data_new', 'length') > 0}}",
            yes_task="trigger_glb_processing_dag",
            no_task="gather_run_ids"
        )

        trigger_glb_processing_dag = rail.TriggerDagRunOperator(
            task_id = "trigger_glb_processing_dag",
            trigger_dag_id=config.workday_user_import_process_gbl_data_child_dag,
            conf = lambda : {
                "file_name": path.split(rail.result("new_file_sensor"))[1],
                "starting_balance_set_to_uri": rail.result("get_starting_balance_script"),
                "prevent_balance_overdraw_uri": rail.result("get_prevent_balance_overdraw_script"),
                "supervisor_user_log": rail.result("create_supervisor_log")
            }
        )

        supervisor_log_has_any_data = rail.IfOperator(
            task_id = "supervisor_log_has_any_data",
            test=lambda: len(rail.load_all_records(rail.result('create_supervisor_log'))) > 0,
            yes_task="process_supervisor_assignment_start",
            no_task="gather_logs"
        )

        process_supervisor_assignment_start = rail.EmptyOperator(
            task_id = "process_supervisor_assignment_start"
        )

        get_all_employeegroup_data = rail.RepliconServiceOperator(
            task_id= "get_all_employeegroup_data",
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data=request_payload.get_all_employeegroup_payload,
            data_handler=response_filter.get_employeegroup_response_filter
        )

        get_all_companycode_data = rail.RepliconServiceOperator(
            task_id= "get_all_companycode_data",
            endpoint="/services/DivisionListService1.svc/GetData",
            data=request_payload.get_all_companycode_payload,
            data_handler=response_filter.get_companycode_response_filter
        )

        @lru_cache(maxsize=8)
        def get_supervisor_assignment_data():
            return {
                    "employee_type_data":rail.write_json_artifact(rail.result("get_all_employeegroup_data")),
                    "division_data": rail.write_json_artifact(rail.result("get_all_companycode_data")),
                    "schedule_manager_permission": list(filter(lambda row: row['Type']=="Supervisor Scheduler Permission", config.MAPPER))[0]['Value'] if list(filter(lambda row: row['Type']=="Supervisor Scheduler Permission", config.MAPPER)) else ""
                }

        process_supervisor_assignment = rail.trigger_parallel_dagrun(
            task_id = "process_supervisor_assignment",
            items="{{result('create_supervisor_log')}}",
            trigger_dag_id=config.workday_user_import_process_supervisor_assignment,
            parallel_count=10,
            execution_timeout = timedelta(days=14),
            conf=lambda item: {
                **get_supervisor_assignment_data(),
                **{
                    "file_name": path.split(rail.result("new_file_sensor"))[1],
                    "user_uri": item['properties']["user_uri|country"].split('|')[0],
                },
                **item['properties']
            }

        )

        process_supervisor_assignment_end = rail.EmptyOperator(
            task_id = "process_supervisor_assignment_end"
        )

        gather_logs = rail.EmptyOperator(
            task_id = "gather_logs"
        )

        has_any_data_to_trigger_for_australia = rail.IfOperator(
            task_id = "has_any_data_to_trigger_for_australia",
            test="{{result('combine_australia_data', 'length') > 0}}",
            yes_task="trigger_gsap_processing_dag",
            no_task="gather_run_ids"
        )

        combine_australia_data = rail.QueryCollectionOperator(
            task_id = "combine_australia_data",
            query = f"""SELECT *, 'GSAP_AUS' as _origin
                    FROM valid_records vr
                    WHERE
                        (LOWER(vr._country_to_use_for_query) == 'australia' AND LOWER(vr._parent_company_code) == 'compass')
                        OR
                        (LOWER(vr._country_to_use_for_query) == 'australia' AND LOWER(vr._parent_company_code) == 'gsap' AND vr.companycode IN {config.gsap_company_codes_lcsc})
                    """,
            name = "splitter_gsap_all_data"
        )


        trigger_gsap_processing_dag = rail.TriggerDagRunOperator(
            task_id = "trigger_gsap_processing_dag",
            trigger_dag_id=config.workday_user_import_process_gsap_data_child_dag,
            conf = lambda : {
                "file_name": path.split(rail.result("new_file_sensor"))[1],
                "starting_balance_set_to_uri": rail.result("get_starting_balance_script"),
                "prevent_balance_overdraw_uri": rail.result("get_prevent_balance_overdraw_script"),
                "supervisor_user_log": rail.result("create_supervisor_log")
            }
        )

        get_c1_usa_csc_data = rail.QueryCollectionOperator(
            task_id = "get_c1_usa_csc_data",
            query = f"""SELECT * FROM valid_records vr
                        WHERE 
                        (LOWER(vr._country_to_use_for_query) == "united states of america" AND LOWER(_parent_company_code) == "c1")
                        OR
                        (LOWER(vr._country_to_use_for_query) == "puerto rico" AND LOWER(_parent_company_code) == "c1")
                    """
        )

        has_any_data_to_trigger_for_usa_csc = rail.IfOperator(
            task_id = "has_any_data_to_trigger_for_usa_csc",
            test="{{result('get_c1_usa_csc_data', 'length') > 0}}",
            yes_task="trigger_usa_csc_dag",
            no_task="gather_run_ids"
        )
        
        trigger_usa_csc_dag = rail.TriggerDagRunOperator(
            task_id = "trigger_usa_csc_dag",
            trigger_dag_id=config.workday_user_import_process_usa_csc_data_child_dag,
            conf = lambda : {
                "file_name": path.split(rail.result("new_file_sensor"))[1],
                "starting_balance_set_to_uri": rail.result("get_starting_balance_script"),
                "prevent_balance_overdraw_uri": rail.result("get_prevent_balance_overdraw_script"),
                "supervisor_user_log": rail.result("create_supervisor_log"),
                "compass_usa_csc_data": rail.result("get_c1_usa_csc_data")
            }
        )

        get_c1_canada_data = rail.QueryCollectionOperator(
            task_id = "get_c1_canada_data",
            query = """SELECT * FROM valid_records vr WHERE 
                        (LOWER(vr._country_to_use_for_query) == "canada" AND LOWER(_parent_company_code) == "c1")""",
            name = "c1_canada_data"
        )

        has_any_data_to_trigger_for_canada = rail.IfOperator(
            task_id = "has_any_data_to_trigger_for_canada",
            test="{{result('get_c1_canada_data', 'length') > 0}}",
            yes_task="trigger_canada_processing_dag",
            no_task="gather_run_ids"
        )

        trigger_canada_processing_dag = rail.TriggerDagRunOperator(
            task_id = "trigger_canada_processing_dag",
            trigger_dag_id=config.workday_user_import_process_canada_data_child_dag,
            conf = lambda : {
                "file_name": path.split(rail.result("new_file_sensor"))[1],
                "starting_balance_set_to_uri": rail.result("get_starting_balance_script"),
                "prevent_balance_overdraw_uri": rail.result("get_prevent_balance_overdraw_script"),
                "supervisor_user_log": rail.result("create_supervisor_log")
            }
        )

        get_compass_portugal_data = rail.QueryCollectionOperator(
            task_id = "get_compass_portugal_data",
            query = """SELECT * FROM valid_records vr WHERE 
                        (LOWER(vr._country_to_use_for_query) == "portugal" AND LOWER(_parent_company_code) == "compass")""",
            name = "compass_portugal_data"
        )

        has_any_data_to_trigger_for_portugal = rail.IfOperator(
            task_id = "has_any_data_to_trigger_for_portugal",
            test="{{result('get_compass_portugal_data', 'length') > 0}}",
            yes_task="trigger_portugal_processing_dag",
            no_task="gather_run_ids"
        )

        trigger_portugal_processing_dag = rail.TriggerDagRunOperator(
            task_id = "trigger_portugal_processing_dag",
            trigger_dag_id=config.workday_user_import_process_portugal_data_child_dag,
            conf = lambda : {
                "file_name": path.split(rail.result("new_file_sensor"))[1],
                "starting_balance_set_to_uri": rail.result("get_starting_balance_script"),
                "prevent_balance_overdraw_uri": rail.result("get_prevent_balance_overdraw_script"),
                "supervisor_user_log": rail.result("create_supervisor_log")
            }
        )

        #costa rica
        get_compass_costa_rica_data = rail.QueryCollectionOperator(
            task_id = "get_compass_costa_rica_data",
            query = """SELECT * FROM valid_records vr WHERE 
                        (LOWER(vr._country_to_use_for_query) == "costa rica" AND LOWER(_parent_company_code) == "compass")""",
            name = "compass_costa_rica_data"
        )

        has_any_data_to_trigger_for_costa_rica = rail.IfOperator(
            task_id = "has_any_data_to_trigger_for_costa_rica",
            test="{{result('get_compass_costa_rica_data', 'length') > 0}}",
            yes_task="trigger_costa_rica_dag",
            no_task="gather_run_ids"
        )

        trigger_costa_rica_dag = rail.TriggerDagRunOperator(
            task_id = "trigger_costa_rica_dag",
            trigger_dag_id=config.workday_user_import_process_costa_rica_data_child_dag,
            conf = lambda : {
                "file_name": path.split(rail.result("new_file_sensor"))[1],
                "starting_balance_set_to_uri": rail.result("get_starting_balance_script"),
                "prevent_balance_overdraw_uri": rail.result("get_prevent_balance_overdraw_script"),
                "supervisor_user_log": rail.result("create_supervisor_log"),
                "costa_rica_users_data": rail.result("get_compass_costa_rica_data")
            }
        )

        get_compass_usa_les_data = rail.QueryCollectionOperator(
            task_id = "get_compass_usa_les_data",
            query = """SELECT * FROM valid_records vr WHERE 
                        (LOWER(vr._country_to_use_for_query) == "united states of america" AND LOWER(_parent_company_code) == "compass")""",
            name = "compass_usa_les_data"
        )

        has_any_data_to_trigger_for_usa_les = rail.IfOperator(
            task_id = "has_any_data_to_trigger_for_usa_les",
            test="{{result('get_compass_usa_les_data', 'length') > 0}}",
            yes_task="trigger_usa_les_dag",
            no_task="gather_run_ids"
        )

        trigger_usa_les_dag = rail.TriggerDagRunOperator(
            task_id = "trigger_usa_les_dag",
            trigger_dag_id=config.workday_user_import_process_usa_les_data_child_dag,
            conf = lambda : {
                "file_name": path.split(rail.result("new_file_sensor"))[1],
                "starting_balance_set_to_uri": rail.result("get_starting_balance_script"),
                "prevent_balance_overdraw_uri": rail.result("get_prevent_balance_overdraw_script"),
                "supervisor_user_log": rail.result("create_supervisor_log"),
                "compass_usa_les_data": rail.result("get_compass_usa_les_data")
            }
        )

        get_compass_india_data = rail.QueryCollectionOperator(
            task_id = "get_compass_india_data",
            query = """SELECT * FROM valid_records vr WHERE 
                        (LOWER(vr._country_to_use_for_query) == "india" AND LOWER(_parent_company_code) == "compass")""",
            name = "compass_india_data"
        )

        has_any_data_to_trigger_for_india = rail.IfOperator(
            task_id = "has_any_data_to_trigger_for_india",
            test="{{result('get_compass_india_data', 'length') > 0}}",
            yes_task="trigger_india_dag",
            no_task="gather_run_ids"
        )

        trigger_india_dag = rail.TriggerDagRunOperator(
            task_id = "trigger_india_dag",
            trigger_dag_id=config.workday_user_import_process_india_data_child_dag,
            conf = lambda : {
                "file_name": path.split(rail.result("new_file_sensor"))[1],
                "starting_balance_set_to_uri": rail.result("get_starting_balance_script"),
                "prevent_balance_overdraw_uri": rail.result("get_prevent_balance_overdraw_script"),
                "supervisor_user_log": rail.result("create_supervisor_log"),
                "india_users_data": rail.result("get_compass_india_data")
            }
        )

        def gather_run_ids_callable():
            run_ids = []
            # Portugal
            if rail.result(trigger_portugal_processing_dag.task_id):
                run_ids.append(rail.result(trigger_portugal_processing_dag.task_id))
            # Canada
            if rail.result(trigger_canada_processing_dag.task_id):
                run_ids.append(rail.result(trigger_canada_processing_dag.task_id))
            # Costa Rica
            if rail.result(trigger_costa_rica_dag.task_id):
                run_ids.append(rail.result(trigger_costa_rica_dag.task_id))
            # USA LES
            if rail.result(trigger_usa_les_dag.task_id):
                run_ids.append(rail.result(trigger_usa_les_dag.task_id))
            # Australia (GSAP and Compass)
            if rail.result(trigger_gsap_processing_dag.task_id):
                run_ids.append(rail.result(trigger_gsap_processing_dag.task_id))
            # All default
            if rail.result(trigger_glb_processing_dag.task_id):
                run_ids.append(rail.result(trigger_glb_processing_dag.task_id))
            # India
            if rail.result(trigger_india_dag.task_id):
                run_ids.append(rail.result(trigger_india_dag.task_id))
            # USA CSC
            if rail.result(trigger_usa_csc_dag.task_id):
                run_ids.append(rail.result(trigger_usa_csc_dag.task_id))
            return run_ids

        gather_run_ids = rail.PythonOperator(
            task_id = "gather_run_ids",
            python_callable=gather_run_ids_callable
        )

        wait_for_complition = rail.WaitForDagRunsSensor(
            task_id = "wait_for_complition",
            dag_runs="{{result('gather_run_ids')}}",
            retries = 0,
            execution_timeout = timedelta(days=14)
        )

        gather_all_logs = rail.GatherResultsFromDagRunsOperator(
            task_id = "gather_all_logs",
            dagrun_task_id = "gather_all_logs",
            dag_runs="{{result('gather_run_ids')}}",
            flatten= True
        )

        trigger_log_generation = rail.TriggerDagRunOperator(
            task_id = "trigger_log_generation",
            trigger_dag_id=config.process_log_generation_dagid,
            conf=lambda: {
                "file_name": path.split(rail.result("new_file_sensor"))[1],
                "exception_log": rail.result("create_log"),
                'logs': rail.result("gather_all_logs"),
                "total_record_count": rail.result("create_input_collection", "length") if rail.result("create_input_collection", "length") else 0,
                "skipped_in_validation": rail.result("query_invalid_records", "length") if rail.result("query_invalid_records", "length") else 0,
                "log_filename": rail.render_template("/log_{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_base }}.csv")
            }
        )

        new_file_sensor >> is_csv >> rail.Label("No") >> send_bad_file_format_email
        is_csv >> rail.Label("Yes") >> download_file >> was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun

        download_file >> can_decrypt_file >> rail.Label("Yes") >> decrypt_file >> upload_decrypted_file_to_sftp >> dummy_load_data >> create_log
        can_decrypt_file >> rail.Label("No") >> dummy_load_data

        create_log >> create_supervisor_log >> load_data >> create_input_collection >> updated_country_state_based_on_ia >> create_input_collection2 >> get_users_for_allowed_locations >> has_any_data >> rail.Label("No") >> send_blank_file_email
        has_any_data >> rail.Label("Yes") >> query_invalid_records >> log_invalid_records >> create_mapper_collection >> query_valid_records >> has_any_valid_data

        has_any_valid_data >> rail.Label("No") >> gather_logs
        has_any_valid_data >> rail.Label("Yes") >> start_pre_check_processing >> get_all_user_custom_fields >> trigger_pre_chec_dag >> wait_trigger_pre_chec_dag

        wait_trigger_pre_chec_dag  >> get_philipines_data >> has_any_philipines_data >> rail.Label("Yes") >> create_phl_data_csv_file >> upload_phl_data_to_sftp >> get_hungary_data
        has_any_philipines_data >> rail.Label("No") >> get_hungary_data
        get_hungary_data >> has_any_hungary_data >> rail.Label("Yes") >> create_hun_data_csv_file >> upload_hun_data_to_sftp >> get_uki_csc_data
        has_any_hungary_data >> rail.Label("No") >> get_uki_csc_data >> has_any_uki_csc_data

        has_any_uki_csc_data >> rail.Label("Yes") >> create_uki_csc_data_csv_file >> upload_uki_csc_data_to_sftp >> get_uki_es_data
        has_any_uki_csc_data >> rail.Label("No") >> get_uki_es_data >> has_any_uki_es_data

        has_any_uki_es_data >> rail.Label("Yes") >> create_uki_es_data_csv_file >> upload_uki_es_data_to_sftp >> get_other_than_philipines_hungary_uki_data
        has_any_uki_es_data >> rail.Label("No") >> get_other_than_philipines_hungary_uki_data

        get_other_than_philipines_hungary_uki_data >> get_all_company_codes_from_mapper >> get_gbl_data_new \
            >> get_prevent_balance_overdraw_script >> get_starting_balance_script

        get_starting_balance_script >> has_any_data_to_trigger_for_gbl >> rail.Label("Yes") >> trigger_glb_processing_dag >> gather_run_ids
        has_any_data_to_trigger_for_gbl >> rail.Label("No") >> gather_run_ids

        get_starting_balance_script >> combine_australia_data >> has_any_data_to_trigger_for_australia >> rail.Label(
            "Yes") >> trigger_gsap_processing_dag >> gather_run_ids
        has_any_data_to_trigger_for_australia >> rail.Label("No") >> gather_run_ids

        get_starting_balance_script >> get_c1_usa_csc_data >> has_any_data_to_trigger_for_usa_csc >> rail.Label(
            "Yes")>> trigger_usa_csc_dag >> gather_run_ids

        get_starting_balance_script >> get_c1_usa_csc_data >> has_any_data_to_trigger_for_usa_csc >> rail.Label(
            "Yes")>> trigger_usa_csc_dag >> gather_run_ids
        has_any_data_to_trigger_for_usa_csc >> rail.Label("No") >> gather_run_ids

        get_starting_balance_script >> get_c1_canada_data >> has_any_data_to_trigger_for_canada >> rail.Label("Yes") >> trigger_canada_processing_dag >> gather_run_ids
        has_any_data_to_trigger_for_canada >> rail.Label("No") >> gather_run_ids
    
        get_starting_balance_script >> get_compass_portugal_data >> has_any_data_to_trigger_for_portugal >> rail.Label("Yes") >> trigger_portugal_processing_dag >> gather_run_ids
        has_any_data_to_trigger_for_portugal >> rail.Label("No") >> gather_run_ids

        get_starting_balance_script >> get_compass_costa_rica_data >> has_any_data_to_trigger_for_costa_rica >> rail.Label("Yes") >> trigger_costa_rica_dag >> gather_run_ids
        has_any_data_to_trigger_for_costa_rica >> rail.Label("No") >> gather_run_ids

        get_starting_balance_script >> get_compass_usa_les_data >> has_any_data_to_trigger_for_usa_les >> rail.Label("Yes") >> trigger_usa_les_dag >> gather_run_ids
        has_any_data_to_trigger_for_usa_les >> rail.Label("No") >> gather_run_ids

        get_starting_balance_script >> get_compass_india_data >> has_any_data_to_trigger_for_india >> rail.Label("Yes") >> trigger_india_dag >> gather_run_ids
        has_any_data_to_trigger_for_india >> rail.Label("No") >> gather_run_ids

        gather_run_ids >> wait_for_complition >> supervisor_log_has_any_data >> rail.Label("Yes") >> process_supervisor_assignment_start \
            >> get_all_employeegroup_data >> get_all_companycode_data >> process_supervisor_assignment >> process_supervisor_assignment_end >> gather_logs

        supervisor_log_has_any_data >> rail.Label("No") >> gather_logs >> gather_all_logs >> trigger_log_generation

        gather_run_ids >> wait_for_complition >> supervisor_log_has_any_data

    return dag

rail.for_each_instance(create_dag)
