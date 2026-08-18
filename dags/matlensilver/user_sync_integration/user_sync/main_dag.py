from datetime import timedelta, datetime
import rail
from matlensilver.user_sync_integration.user_sync.tasks.send_logs import get_send_logs
from matlensilver.user_sync_integration.user_sync.utils import request_payload
from matlensilver.user_sync_integration.user_sync.utils import response_filter
from matlensilver.user_sync_integration.user_sync.tasks.process_departments import process_departments_task_group
from matlensilver.user_sync_integration.user_sync.tasks.process_employeetypes import process_employeetypes_task_group
from matlensilver.user_sync_integration.user_sync.tasks.process_locations import process_locations_task_group


# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'matlen_silver_user_sync_master_{config.instance}',
        description='Matlen_Silver User Sync Master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
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
            bcc=config.internal_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | User Sync - Incorrect Format - {{ current_time_in_specified_tz() }}',
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
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        load_data = rail.LoadCSVFileOperator(
            task_id='load_data',
            document="{{ result('download_file') }}",
            encoding='utf-8-sig'
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source="{{ result('load_data') }}",
            name="inputdatacollection",
            columns={
                'PersonID': 'personid',
                'FirstName': 'firstname',
                'LastName': 'lastname',
                'MiddleName': 'middlename',
                'Supervisor Name': 'supervisorname',
                'Supervisor Employee Code': 'supervisorcode',
                'Home State': 'homestate',
                'Home Zip': 'homezip',
                'HomeAddress': 'homeaddress',
                'HomeCity': 'homecity',
                'timesheetemail': 'timesheetemail',
                'Homephone': 'homephone',
                'Cellphone': 'cellphone',
                'EmergencyContactNumber': 'emergencycontactnumber',
                'EmergencyContactFirstName': 'emergencycontactfirstname',
                'EmergencyContactLastName': 'emergencycontactlastname',
                'EmergencyContactRelationship': 'emergencycontactrelationship',
                'Originalstartdate@matlen': 'originalstartdate',
                'Lastassignmentenddate': 'lastassignmentenddate',
                'EmployeeType': 'employeetype',
                'EmployeeTypeCode': 'employeetypecode',
                'Department': 'departmentname',
                'DepartmentCode': 'departmentcode',
                'WorkLocation': 'worklocation',
                'AssignmentWorkStreet1&2': 'workstreet',
                'AssignmentWorkCity': 'workcity',
                'AssignmentWorkState': 'workstate',
                'AssignmentWorkzip': 'workzip',
                'TimeZone': 'timezone',
                'BenefitAnniversaryDate': 'benefitanniversarydate',
                'ADPID': 'adpid',
                'Net Bill Rate': 'netbillrate',
                'Hourly Pay Rate': 'hourlypayrate',
                'Burden': 'burden',
                'Workweek': 'workweek',
                'Holiday Calendar': 'holidaycalender'
            }
        )

        has_input_data = rail.IfOperator(
            task_id='has_input_data',
            test="{{ result('create_input_data_collection','length') > 0 }}",
            yes_task='create_md5',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | User Sync - no records in file - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html"
        )

        create_md5 = rail.DataAdaptorOperator(
            task_id="create_md5",
            source="{{result('create_input_data_collection')}}",
            columns=['personid', 'firstname', 'lastname', 'middlename', 'supervisorname', 'supervisorcode', 'homestate', 'homezip',
                     'homeaddress', 'homecity', 'timesheetemail', 'homephone', 'cellphone', 'emergencycontactnumber', 'emergencycontactfirstname',
                     'emergencycontactlastname', 'emergencycontactrelationship', 'originalstartdate', 'lastassignmentenddate',
                     'employeetype', 'employeetypecode','departmentname', 'departmentcode', 'worklocation', 'workstreet',
                     'workcity', 'workstate', 'workzip', 'timezone', 'benefitanniversarydate',
                     'adpid', 'netbillrate', 'hourlypayrate', 'burden', 'workweek', 'holidaycalender', 'md5'],
            data=request_payload.get_create_md5_data
        )

        input_data_with_md5 = rail.CreateCollectionOperator(
            task_id="input_data_with_md5",
            name="input_data",
            source="{{result('create_md5')}}"
        )

        get_reference_file = rail.SFTPDownloadFileOperator(
            task_id="get_reference_file",
            remote_filepath=config.reference_file,
        )

        parse_reference_file = rail.LoadCSVFileOperator(
            task_id="parse_reference_file",
            document="{{result('get_reference_file')}}",
        )

        create_reference_data_collection = rail.CreateCollectionOperator(
            task_id="create_reference_data_collection",
            name="reference_data",
            source="{{result('parse_reference_file')}}"
        )

        get_delta_records = rail.QueryCollectionOperator(
            task_id="get_delta_records",
            query="""SELECT * FROM input_data WHERE md5 NOT IN (SELECT DISTINCT md5 FROM reference_data)"""
        )

        has_any_changed_records = rail.IfOperator(
            task_id="has_any_changed_records",
            test="{{result('get_delta_records', 'length') > 0}}",
            yes_task=['query_valid_records', 'query_invalid_records'],
            no_task="no_changed_records"
        )

        no_changed_records = rail.EmptyOperator(
            task_id='no_changed_records'
        )

        get_unchanged_records = rail.QueryCollectionOperator(
            task_id="get_unchanged_records",
            query="""SELECT * FROM input_data WHERE md5 IN (SELECT DISTINCT md5 FROM reference_data)"""
        )

        has_any_unchanged_records = rail.IfOperator(
            task_id="has_any_unchanged_records",
            test="{{result('get_unchanged_records', 'length') > 0}}",
            yes_task="log_unchanged_records",
            no_task="no_unchanged_records_present"
        )

        log_unchanged_records = rail.WriteLogOperator(
            task_id="log_unchanged_records",
            items="{{result('get_unchanged_records')}}",
            message="No changes Recieved",
            severity="Skipped",
            properties=lambda item: {
                "employeeid": item['personid'],
                "firstname": item['firstname'],
                "lastname": item['lastname'],
                'status': "Skipped",
            }
        )

        no_unchanged_records_present = rail.EmptyOperator(
            task_id='no_unchanged_records_present'
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            name='validrecords',
            query="""SELECT * FROM get_delta_records WHERE NULLIF(personid, '') IS NOT NULL and
                    NULLIF(firstname, '') IS NOT NULL and NULLIF(lastname, '') IS NOT NULL and NULLIF(homestate, '') IS NOT NULL and
                    NULLIF(homestate, '') IS NOT NULL and NULLIF(homezip, '') IS NOT NULL and NULLIF(homeaddress, '') IS NOT NULL and
                    NULLIF(homecity, '') IS NOT NULL and NULLIF(timesheetemail, '') IS NOT NULL and NULLIF(originalstartdate, '') IS NOT NULL and
                    NULLIF(employeetype, '') IS NOT NULL and NULLIF(employeetypecode, '') IS NOT NULL and NULLIF(worklocation, '') IS NOT NULL and
                    NULLIF(workcity, '') IS NOT NULL and NULLIF(workstate, '') IS NOT NULL and NULLIF(workzip, '') IS NOT NULL and
                    NULLIF(timezone, '') IS NOT NULL and NULLIF(benefitanniversarydate, '') IS NOT NULL and NULLIF(workweek, '') IS NOT NULL"""
        )

        has_valid_records = rail.IfOperator(
            task_id="has_valid_records",
            test="{{result('query_valid_records', 'length') > 0}}",
            yes_task=['load_valid_departments_data',
                      'load_valid_locations_data', 'load_valid_employeetype_data'],
            no_task="no_valid_records_present"
        )

        no_valid_records_present = rail.EmptyOperator(
            task_id='no_valid_records_present'
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            name='invalidrecords',
            query="""SELECT * FROM get_delta_records WHERE NULLIF(personid, '') IS NULL or
                    NULLIF(firstname, '') IS NULL or NULLIF(lastname, '') IS NULL or NULLIF(homestate, '') IS NULL or
                    NULLIF(homestate, '') IS NULL or NULLIF(homezip, '') IS NULL or NULLIF(homeaddress, '') IS NULL or
                    NULLIF(homecity, '') IS NULL or NULLIF(timesheetemail, '') IS NULL or NULLIF(originalstartdate, '') IS NULL or
                    NULLIF(employeetype, '') IS NULL or NULLIF(employeetypecode, '') IS NULL or NULLIF(worklocation, '') IS NULL or
                    NULLIF(workcity, '') IS NULL or NULLIF(workstate, '') IS NULL or NULLIF(workzip, '') IS NULL or
                    NULLIF(timezone, '') IS NULL or NULLIF(benefitanniversarydate, '') IS NULL or NULLIF(workweek, '') IS NULL"""
        )

        has_invalid_records = rail.IfOperator(
            task_id="has_invalid_records",
            test="{{result('query_invalid_records', 'length') > 0}}",
            yes_task="log_invalid_records",
            no_task="no_invalid_records_present"
        )

        no_invalid_records_present = rail.EmptyOperator(
            task_id='no_invalid_records_present'
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            items='{{result("query_invalid_records")}}',
            message='Mandatory fields are Missing',
            severity='Exception',
            properties=lambda item: {
                "employeeid": item['personid'],
                "firstname": item['firstname'],
                "lastname": item['lastname'],
                'status': 'Exception',
            }
        )

        process_departments_task_entry, process_departments_task_exit = process_departments_task_group(
            config.execution_timeout_days)

        process_locations_task_entry, process_locations_task_exit = process_locations_task_group(
            config.execution_timeout_days)

        process_employeetype_task_entry, process_employeetype_task_exit = process_employeetypes_task_group(
            config.execution_timeout_days)

        get_all_departments = rail.RepliconServiceOperator(
            task_id="get_all_departments",
            endpoint="/services/DepartmentGroupService1.svc/GetAllDepartmentGroups",
        )

        get_all_locations = rail.RepliconServiceOperator(
            task_id="get_all_locations",
            endpoint="/services/LocationService1.svc/GetEnabledLocations",
        )

        get_all_employeetypes = rail.RepliconServiceOperator(
            task_id="get_all_employeetypes",
            endpoint="services/EmployeeTypeGroupListService1.svc/GetData",
            data=request_payload.get_all_employee_grp_payload,
            response_filter=response_filter.get_filtered_employee_grp
        )

        get_all_policy_sets = rail.RepliconServiceOperator(
            task_id="get_all_policy_sets",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        get_all_payrule_scripts = rail.RepliconServiceOperator(
            task_id="get_all_payrule_scripts",
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
        )

        get_all_permission_set = rail.RepliconServiceOperator(
            task_id="get_all_permission_set",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_all_timezones = rail.RepliconServiceOperator(
            task_id="get_all_timezones",
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
        )

        get_all_holiday_calenders = rail.RepliconServiceOperator(
            task_id="get_all_holiday_calenders",
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
        )

        get_user_oefs = rail.RepliconServiceOperator(
            task_id="get_user_oefs",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={"bindingContextUri": "urn:replicon:object-type:user"},
            data_handler=lambda oefs: {
                'homeaddressuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Home Address', 'uri'),
                'homecityuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Home City', 'uri'),
                'homezipuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Home Zip', 'uri'),
                'workstreeturi': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Work Street', 'uri'),
                'workcityuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Work City', 'uri'),
                'workstateuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Work State', 'uri'),
                'workzipuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Work Zip', 'uri'),
                'emergencycontactrelationshipuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Emergency Contact Relationship', 'uri'),
                'emergencycontactnumberuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Emergency Contact Number', 'uri'),
                'emergencycontactfirstnameuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Emergency Contact First Name', 'uri'),
                'emergencycontactlastnameuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Emergency Contact Last Name', 'uri'),
                'worklocationuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Work Location', 'uri'),
                'homestateuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Home State', 'uri'),
                'cellphoneuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Cell Number', 'uri'),
                'homephoneuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Telephone Number', 'uri'),
                'burdenuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Burden', 'uri'),
                'benefitanniversarydateuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Benefit Anniversary Date', 'uri'),
                'middlenameuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Middle Name', 'uri'),
                'adpiduri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'ADP ID', 'uri'),
            },
        )

        get_user_oef_dropdown_value = rail.RepliconServiceOperator(
            task_id="get_user_oef_dropdown_value",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data=lambda: {"objectExtensionTagDefinitionUri": rail.result('get_user_oefs')[
                'worklocationuri']},
            response_filter=response_filter.get_filtered_tag_uri
        )

        process_each_record = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_record',
            items="{{ result('query_valid_records') }}",
            trigger_dag_id=f'matlen_silver_user_sync_child_process_each_records_{config.instance}',
            conf=lambda item: request_payload.get_process_each_record_conf(
                item, config),
            execution_timeout=timedelta(hours=config.execution_timeout_hours),
            retries=0,
        )

        wait_for_process_each_record = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_record',
            dag_runs='{{ result("process_each_record") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_supervisorcheck_pending_logs = rail.FilterLogEntriesOperator(
            task_id='get_supervisorcheck_pending_logs',
            severity='Pending',
            remove_filtered_entries=True
        )

        is_supervisorcheck_pending = rail.IfOperator(
            task_id='is_supervisorcheck_pending',
            test="{{ result('get_supervisorcheck_pending_logs', 'length') > 0 }}",
            yes_task='process_supervisor_check',
            no_task='create_reference_file'
        )

        process_supervisor_check = rail.TriggerDagRunForEachItemOperator(
            task_id='process_supervisor_check',
            retries=0,
            items="{{ result('get_supervisorcheck_pending_logs') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'matlen_silver_user_sync_child_process_supervisor_check_{config.instance}',
            conf=request_payload.get_supervisor_conf
        )

        wait_for_process_supervisor_check = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_supervisor_check',
            dag_runs="{{ result('process_supervisor_check') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        create_reference_file = rail.WriteCSVFileOperator(
            task_id="create_reference_file",
            source=lambda: rail.result('input_data_with_md5'),
            header=['PersonID', 'FirstName', 'LastName', 'MiddleName', 'Supervisor Name', 'Supervisor Employee Code',
                    'Home State', 'Home Zip', 'HomeAddress', 'HomeCity', 'timesheetemail', 'Homephone', 'Cellphone',
                    'EmergencyContactNumber', 'EmergencyContactFirstName', 'EmergencyContactLastName', 'EmergencyContactRelationship',
                    'Originalstartdate@matlen', 'Lastassignmentenddate', 'EmployeeType', 'EmployeeTypeCode', 'Department', 'DepartmentCode',
                    'WorkLocation', 'AssignmentWorkStreet1&2', 'AssignmentWorkCity', 'AssignmentWorkState', 'AssignmentWorkzip', 'TimeZone',
                    'BenefitAnniversaryDate', 'ADPID', 'Net Bill Rate', 'Hourly Pay Rate', 'Burden', 'Workweek', 'Holiday Calendar', 'MD5'],
            row=[
                '{{item.personid}}',
                '{{item.firstname}}',
                '{{item.lastname}}',
                '{{item.middlename}}',
                '{{item.supervisorname}}',
                '{{item.supervisorcode}}',
                '{{item.homestate}}',
                '{{item.homezip}}',
                '{{item.homeaddress}}',
                '{{item.homecity}}',
                '{{item.timesheetemail}}',
                '{{item.homephone}}',
                '{{item.cellphone}}',
                '{{item.emergencycontactnumber}}',
                '{{item.emergencycontactfirstname}}',
                '{{item.emergencycontactlastname}}',
                '{{item.emergencycontactrelationship}}',
                '{{item.originalstartdate}}',
                '{{item.lastassignmentenddate}}',
                '{{item.employeetype}}',
                '{{item.employeetypecode}}',
                '{{item.departmentname}}',
                '{{item.departmentcode}}',
                '{{item.worklocation}}',
                '{{item.workstreet}}',
                '{{item.workcity}}',
                '{{item.workstate}}',
                '{{item.workzip}}',
                '{{item.timezone}}',
                '{{item.benefitanniversarydate}}',
                '{{item.adpid}}',
                '{{item.netbillrate}}',
                '{{item.hourlypayrate}}',
                '{{item.burden}}',
                '{{item.workweek}}',
                '{{item.holidaycalender}}',
                '{{item.md5}}',
            ]
        )

        archive_reference_file = rail.SFTPMoveFileOperator(
            task_id="archive_reference_file",
            new_filename=config.archive_filepath +
            "/user_sync_reference_file_" +
            (datetime.now()).strftime("%Y%m%d%H%M")+".csv",
            existing_filename=config.reference_file
        )

        update_new_reference_file = rail.SFTPUploadFileOperator(
            task_id="update_new_reference_file",
            content="{{result('create_reference_file')}}",
            remote_filepath=config.reference_file
        )

        send_logs_enter, send_logs_end = get_send_logs(config)

        can_log_to_sumo = rail.IfOperator(
            task_id="can_log_to_sumo",
            trigger_rule="all_done",
            test=lambda:  rail.get_current_context()['dag_run'].get_task_instance(
                delete_this_dagrun.task_id).current_state().lower() != "success",
            yes_task="log_to_sumo",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule="all_done",
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "file_name": "{{result('new_file_sensor')}}",
                "archive_file": "{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}",
                "log_file_name": 'log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv'
            }
        )
        new_file_sensor >> is_csv >> rail.Label(
            'Yes') >> download_file >> was_new_file_found
        is_csv >> rail.Label('No') >> send_bad_file_format_email
        was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        download_file >> load_data >> create_input_data_collection >> has_input_data
        has_input_data >> rail.Label('No') >> send_blank_payload_email
        has_input_data >> rail.Label(
            'Yes') >> create_md5 >> input_data_with_md5 >> get_reference_file >> parse_reference_file >> create_reference_data_collection
        create_reference_data_collection >> [
            get_delta_records, get_unchanged_records]
        get_delta_records >> has_any_changed_records >> rail.Label(
            "Yes") >> [query_valid_records, query_invalid_records]
        has_any_changed_records >> rail.Label(
            "No") >> no_changed_records >> send_logs_enter
        get_unchanged_records >> has_any_unchanged_records >> rail.Label(
            "Yes") >> log_unchanged_records >> send_logs_enter
        has_any_unchanged_records >> rail.Label(
            "No") >> no_unchanged_records_present >> send_logs_enter
        query_valid_records >> has_valid_records >> rail.Label('Yes') >> [
            process_departments_task_entry, process_employeetype_task_entry, process_locations_task_entry]
        has_valid_records >> rail.Label(
            'No') >> no_valid_records_present >> send_logs_enter
        query_invalid_records >> has_invalid_records >> rail.Label(
            'Yes') >> log_invalid_records >> send_logs_enter
        has_invalid_records >> rail.Label(
            'No') >> no_invalid_records_present >> send_logs_enter
        [process_departments_task_exit, process_employeetype_task_exit,
            process_locations_task_exit] >> get_all_policy_sets >> get_all_payrule_scripts
        get_all_payrule_scripts >> get_all_permission_set >> get_all_timezones >> get_all_holiday_calenders >> get_user_oefs >> get_user_oef_dropdown_value
        get_user_oef_dropdown_value >> [get_all_departments, get_all_locations,
                                        get_all_employeetypes] >> process_each_record >> wait_for_process_each_record
        wait_for_process_each_record >> get_supervisorcheck_pending_logs >> is_supervisorcheck_pending
        is_supervisorcheck_pending >> rail.Label('No') >> create_reference_file
        is_supervisorcheck_pending >> rail.Label('Yes') >> process_supervisor_check >> wait_for_process_supervisor_check >> create_reference_file
        create_reference_file >> archive_reference_file >> update_new_reference_file >> send_logs_enter
        send_logs_end >> can_log_to_sumo >> rail.Label("Yes") >> log_to_sumo

    return dag


rail.for_each_instance(create_main_dag)
