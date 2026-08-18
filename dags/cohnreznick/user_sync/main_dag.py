from datetime import timedelta, datetime
import itertools
from os import path
import rail
from rail.lib.ecid import get_dagrun_ecid
from rail.filters import split
from airflow.models import Variable

from cohnreznick.user_sync.utils import request_payload, response_filter, python_callable_methods

null = None

# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description='Cohnreznick User Sync',
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
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout)
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

        can_decrypt_file = rail.IfOperator(
            task_id ="can_decrypt_file",
            test=lambda: Variable.get(config.can_decrypt_file_var_name, default_var='true').lower() == 'true',
            yes_task='decrypt_file',
            no_task='dummy_load_data'
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            source='{{ result("download_file") }}',
            pgp_conn_id=config.pgp_conn_id
        )

        dummy_load_data = rail.PythonOperator(
            task_id= "dummy_load_data",
            python_callable= lambda: rail.result('decrypt_file') if Variable.get(config.can_decrypt_file_var_name,
                            default_var='true').lower()== 'true' else  rail.result('download_file'),
            show_return_value_in_logs= False
        )

        load_data = rail.LoadCSVFileOperator(
            task_id='load_data',
            document="{{ result('dummy_load_data') }}",
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source="{{ result('load_data') }}",
            name="inputdatacollection",
            columns={
                'EecEEID': 'employeeid',
                'Employee Number': 'employeenumber',
                'Company': 'company',
                'Preferred First Name': 'preferredfirstname',
                'First Name': 'firstname',
                'Last Name': 'lastname',
                'Employee Email': 'email',
                'Last Hire Date': 'startdate',
                'Employee Status': 'status',
                'Termination Date': 'enddate',
                'Employee Type': 'employeetype',
                'Location Code': 'locationcode',
                'Location Name': 'locationname',
                'Org Level 3 code': 'departmentcode',
                'Org Level 3': 'departmentname',
                'Service Center Code': 'servicecentercode',
                'Service Center Name': 'servicecentername',
                'Cost Center Code': 'costcentercode',
                'Cost Center Name': 'costcentername',
                'Pay group code': 'divisioncode',
                'Pay group Name': 'divisionname',
                'Work schedule': 'workschedule',
                'Time Entry System': 'timeentrysystem',
                'Activity Type Code': 'activitytypecode',
                'Activity Type Description': 'activitytypedescription',
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
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | User Sync - no records in file - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html"
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        create_md5 = rail.DataAdaptorOperator(
            task_id="create_md5",
            source="{{result('create_input_data_collection')}}",
            columns=['employeeid', 'employeenumber', 'company', 'preferredfirstname', 'firstname', 'lastname', 'email', 'startdate',
                     'status', 'enddate', 'employeetype', 'locationcode', 'locationname', 'departmentcode', 'departmentname',
                     'servicecentercode', 'servicecentername', 'costcentercode', 'costcentername',
                     'divisioncode', 'divisionname','workschedule', 'timeentrysystem', 'activitytypecode', 'activitytypedescription','md5'],
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
            query="""SELECT * FROM input_data WHERE md5 NOT IN (SELECT DISTINCT MD5 FROM reference_data)"""
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
            query="""SELECT * FROM input_data WHERE md5 IN (SELECT DISTINCT MD5 FROM reference_data)"""
        )

        has_any_unchanged_records = rail.IfOperator(
            task_id="has_any_unchanged_records",
            test="{{result('get_unchanged_records', 'length') > 0}}",
            yes_task="log_unchanged_records",
            no_task="no_unchanged_records_present"
        )

        log_unchanged_records = rail.WriteLogOperator(
            task_id="log_unchanged_records",
            log="{{ result('create_log') }}",
            items="{{result('get_unchanged_records')}}",
            message="No changes Recieved",
            severity="Skipped",
            properties=lambda item: {
                "employeeid": item['employeeid'],
                "employeenumber":item['employeenumber'],
                "firstname": item['firstname'],
                "lastname": item['lastname'],
                "action":"Validation",
                'status': "Skipped",
            }
        )

        no_unchanged_records_present = rail.EmptyOperator(
            task_id='no_unchanged_records_present'
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            name='validrecords',
            query="""SELECT * FROM get_delta_records WHERE NULLIF(employeeid, '') IS NOT NULL and
                    NULLIF(employeenumber, '') IS NOT NULL and NULLIF(company, '') IS NOT NULL and NULLIF(firstname, '') IS NOT NULL and
                    NULLIF(lastname, '') IS NOT NULL and NULLIF(email, '') IS NOT NULL and NULLIF(startdate, '') IS NOT NULL and
                    NULLIF(status, '') IS NOT NULL and NULLIF(employeetype, '') IS NOT NULL and
                    NULLIF(locationcode, '') IS NOT NULL and NULLIF(locationname, '') IS NOT NULL and NULLIF(departmentcode, '') IS NOT NULL and
                    NULLIF(departmentname, '') IS NOT NULL and NULLIF(servicecentercode, '') IS NOT NULL and NULLIF(servicecentername, '') IS NOT NULL and
                    NULLIF(costcentercode, '') IS NOT NULL and NULLIF(costcentername, '') IS NOT NULL and NULLIF(divisioncode, '') IS NOT NULL and
                    NULLIF(divisionname, '') IS NOT NULL and NULLIF(workschedule, '') IS NOT NULL and NULLIF(timeentrysystem, '') IS NOT NULL and
                    NULLIF(activitytypecode, '') IS NOT NULL and NULLIF(activitytypedescription, '') IS NOT NULL"""
        )

        has_valid_records = rail.IfOperator(
            task_id="has_valid_records",
            test="{{result('query_valid_records', 'length') > 0}}",
            yes_task='process_groups',
            no_task="no_valid_records_present"
        )

        no_valid_records_present = rail.EmptyOperator(
            task_id='no_valid_records_present'
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            name='invalidrecords',
            query="""SELECT * FROM get_delta_records WHERE NULLIF(employeeid, '') IS NULL or
                    NULLIF(employeenumber, '') IS NULL or NULLIF(company, '') IS NULL or NULLIF(firstname, '') IS NULL or
                    NULLIF(lastname, '') IS NULL or NULLIF(email, '') IS NULL or NULLIF(startdate, '') IS NULL or
                    NULLIF(status, '') IS NULL or NULLIF(employeetype, '') IS NULL or
                    NULLIF(locationcode, '') IS NULL or NULLIF(locationname, '') IS NULL or NULLIF(departmentcode, '') IS NULL or
                    NULLIF(departmentname, '') IS NULL or NULLIF(servicecentercode, '') IS NULL or NULLIF(servicecentername, '') IS NULL or
                    NULLIF(costcentercode, '') IS NULL or NULLIF(costcentername, '') IS NULL or NULLIF(divisioncode, '') IS NULL or
                    NULLIF(divisionname, '') IS NULL or NULLIF(workschedule, '') IS NULL or NULLIF(timeentrysystem, '') IS NULL or
                    NULLIF(activitytypecode, '') IS NULL or NULLIF(activitytypedescription, '') IS NULL"""
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
            log="{{ result('create_log') }}",
            items='{{result("query_invalid_records")}}',
            message=request_payload.get_mandatory_fields_exception_message,
            severity='Exception',
            properties=lambda item: {
                "employeeid": item['employeeid'],
                "employeenumber":item['employeenumber'],
                "firstname": item['firstname'],
                "lastname": item['lastname'],
                "action": "Validation",
                'status': 'Exception',
            }
        )

        process_groups = rail.TriggerDagRunOperator(
            task_id="process_groups",
            trigger_dag_id=config.process_groups_dag_id,
            conf={
                "file_name": "{{ result('new_file_sensor') | file_name}}"
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_process_groups = rail.WaitForDagRunsSensor(
            task_id="wait_process_groups",
            dag_runs="{{ result('process_groups') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_updated_locations = rail.RepliconServiceOperator(
            task_id='get_updated_locations',
            endpoint='/services/LocationListService1.svc/GetData',
            data=request_payload.get_location_payload,
            data_handler= response_filter.filter_group_data
        )

        get_updated_departments = rail.RepliconServiceOperator(
            task_id='get_updated_departments',
            endpoint='/services/DepartmentGroupListService1.svc/GetData',
            data=request_payload.get_dept_group_payload,
            data_handler=response_filter.filter_group_data
        )

        get_updated_service_centers = rail.RepliconServiceOperator(
            task_id="get_updated_service_centers",
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:service-center-list-column:name",
                    "urn:replicon:service-center-list-column:code",
                    "urn:replicon:service-center-list-column:description",
                    "urn:replicon:service-center-list-column:service-center"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=response_filter.filter_servicecenters_data
        )

        get_updated_costcenter = rail.RepliconServiceOperator(
            task_id='get_updated_costcenter',
            endpoint='/services/CostCenterListService1.svc/GetData',
            data=request_payload.get_costcenter_payload,
            data_handler=response_filter.filter_group_data
        )

        get_updated_divisions = rail.RepliconServiceOperator(
            task_id="get_updated_divisions",
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
                    "page": "1",
                    "pagesize": "100000",
                    "columnUris": [
                        "urn:replicon:division-list-column:name",
                        "urn:replicon:division-list-column:division"
                    ]
            },
            data_handler=response_filter.filter_divisions_data
        )

        get_all_employeetypes = rail.RepliconServiceOperator(
            task_id="get_all_employeetypes",
            endpoint="services/EmployeeTypeGroupListService1.svc/GetData",
            data=request_payload.get_all_employee_grp_payload,
            data_handler=response_filter.filter_group_data
        )

        get_all_permission_set = rail.RepliconServiceOperator(
            task_id="get_all_permission_set",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_all_payrule_scripts = rail.RepliconServiceOperator(
            task_id="get_all_payrule_scripts",
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
        )

        get_all_policy_sets = rail.RepliconServiceOperator(
            task_id="get_all_policy_sets",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        get_timesheet_approval_paths = rail.RepliconServiceOperator(
            task_id='get_timesheet_approval_paths',
            endpoint='/services/TimesheetApprovalService1.svc/GetAllApprovalPaths',
        )

        get_timeentry_approval_paths = rail.RepliconServiceOperator(
            task_id='get_timeentry_approval_paths',
            endpoint='/services/TimeEntryRevisionGroupApprovalService1.svc/GetPageOfApprovalPathsByTextSearch',
            data={"page": "1", "pageSize": "10000", "textSearch": null},
            data_handler=response_filter.map_response_data
        )

        get_user_oefs = rail.RepliconServiceOperator(
            task_id="get_user_oefs",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={"bindingContextUri": "urn:replicon:object-type:user"},
            data_handler=lambda oefs: {
                'employeenumberdefinitionuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Employee Number', 'uri'),
                'activitytypecodedefinitionuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Activity Type Code', 'uri'),
                'activitytypedescriptiondefinitionuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Activity Type Description', 'uri'),
            },
        )

        get_user_udfs = rail.RepliconServiceOperator(
            task_id="get_user_udfs",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda udfs: {
                'customfieldgroupuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Component Company', 'group.uri'),
                'companydefinitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Component Company', 'uri'),
                'timeentrysystemdefinitionuri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Time Entry System', 'uri'),
            },
        )

        get_componentcompany_udf_dropdown_values = rail.RepliconServiceOperator(
            task_id="get_componentcompany_udf_dropdown_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                    "customFieldUri": rail.result('get_user_udfs')['companydefinitionuri']
                },
            data_handler=response_filter.get_all_drop_down_options_filter
        )

        create_company_udf_collection_replicon = rail.CreateCollectionOperator(
            task_id="create_company_udf_collection_replicon",
            columns=['name', 'uri'],
            name="replicon_company",
            source="{{ result('get_componentcompany_udf_dropdown_values') | to_json }}"
        )

        query_company_udf_values_add = rail.QueryCollectionOperator(
            task_id="query_company_udf_values_add",
            query="""SELECT DISTINCT company FROM validrecords WHERE LOWER(company) NOT IN
                    (SELECT DISTINCT LOWER(name) FROM replicon_company)""",
            name='newcompanyudfvalues'
        )

        has_any_componentcompany_udf_values_to_add = rail.IfOperator(
            task_id="has_any_componentcompany_udf_values_to_add",
            test="{{result('query_company_udf_values_add', 'length') > 0}}",
            yes_task="create_componentcompant_add_payload",
            no_task="get_updated_componentcompany_udf_dropdown_values"
        )

        create_componentcompant_add_payload = rail.PythonOperator(
            task_id="create_componentcompant_add_payload",
            python_callable=python_callable_methods.create_componentcompant_add_payload
        )

        put_componentcompany_dropdown_values = rail.RepliconServiceOperator(
            task_id="put_componentcompany_dropdown_values",
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result('get_user_udfs')['companydefinitionuri'],
                "customFieldDropDownOptionUris": rail.result('create_componentcompant_add_payload')
            }
        )

        get_updated_componentcompany_udf_dropdown_values = rail.RepliconServiceOperator(
            task_id="get_updated_componentcompany_udf_dropdown_values",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda:{
                    "customFieldUri": rail.result('get_user_udfs')['companydefinitionuri']
                },
            data_handler=response_filter.get_all_drop_down_options_filter
        )

        dummy_process_users =rail.EmptyOperator(
            task_id='dummy_process_users'
        )

        process_users = rail.trigger_parallel_dagrun(
            task_id='process_users',
            items="{{ result('query_valid_records') }}",
            parallel_count=config.trigger_parallel_dagrun_count_process_users,
            trigger_dag_id=config.process_users,
            conf= lambda item: request_payload.get_process_users_conf(item, config),
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_process_users_dag_ids =rail.PythonOperator(
            task_id= 'get_process_users_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_users_{x+1}'), range(config.trigger_parallel_dagrun_count_process_users))))),
            show_return_value_in_logs= False
        )

        gather_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_logs',
            dag_runs='{{ result("get_process_users_dag_ids") }}',
            dagrun_task_id='create_user_log',
            execution_timeout=timedelta(
                hours=config.gather_user_logs_timeout_hours),
            flatten=True
        )

        create_reference_file = rail.WriteCSVFileOperator(
            task_id="create_reference_file",
            source=lambda: rail.result('input_data_with_md5'),
            header=['EecEEID', 'Employee Number', 'Company', 'Preferred First Name', 'First Name', 'Last Name',
                    'Employee Email', 'Last Hire Date', 'Employee Status', 'Termination Date', 'Employee Type', 'Location Code', 'Location Name',
                    'Org Level 3 code', 'Org Level 3', 'Service Center Code', 'Service Center Name',
                    'Cost Center Code', 'Cost Center Name', 'Pay group code', 'Pay group Name', 'Work schedule', 'Time Entry System',
                    'Activity type', 'Activity Type Description', 'MD5'],
            row=[
                '{{item.employeeid}}',
                '{{item.employeenumber}}',
                '{{item.company}}',
                '{{item.preferredfirstname}}',
                '{{item.firstname}}',
                '{{item.lastname}}',
                '{{item.email}}',
                '{{item.startdate}}',
                '{{item.status}}',
                '{{item.enddate}}',
                '{{item.employeetype}}',
                '{{item.locationcode}}',
                '{{item.locationname}}',
                '{{item.departmentcode}}',
                '{{item.departmentname}}',
                '{{item.servicecentercode}}',
                '{{item.servicecentername}}',
                '{{item.costcentercode}}',
                '{{item.costcentername}}',
                '{{item.divisioncode}}',
                '{{item.divisionname}}',
                '{{item.workschedule}}',
                '{{item.timeentrysystem}}',
                '{{item.activitytypecode}}',
                '{{item.activitytypedescription}}',
                '{{item.md5}}',
            ]
        )

        archive_old_reference_file = rail.SFTPMoveFileOperator(
            task_id="archive_old_reference_file",
            new_filename=config.archive_filepath +
            "/user_sync_reference_file_" +
            (datetime.now()).strftime("%Y%m%d%H%M")+".csv",
            existing_filename=config.reference_file
        )

        upload_new_reference_file = rail.SFTPUploadFileOperator(
            task_id="upload_new_reference_file",
            content="{{result('create_reference_file')}}",
            remote_filepath=config.reference_file
        )

        dummy_process_log_generation = rail.EmptyOperator(
            task_id='dummy_process_log_generation'
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation,
            conf=lambda dag_run:{
                'userlogs': rail.result('gather_user_logs'),
                'otherlogs': rail.result('create_log'),
                # pylint: disable=line-too-long
                'log_filename': f'log_{ get_dagrun_ecid(dag_run).replace(":", "-")}_{split(string=path.split(rail.result("new_file_sensor"))[1], separator=".")[0] }.csv'
            }
        )

        can_log_to_sumo = rail.IfOperator(
            task_id="can_log_to_sumo",
            trigger_rule="all_done",
            test=lambda:  rail.get_current_context()['dag_run'].get_task_instance(
                delete_this_dagrun.task_id).current_state().lower() != "success" and
                rail.get_current_context()['dag_run'].get_task_instance(
                download_file.task_id).current_state().lower() == "success",
            yes_task="log_to_sumo",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "file_name": "{{result('new_file_sensor')}}",
                "archive_file": "{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}",
                "log_file_name": 'log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv'
            }
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        new_file_sensor >> is_csv >> rail.Label('Yes') >> download_file >> was_new_file_found
        was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun

        is_csv >> rail.Label('No') >> send_bad_file_format_email
        download_file >> can_decrypt_file >> rail.Label("Yes") >> decrypt_file >> dummy_load_data
        can_decrypt_file >> rail.Label("No") >> dummy_load_data
        dummy_load_data >> load_data >> create_input_data_collection >> create_log >> has_input_data
        has_input_data >> rail.Label('No') >> send_blank_payload_email

        has_input_data >> rail.Label(
            'Yes') >> create_md5 >> input_data_with_md5 >> get_reference_file >> parse_reference_file >> create_reference_data_collection
        create_reference_data_collection >> [get_delta_records, get_unchanged_records]
        get_delta_records >> has_any_changed_records >> rail.Label("Yes") >> [query_valid_records, query_invalid_records]

        has_any_changed_records >> rail.Label("No") >> no_changed_records >> create_reference_file
        get_unchanged_records >> has_any_unchanged_records >> rail.Label("Yes") >> log_unchanged_records >> create_reference_file
        has_any_unchanged_records >> rail.Label("No") >> no_unchanged_records_present >> create_reference_file

        query_invalid_records >> has_invalid_records >> rail.Label('Yes') >> log_invalid_records >> create_reference_file
        has_invalid_records >> rail.Label('No') >> no_invalid_records_present >> create_reference_file

        query_valid_records >> has_valid_records
        has_valid_records >> rail.Label('No') >> no_valid_records_present >> create_reference_file
        has_valid_records >> rail.Label('Yes') >> process_groups >> wait_process_groups
        wait_process_groups >> [get_updated_locations, get_updated_departments, get_updated_service_centers,
            get_updated_costcenter, get_updated_divisions] >> get_all_employeetypes

        get_all_employeetypes >> get_all_permission_set >> get_all_payrule_scripts >> get_all_policy_sets >> get_timesheet_approval_paths
        get_timesheet_approval_paths >> get_timeentry_approval_paths >> get_user_oefs >> get_user_udfs >> get_componentcompany_udf_dropdown_values
        get_componentcompany_udf_dropdown_values >> create_company_udf_collection_replicon >> query_company_udf_values_add
        query_company_udf_values_add >> has_any_componentcompany_udf_values_to_add >> rail.Label('No') >> get_updated_componentcompany_udf_dropdown_values
        has_any_componentcompany_udf_values_to_add >> rail.Label('Yes') >> create_componentcompant_add_payload >> put_componentcompany_dropdown_values
        put_componentcompany_dropdown_values >> get_updated_componentcompany_udf_dropdown_values
        get_updated_componentcompany_udf_dropdown_values >> dummy_process_users >> process_users

        process_users >> get_process_users_dag_ids >> gather_user_logs >> create_reference_file >> archive_old_reference_file
        archive_old_reference_file >> upload_new_reference_file >> dummy_process_log_generation
        dummy_process_log_generation >> process_log_generation >> can_log_to_sumo >> rail.Label('Yes') >> log_to_sumo

        log_to_sumo >> can_fail_dag >> rail.Label('Yes') >> fail_dagrun

    return dag

rail.for_each_instance(create_main_dag)
