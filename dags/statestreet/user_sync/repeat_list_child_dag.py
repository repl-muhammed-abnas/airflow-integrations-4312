from datetime import timedelta, datetime
import rail
from airflow.models import Variable
import pytz
from statestreet.user_sync.utils.response_filter import get_locationlist, get_bussinessarea_uri

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'statestreet_repeat_list_child_{config.instance}',
        description=f'Statestreet_repeat_list_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_batch_execution_failed'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_batch_execution_failed',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        if_batch_execution_failed = rail.IfOperator(
            task_id='if_batch_execution_failed',
            test=lambda dag_run: dag_run.conf['wait_batch'][
                'executionState'] == 'urn:replicon-service-model:batch-execution-state:failed',
            yes_task="stop_job_with_error",
            no_task="if_batch_execution_succedded",
        )

        stop_job_with_error = rail.FailOperator(
            task_id='stop_job_with_error',
            message='Report Batch failed'
        )

        if_batch_execution_succedded = rail.IfOperator(
            task_id='if_batch_execution_succedded',
            test=lambda dag_run: dag_run.conf['wait_batch'][
                'executionState'] == 'urn:replicon-service-model:batch-execution-state:succeeded',
            yes_task="load_csv",
            no_task="finish",
        )

        load_csv = rail.LoadCSVFileOperator(
            task_id="load_csv",
            document="{{(dag_run.conf.report_data).reportGenerationResults[0].payload}}",
        )

        create_collection_from_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_csv',
            source="{{ result('load_csv') }}",
            name="allusers",
            columns={
                'Login Name': 'loginname',
                'Employee ID': 'empid',
                'User Status': 'status',
                'User Name': 'username',
                'UserUri': 'uri'
            }
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            sftp_conn_id=config.sftp_client_conn_id,
            remote_filepath="{{dag_run.conf.file_path}}"
        )

        load_csv_from_input_file = rail.LoadCSVFileOperator(
            task_id="load_csv_from_input_file",
            document="{{result('download_file')}}",
            encoding='UTF-8-SIG'
        )

        create_collection_from_input_file = rail.CreateCollectionOperator(
            task_id='create_collection_from_input_file',
            source="{{ result('load_csv_from_input_file') }}",
            name="inputfile",
            columns={
                'Login ID': 'loginid',
                'Employee ID': 'employeeid',
                'Employee Type': 'employeetype',
                'Employee Status': 'employeestatus',
                'Name': 'name',
                'Job Function': 'jobfunction',
                'Job Family': 'jobfamily',
                'Bank Title': 'banktitle',
                'Full/Part Time': 'fullparttime',
                'Standard Hours': 'standardhours',
                'Manager/Non Manager': 'managernonmanager',
                'Email': 'email',
                'Region': 'region',
                'Country': 'country',
                'City': 'city',
                'Location Code': 'locationcode',
                'Service Line': 'serviceline',
                'Business Area': 'businessarea',
                'Division': 'division',
                'Business Line': 'businessline',
                'Sub Business Line': 'subbusinessline',
                'Cost Center - Name': 'costcentername',
                'Cost Center Number': 'costcenternumber',
                'Legal Entity Name': 'legalentityname',
                'Manager ID': 'managerid',
                'Manager': 'manager'
            }
        )

        list_reference_files = rail.SFTPListFilesOperator(
            task_id='list_reference_files',
            sftp_conn_id=config.sftp_conn_id,
            paths=[config.reference_filepath]
        )

        foreach_list_reference_files_do = rail.ForEachOperator(
            task_id='foreach_list_reference_files_do',
            items=lambda: rail.result('list_reference_files')[
                config.reference_filepath],
            start_task='if_reference_files_ends_with_csv',
            end_task='foreach_list_reference_files_do_end'
        )

        if_reference_files_ends_with_csv = rail.IfOperator(
            task_id='if_reference_files_ends_with_csv',
            # pylint: disable=too-many-statements line-too-long
            test="{{ result('foreach_list_reference_files_do').name | ends_with('csv') and result('foreach_list_reference_files_do').name | matches('tatestreet') }}",
            yes_task="log_reference_filename",
            no_task="foreach_list_reference_files_do_end",
        )

        log_reference_filename = rail.PythonOperator(
            task_id='log_reference_filename',
            python_callable=lambda: rail.result(
                'foreach_list_reference_files_do')['name']
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.reference_filepath +
            "/" + "{{result('log_reference_filename')}}"
        )

        foreach_list_reference_files_do_end = rail.EmptyOperator(
            task_id='foreach_list_reference_files_do_end'
        )

        load_downloaded_reference_file = rail.LoadCSVFileOperator(
            task_id="load_downloaded_reference_file",
            document="{{result('download_reference_file')}}",
            encoding='UTF-8-SIG'
        )

        get_enabled_divisions = rail.RepliconServiceOperator(
            task_id='get_enabled_divisions',
            endpoint="/services/divisionService1.svc/GetEnabledDivisions",
        )

        get_enabled_locations = rail.RepliconServiceOperator(
            task_id='get_enabled_locations',
            endpoint="/services/LocationService1.svc/GetEnabledLocations",
        )

        get_enabled_cost_centers = rail.RepliconServiceOperator(
            task_id='get_enabled_cost_centers',
            endpoint="/services/costcenterService1.svc/GetEnabledCostCenters",
        )

        get_enabled_department_hierarchy_details = rail.RepliconServiceOperator(
            task_id='get_enabled_department_hierarchy_details',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartmentHierarchyDetails",
        )

        if_download_reference_file_is_blank = rail.IfOperator(
            task_id='if_download_reference_file_is_blank',
            test="{{result('load_downloaded_reference_file') | is_falsy}}",
            yes_task="stop_job_with_error_message",
            no_task="if_download_reference_file_has_data",
        )

        stop_job_with_error_message = rail.FailOperator(
            task_id='stop_job_with_error_message',
            message='Invalid Reference file'
        )

        if_download_reference_file_has_data = rail.IfOperator(
            task_id='if_download_reference_file_has_data',
            test="{{result('load_downloaded_reference_file') | is_truthy}}",
            yes_task="create_collection_from_reference_file",
            no_task="finish",
        )

        create_collection_from_reference_file = rail.CreateCollectionOperator(
            task_id='create_collection_from_reference_file',
            source="{{ result('load_downloaded_reference_file') }}",
            name="referencefile",
            columns={
                'Login ID': 'loginid',
                'Employee ID': 'employeeid',
                'Employee Type': 'employeetype',
                'Employee Status': 'employeestatus',
                'Name': 'name',
                'Job Function': 'jobfunction',
                'Job Family': 'jobfamily',
                'Bank Title': 'banktitle',
                'Full/Part Time': 'fullparttime',
                'Standard Hours': 'standardhours',
                'Manager/Non Manager': 'managernonmanager',
                'Email': 'email',
                'Region': 'region',
                'Country': 'country',
                'City': 'city',
                'Location Code': 'locationcode',
                'Service Line': 'serviceline',
                'Business Area': 'businessarea',
                'Division': 'division',
                'Business Line': 'businessline',
                'Sub Business Line': 'subbusinessline',
                'Cost Center - Name': 'costcentername',
                'Cost Center Number': 'costcenternumber',
                'Legal Entity Name': 'legalentityname',
                'Manager ID': 'managerid',
                'Manager': 'manager'
            }
        )

        query_list_deltas = rail.QueryCollectionOperator(
            task_id='query_list_deltas',
            query="""SELECT  inputfile.loginid,  inputfile.employeeid,  inputfile.employeetype,
            inputfile.employeestatus,  inputfile.name,  inputfile.jobfunction,  inputfile.jobfamily,
            inputfile.banktitle,  inputfile.fullparttime,  inputfile.standardhours,  inputfile.managernonmanager,
            inputfile.email,  inputfile.region,  inputfile.country,  inputfile.city,  inputfile.locationcode,
            inputfile.serviceline,  inputfile.businessarea,  inputfile.division,  inputfile.businessline,
            inputfile.subbusinessline,  inputfile.costcentername,  inputfile.costcenternumber,
            inputfile.legalentityname,  inputfile.managerid,
            inputfile.manager FROM  inputfile EXCEPT SELECT  referencefile.loginid,
            referencefile.employeeid,  referencefile.employeetype,  referencefile.employeestatus,
            referencefile.name,  referencefile.jobfunction,  referencefile.jobfamily,
            referencefile.banktitle,  referencefile.fullparttime,  referencefile.standardhours,
            referencefile.managernonmanager,  referencefile.email,  referencefile.region,
            referencefile.country,  referencefile.city,  referencefile.locationcode,
            referencefile.serviceline,  referencefile.businessarea,  referencefile.division,
            referencefile.businessline,  referencefile.subbusinessline,
            referencefile.costcentername,  referencefile.costcenternumber,
            referencefile.legalentityname,  referencefile.managerid,  referencefile.manager FROM  referencefile""",
        )

        if_query_list_has_data_present = rail.IfOperator(
            task_id='if_query_list_has_data_present',
            test="{{ result('query_list_deltas', 'length') > 0 }}",
            yes_task="create_deltavalues_collection",
            no_task="send_update_completion_mail",
        )

        create_deltavalues_collection = rail.CreateCollectionOperator(
            task_id='create_deltavalues_collection',
            source="{{result('query_list_deltas')}}",
            name="deltavalues",
        )

        query_list_users_tobe_updated = rail.QueryCollectionOperator(
            task_id='query_list_users_tobe_updated',
            query="""SELECT allusers.uri, deltavalues.* FROM allusers INNER JOIN deltavalues
            ON allusers.loginname = deltavalues.loginid ORDER BY deltavalues.managernonmanager DESC""",
        )

        query_list_all_new_users = rail.QueryCollectionOperator(
            task_id='query_list_all_new_users',
            query="""SELECT * FROM inputfile WHERE NOT EXISTS
            (SELECT * FROM allusers WHERE allusers.loginname = inputfile.loginid)
            ORDER BY inputfile.managernonmanager DESC""",
        )

        statestreet_userimport_logtable = rail.CreateLogOperator(
            task_id='statestreet_userimport_logtable'
        )

        supervisor_assignment_logtable = rail.CreateLogOperator(
            task_id='supervisor_assignment_logtable'
        )

        get_policyset_uri = rail.RepliconServiceOperator(
            task_id='get_policyset_uri',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        get_permissionset_uri = rail.RepliconServiceOperator(
            task_id='get_permissionset_uri',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_customfield_uri = rail.RepliconServiceOperator(
            task_id='get_customfield_uri',
            endpoint="/services/UserCustomFieldListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-custom-field-list-column:field-name"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-custom-field-filter:effectively-enabled"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
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
                        "dateTimeUtcRange": null,
                        "numberRange": null
                    },
                    "filterDefinitionUri": null
                }
            }
        )

        def get_custombanktitle_uri():
            record_details = rail.result(
                'get_customfield_uri')
            record_details_list = record_details if record_details['rows'] else None
            for row in record_details_list['rows']:
                if row['cells'][0].get('textValue') == 'Bank Titles':
                    return row['cells'][0]['uri']
            return None

        def get_customstandardhours_uri():
            record_details = rail.result(
                'get_customfield_uri')
            record_details_list = record_details if record_details['rows'] else None
            for row in record_details_list['rows']:
                if row['cells'][0].get('textValue') == 'Standard Hours':
                    return row['cells'][0]['uri']
            return None

        def get_customfulltime_uri():
            record_details = rail.result(
                'get_customfield_uri')
            record_details_list = record_details if record_details['rows'] else None
            for row in record_details_list['rows']:
                if row['cells'][0].get('textValue') == 'Full / Part Time':
                    return row['cells'][0]['uri']
            return None

        get_enabled_custom_field_dropdown_options = rail.RepliconServiceOperator(
            task_id='get_enabled_custom_field_dropdown_options',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": get_customfulltime_uri(),
            }
        )

        def get_department(item):
            departments = rail.result('get_enabled_department_hierarchy_details')[
                'childDepartments']
            legalentity = {}
            for department in departments:
                if department['department'] and department['department']['displayText'] == item['legalentityname']:
                    legalentity = department
                    break
            costcenter = {}
            get_legalentity = legalentity['childDepartments'] if legalentity else {
            }
            for subdepartment in get_legalentity:
                if subdepartment['department']['displayText'] == item['costcenternumber']:
                    costcenter = subdepartment
                    break
            return {
                "legalentityuri": legalentity['department']['uri'] if legalentity else null,
                "islegalentityenabled": legalentity['department']['isEnabled'] if legalentity else null,
                "costcenteruri": costcenter['department']['uri'] if costcenter else null,
                "iscostcenterenabled": costcenter['department']['isEnabled'] if costcenter else null
            }

        get_enabled_custom_field_drop_down_options = rail.RepliconServiceOperator(
            task_id='get_enabled_custom_field_drop_down_options',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": get_custombanktitle_uri(),
            }
        )

        get_locationlist_data = rail.RepliconServiceOperator(
            task_id='get_locationlist_data',
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                     "urn:replicon:location-list-column:location",
                     "urn:replicon:location-list-column:full-path",
                     "urn:replicon:location-list-column:effectively-enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:location-list-filter:effectively-enabled"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "rightExpression": null,
                        "value": {
                            "bool": "true"
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            data_handler=get_locationlist
        )

        get_jobfunction_child_hierarchy_data = rail.RepliconServiceOperator(
            task_id='get_jobfunction_child_hierarchy_data',
            endpoint="/services/DivisionListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:division-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:division-list-filter:effectively-enabled"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "rightExpression": null,
                        "value": {
                            "bool": "true"
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
        )

        def get_jobfamily_uri(item):
            record_details = rail.result(
                'get_jobfunction_child_hierarchy_data')
            record_details_list = record_details if record_details['rows'][0] else None
            for row in record_details_list['rows']:
                for cell in row['cells']:
                    for cell_data in cell['cellCollection']:
                        if cell_data.get('textValue') == item['jobfamily']:
                            return cell_data['uri']
            return None

        get_bussinessarea_child_hierarchy_details = rail.RepliconServiceOperator(
            task_id='get_bussinessarea_child_hierarchy_details',
            endpoint="/services/CostCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:cost-center-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:cost-center-list-filter:effectively-enabled"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "rightExpression": null,
                        "value": {
                            "bool": "true"
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            data_handler=get_bussinessarea_uri
        )

        process_add_user_child = rail.TriggerDagRunForEachItemOperator(
            # pylint: disable=too-many-statements line-too-long
            task_id='process_add_user_child',
            retries=0,
            items='{{ result("query_list_all_new_users") }}',
            trigger_dag_id=f'statestreet_user_sync_add_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                "user_items": item,

                "region_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_locations'),
                                                                   'displayText', item['region'], 'uri', null) if item['region'] else null,

                "jobfunction_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_divisions'),
                                                                        'displayText', item['jobfunction'], 'uri', null) if item['jobfunction'] else null,

                "location_uri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_locationlist_data')['locationlistinput'], 'fullpath', (str(item['region']) +
                                                                                " | " + str(item['country']) + " | " + str(item['city']) + " | " +
                                                                                str(item['locationcode'])), 'uri', ''),

                "policyset_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_policyset_uri'),
                                                                      'displayText', 'Widget Timesheet – Time distribution grid', 'uri', null) if rail.result('get_policyset_uri')
                else null,

                "permission_set1": rail.find_first_by_attr_and_get_attr(rail.result('get_permissionset_uri'),
                                                                        'displayText', 'Project Resource with Reports', 'uri', null) if rail.result('get_permissionset_uri')
                else null,

                "permission_set2": rail.find_first_by_attr_and_get_attr(rail.result('get_permissionset_uri'),
                                                                        'displayText', 'Supervisor', 'uri', null) if rail.result('get_permissionset_uri') else null,

                "permission_set3": rail.find_first_by_attr_and_get_attr(rail.result('get_permissionset_uri'),
                                                                        'displayText', 'Project Resource', 'uri', null) if rail.result('get_permissionset_uri') else null,

                "permission_set4": rail.find_first_by_attr_and_get_attr(rail.result('get_permissionset_uri'),
                                                                        'displayText', 'Report User', 'uri', null) if rail.result('get_permissionset_uri') else null,

                "jobfamily_uri": get_jobfamily_uri(item),

                "lookup_table": rail.result('statestreet_userimport_logtable'),

                "job_id": rail.render_template("{{dag_run_ecid()}}"),

                "full_parttime_uri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_enabled_custom_field_dropdown_options'), 'displayText', item['fullparttime'],
                    'uri', null) if rail.result('get_enabled_custom_field_dropdown_options')
                else null,

                "bank_title_uri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_enabled_custom_field_drop_down_options'), 'displayText', item['banktitle'],
                    'uri', null) if rail.result('get_enabled_custom_field_drop_down_options')
                else null,

                "file_name": dag_run.conf['file_path'],

                "supervisor_logtable": rail.result('supervisor_assignment_logtable'),

                "custom_field1": get_custombanktitle_uri(),

                "custom_field2": get_customstandardhours_uri(),

                "custom_field3": get_customfulltime_uri(),

                "legalentitycheckobject": get_department(item),

                "check": (str(item['serviceline']) +
                          ' | ' + str(item['businessarea']) +
                          ' | ' + str(item['division']) + ' | ' + str(item['businessline']) +
                          ' | ' + str(item['subbusinessline'])),


                "businessareacheckobject": {
                    "serviceline_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_cost_centers'),
                                                                            'displayText', item['serviceline'], 'uri', null) if item['serviceline'] else null,

                    "subbussinessline_uri": rail.find_first_by_attr_and_get_attr(rail.result(
                        'get_bussinessarea_child_hierarchy_details')['listinput'], 'name', (str(item['serviceline']) +
                                                                                            ' | ' + str(item['businessarea']) +
                                                                                            ' | ' + str(item['division']) + ' | ' + str(item['businessline']) +
                                                                                            ' | ' + str(item['subbusinessline'])), 'uri', null)
                }
            }
        )

        wait_for_process_add_user_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_add_user_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_add_user_child") }}'
        )

        process_update_user_child = rail.TriggerDagRunForEachItemOperator(
            # pylint: disable=too-many-statements line-too-long
            task_id='process_update_user_child',
            retries=0,
            items='{{ result("query_list_users_tobe_updated") }}',
            trigger_dag_id=f'statestreet_user_sync_update_user_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {

                "update_items": item,

                "region_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_locations'),
                                                                   'displayText', item['region'], 'uri', null) if item['region'] else null,

                "user_uri": item['uri'],

                "policyset_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_policyset_uri'),
                                                                      'displayText', 'Widget Timesheet – Time distribution grid',
                                                                      'uri', null) if rail.result('get_policyset_uri') else null,

                "jobfunction_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_divisions'),
                                                                        'displayText', item['jobfunction'], 'uri', null) if item['jobfunction'] else null,

                "permission_set3": rail.find_first_by_attr_and_get_attr(rail.result('get_permissionset_uri'),
                                                                        'displayText', 'Project Resource with Reports', 'uri', null) if rail.result('get_permissionset_uri') else null,

                "permission_set2": rail.find_first_by_attr_and_get_attr(rail.result('get_permissionset_uri'),
                                                                        'displayText', 'Supervisor', 'uri', null) if rail.result('get_permissionset_uri') else null,

                "permission_set4": rail.find_first_by_attr_and_get_attr(rail.result('get_permissionset_uri'),
                                                                        'displayText', 'Project Resource', 'uri', null) if rail.result('get_permissionset_uri') else null,

                "permission_set1": rail.find_first_by_attr_and_get_attr(rail.result('get_permissionset_uri'),
                                                                        'displayText', 'Report User', 'uri', null) if rail.result('get_permissionset_uri') else null,

                "lookup_table": rail.result('statestreet_userimport_logtable'),

                "job_id": rail.render_template("{{dag_run_ecid()}}"),

                "file_name": dag_run.conf['file_path'],

                "supervisor_logtable": rail.result('supervisor_assignment_logtable'),

                "custom_field1": get_custombanktitle_uri(),

                "custom_field2": get_customstandardhours_uri(),

                "custom_field3": get_customfulltime_uri(),

                "check": (str(item['serviceline']) +
                          " | " + str(item['businessarea']) +
                          " | " + \
                          str(item['division']) + " | " + \
                          str(item['businessline']) +
                          " | " + str(item['subbusinessline'])),

                "location_uri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_locationlist_data')['locationlistinput'], 'fullpath', (str(item['region']) +
                                                                                " | " + str(item['country']) + " | " + str(item['city']) +
                                                                                " | " + str(item['locationcode'])), 'uri', ''),

                "legalentitycheckobject": get_department(item),

                "businessareacheckobject": {
                    "serviceline_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_cost_centers'),
                                                                            'displayText', item['serviceline'], 'uri', null) if item['serviceline'] else null,

                    "subbussinessline_uri": rail.find_first_by_attr_and_get_attr(rail.result(
                        'get_bussinessarea_child_hierarchy_details')['listinput'], 'name', (str(item['serviceline']) +
                                                                                            " | " + str(item['businessarea']) +
                                                                                            " | " + str(item['division']) + " | " + str(item['businessline']) +
                                                                                            " | " + str(item['subbusinessline'])), 'uri', '')
                },

                "jobfamily_uri": get_jobfamily_uri(item),

                "full_parttime_uri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_enabled_custom_field_dropdown_options'), 'displayText', item['fullparttime'],
                    'uri', null) if rail.result('get_enabled_custom_field_dropdown_options') else null,

                "bank_title_uri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_enabled_custom_field_drop_down_options'), 'displayText', item['banktitle'],
                    'uri', null) if rail.result('get_enabled_custom_field_drop_down_options') else null,
            }
        )

        wait_for_update_user_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_user_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_update_user_child") }}'
        )

        search_entries_in_lookup_table = rail.FilterLogEntriesOperator(
            task_id='search_entries_in_lookup_table',
            log="{{result('supervisor_assignment_logtable')}}",
            properties={
                'job_id': "{{ dag_run_ecid() }}",
            }
        )

        if_list_has_data_present = rail.IfOperator(
            task_id='if_list_has_data_present',
            test="{{result('search_entries_in_lookup_table','length') > 0}}",
            yes_task='process_supervisor_child',
            no_task='search_entries_in_userimport_logtable'
        )

        process_supervisor_child = rail.TriggerDagRunForEachItemOperator(
            task_id='process_supervisor_child',
            retries=0,
            items='{{ result("search_entries_in_lookup_table") }}',
            trigger_dag_id=f'statestreet_user_sync_supervisorassignment_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "login_id": item['properties']['user_id'].split("|")[0],
                "emp_id": item['properties']['user_id'].split("|")[-1],
                "manager_id": item['properties']['manager_id'],
                "entryid": item['properties']['job_id'],
                "useruri": item['properties']['user_uri'],
                "lookup_table": rail.result('statestreet_userimport_logtable'),
                "job_id": rail.render_template("{{dag_run_ecid()}}"),
                "supervisor_logtable": rail.result('supervisor_assignment_logtable')
            }
        )

        wait_for_supervisor_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_supervisor_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_supervisor_child") }}'
        )

        search_entries_in_userimport_logtable = rail.FilterLogEntriesOperator(
            task_id='search_entries_in_userimport_logtable',
            log="{{result('statestreet_userimport_logtable')}}",
            properties={
                'job_id': "{{ dag_run_ecid() }}",
            }
        )

        if_entries_has_data = rail.IfOperator(
            task_id='if_entries_has_data',
            test="{{result('search_entries_in_userimport_logtable' ,'length') > 0 }}",
            yes_task='create_csv',
            no_task='rename_input_to_archive_file'
        )

        create_csv = rail.WriteCSVFileOperator(
            task_id='create_csv',
            source=lambda: rail.result(
                'search_entries_in_userimport_logtable'),
            header=[
                'Job ID',
                'Login ID',
                'Emp ID',
                'User Name',
                'Status',
                'Details'],
            row=lambda item: [
                item['properties']['job_id'],
                item['properties']['field_name'].split("|")[0],
                item['properties']['field_name'].split("|")[1],
                item['properties']['field_name'].split("|")[2],
                item['properties']['status'],
                item['properties']['details']
            ]
        )

        get_logged_errors = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors',
            log="{{result('search_entries_in_userimport_logtable')}}",
            severity='Failed'
        )

        log_current_time = rail.PythonOperator(
            task_id='log_current_time',
            python_callable=lambda: datetime.now().strftime("%d-%m-%Y-%H-%M-%S") + "- PT"
        )

        log_current_date = rail.PythonOperator(
            task_id='log_current_date',
            python_callable=lambda: datetime.now(
                pytz.timezone("America/New_York")).strftime("%d%m%Y")
        )

        statestreet_userimport_counter = rail.CreateLogOperator(
            task_id='statestreet_userimport_counter'
        )

        search_entries_in_counter_table = rail.FilterLogEntriesOperator(
            task_id='search_entries_in_counter_table',
            log="{{result('statestreet_userimport_counter')}}",
            properties={
                'job_id': "{{ dag_run_ecid() }}",
                'date': "{{result('log_current_date')}}"
            }
        )

        def get_digit_size():
            counter_size = str((rail.result(
                'search_entries_in_counter_table', 'length')) + config.countsize)
            digit_size = len(counter_size)
            return {
                "c_s": counter_size,
                "d_s": digit_size
            }

        log_counter_size = rail.PythonOperator(
            task_id='log_counter_size',
            python_callable=get_digit_size
        )

        def get_file_number():
            if rail.result('log_counter_size')['d_s'] == 1:
                result = '00' + rail.result('log_counter_size')['c_s']
            elif rail.result('log_counter_size')['d_s'] == 2:
                result = '0' + rail.result('log_counter_size')['c_s']
            elif rail.result('log_counter_size')['d_s'] == 3:
                result = rail.result('log_counter_size')['c_s']
            else:
                result = None
            return result

        log_filenumber_72 = rail.PythonOperator(
            task_id='log_filenumber_72',
            python_callable=get_file_number
        )

        upload_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_logs_to_sftp',
            sftp_conn_id=config.sftp_client_conn_id,
            content="{{ result('create_csv')}}",
            remote_filepath=config.log_filepath + "/" + "Statestreet_user_" +
            "{{result('log_filenumber_72')}}" + "_" +
            "{{result('log_current_date')}}" + '.csv',
        )

        if_error_entries_not_present = rail.IfOperator(
            task_id='if_error_entries_not_present',
            test='''{{ result('get_logged_errors','length') == 0 }}''',
            yes_task="send_success_mail",
            no_task="send_completion_with_error_mail",
        )

        send_success_mail = rail.EmailOperator(
            task_id='send_success_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key()}} - Replicon User Import - Completed Successfully {{result("log_current_time")}}',
            html_content='templates/emails/update_completion_mail.html'
        )

        send_completion_with_error_mail = rail.EmailOperator(
            task_id='send_completion_with_error_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key()}} - Replicon User Import - Completed with errors {{result("log_current_time")}}',
            html_content='templates/emails/update_completion_mail_with_error.html'
        )

        statestreet_userimport_counter_add_entry = rail.WriteLogOperator(
            task_id='statestreet_userimport_counter_add_entry',
            log="{{ result('statestreet_userimport_counter') }}",
            message="na",
            severity="",
            properties={
                "jobid": "{{dag_run_ecid()}}",
                "date": "{{ result('log_current_date') }}"
            }
        )

        upload_logs = rail.SFTPUploadFileOperator(
            task_id='upload_logs',
            content="{{ result('create_csv')}}",
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.userimport_log_filepath + "/" +
            "Statestreet_user_" +
            "{{result('log_filenumber_72')}}" + "_" +
            "{{result('log_current_date')}}" + '.csv',
        )

        send_update_completion_mail = rail.EmailOperator(
            task_id='send_update_completion_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} - Replicon User Import - Completed with no changes ',
            html_content="templates/emails/update_completion_with_no_newchanges_mail.html",
        )

        upload_file_to_archive = rail.SFTPUploadFileOperator(
            task_id='upload_file_to_archive',
            sftp_conn_id=config.sftp_conn_id,
            content="{{ result('download_file')}}",
            remote_filepath=config.userimport_archive_filepath +
            "{{dag_run_ecid() | replace(':', '-')}}-{{dag_run.conf.file_path | file_name}}"
        )

        rename_input_to_archive_file = rail.SFTPMoveFileOperator(
            task_id='rename_input_to_archive_file',
            sftp_conn_id=config.sftp_client_conn_id,
            new_filename=config.archive_filepath +
            "{{dag_run_ecid() | replace(':', '-')}}_{{dag_run.conf.file_path | file_name}}",
            existing_filename="{{ dag_run.conf.file_path}}"
        )

        rename_reference_file = rail.SFTPMoveFileOperator(
            task_id='rename_reference_file',
            sftp_conn_id=config.sftp_conn_id,
            new_filename=config.userimport_archive_filepath +
            "{{dag_run_ecid() | replace(':', '-')}}_{{result('log_reference_filename')}}",
            existing_filename=config.reference_filepath + "/" +
            "{{ result('log_reference_filename')}}"
        )

        upload_to_reference = rail.SFTPUploadFileOperator(
            task_id='upload_to_reference',
            sftp_conn_id=config.sftp_conn_id,
            content="{{result('download_file')}}",
            remote_filepath=config.reference_filepath + "/" +
            "{{dag_run_ecid() | replace(':', '-')}}_{{dag_run.conf.file_path | file_name}}"
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> if_batch_execution_failed
        if_batch_execution_failed >> rail.Label(
            'Yes') >> stop_job_with_error >> finish
        if_batch_execution_failed >> rail.Label(
            'No') >> if_batch_execution_succedded
        if_batch_execution_succedded >> rail.Label(
            'Yes') >> load_csv >> create_collection_from_csv >> download_file >> upload_file_to_archive >> load_csv_from_input_file
        load_csv_from_input_file >> create_collection_from_input_file >> list_reference_files
        list_reference_files >> foreach_list_reference_files_do >> if_reference_files_ends_with_csv
        if_reference_files_ends_with_csv >> rail.Label(
            'Yes') >> log_reference_filename >> download_reference_file >> foreach_list_reference_files_do_end
        foreach_list_reference_files_do_end >> load_downloaded_reference_file >> get_enabled_divisions
        get_enabled_divisions >> get_enabled_locations
        get_enabled_locations >> get_enabled_cost_centers >> get_enabled_department_hierarchy_details
        get_enabled_department_hierarchy_details >> if_download_reference_file_is_blank >> rail.Label(
            'Yes') >> stop_job_with_error_message >> finish
        if_download_reference_file_is_blank >> rail.Label(
            'No') >> if_download_reference_file_has_data
        if_download_reference_file_has_data >> rail.Label(
            'Yes') >> create_collection_from_reference_file >> query_list_deltas
        query_list_deltas >> if_query_list_has_data_present >> rail.Label(
            'Yes') >> create_deltavalues_collection >> query_list_users_tobe_updated
        query_list_users_tobe_updated >> query_list_all_new_users >> statestreet_userimport_logtable
        statestreet_userimport_logtable >> supervisor_assignment_logtable >> get_policyset_uri
        get_policyset_uri >> get_permissionset_uri >> get_customfield_uri >> get_enabled_custom_field_dropdown_options
        get_enabled_custom_field_dropdown_options >> get_enabled_custom_field_drop_down_options >> get_locationlist_data
        get_locationlist_data >> get_jobfunction_child_hierarchy_data >> get_bussinessarea_child_hierarchy_details
        get_bussinessarea_child_hierarchy_details >> process_add_user_child
        process_add_user_child >> wait_for_process_add_user_child >> process_update_user_child >> wait_for_update_user_child
        wait_for_update_user_child >> search_entries_in_lookup_table
        search_entries_in_lookup_table >> if_list_has_data_present
        if_list_has_data_present >> rail.Label(
            'Yes') >> process_supervisor_child >> wait_for_supervisor_child >> search_entries_in_userimport_logtable
        if_list_has_data_present >> rail.Label(
            'No') >> search_entries_in_userimport_logtable >> if_entries_has_data
        if_entries_has_data >> rail.Label(
            'Yes') >> create_csv >> get_logged_errors >> log_current_time
        log_current_time >> log_current_date >> statestreet_userimport_counter >> search_entries_in_counter_table
        search_entries_in_counter_table >> log_counter_size >> log_filenumber_72
        log_filenumber_72 >> upload_logs_to_sftp >> if_error_entries_not_present
        if_error_entries_not_present >> rail.Label(
            'Yes') >> send_success_mail >> statestreet_userimport_counter_add_entry
        if_error_entries_not_present >> rail.Label(
            'No') >> send_completion_with_error_mail >> statestreet_userimport_counter_add_entry
        statestreet_userimport_counter_add_entry >> upload_logs >> rename_input_to_archive_file >> rename_reference_file
        if_entries_has_data >> rail.Label(
            'No') >> rename_input_to_archive_file >> rename_reference_file
        if_query_list_has_data_present >> rail.Label(
            'No') >> send_update_completion_mail >> rename_input_to_archive_file >> rename_reference_file >> upload_to_reference >> finish
        if_download_reference_file_has_data >> rail.Label(
            'No') >> finish
        if_reference_files_ends_with_csv >> rail.Label(
            'No') >> foreach_list_reference_files_do_end
        foreach_list_reference_files_do >> foreach_list_reference_files_do_end
        if_batch_execution_succedded >> rail.Label(
            'No') >> finish >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
