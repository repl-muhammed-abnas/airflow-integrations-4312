import rail
from eisner_amper.user_import.utils import response_filter, request_payload
from datetime import datetime, timedelta

# pylint: disable=too-many-statements


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.user_sync_child_dag_id,
        description=f"Eisner Amper Process each user Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        create_user_log = rail.CreateLogOperator(
            task_id='create_user_log'
        )

        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=request_payload.logging_details,
            op_args=[config.time_zone]
        )

        compose_csv = rail.WriteCSVFileOperator(
            task_id="compose_csv",
            source=lambda dag_run: dag_run.conf['webhook']['data']['root']['group'],
            header=[
                "name",
                "username",
                "personworkagreement",
                "personexternalid",
                "firstname",
                "lastname",
                "defaultemailaddress",
                "workagreementstatus",
                "payratetype",
                "jobexempt",
                "companycode",
                "companycodename",
                "costcenter",
                "costcenterdescription",
                "startdate",
                "enddate",
                "weeklyworkinghours",
                "workingtimepercentage",
                "role",
                "roledescription",
                "employeetype",
                "schedule",
                "timesheettemplate",
                "workweek",
                "workweekuri",
                "timesheetperiod",
                "timesheetapprovalpath",
                "timeentryapprovalpath",
                "roleeffectivedate",
                "worklocation",
                "worklocationid",
                "record_id"
            ],
            row=lambda item, **context: [
                item['@name'],
                item['YY1_EmpDataRepliconType']['UserName'].strip(
                ) if item['YY1_EmpDataRepliconType']['UserName'].strip() else None,
                item['YY1_EmpDataRepliconType']['PersonWorkAgreement'].strip(
                ) if item['YY1_EmpDataRepliconType']['PersonWorkAgreement'].strip() else None,
                item['YY1_EmpDataRepliconType']['PersonExternalID'].strip(
                ) if item['YY1_EmpDataRepliconType']['PersonExternalID'].strip() else None,
                item['YY1_EmpDataRepliconType']['FirstName'].strip(
                ) if item['YY1_EmpDataRepliconType']['FirstName'].strip() else None,
                item['YY1_EmpDataRepliconType']['LastName'].strip(
                ) if item['YY1_EmpDataRepliconType']['LastName'].strip() else None,
                item['YY1_EmpDataRepliconType']['DefaultEmailAddress'].strip(
                ) if item['YY1_EmpDataRepliconType']['DefaultEmailAddress'].strip() else None,
                item['YY1_EmpDataRepliconType']['WorkAgreementStatus'].strip(
                ) if item['YY1_EmpDataRepliconType']['WorkAgreementStatus'].strip() else None,
                item['YY1_EmpDataRepliconType']['PayRateType'].strip(),
                item['YY1_EmpDataRepliconType']['JobExempt'].strip(),
                item['YY1_EmpDataRepliconType']['CompanyCode'].strip(
                ) if item['YY1_EmpDataRepliconType']['CompanyCode'].strip() else None,
                item['YY1_EmpDataRepliconType']['CompanyCodeName'].strip(
                ) if item['YY1_EmpDataRepliconType']['CompanyCodeName'].strip() else None,
                item['YY1_EmpDataRepliconType']['CostCenter'].strip(
                ) if item['YY1_EmpDataRepliconType']['CostCenter'].strip() else None,
                item['YY1_EmpDataRepliconType']['CostCenterDescription'].strip(
                ) if item['YY1_EmpDataRepliconType']['CostCenterDescription'].strip() else None,
                item['YY1_EmpDataRepliconType']['StartDate'].strip(
                ) if item['YY1_EmpDataRepliconType']['StartDate'].strip() else None,
                item['YY1_EmpDataRepliconType']['EndDate'].strip(
                ) if item['YY1_EmpDataRepliconType']['EndDate'].strip() else None,
                item['YY1_EmpDataRepliconType']['WeeklyWorkingHours'].strip(
                ) if item['YY1_EmpDataRepliconType']['WeeklyWorkingHours'].strip() else None,
                item['YY1_EmpDataRepliconType']['WorkingTimePercentage'].strip(),
                item['YY1_EmpDataRepliconType']['YY1_Role_Data_RepliconType']['Role'].strip(
                ) if item['YY1_EmpDataRepliconType']['YY1_Role_Data_RepliconType']['Role'].strip() else None,
                item['YY1_EmpDataRepliconType']['YY1_Role_Data_RepliconType']['RoleDescription'].strip(
                ) if item['YY1_EmpDataRepliconType']['YY1_Role_Data_RepliconType']['RoleDescription'].strip() else None,
                request_payload.get_employee_type(
                    item)[0] if request_payload.get_employee_type(item) else None,
                request_payload.get_schedule(
                    item)[0] if request_payload.get_schedule(item) else None,
                request_payload.get_timesheettemplate(
                    item)[0] if request_payload.get_timesheettemplate(item) else None,
                request_payload.get_workweek(
                    item)[0] if request_payload.get_workweek(item) else None,
                request_payload.get_workuri(
                    item)[0] if request_payload.get_workuri(item) else None,
                request_payload.get_timesheetperiod(
                    item)[0] if request_payload.get_timesheetperiod(item) else None,
                request_payload.get_timesheetapprovalpath(
                    item)[0] if request_payload.get_timesheetapprovalpath(item) else None,
                request_payload.get_timeentryapprovalpath(
                    item)[0] if request_payload.get_timeentryapprovalpath(item) else None,
                item['YY1_EmpDataRepliconType']['YY1_Role_Data_RepliconType']['RoleEffectiveDate'].strip(),
                item['YY1_EmpDataRepliconType']['WorkLocation'].strip(),
                item['YY1_EmpDataRepliconType']['WorkLocationID'].strip(),
                context['index']

            ],

        )

        create_user_data_collection = rail.CreateCollectionOperator(
            task_id='create_user_data_collection',
            name='user_data',
            source="{{ result('compose_csv') }}"
        )

        query_invalid_input_records_with_no_work_agreement = rail.QueryCollectionOperator(
            task_id='query_invalid_input_records_with_no_work_agreement',
            query="SELECT * FROM user_data WHERE workagreementstatus IS NULL"
        )

        is_invalid_records_exists = rail.IfOperator(
            task_id='is_invalid_records_exists',
            test='{{ result("query_invalid_input_records_with_no_work_agreement", "length") > 0 }}',
            yes_task='log_invalid_data',
            no_task='query_invalid_input_records_with_disbaled_status'
        )

        log_invalid_data = rail.WriteLogOperator(
            task_id='log_invalid_data',
            message="WorkAgreementStatus is not present",
            items='{{result("query_invalid_input_records_with_no_work_agreement")}}',
            log='{{ result("create_user_log") }}',
            severity='Skipped',
            properties={
                'employeeid': "{{item.personexternalid}}",
                'loginname': "{{item.username}}",
                'action': "Validation",
                'status': "Skipped",
                'details': "WorkAgreementStatus is not present",
                'jobid': "{{dag_run_ecid()}}",
                'childjobid': '',
            }
        )

        query_invalid_input_records_with_disbaled_status = rail.QueryCollectionOperator(
            task_id='query_invalid_input_records_with_disbaled_status',
            query="SELECT * FROM user_data WHERE (NULLIF(username,'') IS NULL OR NULLIF(personworkagreement,'') IS NULL OR NULLIF(personexternalid,'') IS NULL) AND NULLIF(workagreementstatus,'') IS NOT NULL AND  workagreementstatus ='0'"
        )

        is_invalid_disabled_records_exists = rail.IfOperator(
            task_id='is_invalid_disabled_records_exists',
            test='{{ result("query_invalid_input_records_with_disbaled_status", "length") > 0 }}',
            yes_task='log_invalid_disbaled_data',
            no_task='query_valid_input_records_with_disbaled_status'
        )

        log_invalid_disbaled_data = rail.WriteLogOperator(
            task_id='log_invalid_disbaled_data',
            message="WorkAgreementStatus is not present",
            items='{{result("query_invalid_input_records_with_no_work_agreement")}}',
            log='{{ result("create_user_log") }}',
            severity='Skipped',
            properties={
                'employeeid': "{{item.personexternalid}}",
                'loginname': "{{item.username}}",
                'action': "Validation",
                'status': "Skipped",
                'details': '\
                {%- if item.personexternalid | is_falsy -%} \
                    PersonExternalID is not present, \
                {%- endif -%}\
                {%- if item.username | is_falsy -%} \
                    UserName is not present, \
                {%- endif -%}\
                {%- if item.personworkagreement | is_falsy -%} \
                    PersonWorkAgreement is not present, \
                {%- endif -%}\
                {%- if item.workagreementstatus | is_falsy -%} \
                    WorkAgreementStatus is not present, \
                {%- endif -%}',
                'jobid': "{{dag_run_ecid()}}",
                'childjobid': '',
            }
        )

        query_valid_input_records_with_disbaled_status = rail.QueryCollectionOperator(
            task_id='query_valid_input_records_with_disbaled_status',
            query="SELECT * FROM user_data WHERE NULLIF(username,'') IS NOT NULL AND NULLIF(personworkagreement,'') IS NOT NULL AND NULLIF(personexternalid,'') IS NOT NULL AND NULLIF(workagreementstatus,'') IS NOT NULL AND  workagreementstatus ='0'"
        )

        process_each_time_records = rail.trigger_parallel_dagrun(
            task_id='process_each_time_records',
            items='{{ result("query_valid_input_records_with_disbaled_status")}}',
            trigger_dag_id=lambda item: request_payload.get_trigger_id(
                config, item['record_id']),
            parallel_count=config.max_active_parallel_runs_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **dict(item.items()),
                "log": rail.result("create_user_log"),
                "parent_ecid": rail.render_template('{{ecid()}}'
                                                    ),
                "costcenteruri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_cost_center'), 'displayText', item['costcenterdescription'], 'uri'),
                "companycodeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_department_group'), 'displayText', item['companycodename'], 'uri'),
                "employeetypeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_employee_types'), 'displayText', item['employeetype'], 'uri'),
                "worklocationuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_locations'), 'displayText', item['worklocation'], 'uri'),
                "roleuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_divisions'), 'displayText', item['roledescription'], 'uri'),
                "sapudfuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', "SAP Employee ID", 'uri'),
                "Weeklyworkinghoursudfuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', "Weekly Working Hours", 'uri'),
                "notificationudfuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', "Notification", 'uri'),
                "batch_num": int(item['record_id']) % config.BATCH_COUNT
            }
        )

        query_invalid_input_records_with_enabled_status = rail.QueryCollectionOperator(
            task_id='query_invalid_input_records_with_enabled_status',
            query=F"""SELECT * FROM user_data WHERE (NULLIF(username,'') IS NULL OR NULLIF(personworkagreement,'') IS NULL OR 
            NULLIF(personexternalid,'') IS NULL OR NULLIF(firstname,'') IS NULL OR NULLIF(lastname,'') IS NULL OR NULLIF(defaultemailaddress,'') IS NULL 
            OR NULLIF(workagreementstatus,'') IS NULL OR NULLIF(role,'') IS NULL OR NULLIF(roledescription,'') IS NULL OR 
            NULLIF(companycodename,'') IS NULL OR NULLIF(companycode,'') IS NULL OR NULLIF(costcenter,'') IS NULL OR 
            NULLIF(costcenterdescription,'') IS NULL OR NULLIF(startdate,'') IS NULL OR NULLIF(enddate,'') IS NULL 
            OR NULLIF(weeklyworkinghours,'') IS NULL) AND workagreementstatus = '1' """
        )

        is_invalid_enabled_records_exists = rail.IfOperator(
            task_id='is_invalid_enabled_records_exists',
            test='{{ result("query_invalid_input_records_with_enabled_status", "length") > 0 }}',
            yes_task='log_invalid_enabled_data',
            no_task='query_valid_input_records_with_enabled_status'
        )

        log_invalid_enabled_data = rail.WriteLogOperator(
            task_id='log_invalid_enabled_data',
            message="validation",
            items='{{result("query_invalid_input_records_with_no_work_agreement")}}',
            log='{{ result("create_user_log") }}',
            severity='Skipped',
            properties={
                'employeeid': "{{item.personexternalid}}",
                'loginname': "{{item.username}}",
                'action': "Validation",
                'status': "Skipped",
                'details': '\
                {%- if item.personexternalid | is_falsy -%} \
                    PersonExternalID is not present, \
                {%- endif -%}\
                {%- if item.username | is_falsy -%} \
                    UserName is not present, \
                {%- endif -%}\
                {%- if item.personworkagreement | is_falsy -%} \
                    PersonWorkAgreement is not present, \
                {%- endif -%}\
                {%- if item.firstname | is_falsy -%} \
                    firstname is not present, \
                {%- endif -%}\
                {%- if item.lastname | is_falsy -%} \
                    lastname is not present, \
                {%- endif -%}\
                {%- if item.defaultemailaddress | is_falsy -%} \
                    defaultemailaddress is not present, \
                {%- endif -%}\
                {%- if item.workagreementstatus | is_falsy -%} \
                    workagreementstatus is not present, \
                {%- endif -%}\
                {%- if item.role | is_falsy -%} \
                    role is not present, \
                {%- endif -%}\
                {%- if item.roledescription | is_falsy -%} \
                    roledescription is not present, \
                {%- endif -%}\
                {%- if item.companycode | is_falsy -%} \
                    companycode is not present, \
                {%- endif -%}\
                {%- if item.costcenter | is_falsy -%} \
                    costcenter is not present, \
                {%- endif -%}\
                {%- if item.costcenterdescription | is_falsy -%} \
                    costcenterdescription is not present, \
                {%- endif -%}\
                {%- if item.startdate | is_falsy -%} \
                    startdate is not present, \
                {%- endif -%}\
                {%- if item.enddate | is_falsy -%} \
                    enddate is not present, \
                {%- endif -%}\
                {%- if item.weeklyworkinghours | is_falsy -%} \
                    weeklyworkinghours is not present, \
                {%- endif -%}',
                'jobid': "{{dag_run_ecid()}}",
                'childjobid': '',
            }
        )

        query_valid_input_records_with_enabled_status = rail.QueryCollectionOperator(
            task_id='query_valid_input_records_with_enabled_status',
            query=F"""SELECT * FROM user_data WHERE NULLIF(username,'') IS NOT NULL AND NULLIF(personworkagreement,'') IS NOT NULL AND 
            NULLIF(personexternalid,'') IS NOT NULL AND NULLIF(firstname,'') IS NOT NULL AND NULLIF(lastname,'') IS NOT NULL AND NULLIF(defaultemailaddress,'') IS NOT NULL 
            AND NULLIF(workagreementstatus,'') IS NOT NULL AND NULLIF(role,'') IS NOT NULL AND NULLIF(roledescription,'') IS NOT NULL AND 
            NULLIF(companycodename,'') IS NOT NULL AND NULLIF(companycode,'') IS NOT NULL AND NULLIF(costcenter,'') IS NOT NULL AND 
            NULLIF(costcenterdescription,'') IS NOT NULL AND NULLIF(startdate,'') IS NOT NULL AND NULLIF(enddate,'') IS NOT NULL 
            AND NULLIF(weeklyworkinghours,'') IS NOT NULL AND workagreementstatus = '1' """
        )

        is_username_present = rail.IfOperator(
            task_id='is_username_present',
            test='{{ result("query_valid_input_records_with_enabled_status", "length") > 0 }}',
            yes_task='create_valid_data_collection',
            no_task='format_logs'
        )

        create_valid_data_collection = rail.CreateCollectionOperator(
            task_id='create_valid_data_collection',
            name='valid_data',
            source="{{ result('query_valid_input_records_with_enabled_status') }}"
        )

        get_all_cost_center = rail.RepliconServiceOperator(
            task_id='get_all_cost_center',
            endpoint='/services/CostCenterService1.svc/GetAllCostCenters'
        )

        get_all_department_group = rail.RepliconServiceOperator(
            task_id='get_all_department_group',
            endpoint='/services/DepartmentGroupService1.svc/GetAllDepartmentGroups'
        )

        get_all_divisions = rail.RepliconServiceOperator(
            task_id='get_all_divisions',
            endpoint='/services/DivisionService1.svc/GetAllDivisions'
        )

        get_all_locations = rail.RepliconServiceOperator(
            task_id='get_all_locations',
            endpoint='/services/LocationService1.svc/GetAllLocations'
        )

        create_cost_center_collection = rail.CreateCollectionOperator(
            task_id='create_cost_center_collection',
            name='cost_center',
            source=lambda: rail.load_all_records(
                rail.result('get_all_cost_center'))
        )

        query_distinct_costcenter = rail.QueryCollectionOperator(
            task_id='query_distinct_costcenter',
            query=F"""SELECT DISTINCT costcenter,costcenterdescription from valid_data WHERE NULLIF(costcenter, '') IS NOT NULL AND NULLIF(costcenterdescription, '') IS NOT NULL AND LOWER(costcenterdescription)
            NOT IN (SELECT DISTINCT LOWER(displayText) FROM cost_center)"""
        )

        process_each_cost_center_records = rail.trigger_parallel_dagrun(
            task_id='process_each_cost_center_records',
            items='{{ result("query_distinct_costcenter")}}',
            trigger_dag_id=config.user_sync_cost_center_child_dag_id,
            parallel_count=config.max_active_parallel_runs_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **dict(item.items()),
                "log": rail.result("create_user_log"),
                "parent_ecid": rail.render_template('{{ecid()}}'
                                                    )
            }
        )

        create_company_code_collection = rail.CreateCollectionOperator(
            task_id='create_company_code_collection',
            name='company_code',
            source=lambda: rail.load_all_records(
                rail.result('get_all_department_group'))
        )

        query_distinct_company_code = rail.QueryCollectionOperator(
            task_id='query_distinct_company_code',
            query=F"""SELECT DISTINCT companycode,companycodename from valid_data WHERE  NULLIF(companycode, '') IS NOT NULL AND NULLIF(companycodename, '') IS NOT NULL AND LOWER(companycodename)
            NOT IN (SELECT DISTINCT LOWER(displayText) FROM company_code)"""
        )

        process_each_company_code_records = rail.trigger_parallel_dagrun(
            task_id='process_each_company_code_records',
            items='{{ result("query_distinct_company_code")}}',
            trigger_dag_id=config.user_sync_company_code_child_dag_id,
            parallel_count=config.max_active_parallel_runs_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **dict(item.items()),
                "Companydepturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_department_group'), 'displayText', "EisnerAmper", 'uri')
            }
        )

        create_work_location_collection = rail.CreateCollectionOperator(
            task_id='create_work_location_collection',
            name='work_location',
            source=lambda: rail.load_all_records(
                rail.result('get_all_locations'))
        )

        query_distinct_work_location = rail.QueryCollectionOperator(
            task_id='query_distinct_work_location',
            query=F"""SELECT DISTINCT worklocation,worklocationid from valid_data WHERE NULLIF(worklocation, '') IS NOT NULL AND NULLIF(worklocationid, '') IS NOT NULL AND LOWER(worklocation)
            NOT IN (SELECT DISTINCT LOWER(displayText) FROM work_location)"""
        )

        process_each_work_location_records = rail.trigger_parallel_dagrun(
            task_id='process_each_work_location_records',
            items='{{ result("query_distinct_work_location")}}',
            trigger_dag_id=config.user_sync_work_location_child_dag_id,
            parallel_count=config.max_active_parallel_runs_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **dict(item.items())
            }
        )

        create_roles_collection = rail.CreateCollectionOperator(
            task_id='create_roles_collection',
            name='roles',
            source=lambda: rail.load_all_records(
                rail.result('get_all_divisions'))
        )

        query_distinct_roles = rail.QueryCollectionOperator(
            task_id='query_distinct_roles',
            query=F"""SELECT DISTINCT role,roledescription from valid_data WHERE NULLIF(role, '') IS NOT NULL AND NULLIF(roledescription, '') IS NOT NULL AND LOWER(roledescription)
            NOT IN (SELECT DISTINCT LOWER(displayText) FROM roles)"""
        )

        process_each_roles_records = rail.trigger_parallel_dagrun(
            task_id='process_each_roles_records',
            items='{{ result("query_distinct_roles")}}',
            trigger_dag_id=config.user_sync_roles_child_dag_id,
            parallel_count=config.max_active_parallel_runs_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **dict(item.items())
            }
        )

        get_all_cost_centers = rail.RepliconServiceOperator(
            task_id='get_all_cost_centers',
            endpoint='/services/CostCenterService1.svc/GetAllCostCenters'
        )

        get_all_department_groups = rail.RepliconServiceOperator(
            task_id='get_all_department_groups',
            endpoint='/services/DepartmentGroupService1.svc/GetAllDepartmentGroups'
        )

        get_all_division = rail.RepliconServiceOperator(
            task_id='get_all_division',
            endpoint='/services/DivisionService1.svc/GetAllDivisions'
        )

        get_all_location = rail.RepliconServiceOperator(
            task_id='get_all_location',
            endpoint='/services/LocationService1.svc/GetAllLocations'
        )

        get_all_employee_types = rail.RepliconServiceOperator(
            task_id='get_all_employee_types',
            endpoint='/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups'
        )

        get_all_custom_fields = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data=request_payload.get_all_custom_fields_payload
        )

        process_each_valid_user_records = rail.trigger_parallel_dagrun(
            task_id='process_each_valid_user_records',
            items='{{ result("query_valid_input_records_with_enabled_status")}}',
            trigger_dag_id=lambda item: request_payload.get_trigger_id(
                config, item['record_id']),
            parallel_count=config.max_active_parallel_runs_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **dict(item.items()),
                "log": rail.result("create_user_log"),
                "parent_ecid": rail.render_template('{{ecid()}}'
                                                    ),
                "costcenteruri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_cost_centers'), 'displayText', item['costcenterdescription'], 'uri'),
                "companycodeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_department_groups'), 'displayText', item['companycodename'], 'uri'),
                "employeetypeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_employee_types'), 'displayText', item['employeetype'], 'uri'),
                "worklocationuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_location'), 'displayText', item['worklocation'], 'uri'),
                "roleuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_division'), 'displayText', item['roledescription'], 'uri'),
                "sapudfuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', "SAP Employee ID", 'uri'),
                "Weeklyworkinghoursudfuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', "Weekly Working Hours", 'uri'),
                "notificationudfuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', "Notification", 'uri'),
                "batch_num": int(item['record_id']) % config.BATCH_COUNT
            }
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=request_payload.do_format_logs,
            show_return_value_in_logs=False
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=['Employee ID', 'Login name',
                    'Action', 'Status', 'Details', 'jobid'],
            row=['{{ item.employeeid }}', '{{ item.loginname}}', '{{ item.action }}',
                 '{{ item.status }}', '{{ item.details }}', '{{ item.jobid }}'],
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='log_{{dag_run_ecid() | replace(":", "-")}}_{{ result("get_logging_details").timerange }}'+".csv",
            expires_in_seconds=7*24*60*60,
        )

        upload_to_client_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_to_client_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.client_user_log_path +
            'log_{{dag_run_ecid() | replace(":", "-")}}_{{ result("get_logging_details").timerange }}'+".csv",
            sftp_conn_id=config.sftp_conn_id
        )

        upload_to_internal_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_to_internal_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.internal_user_log_path +
            'log_{{dag_run_ecid() | replace(":", "-")}}_{{ result("get_logging_details").timerange }}'+".csv",
            sftp_conn_id=config.sftp_conn_internal_id
        )

        any_records_failed = rail.IfOperator(
            task_id='any_records_failed',
            test="{{ result('format_logs', 'error_record_count') > 0 }}",
            yes_task='send_completion_error_mail',
            no_task='send_completion_mail'
        )

        send_completion_mail = rail.EmailOperator(
            task_id='send_completion_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon user sync -  completed successfully at {{ current_time_in_specified_tz("America/New_York") }}',
            html_content="templates/emails/import_complete.html"
        )

        send_completion_error_mail = rail.EmailOperator(
            task_id='send_completion_error_mail',
            to=config.tenant_email,
            bcc=config.alert_email,
            subject='{{ get_company_key() }} | Replicon user sync is completed with error at {{ current_time_in_specified_tz("America/New_York") }}',
            html_content="templates/emails/import_with_error.html"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
            trigger_rule='all_done'
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        create_user_log >> get_logging_details >> compose_csv >> create_user_data_collection >> query_invalid_input_records_with_no_work_agreement \
            >> is_invalid_records_exists >> rail.Label("Yes") >> log_invalid_data >> query_invalid_input_records_with_disbaled_status

        is_invalid_records_exists >> rail.Label("No") >> query_invalid_input_records_with_disbaled_status\
            >> is_invalid_disabled_records_exists >> rail.Label("Yes") >> log_invalid_disbaled_data >> query_valid_input_records_with_disbaled_status \
            >> process_each_time_records >> query_invalid_input_records_with_enabled_status >> is_invalid_enabled_records_exists >> rail.Label("Yes") >> log_invalid_enabled_data >> query_valid_input_records_with_enabled_status\
            >> is_username_present >> rail.Label("Yes") >> create_valid_data_collection >> get_all_cost_center >> get_all_department_group \
            >> get_all_divisions >> get_all_locations >> create_cost_center_collection >> query_distinct_costcenter >> process_each_cost_center_records \
            >> create_company_code_collection >> query_distinct_company_code >> process_each_company_code_records >> create_work_location_collection \
            >> query_distinct_work_location >> process_each_work_location_records >> create_roles_collection >> query_distinct_roles >> process_each_roles_records\
            >> get_all_cost_centers >> get_all_department_groups >> get_all_division >> get_all_location >> get_all_employee_types >> get_all_custom_fields >> process_each_valid_user_records >> rail.Label(
            "Always") >> format_logs >> render_logs_csv >> generate_download_link >> upload_to_client_sftp >> upload_to_internal_sftp \
            >> any_records_failed >> rail.Label("Yes") >> send_completion_error_mail >> log_to_sumo

        any_records_failed >> rail.Label(
            "No") >> send_completion_mail >> log_to_sumo >> can_fail_dag >> fail_dagrun

        is_invalid_disabled_records_exists >> rail.Label(
            "No") >> query_valid_input_records_with_disbaled_status

        is_invalid_enabled_records_exists >> rail.Label(
            "No") >> query_valid_input_records_with_enabled_status >> is_username_present

        is_username_present >> rail.Label(
            "No") >> format_logs >> render_logs_csv

    return dag


rail.for_each_instance(create_child_dag)
