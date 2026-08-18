from datetime import timedelta
from airflow.models import Variable
import rail
from statestreet.user_sync.utils.response_filter import get_uri

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'statestreet_user_sync_add_user_child_{config.instance}',
        description=f'Statestreet_user_sync_add_user_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_childs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_loginid_blank'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_loginid_blank',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        if_loginid_blank = rail.IfOperator(
            task_id='if_loginid_blank',
            test="{{ dag_run.conf.user_items.loginid | is_falsy }}",
            yes_task="add_failure_entries",
            no_task="search_users",
        )

        add_failure_entries = rail.WriteLogOperator(
            task_id='add_failure_entries',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" + "Login ID must be provided",
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Failed",

            }
        )

        search_users = rail.RepliconServiceOperator(
            task_id='search_users',
            endpoint='/services/UserListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "50",
                "columnUris": [
                    "urn:replicon:user-list-column:login-name"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": "{{dag_run.conf.user_items.loginid}}"
                        }
                    }
                }
            },
            data_handler=get_uri
        )

        if_log_uri_is_present = rail.IfOperator(
            task_id='if_log_uri_is_present',
            test="{{result('search_users')['input'] | is_truthy }}",
            yes_task="log_failure_entries_for_loginname",
            no_task="if_empid_blank",
        )

        log_failure_entries_for_loginname = rail.WriteLogOperator(
            task_id='log_failure_entries_for_loginname',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" + dag_run.conf['user_items']['loginid'] + " already exists",
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Failed",

            }
        )

        if_empid_blank = rail.IfOperator(
            task_id='if_empid_blank',
            test="{{ dag_run.conf.user_items.employeeid | is_falsy }}",
            yes_task="add_failure_entries_for_empid",
            no_task="if_email_blank",
        )

        add_failure_entries_for_empid = rail.WriteLogOperator(
            task_id='add_failure_entries_for_empid',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" + "Emp ID cannot be blank",
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Failed",

            }
        )

        if_email_blank = rail.IfOperator(
            task_id='if_email_blank',
            test="{{ dag_run.conf.user_items.email | is_falsy }}",
            yes_task="add_failure_entries_for_email",
            no_task="if_costcentername_blank",
        )

        add_failure_entries_for_email = rail.WriteLogOperator(
            task_id='add_failure_entries_for_email',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" + "Email cannot be blank",
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Failed",

            }
        )

        if_costcentername_blank = rail.IfOperator(
            task_id='if_costcentername_blank',
            test="{{ dag_run.conf.user_items.costcentername | is_falsy }}",
            yes_task="add_failure_entries_for_costcentrename",
            no_task="if_costcenternumber_blank",
        )

        add_failure_entries_for_costcentrename = rail.WriteLogOperator(
            task_id='add_failure_entries_for_costcentrename',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" + "Cost Center Name cannot be blank",
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Failed",

            }
        )

        if_costcenternumber_blank = rail.IfOperator(
            task_id='if_costcenternumber_blank',
            test="{{ dag_run.conf.user_items.costcenternumber | is_falsy }}",
            yes_task="add_failure_entries_for_costcenternumber",
            no_task="if_legalentityname_blank",
        )

        add_failure_entries_for_costcenternumber = rail.WriteLogOperator(
            task_id='add_failure_entries_for_costcenternumber',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" + "Cost Center Number cannot be blank",
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Failed",

            }
        )

        if_legalentityname_blank = rail.IfOperator(
            task_id='if_legalentityname_blank',
            test="{{ dag_run.conf.user_items.legalentityname | is_falsy }}",
            yes_task="add_failure_entries_for_legalentityname",
            no_task="create_new_draft",
        )

        add_failure_entries_for_legalentityname = rail.WriteLogOperator(
            task_id='add_failure_entries_for_legalentityname',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details":  "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" + "Legal Entity Name cannot be blank",
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Failed",

            }
        )

        create_new_draft = rail.RepliconServiceOperator(
            task_id='create_new_draft',
            endpoint="/services/UserService1.svc/CreateNewDraft"
        )

        if_request_email_contains_special_character = rail.IfOperator(
            task_id='if_request_email_contains_special_character',
            test="{{ dag_run.conf.user_items.email | matches('@') }}",
            yes_task="update_email",
            no_task="add_failure_entries_for_invalidemail",
        )

        update_email = rail.RepliconServiceOperator(
            task_id='update_email',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('create_new_draft') }}",
                "email": "{{ dag_run.conf.user_items.email }}"
            }
        )

        add_failure_entries_for_invalidemail = rail.WriteLogOperator(
            task_id='add_failure_entries_for_invalidemail',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User - " + rail.render_template("{{dag_run_ecid()}}") + "-" + dag_run.conf['user_items']['email'],
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Failed",

            }
        )

        if_name_present = rail.IfOperator(
            task_id='if_name_present',
            test="{{ dag_run.conf.user_items.name | is_truthy }}",
            yes_task="if_name_has_character",
            no_task="add_failure_entries_for_invalidname",
        )

        if_name_has_character = rail.IfOperator(
            task_id='if_name_has_character',
            test="{{ dag_run.conf.user_items.name | matches(',') }}",
            yes_task="log_first_and_last_names",
            no_task="add_failure_entries_for_invalidname_format",
        )

        log_first_and_last_names = rail.PythonOperator(
            task_id='log_first_and_last_names',
            python_callable=lambda dag_run: {
                "first_name": dag_run.conf['user_items']['name'].split(", ")[1] if dag_run.conf['user_items']['name'] else null,
                "last_name": dag_run.conf['user_items']['name'].split(
                    ", ")[0] if dag_run.conf['user_items']['name'] else null
            }
        )

        if_log_first_and_lastname_present = rail.IfOperator(
            task_id='if_log_first_and_lastname_present',
            test="{{result('log_first_and_last_names').first_name | is_truthy and result('log_first_and_last_names').last_name | is_truthy}}",
            yes_task="update_first_name",
            no_task="if_log_first_and_lastname_not_present",
        )

        update_first_name = rail.RepliconServiceOperator(
            task_id='update_first_name',
            endpoint="/services/UserService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ result('create_new_draft') }}",
                "firstname": "{{ result('log_first_and_last_names').first_name }}"
            }
        )

        update_last_name = rail.RepliconServiceOperator(
            task_id='update_last_name',
            endpoint="/services/UserService1.svc/UpdateLastName",
            data={
                "userUri": "{{ result('create_new_draft') }}",
                "lastname": "{{result('log_first_and_last_names').last_name }}"
            }
        )

        if_log_first_and_lastname_not_present = rail.IfOperator(
            task_id='if_log_first_and_lastname_not_present',
            test="{{result('log_first_and_last_names').first_name | is_falsy or result('log_first_and_last_names').last_name | is_falsy}}",
            yes_task="add_failure_entries_for_invalid_formatname",
            no_task="update_employee_id",
        )

        add_failure_entries_for_invalid_formatname = rail.WriteLogOperator(
            task_id='add_failure_entries_for_invalid_formatname',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" +
                "Invalid name format provided" + "-" +
                    dag_run.conf['user_items']['name'],
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Failed",

            }
        )

        update_employee_id = rail.RepliconServiceOperator(
            task_id='update_employee_id',
            endpoint="/services/UserService1.svc/UpdateEmployeeId",
            data={
                "userUri": "{{ result('create_new_draft') }}",
                "employeeId": "{{ dag_run.conf.user_items.employeeid }}"
            }
        )

        add_failure_entries_for_invalidname_format = rail.WriteLogOperator(
            task_id='add_failure_entries_for_invalidname_format',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" +
                "Invalid name format provided" + "-" +
                    dag_run.conf['user_items']['name'],
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Failed",

            }
        )

        add_failure_entries_for_invalidname = rail.WriteLogOperator(
            task_id='add_failure_entries_for_invalidname',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" + "Name cannot be blank",
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Failed",

            }
        )

        if_request_emptype_present = rail.IfOperator(
            task_id='if_request_emptype_present',
            test="{{ dag_run.conf.user_items.employeetype | is_truthy }}",
            yes_task="get_all_employee_type_details",
            no_task="add_failure_entries_for_emptype",
        )

        get_all_employee_type_details = rail.RepliconServiceOperator(
            task_id='get_all_employee_type_details',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails"
        )

        log_employee_uri = rail.PythonOperator(
            task_id='log_employee_uri',
            python_callable=lambda dag_run:  rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_employee_type_details'), 'displayText', dag_run.conf['user_items']['employeetype'], 'uri', null)
        )

        if_log_employee_uri_present = rail.IfOperator(
            task_id='if_log_employee_uri_present',
            test="{{ result('log_employee_uri') | is_truthy }}",
            yes_task="update_employee_type_for_user",
            no_task="add_failure_entries_for_employee_update",
        )

        update_employee_type_for_user = rail.RepliconServiceOperator(
            task_id='update_employee_type_for_user',
            endpoint="/services/EmployeeTypeService1.svc/UpdateEmployeeTypeForUser",
            data={
                "userUri": "{{ result('create_new_draft') }}",
                "employeeTypeUri": "{{ result('log_employee_uri') }}"
            }
        )

        add_failure_entries_for_employee_update = rail.WriteLogOperator(
            task_id='add_failure_entries_for_employee_update',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" +
                "Employee type not found" + "-" +
                    dag_run.conf['user_items']['employeeid'],
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Failed",

            }
        )

        add_failure_entries_for_emptype = rail.WriteLogOperator(
            task_id='add_failure_entries_for_emptype',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" + "Employee Type cannot be blank",
                "field_name": " ",
                "status": "Failed",

            }
        )

        if_legal_entityname_present = rail.IfOperator(
            task_id='if_legal_entityname_present',
            test="{{ dag_run.conf.user_items.legalentityname | is_truthy }}",
            yes_task="if_legal_entity_uri_present",
            no_task="add_failure_entries_for_legalentity",
        )

        if_legal_entity_uri_present = rail.IfOperator(
            task_id='if_legal_entity_uri_present',
            test="{{ dag_run.conf.legalentitycheckobject.legalentityuri | is_truthy}}",
            yes_task="if_isenabled_is_true",
            no_task="add_failure_entries_for_legalentity_uri",
        )

        if_isenabled_is_true = rail.IfOperator(
            task_id='if_isenabled_is_true',
            test="{{ dag_run.conf.legalentitycheckobject.islegalentityenabled | is_truthy }}",
            yes_task="if_log_cost_center_number_uri_present",
            no_task="add_failure_entries_for_isenabled",
        )

        if_log_cost_center_number_uri_present = rail.IfOperator(
            task_id='if_log_cost_center_number_uri_present',
            test="{{ dag_run.conf.legalentitycheckobject.costcenteruri | is_truthy }}",
            yes_task="if_isenabled_is_true_for_costcenter",
            no_task="add_failure_entries_for_costcenter_data",
        )

        if_isenabled_is_true_for_costcenter = rail.IfOperator(
            task_id='if_isenabled_is_true_for_costcenter',
            test="{{dag_run.conf.legalentitycheckobject.iscostcenterenabled | is_truthy }}",
            yes_task="update_department_for_user",
            no_task="add_failure_entries_for_costcenter_isenabled",
        )

        update_department_for_user = rail.RepliconServiceOperator(
            task_id='update_department_for_user',
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            data={
                "userUri": "{{ result('create_new_draft') }}",
                "departmentUri": "{{ dag_run.conf.legalentitycheckobject.costcenteruri }}"
            }
        )

        add_failure_entries_for_costcenter_isenabled = rail.WriteLogOperator(
            task_id='add_failure_entries_for_costcenter_isenabled',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" +
                "Cost Center Number provided is disabled " + "-" +
                    dag_run.conf['user_items']['costcenternumber'],
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Failed",

            }
        )

        add_failure_entries_for_costcenter_data = rail.WriteLogOperator(
            task_id='add_failure_entries_for_costcenter_data',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" +
                "Incorrect Cost Center Number " + "-" +
                    dag_run.conf['user_items']['costcenternumber'],
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Failed",

            }
        )

        add_failure_entries_for_isenabled = rail.WriteLogOperator(
            task_id='add_failure_entries_for_isenabled',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" +
                "Legal Entity provided is disabled" + "-" +
                    dag_run.conf['user_items']['legalentityname'],
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Failed",

            }
        )

        add_failure_entries_for_legalentity_uri = rail.WriteLogOperator(
            task_id='add_failure_entries_for_legalentity_uri',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" +
                "Incorrect Legal Entity Name" + "-" +
                    dag_run.conf['user_items']['legalentityname'],
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Failed",

            }
        )

        add_failure_entries_for_legalentity = rail.WriteLogOperator(
            task_id='add_failure_entries_for_legalentity',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" + "No Legal Entity name provided",
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Failed",

            }
        )

        set_s_s_o_authentication_for_user = rail.RepliconServiceOperator(
            task_id='set_s_s_o_authentication_for_user',
            endpoint="/services/securityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('create_new_draft') }}",
                "loginName": "{{ dag_run.conf.user_items.loginid }}"
            }
        )

        put_product_assignments_for_user = rail.RepliconServiceOperator(
            task_id='put_product_assignments_for_user',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data={
                "userUri": "{{ result('create_new_draft') }}",
                "productUris": [
                    "urn:replicon-saas:product:time-bill-plus"
                ]
            }
        )

        publish_draft = rail.RepliconServiceOperator(
            task_id='publish_draft',
            endpoint="/services/UserService1.svc/PublishDraft",
            data={
                "draftUri": "{{ result('create_new_draft') }}"
            }
        )

        if_log_user_uri_present = rail.IfOperator(
            task_id='if_log_user_uri_present',
            test="{{ result('publish_draft').uri| is_truthy }}",
            yes_task="add_success_entries_for_useruri",
            no_task="assignpolicy_set_to_user_widget_timesheet",
        )

        add_success_entries_for_useruri = rail.WriteLogOperator(
            task_id='add_success_entries_for_useruri',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" +
                "User Added Successfully with Login ID" +
                    dag_run.conf['user_items']['loginid'],
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Success",

            }
        )

        assignpolicy_set_to_user_widget_timesheet = rail.RepliconServiceOperator(
            task_id='assignpolicy_set_to_user_widget_timesheet',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data=lambda dag_run: {
                "userUri": rail.result('publish_draft')['uri'],
                "policySetUri": dag_run.conf['policyset_uri']
            }
        )

        if_request_banktitle_present = rail.IfOperator(
            task_id='if_request_banktitle_present',
            test="{{ dag_run.conf.user_items.banktitle | is_truthy }}",
            yes_task="if_log_banktitle_uri_present",
            no_task="if_request_fullparttime_present",
        )

        if_log_banktitle_uri_present = rail.IfOperator(
            task_id='if_log_banktitle_uri_present',
            test="{{ dag_run.conf.bank_title_uri | is_truthy }}",
            yes_task="update_dropdown_value",
            no_task="add_ignored_entries_for_banktitle",
        )

        update_dropdown_value = rail.RepliconServiceOperator(
            task_id='update_dropdown_value',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": rail.result('publish_draft')['uri'],
                "customFieldUri": dag_run.conf['custom_field1'],
                "customFieldDropDownOptionUri": dag_run.conf['bank_title_uri']
            }
        )

        if_request_banktitle_equals_to_managingdirector = rail.IfOperator(
            task_id='if_request_banktitle_equals_to_managingdirector',
            test="{{ dag_run.conf.user_items.banktitle == 'Managing Director' }}",
            yes_task="put_permission_set_assignments_for_user_supervisor",
            no_task="if_request_fullparttime_present",
        )

        put_permission_set_assignments_for_user_supervisor = rail.RepliconServiceOperator(
            task_id='put_permission_set_assignments_for_user_supervisor',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": rail.result('publish_draft')['uri'],
                "permissionSetUris": [dag_run.conf['permission_set4'], dag_run.conf['permission_set2']
                                      ]
            }
        )

        remove_policy_set_assignment_from_user = rail.RepliconServiceOperator(
            task_id='remove_policy_set_assignment_from_user',
            endpoint="/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data=lambda dag_run: {
                "userUri": rail.result('publish_draft')['uri'],
                "policySetUri": dag_run.conf['policyset_uri']
            }
        )

        add_ignored_entries_for_banktitle = rail.WriteLogOperator(
            task_id='add_ignored_entries_for_banktitle',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Ignored",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" +
                "Invalid value for Bank Title provided" +
                    dag_run.conf['user_items']['banktitle'],
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Ignored",

            }
        )

        if_request_standardhours_present = rail.IfOperator(
            task_id='if_request_standardhours_present',
            test='''{{ dag_run.conf.user_items.standardhours | is_truthy }}''',
            yes_task="update_numeric_value",
            no_task="on_error",
        )

        update_numeric_value = rail.RepliconServiceOperator(
            task_id='update_numeric_value',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data=lambda dag_run: {
                "objectUri": rail.result('publish_draft')['uri'],
                "customFieldUri": dag_run.conf['custom_field2'],
                "value": dag_run.conf['user_items']['standardhours']
            }
        )

        if_request_fullparttime_present = rail.IfOperator(
            task_id='if_request_fullparttime_present',
            test="{{ dag_run.conf.user_items.fullparttime | is_truthy }}",
            yes_task="if_log_fullparttime_uri_present",
            no_task="if_request_banktitle_not_equals_to_managingdirector",
        )

        if_log_fullparttime_uri_present = rail.IfOperator(
            task_id='if_log_fullparttime_uri_present',
            test="{{ dag_run.conf.full_parttime_uri | is_truthy }}",
            yes_task="update_drop_down_value",
            no_task="add_ignored_entries_for_fullparttime",
        )

        update_drop_down_value = rail.RepliconServiceOperator(
            task_id='update_drop_down_value',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": rail.result('publish_draft')['uri'],
                "customFieldUri": dag_run.conf['custom_field3'],
                "customFieldDropDownOptionUri": dag_run.conf['full_parttime_uri']
            }
        )

        add_ignored_entries_for_fullparttime = rail.WriteLogOperator(
            task_id='add_ignored_entries_for_fullparttime',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Ignored",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" +
                "Invalid value for Full/Part Time provided" +
                    dag_run.conf['user_items']['fullparttime'],
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Ignored",

            }
        )

        if_request_banktitle_not_equals_to_managingdirector = rail.IfOperator(
            task_id='if_request_banktitle_not_equals_to_managingdirector',
            test="{{ dag_run.conf.user_items.banktitle != 'Managing Director' }}",
            yes_task="if_request_managernonmanager_present",
            no_task="if_request_region_present",
        )

        if_request_managernonmanager_present = rail.IfOperator(
            task_id='if_request_managernonmanager_present',
            test="{{ dag_run.conf.user_items.managernonmanager | is_truthy }}",
            yes_task="if_request_managernonmanager_equals_to_yes",
            no_task="put_permission_set_assignments_for_user_project_resource",
        )

        if_request_managernonmanager_equals_to_yes = rail.IfOperator(
            task_id='if_request_managernonmanager_equals_to_yes',
            test="{{ dag_run.conf.user_items.managernonmanager == 'Yes' }}",
            yes_task="put_permission_set_assignments_for_user_supervisorand_project",
            no_task="put_permission_set_assignments_for_user_project_resource",
        )

        put_permission_set_assignments_for_user_supervisorand_project = rail.RepliconServiceOperator(
            task_id='put_permission_set_assignments_for_user_supervisorand_project',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": rail.result('publish_draft')['uri'],
                "permissionSetUris": [
                    dag_run.conf['permission_set1'], dag_run.conf['permission_set2']
                ]
            }
        )

        put_permission_set_assignments_for_user_project_resource = rail.RepliconServiceOperator(
            task_id='put_permission_set_assignments_for_user_project_resource',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": rail.result('publish_draft')['uri'],
                "permissionSetUris": [dag_run.conf['permission_set3']
                                      ]
            }
        )

        if_request_region_present = rail.IfOperator(
            task_id='if_request_region_present',
            test="{{ dag_run.conf.user_items.region | is_truthy }}",
            yes_task="if_request_regionuri_present",
            no_task="add_ignored_entries_for_region",
        )

        if_request_regionuri_present = rail.IfOperator(
            task_id='if_request_regionuri_present',
            test="{{ dag_run.conf.region_uri | is_truthy }}",
            yes_task="if_request_locationuri_present",
            no_task="add_ignored_entries_for_region_uri",
        )

        if_request_locationuri_present = rail.IfOperator(
            task_id='if_request_locationuri_present',
            test="{{ dag_run.conf.location_uri | is_truthy }}",
            yes_task="put_location_schedule_for_user",
            no_task="add_ignored_entries_for_location_uri",
        )

        put_location_schedule_for_user = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": "{{ result('publish_draft').uri }}",
                "scheduleEntries": [
                    {
                        "location": {
                            "uri": "{{ dag_run.conf.location_uri }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        add_ignored_entries_for_location_uri = rail.WriteLogOperator(
            task_id='add_ignored_entries_for_location_uri',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Ignored",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" +
                "Location not present or disabled in Replicon" +
                    "-" + dag_run.conf['user_items']['region'],
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Ignored",

            }
        )

        add_ignored_entries_for_region_uri = rail.WriteLogOperator(
            task_id='add_ignored_entries_for_region_uri',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Ignored",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" +
                "Incorrect Region value" + "-" +
                    dag_run.conf['user_items']['region'],
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Ignored",

            }
        )

        add_ignored_entries_for_region = rail.WriteLogOperator(
            task_id='add_ignored_entries_for_region',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Ignored",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" + "No Region value provided",
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Ignored",

            }
        )

        if_request_service_line_present = rail.IfOperator(
            task_id='if_request_service_line_present',
            test="{{ dag_run.conf.user_items.serviceline | is_truthy }}",
            yes_task="if_request_servicelineuri_present",
            no_task="add_ignored_entries_for_serviceline",
        )

        if_request_servicelineuri_present = rail.IfOperator(
            task_id='if_request_servicelineuri_present',
            test="{{ dag_run.conf.businessareacheckobject.serviceline_uri | is_truthy }}",
            yes_task="if_log_sub_business_line_uri_present",
            no_task="add_ignored_entries_for_serviceline_uri",
        )

        if_log_sub_business_line_uri_present = rail.IfOperator(
            task_id='if_log_sub_business_line_uri_present',
            test="{{ dag_run.conf.businessareacheckobject.subbussinessline_uri | is_truthy }}",
            yes_task="put_costcenter_schedule_for_user",
            no_task="add_ignored_entries_for_subbusinessline",
        )

        put_costcenter_schedule_for_user = rail.RepliconServiceOperator(
            task_id='put_costcenter_schedule_for_user',
            endpoint="/services/costcenterService1.svc/PutCostCenterScheduleForUser",
            data={
                "userUri": "{{ result('publish_draft').uri }}",
                "scheduleEntries": [
                    {
                        "costCenter": {
                            "uri": "{{ dag_run.conf.businessareacheckobject.subbussinessline_uri }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        add_ignored_entries_for_subbusinessline = rail.WriteLogOperator(
            task_id='add_ignored_entries_for_subbusinessline',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Ignored",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" +
                "Incorrect Sub Business Line value" + "-" +
                    dag_run.conf['user_items']['subbusinessline'],
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Ignored",

            }
        )

        add_ignored_entries_for_serviceline_uri = rail.WriteLogOperator(
            task_id='add_ignored_entries_for_serviceline_uri',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Ignored",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" +
                "Incorrect Service Line value" + "-" +
                    dag_run.conf['user_items']['serviceline'],
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Ignored",

            }
        )

        add_ignored_entries_for_serviceline = rail.WriteLogOperator(
            task_id='add_ignored_entries_for_serviceline',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Ignored",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" + "No Service Line value provided",
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Ignored",

            }
        )

        if_request_jobfunction_present = rail.IfOperator(
            task_id='if_request_jobfunction_present',
            test="{{ dag_run.conf.user_items.jobfunction | is_truthy }}",
            yes_task="if_request_jobfunctionuri_present",
            no_task="add_ignored_entries_for_jobfunction",
        )

        if_request_jobfunctionuri_present = rail.IfOperator(
            task_id='if_request_jobfunctionuri_present',
            test="{{ dag_run.conf.jobfunction_uri | is_truthy }}",
            yes_task="if_log_job_family_uri_present",
            no_task="add_ignored_entries_for_jobfunction_uri",
        )

        if_log_job_family_uri_present = rail.IfOperator(
            task_id='if_log_job_family_uri_present',
            test="{{ dag_run.conf.jobfamily_uri | is_truthy }}",
            yes_task="put_jobfamily_division_schedule_for_user",
            no_task="add_ignored_entries_for_jobfamily_uri",
        )

        put_jobfamily_division_schedule_for_user = rail.RepliconServiceOperator(
            task_id='put_jobfamily_division_schedule_for_user',
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data={
                "userUri": "{{ result('publish_draft').uri }}",
                "scheduleEntries": [
                    {
                        "division": {
                            "uri": "{{dag_run.conf.jobfamily_uri }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        add_ignored_entries_for_jobfamily_uri = rail.WriteLogOperator(
            task_id='add_ignored_entries_for_jobfamily_uri',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Ignored",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" +
                "Incorrect Job Family value-" +
                    dag_run.conf['user_items']['jobfunction'],
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Ignored",

            }
        )

        add_ignored_entries_for_jobfunction_uri = rail.WriteLogOperator(
            task_id='add_ignored_entries_for_jobfunction_uri',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Ignored",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" +
                "Incorrect Job Function value" +
                    dag_run.conf['user_items']['jobfunction'],
                "field_name": dag_run.conf['user_items']['loginid'] + "|" +
                dag_run.conf['user_items']['employeeid'] +
                    "|" + dag_run.conf['user_items']['name'],
                "status": "Ignored",

            }
        )

        add_ignored_entries_for_jobfunction = rail.WriteLogOperator(
            task_id='add_ignored_entries_for_jobfunction',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Ignored",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" + "No Job Function provided",
                "field_name": dag_run.conf['user_items']['loginid'] + "|" +
                dag_run.conf['user_items']['employeeid'] +
                    "|" + dag_run.conf['user_items']['name'],
                "status": "Ignored",

            }
        )

        if_request_managerid_present = rail.IfOperator(
            task_id='if_request_managerid_present',
            test="{{ dag_run.conf.user_items.managerid | is_truthy }}",
            yes_task="get_enabled_users",
            no_task="add_ignored_entries_for_managerid",
        )

        get_enabled_users = rail.RepliconServiceOperator(
            task_id='get_enabled_users',
            endpoint="/services/userlistService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": {
                            "leftExpression": null,
                            "operatorUri": null,
                            "rightExpression": null,
                            "value": null,
                            "filterDefinitionUri": "urn:replicon:user-list-filter:text"
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
                                "text": "{{ dag_run.conf.user_items.managerid }}",
                                "time": null,
                                "calendarDayDurationValue": null,
                                "workdayDurationValue": null,
                                "dateRange": null,
                                "dateTimeUtc": null
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
                            "filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
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
                                "dateTimeUtc": null
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

        if_first_datatype_present_218 = rail.IfOperator(
            task_id='if_first_datatype_present_218',
            test=lambda: (rail.result('get_enabled_users')['rows'][0]['cells'][0]['dataType']) if (rail.result('get_enabled_users')['rows'])
            and (rail.result('get_enabled_users')['rows'][0]) and (rail.result('get_enabled_users')['rows'][0]['cells'][0]) and
            (rail.result('get_enabled_users')['rows'][0]['cells'][0]['dataType']) else None,
            yes_task="foreach_d_219",
            no_task="if_log_supervisor_uri_225_present_226",
        )

        foreach_d_219 = rail.ForEachOperator(
            task_id='foreach_d_219',
            items="{{ result('get_enabled_users').rows | to_json}}",
            start_task='accumulate_list_items_220',
            end_task='foreach_d_219_end'
        )

        accumulate_list_items_220 = rail.SetVariableOperator(
            task_id='accumulate_list_items_220',
            name='Supervisors',
            append=True,
            value=lambda: {
                "name": rail.find_first_by_attr_and_get_attr(rail.result('foreach_d_219')['cells'], 'objectType',
                                                             'urn:replicon:object-type:user', 'textValue', ''),
                "employeeid": rail.find_first_by_attr_and_get_attr(rail.result('foreach_d_219')['cells'], 'dataType',
                                                                   'urn:replicon:list-type:string', 'textValue', ''),
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('foreach_d_219')['cells'], 'objectType',
                                                            'urn:replicon:object-type:user', 'uri', '')
            }
        )

        foreach_d_219_end = rail.EmptyOperator(
            task_id='foreach_d_219_end',
        )

        def get_occurence_count(dag_run):
            record_data = rail.result('accumulate_list_items_220')['value']
            list_count = record_data if record_data else None
            useruri = ''
            count = 0
            for data in list_count:
                if data['employeeid'] == dag_run.conf['user_items']['managerid']:
                    useruri = data['uri']
                    count += 1
            return {
                'count': count,
                'uri_count': useruri
            }

        log_supervisorcount_221 = rail.PythonOperator(
            task_id='log_supervisorcount_221',
            python_callable=get_occurence_count
        )

        if_log_supervisorcount_221_greater_than_1_222 = rail.IfOperator(
            task_id='if_log_supervisorcount_221_greater_than_1_222',
            test="{{result('log_supervisorcount_221').count > 1}}",
            yes_task="statestreet_userimport_logs_add_entry_223",
            no_task="if_log_supervisorcount_221_equals_to_1_224",
        )

        statestreet_userimport_logs_add_entry_223 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_223',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Ignored",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" +
                "Multiple Users found with Manager ID-" +
                    dag_run.conf['user_items']['managerid'],
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Ignored",

            }
        )

        if_log_supervisorcount_221_equals_to_1_224 = rail.IfOperator(
            task_id='if_log_supervisorcount_221_equals_to_1_224',
            test="{{result('log_supervisorcount_221').count == 1}}",
            yes_task="log_supervisor_uri_225",
            no_task="if_log_supervisor_uri_225_present_226",
        )

        log_supervisor_uri_225 = rail.PythonOperator(
            task_id='log_supervisor_uri_225',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'accumulate_list_items_220')['value'], 'employeeid', dag_run.conf['user_items']['managerid'], 'uri', ''),
        )

        if_log_supervisor_uri_225_present_226 = rail.IfOperator(
            task_id='if_log_supervisor_uri_225_present_226',
            test='''{{ result('log_supervisor_uri_225') | is_truthy }}''',
            yes_task="get_assigned_permission_sets_for_user2_227",
            no_task="statestreet_supervisorassignment_add_entry_234",
        )

        get_assigned_permission_sets_for_user2_227 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user2_227',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('log_supervisor_uri_225') }}"
            }
        )

        log_check_supervisor_permission_228 = rail.PythonOperator(
            # pylint: disable=too-many-statements line-too-long
            task_id='log_check_supervisor_permission_228',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_assigned_permission_sets_for_user2_227'), 'policyUri', 'urn:replicon:policy:supervision', 'user.displayText', '') if rail.result(
                'get_assigned_permission_sets_for_user2_227') and rail.result('get_assigned_permission_sets_for_user2_227')[0]['policyUri'] else None
        )

        if_log_check_supervisor_permission_228_present_229 = rail.IfOperator(
            task_id='if_log_check_supervisor_permission_228_present_229',
            test='''{{ result('log_check_supervisor_permission_228') | is_truthy }}''',
            yes_task="put_supervisor_assignment_schedule2_230",
            no_task="statestreet_supervisorassignment_add_entry_232",
        )

        put_supervisor_assignment_schedule2_230 = rail.RepliconServiceOperator(
            task_id='put_supervisor_assignment_schedule2_230',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule2",
            data={
                "userUri": "{{ result('publish_draft').uri}}",
                "scheduleEntries": [
                    {
                        "supervisor": {
                            "uri": "{{ result('log_supervisor_uri_225') }}",
                            "loginName": null,
                            "parameterCorrelationId": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        statestreet_supervisorassignment_add_entry_232 = rail.WriteLogOperator(
            task_id='statestreet_supervisorassignment_add_entry_232',
            log="{{ dag_run.conf.supervisor_logtable}}",
            message="na",
            severity="",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "user_uri": rail.result('publish_draft')['uri'],
                "manager_id": dag_run.conf['user_items']['managerid'],
                "user_id": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'],
                "status": "Not Assigned"
            }
        )

        statestreet_supervisorassignment_add_entry_234 = rail.WriteLogOperator(
            task_id='statestreet_supervisorassignment_add_entry_234',
            log="{{ dag_run.conf.supervisor_logtable}}",
            message="na",
            severity="",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "user_uri": rail.result('publish_draft')['uri'],
                "manager_id": dag_run.conf['user_items']['managerid'],
                "user_id": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'],
                "status": "Not Assigned"
            }
        )

        add_ignored_entries_for_managerid = rail.WriteLogOperator(
            task_id='add_ignored_entries_for_managerid',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Ignored",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" + "No Manager ID provided",
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Ignored",

            }
        )

        if_request_empstatus_equals_to_onleave_237 = rail.IfOperator(
            task_id='if_request_empstatus_equals_to_onleave_237',
            test='''{{ dag_run.conf.user_items.employeestatus == 'On Leave' }}''',
            yes_task="disable_login_238",
            no_task="if_request_standardhours_present",
        )

        disable_login_238 = rail.RepliconServiceOperator(
            task_id='disable_login_238',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ result('publish_draft').uri }}"
            }
        )

        statestreet_userimport_logs_add_entry_239 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_239',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Add User -" + rail.render_template("{{dag_run_ecid()}}") + "-" +
                "User disabled based on Emp Status-" +
                    dag_run.conf['user_items']['employeestatus'],
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": "Success",

            }
        )

        def get_status():
            if rail.get_current_context()['dag_run'].get_task_instance('update_numeric_value').current_state() == 'failed':
                result = "Ignored"
            else:
                result = "Error"
            return result

        def get_details(dag_run):
            error_message = rail.render_template("{{get_error_message()}}")
            if rail.get_current_context()['dag_run'].get_task_instance('update_numeric_value').current_state() == 'failed':
                output = "Add User -" + rail.render_template(
                    "{{dag_run_ecid()}}") + "- Invalid value for Standard Hours" + dag_run.conf['user_items']['standardhours']
            else:
                output = "Add User -" + rail.render_template(
                    "{{dag_run_ecid()}}") + error_message
            return output

        on_error = rail.EmptyOperator(
            task_id='on_error',
            trigger_rule='one_failed'
        )

        catch_and_log = rail.WriteLogOperator(
            task_id='catch_and_log',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity=get_status,
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": get_details(dag_run),
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": get_status(),

            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> if_loginid_blank
        if_loginid_blank >> rail.Label(
            'Yes') >> add_failure_entries >> on_error
        if_loginid_blank >> rail.Label('No') >> search_users
        search_users >> if_log_uri_is_present >> rail.Label(
            'Yes') >> log_failure_entries_for_loginname >> on_error
        if_log_uri_is_present >> rail.Label(
            'No') >> if_empid_blank >> rail.Label('Yes') >> add_failure_entries_for_empid >> on_error
        if_empid_blank >> rail.Label(
            'No') >> if_email_blank >> rail.Label('Yes') >> add_failure_entries_for_email >> on_error
        if_email_blank >> rail.Label(
            'No') >> if_costcentername_blank >> rail.Label('Yes') >> add_failure_entries_for_costcentrename
        add_failure_entries_for_costcentrename >> on_error
        if_costcentername_blank >> rail.Label(
            'No') >> if_costcenternumber_blank >> rail.Label('Yes') >> add_failure_entries_for_costcenternumber
        add_failure_entries_for_costcenternumber >> on_error
        if_costcenternumber_blank >> rail.Label(
            'No') >> if_legalentityname_blank >> rail.Label('Yes') >> add_failure_entries_for_legalentityname
        add_failure_entries_for_legalentityname >> on_error
        if_legalentityname_blank >> rail.Label(
            'No') >> create_new_draft >> if_request_email_contains_special_character
        if_request_email_contains_special_character >> rail.Label(
            'Yes') >> update_email >> if_name_present >> rail.Label('Yes') >> if_name_has_character
        if_name_has_character >> rail.Label(
            'Yes') >> log_first_and_last_names >> if_log_first_and_lastname_present
        if_log_first_and_lastname_present >> rail.Label(
            'Yes') >> update_first_name >> update_last_name >> if_log_first_and_lastname_not_present
        if_log_first_and_lastname_present >> rail.Label(
            'No') >> if_log_first_and_lastname_not_present
        if_log_first_and_lastname_not_present >> rail.Label(
            'Yes') >> add_failure_entries_for_invalid_formatname >> on_error
        if_log_first_and_lastname_not_present >> rail.Label(
            'No') >> update_employee_id >> if_request_emptype_present
        if_request_emptype_present >> rail.Label(
            'Yes') >> get_all_employee_type_details >> log_employee_uri
        log_employee_uri >> if_log_employee_uri_present >> rail.Label(
            'Yes') >> update_employee_type_for_user >> if_legal_entityname_present

        if_legal_entityname_present >> rail.Label(
            'Yes') >> if_legal_entity_uri_present
        if_legal_entity_uri_present >> rail.Label(
            'Yes') >> if_isenabled_is_true
        if_isenabled_is_true >> rail.Label(
            'Yes') >> if_log_cost_center_number_uri_present
        if_log_cost_center_number_uri_present >> rail.Label(
            'Yes') >> if_isenabled_is_true_for_costcenter
        if_isenabled_is_true_for_costcenter >> rail.Label(
            'Yes') >> update_department_for_user >> set_s_s_o_authentication_for_user
        set_s_s_o_authentication_for_user >> put_product_assignments_for_user
        put_product_assignments_for_user >> publish_draft >> if_log_user_uri_present
        if_log_user_uri_present >> rail.Label(
            'Yes') >> add_success_entries_for_useruri >> assignpolicy_set_to_user_widget_timesheet
        if_log_user_uri_present >> rail.Label(
            'No') >> assignpolicy_set_to_user_widget_timesheet >> if_request_banktitle_present
        if_request_banktitle_present >> rail.Label(
            'Yes') >> if_log_banktitle_uri_present
        if_log_banktitle_uri_present >> rail.Label(
            'Yes') >> update_dropdown_value >> if_request_banktitle_equals_to_managingdirector
        if_request_banktitle_equals_to_managingdirector >> rail.Label(
            'Yes') >> put_permission_set_assignments_for_user_supervisor >> remove_policy_set_assignment_from_user
        remove_policy_set_assignment_from_user >> if_request_fullparttime_present
        if_request_banktitle_equals_to_managingdirector >> rail.Label(
            'No') >> if_request_fullparttime_present
        if_request_banktitle_present >> rail.Label(
            'No') >> if_request_fullparttime_present
        if_request_standardhours_present >> rail.Label(
            'Yes') >> update_numeric_value >> on_error
        if_request_standardhours_present >> rail.Label(
            'No') >> on_error
        if_request_fullparttime_present >> rail.Label(
            'Yes') >> if_log_fullparttime_uri_present
        if_log_fullparttime_uri_present >> rail.Label(
            'Yes') >> update_drop_down_value >> if_request_banktitle_not_equals_to_managingdirector
        if_log_fullparttime_uri_present >> rail.Label(
            'No') >> add_ignored_entries_for_fullparttime >> if_request_banktitle_not_equals_to_managingdirector
        if_request_fullparttime_present >> rail.Label(
            'No') >> if_request_banktitle_not_equals_to_managingdirector
        if_request_banktitle_not_equals_to_managingdirector >> rail.Label(
            'Yes') >> if_request_managernonmanager_present
        if_request_managernonmanager_present >> rail.Label(
            'Yes') >> if_request_managernonmanager_equals_to_yes
        if_request_managernonmanager_equals_to_yes >> rail.Label(
            'Yes') >> put_permission_set_assignments_for_user_supervisorand_project >> if_request_region_present
        if_request_managernonmanager_present >> rail.Label(
            'No') >> put_permission_set_assignments_for_user_project_resource >> if_request_region_present
        if_request_managernonmanager_equals_to_yes >> rail.Label(
            'No') >> put_permission_set_assignments_for_user_project_resource
        if_request_banktitle_not_equals_to_managingdirector >> rail.Label(
            'No') >> if_request_region_present

        if_request_region_present >> rail.Label(
            'Yes') >> if_request_regionuri_present
        if_request_regionuri_present >> rail.Label(
            'Yes') >> if_request_locationuri_present
        if_request_regionuri_present >> rail.Label(
            'No') >> add_ignored_entries_for_region_uri >> if_request_service_line_present
        if_request_locationuri_present >> rail.Label(
            'Yes') >> put_location_schedule_for_user >> if_request_service_line_present
        if_request_region_present >> rail.Label(
            'No') >> add_ignored_entries_for_region >> if_request_service_line_present
        if_request_locationuri_present >> rail.Label(
            'No') >> add_ignored_entries_for_location_uri >> if_request_service_line_present

        if_request_service_line_present >> rail.Label(
            'Yes') >> if_request_servicelineuri_present
        if_request_servicelineuri_present >> rail.Label(
            'Yes') >> if_log_sub_business_line_uri_present
        if_log_sub_business_line_uri_present >> rail.Label(
            'Yes') >> put_costcenter_schedule_for_user >> if_request_jobfunction_present
        if_request_service_line_present >> rail.Label(
            'No') >> add_ignored_entries_for_serviceline >> if_request_jobfunction_present
        if_request_servicelineuri_present >> rail.Label(
            'No') >> add_ignored_entries_for_serviceline_uri >> if_request_jobfunction_present
        if_log_sub_business_line_uri_present >> rail.Label(
            'No') >> add_ignored_entries_for_subbusinessline >> if_request_jobfunction_present

        if_request_jobfunction_present >> rail.Label(
            'Yes') >> if_request_jobfunctionuri_present
        if_request_jobfunctionuri_present >> rail.Label(
            'Yes') >> if_log_job_family_uri_present
        if_request_jobfunctionuri_present >> rail.Label(
            'No') >> add_ignored_entries_for_jobfunction_uri >> if_request_managerid_present

        if_log_job_family_uri_present >> rail.Label(
            'Yes') >> put_jobfamily_division_schedule_for_user >> if_request_managerid_present
        if_request_jobfunction_present >> rail.Label(
            'No') >> add_ignored_entries_for_jobfunction >> if_request_managerid_present
        if_log_job_family_uri_present >> rail.Label(
            'No') >> add_ignored_entries_for_jobfamily_uri >> if_request_managerid_present

        if_request_managerid_present >> rail.Label(
            'Yes') >> get_enabled_users >> if_first_datatype_present_218
        if_first_datatype_present_218 >> rail.Label(
            'Yes') >> foreach_d_219 >> accumulate_list_items_220 >> foreach_d_219_end
        foreach_d_219 >> foreach_d_219_end >> log_supervisorcount_221 >> if_log_supervisorcount_221_greater_than_1_222
        if_log_supervisorcount_221_greater_than_1_222 >> rail.Label(
            'Yes') >> statestreet_userimport_logs_add_entry_223 >> if_log_supervisorcount_221_equals_to_1_224
        if_log_supervisorcount_221_greater_than_1_222 >> rail.Label(
            'No') >> if_log_supervisorcount_221_equals_to_1_224
        if_log_supervisorcount_221_equals_to_1_224 >> rail.Label(
            'Yes') >> log_supervisor_uri_225
        log_supervisor_uri_225 >> if_log_supervisor_uri_225_present_226
        if_log_supervisorcount_221_equals_to_1_224 >> rail.Label(
            'No') >> if_log_supervisor_uri_225_present_226
        if_first_datatype_present_218 >> rail.Label(
            'No') >> if_log_supervisor_uri_225_present_226
        if_log_supervisor_uri_225_present_226 >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_user2_227 >> log_check_supervisor_permission_228
        log_check_supervisor_permission_228 >> if_log_check_supervisor_permission_228_present_229
        if_log_check_supervisor_permission_228_present_229 >> rail.Label(
            'Yes') >> put_supervisor_assignment_schedule2_230 >> if_request_empstatus_equals_to_onleave_237
        if_log_check_supervisor_permission_228_present_229 >> rail.Label(
            'No') >> statestreet_supervisorassignment_add_entry_232
        statestreet_supervisorassignment_add_entry_232 >> if_request_empstatus_equals_to_onleave_237
        if_log_supervisor_uri_225_present_226 >> rail.Label(
            'No') >> statestreet_supervisorassignment_add_entry_234 >> if_request_empstatus_equals_to_onleave_237
        if_request_empstatus_equals_to_onleave_237 >> rail.Label(
            'Yes') >> disable_login_238 >> statestreet_userimport_logs_add_entry_239
        statestreet_userimport_logs_add_entry_239 >> if_request_standardhours_present
        if_request_empstatus_equals_to_onleave_237 >> rail.Label(
            'No') >> if_request_standardhours_present
        if_request_managerid_present >> rail.Label(
            'No') >> add_ignored_entries_for_managerid >> if_request_empstatus_equals_to_onleave_237
        if_request_banktitle_present >> if_log_banktitle_uri_present >> rail.Label(
            'No') >> add_ignored_entries_for_banktitle >> on_error
        if_isenabled_is_true_for_costcenter >> rail.Label(
            'No') >> add_failure_entries_for_costcenter_isenabled >> on_error
        if_isenabled_is_true >> if_log_cost_center_number_uri_present >> rail.Label(
            'No') >> add_failure_entries_for_costcenter_data >> on_error
        if_isenabled_is_true >> rail.Label(
            'No') >> add_failure_entries_for_isenabled >> on_error
        if_legal_entity_uri_present >> rail.Label(
            'No') >> add_failure_entries_for_legalentity_uri >> on_error
        if_legal_entityname_present >> rail.Label(
            'No') >> add_failure_entries_for_legalentity >> on_error
        if_log_employee_uri_present >> rail.Label(
            'No') >> add_failure_entries_for_employee_update >> on_error
        if_request_emptype_present >> rail.Label(
            'No') >> add_failure_entries_for_emptype >> on_error
        if_name_has_character >> rail.Label(
            'No') >> add_failure_entries_for_invalidname_format >> on_error
        if_name_present >> rail.Label(
            'No') >> add_failure_entries_for_invalidname >> on_error
        if_request_email_contains_special_character >> rail.Label(
            'Yes') >> add_failure_entries_for_invalidemail >> on_error >> catch_and_log >> log_to_sumo
        return dag


rail.for_each_instance(create_dag)
