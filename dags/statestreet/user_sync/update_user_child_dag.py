from datetime import datetime as dt, timedelta
import rail
from airflow.models import Variable
from statestreet.user_sync.utils.response_filter import get_jobfamilyvalue, get_costcenteruri, get_currentlocation_uri

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'statestreet_user_sync_update_user_child_{config.instance}',
        description=f'Statestreet_user_sync_update_user_child {config.instance}',
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
            no_task='get_user_details_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_user_details_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        get_user_details_3 = rail.RepliconServiceOperator(
            task_id='get_user_details_3',
            endpoint="/services/userService1.svc/GetUserDetails",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}"
            }
        )

        def get_current_date():
            date_now = dt.utcnow()
            return {
                "year": date_now.year,
                "month": date_now.month,
                "day": date_now.day
            }

        log_current_date = rail.PythonOperator(
            task_id='log_current_date',
            python_callable=get_current_date
        )

        if_request_email_present_4 = rail.IfOperator(
            task_id='if_request_email_present_4',
            test='''{{ dag_run.conf.update_items.email | is_truthy }}''',
            yes_task="if_request_email_contains_5",
            no_task="if_request_name_present_11",
        )

        if_request_email_contains_5 = rail.IfOperator(
            task_id='if_request_email_contains_5',
            test='''{{ dag_run.conf.update_items.email | matches('@') }}''',
            yes_task="if_request_email_not_equals_to_datarestget_user_details_3responsedemailaddress_6",
            no_task="statestreet_userimport_logs_add_entry_10",
        )

        if_request_email_not_equals_to_datarestget_user_details_3responsedemailaddress_6 = rail.IfOperator(
            task_id='if_request_email_not_equals_to_datarestget_user_details_3responsedemailaddress_6',
            test='''{{ dag_run.conf.update_items.email != result('get_user_details_3').emailAddress }}''',
            yes_task="update_email_7",
            no_task="if_request_name_present_11",
        )

        update_email_7 = rail.RepliconServiceOperator(
            task_id='update_email_7',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}",
                "email": "{{ dag_run.conf.update_items.email }}"
            }
        )

        statestreet_userimport_logs_add_entry_8 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_8',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") + "-" +
                " Email address updated -" +
                    dag_run.conf['update_items']['email'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Success",
            }
        )

        statestreet_userimport_logs_add_entry_10 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_10',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") + "-" +
                " Invalid Email address -" +
                    dag_run.conf['update_items']['email'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        if_request_name_present_11 = rail.IfOperator(
            task_id='if_request_name_present_11',
            test='''{{ dag_run.conf.update_items.name | is_truthy }}''',
            yes_task="if_request_name_contains_12",
            no_task="if_request_emptype_present_25",
        )

        if_request_name_contains_12 = rail.IfOperator(
            task_id='if_request_name_contains_12',
            test='''{{ dag_run.conf.update_items.name | matches(',') }}''',
            yes_task="if_request_name_not_equals_to_datarestget_user_details_3responseddisplaytext_13",
            no_task="statestreet_userimport_logs_add_entry_23",
        )

        if_request_name_not_equals_to_datarestget_user_details_3responseddisplaytext_13 = rail.IfOperator(
            task_id='if_request_name_not_equals_to_datarestget_user_details_3responseddisplaytext_13',
            test='''{{ dag_run.conf.update_items.name != result('get_user_details_3').displayText }}''',
            yes_task="log_user_name",
            no_task="if_request_emptype_present_25",
        )

        log_user_name = rail.PythonOperator(
            task_id='log_user_name',
            python_callable=lambda dag_run: {
                "firstname": dag_run.conf['update_items']['name'].split(",")[1],
                "lastname": dag_run.conf['update_items']['name'].split(",")[0]
            }
        )

        if_first_name_present = rail.IfOperator(
            task_id='if_first_name_present',
            test='''{{ result('log_user_name').firstname | is_truthy  and result('log_user_name').lastname | is_truthy }}''',
            yes_task="update_first_name_17",
            no_task="if_log_first_name_14_blank_20",
        )

        update_first_name_17 = rail.RepliconServiceOperator(
            task_id='update_first_name_17',
            endpoint="/services/UserService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}",
                "firstname": "{{ result('log_user_name').firstname }}"
            }
        )

        update_last_name_18 = rail.RepliconServiceOperator(
            task_id='update_last_name_18',
            endpoint="/services/UserService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}",
                "lastname": "{{result('log_user_name').lastname}}"
            }
        )

        statestreet_userimport_logs_add_entry_19 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_19',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Name updated to -" +
                    dag_run.conf['update_items']['name'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Success",
            }
        )

        if_log_first_name_14_blank_20 = rail.IfOperator(
            task_id='if_log_first_name_14_blank_20',
            test='''{{ result('log_user_name').firstname  | is_falsy  or result('log_user_name').lastname | is_falsy }}''',
            yes_task="statestreet_userimport_logs_add_entry_21",
            no_task="if_request_emptype_present_25",
        )

        statestreet_userimport_logs_add_entry_21 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_21',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Invalid name format provided -" +
                    dag_run.conf['update_items']['name'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        statestreet_userimport_logs_add_entry_23 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_23',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Invalid name format provided -" +
                    dag_run.conf['update_items']['name'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        if_request_emptype_present_25 = rail.IfOperator(
            task_id='if_request_emptype_present_25',
            test='''{{ dag_run.conf.update_items.employeetype | is_truthy }}''',
            yes_task="get_employee_type_for_user_26",
            no_task="if_request_costcenternumber_present_36",
        )

        get_employee_type_for_user_26 = rail.RepliconServiceOperator(
            task_id='get_employee_type_for_user_26',
            endpoint="/services/employeetypeService1.svc/GetEmployeeTypeForUser",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}"
            }
        )

        if_request_emptype_not_equals_to_datarestget_employee_type_for_user_26responsedname_27 = rail.IfOperator(
            task_id='if_request_emptype_not_equals_to_datarestget_employee_type_for_user_26responsedname_27',
            test='''{{ dag_run.conf.update_items.employeetype != result('get_employee_type_for_user_26').name }}''',
            yes_task="get_all_employee_type_details_28",
            no_task="if_request_costcenternumber_present_36",
        )

        get_all_employee_type_details_28 = rail.RepliconServiceOperator(
            task_id='get_all_employee_type_details_28',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
            data=None
        )

        log_employeetype_uri = rail.PythonOperator(
            task_id='log_employeetype_uri',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_employee_type_details_28'), 'displayText', dag_run.conf['update_items']['employeetype'], 'uri', null)
        )

        if_log_employeetype_uri_present_30 = rail.IfOperator(
            task_id='if_log_employeetype_uri_present_30',
            test='''{{ result('log_employeetype_uri') | is_truthy }}''',
            yes_task="update_employee_type_for_user_31",
            no_task="statestreet_userimport_logs_add_entry_34",
        )

        update_employee_type_for_user_31 = rail.RepliconServiceOperator(
            task_id='update_employee_type_for_user_31',
            endpoint="/services/EmployeeTypeService1.svc/UpdateEmployeeTypeForUser",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}",
                "employeeTypeUri": "{{ result('log_employeetype_uri') }}"
            }
        )

        statestreet_userimport_logs_add_entry_32 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_32',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") + "-" +
                " Employee type updated -" +
                    dag_run.conf['update_items']['employeetype'],
                "field_name": dag_run.conf['update_items']['loginid'] + "|" +
                dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Success",
            }
        )

        statestreet_userimport_logs_add_entry_34 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_34',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") + "-" +
                " Employee type not found  -" +
                    dag_run.conf['update_items']['employeetype'],
                "field_name": dag_run.conf['update_items']['loginid'] + "|" +
                dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        if_request_costcenternumber_present_36 = rail.IfOperator(
            task_id='if_request_costcenternumber_present_36',
            test='''{{ dag_run.conf.update_items.costcenternumber | is_truthy }}''',
            yes_task="get_department_for_user_37",
            no_task="if_request_banktitle_present_62",
        )

        get_department_for_user_37 = rail.RepliconServiceOperator(
            task_id='get_department_for_user_37',
            endpoint="/services/DepartmentService1.svc/GetDepartmentForUser",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}"
            }
        )

        if_request_costcenternumber_not_equals_to_datarestget_department_for_user_37responseddisplaytext_38 = rail.IfOperator(
            task_id='if_request_costcenternumber_not_equals_to_datarestget_department_for_user_37responseddisplaytext_38',
            test='''{{ dag_run.conf.update_items.costcenternumber != result('get_department_for_user_37').displayText }}''',
            yes_task="if_request_legalentityname_present_39",
            no_task="if_request_banktitle_present_62",
        )

        if_request_legalentityname_present_39 = rail.IfOperator(
            task_id='if_request_legalentityname_present_39',
            test='''{{ dag_run.conf.update_items.legalentityname | is_truthy }}''',
            yes_task="if_log_legal_entity_uri_40_present_41",
            no_task="statestreet_userimport_logs_add_entry_60",
        )

        if_log_legal_entity_uri_40_present_41 = rail.IfOperator(
            task_id='if_log_legal_entity_uri_40_present_41',
            test='''{{ dag_run.conf.legalentitycheckobject.legalentityuri | is_truthy }}''',
            yes_task="if_d_isenabled_is_true_43",
            no_task="statestreet_userimport_logs_add_entry_58",
        )

        if_d_isenabled_is_true_43 = rail.IfOperator(
            task_id='if_d_isenabled_is_true_43',
            test='''{{ dag_run.conf.legalentitycheckobject.islegalentityenabled | is_truthy }}''',
            yes_task="if_log_cost_center_number_uri_45_present_46",
            no_task="statestreet_userimport_logs_add_entry_56",
        )

        if_log_cost_center_number_uri_45_present_46 = rail.IfOperator(
            task_id='if_log_cost_center_number_uri_45_present_46',
            test='''{{ dag_run.conf.legalentitycheckobject.costcenteruri | is_truthy }}''',
            yes_task="if_d_isenabled_is_true_48",
            no_task="statestreet_userimport_logs_add_entry_54",
        )

        if_d_isenabled_is_true_48 = rail.IfOperator(
            task_id='if_d_isenabled_is_true_48',
            test='''{{ dag_run.conf.legalentitycheckobject.iscostcenterenabled | is_truthy }}''',
            yes_task="update_department_for_user_49",
            no_task="statestreet_userimport_logs_add_entry_52",
        )

        update_department_for_user_49 = rail.RepliconServiceOperator(
            task_id='update_department_for_user_49',
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}",
                "departmentUri": "{{ dag_run.conf.legalentitycheckobject.costcenteruri }}"
            }
        )

        statestreet_userimport_logs_add_entry_50 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_50',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Cost Center Number updated  -" +
                    dag_run.conf['update_items']['costcenternumber'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Success",
            }
        )

        statestreet_userimport_logs_add_entry_52 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_52',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Cost Center Number provided is disabled  -" +
                    dag_run.conf['update_items']['costcenternumber'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        statestreet_userimport_logs_add_entry_54 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_54',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Incorrect Cost Center Number  -" +
                    dag_run.conf['update_items']['costcenternumber'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        statestreet_userimport_logs_add_entry_56 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_56',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Legal Entity provided is disabled  -" +
                    dag_run.conf['update_items']['legalentityname'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        statestreet_userimport_logs_add_entry_58 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_58',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Incorrect Legal Entity Name   -" +
                    dag_run.conf['update_items']['legalentityname'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        statestreet_userimport_logs_add_entry_60 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_60',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " No Legal Entity name provided",
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        if_request_banktitle_present_62 = rail.IfOperator(
            task_id='if_request_banktitle_present_62',
            test='''{{ dag_run.conf.update_items.banktitle | is_truthy }}''',
            yes_task="log_bantitle",
            no_task="if_request_fullparttime_present_85",
        )

        log_bantitle = rail.PythonOperator(
            task_id='log_bantitle',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_user_details_3')[
                    'customFieldValues'], 'customField.displayText', 'Bank Titles', 'text', '') if rail.result('get_user_details_3') else None
        )

        if_request_banktitle_not_equals_to_dataloggerlog_63message_64 = rail.IfOperator(
            task_id='if_request_banktitle_not_equals_to_dataloggerlog_63message_64',
            test='''{{ dag_run.conf.update_items.banktitle != result('log_bantitle') }}''',
            yes_task="if_log_66_present_67",
            no_task="if_request_fullparttime_present_85",
        )

        if_log_66_present_67 = rail.IfOperator(
            task_id='if_log_66_present_67',
            test='''{{ dag_run.conf.bank_title_uri | is_truthy }}''',
            yes_task="update_dropdown_value_68",
            no_task="statestreet_userimport_logs_add_entry_76",
        )

        update_dropdown_value_68 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_68',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['user_uri'],
                "customFieldUri": dag_run.conf['custom_field1'],
                "customFieldDropDownOptionUri": dag_run.conf['bank_title_uri']
            }
        )

        statestreet_userimport_logs_add_entry_69 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_69',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Bank Title updated -" +
                    dag_run.conf['update_items']['banktitle'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Success",
            }
        )

        if_request_banktitle_equals_to_managingdirector_70 = rail.IfOperator(
            task_id='if_request_banktitle_equals_to_managingdirector_70',
            test='''{{ dag_run.conf.update_items.banktitle == 'Managing Director' }}''',
            yes_task="put_permission_set_assignments_for_user_supervisorand_report_user_71",
            no_task="if_request_fullparttime_present_85",
        )

        put_permission_set_assignments_for_user_supervisorand_report_user_71 = rail.RepliconServiceOperator(
            task_id='put_permission_set_assignments_for_user_supervisorand_report_user_71',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "permissionSetUris": [dag_run.conf['permission_set1'], dag_run.conf['permission_set2']]
            }
        )

        statestreet_userimport_logs_add_entry_72 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_72',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Permissions assigned based on Bank Title   -" +
                    dag_run.conf['update_items']['banktitle'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Success",
            }
        )

        remove_policy_set_assignment_from_user_remove_timesheet_template_73 = rail.RepliconServiceOperator(
            task_id='remove_policy_set_assignment_from_user_remove_timesheet_template_73',
            endpoint="/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "policySetUri": dag_run.conf['policyset_uri']
            }
        )

        statestreet_userimport_logs_add_entry_74 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_74',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Timesheet Template removed based on Bank Title -" +
                    dag_run.conf['update_items']['banktitle'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Success",
            }
        )

        statestreet_userimport_logs_add_entry_76 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_76',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Invalid value for Bank Title- provided -" +
                    dag_run.conf['update_items']['banktitle'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        if_request_standardhours_present_77 = rail.IfOperator(
            task_id='if_request_standardhours_present_77',
            test='''{{ dag_run.conf.update_items.standardhours | is_truthy }}''',
            yes_task="log_78",
            no_task="catch",
        )

        def get_number():
            user_details = rail.result('get_user_details_3')
            user_data = rail.find_first_by_attr_and_get_attr(
                user_details['customFieldValues'], 'customField.displayText', 'Standard Hours', 'number', '')
            return user_data

        log_78 = rail.PythonOperator(
            task_id='log_78',
            python_callable=get_number
        )

        if_request_standardhours_not_equals_to_dataloggerlog_78message_79 = rail.IfOperator(
            task_id='if_request_standardhours_not_equals_to_dataloggerlog_78message_79',
            test='''{{ dag_run.conf.update_items.standardhours != result('log_78') }}''',
            yes_task="update_numeric_value_81",
            no_task="catch",
        )

        update_numeric_value_81 = rail.RepliconServiceOperator(
            task_id='update_numeric_value_81',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['user_uri'],
                "customFieldUri": dag_run.conf['custom_field2'],
                "value": dag_run.conf['update_items']['standardhours']
            }
        )

        statestreet_userimport_logs_add_entry_82 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_82',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Standard Hours updated -" +
                    dag_run.conf['update_items']['standardhours'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Success",
            }
        )

        if_request_fullparttime_present_85 = rail.IfOperator(
            task_id='if_request_fullparttime_present_85',
            test='''{{ dag_run.conf.update_items.fullparttime | is_truthy }}''',
            yes_task="log_86",
            no_task="if_request_banktitle_not_equals_to_managingdirector_95",
        )

        def get_text():
            user_details = rail.result('get_user_details_3')
            user_data = rail.find_first_by_attr_and_get_attr(
                user_details['customFieldValues'], 'customField.displayText', 'Full / Part Time', 'text', '')
            return user_data

        log_86 = rail.PythonOperator(
            task_id='log_86',
            python_callable=get_text
        )

        if_request_fullparttime_not_equals_to_dataloggerlog_86message_87 = rail.IfOperator(
            task_id='if_request_fullparttime_not_equals_to_dataloggerlog_86message_87',
            test='''{{ dag_run.conf.update_items.fullparttime != result('log_86') }}''',
            yes_task="if_log_89_present_90",
            no_task="if_request_banktitle_not_equals_to_managingdirector_95",
        )

        if_log_89_present_90 = rail.IfOperator(
            task_id='if_log_89_present_90',
            test='''{{ dag_run.conf.full_parttime_uri | is_truthy }}''',
            yes_task="update_dropdown_value_91",
            no_task="statestreet_userimport_logs_add_entry_94",
        )

        update_dropdown_value_91 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_91',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['user_uri'],
                "customFieldUri": dag_run.conf['custom_field3'],
                "customFieldDropDownOptionUri": dag_run.conf['full_parttime_uri']
            }
        )

        statestreet_userimport_logs_add_entry_92 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_92',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Full/Part Time updated -" +
                    dag_run.conf['update_items']['fullparttime'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Success",
            }
        )

        statestreet_userimport_logs_add_entry_94 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_94',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Invalid value for Full/Part Time provided -" +
                    dag_run.conf['update_items']['fullparttime'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        if_request_banktitle_not_equals_to_managingdirector_95 = rail.IfOperator(
            task_id='if_request_banktitle_not_equals_to_managingdirector_95',
            test='''{{ dag_run.conf.update_items.banktitle != 'Managing Director' }}''',
            yes_task="if_request_managernonmanager_present_96",
            no_task="if_request_locationcode_present_104",
        )

        if_request_managernonmanager_present_96 = rail.IfOperator(
            task_id='if_request_managernonmanager_present_96',
            test='''{{ dag_run.conf.update_items.managernonmanager | is_truthy }}''',
            yes_task="if_request_managernonmanager_equals_to_yes_97",
            no_task="put_permission_set_assignments_for_user_project_resource_100",
        )

        if_request_managernonmanager_equals_to_yes_97 = rail.IfOperator(
            task_id='if_request_managernonmanager_equals_to_yes_97',
            test='''{{ dag_run.conf.update_items.managernonmanager == 'Yes' }}''',
            yes_task="put_permission_set_assignments_for_user_supervisorand_project_resourcewith_reports_98",
            no_task="put_permission_set_assignments_for_user_project_resource_100",
        )

        put_permission_set_assignments_for_user_supervisorand_project_resourcewith_reports_98 = rail.RepliconServiceOperator(
            task_id='put_permission_set_assignments_for_user_supervisorand_project_resourcewith_reports_98',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "permissionSetUris": [dag_run.conf['permission_set3'], dag_run.conf['permission_set2']
                                      ]
            }
        )

        put_permission_set_assignments_for_user_project_resource_100 = rail.RepliconServiceOperator(
            task_id='put_permission_set_assignments_for_user_project_resource_100',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "permissionSetUris": [dag_run.conf['permission_set4']]
            }
        )

        if_request_locationcode_present_104 = rail.IfOperator(
            task_id='if_request_locationcode_present_104',
            test='''{{ dag_run.conf.update_items.locationcode | is_truthy }}''',
            yes_task="get_current_location_105",
            no_task="if_request_subbusinessline_present_147",
        )

        get_current_location_105 = rail.RepliconServiceOperator(
            task_id='get_current_location_105',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:location"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:user"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": "{{ dag_run.conf.user_uri}}",
                            "uris": [],
                            "bool": null,
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
                }
            },
            data_handler=get_currentlocation_uri
        )

        if_request_locationcode_not_equals_to_dataloggerlog_106message_107 = rail.IfOperator(
            task_id='if_request_locationcode_not_equals_to_dataloggerlog_106message_107',
            test='''{{ dag_run.conf.update_items.locationcode != result('get_current_location_105')}}''',
            yes_task="if_request_region_present_108",
            no_task="if_request_subbusinessline_present_147",
        )

        if_request_region_present_108 = rail.IfOperator(
            task_id='if_request_region_present_108',
            test='''{{ dag_run.conf.update_items.region | is_truthy }}''',
            yes_task="if_request_regionuri_present_109",
            no_task="statestreet_userimport_logs_add_entry_146",
        )

        if_request_regionuri_present_109 = rail.IfOperator(
            task_id='if_request_regionuri_present_109',
            test='''{{ dag_run.conf.region_uri| is_truthy }}''',
            yes_task="if_log_location_uri_121_present_122",
            no_task="statestreet_userimport_logs_add_entry_144",
        )

        if_log_location_uri_121_present_122 = rail.IfOperator(
            task_id='if_log_location_uri_121_present_122',
            test='''{{ dag_run.conf.location_uri | is_truthy }}''',
            yes_task="apply_user_modifications_125",
            no_task="statestreet_userimport_logs_add_entry_134",
        )

        apply_user_modifications_125 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications_125',
            endpoint="/services/importService1.svc/ApplyUserModifications",
            data=lambda dag_run: {
                "user": {
                     "uri": dag_run.conf['user_uri'],
                    "loginName": null,
                     "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": {
                        "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementLocationSchedule": [],
                        "updateLocationScheduleOverDateRange": {
                            "replacementLocationScheduleEntries": [
                                {
                                    "location": {
                                        "uri": dag_run.conf['location_uri'],
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": {
                                        "year": rail.result('log_current_date')['year'],
                                        "month": rail.result('log_current_date')['month'],
                                        "day": rail.result('log_current_date')['day'],
                                    }
                                }
                            ],
                            "endDate": null
                        }
                    },
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": null,
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        if_d_errors_present_126 = rail.IfOperator(
            task_id='if_d_errors_present_126',
            test='''{{ result('apply_user_modifications_125').errors | is_truthy }}''',
            yes_task="log_127",
            no_task="statestreet_userimport_logs_add_entry_130",
        )

        log_127 = rail.PythonOperator(
            task_id='log_127',
            python_callable=lambda: rail.result('apply_user_modifications_125')[
                'user']['displayText'] if rail.result('apply_user_modifications_125')['errors'] else None
        )

        statestreet_userimport_logs_add_entry_128 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_128',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Location Code not updated" +
                    rail.result('log_127') +
                dag_run.conf['update_items']['locationcode'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        statestreet_userimport_logs_add_entry_130 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_130',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Location Code updated" +
                    dag_run.conf['update_items']['locationcode'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Success",
            }
        )

        statestreet_userimport_logs_add_entry_134 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_134',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + "Incorrect Location Code value" +
                    dag_run.conf['update_items']['locationcode'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        statestreet_userimport_logs_add_entry_144 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_144',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + "Incorrect Region value" +
                    dag_run.conf['update_items']['region'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        statestreet_userimport_logs_add_entry_146 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_146',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + "- No Region value provided",
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        if_request_subbusinessline_present_147 = rail.IfOperator(
            task_id='if_request_subbusinessline_present_147',
            test='''{{ dag_run.conf.update_items.subbusinessline | is_truthy }}''',
            yes_task="get_current_cost_center_148",
            no_task="if_request_jobfamily_present_199",
        )

        get_current_cost_center_148 = rail.RepliconServiceOperator(
            task_id='get_current_cost_center_148',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                     "urn:replicon:user-list-column:cost-center"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:user"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": "{{ dag_run.conf.user_uri }}",
                            "uris": [],
                            "bool": null,
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
                }
            },
            data_handler=get_costcenteruri
        )

        if_request_subbusinessline_not_equals_to_dataloggerlog_149message_150 = rail.IfOperator(
            task_id='if_request_subbusinessline_not_equals_to_dataloggerlog_149message_150',
            test='''{{ dag_run.conf.update_items.subbusinessline != result('get_current_cost_center_148')}}''',
            yes_task="if_request_serviceline_present_151",
            no_task="if_request_jobfamily_present_199",
        )

        if_request_serviceline_present_151 = rail.IfOperator(
            task_id='if_request_serviceline_present_151',
            test='''{{ dag_run.conf.update_items.serviceline | is_truthy }}''',
            yes_task="if_request_servicelineuri_present_152",
            no_task="statestreet_userimport_logs_add_entry_198",
        )

        if_request_servicelineuri_present_152 = rail.IfOperator(
            task_id='if_request_servicelineuri_present_152',
            test='''{{ dag_run.conf.businessareacheckobject.serviceline_uri | is_truthy }}''',
            yes_task="if_log_sub_business_line_uri_169_present_170",
            no_task="statestreet_userimport_logs_add_entry_196",
        )

        if_log_sub_business_line_uri_169_present_170 = rail.IfOperator(
            task_id='if_log_sub_business_line_uri_169_present_170',
            test='''{{ dag_run.conf.businessareacheckobject.subbussinessline_uri | is_truthy }}''',
            yes_task="apply_user_modifications_173",
            no_task="statestreet_userimport_logs_add_entry_182",
        )

        apply_user_modifications_173 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications_173',
            endpoint="/services/importService1.svc/ApplyUserModifications",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['user_uri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": {
                        "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementCostCenterSchedule": [],
                        "updateCostCenterScheduleOverDateRange": {
                            "replacementCostCenterScheduleEntries": [
                                {
                                    "costCenter": {
                                        "uri": dag_run.conf['businessareacheckobject']['subbussinessline_uri'],
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": {
                                        "year": rail.result('log_current_date')['year'],
                                        "month": rail.result('log_current_date')['month'],
                                        "day": rail.result('log_current_date')['day'],
                                    }
                                }
                            ],
                            "endDate": null
                        }
                    },
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": null,
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }

        )

        if_d_errors_present_174 = rail.IfOperator(
            task_id='if_d_errors_present_174',
            test='''{{ result('apply_user_modifications_173').errors | is_truthy }}''',
            yes_task="log_175",
            no_task="statestreet_userimport_logs_add_entry_178",
        )

        log_175 = rail.PythonOperator(
            task_id='log_175',
            python_callable=lambda: rail.result('apply_user_modifications_173')[
                'user']['displayText'] if rail.result('apply_user_modifications_173')['errors'] else None
        )

        statestreet_userimport_logs_add_entry_176 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_176',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Sub Business Line not updated" +
                    rail.result('log_175') +
                dag_run.conf['update_items']['subbusinessline'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        statestreet_userimport_logs_add_entry_178 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_178',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Sub Business Line updated" +
                    dag_run.conf['update_items']['subbusinessline'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Success",
            }
        )

        statestreet_userimport_logs_add_entry_182 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_182',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Incorrect Sub Business Line value" +
                    dag_run.conf['update_items']['subbusinessline'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        statestreet_userimport_logs_add_entry_196 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_196',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + "Incorrect Service Line value" +
                    dag_run.conf['update_items']['serviceline'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        statestreet_userimport_logs_add_entry_198 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_198',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + "No Service Line value provided",
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        if_request_jobfamily_present_199 = rail.IfOperator(
            task_id='if_request_jobfamily_present_199',
            test='''{{ dag_run.conf.update_items.jobfamily | is_truthy }}''',
            yes_task="get_current_division_200",
            no_task="if_request_managerid_present_225",
        )

        get_current_division_200 = rail.RepliconServiceOperator(
            task_id='get_current_division_200',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:division"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:user"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": "{{ dag_run.conf.user_uri }}",
                            "uris": [],
                            "bool": null,
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
                }
            },
            data_handler=get_jobfamilyvalue

        )

        if_request_jobfamily_not_equals_to_dataloggerlog_201message_202 = rail.IfOperator(
            task_id='if_request_jobfamily_not_equals_to_dataloggerlog_201message_202',
            test='''{{ dag_run.conf.update_items.jobfamily != result('get_current_division_200')}}''',
            yes_task="if_request_jobfunction_present_203",
            no_task="if_request_managerid_present_225",
        )

        if_request_jobfunction_present_203 = rail.IfOperator(
            task_id='if_request_jobfunction_present_203',
            test='''{{ dag_run.conf.update_items.jobfunction | is_truthy }}''',
            yes_task="if_request_jobfunctionuri_present_204",
            no_task="statestreet_userimport_logs_add_entry_224",
        )

        if_request_jobfunctionuri_present_204 = rail.IfOperator(
            task_id='if_request_jobfunctionuri_present_204',
            test='''{{ dag_run.conf.jobfunction_uri  | is_truthy }}''',
            yes_task="if_log_job_family_uri_207_present_208",
            no_task="statestreet_userimport_logs_add_entry_222",
        )

        if_log_job_family_uri_207_present_208 = rail.IfOperator(
            task_id='if_log_job_family_uri_207_present_208',
            test='''{{ dag_run.conf.jobfamily_uri | is_truthy }}''',
            yes_task="apply_user_modifications_211",
            no_task="statestreet_userimport_logs_add_entry_220",
        )

        apply_user_modifications_211 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications_211',
            endpoint="/services/importService1.svc/ApplyUserModifications",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['user_uri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": {
                        "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementDivisionSchedule": [],
                        "updateDivisionScheduleOverDateRange": {
                            "replacementDivisionScheduleEntries": [
                                {
                                    "division": {
                                        "uri": dag_run.conf['jobfamily_uri'],
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": {
                                        "year": rail.result('log_current_date')['year'],
                                        "month": rail.result('log_current_date')['month'],
                                        "day": rail.result('log_current_date')['day'],
                                    }
                                }
                            ],
                            "endDate": null
                        }
                    },
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": null,
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        if_d_errors_present_212 = rail.IfOperator(
            task_id='if_d_errors_present_212',
            test='''{{ result('apply_user_modifications_211').errors | is_truthy }}''',
            yes_task="log_213",
            no_task="statestreet_userimport_logs_add_entry_216",
        )

        log_213 = rail.PythonOperator(
            task_id='log_213',
            python_callable=lambda: rail.result('apply_user_modifications_211')[
                'user']['displayText'] if rail.result('apply_user_modifications_211')['errors'] else None
        )

        statestreet_userimport_logs_add_entry_214 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_214',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + "Job Family not updated " +
                    dag_run.conf['update_items']['jobfamily'],
                "field_name": dag_run.conf['update_items']['loginid'] + "|" +
                dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        statestreet_userimport_logs_add_entry_216 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_216',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + "Job Family updated" +
                    dag_run.conf['update_items']['jobfamily'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Success",
            }
        )

        statestreet_userimport_logs_add_entry_220 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_220',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + "Incorrect Job Family value" +
                    dag_run.conf['update_items']['jobfamily'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        statestreet_userimport_logs_add_entry_222 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_222',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Incorrect Job Function value " +
                    dag_run.conf['update_items']['jobfunction'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        statestreet_userimport_logs_add_entry_224 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_224',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + "No Job Function provided",
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        if_request_managerid_present_225 = rail.IfOperator(
            task_id='if_request_managerid_present_225',
            test='''{{ dag_run.conf.update_items.managerid | is_truthy }}''',
            yes_task="get_enabled_users_226",
            no_task="if_log_supervisor_uri_234_present_235",
        )

        get_enabled_users_226 = rail.RepliconServiceOperator(
            task_id='get_enabled_users_226',
            endpoint="/services/userlistService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
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
                                "text": "{{ dag_run.conf.update_items.managerid }}",
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

        if_first_datatype_present_227 = rail.IfOperator(
            task_id='if_first_datatype_present_227',
            # pylint: disable=too-many-statements line-too-long
            test=lambda: (rail.result('get_enabled_users_226')['rows'][0]['cells'][0]['dataType']) if (rail.result('get_enabled_users_226')['rows']) and (rail.result('get_enabled_users_226')[
                'rows'][0]) and (rail.result('get_enabled_users_226')['rows'][0]['cells'][0]) and (rail.result('get_enabled_users_226')['rows'][0]['cells'][0]['dataType']) else None,
            yes_task='foreach_d_228',
            no_task="if_log_supervisor_uri_234_present_235",
        )

        foreach_d_228 = rail.ForEachOperator(
            task_id='foreach_d_228',
            items="{{ result('get_enabled_users_226').rows | to_json }}",
            start_task='accumulate_list_items_229',
            end_task='foreach_d_228_end'
        )

        accumulate_list_items_229 = rail.SetVariableOperator(
            task_id='accumulate_list_items_229',
            name='Supervisors',
            append=True,
            value=lambda: {
                "name": rail.find_first_by_attr_and_get_attr(rail.result('foreach_d_228')['cells'],
                                                             'objectType', 'urn:replicon:object-type:user', 'textValue', ''),
                "employeeid": rail.find_first_by_attr_and_get_attr(rail.result('foreach_d_228')['cells'],
                                                                   'dataType', 'urn:replicon:list-type:string', 'textValue', ''),
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('foreach_d_228')['cells'],
                                                            'objectType', 'urn:replicon:object-type:user', 'uri', '')
            }
        )

        foreach_d_228_end = rail.EmptyOperator(
            task_id='foreach_d_228_end',
        )

        def get_supervisor_count(dag_run):
            record_data = rail.result('accumulate_list_items_229')['value']
            list_count = record_data if record_data else None
            supervisor1 = ''
            count = 0
            for data in list_count:
                if data['employeeid'] == dag_run.conf['update_items']['managerid']:
                    supervisor1 = data['uri']
                    count += 1
            return {
                'count': count,
                'uri': supervisor1
            }

        log_supervisorcount_230 = rail.PythonOperator(
            task_id='log_supervisorcount_230',
            python_callable=get_supervisor_count
        )

        if_log_supervisorcount_230_greater_than_1_231 = rail.IfOperator(
            task_id='if_log_supervisorcount_230_greater_than_1_231',
            test=lambda: rail.result('log_supervisorcount_230')['count'] > 1,
            yes_task="statestreet_userimport_logs_add_entry_232",
            no_task="if_log_supervisorcount_230_equals_to_1_233",
        )

        statestreet_userimport_logs_add_entry_232 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_232',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + " Multiple Users found with Manager ID" +
                    dag_run.conf['update_items']['managerid'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        if_log_supervisorcount_230_equals_to_1_233 = rail.IfOperator(
            task_id='if_log_supervisorcount_230_equals_to_1_233',
            test=lambda: rail.result('log_supervisorcount_230')['count'] == 1,
            yes_task="log_supervisor_uri_234",
            no_task="if_log_supervisor_uri_234_present_235",
        )

        log_supervisor_uri_234 = rail.PythonOperator(
            task_id='log_supervisor_uri_234',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'accumulate_list_items_229')['value'], 'employeeid', dag_run.conf['update_items']['managerid'], 'uri', '')
        )

        if_log_supervisor_uri_234_present_235 = rail.IfOperator(
            task_id='if_log_supervisor_uri_234_present_235',
            test='''{{ result('log_supervisor_uri_234') | is_truthy }}''',
            yes_task="if_log_supervisor_uri_234_not_equals_to_datarestget_user_details_3responsedsupervisoruri_236",
            no_task="statestreet_supervisorassignment_add_entry_249",
        )

        if_log_supervisor_uri_234_not_equals_to_datarestget_user_details_3responsedsupervisoruri_236 = rail.IfOperator(
            task_id='if_log_supervisor_uri_234_not_equals_to_datarestget_user_details_3responsedsupervisoruri_236',
            test=lambda: rail.result('log_supervisor_uri_234') != (rail.result('get_user_details_3')['supervisor']['uri'] if rail.result(
                'get_user_details_3')['supervisor'] and rail.result('get_user_details_3')['supervisor']['uri'] else null),
            yes_task="get_assigned_permission_sets_for_user2_237",
            no_task="if_request_empstatus_equals_to_active_251",
        )

        get_assigned_permission_sets_for_user2_237 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user2_237',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('log_supervisor_uri_234') }}"
            }
        )

        log_check_supervisor_permission_238 = rail.PythonOperator(
            # pylint: disable=too-many-statements line-too-long
            task_id='log_check_supervisor_permission_238',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_assigned_permission_sets_for_user2_237'), 'policyUri', 'urn:replicon:policy:supervision',
                'user.displayText', '') if rail.result('get_assigned_permission_sets_for_user2_237') and rail.result('get_assigned_permission_sets_for_user2_237')[0]['policyUri'] else None
        )

        if_log_check_supervisor_permission_238_present_239 = rail.IfOperator(
            task_id='if_log_check_supervisor_permission_238_present_239',
            test='''{{ result('log_check_supervisor_permission_238') | is_truthy }}''',
            yes_task="apply_user_modifications_240",
            no_task="statestreet_supervisorassignment_add_entry_247",
        )

        apply_user_modifications_240 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications_240',
            endpoint="/services/importService1.svc/ApplyUserModifications",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['user_uri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": {
                        "scheduleEntriesToAdd": [
                            {
                                "supervisor": {
                                    "uri": rail.result('log_supervisor_uri_234'),
                                    "loginName": null,
                                    "parameterCorrelationId": null
                                },
                                "effectiveDate": {
                                    "year": rail.result('log_current_date')['year'],
                                    "month": rail.result('log_current_date')['month'],
                                    "day": rail.result('log_current_date')['day'],
                                }
                            }
                        ],
                        "scheduleEntriesToPut": []
                    },
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": null,
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        if_d_errors_present_241 = rail.IfOperator(
            task_id='if_d_errors_present_241',
            test='''{{ result('apply_user_modifications_240').errors | is_truthy }}''',
            yes_task="log_242",
            no_task="statestreet_userimport_logs_add_entry_245",
        )

        log_242 = rail.PythonOperator(
            task_id='log_242',
            python_callable=lambda: rail.result('apply_user_modifications_240')[
                'user']['displayText'] if rail.result('apply_user_modifications_240')['errors'] else None
        )

        statestreet_userimport_logs_add_entry_243 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_243',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User Supervisor -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + "Supervisor not updated" +
                    rail.result('log_242') +
                dag_run.conf['update_items']['managerid'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Failed",
            }
        )

        statestreet_userimport_logs_add_entry_245 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_245',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User Supervisor -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + "Supervisor Updated to " +
                    dag_run.conf['update_items']['managerid'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Success",
            }
        )

        statestreet_supervisorassignment_add_entry_247 = rail.WriteLogOperator(
            task_id='statestreet_supervisorassignment_add_entry_247',
            log="{{ dag_run.conf.supervisor_logtable}}",
            message="na",
            severity="",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "user_uri": dag_run.conf['user_uri'],
                "manager_id": dag_run.conf['update_items']['managerid'],
                "user_id": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'],
                "status": "Not Assigned"
            }
        )

        statestreet_supervisorassignment_add_entry_249 = rail.WriteLogOperator(
            task_id='statestreet_supervisorassignment_add_entry_249',
            log="{{ dag_run.conf.supervisor_logtable}}",
            message="na",
            severity="",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "user_uri": dag_run.conf['user_uri'],
                "manager_id": dag_run.conf['update_items']['managerid'],
                "user_id": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'],
                "status": "Not Assigned"
            }
        )

        if_request_empstatus_equals_to_active_251 = rail.IfOperator(
            task_id='if_request_empstatus_equals_to_active_251',
            test='''{{ dag_run.conf.update_items.employeestatus == 'Active'  and result('get_user_details_3').isEnabled | is_falsy }}''',
            yes_task="enable_login_252",
            no_task="if_request_empstatus_equals_to_onleave_254",
        )

        enable_login_252 = rail.RepliconServiceOperator(
            task_id='enable_login_252',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.user_uri}}"
            }
        )

        statestreet_userimport_logs_add_entry_253 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_253',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + "User Enabled" +
                    dag_run.conf['update_items']['employeestatus'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Success",
            }
        )

        if_request_empstatus_equals_to_onleave_254 = rail.IfOperator(
            task_id='if_request_empstatus_equals_to_onleave_254',
            test='''{{ dag_run.conf.update_items.employeestatus == 'On Leave'  and result('get_user_details_3').isEnabled | is_truthy }}''',
            yes_task="disable_login_255",
            no_task="if_request_standardhours_present_77",
        )

        disable_login_255 = rail.RepliconServiceOperator(
            task_id='disable_login_255',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}"
            }
        )

        statestreet_userimport_logs_add_entry_256 = rail.WriteLogOperator(
            task_id='statestreet_userimport_logs_add_entry_256',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": "Update User -" + rail.render_template("{{dag_run_ecid()}}") +
                "-" + "User Disabled" +
                    dag_run.conf['update_items']['employeestatus'],
                "field_name": dag_run.conf['update_items']['loginid'] +
                "|" + dag_run.conf['update_items']['employeeid'] +
                    "|" + dag_run.conf['update_items']['name'],
                "status": "Success",
            }
        )

        def get_severity():
            if rail.get_current_context()['dag_run'].get_task_instance('update_numeric_value_81').current_state() == 'failed':
                res = "Failed"
            else:
                res = "Error"
            return res

        catch = rail.EmptyOperator(
            task_id='catch',
            trigger_rule='one_failed'
        )

        def get_details_data(dag_run):
            error_message = rail.render_template("{{get_error_message()}}")
            if rail.get_current_context()['dag_run'].get_task_instance('update_numeric_value_81').current_state() == 'failed':
                output_res = "Update User -" + rail.render_template(
                    "{{dag_run_ecid()}}") + "-  Invalid value for Standard Hours provided " + dag_run.conf['update_items']['standardhours']
            else:
                output_res = "Update User -" + rail.render_template(
                    "{{dag_run_ecid()}}") + error_message
            return output_res

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity=get_severity,
            properties=lambda dag_run: {
                "job_id": dag_run.conf['job_id'],
                "unknown_field": " ",
                "details": get_details_data(dag_run),
                "field_name": dag_run.conf['user_items']['loginid'] + "|" + dag_run.conf['user_items']['employeeid'] + "|" + dag_run.conf['user_items']['name'],
                "status": get_severity(),

            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_user_details_3
        get_user_details_3 >> log_current_date >> if_request_email_present_4
        if_request_email_present_4 >> rail.Label(
            'Yes') >> if_request_email_contains_5
        if_request_email_contains_5 >> rail.Label(
            'Yes') >> if_request_email_not_equals_to_datarestget_user_details_3responsedemailaddress_6
        if_request_email_not_equals_to_datarestget_user_details_3responsedemailaddress_6 >> rail.Label(
            'Yes') >> update_email_7 >> statestreet_userimport_logs_add_entry_8 >> if_request_name_present_11
        if_request_email_not_equals_to_datarestget_user_details_3responsedemailaddress_6 >> rail.Label(
            'No') >> if_request_name_present_11
        if_request_email_not_equals_to_datarestget_user_details_3responsedemailaddress_6 >> rail.Label(
            'No') >> if_request_name_present_11
        if_request_email_present_4 >> rail.Label(
            'No') >> if_request_name_present_11
        if_request_email_contains_5 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_10 >> if_request_name_present_11
        if_request_name_present_11 >> rail.Label(
            'No') >> if_request_emptype_present_25
        if_request_name_present_11 >> rail.Label(
            'Yes') >> if_request_name_contains_12
        if_request_name_contains_12 >> rail.Label(
            'Yes') >> if_request_name_not_equals_to_datarestget_user_details_3responseddisplaytext_13
        if_request_name_contains_12 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_23 >> if_request_emptype_present_25
        if_request_name_not_equals_to_datarestget_user_details_3responseddisplaytext_13 >> rail.Label(
            'Yes') >> log_user_name >> if_first_name_present
        if_first_name_present >> rail.Label(
            'Yes') >> update_first_name_17 >> update_last_name_18 >> statestreet_userimport_logs_add_entry_19
        statestreet_userimport_logs_add_entry_19 >> if_log_first_name_14_blank_20
        if_first_name_present >> rail.Label(
            'No') >> if_log_first_name_14_blank_20
        if_log_first_name_14_blank_20 >> rail.Label(
            'Yes') >> statestreet_userimport_logs_add_entry_21 >> if_request_emptype_present_25
        if_log_first_name_14_blank_20 >> rail.Label(
            'No') >> if_request_emptype_present_25
        if_request_name_not_equals_to_datarestget_user_details_3responseddisplaytext_13 >> rail.Label(
            'No') >> if_request_emptype_present_25
        if_log_first_name_14_blank_20 >> rail.Label(
            'No') >> if_request_emptype_present_25
        if_request_emptype_present_25 >> rail.Label(
            'Yes') >> get_employee_type_for_user_26
        get_employee_type_for_user_26 >> if_request_emptype_not_equals_to_datarestget_employee_type_for_user_26responsedname_27
        if_request_emptype_not_equals_to_datarestget_employee_type_for_user_26responsedname_27 >> rail.Label(
            'Yes') >> get_all_employee_type_details_28 >> log_employeetype_uri >> if_log_employeetype_uri_present_30
        if_log_employeetype_uri_present_30 >> rail.Label(
            'Yes') >> update_employee_type_for_user_31 >> statestreet_userimport_logs_add_entry_32
        statestreet_userimport_logs_add_entry_32 >> if_request_costcenternumber_present_36
        if_log_employeetype_uri_present_30 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_34 >> if_request_costcenternumber_present_36
        if_request_emptype_not_equals_to_datarestget_employee_type_for_user_26responsedname_27 >> rail.Label(
            'No') >> if_request_costcenternumber_present_36
        if_request_emptype_present_25 >> rail.Label(
            'No') >> if_request_costcenternumber_present_36
        if_request_costcenternumber_present_36 >> rail.Label(
            'Yes') >> get_department_for_user_37
        get_department_for_user_37 >> if_request_costcenternumber_not_equals_to_datarestget_department_for_user_37responseddisplaytext_38
        if_request_costcenternumber_not_equals_to_datarestget_department_for_user_37responseddisplaytext_38 >> rail.Label(
            'Yes') >> if_request_legalentityname_present_39

        if_request_legalentityname_present_39 >> rail.Label(
            'Yes') >> if_log_legal_entity_uri_40_present_41
        if_log_legal_entity_uri_40_present_41 >> rail.Label(
            'Yes') >> if_d_isenabled_is_true_43
        if_d_isenabled_is_true_43 >> rail.Label(
            'Yes') >> if_log_cost_center_number_uri_45_present_46
        if_log_cost_center_number_uri_45_present_46 >> rail.Label(
            'Yes') >> if_d_isenabled_is_true_48
        if_d_isenabled_is_true_48 >> rail.Label(
            'Yes') >> update_department_for_user_49 >> statestreet_userimport_logs_add_entry_50 >> if_request_banktitle_present_62
        if_d_isenabled_is_true_48 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_52 >> if_request_banktitle_present_62
        if_log_cost_center_number_uri_45_present_46 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_54 >> if_request_banktitle_present_62
        if_d_isenabled_is_true_43 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_56 >> if_request_banktitle_present_62
        if_log_legal_entity_uri_40_present_41 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_58 >> if_request_banktitle_present_62
        if_request_legalentityname_present_39 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_60 >> if_request_banktitle_present_62
        if_request_costcenternumber_not_equals_to_datarestget_department_for_user_37responseddisplaytext_38 >> rail.Label(
            'No') >> if_request_banktitle_present_62
        if_request_costcenternumber_present_36 >> rail.Label(
            'No') >> if_request_banktitle_present_62
        if_request_banktitle_present_62 >> rail.Label(
            'Yes') >> log_bantitle >> if_request_banktitle_not_equals_to_dataloggerlog_63message_64
        if_request_banktitle_not_equals_to_dataloggerlog_63message_64 >> rail.Label(
            'Yes') >> if_log_66_present_67
        if_log_66_present_67 >> rail.Label(
            'Yes') >> update_dropdown_value_68 >> statestreet_userimport_logs_add_entry_69
        statestreet_userimport_logs_add_entry_69 >> if_request_banktitle_equals_to_managingdirector_70
        if_request_banktitle_equals_to_managingdirector_70 >> rail.Label(
            'Yes') >> put_permission_set_assignments_for_user_supervisorand_report_user_71 >> statestreet_userimport_logs_add_entry_72
        statestreet_userimport_logs_add_entry_72 >> remove_policy_set_assignment_from_user_remove_timesheet_template_73
        remove_policy_set_assignment_from_user_remove_timesheet_template_73 >> statestreet_userimport_logs_add_entry_74
        statestreet_userimport_logs_add_entry_74 >> if_request_fullparttime_present_85
        if_request_banktitle_equals_to_managingdirector_70 >> rail.Label(
            'No') >> if_request_fullparttime_present_85
        if_request_banktitle_present_62 >> rail.Label(
            'No') >> if_request_fullparttime_present_85
        if_log_66_present_67 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_76 >> if_request_fullparttime_present_85
        if_request_banktitle_not_equals_to_dataloggerlog_63message_64 >> rail.Label(
            'No') >> if_request_fullparttime_present_85
        if_request_standardhours_present_77 >> rail.Label(
            'Yes') >> log_78 >> if_request_standardhours_not_equals_to_dataloggerlog_78message_79
        if_request_standardhours_not_equals_to_dataloggerlog_78message_79 >> rail.Label(
            'Yes') >> update_numeric_value_81 >> statestreet_userimport_logs_add_entry_82
        statestreet_userimport_logs_add_entry_82 >> catch
        if_request_standardhours_not_equals_to_dataloggerlog_78message_79 >> rail.Label(
            'No') >> catch
        if_request_standardhours_present_77 >> rail.Label(
            'No') >> catch >> catch_and_log_error >> log_to_sumo
        if_request_fullparttime_present_85 >> rail.Label(
            'Yes') >> log_86 >> if_request_fullparttime_not_equals_to_dataloggerlog_86message_87
        if_request_fullparttime_not_equals_to_dataloggerlog_86message_87 >> rail.Label(
            'Yes') >> if_log_89_present_90
        if_log_89_present_90 >> rail.Label(
            'Yes') >> update_dropdown_value_91 >> statestreet_userimport_logs_add_entry_92
        statestreet_userimport_logs_add_entry_92 >> if_request_banktitle_not_equals_to_managingdirector_95
        if_log_89_present_90 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_94 >> if_request_banktitle_not_equals_to_managingdirector_95
        if_request_fullparttime_not_equals_to_dataloggerlog_86message_87 >> rail.Label(
            'No') >> if_request_banktitle_not_equals_to_managingdirector_95
        if_request_fullparttime_present_85 >> rail.Label(
            'No') >> if_request_banktitle_not_equals_to_managingdirector_95
        if_request_banktitle_not_equals_to_managingdirector_95 >> rail.Label(
            'Yes') >> if_request_managernonmanager_present_96
        if_request_managernonmanager_present_96 >> rail.Label(
            'Yes') >> if_request_managernonmanager_equals_to_yes_97
        if_request_managernonmanager_equals_to_yes_97 >> rail.Label(
            'Yes') >> put_permission_set_assignments_for_user_supervisorand_project_resourcewith_reports_98
        put_permission_set_assignments_for_user_supervisorand_project_resourcewith_reports_98 >> if_request_locationcode_present_104
        if_request_managernonmanager_equals_to_yes_97 >> rail.Label(
            'No') >> put_permission_set_assignments_for_user_project_resource_100
        if_request_banktitle_not_equals_to_managingdirector_95 >> rail.Label(
            'No') >> if_request_locationcode_present_104
        if_request_managernonmanager_present_96 >> rail.Label(
            'No') >> put_permission_set_assignments_for_user_project_resource_100 >> if_request_locationcode_present_104
        if_request_locationcode_present_104 >> rail.Label(
            'Yes') >> get_current_location_105 >> if_request_locationcode_not_equals_to_dataloggerlog_106message_107
        if_request_locationcode_not_equals_to_dataloggerlog_106message_107 >> rail.Label(
            'Yes') >> if_request_region_present_108
        if_request_region_present_108 >> rail.Label(
            'Yes') >> if_request_regionuri_present_109
        if_request_regionuri_present_109 >> rail.Label(
            'Yes') >> if_log_location_uri_121_present_122
        if_log_location_uri_121_present_122 >> rail.Label(
            'Yes') >> apply_user_modifications_125 >> if_d_errors_present_126
        if_d_errors_present_126 >> rail.Label(
            'Yes') >> log_127 >> statestreet_userimport_logs_add_entry_128 >> if_request_subbusinessline_present_147
        if_d_errors_present_126 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_130 >> if_request_subbusinessline_present_147
        if_log_location_uri_121_present_122 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_134 >> if_request_subbusinessline_present_147
        if_request_regionuri_present_109 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_144 >> if_request_subbusinessline_present_147
        if_request_region_present_108 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_146 >> if_request_subbusinessline_present_147
        if_request_locationcode_not_equals_to_dataloggerlog_106message_107 >> rail.Label(
            'No') >> if_request_subbusinessline_present_147
        if_request_locationcode_present_104 >> rail.Label(
            'No') >> if_request_subbusinessline_present_147
        if_request_subbusinessline_present_147 >> rail.Label(
            'Yes') >> get_current_cost_center_148 >> if_request_subbusinessline_not_equals_to_dataloggerlog_149message_150
        if_request_subbusinessline_not_equals_to_dataloggerlog_149message_150 >> rail.Label(
            'Yes') >> if_request_serviceline_present_151
        if_request_serviceline_present_151 >> rail.Label(
            'Yes') >> if_request_servicelineuri_present_152
        if_request_servicelineuri_present_152 >> rail.Label(
            'Yes') >> if_log_sub_business_line_uri_169_present_170 >> apply_user_modifications_173 >> if_d_errors_present_174
        if_d_errors_present_174 >> rail.Label(
            'Yes') >> log_175 >> statestreet_userimport_logs_add_entry_176 >> if_request_jobfamily_present_199
        if_d_errors_present_174 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_178 >> if_request_jobfamily_present_199
        if_log_sub_business_line_uri_169_present_170 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_182 >> if_request_jobfamily_present_199
        if_request_servicelineuri_present_152 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_196 >> if_request_jobfamily_present_199
        if_request_serviceline_present_151 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_198 >> if_request_jobfamily_present_199

        if_request_subbusinessline_not_equals_to_dataloggerlog_149message_150 >> rail.Label(
            'No') >> if_request_jobfamily_present_199
        if_request_subbusinessline_present_147 >> rail.Label(
            'No') >> if_request_jobfamily_present_199
        if_request_jobfamily_present_199 >> rail.Label(
            'Yes') >> get_current_division_200 >> if_request_jobfamily_not_equals_to_dataloggerlog_201message_202
        if_request_jobfamily_not_equals_to_dataloggerlog_201message_202 >> rail.Label(
            'Yes') >> if_request_jobfunction_present_203
        if_request_jobfunction_present_203 >> rail.Label(
            'Yes') >> if_request_jobfunctionuri_present_204
        if_request_jobfunctionuri_present_204 >> rail.Label(
            'Yes') >> if_log_job_family_uri_207_present_208
        if_log_job_family_uri_207_present_208 >> rail.Label(
            'Yes') >> apply_user_modifications_211 >> if_d_errors_present_212
        if_d_errors_present_212 >> rail.Label(
            'Yes') >> log_213 >> statestreet_userimport_logs_add_entry_214 >> if_request_managerid_present_225
        if_d_errors_present_212 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_216 >> if_request_managerid_present_225
        if_log_job_family_uri_207_present_208 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_220 >> if_request_managerid_present_225
        if_request_jobfunctionuri_present_204 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_222 >> if_request_managerid_present_225
        if_request_jobfunction_present_203 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_224 >> if_request_managerid_present_225
        if_request_jobfamily_not_equals_to_dataloggerlog_201message_202 >> rail.Label(
            'No') >> if_request_managerid_present_225
        if_request_jobfamily_present_199 >> rail.Label(
            'Yes') >> if_request_managerid_present_225
        if_request_managerid_present_225 >> rail.Label(
            'Yes') >> get_enabled_users_226 >> if_first_datatype_present_227
        if_first_datatype_present_227 >> rail.Label(
            'Yes') >> foreach_d_228 >> accumulate_list_items_229 >> foreach_d_228_end
        foreach_d_228 >> foreach_d_228_end >> log_supervisorcount_230
        log_supervisorcount_230 >> if_log_supervisorcount_230_greater_than_1_231
        if_log_supervisorcount_230_greater_than_1_231 >> rail.Label(
            'Yes') >> statestreet_userimport_logs_add_entry_232 >> if_log_supervisorcount_230_equals_to_1_233
        if_log_supervisorcount_230_greater_than_1_231 >> rail.Label(
            'No') >> if_log_supervisorcount_230_equals_to_1_233
        if_log_supervisorcount_230_equals_to_1_233 >> rail.Label(
            'Yes') >> log_supervisor_uri_234 >> if_log_supervisor_uri_234_present_235
        if_log_supervisorcount_230_equals_to_1_233 >> rail.Label(
            'No') >> if_log_supervisor_uri_234_present_235
        if_first_datatype_present_227 >> rail.Label(
            'No') >> if_log_supervisor_uri_234_present_235
        if_request_managerid_present_225 >> rail.Label(
            'No') >> if_log_supervisor_uri_234_present_235
        if_log_supervisor_uri_234_present_235 >> rail.Label(
            'Yes') >> if_log_supervisor_uri_234_not_equals_to_datarestget_user_details_3responsedsupervisoruri_236
        if_log_supervisor_uri_234_not_equals_to_datarestget_user_details_3responsedsupervisoruri_236 >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_user2_237 >> log_check_supervisor_permission_238 >> if_log_check_supervisor_permission_238_present_239
        if_log_check_supervisor_permission_238_present_239 >> rail.Label(
            'Yes') >> apply_user_modifications_240 >> if_d_errors_present_241
        if_d_errors_present_241 >> rail.Label(
            'Yes') >> log_242 >> statestreet_userimport_logs_add_entry_243 >> if_request_empstatus_equals_to_active_251
        if_d_errors_present_241 >> rail.Label(
            'No') >> statestreet_userimport_logs_add_entry_245 >> if_request_empstatus_equals_to_active_251
        if_log_check_supervisor_permission_238_present_239 >> rail.Label(
            'No') >> statestreet_supervisorassignment_add_entry_247 >> if_request_empstatus_equals_to_active_251
        if_log_supervisor_uri_234_not_equals_to_datarestget_user_details_3responsedsupervisoruri_236 >> rail.Label(
            'No') >> if_request_empstatus_equals_to_active_251
        if_log_supervisor_uri_234_present_235 >> rail.Label(
            'No') >> statestreet_supervisorassignment_add_entry_249 >> if_request_empstatus_equals_to_active_251
        if_request_empstatus_equals_to_active_251 >> rail.Label(
            'Yes') >> enable_login_252 >> statestreet_userimport_logs_add_entry_253 >> if_request_empstatus_equals_to_onleave_254
        if_request_empstatus_equals_to_active_251 >> rail.Label(
            'No') >> if_request_empstatus_equals_to_onleave_254
        if_request_empstatus_equals_to_onleave_254 >> rail.Label(
            'Yes') >> disable_login_255 >> statestreet_userimport_logs_add_entry_256
        statestreet_userimport_logs_add_entry_256 >> if_request_standardhours_present_77
        if_request_empstatus_equals_to_onleave_254 >> rail.Label(
            'No') >> if_request_standardhours_present_77

        return dag


rail.for_each_instance(create_dag)
