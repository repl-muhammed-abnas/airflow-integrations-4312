
from datetime import datetime, timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.horizonmedia_user_import_update_user_child,
        description=f'Horizonmedia_Child_Add User_V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='declare_list_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_list_2',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_list_2 = rail.SetVariableOperator(
            task_id='declare_list_2',
            append=False,
            name='Exception',
            value=[]
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        bulk_get_users3_4 = rail.RepliconServiceOperator(
            task_id='bulk_get_users3_4',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ dag_run.conf.useruri }}",
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        get_effective_user_group_membership_5 = rail.RepliconServiceOperator(
            task_id='get_effective_user_group_membership_5',
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": null
            }
        )

        declare_variable_6 = rail.SetVariableOperator(
            task_id='declare_variable_6',
            append=False,
            name='declare_variable_6',
            value=None
        )

        date_split_today_7 = rail.PythonOperator(
            task_id='date_split_today_7',
            python_callable=lambda: {
                    "day": datetime.utcnow().day, "month": datetime.utcnow().month, "year": datetime.utcnow().year
            }
        )

        def get_replicon_date(date_str):
            if not date_str:
                return None
            # date format in "07/15/2019"
            date = datetime.strptime(date_str.split(" ")[0], '%m/%d/%Y')
            return {
                'year': date.year,
                'month': date.month,
                'day': date.day
            }

        date_split_startdate_11 = rail.PythonOperator(
            task_id='date_split_startdate_11',
            python_callable=lambda: get_replicon_date(
                rail.get_dag_run_conf()['Start_Date'])
        )

        is_holidaycalendar_uri_present = rail.IfOperator(
            task_id='is_holidaycalendar_uri_present',
            test="{{ dag_run.conf.holiday_calendar_uri | is_truthy }}",
            yes_task="if_holiday_calendar_mismatch",
            no_task="if_userdetails_isenabled_is_not_true_12"
        )

        if_holiday_calendar_mismatch = rail.IfOperator(
            task_id='if_holiday_calendar_mismatch',
            test='''{{ result('bulk_get_users3_4')[0].holidayCalendar | is_falsy or result('bulk_get_users3_4')[0].holidayCalendar | is_truthy and \
                result('bulk_get_users3_4')[0].holidayCalendar.uri != dag_run.conf.holiday_calendar_uri }}''',
            yes_task="update_holiday_calendar",
            no_task="if_userdetails_isenabled_is_not_true_12",
        )

        update_holiday_calendar = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                'userUri': "{{ dag_run.conf.useruri }}",
                "holidayCalendarUri": "{{ dag_run.conf.holiday_calendar_uri }}"
            }
        )

        if_userdetails_isenabled_is_not_true_12 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_12',
            test='''{{ result('bulk_get_users3_4')[0].userDetails.isEnabled | is_falsy }}''',
            yes_task="enable_login_enablelogin_13",
            no_task="log_displayname_16",
        )

        enable_login_enablelogin_13 = rail.RepliconServiceOperator(
            task_id='enable_login_enablelogin_13',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        update_employment_date_rangeforenddate_updatestartdatewithoutenddate_14 = rail.RepliconServiceOperator(
            task_id='update_employment_date_rangeforenddate_updatestartdatewithoutenddate_14',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('date_split_startdate_11').year }}",
                        "month": "{{ result('date_split_startdate_11').month }}",
                        "day": "{{ result('date_split_startdate_11').day }}",
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        log_displayname_16 = rail.PythonOperator(
            task_id='log_displayname_16',
            python_callable=lambda:  f"{rail.get_dag_run_conf()['firstname']} {rail.get_dag_run_conf()['lastname']}"
        )

        if_userdetails_customdisplayname_not_equals_to_datalogger331589ebmessage_17 = rail.IfOperator(
            task_id='if_userdetails_customdisplayname_not_equals_to_datalogger331589ebmessage_17',
            test='''{{ result('bulk_get_users3_4')[0].userDetails.customDisplayName != result('log_displayname_16') or result('bulk_get_users3_4')[0].timesheetApprovalPath.displayText != dag_run.conf.TS_Approval_Path }}''',
            yes_task="update_timesheet_approval_path",
            no_task="if_request_start_date_present_19",
        )

        update_timesheet_approval_path = rail.RepliconServiceOperator(
            task_id='update_timesheet_approval_path',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timesheetApprovalPathToApply": {
                        "uri": null,
                        "name": "{{ dag_run.conf.TS_Approval_Path }}"
                    },
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": null,
                        "employeeId": null,
                        "displayNameParameter": {
                            "displayName": "{{ result('log_displayname_16') }}"
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_start_date_present_19 = rail.IfOperator(
            task_id='if_request_start_date_present_19',
            test='''{{ dag_run.conf.Start_Date | is_truthy }}''',
            yes_task="if_emp_date_changed",
            no_task="if_first_presence_not_equals_to_urnrepliconuserauthenticationtypesso_23",
        )

        if_emp_date_changed = rail.IfOperator(
            task_id='if_emp_date_changed',
            test=lambda: datetime(**rail.result('date_split_startdate_11')) != datetime(
                **rail.result('bulk_get_users3_4')[0]['userDetails']['employmentDateRange']['startDate']),
            yes_task="update_employment_date_rangeforenddate_updatestartdatewithoutenddate_21",
            no_task="if_first_presence_not_equals_to_urnrepliconuserauthenticationtypesso_23",
        )

        update_employment_date_rangeforenddate_updatestartdatewithoutenddate_21 = rail.RepliconServiceOperator(
            task_id='update_employment_date_rangeforenddate_updatestartdatewithoutenddate_21',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('date_split_startdate_11').year }}",
                        "month": "{{ result('date_split_startdate_11').month }}",
                        "day": "{{ result('date_split_startdate_11').day }}",
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        if_first_presence_not_equals_to_urnrepliconuserauthenticationtypesso_23 = rail.IfOperator(
            task_id='if_first_presence_not_equals_to_urnrepliconuserauthenticationtypesso_23',
            test='''{{ result('bulk_get_users3_4')[0].securityConfiguration.enabledAuthenticationTypeUris[0]!='urn:replicon:user-authentication-type:sso' }}''',
            yes_task="set_s_s_o_authentication_for_user_24",
            no_task="if_request_user_name_present_25",
        )

        set_s_s_o_authentication_for_user_24 = rail.RepliconServiceOperator(
            task_id='set_s_s_o_authentication_for_user_24',
            endpoint="/services/securityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "loginName": "{{ dag_run.conf.User_Name }}"
            }
        )

        if_request_user_name_present_25 = rail.IfOperator(
            task_id='if_request_user_name_present_25',
            test='''{{ dag_run.conf.User_Name | is_truthy  and dag_run.conf.User_Name != result('bulk_get_users3_4')[0].securityConfiguration.user.loginName }}''',
            yes_task="set_s_s_o_authentication_for_user_updateloginname_26",
            no_task="if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_27",
        )

        set_s_s_o_authentication_for_user_updateloginname_26 = rail.RepliconServiceOperator(
            task_id='set_s_s_o_authentication_for_user_updateloginname_26',
            endpoint="/services/securityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "loginName": "{{ dag_run.conf.User_Name }}"
            }
        )

        if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_27 = rail.IfOperator(
            task_id='if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_27',
            test='''{{ dag_run.conf.firstname | is_truthy and result('bulk_get_users3_4')[0].userDetails.firstName | lower != dag_run.conf.firstname | lower }}''',
            yes_task="update_first_name_28",
            no_task="if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_29",
        )

        update_first_name_28 = rail.RepliconServiceOperator(
            task_id='update_first_name_28',
            endpoint="/services/userService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.firstname }}"
            }
        )

        if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_29 = rail.IfOperator(
            task_id='if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_29',
            test='''{{ dag_run.conf.lastname | is_truthy  and result('bulk_get_users3_4')[0].userDetails.lastName | lower != dag_run.conf.lastname  | lower }}''',
            yes_task="update_last_name_30",
            no_task="if_request_work_email_present_31",
        )

        update_last_name_30 = rail.RepliconServiceOperator(
            task_id='update_last_name_30',
            endpoint="/services/userService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        if_request_work_email_present_31 = rail.IfOperator(
            task_id='if_request_work_email_present_31',
            test='''{{ dag_run.conf.Work_Email | is_truthy and result('bulk_get_users3_4')[0].userDetails.emailAddress != dag_run.conf.Work_Email }}''',
            yes_task="update_email_32",
            no_task="invoke_custom_ruby_code_33",
        )

        update_email_32 = rail.RepliconServiceOperator(
            task_id='update_email_32',
            endpoint="/services/userService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.Work_Email }}"
            }
        )

        invoke_custom_ruby_code_33 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_33',
            python_callable=lambda: {
                "positionid": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Position ID", "text"),
                "businesstitle": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Business Title", "text"),
                "workspace": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Work Space", "text"),
                "costcenter": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Cost Center", "text"),
                "costcentercode": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Cost Center Code", "text"),
                "department": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Department", "text"),
                "departmentcode": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Department Code", "text"),
                "profitcenter": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Profit Center", "text"),
                "profitcentercode": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Profit Center Code", "text"),
                "company": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Company", "text"),
                "companycode": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Company Code", "text"),
                "prefferedfullname": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Preferred Full Name", "text"),
                "fulllegalname": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Full Legal Name", "text"),
                "managementlevel": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Management Level", "text"),
                "managementlevelcode": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Management Level Code", "text"),
                "employeeresidence": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Employee Residence - State", "text"),
                "ceo": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "CEO", "text"),
                "ceo1": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "CEO -1", "text"),
                "ceo2": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "CEO -2", "text"),
                "ceo3": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "CEO -3", "text"),
                "ceo4": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "CEO -4", "text"),
                "ceo5": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "CEO -5", "text"),
                "ceo6": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "CEO -6", "text"),
                "groupleader": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Group Leader", "text"),
                "businesleader": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Business Leader", "text"),
                "contingentworkertype": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Time_Type", "text"),
                "workerstatus": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Worker Status", "text"),
                "firstdayofleave": rail.find_first_by_attr_and_get_attr(rail.result("bulk_get_users3_4")[0]['userDetails']['customFieldValues'], "customField.displayText", 'First Day of Leave', 'date'),
                "lastdayofleave": rail.find_first_by_attr_and_get_attr(rail.result("bulk_get_users3_4")[0]['userDetails']['customFieldValues'], "customField.displayText", 'Actual Last Day of Leave', 'date'),
                "country": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Country", "text"),
                "scheduledweeklyhours": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Scheduled Weekly Hours", "text"),
                "payrollid": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Payroll ID", "text"),
                "manager": rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3_4')[0]['userDetails']['customFieldValues'], "customField.displayText", "Manager", 'text')
            }
        )

        declare_variable_34 = rail.SetVariableOperator(
            task_id='declare_variable_34',
            append=False,
            name='declare_variable_34',
            value=None
        )

        if_request_position_id_present_35 = rail.IfOperator(
            task_id='if_request_position_id_present_35',
            test='''{{ dag_run.conf.Position_ID | is_truthy  and result('invoke_custom_ruby_code_33').positionid != dag_run.conf.Position_ID }}''',
            yes_task="update_text_value_position_i_d_36",
            no_task="if_request_businesstitle_present_37",
        )

        update_text_value_position_i_d_36 = rail.RepliconServiceOperator(
            task_id='update_text_value_position_i_d_36',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.positionid_udfuri }}",
                "value": "{{ dag_run.conf.Position_ID }}"
            }
        )

        if_request_businesstitle_present_37 = rail.IfOperator(
            task_id='if_request_businesstitle_present_37',
            test='''{{ dag_run.conf.BusinessTitle | is_truthy  and result('invoke_custom_ruby_code_33').businesstitle != dag_run.conf.BusinessTitle }}''',
            yes_task="update_text_value_businesstitle_38",
            no_task="if_request_cost_center_code_present_39",
        )

        update_text_value_businesstitle_38 = rail.RepliconServiceOperator(
            task_id='update_text_value_businesstitle_38',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.businesstitle_udfuri }}",
                "value": "{{ dag_run.conf.BusinessTitle }}"
            }
        )

        if_request_cost_center_code_present_39 = rail.IfOperator(
            task_id='if_request_cost_center_code_present_39',
            test='''{{ dag_run.conf.Cost_Center_Code | is_truthy  and result('invoke_custom_ruby_code_33').costcentercode != dag_run.conf.Cost_Center_Code }}''',
            yes_task="update_text_value_costcentercode_40",
            no_task="if_request_department_code_present_41",
        )

        update_text_value_costcentercode_40 = rail.RepliconServiceOperator(
            task_id='update_text_value_costcentercode_40',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.costcentercode_udfuri }}",
                "value": "{{ dag_run.conf.Cost_Center_Code }}"
            }
        )

        if_request_department_code_present_41 = rail.IfOperator(
            task_id='if_request_department_code_present_41',
            test='''{{ dag_run.conf.Department_Code | is_truthy  and result('invoke_custom_ruby_code_33').departmentcode != dag_run.conf.Department_Code }}''',
            yes_task="update_text_value_departmentcode_42",
            no_task="if_request_profit_center_code_present_43",
        )

        update_text_value_departmentcode_42 = rail.RepliconServiceOperator(
            task_id='update_text_value_departmentcode_42',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.departmentcode_udfuri }}",
                "value": "{{ dag_run.conf.Department_Code }}"
            }
        )

        if_request_profit_center_code_present_43 = rail.IfOperator(
            task_id='if_request_profit_center_code_present_43',
            test='''{{ dag_run.conf.Profit_Center_Code | is_truthy  and result('invoke_custom_ruby_code_33').profitcentercode != dag_run.conf.Profit_Center_Code }}''',
            yes_task="update_text_value_profitcentercode_44",
            no_task="if_request_company_code_present_45",
        )

        update_text_value_profitcentercode_44 = rail.RepliconServiceOperator(
            task_id='update_text_value_profitcentercode_44',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.profitcentercode_udfuri }}",
                "value": "{{ dag_run.conf.Profit_Center_Code }}"
            }
        )

        if_request_company_code_present_45 = rail.IfOperator(
            task_id='if_request_company_code_present_45',
            test='''{{ dag_run.conf.Company_Code | is_truthy  and result('invoke_custom_ruby_code_33').companycode != dag_run.conf.Company_Code }}''',
            yes_task="update_text_value_companycode_46",
            no_task="if_request_pref_name_present_47",
        )

        update_text_value_companycode_46 = rail.RepliconServiceOperator(
            task_id='update_text_value_companycode_46',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.companycode_udfuri }}",
                "value": "{{ dag_run.conf.Company_Code }}"
            }
        )

        if_request_pref_name_present_47 = rail.IfOperator(
            task_id='if_request_pref_name_present_47',
            test='''{{ dag_run.conf.Pref_Name | is_truthy  and result('invoke_custom_ruby_code_33').prefferedfullname != dag_run.conf.Pref_Name }}''',
            yes_task="update_text_value_preferredfullname_48",
            no_task="if_request_legal_name_present_49",
        )

        update_text_value_preferredfullname_48 = rail.RepliconServiceOperator(
            task_id='update_text_value_preferredfullname_48',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.prefferedfullname_udfuri }}",
                "value": "{{ dag_run.conf.Pref_Name }}"
            }
        )

        if_request_legal_name_present_49 = rail.IfOperator(
            task_id='if_request_legal_name_present_49',
            test='''{{ dag_run.conf.Legal_Name | is_truthy  and result('invoke_custom_ruby_code_33').fulllegalname != dag_run.conf.Legal_Name }}''',
            yes_task="update_text_value_companycode_50",
            no_task="if_request_mgmt_code_present_51",
        )

        update_text_value_companycode_50 = rail.RepliconServiceOperator(
            task_id='update_text_value_companycode_50',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.fulllegalname_udfuri }}",
                "value": "{{ dag_run.conf.Legal_Name }}"
            }
        )

        if_request_mgmt_code_present_51 = rail.IfOperator(
            task_id='if_request_mgmt_code_present_51',
            test='''{{ dag_run.conf.Mgmt_Code | is_truthy  and result('invoke_custom_ruby_code_33').managementlevelcode != dag_run.conf.Mgmt_Code }}''',
            yes_task="update_text_value_managementcode_52",
            no_task="if_request_scheduledweeklyhours_present_53",
        )

        update_text_value_managementcode_52 = rail.RepliconServiceOperator(
            task_id='update_text_value_managementcode_52',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.managementlevelcode_udfuri }}",
                "value": "{{ dag_run.conf.Mgmt_Code }}"
            }
        )

        if_request_scheduledweeklyhours_present_53 = rail.IfOperator(
            task_id='if_request_scheduledweeklyhours_present_53',
            test='''{{ dag_run.conf.scheduledweeklyhours | is_truthy  and result('invoke_custom_ruby_code_33').scheduledweeklyhours != dag_run.conf.scheduledweeklyhours }}''',
            yes_task="update_text_value_schedulehours_55",
            no_task="if_request_payrollid_present_56",
        )

        update_text_value_schedulehours_55 = rail.RepliconServiceOperator(
            task_id='update_text_value_schedulehours_55',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.scheduledweeklyhours_udfuri }}",
                "value": "{{ dag_run.conf.scheduledweeklyhours }}"
            }
        )

        if_request_payrollid_present_56 = rail.IfOperator(
            task_id='if_request_payrollid_present_56',
            test='''{{ dag_run.conf.payrollid | is_truthy  and result('invoke_custom_ruby_code_33').payrollid != dag_run.conf.payrollid }}''',
            yes_task="update_text_value_payrollid_58",
            no_task="if_request_manager_optionuri_present_59",
        )

        update_text_value_payrollid_58 = rail.RepliconServiceOperator(
            task_id='update_text_value_payrollid_58',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.payrollid_udfuri }}",
                "value": "{{ dag_run.conf.payrollid }}"
            }
        )

        if_request_manager_optionuri_present_59 = rail.IfOperator(
            task_id='if_request_manager_optionuri_present_59',
            test='''{{ dag_run.conf.manager_optionuri | is_truthy  and result('invoke_custom_ruby_code_33').manager != dag_run.conf.manager }}''',
            yes_task="update_dropdown_value_manager_60",
            no_task="if_request_workspace_optionuri_present_61",
        )

        update_dropdown_value_manager_60 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_manager_60',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.manager_udfuri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.manager_optionuri }}"
            }
        )

        if_request_workspace_optionuri_present_61 = rail.IfOperator(
            task_id='if_request_workspace_optionuri_present_61',
            test='''{{ dag_run.conf.workspace_optionuri | is_truthy  and result('invoke_custom_ruby_code_33').workspace != dag_run.conf.Work_Space }}''',
            yes_task="update_dropdown_value_workspace_62",
            no_task="if_request_costcenter_optionuri_present_63",
        )

        update_dropdown_value_workspace_62 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_workspace_62',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.workspace_udfuri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.workspace_optionuri }}"
            }
        )

        if_request_costcenter_optionuri_present_63 = rail.IfOperator(
            task_id='if_request_costcenter_optionuri_present_63',
            test='''{{ dag_run.conf.costcenter_optionuri | is_truthy  and result('invoke_custom_ruby_code_33').costcenter != dag_run.conf.Cost_Center }}''',
            yes_task="update_dropdown_value_costcenter_64",
            no_task="if_request_department_present_67",
        )

        update_dropdown_value_costcenter_64 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_costcenter_64',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.costcenter_udfuri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.costcenter_optionuri }}"
            }
        )

        if_request_department_present_67 = rail.IfOperator(
            task_id='if_request_department_present_67',
            test='''{{ dag_run.conf.Department | is_truthy  and result('invoke_custom_ruby_code_33').department != dag_run.conf.Department }}''',
            yes_task="update_dropdown_value_department_68",
            no_task="if_request_profitcenter_optionuri_present_69",
        )

        update_dropdown_value_department_68 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_department_68',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.department_udfuri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.department_optionuri }}"
            }
        )

        if_request_profitcenter_optionuri_present_69 = rail.IfOperator(
            task_id='if_request_profitcenter_optionuri_present_69',
            test='''{{ dag_run.conf.profitcenter_optionuri | is_truthy  and result('invoke_custom_ruby_code_33').profitcenter != dag_run.conf.Profit_Center }}''',
            yes_task="update_dropdown_value_profitcenter_70",
            no_task="if_request_company_present_71",
        )

        update_dropdown_value_profitcenter_70 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_profitcenter_70',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.profitcenter_udfuri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.profitcenter_optionuri }}"
            }
        )

        if_request_company_present_71 = rail.IfOperator(
            task_id='if_request_company_present_71',
            test='''{{ dag_run.conf.Company | is_truthy  and result('invoke_custom_ruby_code_33').company != dag_run.conf.Company }}''',
            yes_task="update_dropdown_value_profitcenter_72",
            no_task="if_request_mgmt_level_present_73",
        )

        update_dropdown_value_profitcenter_72 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_profitcenter_72',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.company_udfuri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.company_optionuri }}"
            }
        )

        if_request_mgmt_level_present_73 = rail.IfOperator(
            task_id='if_request_mgmt_level_present_73',
            test='''{{ dag_run.conf.Mgmt_Level | is_truthy  and result('invoke_custom_ruby_code_33').managementlevel != dag_run.conf.Mgmt_Level }}''',
            yes_task="update_dropdown_value_mgmtlevel_74",
            no_task="if_request_home_state_present_75",
        )

        update_dropdown_value_mgmtlevel_74 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_mgmtlevel_74',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.managementlevel_udfuri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.managementlevel_optionuri }}"
            }
        )

        if_request_home_state_present_75 = rail.IfOperator(
            task_id='if_request_home_state_present_75',
            test='''{{ dag_run.conf.Home_State | is_truthy  and result('invoke_custom_ruby_code_33').employeeresidence != dag_run.conf.Home_State }}''',
            yes_task="update_dropdown_value_employeeresidence_76",
            no_task="if_request_ceo_present_77",
        )

        update_dropdown_value_employeeresidence_76 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_employeeresidence_76',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.employeeresidence_udfuri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.employeeresidence_optionuri }}"
            }
        )

        if_request_ceo_present_77 = rail.IfOperator(
            task_id='if_request_ceo_present_77',
            test='''{{ dag_run.conf.CEO | is_truthy  and result('invoke_custom_ruby_code_33').ceo != dag_run.conf.CEO }}''',
            yes_task="update_dropdown_value_c_e_o_78",
            no_task="if_request_ceo_1_present_79",
        )

        update_dropdown_value_c_e_o_78 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_c_e_o_78',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.ceo_udfuri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.ceo_optionuri }}"
            }
        )

        if_request_ceo_1_present_79 = rail.IfOperator(
            task_id='if_request_ceo_1_present_79',
            test='''{{ dag_run.conf.CEO_1 | is_truthy  and result('invoke_custom_ruby_code_33').ceo1 != dag_run.conf.CEO_1 }}''',
            yes_task="update_dropdown_value_c_e_o1_80",
            no_task="if_request_ceo_2_present_81",
        )

        update_dropdown_value_c_e_o1_80 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_c_e_o1_80',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.ceo1_udfuri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.ceo1_optionuri }}"
            }
        )

        if_request_ceo_2_present_81 = rail.IfOperator(
            task_id='if_request_ceo_2_present_81',
            test='''{{ dag_run.conf.CEO_2 | is_truthy  and result('invoke_custom_ruby_code_33').ceo2 != dag_run.conf.CEO_2 }}''',
            yes_task="update_dropdown_value_c_e_o2_82",
            no_task="if_request_ceo_3_present_83",
        )

        update_dropdown_value_c_e_o2_82 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_c_e_o2_82',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.ceo2_udfuri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.ceo2_optionuri }}"
            }
        )

        if_request_ceo_3_present_83 = rail.IfOperator(
            task_id='if_request_ceo_3_present_83',
            test='''{{ dag_run.conf.CEO_3 | is_truthy  and result('invoke_custom_ruby_code_33').ceo3 != dag_run.conf.CEO_3 }}''',
            yes_task="update_dropdown_value_c_e_o3_84",
            no_task="if_request_ceo_4_present_85",
        )

        update_dropdown_value_c_e_o3_84 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_c_e_o3_84',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.ceo3_udfuri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.ceo3_optionuri }}"
            }
        )

        if_request_ceo_4_present_85 = rail.IfOperator(
            task_id='if_request_ceo_4_present_85',
            test='''{{ dag_run.conf.CEO_4 | is_truthy  and result('invoke_custom_ruby_code_33').ceo4 != dag_run.conf.CEO_4 }}''',
            yes_task="update_dropdown_value_c_e_o4_86",
            no_task="if_request_ceo_5_present_87",
        )

        update_dropdown_value_c_e_o4_86 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_c_e_o4_86',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.ceo4_udfuri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.ceo4_optionuri }}"
            }
        )

        if_request_ceo_5_present_87 = rail.IfOperator(
            task_id='if_request_ceo_5_present_87',
            test='''{{ dag_run.conf.CEO_5 | is_truthy  and result('invoke_custom_ruby_code_33').ceo5 != dag_run.conf.CEO_5 }}''',
            yes_task="update_dropdown_value_c_e_o5_88",
            no_task="if_request_ceo_6_present_89",
        )

        update_dropdown_value_c_e_o5_88 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_c_e_o5_88',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.ceo5_udfuri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.ceo5_optionuri }}"
            }
        )

        if_request_ceo_6_present_89 = rail.IfOperator(
            task_id='if_request_ceo_6_present_89',
            test='''{{ dag_run.conf.CEO_6 | is_truthy  and result('invoke_custom_ruby_code_33').ceo6 != dag_run.conf.CEO_6 }}''',
            yes_task="update_dropdown_value_c_e_o6_90",
            no_task="if_request_group_head_present_91",
        )

        update_dropdown_value_c_e_o6_90 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_c_e_o6_90',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.ceo6_udfuri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.ceo6_optionuri }}"
            }
        )

        if_request_group_head_present_91 = rail.IfOperator(
            task_id='if_request_group_head_present_91',
            test='''{{ dag_run.conf.Group_Head | is_truthy  and result('invoke_custom_ruby_code_33').groupleader != dag_run.conf.Group_Head }}''',
            yes_task="update_dropdown_value_grouphead_92",
            no_task="if_request_business_leader_present_93",
        )

        update_dropdown_value_grouphead_92 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_grouphead_92',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.groupleader_udfuri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.groupleader_optionuri }}"
            }
        )

        if_request_business_leader_present_93 = rail.IfOperator(
            task_id='if_request_business_leader_present_93',
            test='''{{ dag_run.conf.Business_Leader | is_truthy  and result('invoke_custom_ruby_code_33').businesleader != dag_run.conf.Business_Leader }}''',
            yes_task="update_dropdown_value_c_e_o6_94",
            no_task="if_request_contingent_worker_type_present_95",
        )

        update_dropdown_value_c_e_o6_94 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_c_e_o6_94',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.businesleader_udfuri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.businesleader_optionuri }}"
            }
        )

        if_request_contingent_worker_type_present_95 = rail.IfOperator(
            task_id='if_request_contingent_worker_type_present_95',
            test='''{{ dag_run.conf.Contingent_Worker_Type | is_truthy  and result('invoke_custom_ruby_code_33').contingentworkertype != dag_run.conf.Contingent_Worker_Type }}''',
            yes_task="update_dropdown_value_c_e_o6_96",
            no_task="if_request_worker_status_present_97",
        )

        update_dropdown_value_c_e_o6_96 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_c_e_o6_96',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.contingentworkertype_udfuri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.contingentworkertype_optionuri }}"
            }
        )

        if_request_worker_status_present_97 = rail.IfOperator(
            task_id='if_request_worker_status_present_97',
            test='''{{ dag_run.conf.Worker_Status | is_truthy  and result('invoke_custom_ruby_code_33').workerstatus != dag_run.conf.Worker_Status }}''',
            yes_task="update_dropdown_valueworkerstatus_98",
            no_task="if_request_country_present_99",
        )

        update_dropdown_valueworkerstatus_98 = rail.RepliconServiceOperator(
            task_id='update_dropdown_valueworkerstatus_98',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.workerstatus_udfuri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.workerstatus_optionuri }}"
            }
        )

        if_request_country_present_99 = rail.IfOperator(
            task_id='if_request_country_present_99',
            test='''{{ dag_run.conf.Country | is_truthy  and result('invoke_custom_ruby_code_33').country != dag_run.conf.Country }}''',
            yes_task="update_dropdown_valuecountry_100",
            no_task="if_request_firstdayofleave_present_101",
        )

        update_dropdown_valuecountry_100 = rail.RepliconServiceOperator(
            task_id='update_dropdown_valuecountry_100',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.country_udfuri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.country_optionuri }}"
            }
        )

        if_request_firstdayofleave_present_101 = rail.IfOperator(
            task_id='if_request_firstdayofleave_present_101',
            test='''{{ dag_run.conf.FirstDayofLeave | is_truthy }}''',
            yes_task="date_split_firstdayofleave_102",
            no_task="if_request_actuallastdayofleave_present_105",
        )

        date_split_firstdayofleave_102 = rail.PythonOperator(
            task_id='date_split_firstdayofleave_102',
            python_callable=lambda: get_replicon_date(
                rail.get_dag_run_conf()['FirstDayofLeave'])
        )

        if_t_firstdayofleave_changed_103 = rail.IfOperator(
            task_id='if_t_firstdayofleave_changed_103',
            test=lambda: datetime(**rail.result('date_split_firstdayofleave_102')) != ((datetime(**rail.result(
                'invoke_custom_ruby_code_33')['firstdayofleave'])) if rail.result('invoke_custom_ruby_code_33')['firstdayofleave'] else null),
            yes_task="update_date_value_firstdayofleave_104",
            no_task="if_request_actuallastdayofleave_present_105",
        )

        update_date_value_firstdayofleave_104 = rail.RepliconServiceOperator(
            task_id='update_date_value_firstdayofleave_104',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.firstdayofleave_udfuri }}",
                "value":  {
                    "year": "{{ result('date_split_firstdayofleave_102').year }}",
                    "month": "{{ result('date_split_firstdayofleave_102').month }}",
                    "day": "{{ result('date_split_firstdayofleave_102').day }}",
                }
            }
        )

        if_request_actuallastdayofleave_present_105 = rail.IfOperator(
            task_id='if_request_actuallastdayofleave_present_105',
            test='''{{ dag_run.conf.ActualLastDayofLeave | is_truthy }}''',
            yes_task="date_split_actuallastdayofleave_106",
            no_task="if_request_sup_org_present_121",
        )

        date_split_actuallastdayofleave_106 = rail.PythonOperator(
            task_id='date_split_actuallastdayofleave_106',
            python_callable=lambda: get_replicon_date(
                rail.get_dag_run_conf()['ActualLastDayofLeave'])
        )

        if_actual_day_of_leave_changed_107 = rail.IfOperator(
            task_id='if_actual_day_of_leave_changed_107',
            test=lambda: datetime(**rail.result('date_split_actuallastdayofleave_106')) != ((datetime(**rail.result(
                'invoke_custom_ruby_code_33')['lastdayofleave'])) if rail.result('invoke_custom_ruby_code_33')['lastdayofleave'] else null),
            yes_task="update_date_valuelastdayofleave_108",
            no_task="if_request_sup_org_present_121",
        )

        update_date_valuelastdayofleave_108 = rail.RepliconServiceOperator(
            task_id='update_date_valuelastdayofleave_108',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.lastdayofleave_udfuri }}",
                "value":  {
                    "year": "{{ result('date_split_actuallastdayofleave_106').year }}",
                    "month": "{{ result('date_split_actuallastdayofleave_106').month }}",
                    "day": "{{ result('date_split_actuallastdayofleave_106').day }}",
                }
            }
        )

        if_request_sup_org_present_121 = rail.IfOperator(
            task_id='if_request_sup_org_present_121',
            test='''{{ dag_run.conf.Sup_Org | is_truthy  and dag_run.conf.Sup_Org != (result('get_effective_user_group_membership_5').departments[0].department.department.displayText if result('get_effective_user_group_membership_5').departments else None)}}''',
            yes_task="if_request_sup_org_code_blank_122",
            no_task="if_request_employee_type_present_126",
        )

        if_request_sup_org_code_blank_122 = rail.IfOperator(
            task_id='if_request_sup_org_code_blank_122',
            test='''{{ dag_run.conf.Sup_Org_Code | is_falsy }}''',
            yes_task="insert_to_list_123",
            no_task="update_department_group_125",
        )

        insert_to_list_123 = rail.SetVariableOperator(
            task_id='insert_to_list_123',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": '''Sup org "{{dag_run.conf.Sup_Org}}" not available in Replicon'''
            }
        )

        update_department_group_125 = rail.RepliconServiceOperator(
            task_id='update_department_group_125',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "departmentGroupScheduleToApply": {
                        "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementDepartmentGroupSchedule": [],
                        "updateDepartmentGroupScheduleOverDateRange": {
                            "replacementDepartmentGroupScheduleEntries": [
                                {
                                    "departmentGroup": {
                                        "uri": "{{ dag_run.conf.Sup_Org_Code }}",
                                        "parent": null,
                                        "name": null,
                                        "parameterCorrelationId": null
                                    },
                                    "effectiveDate": {
                                        "year": "{{ result('date_split_today_7').year }}",
                                        "month": "{{ result('date_split_today_7').month }}",
                                        "day": "{{ result('date_split_today_7').day }}",
                                    }
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_employee_type_present_126 = rail.IfOperator(
            task_id='if_request_employee_type_present_126',
            test='''{{ dag_run.conf.Employee_Type | is_truthy  and dag_run.conf.Employee_Type != (result('get_effective_user_group_membership_5').employeeTypes[0].employeeType.employeeType.displayText if result('get_effective_user_group_membership_5').employeeTypes  else None)  }}''',
            yes_task="date_split_employeetypeeffectivedate_127",
            no_task="if_request_job_profile_present_132",
        )

        date_split_employeetypeeffectivedate_127 = rail.PythonOperator(
            task_id='date_split_employeetypeeffectivedate_127',
            python_callable=lambda: get_replicon_date(
                rail.get_dag_run_conf()['Employee_Type_Eff_Date'])
        )

        if_request_employeetypeuri_blank_128 = rail.IfOperator(
            task_id='if_request_employeetypeuri_blank_128',
            test='''{{ dag_run.conf.employeetypeuri | is_falsy }}''',
            yes_task="insert_to_list_129",
            no_task="update_employeetype_group_131",
        )

        insert_to_list_129 = rail.SetVariableOperator(
            task_id='insert_to_list_129',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": 'Employee type "{{dag_run.conf.Employee_Type}}" not available in Replicon'
            }
        )

        update_employeetype_group_131 = rail.RepliconServiceOperator(
            task_id='update_employeetype_group_131',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "employeeTypeGroupScheduleToApply": {
                        "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementEmployeeTypeGroupSchedule": [],
                        "updateEmployeeTypeGroupScheduleOverDateRange": {
                            "replacementEmployeeTypeGroupScheduleEntries": [
                                {
                                    "employeeTypeGroup": {
                                        "uri": "{{ dag_run.conf.employeetypeuri }}",
                                        "parent": null,
                                        "name": null,
                                        "parameterCorrelationId": null
                                    },
                                    "effectiveDate": {
                                        "year": "{{ result('date_split_employeetypeeffectivedate_127').year }}",
                                        "month": "{{ result('date_split_employeetypeeffectivedate_127').month }}",
                                        "day": "{{ result('date_split_employeetypeeffectivedate_127').day }}",
                                    }
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_job_profile_present_132 = rail.IfOperator(
            task_id='if_request_job_profile_present_132',
            test='''{{ dag_run.conf.Job_Profile | is_truthy  and dag_run.conf.Job_Profile != (result('get_effective_user_group_membership_5').serviceCenters[0].serviceCenter.serviceCenter.displayText if result('get_effective_user_group_membership_5').serviceCenters else None) }}''',
            yes_task="if_request_job_profile_code_blank_133",
            no_task="if_request_location_present_138",
        )

        if_request_job_profile_code_blank_133 = rail.IfOperator(
            task_id='if_request_job_profile_code_blank_133',
            test='''{{ dag_run.conf.Job_Profile_Code | is_falsy }}''',
            yes_task="insert_to_list_134",
            no_task="date_split_jobprofileeffectivedate_136",
        )

        insert_to_list_134 = rail.SetVariableOperator(
            task_id='insert_to_list_134',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": 'Company "{{dag_run.conf.Company}}" not available in Replicon'
            }
        )

        date_split_jobprofileeffectivedate_136 = rail.PythonOperator(
            task_id='date_split_jobprofileeffectivedate_136',
            python_callable=lambda: get_replicon_date(
                rail.get_dag_run_conf()['Job_Profile_Eff_Date'])
        )

        update_servicecenter_137 = rail.RepliconServiceOperator(
            task_id='update_servicecenter_137',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "serviceCenterScheduleToApply": {
                        "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementServiceCenterSchedule": [],
                        "updateServiceCenterScheduleOverDateRange": {
                            "replacementServiceCenterScheduleEntries": [
                                {
                                    "serviceCenter": {
                                        "uri": "{{ dag_run.conf.Job_Profile_Code }}",
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": {
                                        "year": "{{ result('date_split_jobprofileeffectivedate_136').year }}",
                                        "month": "{{ result('date_split_jobprofileeffectivedate_136').month }}",
                                        "day": "{{ result('date_split_jobprofileeffectivedate_136').day }}",

                                    }
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_location_present_138 = rail.IfOperator(
            task_id='if_request_location_present_138',
            test='''{{ dag_run.conf.Location | is_truthy  and dag_run.conf.Location != (result('get_effective_user_group_membership_5').locations[0].location.location.displayText if result('get_effective_user_group_membership_5').locations else None) }}''',
            yes_task="if_request_location_code_blank_139",
            no_task="if_request_jobpositiontag_present_144",
        )

        if_request_location_code_blank_139 = rail.IfOperator(
            task_id='if_request_location_code_blank_139',
            test='''{{ dag_run.conf.Location_Code | is_falsy }}''',
            yes_task="insert_to_list_140",
            no_task="date_split_locationeffectivedate_142",
        )

        insert_to_list_140 = rail.SetVariableOperator(
            task_id='insert_to_list_140',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": 'Location "{{dag_run.conf.Location}}" not available in Replicon'
            }
        )

        date_split_locationeffectivedate_142 = rail.PythonOperator(
            task_id='date_split_locationeffectivedate_142',
            python_callable=lambda: get_replicon_date(rail.get_dag_run_conf()['Location_Eff_Date']) if rail.get_dag_run_conf()[
                'Location_Eff_Date'] else rail.result('date_split_today_7')
        )

        update_location_143 = rail.RepliconServiceOperator(
            task_id='update_location_143',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "locationScheduleToApply": {
                        "userLocationScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementLocationSchedule": [],
                        "updateLocationScheduleOverDateRange": {
                            "replacementLocationScheduleEntries": [
                                {
                                    "location": {
                                        "uri": "{{ dag_run.conf.Location_Code }}",
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": {
                                        "year": "{{ result('date_split_locationeffectivedate_142').year }}",
                                        "month": "{{ result('date_split_locationeffectivedate_142').month }}",
                                        "day": "{{ result('date_split_locationeffectivedate_142').day }}",

                                    }
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_jobpositiontag_present_144 = rail.IfOperator(
            task_id='if_request_jobpositiontag_present_144',
            test='''{{ dag_run.conf.JobPositionTag | is_truthy  and dag_run.conf.JobPositionTag != (result('get_effective_user_group_membership_5').divisions[0].division.division.displayText if result('get_effective_user_group_membership_5').divisions else None) }}''',
            yes_task="if_request_jobpositiontagcode_blank_145",
            no_task="if_request_flsa_present_150",
        )

        if_request_jobpositiontagcode_blank_145 = rail.IfOperator(
            task_id='if_request_jobpositiontagcode_blank_145',
            test='''{{ dag_run.conf.JobPositionTagCode | is_falsy }}''',
            yes_task="insert_to_list_146",
            no_task="date_split_jobpositiontageffectivedate_148",
        )

        insert_to_list_146 = rail.SetVariableOperator(
            task_id='insert_to_list_146',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": 'Job position tag "{{dag_run.conf.JobPositionTag}}" not available in Replicon'
            }
        )

        date_split_jobpositiontageffectivedate_148 = rail.PythonOperator(
            task_id='date_split_jobpositiontageffectivedate_148',
            python_callable=lambda: get_replicon_date(rail.get_dag_run_conf()['JobPositionTagEffDate']) if rail.get_dag_run_conf()[
                'JobPositionTagEffDate'] else rail.result('date_split_today_7')
        )

        update_division_149 = rail.RepliconServiceOperator(
            task_id='update_division_149',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "divisionScheduleToApply": {
                        "userDivisionScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementDivisionSchedule": [],
                        "updateDivisionScheduleOverDateRange": {
                            "replacementDivisionScheduleEntries": [
                                {
                                    "division": {
                                        "uri": "{{ dag_run.conf.JobPositionTagCode }}",
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": {
                                        "year": "{{ result('date_split_jobpositiontageffectivedate_148').year }}",
                                        "month": "{{ result('date_split_jobpositiontageffectivedate_148').month }}",
                                        "day": "{{ result('date_split_jobpositiontageffectivedate_148').day }}",

                                    }
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_flsa_present_150 = rail.IfOperator(
            task_id='if_request_flsa_present_150',
            test='''{{ dag_run.conf.FLSA | is_truthy  and dag_run.conf.FLSA != (result('get_effective_user_group_membership_5').costCenters[0].costCenter.costCenter.displayText if result('get_effective_user_group_membership_5').costCenters else None) }}''',
            yes_task="if_request_jobpositiontagcode_blank_151",
            no_task="if_request_timesheettemplate_present_156",
        )

        if_request_jobpositiontagcode_blank_151 = rail.IfOperator(
            task_id='if_request_jobpositiontagcode_blank_151',
            test='''{{ dag_run.conf.JobPositionTagCode | is_falsy }}''',
            yes_task="insert_to_list_152",
            no_task="date_split_f_l_s_aeffectivedate_154",
        )

        insert_to_list_152 = rail.SetVariableOperator(
            task_id='insert_to_list_152',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": 'FLSA "{{dag_run.conf.FLSA}}" not available in Replicon'
            }
        )

        date_split_f_l_s_aeffectivedate_154 = rail.PythonOperator(
            task_id='date_split_f_l_s_aeffectivedate_154',
            python_callable=lambda: get_replicon_date(rail.get_dag_run_conf()['FLSA_Eff_Date']) if rail.get_dag_run_conf()[
                'FLSA_Eff_Date'] else rail.result('date_split_today_7')
        )

        update_costcenter_155 = rail.RepliconServiceOperator(
            task_id='update_costcenter_155',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "costCenterScheduleToApply": {
                        "userCostCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementCostCenterSchedule": [],
                        "updateCostCenterScheduleOverDateRange": {
                            "replacementCostCenterScheduleEntries": [
                                {
                                    "costCenter": {
                                        "uri": null,
                                        "parentUri": null,
                                        "name": "{{ dag_run.conf.FLSA }}"
                                    },
                                    "effectiveDate": {
                                        "year": "{{ result('date_split_f_l_s_aeffectivedate_154').year }}",
                                        "month": "{{ result('date_split_f_l_s_aeffectivedate_154').month }}",
                                        "day": "{{ result('date_split_f_l_s_aeffectivedate_154').day }}",

                                    }
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_timesheettemplate_present_156 = rail.IfOperator(
            task_id='if_request_timesheettemplate_present_156',
            test='''{{ dag_run.conf.timesheettemplate | is_truthy  and dag_run.conf.timesheettemplate != (result('bulk_get_users3_4')[0].timesheetTemplate or {}).get('uri') }}''',
            yes_task="assign_policy_set_to_user_timesheettemplate_157",
            no_task="if_request_payrule_present_158",
        )

        assign_policy_set_to_user_timesheettemplate_157 = rail.RepliconServiceOperator(
            task_id='assign_policy_set_to_user_timesheettemplate_157',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ dag_run.conf.timesheettemplate }}"
            }
        )

        if_request_payrule_present_158 = rail.IfOperator(
            task_id='if_request_payrule_present_158',
            test='''{{ dag_run.conf.payrule | is_truthy }}''',
            yes_task="if_payrulescriptschedule_to_json_contains_urn_159",
            no_task="if_request_flsa_equals_to_exempt_167",
        )

        if_payrulescriptschedule_to_json_contains_urn_159 = rail.IfOperator(
            task_id='if_payrulescriptschedule_to_json_contains_urn_159',
            test='''{{ result('bulk_get_users3_4')[0].payRuleScriptSchedule | to_json | matches('urn') }}''',
            yes_task="parse_json_payrule_schedule_160",
            no_task="if_schedulepolicies_uri_blank_162",
        )

        parse_json_payrule_schedule_160 = rail.PythonOperator(
            task_id='parse_json_payrule_schedule_160',
            python_callable=lambda: rail.result('bulk_get_users3_4')[
                0]['payRuleScriptSchedule']
        )

        def get_current_schedule(data):
            if not data and len(data) == 0:
                return None
            current_schedule = list(filter(lambda x: datetime(
                **x['effectiveDate']) if x['effectiveDate'] else datetime.min <= datetime(**rail.result('date_split_today_7')), data))
            return None if len(current_schedule) == 0 else current_schedule[-1]

        get_current_schedule_161 = rail.PythonOperator(
            task_id='get_current_schedule_161',
            python_callable=lambda: get_current_schedule(
                rail.result('parse_json_payrule_schedule_160'))
        )

        if_schedulepolicies_uri_blank_162 = rail.IfOperator(
            task_id='if_schedulepolicies_uri_blank_162',
            test='''{{ result('get_current_schedule_161') | is_falsy  or (result('get_current_schedule_161') or {}).get('uri') != dag_run.conf.payrule }}''',
            yes_task="if_request_payrule_blank_163",
            no_task="if_request_flsa_equals_to_exempt_167",
        )

        if_request_payrule_blank_163 = rail.IfOperator(
            task_id='if_request_payrule_blank_163',
            test='''{{ dag_run.conf.payrule | is_falsy }}''',
            yes_task="insert_to_list_164",
            no_task="updatepayrule_166",
        )

        insert_to_list_164 = rail.SetVariableOperator(
            task_id='insert_to_list_164',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": 'Payrule "{{dag_run.conf.payrule}}" not available in Replicon'
            }
        )

        updatepayrule_166 = rail.RepliconServiceOperator(
            task_id='updatepayrule_166',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "payRulesScheduleModifications": {
                        "scheduleEntries": [
                            {
                                "payRuleScript": {
                                    "uri": null,
                                    "name": "{{ dag_run.conf.payrule }}"
                                },
                                "effectiveDate": {
                                    "year": "{{ result('date_split_today_7').year }}",
                                    "month": "{{ result('date_split_today_7').month }}",
                                    "day": "{{ result('date_split_today_7').day }}",
                                }
                            }
                        ]
                    },
                    "projectRolesToApply": null
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_flsa_equals_to_exempt_167 = rail.IfOperator(
            task_id='if_request_flsa_equals_to_exempt_167',
            test='''{{ dag_run.conf.FLSA == 'Exempt'  and (result('get_effective_user_group_membership_5').costCenters[0].costCenter.costCenter.displayText if result('get_effective_user_group_membership_5').costCenters else None) == 'Non-Exempt' }}''',
            yes_task="updatepayrule_no_overtime_rule_168",
            no_task="if_request_timezoneuri_present_169",
        )

        updatepayrule_no_overtime_rule_168 = rail.RepliconServiceOperator(
            task_id='updatepayrule_no_overtime_rule_168',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "payRulesScheduleModifications": {
                        "scheduleEntries": [
                            {
                                "payRuleScript": {
                                    "uri": null,
                                    "name": "No Overtime Rule"
                                },
                                "effectiveDate": {
                                    "year": "{{ result('date_split_today_7').year }}",
                                    "month": "{{ result('date_split_today_7').month }}",
                                    "day": "{{ result('date_split_today_7').day }}",

                                }
                            }
                        ]
                    },
                    "projectRolesToApply": null
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_timezoneuri_present_169 = rail.IfOperator(
            task_id='if_request_timezoneuri_present_169',
            test='''{{ dag_run.conf.timezoneuri | is_truthy }}''',
            yes_task="if_timezone_changed_170",
            no_task="if_request_worker_status_present_172",
        )

        if_timezone_changed_170 = rail.IfOperator(
            task_id='if_timezone_changed_170',
            test='''{{ dag_run.conf.timezoneuri != (result('bulk_get_users3_4')[0].timeZone or {}).get('uri') }}''',
            yes_task="update_time_zone_for_user_171",
            no_task="if_request_worker_status_present_172",
        )

        update_time_zone_for_user_171 = rail.RepliconServiceOperator(
            task_id='update_time_zone_for_user_171',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "timeZoneUri": "{{ dag_run.conf.timezoneuri }}"
            }
        )

        if_request_worker_status_present_172 = rail.IfOperator(
            task_id='if_request_worker_status_present_172',
            test='''{{ dag_run.conf.Worker_Status | is_truthy  and dag_run.conf.Worker_Status != result('invoke_custom_ruby_code_33').workerstatus }}''',
            yes_task="if_request_worker_status_equals_to_active_173",
            no_task="get_all_permission_sets_177",
        )

        if_request_worker_status_equals_to_active_173 = rail.IfOperator(
            task_id='if_request_worker_status_equals_to_active_173',
            test='''{{ dag_run.conf.Worker_Status == 'Active' }}''',
            yes_task="put_user_notification_preferences_assigntimesheetandusernotifications_174",
            no_task="if_request_worker_status_equals_to_onleave_175",
        )

        put_user_notification_preferences_assigntimesheetandusernotifications_174 = rail.RepliconServiceOperator(
            task_id='put_user_notification_preferences_assigntimesheetandusernotifications_174',
            endpoint="/services/NotificationScriptAdministrationService1.svc/PutUserNotificationPreferences",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "preferences": {
                    "notificationDeliveryPreferences": [
                        {
                            "objectTypeUri": "urn:replicon:object-type:timesheet",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:user",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:always-deliver"
                        }
                    ],
                    "sharedDeliveryPreferenceOptionUris": [
                        "urn:replicon:user-shared-delivery-preference-option:always-deliver"
                    ]
                }
            }
        )

        if_request_worker_status_equals_to_onleave_175 = rail.IfOperator(
            task_id='if_request_worker_status_equals_to_onleave_175',
            test='''{{ dag_run.conf.Worker_Status == 'On Leave'  or dag_run.conf.Worker_Status == 'Terminated' }}''',
            yes_task="put_user_notification_preferences_removenotifications_176",
            no_task="get_all_permission_sets_177",
        )

        put_user_notification_preferences_removenotifications_176 = rail.RepliconServiceOperator(
            task_id='put_user_notification_preferences_removenotifications_176',
            endpoint="/services/NotificationScriptAdministrationService1.svc/PutUserNotificationPreferences",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "preferences": {
                    "notificationDeliveryPreferences": [
                        {
                            "objectTypeUri": "urn:replicon:object-type:project",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:user",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:timesheet",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:time-entry-revision-group",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:pay-rule-script",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:time-off",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:holiday",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        }
                    ],
                    "sharedDeliveryPreferenceOptionUris": [
                        "urn:replicon:user-shared-delivery-preference-option:always-deliver"
                    ]
                }
            }
        )

        get_all_permission_sets_177 = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets_177',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data=None
        )

        if_request_supervisor_present_178 = rail.IfOperator(
            task_id='if_request_supervisor_present_178',
            test='''{{ dag_run.conf.Supervisor | is_truthy  and dag_run.conf.Supervisor != 'N/A' }}''',
            yes_task="get_data_supervisor_180",
            no_task="bulk_get_users3_208",
        )

        get_data_supervisor_180 = rail.RepliconServiceOperator(
            task_id='get_data_supervisor_180',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
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
                            "text": "{{ dag_run.conf.Supervisor }}",
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
        )

        invoke_custom_ruby_code_181 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_181',
            python_callable=lambda: list(filter(lambda x: x['employeeid'] == rail.get_dag_run_conf()['Supervisor'], map(lambda x: {
                "name": x['cells'][0]['textValue'],
                "loginname": x['cells'][1]['textValue'],
                "uri": x['cells'][0]['uri'],
                "employeeid": x['cells'][2]['textValue']
            }, rail.result('get_data_supervisor_180')['rows'])))
        )

        if_first_uri_present_182 = rail.IfOperator(
            task_id='if_first_uri_present_182',
            test='''{{ result('invoke_custom_ruby_code_181') | is_truthy }}''',
            yes_task="if_split_lengthnil_greater_than_1_183",
            no_task="queue_supervisor_assignment",
        )

        if_split_lengthnil_greater_than_1_183 = rail.IfOperator(
            task_id='if_split_lengthnil_greater_than_1_183',
            test='''{{ result('invoke_custom_ruby_code_181') | length > 1 }}''',
            yes_task="insert_to_list_184",
            no_task="log_supervisorcheck_186",
        )

        insert_to_list_184 = rail.SetVariableOperator(
            task_id='insert_to_list_184',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Supervisor not updated as there are multiple users with the ID '{{ dag_run.conf.Supervisor }}' in Replicon."
            }
        )

        log_supervisorcheck_186 = rail.PythonOperator(
            task_id='log_supervisorcheck_186',
            python_callable=lambda:  rail.result(
                'invoke_custom_ruby_code_181')[0]['uri']
        )

        if_log_supervisorcheck_186_blank_187 = rail.IfOperator(
            task_id='if_log_supervisorcheck_186_blank_187',
            test='''{{ result('log_supervisorcheck_186') | is_falsy }}''',
            yes_task="queue_supervisor_assignment",
            no_task="bulk_get_users3_190",
        )

        queue_supervisor_assignment = rail.PythonOperator(
            task_id='queue_supervisor_assignment',
            python_callable=lambda: {
                "userloginname": rail.get_dag_run_conf()['User_Name'],
                "username": f"{rail.get_dag_run_conf()['firstname']} {rail.get_dag_run_conf()['lastname']}",
                "supervisorempid": rail.get_dag_run_conf()['Supervisor'],
                "employeeid": rail.get_dag_run_conf()['employeeid'],
                "useruri": rail.get_dag_run_conf()['useruri'],
                "action": "Update",
                "effectivedate": {
                    "day": datetime.utcnow().day, "month": datetime.utcnow().month, "year": datetime.utcnow().year
                }
            }
        )

        bulk_get_users3_190 = rail.RepliconServiceOperator(
            task_id='bulk_get_users3_190',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ result('log_supervisorcheck_186') }}",
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        if_request_supervisor_equals_to_dataworkato_servicereceive_requestrequestemployeeid_191 = rail.IfOperator(
            task_id='if_request_supervisor_equals_to_dataworkato_servicereceive_requestrequestemployeeid_191',
            test='''{{ dag_run.conf.Supervisor == dag_run.conf.employeeid }}''',
            yes_task="insert_to_list_192",
            no_task="get_supervisor_assignment_detailsforuser_194",
        )

        insert_to_list_192 = rail.SetVariableOperator(
            task_id='insert_to_list_192',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Supervisor not updated  - Supervisor's employee id is same as User's employee id"
            }
        )

        get_supervisor_assignment_detailsforuser_194 = rail.RepliconServiceOperator(
            task_id='get_supervisor_assignment_detailsforuser_194',
            endpoint="/services/UserService1.svc/GetSupervisorAssignmentDetails",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "asOfDate": {
                    "year": "{{ result('date_split_today_7').year }}",
                    "month": "{{ result('date_split_today_7').month }}",
                    "day": "{{ result('date_split_today_7').day }}",
                }
            }
        )

        if_user_loginname_blank_195 = rail.IfOperator(
            task_id='if_user_loginname_blank_195',
            test='''{{ result('get_supervisor_assignment_detailsforuser_194') | is_falsy or result('get_supervisor_assignment_detailsforuser_194').supervisor.user.loginName | is_falsy  or result('get_supervisor_assignment_detailsforuser_194').supervisor.user.loginName | lower != result('bulk_get_users3_190')[0].securityConfiguration.loginName  | lower }}''',
            yes_task="get_timesheet_periods_for_user_196",
            no_task="bulk_get_users3_208",
        )

        get_timesheet_periods_for_user_196 = rail.RepliconServiceOperator(
            task_id='get_timesheet_periods_for_user_196',
            endpoint="/services/TimesheetPeriodService1.svc/GetTimesheetPeriodsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('date_split_today_7').year }}",
                        "month": "{{ result('date_split_today_7').month }}",
                        "day": "{{ result('date_split_today_7').day }}",
                    },
                    "endDate":  {
                        "year": "{{ result('date_split_today_7').year }}",
                        "month": "{{ result('date_split_today_7').month }}",
                        "day": "{{ result('date_split_today_7').day }}",
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        date_split_supervisoreffectivedate_197 = rail.PythonOperator(
            task_id='date_split_supervisoreffectivedate_197',
            python_callable=lambda: rail.result('get_timesheet_periods_for_user_196')[0]['dateRange']['startDate'] if rail.result(
                'get_timesheet_periods_for_user_196') and rail.result('get_timesheet_periods_for_user_196')[0]['dateRange']['startDate'] else rail.result('date_split_today_7')
        )

        if_userdetails_isenabled_is_true_198 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_true_198',
            test='''{{ result('bulk_get_users3_190')[0].userDetails.isEnabled | is_truthy }}''',
            yes_task="log_checkifmanagerpermissionisassigned_199",
            no_task="queue_supervisor_assignment",
        )

        log_checkifmanagerpermissionisassigned_199 = rail.PythonOperator(
            task_id='log_checkifmanagerpermissionisassigned_199',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'bulk_get_users3_190')[0]['permissionSets'], 'displayText', "Supervisor")
        )

        if_log_checkifmanagerpermissionisassigned_199_blank_200 = rail.IfOperator(
            task_id='if_log_checkifmanagerpermissionisassigned_199_blank_200',
            test='''{{ result('log_checkifmanagerpermissionisassigned_199') | is_falsy and result('bulk_get_users3_190')[0].userDetails.customFieldValues |find_first_by_attr_and_get_attr("customField.displayText","Manager","text") | matches('Yes')  }}''',
            yes_task="assign_permission_set_to_user_manager_201",
            no_task="update_supervisor_assignment_schedule_over_date_range_203",
        )

        assign_permission_set_to_user_manager_201 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_manager_201',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('bulk_get_users3_190')[0].userDetails.uri }}",
                "permissionSetUri": "{{ dag_run.conf.supervisorpermissionuri }}"
            }
        )

        assign_permission_set_to_user_team_manager_202 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_team_manager_202',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('bulk_get_users3_190')[0].userDetails.uri }}",
                "permissionSetUri": "{{ dag_run.conf.teammanagerpermissionuri }}"
            }
        )

        update_supervisor_assignment_schedule_over_date_range_203 = rail.RepliconServiceOperator(
            task_id='update_supervisor_assignment_schedule_over_date_range_203',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('bulk_get_users3_190')[0].userDetails.uri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('date_split_supervisoreffectivedate_197').year }}",
                        "month": "{{ result('date_split_supervisoreffectivedate_197').month }}",
                        "day": "{{ result('date_split_supervisoreffectivedate_197').day }}",

                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        bulk_get_users3_208 = rail.RepliconServiceOperator(
            task_id='bulk_get_users3_208',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ dag_run.conf.useruri }}",
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        if_request_manager_equals_to_yes_209 = rail.IfOperator(
            task_id='if_request_manager_equals_to_yes_209',
            test='''{{ dag_run.conf.manager == 'Yes'  and result('bulk_get_users3_208')[0].userDetails.customFieldValues|find_first_by_attr_and_get_attr("customField.displayText","Manager","text") | matches('Yes')  }}''',
            yes_task="assign_supervsior_permission_set_to_user_manager_210",
            no_task="if_request_manager_equals_to_no_212",
        )

        assign_supervsior_permission_set_to_user_manager_210 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_manager_210',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "permissionSetUri": "{{ dag_run.conf.supervisorpermissionuri }}"
            }
        )

        assign_supervsior_permission_set_to_user_manager_211 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_manager_211',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "permissionSetUri": "{{ dag_run.conf.teammanagerpermissionuri }}"
            }
        )

        if_request_manager_equals_to_no_212 = rail.IfOperator(
            task_id='if_request_manager_equals_to_no_212',
            test='''{{ dag_run.conf.manager == 'No'  and result('bulk_get_users3_208')[0].userDetails.customFieldValues|find_first_by_attr_and_get_attr("customField.displayText","Manager","text") | matches('No') }}''',
            yes_task="assign_supervsior_permission_set_to_user_manager_213",
            no_task="if_request_supervisor_blank_215",
        )

        assign_supervsior_permission_set_to_user_manager_213 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_manager_213',
            endpoint="/services/PermissionSetService1.svc/RemovePermissionSetAssignmentFromUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "permissionSetUri": "{{ dag_run.conf.supervisorpermissionuri }}"
            }
        )

        assign_supervsior_permission_set_to_user_manager_214 = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user_manager_214',
            endpoint="/services/PermissionSetService1.svc/RemovePermissionSetAssignmentFromUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "permissionSetUri": "{{ dag_run.conf.teammanagerpermissionuri }}"
            }
        )

        if_request_supervisor_blank_215 = rail.IfOperator(
            task_id='if_request_supervisor_blank_215',
            test='''{{ dag_run.conf.Supervisor | is_falsy }}''',
            yes_task="insert_to_list_216",
            no_task="if_request_substitute_user_present_217",
        )

        insert_to_list_216 = rail.SetVariableOperator(
            task_id='insert_to_list_216',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Supervisor ID was not present in the Input file."
            }
        )

        if_request_substitute_user_present_217 = rail.IfOperator(
            task_id='if_request_substitute_user_present_217',
            test='''{{ dag_run.conf.Substitute_User | is_truthy  and dag_run.conf.Subs_User_StartDate | is_truthy  and dag_run.conf.Sub_User_EndDate | is_truthy  and dag_run.conf.Substitute_User != 'N/A' }}''',
            yes_task="get_datasubstituteuser_219",
            no_task="log_all_exceptions",
        )

        get_datasubstituteuser_219 = rail.RepliconServiceOperator(
            task_id='get_datasubstituteuser_219',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
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
                            "text": "{{ dag_run.conf.Substitute_User }}",
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
        )

        invoke_custom_ruby_code_220 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_220',
            python_callable=lambda: list(filter(lambda x: x['employeeid'] == rail.get_dag_run_conf()['Substitute_User'], map(lambda x: {
                "name": x['cells'][0]['textValue'],
                "loginname": x['cells'][1]['textValue'],
                "uri": x['cells'][0]['uri'],
                "employeeid": x['cells'][2]['textValue'],
            }, rail.result('get_datasubstituteuser_219')['rows'])))
        )

        if_first_uri_present_221 = rail.IfOperator(
            task_id='if_first_uri_present_221',
            test='''{{ result('invoke_custom_ruby_code_220')| is_truthy }}''',
            yes_task="if_split_lengthnil_greater_than_1_222",
            no_task="log_all_exceptions",
        )

        if_split_lengthnil_greater_than_1_222 = rail.IfOperator(
            task_id='if_split_lengthnil_greater_than_1_222',
            test='''{{ result('invoke_custom_ruby_code_220')| length > 1 }}''',
            yes_task="insert_to_list_223",
            no_task="log_substituteusercheck_225",
        )

        insert_to_list_223 = rail.SetVariableOperator(
            task_id='insert_to_list_223',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Subsititute user not assigned as there are multiple users with the ID '{{ dag_run.conf.Substitute_User }}' in Replicon."
            }
        )

        log_substituteusercheck_225 = rail.PythonOperator(
            task_id='log_substituteusercheck_225',
            python_callable=lambda:  rail.result(
                'invoke_custom_ruby_code_220')[0]['uri']
        )

        if_log_substituteusercheck_225_present_226 = rail.IfOperator(
            task_id='if_log_substituteusercheck_225_present_226',
            test='''{{ result('log_substituteusercheck_225') | is_truthy }}''',
            yes_task="impersonate_and_create_interactive_session_227",
            no_task="insert_to_list_246",
        )

        def map_impersonate_and_create_interactive_session(res):
            data = res.json()['d']
            auth_token = list(
                filter(lambda x: x['name'] == 'AUTHTOKEN', data['sessionCookies']))[0]['value']
            tenant = list(
                filter(lambda x: x['name'] == 'TENANT', data['sessionCookies']))[0]['value']
            return {'cookie': f'AUTHTOKEN={auth_token};TENANT={tenant}', 'Path': '/'}

        impersonate_and_create_interactive_session_227 = rail.RepliconServiceOperator(
            task_id='impersonate_and_create_interactive_session_227',
            endpoint="/services/UserImpersonationService1.svc/AdministrativeImpersonationAndCreateInteractiveSession",
            data={
                "impersonatedUserUri": "{{ dag_run.conf.useruri }}"
            },
            response_filter=map_impersonate_and_create_interactive_session
        )

        log_authtoken_228 = rail.PythonOperator(
            task_id='log_authtoken_228',
            python_callable=lambda:  rail.result(
                'impersonate_and_create_interactive_session_227')
        )

        get_all_substitute_user_assignments_for_user_230 = rail.RepliconServiceOperator(
            task_id='get_all_substitute_user_assignments_for_user_230',
            endpoint="/services/SubstituteUserAssignmentService1.svc/GetAllSubstituteUserAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            headers=lambda: rail.result('log_authtoken_228'),
        )

        log_currentlyassignedsubstituteuser_231 = rail.PythonOperator(
            task_id='log_currentlyassignedsubstituteuser_231',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_all_substitute_user_assignments_for_user_230'), 'user.uri', rail.result(
                'log_substituteusercheck_225'), 'user.uri') if rail.result('get_all_substitute_user_assignments_for_user_230') else null
        )

        if_log_currentlyassignedsubstituteuser_231_blank_232 = rail.IfOperator(
            task_id='if_log_currentlyassignedsubstituteuser_231_blank_232',
            test='''{{ result('log_currentlyassignedsubstituteuser_231') | is_falsy }}''',
            yes_task="create_new_draft_233",
            no_task="if_log_currentlyassignedsubstituteuser_231_equals_to_replicon_241",
        )

        create_new_draft_233 = rail.RepliconServiceOperator(
            task_id='create_new_draft_233',
            endpoint="/services/SubstituteUserAssignmentService1.svc/CreateNewDraft",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            headers=lambda: rail.result('log_authtoken_228'),
        )

        update_substitute_user_234 = rail.RepliconServiceOperator(
            task_id='update_substitute_user_234',
            endpoint="/services/SubstituteUserAssignmentService1.svc/UpdateSubstituteUser",
            data={
                "substituteUserAssignmentUri": "{{ result('create_new_draft_233') }}",
                "substituteUser": {
                    "uri": "{{ result('log_substituteusercheck_225') }}",
                    "loginName": null,
                    "parameterCorrelationId": "{{ dag_run_ecid() }}"
                }
            },
            headers=lambda: rail.result('log_authtoken_228'),
        )

        date_split_subsititutestartdate_235 = rail.PythonOperator(
            task_id='date_split_subsititutestartdate_235',
            python_callable=lambda: get_replicon_date(
                rail.get_dag_run_conf()['Subs_User_StartDate'])
        )

        date_split_subsitituteenddate_236 = rail.PythonOperator(
            task_id='date_split_subsitituteenddate_236',
            python_callable=lambda: get_replicon_date(
                rail.get_dag_run_conf()['Sub_User_EndDate'])
        )

        update_date_range_237 = rail.RepliconServiceOperator(
            task_id='update_date_range_237',
            endpoint="/services/SubstituteUserAssignmentService1.svc/UpdateDateRange",
            data={
                "substituteUserAssignmentUri": "{{ result('create_new_draft_233') }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('date_split_subsititutestartdate_235').year }}",
                        "month": "{{ result('date_split_subsititutestartdate_235').month }}",
                        "day": "{{ result('date_split_subsititutestartdate_235').day }}",

                    },
                    "endDate": {
                        "year": "{{ result('date_split_subsitituteenddate_236').year }}",
                        "month": "{{ result('date_split_subsitituteenddate_236').month }}",
                        "day": "{{ result('date_split_subsitituteenddate_236').day }}",

                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            },
            headers=lambda: rail.result('log_authtoken_228'),
        )

        put_access_levels_238 = rail.RepliconServiceOperator(
            task_id='put_access_levels_238',
            endpoint="/services/SubstituteUserAssignmentService1.svc/PutAccessLevels",
            data={
                "substituteUserAssignmentUri": "{{ result('create_new_draft_233') }}",
                "accessLevelUris": [
                    "urn:replicon:substitute-user-access-level:full-access"
                ]
            },
            headers=lambda: rail.result('log_authtoken_228'),
        )

        update_is_notification_forwarding_enabled_239 = rail.RepliconServiceOperator(
            task_id='update_is_notification_forwarding_enabled_239',
            endpoint="/services/SubstituteUserAssignmentService1.svc/UpdateIsNotificationForwardingEnabled",
            data={
                "substituteUserAssignmentUri": "{{ result('create_new_draft_233') }}",
                "isEnabled": "1"
            },
            headers=lambda: rail.result('log_authtoken_228'),
        )

        publish_draft_240 = rail.RepliconServiceOperator(
            task_id='publish_draft_240',
            endpoint="/services/SubstituteUserAssignmentService1.svc/PublishDraft",
            data={
                "draftUri": "{{ result('create_new_draft_233') }}"
            },
            headers=lambda: rail.result('log_authtoken_228'),
        )

        if_log_currentlyassignedsubstituteuser_231_equals_to_replicon_241 = rail.IfOperator(
            task_id='if_log_currentlyassignedsubstituteuser_231_equals_to_replicon_241',
            test=lambda: rail.result('log_currentlyassignedsubstituteuser_231') == rail.result('log_substituteusercheck_225') and
            datetime(**get_replicon_date(rail.get_dag_run_conf()['Sub_User_EndDate'])) !=
            datetime(**rail.find_first_by_attr_and_get_attr(rail.result('get_all_substitute_user_assignments_for_user_230'),
                                                            'user.uri', rail.result('log_substituteusercheck_225'), 'dateRange.endDate'))
            if rail.find_first_by_attr_and_get_attr(rail.result('get_all_substitute_user_assignments_for_user_230'),
                                                    'user.uri', rail.result('log_substituteusercheck_225'), 'dateRange.endDate') else null,
            yes_task="create_edit_draft_242",
            no_task="log_all_exceptions",
        )

        create_edit_draft_242 = rail.RepliconServiceOperator(
            task_id='create_edit_draft_242',
            endpoint="/services/SubstituteUserAssignmentService1.svc/CreateEditDraft",
            data={
                "substituteUserAssignmentUri": "{{ result('get_all_substitute_user_assignments_for_user_230') | find_first_by_attr_and_get_attr('user.uri', result('log_substituteusercheck_225'),'uri') }}"
            },
            headers=lambda: rail.result('log_authtoken_228'),
        )

        update_date_range_243 = rail.RepliconServiceOperator(
            task_id='update_date_range_243',
            endpoint="/services/SubstituteUserAssignmentService1.svc/UpdateDateRange",
            data={
                "substituteUserAssignmentUri": "{{ result('create_edit_draft_242') }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ parse_date(dag_run.conf.Subs_User_StartDate,'%m/%d/%Y').year }}",
                        "month": "{{ parse_date(dag_run.conf.Subs_User_StartDate,'%m/%d/%Y').month }}",
                        "day": "{{ parse_date(dag_run.conf.Subs_User_StartDate,'%m/%d/%Y').day }}",

                    },
                    "endDate": {
                        "year": "{{ parse_date(dag_run.conf.Sub_User_EndDate,'%m/%d/%Y').year }}",
                        "month": "{{ parse_date(dag_run.conf.Sub_User_EndDate,'%m/%d/%Y').month }}",
                        "day": "{{ parse_date(dag_run.conf.Sub_User_EndDate,'%m/%d/%Y').day }}",

                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            },
            headers=lambda: rail.result('log_authtoken_228'),
        )

        publish_draft_244 = rail.RepliconServiceOperator(
            task_id='publish_draft_244',
            endpoint="/services/SubstituteUserAssignmentService1.svc/PublishDraft",
            data={
                "draftUri": "{{ result('create_edit_draft_242') }}"
            },
            headers=lambda: rail.result('log_authtoken_228'),
        )

        insert_to_list_246 = rail.SetVariableOperator(
            task_id='insert_to_list_246',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "value": "Subsititute user not assigned as the required user with the ID '{{ dag_run.conf.Substitute_User }}' is not available in Replicon."
            }
        )

        log_all_exceptions = rail.PythonOperator(
            task_id='log_all_exceptions',
            python_callable=lambda:  "|".join(list(map(lambda x: x['value'], rail.get_dag_run_var(rail.result('declare_list_2')[
                                              'name'])))) if rail.get_dag_run_var(rail.result('declare_list_2')['name']) else null
        )

        horizonmedia_user_import_logs_add_entry_248 = rail.WriteLogOperator(
            task_id='horizonmedia_user_import_logs_add_entry_248',
            log="{{ result('create_log') }}",
            message="na",
            severity='''{{ "Exception" if result('log_all_exceptions') | is_truthy  else  "Success" }}''',
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "action": "Update",
                "status": '''{{ "Exception" if result('log_all_exceptions') | is_truthy  else  "Success" }}''',
                "details": '''{{ "User Updated partially - " + result('log_all_exceptions') if result('log_all_exceptions') | is_truthy else "User Updated successfully"}}''',
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{ result('create_log') }}",
            message="na",
            severity="Error",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "action": "Update",
                "status": "Error",
                "details": '{{ get_error_message() }}',
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> declare_list_2
        declare_list_2 >> create_log >> bulk_get_users3_4 >> get_effective_user_group_membership_5 >> declare_variable_6 >> date_split_today_7 >> date_split_startdate_11 >> is_holidaycalendar_uri_present >> rail.Label(
            "Yes") >> if_holiday_calendar_mismatch >> rail.Label("Yes") >> update_holiday_calendar >> if_userdetails_isenabled_is_not_true_12
        if_holiday_calendar_mismatch >> rail.Label("No") >> if_userdetails_isenabled_is_not_true_12
        is_holidaycalendar_uri_present >> rail.Label(
            "No") >> if_userdetails_isenabled_is_not_true_12
        if_userdetails_isenabled_is_not_true_12 >> rail.Label(
            'Yes') >> enable_login_enablelogin_13 >> update_employment_date_rangeforenddate_updatestartdatewithoutenddate_14 >> log_displayname_16
        if_userdetails_isenabled_is_not_true_12 >> rail.Label(
            'No') >> log_displayname_16 >> if_userdetails_customdisplayname_not_equals_to_datalogger331589ebmessage_17
        if_userdetails_customdisplayname_not_equals_to_datalogger331589ebmessage_17 >> rail.Label(
            'Yes') >> update_timesheet_approval_path >> if_request_start_date_present_19
        if_userdetails_customdisplayname_not_equals_to_datalogger331589ebmessage_17 >> rail.Label(
            'No') >> if_request_start_date_present_19
        if_request_start_date_present_19 >> rail.Label(
            'Yes') >> if_emp_date_changed
        if_emp_date_changed >> rail.Label(
            'Yes') >> update_employment_date_rangeforenddate_updatestartdatewithoutenddate_21 >> if_first_presence_not_equals_to_urnrepliconuserauthenticationtypesso_23
        if_emp_date_changed >> rail.Label(
            'No') >> if_first_presence_not_equals_to_urnrepliconuserauthenticationtypesso_23
        if_request_start_date_present_19 >> rail.Label(
            'No') >> if_first_presence_not_equals_to_urnrepliconuserauthenticationtypesso_23
        if_first_presence_not_equals_to_urnrepliconuserauthenticationtypesso_23 >> rail.Label(
            'Yes') >> set_s_s_o_authentication_for_user_24 >> if_request_user_name_present_25
        if_first_presence_not_equals_to_urnrepliconuserauthenticationtypesso_23 >> rail.Label(
            'No') >> if_request_user_name_present_25
        if_request_user_name_present_25 >> rail.Label(
            'Yes') >> set_s_s_o_authentication_for_user_updateloginname_26 >> if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_27
        if_request_user_name_present_25 >> rail.Label(
            'No') >> if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_27
        if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_27 >> rail.Label(
            'Yes') >> update_first_name_28 >> if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_29
        if_request_firstname_present_dataworkato_servicereceive_requestrequestemployeefirstnamedowncase_27 >> rail.Label(
            'No') >> if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_29
        if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_29 >> rail.Label(
            'Yes') >> update_last_name_30 >> if_request_work_email_present_31
        if_request_lastname_present_dataworkato_servicereceive_requestrequestlastnamedowncase_29 >> rail.Label(
            'No') >> if_request_work_email_present_31
        if_request_work_email_present_31 >> rail.Label(
            'Yes') >> update_email_32 >> invoke_custom_ruby_code_33
        if_request_work_email_present_31 >> rail.Label(
            'No') >> invoke_custom_ruby_code_33 >> declare_variable_34 >> if_request_position_id_present_35
        if_request_position_id_present_35 >> rail.Label(
            'Yes') >> update_text_value_position_i_d_36 >> if_request_businesstitle_present_37
        if_request_position_id_present_35 >> rail.Label(
            'No') >> if_request_businesstitle_present_37
        if_request_businesstitle_present_37 >> rail.Label(
            'Yes') >> update_text_value_businesstitle_38 >> if_request_cost_center_code_present_39
        if_request_businesstitle_present_37 >> rail.Label(
            'No') >> if_request_cost_center_code_present_39
        if_request_cost_center_code_present_39 >> rail.Label(
            'Yes') >> update_text_value_costcentercode_40 >> if_request_department_code_present_41
        if_request_cost_center_code_present_39 >> rail.Label(
            'No') >> if_request_department_code_present_41
        if_request_department_code_present_41 >> rail.Label(
            'Yes') >> update_text_value_departmentcode_42 >> if_request_profit_center_code_present_43
        if_request_department_code_present_41 >> rail.Label(
            'No') >> if_request_profit_center_code_present_43
        if_request_profit_center_code_present_43 >> rail.Label(
            'Yes') >> update_text_value_profitcentercode_44 >> if_request_company_code_present_45
        if_request_profit_center_code_present_43 >> rail.Label(
            'No') >> if_request_company_code_present_45
        if_request_company_code_present_45 >> rail.Label(
            'Yes') >> update_text_value_companycode_46 >> if_request_pref_name_present_47
        if_request_company_code_present_45 >> rail.Label(
            'No') >> if_request_pref_name_present_47
        if_request_pref_name_present_47 >> rail.Label(
            'Yes') >> update_text_value_preferredfullname_48 >> if_request_legal_name_present_49
        if_request_pref_name_present_47 >> rail.Label(
            'No') >> if_request_legal_name_present_49
        if_request_legal_name_present_49 >> rail.Label(
            'Yes') >> update_text_value_companycode_50 >> if_request_mgmt_code_present_51
        if_request_legal_name_present_49 >> rail.Label(
            'No') >> if_request_mgmt_code_present_51
        if_request_mgmt_code_present_51 >> rail.Label(
            'Yes') >> update_text_value_managementcode_52 >> if_request_scheduledweeklyhours_present_53
        if_request_mgmt_code_present_51 >> rail.Label(
            'No') >> if_request_scheduledweeklyhours_present_53
        if_request_scheduledweeklyhours_present_53 >> rail.Label(
            'Yes') >> update_text_value_schedulehours_55 >> if_request_payrollid_present_56
        if_request_scheduledweeklyhours_present_53 >> rail.Label(
            'No') >> if_request_payrollid_present_56
        if_request_payrollid_present_56 >> rail.Label(
            'Yes') >> update_text_value_payrollid_58 >> if_request_manager_optionuri_present_59
        if_request_payrollid_present_56 >> rail.Label(
            'No') >> if_request_manager_optionuri_present_59
        if_request_manager_optionuri_present_59 >> rail.Label(
            'Yes') >> update_dropdown_value_manager_60 >> if_request_workspace_optionuri_present_61
        if_request_manager_optionuri_present_59 >> rail.Label(
            'No') >> if_request_workspace_optionuri_present_61
        if_request_workspace_optionuri_present_61 >> rail.Label(
            'Yes') >> update_dropdown_value_workspace_62 >> if_request_costcenter_optionuri_present_63
        if_request_workspace_optionuri_present_61 >> rail.Label(
            'No') >> if_request_costcenter_optionuri_present_63
        if_request_costcenter_optionuri_present_63 >> rail.Label(
            'Yes') >> update_dropdown_value_costcenter_64 >> if_request_department_present_67
        if_request_costcenter_optionuri_present_63 >> rail.Label(
            'No') >> if_request_department_present_67
        if_request_department_present_67 >> rail.Label(
            'Yes') >> update_dropdown_value_department_68 >> if_request_profitcenter_optionuri_present_69
        if_request_department_present_67 >> rail.Label(
            'No') >> if_request_profitcenter_optionuri_present_69
        if_request_profitcenter_optionuri_present_69 >> rail.Label(
            'Yes') >> update_dropdown_value_profitcenter_70 >> if_request_company_present_71
        if_request_profitcenter_optionuri_present_69 >> rail.Label(
            'No') >> if_request_company_present_71
        if_request_company_present_71 >> rail.Label(
            'Yes') >> update_dropdown_value_profitcenter_72 >> if_request_mgmt_level_present_73
        if_request_company_present_71 >> rail.Label(
            'No') >> if_request_mgmt_level_present_73
        if_request_mgmt_level_present_73 >> rail.Label(
            'Yes') >> update_dropdown_value_mgmtlevel_74 >> if_request_home_state_present_75
        if_request_mgmt_level_present_73 >> rail.Label(
            'No') >> if_request_home_state_present_75
        if_request_home_state_present_75 >> rail.Label(
            'Yes') >> update_dropdown_value_employeeresidence_76 >> if_request_ceo_present_77
        if_request_home_state_present_75 >> rail.Label(
            'No') >> if_request_ceo_present_77
        if_request_ceo_present_77 >> rail.Label(
            'Yes') >> update_dropdown_value_c_e_o_78 >> if_request_ceo_1_present_79
        if_request_ceo_present_77 >> rail.Label(
            'No') >> if_request_ceo_1_present_79
        if_request_ceo_1_present_79 >> rail.Label(
            'Yes') >> update_dropdown_value_c_e_o1_80 >> if_request_ceo_2_present_81
        if_request_ceo_1_present_79 >> rail.Label(
            'No') >> if_request_ceo_2_present_81
        if_request_ceo_2_present_81 >> rail.Label(
            'Yes') >> update_dropdown_value_c_e_o2_82 >> if_request_ceo_3_present_83
        if_request_ceo_2_present_81 >> rail.Label(
            'No') >> if_request_ceo_3_present_83
        if_request_ceo_3_present_83 >> rail.Label(
            'Yes') >> update_dropdown_value_c_e_o3_84 >> if_request_ceo_4_present_85
        if_request_ceo_3_present_83 >> rail.Label(
            'No') >> if_request_ceo_4_present_85
        if_request_ceo_4_present_85 >> rail.Label(
            'Yes') >> update_dropdown_value_c_e_o4_86 >> if_request_ceo_5_present_87
        if_request_ceo_4_present_85 >> rail.Label(
            'No') >> if_request_ceo_5_present_87
        if_request_ceo_5_present_87 >> rail.Label(
            'Yes') >> update_dropdown_value_c_e_o5_88 >> if_request_ceo_6_present_89
        if_request_ceo_5_present_87 >> rail.Label(
            'No') >> if_request_ceo_6_present_89
        if_request_ceo_6_present_89 >> rail.Label(
            'Yes') >> update_dropdown_value_c_e_o6_90 >> if_request_group_head_present_91
        if_request_ceo_6_present_89 >> rail.Label(
            'No') >> if_request_group_head_present_91
        if_request_group_head_present_91 >> rail.Label(
            'Yes') >> update_dropdown_value_grouphead_92 >> if_request_business_leader_present_93
        if_request_group_head_present_91 >> rail.Label(
            'No') >> if_request_business_leader_present_93
        if_request_business_leader_present_93 >> rail.Label(
            'Yes') >> update_dropdown_value_c_e_o6_94 >> if_request_contingent_worker_type_present_95
        if_request_business_leader_present_93 >> rail.Label(
            'No') >> if_request_contingent_worker_type_present_95
        if_request_contingent_worker_type_present_95 >> rail.Label(
            'Yes') >> update_dropdown_value_c_e_o6_96 >> if_request_worker_status_present_97
        if_request_contingent_worker_type_present_95 >> rail.Label(
            'No') >> if_request_worker_status_present_97
        if_request_worker_status_present_97 >> rail.Label(
            'Yes') >> update_dropdown_valueworkerstatus_98 >> if_request_country_present_99
        if_request_worker_status_present_97 >> rail.Label(
            'No') >> if_request_country_present_99
        if_request_country_present_99 >> rail.Label(
            'Yes') >> update_dropdown_valuecountry_100 >> if_request_firstdayofleave_present_101
        if_request_country_present_99 >> rail.Label(
            'No') >> if_request_firstdayofleave_present_101
        if_request_firstdayofleave_present_101 >> rail.Label(
            'Yes') >> date_split_firstdayofleave_102 >> if_t_firstdayofleave_changed_103
        if_t_firstdayofleave_changed_103 >> rail.Label(
            'Yes') >> update_date_value_firstdayofleave_104 >> if_request_actuallastdayofleave_present_105
        if_t_firstdayofleave_changed_103 >> rail.Label(
            'No') >> if_request_actuallastdayofleave_present_105
        if_request_firstdayofleave_present_101 >> rail.Label(
            'No') >> if_request_actuallastdayofleave_present_105
        if_request_actuallastdayofleave_present_105 >> rail.Label(
            'Yes') >> date_split_actuallastdayofleave_106 >> if_actual_day_of_leave_changed_107
        if_actual_day_of_leave_changed_107 >> rail.Label(
            'Yes') >> update_date_valuelastdayofleave_108 >> if_request_sup_org_present_121
        if_actual_day_of_leave_changed_107 >> rail.Label(
            'No') >> if_request_sup_org_present_121
        if_request_actuallastdayofleave_present_105 >> rail.Label(
            'No') >> if_request_sup_org_present_121
        if_request_sup_org_present_121 >> rail.Label(
            'Yes') >> if_request_sup_org_code_blank_122
        if_request_sup_org_code_blank_122 >> rail.Label(
            'Yes') >> insert_to_list_123 >> if_request_employee_type_present_126
        if_request_sup_org_code_blank_122 >> rail.Label(
            'No') >> update_department_group_125 >> if_request_employee_type_present_126
        if_request_sup_org_present_121 >> rail.Label(
            'No') >> if_request_employee_type_present_126
        if_request_employee_type_present_126 >> rail.Label(
            'Yes') >> date_split_employeetypeeffectivedate_127 >> if_request_employeetypeuri_blank_128
        if_request_employeetypeuri_blank_128 >> rail.Label(
            'Yes') >> insert_to_list_129 >> if_request_job_profile_present_132
        if_request_employeetypeuri_blank_128 >> rail.Label(
            'No') >> update_employeetype_group_131 >> if_request_job_profile_present_132
        if_request_employee_type_present_126 >> rail.Label(
            'No') >> if_request_job_profile_present_132
        if_request_job_profile_present_132 >> rail.Label(
            'Yes') >> if_request_job_profile_code_blank_133
        if_request_job_profile_code_blank_133 >> rail.Label(
            'Yes') >> insert_to_list_134 >> if_request_location_present_138
        if_request_job_profile_code_blank_133 >> rail.Label(
            'No') >> date_split_jobprofileeffectivedate_136 >> update_servicecenter_137 >> if_request_location_present_138
        if_request_job_profile_present_132 >> rail.Label(
            'No') >> if_request_location_present_138
        if_request_location_present_138 >> rail.Label(
            'Yes') >> if_request_location_code_blank_139
        if_request_location_code_blank_139 >> rail.Label(
            'Yes') >> insert_to_list_140 >> if_request_jobpositiontag_present_144
        if_request_location_code_blank_139 >> rail.Label(
            'No') >> date_split_locationeffectivedate_142 >> update_location_143 >> if_request_jobpositiontag_present_144
        if_request_location_present_138 >> rail.Label(
            'No') >> if_request_jobpositiontag_present_144
        if_request_jobpositiontag_present_144 >> rail.Label(
            'Yes') >> if_request_jobpositiontagcode_blank_145
        if_request_jobpositiontagcode_blank_145 >> rail.Label(
            'Yes') >> insert_to_list_146 >> if_request_flsa_present_150
        if_request_jobpositiontagcode_blank_145 >> rail.Label(
            'No') >> date_split_jobpositiontageffectivedate_148 >> update_division_149 >> if_request_flsa_present_150
        if_request_jobpositiontag_present_144 >> rail.Label(
            'No') >> if_request_flsa_present_150
        if_request_flsa_present_150 >> rail.Label(
            'Yes') >> if_request_jobpositiontagcode_blank_151
        if_request_jobpositiontagcode_blank_151 >> rail.Label(
            'Yes') >> insert_to_list_152 >> if_request_timesheettemplate_present_156
        if_request_jobpositiontagcode_blank_151 >> rail.Label(
            'No') >> date_split_f_l_s_aeffectivedate_154 >> update_costcenter_155 >> if_request_timesheettemplate_present_156
        if_request_flsa_present_150 >> rail.Label(
            'No') >> if_request_timesheettemplate_present_156
        if_request_timesheettemplate_present_156 >> rail.Label(
            'Yes') >> assign_policy_set_to_user_timesheettemplate_157 >> if_request_payrule_present_158
        if_request_timesheettemplate_present_156 >> rail.Label(
            'No') >> if_request_payrule_present_158
        if_request_payrule_present_158 >> rail.Label(
            'Yes') >> if_payrulescriptschedule_to_json_contains_urn_159
        if_payrulescriptschedule_to_json_contains_urn_159 >> rail.Label(
            'Yes') >> parse_json_payrule_schedule_160 >> get_current_schedule_161 >> if_schedulepolicies_uri_blank_162
        if_payrulescriptschedule_to_json_contains_urn_159 >> rail.Label(
            'No') >> if_schedulepolicies_uri_blank_162
        if_schedulepolicies_uri_blank_162 >> rail.Label(
            'Yes') >> if_request_payrule_blank_163
        if_request_payrule_blank_163 >> rail.Label(
            'Yes') >> insert_to_list_164 >> if_request_flsa_equals_to_exempt_167
        if_request_payrule_blank_163 >> rail.Label(
            'No') >> updatepayrule_166 >> if_request_flsa_equals_to_exempt_167
        if_schedulepolicies_uri_blank_162 >> rail.Label(
            'No') >> if_request_flsa_equals_to_exempt_167
        if_request_payrule_present_158 >> rail.Label(
            'No') >> if_request_flsa_equals_to_exempt_167
        if_request_flsa_equals_to_exempt_167 >> rail.Label(
            'Yes') >> updatepayrule_no_overtime_rule_168 >> if_request_timezoneuri_present_169
        if_request_flsa_equals_to_exempt_167 >> rail.Label(
            'No') >> if_request_timezoneuri_present_169
        if_request_timezoneuri_present_169 >> rail.Label(
            'Yes') >> if_timezone_changed_170
        if_timezone_changed_170 >> rail.Label(
            'Yes') >> update_time_zone_for_user_171 >> if_request_worker_status_present_172
        if_timezone_changed_170 >> rail.Label(
            'No') >> if_request_worker_status_present_172
        if_request_timezoneuri_present_169 >> rail.Label(
            'No') >> if_request_worker_status_present_172
        if_request_worker_status_present_172 >> rail.Label(
            'Yes') >> if_request_worker_status_equals_to_active_173
        if_request_worker_status_equals_to_active_173 >> rail.Label(
            'Yes') >> put_user_notification_preferences_assigntimesheetandusernotifications_174 >> get_all_permission_sets_177
        if_request_worker_status_equals_to_active_173 >> rail.Label(
            'No') >> if_request_worker_status_equals_to_onleave_175
        if_request_worker_status_equals_to_onleave_175 >> rail.Label(
            'Yes') >> put_user_notification_preferences_removenotifications_176 >> get_all_permission_sets_177
        if_request_worker_status_equals_to_onleave_175 >> rail.Label(
            'No') >> get_all_permission_sets_177
        if_request_worker_status_present_172 >> rail.Label(
            'No') >> get_all_permission_sets_177 >> if_request_supervisor_present_178
        if_request_supervisor_present_178 >> rail.Label(
            'Yes') >> get_data_supervisor_180 >> invoke_custom_ruby_code_181 >> if_first_uri_present_182
        if_first_uri_present_182 >> rail.Label(
            'Yes') >> if_split_lengthnil_greater_than_1_183
        if_split_lengthnil_greater_than_1_183 >> rail.Label(
            'Yes') >> insert_to_list_184 >> bulk_get_users3_208
        if_log_supervisorcheck_186_blank_187 >> rail.Label(
            'Yes') >> queue_supervisor_assignment >> bulk_get_users3_208
        if_request_supervisor_equals_to_dataworkato_servicereceive_requestrequestemployeeid_191 >> rail.Label(
            'Yes') >> insert_to_list_192 >> bulk_get_users3_208
        if_user_loginname_blank_195 >> rail.Label(
            'Yes') >> get_timesheet_periods_for_user_196 >> date_split_supervisoreffectivedate_197 >> if_userdetails_isenabled_is_true_198
        if_userdetails_isenabled_is_true_198 >> rail.Label(
            'Yes') >> log_checkifmanagerpermissionisassigned_199 >> if_log_checkifmanagerpermissionisassigned_199_blank_200
        if_log_checkifmanagerpermissionisassigned_199_blank_200 >> rail.Label(
            'Yes') >> assign_permission_set_to_user_manager_201 >> assign_permission_set_to_user_team_manager_202 >> update_supervisor_assignment_schedule_over_date_range_203
        if_log_checkifmanagerpermissionisassigned_199_blank_200 >> rail.Label(
            'No') >> update_supervisor_assignment_schedule_over_date_range_203 >> bulk_get_users3_208
        if_userdetails_isenabled_is_true_198 >> rail.Label(
            'No') >> queue_supervisor_assignment >> bulk_get_users3_208
        if_user_loginname_blank_195 >> rail.Label('No') >> bulk_get_users3_208
        if_request_supervisor_equals_to_dataworkato_servicereceive_requestrequestemployeeid_191 >> rail.Label(
            'No') >> get_supervisor_assignment_detailsforuser_194 >> if_user_loginname_blank_195
        if_log_supervisorcheck_186_blank_187 >> rail.Label(
            'No') >> bulk_get_users3_190 >> if_request_supervisor_equals_to_dataworkato_servicereceive_requestrequestemployeeid_191
        if_split_lengthnil_greater_than_1_183 >> rail.Label(
            'No') >> log_supervisorcheck_186 >> if_log_supervisorcheck_186_blank_187
        if_first_uri_present_182 >> rail.Label(
            'No') >> queue_supervisor_assignment >> bulk_get_users3_208
        if_request_supervisor_present_178 >> rail.Label(
            'No') >> bulk_get_users3_208 >> if_request_manager_equals_to_yes_209
        if_request_manager_equals_to_yes_209 >> rail.Label(
            'Yes') >> assign_supervsior_permission_set_to_user_manager_210 >> assign_supervsior_permission_set_to_user_manager_211 >> if_request_manager_equals_to_no_212
        if_request_manager_equals_to_yes_209 >> rail.Label(
            'No') >> if_request_manager_equals_to_no_212
        if_request_manager_equals_to_no_212 >> rail.Label(
            'Yes') >> assign_supervsior_permission_set_to_user_manager_213 >> assign_supervsior_permission_set_to_user_manager_214 >> if_request_supervisor_blank_215
        if_request_manager_equals_to_no_212 >> rail.Label(
            'No') >> if_request_supervisor_blank_215
        if_request_supervisor_blank_215 >> rail.Label(
            'Yes') >> insert_to_list_216 >> if_request_substitute_user_present_217
        if_request_supervisor_blank_215 >> rail.Label(
            'No') >> if_request_substitute_user_present_217
        if_request_substitute_user_present_217 >> rail.Label(
            'Yes') >> get_datasubstituteuser_219 >> invoke_custom_ruby_code_220 >> if_first_uri_present_221
        if_first_uri_present_221 >> rail.Label(
            'Yes') >> if_split_lengthnil_greater_than_1_222
        if_split_lengthnil_greater_than_1_222 >> rail.Label(
            'Yes') >> insert_to_list_223 >> log_all_exceptions
        if_log_substituteusercheck_225_present_226 >> rail.Label(
            'Yes') >> impersonate_and_create_interactive_session_227 >> log_authtoken_228 >> get_all_substitute_user_assignments_for_user_230 >> log_currentlyassignedsubstituteuser_231 >> if_log_currentlyassignedsubstituteuser_231_blank_232
        if_log_currentlyassignedsubstituteuser_231_blank_232 >> rail.Label(
            'Yes') >> create_new_draft_233 >> update_substitute_user_234 >> date_split_subsititutestartdate_235 >> date_split_subsitituteenddate_236 >> update_date_range_237 >> put_access_levels_238 >> update_is_notification_forwarding_enabled_239 >> publish_draft_240 >> if_log_currentlyassignedsubstituteuser_231_equals_to_replicon_241
        if_log_currentlyassignedsubstituteuser_231_blank_232 >> rail.Label(
            'No') >> if_log_currentlyassignedsubstituteuser_231_equals_to_replicon_241
        if_log_currentlyassignedsubstituteuser_231_equals_to_replicon_241 >> rail.Label(
            'Yes') >> create_edit_draft_242 >> update_date_range_243 >> publish_draft_244 >> log_all_exceptions
        if_log_currentlyassignedsubstituteuser_231_equals_to_replicon_241 >> rail.Label(
            'No') >> log_all_exceptions
        if_log_substituteusercheck_225_present_226 >> rail.Label(
            'No') >> insert_to_list_246 >> log_all_exceptions
        if_split_lengthnil_greater_than_1_222 >> rail.Label(
            'No') >> log_substituteusercheck_225 >> if_log_substituteusercheck_225_present_226
        if_first_uri_present_221 >> rail.Label(
            'No') >> log_all_exceptions
        if_request_substitute_user_present_217 >> rail.Label(
            'No') >> log_all_exceptions
        log_all_exceptions >> horizonmedia_user_import_logs_add_entry_248 >> finish
        finish >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
