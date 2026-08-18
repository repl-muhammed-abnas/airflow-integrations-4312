from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail
from ascendmaterials.user_import.mappers.ascend_master_mapper_file_mapper import ascend_master_mapper_file
from ascendmaterials.user_import.utils import python_callable, request_payload, response_filter

null = None


def _process_schedule_entries(entries):
    """Convert 'skip' effectiveDate sentinels to None, cast date int-strings to int,
    and deduplicate by effective date (last entry per date wins)."""
    if not entries:
        return entries
    processed = {}
    for entry in entries:
        ed = entry.get('effectiveDate')
        if ed and ed.get('day') == 'skip':
            entry = {**entry, 'effectiveDate': None}
            key = 'null'
        elif ed:
            entry = {**entry, 'effectiveDate': {k: int(v) for k, v in ed.items()}}
            key = (entry['effectiveDate']['day'], entry['effectiveDate']['month'], entry['effectiveDate']['year'])
        else:
            key = 'null'
        processed[key] = entry
    return list(processed.values())


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.update_user_dag_id,
        description=f'Ascend_Child_Update User {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_seconday_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='declare_variable'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_variable',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_variable = rail.SetVariableOperator(
            task_id='declare_variable',
            append=False,
            name='locationandemployeetypebasedchange',
            value=None
        )

        bulk_get_users3 = rail.RepliconServiceOperator(
            task_id='bulk_get_users3',
            endpoint="/services/importservice1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": '{{ dag_run.conf["useruri"] }}',
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        if_userdetails_isenabled_is_true = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_true',
            test='''{{ result('bulk_get_users3')[0].userDetails.isEnabled | is_truthy  and dag_run.conf["enabled"] | is_truthy and dag_run.conf["enabled"].lower() == 'no' }}''',
            yes_task="log_start_datefrom_replicon",
            no_task="if_userdetails_isenabled_is_not_true_1",
        )

        log_start_datefrom_replicon = rail.PythonOperator(
            task_id='log_start_datefrom_replicon',
            python_callable=lambda:  str(rail.result('bulk_get_users3')[0]['userDetails']['employmentDateRange']['startDate']['month']) + "/" +
            str(rail.result('bulk_get_users3')[0]['userDetails']['employmentDateRange']['startDate']['day']) + "/" +
            str(rail.result('bulk_get_users3')[
                0]['userDetails']['employmentDateRange']['startDate']['year'])
        )

        trigger_disable_user = rail.TriggerDagRunOperator(
            task_id='trigger_disable_user',
            retries=0,
            trigger_dag_id=config.disable_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                    "parentjobid": dag_run.conf["parentjobid"],
                    "userloginname": dag_run.conf["loginname"],
                    "useruri": dag_run.conf["useruri"],
                    "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
                    "startdate": dag_run.conf["startdate"] if dag_run.conf["startdate"] else rail.result('log_start_datefrom_replicon'),
                    "firstname": dag_run.conf["employeefirstname"],
                    "lastname": dag_run.conf["employeelastname"],
                    "enddate": dag_run.conf["terminationdate"],
                    "ascend_user_import_logs_lookuptable": dag_run.conf["ascend_user_import_logs_lookuptable"]
            }
        )

        wait_live_ascend_disable_user = rail.WaitForDagRunsSensor(
            task_id='wait_live_ascend_disable_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_disable_user") }}'
        )

        stop_1 = rail.EmptyOperator(
            task_id='stop_1',

        )

        if_userdetails_isenabled_is_not_true_1 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_1',
            test='''{{ result('bulk_get_users3')[0].userDetails.isEnabled | is_falsy  and dag_run.conf["enabled"] | is_truthy and dag_run.conf["enabled"] == 'no' }}''',
            yes_task="log_entry_1",
            no_task="declare_rehire_variable",
        )

        log_entry_1 = rail.WriteLogOperator(
            task_id='log_entry_1',
            log='{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}',
            message="na",
            severity="Skipped",
            properties=lambda dag_run: {
                "userloginname": dag_run.conf.get('loginname', ''),
                "username": dag_run.conf.get('employeefirstname', '') + " " + dag_run.conf.get('employeelastname', ''),
                "action": "Update",
                "status": "Skipped",
                "details": "User is already disabled in Replicon"
            }
        )

        stop_2 = rail.EmptyOperator(
            task_id='stop_2',

        )

        declare_rehire_variable = rail.SetVariableOperator(
            task_id='declare_rehire_variable',
            append=False,
            name='rehire_log',
            value=None
        )

        if_userdetails_isenabled_is_not_true_2 = rail.IfOperator(
            task_id='if_userdetails_isenabled_is_not_true_2',
            test='''{{ result('bulk_get_users3')[0].userDetails.isEnabled | is_falsy  and dag_run.conf["enabled"].lower() == 'yes' }}''',
            yes_task="enable_login",
            no_task="if_employeefirstname_present",
        )

        enable_login = rail.RepliconServiceOperator(
            task_id='enable_login',
            endpoint="/services/SecurityService1.svc/EnableLogin",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}'
            }
        )

        update_employment_date_rangetoremoveenddate = rail.RepliconServiceOperator(
            task_id='update_employment_date_rangetoremoveenddate',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('bulk_get_users3')[0].userDetails.employmentDateRange.startDate.year }}",
                        "month": "{{ result('bulk_get_users3')[0].userDetails.employmentDateRange.startDate.month }}",
                        "day": "{{ result('bulk_get_users3')[0].userDetails.employmentDateRange.startDate.day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        log_rehire_log = rail.SetVariableOperator(
            task_id='log_rehire_log',
            append=False,
            name='rehire_log',
            value="User enabled"
        )

        if_employeefirstname_present = rail.IfOperator(
            task_id='if_employeefirstname_present',
            test='''{{ dag_run.conf["employeefirstname"] | is_truthy  and result('bulk_get_users3')[0].userDetails.firstName != dag_run.conf["employeefirstname"] }}''',
            yes_task="update_first_name",
            no_task="if_employeelastname_present",
        )

        update_first_name = rail.RepliconServiceOperator(
            task_id='update_first_name',
            endpoint="/services/UserService1.svc/UpdateFirstName",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "firstname": '{{ dag_run.conf["employeefirstname"] }}'
            }
        )

        if_employeelastname_present = rail.IfOperator(
            task_id='if_employeelastname_present',
            test='''{{ dag_run.conf["employeelastname"] | is_truthy  and dag_run.conf["employeelastname"] != result('bulk_get_users3')[0].userDetails.lastName }}''',
            yes_task="update_last_name",
            no_task="if_emailaddress_contains",
        )

        update_last_name = rail.RepliconServiceOperator(
            task_id='update_last_name',
            endpoint="/services/UserService1.svc/UpdateLastName",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "lastname": '{{ dag_run.conf["employeelastname"] }}'
            }
        )

        if_emailaddress_contains = rail.IfOperator(
            task_id='if_emailaddress_contains',
            test='''{{ dag_run.conf["emailaddress"] | matches('@')  and result('bulk_get_users3')[0].userDetails.emailAddress != dag_run.conf["emailaddress"] }}''',
            yes_task="update_email",
            no_task="if_employeeid_present",
        )

        update_email = rail.RepliconServiceOperator(
            task_id='update_email',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "email": '{{ dag_run.conf["emailaddress"] }}'
            }
        )

        if_employeeid_present = rail.IfOperator(
            task_id='if_employeeid_present',
            test='''{{ dag_run.conf["employeeid"] | is_truthy  and result('bulk_get_users3')[0].userDetails.employeeId != dag_run.conf["employeeid"] }}''',
            yes_task="update_employee_id",
            no_task="if_startdate_present",
        )

        update_employee_id = rail.RepliconServiceOperator(
            task_id='update_employee_id',
            endpoint="/services/UserService1.svc/UpdateEmployeeId",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "employeeId": '{{ dag_run.conf["employeeid"] }}'
            }
        )

        if_startdate_present = rail.IfOperator(
            task_id='if_startdate_present',
            test='''{{ dag_run.conf["startdate"] | is_truthy }}''',
            yes_task="if_startdate_not_contains",
            no_task="get_today",
        )

        if_startdate_not_contains = rail.IfOperator(
            task_id='if_startdate_not_contains',
            test='''{{ dag_run.conf["startdate"] | matches('/') | is_falsy }}''',
            yes_task="log_start_dateupdate_skipped",
            no_task="log_start_dateday",
        )

        log_start_dateupdate_skipped = rail.PythonOperator(
            task_id='log_start_dateupdate_skipped',
            python_callable=lambda:  "Start date is not in the predefined format"
        )

        log_start_dateday = rail.PythonOperator(
            task_id='log_start_dateday',
            python_callable=lambda dag_run: python_callable.get_datetime_obj(
                dag_run.conf["startdate"])
        )

        log_start_dateasperfeedfile = rail.PythonOperator(
            task_id='log_start_dateasperfeedfile',
            python_callable=lambda:  str(rail.result('log_start_dateday')['day']) + "/" +
            str(rail.result('log_start_dateday')['month']) + "/" +
            str(rail.result('log_start_dateday')['year'])
        )

        log_start_dateasper_repliconprofile = rail.PythonOperator(
            task_id='log_start_dateasper_repliconprofile',
            python_callable=lambda:  str(rail.result('bulk_get_users3')[0]['userDetails']['employmentDateRange']['startDate']['day']) + "/" +
            str(rail.result('bulk_get_users3')[0]['userDetails']['employmentDateRange']['startDate']['month']) + "/" +
            str(rail.result('bulk_get_users3')[
                0]['userDetails']['employmentDateRange']['startDate']['year'])
        )

        if_startdate_changed_1 = rail.IfOperator(
            task_id='if_startdate_changed_1',
            test='''{{result('log_start_dateasperfeedfile') != result('log_start_dateasper_repliconprofile')}}''',
            yes_task="update_employment_date_rangeforstartdate",
            no_task="get_today",
        )

        update_employment_date_rangeforstartdate = rail.RepliconServiceOperator(
            task_id='update_employment_date_rangeforstartdate',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('log_start_dateday').year }}",
                        "month": "{{ result('log_start_dateday').month }}",
                        "day": "{{ result('log_start_dateday').day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        log_get_udf_uri_most_recent_hire_date = rail.PythonOperator(
            task_id='log_get_udf_uri_most_recent_hire_date',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3')[0]['userDetails']['customFieldValues'],
                                                                         'customField.displayText',
                                                                         'Most Recent Hire Date',
                                                                         'customField.uri')
        )

        if_get_udf_uri_most_recent_hire_date_present = rail.IfOperator(
            task_id='if_get_udf_uri_most_recent_hire_date_present',
            test='''{{ result('log_get_udf_uri_most_recent_hire_date') | is_truthy }}''',
            yes_task="update_date_valuefor_most_recent_hire_date",
            no_task="get_today",
        )

        update_date_valuefor_most_recent_hire_date = rail.RepliconServiceOperator(
            task_id='update_date_valuefor_most_recent_hire_date',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data={
                "objectUri": '{{ dag_run.conf["useruri"] }}',
                "customFieldUri": "{{ result('log_get_udf_uri_most_recent_hire_date') }}",
                "value": {
                    "year": "{{ result('log_start_dateday').year }}",
                    "month": "{{ result('log_start_dateday').month }}",
                    "day": "{{ result('log_start_dateday').day }}"
                }
            }
        )

        def get_todays_date():
            date = datetime.utcnow()
            return {
                'date': date.strftime("%m/%d/%Y"),
                'description': 'Effective on ' + date.strftime("%m/%d/%Y"),
                'day': date.day,
                'month': date.month,
                'year': date.year
            }

        get_today = rail.PythonOperator(
            task_id='get_today',
            python_callable=get_todays_date
        )

        if_timetype_present = rail.IfOperator(
            task_id='if_timetype_present',
            test='''{{ dag_run.conf["timetype"] | is_truthy }}''',
            yes_task="log_get_f_t_p_t_uri",
            no_task="if_homecountry_present",
        )

        log_get_f_t_p_t_uri = rail.PythonOperator(
            task_id='log_get_f_t_p_t_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3')[0]['userDetails']['customFieldValues'],
                                                                         "customField.displayText", "FT/PT", "customField.uri")
        )

        log_get_f_t_p_t_value = rail.PythonOperator(
            task_id='log_get_f_t_p_t_value',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3')[0]['userDetails']['customFieldValues'],
                                                                         "customField.displayText", "FT/PT", "text")
        )

        if_get_f_t_p_t_value_41_ne_timetype = rail.IfOperator(
            task_id='if_get_f_t_p_t_value_41_ne_timetype',
            test='''{{ result('log_get_f_t_p_t_value') != dag_run.conf["timetype"] }}''',
            yes_task="get_enabled_custom_field",
            no_task="if_homecountry_present",
        )

        get_enabled_custom_field = rail.RepliconServiceOperator(
            task_id='get_enabled_custom_field',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('log_get_f_t_p_t_uri') }}"
            }
        )

        log_gettherequiredudfdropdownuri = rail.PythonOperator(
            task_id='log_gettherequiredudfdropdownuri',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_custom_field'),
                                                                                 'displayText',
                                                                                 dag_run.conf["timetype"],
                                                                                 'uri')
        )

        if_gettherequiredudfdropdownuri_present = rail.IfOperator(
            task_id='if_gettherequiredudfdropdownuri_present',
            test='''{{ result('log_gettherequiredudfdropdownuri') | is_truthy }}''',
            yes_task="update_dropdown_valuefor_f_t_p_t",
            no_task="if_homecountry_present",
        )

        update_dropdown_valuefor_f_t_p_t = rail.RepliconServiceOperator(
            task_id='update_dropdown_valuefor_f_t_p_t',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": '{{ dag_run.conf["useruri"] }}',
                "customFieldUri": "{{ result('log_get_f_t_p_t_uri') }}",
                "customFieldDropDownOptionUri": "{{ result('log_gettherequiredudfdropdownuri') }}"
            }
        )

        if_homecountry_present = rail.IfOperator(
            task_id='if_homecountry_present',
            test='''{{ dag_run.conf["homecountry"] | is_truthy }}''',
            yes_task="log_get_udf_uri_home_country",
            no_task="if_udf_present",
        )

        log_get_udf_uri_home_country = rail.PythonOperator(
            task_id='log_get_udf_uri_home_country',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3')[0]['userDetails']['customFieldValues'],
                                                                         "customField.displayText", "Home - Country", "customField.uri")
        )

        log_get_home_country_value = rail.PythonOperator(
            task_id='log_get_home_country_value',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3')[0]['userDetails']['customFieldValues'],
                                                                         "customField.displayText", "Home - Country", "text")
        )

        if_get_home_country_value_49_ne_homecountry = rail.IfOperator(
            task_id='if_get_home_country_value_49_ne_homecountry',
            test='''{{ result('log_get_home_country_value') != dag_run.conf["homecountry"] }}''',
            yes_task="update_text_valuefor_home_country_1",
            no_task="if_udf_present",
        )

        update_text_valuefor_home_country_1 = rail.RepliconServiceOperator(
            task_id='update_text_valuefor_home_country_1',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": '{{ dag_run.conf["useruri"] }}',
                "customFieldUri": "{{ result('log_get_udf_uri_home_country') }}",
                "value": '{{ dag_run.conf["homecountry"] }}'
            }
        )

        if_udf_present = rail.IfOperator(
            task_id='if_udf_present',
            test='''{{ dag_run.conf["udf"] | is_truthy }}''',
            yes_task="log_get_udf_uri_scheduled_hours_1",
            no_task="if_udf_blank",
        )

        log_get_udf_uri_scheduled_hours_1 = rail.PythonOperator(
            task_id='log_get_udf_uri_scheduled_hours_1',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3')[0]['userDetails']['customFieldValues'],
                                                                         "customField.displayText", "Scheduled Hours", "customField.uri")
        )

        log_get_scheduled_hours_value = rail.PythonOperator(
            task_id='log_get_scheduled_hours_value',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3')[0]['userDetails']['customFieldValues'],
                                                                         "customField.displayText", "Scheduled Hours", "text")
        )

        if_to_f_ne_udfto_f = rail.IfOperator(
            task_id='if_to_f_ne_udfto_f',
            test=lambda dag_run: float(dag_run.conf["udf"]) != float(
                rail.result('log_get_scheduled_hours_value')),
            yes_task="update_numeric_valuefor_scheduled_hours_1",
            no_task="if_udf_blank",
        )

        update_numeric_valuefor_scheduled_hours_1 = rail.RepliconServiceOperator(
            task_id='update_numeric_valuefor_scheduled_hours_1',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": '{{ dag_run.conf["useruri"] }}',
                "customFieldUri": "{{ result('log_get_udf_uri_scheduled_hours_1') }}",
                "value": '{{ dag_run.conf["udf"] }}'
            }
        )

        update_variable_1 = rail.SetVariableOperator(
            task_id='update_variable_1',
            append=False,
            name='{{ result("declare_variable").name }}',
            value='yes'
        )

        if_udf_blank = rail.IfOperator(
            task_id='if_udf_blank',
            test='''{{ dag_run.conf["udf"] | is_falsy }}''',
            yes_task="log_get_udf_uri_scheduled_hours_2",
            no_task="if_homestateprovince_present",
        )

        log_get_udf_uri_scheduled_hours_2 = rail.PythonOperator(
            task_id='log_get_udf_uri_scheduled_hours_2',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3')[0]['userDetails']['customFieldValues'],
                                                                         "customField.displayText", "Scheduled Hours", "customField.uri")
        )

        update_numeric_valuefor_scheduled_hours_2 = rail.RepliconServiceOperator(
            task_id='update_numeric_valuefor_scheduled_hours_2',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": '{{ dag_run.conf["useruri"] }}',
                "customFieldUri": "{{ result('log_get_udf_uri_scheduled_hours_2') }}",
                "value": 0
            }
        )

        update_variable_2 = rail.SetVariableOperator(
            task_id='update_variable_2',
            append=False,
            name='{{ result("declare_variable").name }}',
            value='yes'
        )

        if_homestateprovince_present = rail.IfOperator(
            task_id='if_homestateprovince_present',
            test='''{{ dag_run.conf["homestateprovince"] | is_truthy }}''',
            yes_task="log_get_udf_uri_home_state_province",
            no_task="if_homecity_present",
        )

        log_get_udf_uri_home_state_province = rail.PythonOperator(
            task_id='log_get_udf_uri_home_state_province',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3')[0]['userDetails']['customFieldValues'],
                                                                         "customField.displayText", "Home - State/Province", "customField.uri")
        )

        log_get_home_state_province_value = rail.PythonOperator(
            task_id='log_get_home_state_province_value',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3')[0]['userDetails']['customFieldValues'],
                                                                         "customField.displayText", "Home - State/Province", "text")
        )

        if_home_state_changed = rail.IfOperator(
            task_id='if_home_state_changed',
            test='''{{ result('log_get_home_state_province_value') != dag_run.conf["homestateprovince"] }}''',
            yes_task="update_text_valuefor_home_country_2",
            no_task="if_homecity_present",
        )

        update_text_valuefor_home_country_2 = rail.RepliconServiceOperator(
            task_id='update_text_valuefor_home_country_2',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": '{{ dag_run.conf["useruri"] }}',
                "customFieldUri": "{{ result('log_get_udf_uri_home_state_province') }}",
                "value": '{{ dag_run.conf["homestateprovince"] }}'
            }
        )

        if_homecity_present = rail.IfOperator(
            task_id='if_homecity_present',
            test='''{{ dag_run.conf["homecity"] | is_truthy }}''',
            yes_task="log_get_udf_uri_home_city",
            no_task="if_continuousservicedate_present",
        )

        log_get_udf_uri_home_city = rail.PythonOperator(
            task_id='log_get_udf_uri_home_city',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3')[0]['userDetails']['customFieldValues'],
                                                                         "customField.displayText", "Home - City", "customField.uri")
        )

        log_get_home_city_value = rail.PythonOperator(
            task_id='log_get_home_city_value',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3')[0]['userDetails']['customFieldValues'],
                                                                         "customField.displayText", "Home - City", "text")
        )

        if_get_home_city_value_69_ne_homecity = rail.IfOperator(
            task_id='if_get_home_city_value_69_ne_homecity',
            test='''{{ result('log_get_home_city_value') != dag_run.conf["homecity"] }}''',
            yes_task="update_text_valuefor_home_city",
            no_task="if_continuousservicedate_present",
        )

        update_text_valuefor_home_city = rail.RepliconServiceOperator(
            task_id='update_text_valuefor_home_city',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": '{{ dag_run.conf["useruri"] }}',
                "customFieldUri": "{{ result('log_get_udf_uri_home_city') }}",
                "value": '{{ dag_run.conf["homecity"] }}'
            }
        )

        if_continuousservicedate_present = rail.IfOperator(
            task_id='if_continuousservicedate_present',
            test='''{{ dag_run.conf["continuousservicedate"] | is_truthy }}''',
            yes_task="if_continuousservicedate_not_contains",
            no_task="if_employeetype_present_fulltimehourly",
        )

        if_continuousservicedate_not_contains = rail.IfOperator(
            task_id='if_continuousservicedate_not_contains',
            test='''{{ dag_run.conf["continuousservicedate"] | matches('/') | is_falsy }}''',
            yes_task="log_skipchecking_continuous_service_date",
            no_task="log_get_udf_uri_continuous_service_date",
        )

        log_skipchecking_continuous_service_date = rail.PythonOperator(
            task_id='log_skipchecking_continuous_service_date',
            python_callable=lambda:  "Continuous service date not is the predefined format"
        )

        log_get_udf_uri_continuous_service_date = rail.PythonOperator(
            task_id='log_get_udf_uri_continuous_service_date',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3')[0]['userDetails']['customFieldValues'],
                                                                         "customField.displayText", "Continuous Service Date", "customField.uri")
        )

        log_get_continuous_service_date_value = rail.PythonOperator(
            task_id='log_get_continuous_service_date_value',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3')[0]['userDetails']['customFieldValues'],
                                                                         "customField.displayText", "Continuous Service Date", "text")
        )

        get_continuous_date_1 = rail.PythonOperator(
            task_id='get_continuous_date_1',
            python_callable=python_callable.get_datetime_obj,
            op_args=[
                "{{result('log_get_continuous_service_date_value')}}", "%Y %B %d"]
        )

        get_continuous_date_2 = rail.PythonOperator(
            task_id='get_continuous_date_2',
            python_callable=python_callable.get_datetime_obj,
            op_args=['{{ dag_run.conf["continuousservicedate"] }}', "%m/%d/%Y"]
        )

        if_continuous_date_changed = rail.IfOperator(
            task_id='if_continuous_date_changed',
            test='''{{ result('get_continuous_date_1') != result('get_continuous_date_2') }}''',
            yes_task="update_date_valuefor_continuous_service_date",
            no_task="if_employeetype_present_fulltimehourly",
        )

        update_date_valuefor_continuous_service_date = rail.RepliconServiceOperator(
            task_id='update_date_valuefor_continuous_service_date',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf["useruri"],
                "customFieldUri": rail.result('log_get_udf_uri_continuous_service_date'),
                "value": {
                    "year": rail.result('get_continuous_date_2')['year'],
                    "month": rail.result('get_continuous_date_2')['month'],
                    "day": rail.result('get_continuous_date_2')['day']
                }
            }
        )

        if_employeetype_present_fulltimehourly = rail.IfOperator(
            task_id='if_employeetype_present_fulltimehourly',
            test='''{{ dag_run.conf["employeetype"] | is_truthy and result('bulk_get_users3')[0].employeeType.name != dag_run.conf["employeetype"] }}''',
            yes_task="get_all_employee_type_details",
            no_task="if_department_present",
        )

        get_all_employee_type_details = rail.RepliconServiceOperator(
            task_id='get_all_employee_type_details',
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
            data=None
        )

        log_employee_type_uri = rail.PythonOperator(
            task_id='log_employee_type_uri',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result('get_all_employee_type_details'),
                                                                                 'displayText',
                                                                                 dag_run.conf["employeetype"],
                                                                                 'uri', '')
        )

        if_employee_type_uri_present = rail.IfOperator(
            task_id='if_employee_type_uri_present',
            test='''{{ result('log_employee_type_uri') | is_truthy }}''',
            yes_task="update_employee_type_for_user",
            no_task="if_department_present",
        )

        update_employee_type_for_user = rail.RepliconServiceOperator(
            task_id='update_employee_type_for_user',
            endpoint="/services/EmployeeTypeService1.svc/UpdateEmployeeTypeForUser",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "employeeTypeUri": "{{ result('log_employee_type_uri') }}"
            }
        )

        update_variable_3 = rail.SetVariableOperator(
            task_id='update_variable_3',
            append=False,
            name='{{ result("declare_variable").name }}',
            value='yes'
        )

        if_department_present = rail.IfOperator(
            task_id='if_department_present',
            test='''{{ dag_run.conf["department"] | is_truthy  and dag_run.conf["department"] != result('bulk_get_users3')[0].userDetails.department.name }}''',
            yes_task="if_departmenturi_present",
            no_task="get_datafortherequireduser",
        )

        if_departmenturi_present = rail.IfOperator(
            task_id='if_departmenturi_present',
            test='''{{ dag_run.conf["departmenturi"] | is_truthy }}''',
            yes_task="update_department_for_user",
            no_task="log_error_logfordepartmentnotpresent",
        )

        update_department_for_user = rail.RepliconServiceOperator(
            task_id='update_department_for_user',
            endpoint="/services/DepartmentService1.svc/UpdateDepartmentForUser",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "departmentUri": '{{ dag_run.conf["departmenturi"] }}'
            }
        )

        log_error_logfordepartmentnotpresent = rail.PythonOperator(
            task_id='log_error_logfordepartmentnotpresent',
            python_callable=lambda dag_run: f'''Department not updated for User "{dag_run.conf["employeefirstname"]} {dag_run.conf["employeelastname"]}". "{dag_run.conf["department"]}" is not available in Replicon.'''
        )

        get_datafortherequireduser = rail.RepliconServiceOperator(
            task_id='get_datafortherequireduser',
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_data_for_required_user_payload
        )

        def get_if_supervisor_assigned():
            user_data = rail.result('get_datafortherequireduser')['rows']
            for entry in user_data:
                for i in entry['cells']:
                    return i.get('dataType')

        log_checkifsupervsorisassigned = rail.PythonOperator(
            task_id='log_checkifsupervsorisassigned',
            python_callable=get_if_supervisor_assigned
        )

        if_manger_present_1 = rail.IfOperator(
            task_id='if_manger_present_1',
            test='''{{ dag_run.conf["manager"] | is_truthy }}''',
            yes_task="if_loginname_eq_manger",
            no_task="if_costcenter_present",
        )

        if_loginname_eq_manger = rail.IfOperator(
            task_id='if_loginname_eq_manger',
            test='''{{ dag_run.conf["loginname"] == dag_run.conf["manager"] }}''',
            yes_task="log_error_supervisor_self",
            no_task="if_loginname_ne_manger",
        )

        log_error_supervisor_self = rail.PythonOperator(
            task_id='log_error_supervisor_self',
            python_callable=lambda dag_run: f'Supervsior not updated for {dag_run.conf["loginname"]} as user\'s and supervsior\'s login name are same'
        )

        if_loginname_ne_manger = rail.IfOperator(
            task_id='if_loginname_ne_manger',
            test='''{{ dag_run.conf["loginname"] != dag_run.conf["manager"] }}''',
            yes_task="get_all_permissionsets",
            no_task="if_costcenter_present",
        )

        get_all_permissionsets = rail.RepliconServiceOperator(
            task_id='get_all_permissionsets',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data=None
        )

        search_users = rail.RepliconServiceOperator(
            task_id='search_users',
            endpoint='/services/UserListService1.svc/GetData',
            data=lambda dag_run: {
                'page': '1',
                'pagesize': '100',
                'columnUris': [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:enabled"
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:login-name'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf["manager"]
                        }
                    }
                }
            },
            data_handler=response_filter.get_manager
        )

        if_getsupervisor_uri_101_present_1 = rail.IfOperator(
            task_id='if_getsupervisor_uri_101_present_1',
            test='''{{ result('search_users').uri | is_truthy }}''',
            yes_task="get_assigned_permissionsets",
            no_task="if_supervisor_not_assigned",
        )

        get_assigned_permissionsets = rail.RepliconServiceOperator(
            task_id='get_assigned_permissionsets',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('search_users').uri }}"
            }
        )

        log_checkifsupervisorhassupervisorpermission = rail.PythonOperator(
            task_id='log_checkifsupervisorhassupervisorpermission',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_assigned_permissionsets'), 'permissionSet.displayText', 'Supervisor', 'permissionSet.uri', '')
        )

        if_supervisorhassupervisorpermission_105_blank = rail.IfOperator(
            task_id='if_supervisorhassupervisorpermission_105_blank',
            test='''{{ result('log_checkifsupervisorhassupervisorpermission') | is_falsy }}''',
            yes_task="log_get_supervisor_permission",
            no_task="if_supervisor_not_assigned",
        )

        log_get_supervisor_permission = rail.PythonOperator(
            task_id='log_get_supervisor_permission',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_permissionsets'), 'displayText', "Supervisor", 'uri')
        )

        assign_supervsior_permission_set_to_user = rail.RepliconServiceOperator(
            task_id='assign_supervsior_permission_set_to_user',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ result('search_users').uri }}",
                "permissionSetUri": "{{ result('log_get_supervisor_permission') }}"
            }
        )

        if_supervisor_not_assigned = rail.IfOperator(
            task_id='if_supervisor_not_assigned',
            test='''{{ result('log_checkifsupervsorisassigned') == 'urn:replicon:list-type:null' }}''',
            yes_task="if_getsupervisor_status_102_eq_true_1",
            no_task="if_supervisor_is_assigned",
        )

        if_getsupervisor_status_102_eq_true_1 = rail.IfOperator(
            task_id='if_getsupervisor_status_102_eq_true_1',
            test='''{{ result('search_users').status == 'True' }}''',
            yes_task="if_getsupervisor_uri_101_present_2",
            no_task="ascend_supervisor_assignment_table_add_entry_2",
        )

        if_getsupervisor_uri_101_present_2 = rail.IfOperator(
            task_id='if_getsupervisor_uri_101_present_2',
            test='''{{ result('search_users').uri | is_truthy }}''',
            yes_task="update_initial_supervisor",
            no_task="if_getsupervisor_uri_101_blank",
        )

        update_initial_supervisor = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor',
            endpoint="/services/UserService1.svc/PutSupervisorAssignmentSchedule",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "initialSupervisorUri": "{{ result('search_users').uri }}",
                "scheduleEntries": []
            }
        )

        if_getsupervisor_uri_101_blank = rail.IfOperator(
            task_id='if_getsupervisor_uri_101_blank',
            test='''{{ result('search_users').uri | is_falsy }}''',
            yes_task="ascend_supervisor_assignment_table_add_entry_1",
            no_task="if_supervisor_is_assigned",
        )

        ascend_supervisor_assignment_table_add_entry_1 = rail.WriteLogOperator(
            task_id='ascend_supervisor_assignment_table_add_entry_1',
            log='{{ dag_run.conf["ascend_supervisor_assignments_logs_lookuptable"] }}',
            message="na",
            properties={
                "userloginname": '{{ dag_run.conf["loginname"] }}',
                "useruri": '{{ dag_run.conf["useruri"] }}',
                "supervisorloginname": '{{ dag_run.conf["manager"] }}',
                "action": "Add",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        ascend_supervisor_assignment_table_add_entry_2 = rail.WriteLogOperator(
            task_id='ascend_supervisor_assignment_table_add_entry_2',
            log='{{ dag_run.conf["ascend_supervisor_assignments_logs_lookuptable"] }}',
            message="na",
            properties={
                "userloginname": '{{ dag_run.conf["loginname"] }}',
                "useruri": '{{ dag_run.conf["useruri"] }}',
                "supervisorloginname": '{{ dag_run.conf["manager"] }}',
                "action": "Add",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_supervisor_is_assigned = rail.IfOperator(
            task_id='if_supervisor_is_assigned',
            test='''{{ result('log_checkifsupervsorisassigned') != 'urn:replicon:list-type:null' }}''',
            yes_task="log_current_supervisor_name",
            no_task="if_costcenter_present",
        )

        log_current_supervisor_name = rail.PythonOperator(
            task_id='log_current_supervisor_name',
            python_callable=lambda:  rail.result('get_datafortherequireduser')[
                'rows'][0]['cells'][0]['textValue']
        )

        log_getthesupervisorloginname = rail.PythonOperator(
            task_id='log_getthesupervisorloginname',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result('bulk_get_users3')[0]['supervisorAssignmentSchedule'],
                                                                          'supervisor.displayText', rail.result('log_current_supervisor_name'), 'supervisor.user.loginName')
        )

        if_manger_present_2 = rail.IfOperator(
            task_id='if_manger_present_2',
            test='''{{ dag_run.conf["manager"] | is_truthy  and dag_run.conf["manager"] != result('log_getthesupervisorloginname') }}''',
            yes_task="if_getsupervisor_uri_101_present_3",
            no_task="if_costcenter_present",
        )

        if_getsupervisor_uri_101_present_3 = rail.IfOperator(
            task_id='if_getsupervisor_uri_101_present_3',
            test='''{{ result('search_users').uri | is_truthy }}''',
            yes_task="if_getsupervisor_status_102_eq_true_2",
            no_task="ascend_supervisor_assignment_table_add_entry_4",
        )

        if_getsupervisor_status_102_eq_true_2 = rail.IfOperator(
            task_id='if_getsupervisor_status_102_eq_true_2',
            test='''{{ result('search_users').status == 'True' }}''',
            yes_task="update_supervisor_schedule",
            no_task="ascend_supervisor_assignment_table_add_entry_3",
        )

        update_supervisor_schedule = rail.RepliconServiceOperator(
            task_id='update_supervisor_schedule',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "supervisorUri": "{{ result('search_users').uri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('get_today').year }}",
                        "month": "{{ result('get_today').month }}",
                        "day": "{{ result('get_today').day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        ascend_supervisor_assignment_table_add_entry_3 = rail.WriteLogOperator(
            task_id='ascend_supervisor_assignment_table_add_entry_3',
            log='{{ dag_run.conf["ascend_supervisor_assignments_logs_lookuptable"] }}',
            message="na",
            properties={
                "userloginname": '{{ dag_run.conf["loginname"] }}',
                "useruri": '{{ dag_run.conf["useruri"] }}',
                "supervisorloginname": '{{ dag_run.conf["manager"] }}',
                "action": "Update",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        ascend_supervisor_assignment_table_add_entry_4 = rail.WriteLogOperator(
            task_id='ascend_supervisor_assignment_table_add_entry_4',
            log='{{ dag_run.conf["ascend_supervisor_assignments_logs_lookuptable"] }}',
            message="na",
            properties={
                "userloginname": '{{ dag_run.conf["loginname"] }}',
                "useruri": '{{ dag_run.conf["useruri"] }}',
                "supervisorloginname": '{{ dag_run.conf["manager"] }}',
                "action": "Update",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_costcenter_present = rail.IfOperator(
            task_id='if_costcenter_present',
            test='''{{ dag_run.conf["costcenter"] | is_truthy }}''',
            yes_task="log_checkifanycostcenterisassigned",
            no_task="if_location_present",
        )

        log_checkifanycostcenterisassigned = rail.PythonOperator(
            task_id='log_checkifanycostcenterisassigned',
            python_callable=lambda:  rail.result('get_datafortherequireduser')[
                'rows'][0]['cells'][2]['dataType']
        )

        log_required_cost_center = rail.PythonOperator(
            task_id='log_required_cost_center',
            python_callable=lambda dag_run:  dag_run.conf["costcenter"].rsplit(
                '|', maxsplit=1)[-1]
        )

        get_dataforcostcenters = rail.RepliconServiceOperator(
            task_id='get_dataforcostcenters',
            endpoint="/services/CostCenterListService1.svc/GetData",
            data=request_payload.get_cost_center_payload
        )

        # Replaced: foreach_d + accumulate_list_items_1 + foreach_d_134_end
        # Finds matching cost center URI by matching full path (joined cellCollection textValues)
        # against the conf costcenter value. Handles ' | ' and '/' separators,
        # strips whitespace, and compares case-insensitively.
        log_get_required_costcenter_uri = rail.PythonOperator(
            task_id='log_get_required_costcenter_uri',
            python_callable=lambda dag_run: next(
                (
                    row['cells'][0]['cellCollection'][-1]['uri']
                    for row in rail.result('get_dataforcostcenters')['rows']
                    if '/'.join(cell['textValue'].strip() for cell in row['cells'][0]['cellCollection']).lower() ==
                       dag_run.conf["costcenter"].replace(' | ', '/').strip().lower()
                ),
                None
            )
        )

        if_costcenter_changed_1 = rail.IfOperator(
            task_id='if_costcenter_changed_1',
            test='''{{ result('log_get_required_costcenter_uri') | is_falsy }}''',
            yes_task="log_errormessageincasewhencostcenterisnotavailable",
            no_task="if_costcenter_changed_2",
        )

        log_errormessageincasewhencostcenterisnotavailable = rail.PythonOperator(
            task_id='log_errormessageincasewhencostcenterisnotavailable',
            python_callable=lambda dag_run: f'''Cost center not assigned for User as "{dag_run.conf["costcenter"]}" is not available in Replicon'''
        )

        if_costcenter_changed_2 = rail.IfOperator(
            task_id='if_costcenter_changed_2',
            test='''{{ result('log_get_required_costcenter_uri') | is_truthy }}''',
            yes_task="if_costcenter_not_assigned",
            no_task="if_location_present",
        )

        if_costcenter_not_assigned = rail.IfOperator(
            task_id='if_costcenter_not_assigned',
            test='''{{ result('log_checkifanycostcenterisassigned') == 'urn:replicon:list-type:null' }}''',
            yes_task="put_cost_center_schedule_for_user_1",
            no_task="if_costcenter_is_assigned",
        )

        put_cost_center_schedule_for_user_1 = rail.RepliconServiceOperator(
            task_id='put_cost_center_schedule_for_user_1',
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "scheduleEntries": [
                    {
                        "costCenter": {
                            "uri": "{{ result('log_get_required_costcenter_uri') }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        if_costcenter_is_assigned = rail.IfOperator(
            task_id='if_costcenter_is_assigned',
            test='''{{ result('log_checkifanycostcenterisassigned') != 'urn:replicon:list-type:null' }}''',
            yes_task="log_getthecurrentcostcenter_1",
            no_task="if_location_present",
        )

        log_getthecurrentcostcenter_1 = rail.PythonOperator(
            task_id='log_getthecurrentcostcenter_1',
            python_callable=lambda:  rail.result('get_datafortherequireduser')[
                'rows'][0]['cells'][2]
        )

        parse_json_1 = rail.PythonOperator(
            task_id='parse_json_1',
            python_callable=lambda: json.loads(json.dumps(
                rail.result('log_getthecurrentcostcenter_1')))
        )

        log_getthecurrentcostcenter_2 = rail.PythonOperator(
            task_id='log_getthecurrentcostcenter_2',
            python_callable=lambda:  rail.result('parse_json_1')[
                'cellCollection'][-1]['textValue']
        )

        log_getthecurrentcostcenteruri = rail.PythonOperator(
            task_id='log_getthecurrentcostcenteruri',
            python_callable=lambda:  rail.result('parse_json_1')[
                'cellCollection'][-1]['uri']
        )

        if_costcenter_changed_3 = rail.IfOperator(
            task_id='if_costcenter_changed_3',
            test='''{{ result('log_get_required_costcenter_uri') != result('log_getthecurrentcostcenteruri') }}''',
            yes_task="if_costcenter_changed_4",
            no_task="if_location_present",
        )

        if_costcenter_changed_4 = rail.IfOperator(
            task_id='if_costcenter_changed_4',
            test='''{{ result('log_get_required_costcenter_uri') | is_truthy }}''',
            yes_task="declare_list_1",
            no_task="if_location_present",
        )

        declare_list_1 = rail.SetVariableOperator(
            task_id='declare_list_1',
            append=False,
            name='costcenter_schedule',
            value=[]
        )

        foreach_response_1 = rail.ForEachOperator(
            task_id='foreach_response_1',
            items="{{ result('bulk_get_users3') | to_json }}",
            start_task='foreach_foreach_response_1',
            end_task='foreach_response_150_end'
        )

        foreach_foreach_response_1 = rail.ForEachOperator(
            task_id='foreach_foreach_response_1',
            items="{{ result('foreach_response_1').costCenterSchedule | to_json }}",
            start_task='if_effectivedate_day_blank_1',
            end_task='foreach_foreach_response_150_151_end'
        )

        if_effectivedate_day_blank_1 = rail.IfOperator(
            task_id='if_effectivedate_day_blank_1',
            test='''{{ result('foreach_foreach_response_1').effectiveDate | is_falsy or result('foreach_foreach_response_1').effectiveDate.day | is_falsy }}''',
            yes_task="insert_to_list_1",
            no_task="log_effective_date_1",
        )

        insert_to_list_1 = rail.SetVariableOperator(
            task_id='insert_to_list_1',
            append=True,
            name='{{ result("declare_list_1").name }}',
            value={
                "effectiveDate": {
                    "year": "skip",
                    "month": "skip",
                    "day": "skip"
                },
                "costCenter": {
                    "uri": "{{ result('foreach_foreach_response_1').costCenter.uri }}",
                    "parentUri": null,
                    "name": null
                }
            }
        )

        log_effective_date_1 = rail.PythonOperator(
            task_id='log_effective_date_1',
            python_callable=lambda: str(rail.result('foreach_foreach_response_1')['effectiveDate']['day']) + "/" +
            str(rail.result('foreach_foreach_response_1')['effectiveDate']['month']) + "/" +
            str(rail.result('foreach_foreach_response_1')
                ['effectiveDate']['year'])
        )

        if_to_time_ne_todayto_time_1 = rail.IfOperator(
            task_id='if_to_time_ne_todayto_time_1',
            test=lambda: datetime.strptime(rail.result(
                'log_effective_date_1'), '%d/%m/%Y') < datetime.now(),
            yes_task="insert_to_list_2",
            no_task="foreach_foreach_response_150_151_end",
        )

        insert_to_list_2 = rail.SetVariableOperator(
            task_id='insert_to_list_2',
            append=True,
            name='{{ result("declare_list_1").name }}',
            value={
                "effectiveDate": {
                    "year": "{{ result('foreach_foreach_response_1').effectiveDate.year }}",
                    "month": "{{ result('foreach_foreach_response_1').effectiveDate.month }}",
                    "day": "{{ result('foreach_foreach_response_1').effectiveDate.day }}"
                },
                "costCenter": {
                    "uri": "{{ result('foreach_foreach_response_1').costCenter.uri }}",
                    "parentUri": null,
                    "name": null
                }
            }
        )

        foreach_foreach_response_150_151_end = rail.EmptyOperator(
            task_id='foreach_foreach_response_150_151_end',
        )

        foreach_response_150_end = rail.EmptyOperator(
            task_id='foreach_response_150_end',
        )

        insert_to_list_3 = rail.SetVariableOperator(
            task_id='insert_to_list_3',
            append=True,
            name='{{ result("declare_list_1").name }}',
            value={
                "effectiveDate": {
                    "year": "{{ result('get_today').year }}",
                    "month": "{{ result('get_today').month }}",
                    "day": "{{ result('get_today').day }}"
                },
                "costCenter": {
                    "uri": "{{ result('log_get_required_costcenter_uri') }}",
                    "parentUri": null,
                    "name": null
                }
            }
        )

        log_cost_center_schedule = rail.PythonOperator(
            task_id='log_cost_center_schedule',
            python_callable=lambda: _process_schedule_entries(json.loads(
                json.dumps(rail.get_dag_run_var(rail.result('declare_list_1')['name']) or [])
                .replace('"effectiveDate": {}', '"effectiveDate": null')
            ))
        )

        if_cost_center_schedule_present = rail.IfOperator(
            task_id='if_cost_center_schedule_present',
            test='''{{ result('log_cost_center_schedule') | is_truthy }}''',
            yes_task="put_cost_center_schedule_for_user_2",
            no_task="if_location_present",
        )

        put_cost_center_schedule_for_user_2 = rail.RepliconServiceOperator(
            task_id='put_cost_center_schedule_for_user_2',
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf["useruri"],
                "scheduleEntries": rail.result('log_cost_center_schedule')
            }
        )

        if_location_present = rail.IfOperator(
            task_id='if_location_present',
            test='''{{ dag_run.conf["location"] | is_truthy }}''',
            yes_task="log_checkifanylocationisassigned",
            no_task="if_hourlypayrollrate_present",
        )

        log_checkifanylocationisassigned = rail.PythonOperator(
            task_id='log_checkifanylocationisassigned',
            python_callable=lambda:  rail.result('get_datafortherequireduser')[
                'rows'][0]['cells'][1]['dataType']
        )

        get_all_locations = rail.RepliconServiceOperator(
            task_id='get_all_locations',
            endpoint="/services/LocationService1.svc/GetAllLocations",
            data=None
        )

        log_get_required_location_uri = rail.PythonOperator(
            task_id='log_get_required_location_uri',
            python_callable=lambda dag_run:  rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_locations'), 'displayText', dag_run.conf["location"], 'uri')
        )

        if_location_changed_1 = rail.IfOperator(
            task_id='if_location_changed_1',
            test='''{{ result('log_get_required_location_uri') | is_truthy }}''',
            yes_task="if_location_not_assigned",
            no_task="if_location_changed_3",
        )

        if_location_not_assigned = rail.IfOperator(
            task_id='if_location_not_assigned',
            test='''{{ result('log_checkifanylocationisassigned') == 'urn:replicon:list-type:null' }}''',
            yes_task="put_location_schedule_for_user_1",
            no_task="if_location_is_assigned",
        )

        put_location_schedule_for_user_1 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_1',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "scheduleEntries": [
                    {
                        "location": {
                            "uri": "{{ result('log_get_required_location_uri') }}",
                            "parentUri": null,
                            "name": null
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        update_variable_4 = rail.SetVariableOperator(
            task_id='update_variable_4',
            append=False,
            name='{{ result("declare_variable").name }}',
            value='yes'
        )

        log_location_assignment_done = rail.PythonOperator(
            task_id='log_location_assignment_done',
            python_callable=lambda:  "Location assignment is done"
        )

        if_location_is_assigned = rail.IfOperator(
            task_id='if_location_is_assigned',
            test='''{{ result('log_checkifanylocationisassigned') != 'urn:replicon:list-type:null' }}''',
            yes_task="log_getthecurrent_location_1",
            no_task="if_location_changed_3",
        )

        log_getthecurrent_location_1 = rail.PythonOperator(
            task_id='log_getthecurrent_location_1',
            python_callable=lambda:  rail.result('get_datafortherequireduser')[
                'rows'][0]['cells'][1]
        )

        parse_json_2 = rail.PythonOperator(
            task_id='parse_json_2',
            python_callable=lambda: json.loads(json.dumps(
                rail.result('log_getthecurrent_location_1')))
        )

        log_getthecurrent_location_2 = rail.PythonOperator(
            task_id='log_getthecurrent_location_2',
            python_callable=lambda:  rail.result('parse_json_2')[
                'cellCollection'][-1]['textValue']
        )

        log_getthecurrent_location_uri = rail.PythonOperator(
            task_id='log_getthecurrent_location_uri',
            python_callable=lambda:  rail.result('parse_json_2')[
                'cellCollection'][-1]['uri']
        )

        if_location_changed_2 = rail.IfOperator(
            task_id='if_location_changed_2',
            test='''{{ result('log_get_required_location_uri') != result('log_getthecurrent_location_uri') }}''',
            yes_task="declare_list_2",
            no_task="if_location_changed_3",
        )

        declare_list_2 = rail.SetVariableOperator(
            task_id='declare_list_2',
            append=False,
            name='location_schedule',
            value=[]
        )

        foreach_response_2 = rail.ForEachOperator(
            task_id='foreach_response_2',
            items="{{ result('bulk_get_users3') | to_json }}",
            start_task='foreach_foreach_response_2',
            end_task='foreach_response_178_end'
        )

        foreach_foreach_response_2 = rail.ForEachOperator(
            task_id='foreach_foreach_response_2',
            items="{{ result('foreach_response_2').locationSchedule | to_json }}",
            start_task='if_effectivedate_day_blank_2',
            end_task='foreach_foreach_response_178_179_end'
        )

        if_effectivedate_day_blank_2 = rail.IfOperator(
            task_id='if_effectivedate_day_blank_2',
            test='''{{ result('foreach_foreach_response_2').effectiveDate | is_falsy or result('foreach_foreach_response_2').effectiveDate.day | is_falsy }}''',
            yes_task="insert_to_list_4",
            no_task="log_effective_date_2",
        )

        insert_to_list_4 = rail.SetVariableOperator(
            task_id='insert_to_list_4',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "location": {
                    "uri": "{{ result('foreach_foreach_response_2').location.uri }}",
                    "parentUri": null,
                    "name": null
                },
                "effectiveDate": {
                    "year": "skip",
                    "month": "skip",
                    "day": "skip"
                }
            }
        )

        log_effective_date_2 = rail.PythonOperator(
            task_id='log_effective_date_2',
            python_callable=lambda: str(rail.result('foreach_foreach_response_2')['effectiveDate']['day']) + "/" +
            str(rail.result('foreach_foreach_response_2')['effectiveDate']['month']) + "/" +
            str(rail.result('foreach_foreach_response_2')
                ['effectiveDate']['year'])
        )

        if_to_time_ne_todayto_time_2 = rail.IfOperator(
            task_id='if_to_time_ne_todayto_time_2',
            test=lambda: datetime.strptime(rail.result(
                'log_effective_date_2'), '%d/%m/%Y') < datetime.now(),
            yes_task="insert_to_list_5",
            no_task="foreach_foreach_response_178_179_end",
        )

        insert_to_list_5 = rail.SetVariableOperator(
            task_id='insert_to_list_5',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "location": {
                    "uri": "{{ result('foreach_foreach_response_2').location.uri }}",
                    "parentUri": null,
                    "name": null
                },
                "effectiveDate": {
                    "year": "{{ result('foreach_foreach_response_2').effectiveDate.year }}",
                    "month": "{{ result('foreach_foreach_response_2').effectiveDate.month }}",
                    "day": "{{ result('foreach_foreach_response_2').effectiveDate.day }}"
                }
            }
        )

        foreach_foreach_response_178_179_end = rail.EmptyOperator(
            task_id='foreach_foreach_response_178_179_end',
        )

        foreach_response_178_end = rail.EmptyOperator(
            task_id='foreach_response_178_end',
        )

        insert_to_list_6 = rail.SetVariableOperator(
            task_id='insert_to_list_6',
            append=True,
            name='{{ result("declare_list_2").name }}',
            value={
                "location": {
                    "uri": "{{ result('log_get_required_location_uri') }}",
                    "parentUri": null,
                    "name": null
                },
                "effectiveDate": {
                    "year": "{{ result('get_today').year }}",
                    "month": "{{ result('get_today').month }}",
                    "day": "{{ result('get_today').day }}"
                }
            }
        )

        log_location_schedule = rail.PythonOperator(
            task_id='log_location_schedule',
            python_callable=lambda: _process_schedule_entries(json.loads(
                json.dumps(rail.get_dag_run_var(rail.result('declare_list_2')['name']) or [])
                .replace('"effectiveDate": {}', '"effectiveDate": null')
            ))
        )

        if_location_schedule_present = rail.IfOperator(
            task_id='if_location_schedule_present',
            test='''{{ result('log_location_schedule') | is_truthy }}''',
            yes_task="put_location_schedule_for_user_2",
            no_task="if_location_changed_3",
        )

        put_location_schedule_for_user_2 = rail.RepliconServiceOperator(
            task_id='put_location_schedule_for_user_2',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf["useruri"],
                "scheduleEntries": rail.result('log_location_schedule')
            }
        )

        update_variable_5 = rail.SetVariableOperator(
            task_id='update_variable_5',
            append=False,
            name='{{ result("declare_variable").name }}',
            value='yes'
        )

        log_location_change_done = rail.PythonOperator(
            task_id='log_location_change_done',
            python_callable=lambda:  "Location change is done"
        )

        if_location_changed_3 = rail.IfOperator(
            task_id='if_location_changed_3',
            test='''{{ result('log_get_required_location_uri') | is_falsy }}''',
            yes_task="log_errormessageincasewhenlocationisnotavailable",
            no_task="if_hourlypayrollrate_present",
        )

        log_errormessageincasewhenlocationisnotavailable = rail.PythonOperator(
            task_id='log_errormessageincasewhenlocationisnotavailable',
            python_callable=lambda dag_run: "Location not assigned for User as " +
            str(dag_run.conf["location"]) + " is not available in Replicon"
        )

        if_hourlypayrollrate_present = rail.IfOperator(
            task_id='if_hourlypayrollrate_present',
            test='''{{ dag_run.conf["hourlypayrollrate"] | is_truthy }}''',
            yes_task="declare_list_3",
            no_task="log_getthemindaydiff",
        )

        declare_list_3 = rail.SetVariableOperator(
            task_id='declare_list_3',
            append=False,
            name='payrollrate_schedule',
            value=[]
        )

        foreach_response_3 = rail.ForEachOperator(
            task_id='foreach_response_3',
            items="{{ result('bulk_get_users3') | to_json }}",
            start_task='foreach_foreach_response_3',
            end_task='foreach_response_196_end'
        )

        foreach_foreach_response_3 = rail.ForEachOperator(
            task_id='foreach_foreach_response_3',
            items="{{ result('foreach_response_3').payrollRateSchedule | to_json }}",
            start_task='if_effectivedate_day_present_1',
            end_task='foreach_foreach_response_196_197_end'
        )

        if_effectivedate_day_present_1 = rail.IfOperator(
            task_id='if_effectivedate_day_present_1',
            test='''{{ result('foreach_foreach_response_3').effectiveDate | is_truthy and result('foreach_foreach_response_3').effectiveDate.day | is_truthy }}''',
            yes_task="log_effective_date_3",
            no_task="log_initial_hourly_rate_1",
        )

        log_effective_date_3 = rail.PythonOperator(
            task_id='log_effective_date_3',
            python_callable=lambda: str(rail.result('foreach_foreach_response_3')['effectiveDate']['day']) + "/" +
            str(rail.result('foreach_foreach_response_3')['effectiveDate']['month']) + "/" +
            str(rail.result('foreach_foreach_response_3')
                ['effectiveDate']['year'])
        )

        if_to_time_ne_todayto_time_3 = rail.IfOperator(
            task_id='if_to_time_ne_todayto_time_3',
            test=lambda: datetime.strptime(rail.result(
                'log_effective_date_3'), '%d/%m/%Y') < datetime.utcnow(),
            yes_task="accumulate_list_items_2",
            no_task="foreach_foreach_response_196_197_end",
        )

        accumulate_list_items_2 = rail.SetVariableOperator(
            task_id='accumulate_list_items_2',
            name='payroll_rate_schedule',
            append=True,
            value=lambda: {
                "effectivedate": rail.result('log_effective_date_3'),
                "rate": (rail.result('foreach_foreach_response_3').get('hourlyRate') or {}).get('amount'),
                "currency": ((rail.result('foreach_foreach_response_3').get('hourlyRate') or {}).get('currency') or {}).get('name'),
                "daydiff": int((datetime.utcnow() - (
                    datetime(**rail.result('foreach_foreach_response_3')['effectiveDate'])
                    if rail.result('foreach_foreach_response_3').get('effectiveDate')
                    else datetime(2000, 1, 1)
                )).days)
            }
        )

        insert_to_list_7 = rail.SetVariableOperator(
            task_id='insert_to_list_7',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "hourlyRate": {
                    "amount": "{{ result('foreach_foreach_response_3').hourlyRate.amount }}",
                    "currency": {
                        "uri": "{{ result('foreach_foreach_response_3').hourlyRate.currency.uri }}",
                        "name": null,
                        "symbol": null
                    }
                },
                "effectiveDate": {
                    "year": "{{ result('foreach_foreach_response_3').effectiveDate.year }}",
                    "month": "{{ result('foreach_foreach_response_3').effectiveDate.month }}",
                    "day": "{{ result('foreach_foreach_response_3').effectiveDate.day }}"
                }
            }
        )

        log_initial_hourly_rate_1 = rail.PythonOperator(
            task_id='log_initial_hourly_rate_1',
            python_callable=lambda:  {
                "amount": (rail.result('foreach_foreach_response_3').get('hourlyRate') or {}).get('amount'),
                "currency": {
                    "uri": ((rail.result('foreach_foreach_response_3').get('hourlyRate') or {}).get('currency') or {}).get('uri'),
                    "name": null,
                    "symbol": null
                }
            }
        )

        log_initial_hourly_rate_2 = rail.PythonOperator(
            task_id='log_initial_hourly_rate_2',
            python_callable=lambda: (rail.result('foreach_foreach_response_3').get('hourlyRate') or {}).get('amount')
        )

        foreach_foreach_response_196_197_end = rail.EmptyOperator(
            task_id='foreach_foreach_response_196_197_end',
        )

        foreach_response_196_end = rail.EmptyOperator(
            task_id='foreach_response_196_end',
        )

        def min_daydiff_206():
            return min(item['daydiff'] for item in rail.get_dag_run_var('payroll_rate_schedule')) if rail.get_dag_run_var('payroll_rate_schedule') else ""

        log_getthemindaydiff = rail.PythonOperator(
            task_id='log_getthemindaydiff',
            python_callable=min_daydiff_206
        )

        log_get_employee_hourly_cost = rail.PythonOperator(
            task_id='log_get_employee_hourly_cost',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var('payroll_rate_schedule') or [], 'daydiff', rail.result(
                'log_getthemindaydiff'), 'rate', "") if rail.result('log_getthemindaydiff') else rail.result('log_initial_hourly_rate_2')
        )

        if_hourly_rate_changed = rail.IfOperator(
            task_id='if_hourly_rate_changed',
            test=lambda dag_run: bool(dag_run.conf.get("hourlypayrollrate", "").strip()) and
                float(dag_run.conf["hourlypayrollrate"]) != float(
                    rail.result('log_get_employee_hourly_cost') or 0),
            yes_task="if_hourlypayrollcurrency_present",
            no_task="mapper_search_entries",
        )

        if_hourlypayrollcurrency_present = rail.IfOperator(
            task_id='if_hourlypayrollcurrency_present',
            test='''{{ dag_run.conf["hourlypayrollcurrency"] | is_truthy }}''',
            yes_task="get_all_currencies",
            no_task="get_base_currencies",
        )

        get_all_currencies = rail.RepliconServiceOperator(
            task_id='get_all_currencies',
            endpoint="/services/CurrencyService2.svc/GetAllCurrencies",
            data=None
        )

        log_get_currency_uri_1 = rail.PythonOperator(
            task_id='log_get_currency_uri_1',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result('get_all_currencies'),
                                                                                 'name',
                                                                                 dag_run.conf["hourlypayrollcurrency"],
                                                                                 'uri')
        )

        if_get_currency_uri_present_1 = rail.IfOperator(
            task_id='if_get_currency_uri_present_1',
            test='''{{ result('log_get_currency_uri_1') | is_truthy }}''',
            yes_task="insert_to_list_8",
            no_task="mapper_search_entries",
        )

        insert_to_list_8 = rail.SetVariableOperator(
            task_id='insert_to_list_8',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "hourlyRate": {
                    "amount": '{{ dag_run.conf["hourlypayrollrate"] }}',
                    "currency": {
                        "uri": "{{ result('log_get_currency_uri_1') }}",
                        "name": null,
                        "symbol": null
                    }
                },
                "effectiveDate": {
                    "year": "{{ result('get_today').year }}",
                    "month": "{{ result('get_today').month }}",
                    "day": "{{ result('get_today').day }}"
                }
            }
        )

        log_checkforadditionalschedule_1 = rail.PythonOperator(
            task_id='log_checkforadditionalschedule_1',
            python_callable=lambda: _process_schedule_entries(json.loads(
                json.dumps(rail.get_dag_run_var(rail.result('declare_list_3')['name']) or [])
                .replace('"effectiveDate": {}', '"effectiveDate": null')
            ))
        )

        put_user_payroll_rate_schedule_1 = rail.RepliconServiceOperator(
            task_id='put_user_payroll_rate_schedule_1',
            endpoint="/services/PayrollService1.svc/PutUserPayrollRateSchedule",
            data=lambda dag_run: {
                "userUri": dag_run.conf["useruri"],
                "schedule": {
                    "initialHourlyRate": rail.result('log_initial_hourly_rate_1'),
                    "scheduleEntries": rail.result('log_checkforadditionalschedule_1')
                }
            }
        )

        get_base_currencies = rail.RepliconServiceOperator(
            task_id='get_base_currencies',
            endpoint="/services/CurrencyService2.svc/GetBaseCurrency",
            data=None
        )

        log_get_currency_uri_2 = rail.PythonOperator(
            task_id='log_get_currency_uri_2',
            python_callable=lambda: rail.result('get_base_currencies')['uri']
        )

        if_get_currency_uri_present_2 = rail.IfOperator(
            task_id='if_get_currency_uri_present_2',
            test='''{{ result('log_get_currency_uri_2') | is_truthy }}''',
            yes_task="insert_to_list_9",
            no_task="mapper_search_entries",
        )

        insert_to_list_9 = rail.SetVariableOperator(
            task_id='insert_to_list_9',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value={
                "hourlyRate": {
                    "amount": '{{ dag_run.conf["hourlypayrollrate"] }}',
                    "currency": {
                        "uri": "{{ result('log_get_currency_uri_2') }}",
                        "name": null,
                        "symbol": null
                    }
                },
                "effectiveDate": {
                    "year": "{{ result('get_today').year }}",
                    "month": "{{ result('get_today').month }}",
                    "day": "{{ result('get_today').day }}"
                }
            }
        )

        log_checkforadditionalschedule_2 = rail.PythonOperator(
            task_id='log_checkforadditionalschedule_2',
            python_callable=lambda: _process_schedule_entries(json.loads(
                json.dumps(rail.get_dag_run_var(rail.result('declare_list_3')['name']) or [])
                .replace('"effectiveDate": {}', '"effectiveDate": null')
            ))
        )

        put_user_payroll_rate_schedule_2 = rail.RepliconServiceOperator(
            task_id='put_user_payroll_rate_schedule_2',
            endpoint="/services/PayrollService1.svc/PutUserPayrollRateSchedule",
            data=lambda dag_run: {
                "userUri": dag_run.conf["useruri"],
                "schedule": {
                    "initialHourlyRate": rail.result('log_initial_hourly_rate_1'),
                    "scheduleEntries": rail.result('log_checkforadditionalschedule_2')
                }
            }
        )

        mapper_search_entries = rail.PythonOperator(
            task_id='mapper_search_entries',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["location"] == dag_run.conf["location"], ascend_master_mapper_file))
        )

        if_entry_col2_present = rail.IfOperator(
            task_id='if_entry_col2_present',
            test='''{{ result('mapper_search_entries') | is_truthy }}''',
            yes_task="if_declare_variable_2_value_eq_yes",
            no_task="log_detailsnotavailableinmapperfile",
        )

        if_declare_variable_2_value_eq_yes = rail.IfOperator(
            task_id='if_declare_variable_2_value_eq_yes',
            test= lambda: (rail.get_dag_run_var('locationandemployeetypebasedchange') or '').lower() == 'yes',
            yes_task="declare_effectivedate_var",
            no_task="if_rehire_log_present",
        )

        declare_effectivedate_var = rail.SetVariableOperator(
            task_id='declare_effectivedate_var',
            append=False,
            name='effectivedate',
            value=None
        )

        log_pluckif_pay_ruleispresent = rail.PythonOperator(
            task_id='log_pluckif_pay_ruleispresent',
            python_callable=lambda: next(
                (x['value'] for x in (rail.result('mapper_search_entries') or [])
                 if x["type"] == "Payrule Name" and x["employee_type"] == "All"),
                None
            )
        )

        if_payruleispresent_present = rail.IfOperator(
            task_id='if_payruleispresent_present',
            test='''{{ result('log_pluckif_pay_ruleispresent') | is_truthy }}''',
            yes_task="foreach_foreach_response_4",
            no_task="log_pluckif_activityispresent",
        )

        foreach_foreach_response_4 = rail.ForEachOperator(
            task_id='foreach_foreach_response_4',
            items="{{ result('bulk_get_users3')[0].payRuleScriptSchedule | to_json }}",
            start_task='if_effectivedate_day_present_2',
            end_task='foreach_foreach_response_229_230_end'
        )

        if_effectivedate_day_present_2 = rail.IfOperator(
            task_id='if_effectivedate_day_present_2',
            test='''{{ result('foreach_foreach_response_4').effectiveDate | is_truthy and result('foreach_foreach_response_4').effectiveDate.day | is_truthy }}''',
            yes_task="update_variable_6",
            no_task="if_value_to_date_less_than_today",
        )

        update_variable_6 = rail.SetVariableOperator(
            task_id='update_variable_6',
            append=False,
            name='{{ result("declare_effectivedate_var").name }}',
            value=lambda: rail.result('foreach_foreach_response_4')[
                'effectiveDate']
        )

        if_value_to_date_less_than_today = rail.IfOperator(
            task_id='if_value_to_date_less_than_today',
            # pylint: disable=unnecessary-lambda
            test=lambda: (
                rail.result('foreach_foreach_response_4')['effectiveDate'] is None
                or bool(datetime(**rail.result('foreach_foreach_response_4')['effectiveDate']) <= datetime.utcnow())
            ),
            yes_task="accumulate_list_items_3",
            no_task="foreach_foreach_response_229_230_end",
        )

        accumulate_list_items_3 = rail.SetVariableOperator(
            task_id='accumulate_list_items_3',
            name='payrule_schedule',
            append=True,
            value=lambda: {
                "name": rail.result('foreach_foreach_response_4')['payRuleScript']['displayText'],
                "effectivedate": rail.get_dag_run_var('effectivedate'),
                "uri": rail.result('foreach_foreach_response_4')['payRuleScript']['uri'],
                "daydiff": int((datetime.utcnow() - (
                    datetime(**rail.result('foreach_foreach_response_4')['effectiveDate'])
                    if rail.result('foreach_foreach_response_4')['effectiveDate']
                    else datetime(2000, 1, 1)
                )).days)
            }
        )

        foreach_foreach_response_229_230_end = rail.EmptyOperator(
            task_id='foreach_foreach_response_229_230_end',
        )


        log_min_day_diff_1 = rail.PythonOperator(
            task_id='log_min_day_diff_1',
            python_callable=lambda: min(
                (x['daydiff'] for x in rail.get_dag_run_var('payrule_schedule') or []),
                default=None
            )
        )

        log_current_payrule = rail.PythonOperator(
            task_id='log_current_payrule',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.get_dag_run_var('payrule_schedule') or [],
                'daydiff', rail.result('log_min_day_diff_1'), 'name'
            ) if rail.result('log_min_day_diff_1') is not None else None
        )

        if_payrule_changed = rail.IfOperator(
            task_id='if_payrule_changed',
            test='''{{ result('log_pluckif_pay_ruleispresent') != result('log_current_payrule') }}''',
            yes_task="get_all_payrule_scripts",
            no_task="log_pluckif_activityispresent",
        )

        get_all_payrule_scripts = rail.RepliconServiceOperator(
            task_id='get_all_payrule_scripts',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
            data=None
        )

        declare_list_4 = rail.SetVariableOperator(
            task_id='declare_list_4',
            append=False,
            name='payroll_details',
            value=[]
        )

        log_get_pay_rule_script_uri = rail.PythonOperator(
            task_id='log_get_pay_rule_script_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_payrule_scripts'), 'displayText', rail.result('log_pluckif_pay_ruleispresent'), 'uri')
        )

        foreach_foreach_response_5 = rail.ForEachOperator(
            task_id='foreach_foreach_response_5',
            items="{{ result('bulk_get_users3')[0].payRuleScriptSchedule | to_json }}",
            start_task='if_effectivedate_day_blank_3',
            end_task='foreach_foreach_response_241_242_end'
        )

        if_effectivedate_day_blank_3 = rail.IfOperator(
            task_id='if_effectivedate_day_blank_3',
            test='''{{ result('foreach_foreach_response_5').effectiveDate | is_falsy or result('foreach_foreach_response_5').effectiveDate.day | is_falsy }}''',
            yes_task="insert_to_list_10",
            no_task="if_effectivedate_day_present_3",
        )

        insert_to_list_10 = rail.SetVariableOperator(
            task_id='insert_to_list_10',
            append=True,
            name='{{ result("declare_list_4").name }}',
            value={
                "payRuleScript": {
                    "uri": "{{ result('foreach_foreach_response_5').payRuleScript.uri }}",
                    "name": null
                },
                "effectiveDate": {
                    "year": "skip",
                    "month": "skip",
                    "day": "skip"
                }
            }
        )

        if_effectivedate_day_present_3 = rail.IfOperator(
            task_id='if_effectivedate_day_present_3',
            test='''{{ result('foreach_foreach_response_5').effectiveDate | is_truthy and result('foreach_foreach_response_5').effectiveDate.day | is_truthy }}''',
            yes_task="log_effective_date_4",
            no_task="foreach_foreach_response_241_242_end",
        )

        log_effective_date_4 = rail.PythonOperator(
            task_id='log_effective_date_4',
            python_callable=lambda: rail.result('foreach_foreach_response_5')['effectiveDate']
        )

        if_startdate_changed_2 = rail.IfOperator(
            task_id='if_startdate_changed_2',
            test=lambda: datetime(**rail.result('log_effective_date_4')) < datetime.utcnow(),
            yes_task="insert_to_list_11",
            no_task="foreach_foreach_response_241_242_end",
        )

        insert_to_list_11 = rail.SetVariableOperator(
            task_id='insert_to_list_11',
            append=True,
            name='{{ result("declare_list_4").name }}',
            value={
                "payRuleScript": {
                    "uri": "{{ result('foreach_foreach_response_5').payRuleScript.uri }}",
                    "name": null
                },
                "effectiveDate": {
                    "year": "{{ result('foreach_foreach_response_5').effectiveDate.year }}",
                    "month": "{{ result('foreach_foreach_response_5').effectiveDate.month }}",
                    "day": "{{ result('foreach_foreach_response_5').effectiveDate.day }}"
                }
            }
        )

        foreach_foreach_response_241_242_end = rail.EmptyOperator(
            task_id='foreach_foreach_response_241_242_end',
        )

        if_declare_list_239_list_items_lt = rail.IfOperator(
            task_id='if_declare_list_239_list_items_lt',
            test=lambda: len(rail.get_dag_run_var('payroll_details') or []) < 1,
            yes_task="if_get_pay_rule_script_uri_240_present_enabled",
            no_task="if_location_change_done_present",
        )

        if_get_pay_rule_script_uri_240_present_enabled = rail.IfOperator(
            task_id='if_get_pay_rule_script_uri_240_present_enabled',
            test='''{{ result('log_get_pay_rule_script_uri') | is_truthy }}''',
            yes_task="put_payroll_assignment_1",
            no_task="if_location_change_done_present",
        )

        put_payroll_assignment_1 = rail.RepliconServiceOperator(
            task_id='put_payroll_assignment_1',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data={
                    "userUri": '{{ dag_run.conf["useruri"] }}',
                    "scheduleEntries": [
                        {
                            "payRuleScript": {
                                "uri": "{{ result('log_get_pay_rule_script_uri') }}",
                                "name": null
                            },
                            "effectiveDate": null
                        }
                    ]
            }
        )

        if_location_change_done_present = rail.IfOperator(
            task_id='if_location_change_done_present',
            test='''{{ result('log_location_change_done') | is_truthy  or result('log_location_assignment_done') | is_truthy }}''',
            yes_task="if_get_pay_rule_script_uri_present",
            no_task="foreach_response_241_end",
        )

        if_get_pay_rule_script_uri_present = rail.IfOperator(
            task_id='if_get_pay_rule_script_uri_present',
            test='''{{ result('log_get_pay_rule_script_uri') | is_truthy }}''',
            yes_task="insert_to_list_12",
            no_task="foreach_response_241_end",
        )

        insert_to_list_12 = rail.SetVariableOperator(
            task_id='insert_to_list_12',
            append=True,
            name='{{ result("declare_list_4").name }}',
            value={
                "payRuleScript": {
                    "uri": "{{ result('log_get_pay_rule_script_uri') }}",
                    "name": null
                },
                "effectiveDate": {
                    "year": "{{ result('get_today').year }}",
                    "month": "{{ result('get_today').month }}",
                    "day": "{{ result('get_today').day }}"
                }
            }
        )

        log_get_existing_payrule_schedule = rail.PythonOperator(
            task_id='log_get_existing_payrule_schedule',
            python_callable=lambda: _process_schedule_entries(json.loads(
                json.dumps(rail.get_dag_run_var(rail.result('declare_list_4')['name']) or [])
                .replace('"effectiveDate": {}', '"effectiveDate": null')
            ))
        )

        put_payroll_assignment_2 = rail.RepliconServiceOperator(
            task_id='put_payroll_assignment_2',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf["useruri"],
                "scheduleEntries": rail.result('log_get_existing_payrule_schedule')
            }
        )

        foreach_response_241_end = rail.EmptyOperator(
            task_id='foreach_response_241_end',
        )

        log_pluckif_activityispresent = rail.PythonOperator(
            task_id='log_pluckif_activityispresent',
            python_callable=lambda: next(
                (x['value'] for x in (rail.result('mapper_search_entries') or []) if x["type"] == "Activity"),
                None
            )
        )

        get_activity_assignments_for_user = rail.RepliconServiceOperator(
            task_id='get_activity_assignments_for_user',
            endpoint="/services/ActivityService1.svc/GetActivityAssignmentsForUser",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}'
            }
        )

        log_checkif_business_tripisassigned = rail.PythonOperator(
            task_id='log_checkif_business_tripisassigned',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_activity_assignments_for_user'), 'displayText', 'Business Trip', 'uri')
        )

        if_business_tripisassigned_present = rail.IfOperator(
            task_id='if_business_tripisassigned_present',
            test='''{{ result('log_checkif_business_tripisassigned') | is_truthy }}''',
            yes_task="if_activityispresent_present",
            no_task="if_activityispresent_present",
        )

        if_activityispresent_present = rail.IfOperator(
            task_id='if_activityispresent_present',
            test='''{{ result('log_pluckif_activityispresent') | is_truthy }}''',
            yes_task="get_enabled_activities",
            no_task="if_activityispresent_258_blank_1",
        )

        get_enabled_activities = rail.RepliconServiceOperator(
            task_id='get_enabled_activities',
            endpoint="/services/ActivityService1.svc/GetEnabledActivities",
            data=None
        )

        log_activitiestobeassigned = rail.PythonOperator(
            task_id='log_activitiestobeassigned',
            python_callable=lambda: rail.result(
                'log_pluckif_activityispresent').split("|")
        )

        # Replaced: declare_list_5 + insert_to_list_13 + create_list_1
        #           + foreach_create_list_1 + insert_to_list_14 + foreach_create_list_267_268_end
        # Builds activity URI list directly, including Business Trip if previously assigned
        log_activity_uristobeassigned = rail.PythonOperator(
            task_id='log_activity_uristobeassigned',
            python_callable=lambda: list(filter(None, [
                rail.result('log_checkif_business_tripisassigned') or None,
                *[
                    rail.find_first_by_attr_and_get_attr(
                        rail.result('get_enabled_activities'), 'displayText', name, 'uri'
                    )
                    for name in rail.result('log_activitiestobeassigned')
                ]
            ])) or None
        )

        if_activity_uristobeassigned_present = rail.IfOperator(
            task_id='if_activity_uristobeassigned_present',
            test='''{{ result('log_activity_uristobeassigned') | is_truthy }}''',
            yes_task="put_activity_assignments_for_user_1",
            no_task="if_activityispresent_258_blank_1",
        )

        put_activity_assignments_for_user_1 = rail.RepliconServiceOperator(
            task_id='put_activity_assignments_for_user_1',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf["useruri"],
                "activityUris": rail.result('log_activity_uristobeassigned')
            }
        )

        if_activityispresent_258_blank_1 = rail.IfOperator(
            task_id='if_activityispresent_258_blank_1',
            test='''{{ result('log_pluckif_activityispresent') | is_falsy  and result('log_checkif_business_tripisassigned') | is_falsy }}''',
            yes_task="put_activity_assignments_for_user_2",
            no_task="if_activityispresent_258_blank_2",
        )

        put_activity_assignments_for_user_2 = rail.RepliconServiceOperator(
            task_id='put_activity_assignments_for_user_2',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "activityUris": []
            }
        )

        if_activityispresent_258_blank_2 = rail.IfOperator(
            task_id='if_activityispresent_258_blank_2',
            test='''{{ result('log_pluckif_activityispresent') | is_falsy  and result('log_checkif_business_tripisassigned') | is_truthy }}''',
            yes_task="put_activity_assignments_for_user_3",
            no_task="log_pluckiftimesheetapprovalpathispresent",
        )

        put_activity_assignments_for_user_3 = rail.RepliconServiceOperator(
            task_id='put_activity_assignments_for_user_3',
            endpoint="/services/ActivityService1.svc/PutActivityAssignmentsForUser",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "activityUris": ["{{ result('log_checkif_business_tripisassigned') }}"]
            }
        )

        log_pluckiftimesheetapprovalpathispresent = rail.PythonOperator(
            task_id='log_pluckiftimesheetapprovalpathispresent',
            python_callable=lambda: next(
                (x['value'] for x in (rail.result('mapper_search_entries') or [])
                 if x["type"] == "Timesheet Approval Path" and x["employee_type"] == "All"),
                None
            )
        )

        if_timesheetapprovalpathispresent_present = rail.IfOperator(
            task_id='if_timesheetapprovalpathispresent_present',
            test='''{{ result('log_pluckiftimesheetapprovalpathispresent') | is_truthy  and result('log_pluckiftimesheetapprovalpathispresent') != result('bulk_get_users3')[0].timesheetApprovalPath.displayText }}''',
            yes_task="get_all_timesheet_approval_paths",
            no_task="log_pluckiftimesheettemplateispresent",
        )

        get_all_timesheet_approval_paths = rail.RepliconServiceOperator(
            task_id='get_all_timesheet_approval_paths',
            endpoint="/services/TimesheetApprovalService1.svc/GetAllApprovalPaths",
            data=None
        )

        log_timesheetapprovalpathuri = rail.PythonOperator(
            task_id='log_timesheetapprovalpathuri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_timesheet_approval_paths'), 'displayText', rail.result('log_pluckiftimesheetapprovalpathispresent'), 'uri')
        )

        if_timesheetapprovalpathuri_present = rail.IfOperator(
            task_id='if_timesheetapprovalpathuri_present',
            test='''{{ result('log_timesheetapprovalpathuri') | is_truthy }}''',
            yes_task="update_approval_path_for_userfortimesheet",
            no_task="log_pluckiftimesheettemplateispresent",
        )

        update_approval_path_for_userfortimesheet = rail.RepliconServiceOperator(
            task_id='update_approval_path_for_userfortimesheet',
            endpoint="/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "approvalPathUri": "{{ result('log_timesheetapprovalpathuri') }}"
            }
        )

        log_pluckiftimesheettemplateispresent = rail.PythonOperator(
            task_id='log_pluckiftimesheettemplateispresent',
            python_callable=lambda dag_run: next(
                (x['value'] for x in (rail.result('mapper_search_entries') or [])
                 if x["type"] == "Timesheet Template" and x["employee_type"] == dag_run.conf["employeetype"]),
                None
            )
        )

        log_pluckifpunchentrypolicyispresent = rail.PythonOperator(
            task_id='log_pluckifpunchentrypolicyispresent',
            python_callable=lambda dag_run: next(
                (x['value'] for x in (rail.result('mapper_search_entries') or [])
                 if x["type"] == "Punch Entry Policy" and x["employee_type"] == dag_run.conf["employeetype"]),
                None
            )
        )

        log_pluckiftimeofftemplateispresent = rail.PythonOperator(
            task_id='log_pluckiftimeofftemplateispresent',
            python_callable=lambda: next(
                (x['value'] for x in (rail.result('mapper_search_entries') or [])
                 if x["type"] == "TimeOff Template" and x["employee_type"] == "All"),
                None
            )
        )

        if_timesheettemplateispresent_285_blank = rail.IfOperator(
            task_id='if_timesheettemplateispresent_285_blank',
            test='''{{ result('log_pluckiftimesheettemplateispresent') | is_falsy  and result('log_pluckifpunchentrypolicyispresent') | is_falsy  and result('log_pluckiftimeofftemplateispresent') | is_falsy }}''',
            yes_task="put_policy_set_assignments_for_user_1",
            no_task="if_timesheettemplateispresent_present",
        )

        put_policy_set_assignments_for_user_1 = rail.RepliconServiceOperator(
            task_id='put_policy_set_assignments_for_user_1',
            endpoint="/services/PolicySetService1.svc/PutPolicySetAssignmentsForUser",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "policySetUris": []
            }
        )

        if_timesheettemplateispresent_present = rail.IfOperator(
            task_id='if_timesheettemplateispresent_present',
            test='''{{ result('log_pluckiftimesheettemplateispresent') | is_truthy  or result('log_pluckifpunchentrypolicyispresent') | is_truthy  or result('log_pluckiftimeofftemplateispresent') | is_truthy }}''',
            yes_task="get_all_policysets",
            no_task="log_pluckiftimesoffapprovalpathispresent",
        )

        def get_required_policysets(response):
            policysets_uris = []

            timesheet_template_uri = rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('log_pluckiftimesheettemplateispresent'), 'uri', '')
            if timesheet_template_uri:
                policysets_uris.append(timesheet_template_uri)

            punchentrypolicy_uri = rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('log_pluckifpunchentrypolicyispresent'), 'uri', '')
            if punchentrypolicy_uri:
                policysets_uris.append(punchentrypolicy_uri)

            timeoff_template_uri = rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('log_pluckiftimeofftemplateispresent'), 'uri', '')
            if timeoff_template_uri:
                policysets_uris.append(timeoff_template_uri)

            return policysets_uris

        get_all_policysets = rail.RepliconServiceOperator(
            task_id='get_all_policysets',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
            data_handler=get_required_policysets
        )

        if_policysetstoassign_present = rail.IfOperator(
            task_id='if_policysetstoassign_present',
            test='''{{ result('get_all_policysets') | length > 0  }}''',
            yes_task="put_policy_set_assignments_for_user_2",
            no_task="log_pluckiftimesoffapprovalpathispresent",
        )

        put_policy_set_assignments_for_user_2 = rail.RepliconServiceOperator(
            task_id='put_policy_set_assignments_for_user_2',
            endpoint="/services/PolicySetService1.svc/PutPolicySetAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf["useruri"],
                "policySetUris": rail.result('get_all_policysets')
            }
        )

        log_pluckiftimesoffapprovalpathispresent = rail.PythonOperator(
            task_id='log_pluckiftimesoffapprovalpathispresent',
            python_callable=lambda: next(
                (x['value'] for x in (rail.result('mapper_search_entries') or [])
                 if x["type"] == "TimeOff Approval Path" and x["employee_type"] == "All"),
                None
            )
        )

        if_timeoffapprovalpathispresent_present = rail.IfOperator(
            task_id='if_timeoffapprovalpathispresent_present',
            test='''{{ result('log_pluckiftimesoffapprovalpathispresent') | is_truthy  and result('log_pluckiftimesoffapprovalpathispresent') != result('bulk_get_users3')[0].timeOffApprovalPath.displayText }}''',
            yes_task="get_all_timeoff_approval_paths",
            no_task="log_pluckifscheduleispresent",
        )

        get_all_timeoff_approval_paths = rail.RepliconServiceOperator(
            task_id='get_all_timeoff_approval_paths',
            endpoint="/services/TimeOffApprovalService1.svc/GetAllApprovalPaths",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('log_pluckiftimesoffapprovalpathispresent'), 'uri', '')
        )

        if_timeoffapprovalpathuri_present = rail.IfOperator(
            task_id='if_timeoffapprovalpathuri_present',
            test='''{{ result('get_all_timeoff_approval_paths') | is_truthy }}''',
            yes_task="update_approval_path_for_userfortimeoff",
            no_task="log_pluckifscheduleispresent",
        )

        update_approval_path_for_userfortimeoff = rail.RepliconServiceOperator(
            task_id='update_approval_path_for_userfortimeoff',
            endpoint="/services/TimeOffApprovalService1.svc/UpdateApprovalPathForUser",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "approvalPathUri": "{{ result('get_all_timeoff_approval_paths') }}"
            }
        )

        log_pluckifscheduleispresent = rail.PythonOperator(
            task_id='log_pluckifscheduleispresent',
            python_callable=lambda: next(
                (x['value'] for x in (rail.result('mapper_search_entries') or [])
                 if x["type"] == "Schedule Name" and x["employee_type"] == "All"),
                None
            )
        )

        if_scheduleispresent_present = rail.IfOperator(
            task_id='if_scheduleispresent_present',
            test='''{{ result('log_pluckifscheduleispresent') | is_truthy }}''',
            yes_task="foreach_foreach_response_6",
            no_task="log_pluckifworkweekispresent",
        )

        foreach_foreach_response_6 = rail.ForEachOperator(
            task_id='foreach_foreach_response_6',
            items="{{ result('bulk_get_users3')[0].schedulePolicies | to_json }}",
            start_task='if_effectivedate_day_present_4',
            end_task='foreach_foreach_response_306_307_end'
        )

        if_effectivedate_day_present_4 = rail.IfOperator(
            task_id='if_effectivedate_day_present_4',
            test='''{{ result('foreach_foreach_response_6').effectiveDate | is_truthy and result('foreach_foreach_response_6').effectiveDate.day | is_truthy }}''',
            yes_task="log_schedule_effective_date_1",
            no_task="if_effectivedate_day_blank_4",
        )

        log_schedule_effective_date_1 = rail.PythonOperator(
            task_id='log_schedule_effective_date_1',
            python_callable=lambda: rail.result(
                'foreach_foreach_response_6')['effectiveDate']
        )

        if_effectivedate_day_blank_4 = rail.IfOperator(
            task_id='if_effectivedate_day_blank_4',
            test='''{{ result('foreach_foreach_response_6').effectiveDate | is_falsy or result('foreach_foreach_response_6').effectiveDate.day | is_falsy }}''',
            yes_task="log_schedule_effective_date_2",
            no_task="log_schedule_effective_date_3",
        )

        log_schedule_effective_date_2 = rail.PythonOperator(
            task_id='log_schedule_effective_date_2',
            python_callable=lambda: rail.result('bulk_get_users3')[
                0]['userDetails']['employmentDateRange']['startDate']
        )

        log_schedule_effective_date_3 = rail.PythonOperator(
            task_id='log_schedule_effective_date_3',
            python_callable=lambda:  rail.result('log_schedule_effective_date_1') if rail.result(
                'log_schedule_effective_date_1') else rail.result('log_schedule_effective_date_2')
        )

        if_to_date_less_than_today = rail.IfOperator(
            task_id='if_to_date_less_than_today',
            test=lambda: (
                rail.result('log_schedule_effective_date_3') is None
                or bool(datetime(**rail.result('log_schedule_effective_date_3')) <= datetime.utcnow())
            ),
            yes_task="accumulate_list_items_4",
            no_task="foreach_foreach_response_306_307_end",
        )

        accumulate_list_items_4 = rail.SetVariableOperator(
            task_id='accumulate_list_items_4',
            name='office_schedule',
            append=True,
            value=lambda: {
                "name": ((rail.result('foreach_foreach_response_6').get('officeSchedule') or {}).get('displayText')) or "Shift Schedule",
                "effectivedate": rail.result('log_schedule_effective_date_3'),
                "uri": (rail.result('foreach_foreach_response_6').get('officeSchedule') or {}).get('uri'),
                "daydiff": int((datetime.utcnow() - (
                    datetime(**rail.result('log_schedule_effective_date_3'))
                    if rail.result('log_schedule_effective_date_3')
                    else datetime(2000, 1, 1)
                )).days)
            }
        )

        foreach_foreach_response_306_307_end = rail.EmptyOperator(
            task_id='foreach_foreach_response_306_307_end',
        )


        def min_daydiff_315():
            return min(item['daydiff'] for item in rail.get_dag_run_var('office_schedule')) if rail.get_dag_run_var('office_schedule') else ""

        log_min_day_diff_2 = rail.PythonOperator(
            task_id='log_min_day_diff_2',
            python_callable=min_daydiff_315
        )

        log_current_office_schedule = rail.PythonOperator(
            task_id='log_current_office_schedule',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var('office_schedule') or [], 'daydiff', rail.result(
                'log_min_day_diff_2'), 'name', "") if rail.result('log_min_day_diff_2') else null
        )

        if_schedule_changed = rail.IfOperator(
            task_id='if_schedule_changed',
            test='''{{ result('log_pluckifscheduleispresent') != result('log_current_office_schedule') }}''',
            yes_task="get_all_office_schedules",
            no_task="log_pluckifworkweekispresent",
        )

        get_all_office_schedules = rail.RepliconServiceOperator(
            task_id='get_all_office_schedules',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
            data=None
        )

        log_office_schedule_uri = rail.PythonOperator(
            task_id='log_office_schedule_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_all_office_schedules'), 'displayText', rail.result(
                'log_pluckifscheduleispresent'), 'uri', "")
        )

        declare_list_6 = rail.SetVariableOperator(
            task_id='declare_list_6',
            name='schedule_entries',
            value=[]
        )

        foreach_foreach_response_7 = rail.ForEachOperator(
            task_id='foreach_foreach_response_7',
            items="{{ result('bulk_get_users3')[0].schedulePolicies | to_json }}",
            start_task='if_schedule_is_shift',
            end_task='foreach_foreach_response_321_322_end'
        )

        if_schedule_is_shift = rail.IfOperator(
            task_id='if_schedule_is_shift',
            test='''{{ result('foreach_foreach_response_7').scheduleTypeUri | matches('shift') }}''',
            yes_task="if_effectivedate_day_blank_5",
            no_task="if_schedule_is_officeschedule",
        )

        if_effectivedate_day_blank_5 = rail.IfOperator(
            task_id='if_effectivedate_day_blank_5',
            test='''{{ result('foreach_foreach_response_7').effectiveDate | is_falsy or result('foreach_foreach_response_7').effectiveDate.day | is_falsy }}''',
            yes_task="insert_to_list_15",
            no_task="insert_to_list_16",
        )

        insert_to_list_15 = rail.SetVariableOperator(
            task_id='insert_to_list_15',
            append=True,
            name='{{ result("declare_list_6").name }}',
            value={
                "schedulePolicy": {
                    "officeScheduleUri": null,
                    "name": null,
                    "officeSchedule": null,
                    "scheduleTypeUri": "{{ result('foreach_foreach_response_7').scheduleTypeUri }}"
                },
                "effectiveDate": null
            }
        )

        insert_to_list_16 = rail.SetVariableOperator(
            task_id='insert_to_list_16',
            append=True,
            name='{{ result("declare_list_6").name }}',
            value={
                "schedulePolicy": {
                    "officeScheduleUri": null,
                    "name": null,
                    "officeSchedule": null,
                    "scheduleTypeUri": "{{ result('foreach_foreach_response_7').scheduleTypeUri }}"
                },
                "effectiveDate": {
                    "year": "{{ result('foreach_foreach_response_7').effectiveDate.year }}",
                    "month": "{{ result('foreach_foreach_response_7').effectiveDate.month }}",
                    "day": "{{ result('foreach_foreach_response_7').effectiveDate.day }}"
                }
            }
        )

        if_schedule_is_officeschedule = rail.IfOperator(
            task_id='if_schedule_is_officeschedule',
            test='''{{ result('foreach_foreach_response_7').scheduleTypeUri | matches('office-schedule') }}''',
            yes_task="if_effectivedate_day_blank_6",
            no_task="foreach_foreach_response_321_322_end",
        )

        if_effectivedate_day_blank_6 = rail.IfOperator(
            task_id='if_effectivedate_day_blank_6',
            test='''{{ result('foreach_foreach_response_7').effectiveDate | is_falsy or result('foreach_foreach_response_7').effectiveDate.day | is_falsy }}''',
            yes_task="insert_to_list_17",
            no_task="insert_to_list_18",
        )

        insert_to_list_17 = rail.SetVariableOperator(
            task_id='insert_to_list_17',
            append=True,
            name='{{ result("declare_list_6").name }}',
            value={
                "schedulePolicy": {
                    "officeScheduleUri": "{{ result('foreach_foreach_response_7').officeSchedule.uri }}",
                    "name": null,
                    "officeSchedule": null,
                    "scheduleTypeUri": "{{ result('foreach_foreach_response_7').scheduleTypeUri }}"
                },
                "effectiveDate": {}
            }
        )

        insert_to_list_18 = rail.SetVariableOperator(
            task_id='insert_to_list_18',
            append=True,
            name='{{ result("declare_list_6").name }}',
            value={
                "schedulePolicy": {
                    "officeScheduleUri": "{{ result('foreach_foreach_response_7').officeSchedule.uri }}",
                    "name": null,
                    "officeSchedule": null,
                    "scheduleTypeUri": "{{ result('foreach_foreach_response_7').scheduleTypeUri }}"
                },
                "effectiveDate": {
                    "year": "{{ result('foreach_foreach_response_7').effectiveDate.year }}",
                    "month": "{{ result('foreach_foreach_response_7').effectiveDate.month }}",
                    "day": "{{ result('foreach_foreach_response_7').effectiveDate.day }}"
                }
            }
        )

        foreach_foreach_response_321_322_end = rail.EmptyOperator(
            task_id='foreach_foreach_response_321_322_end',
        )


        if_declare_list_320_list_items_lt = rail.IfOperator(
            task_id='if_declare_list_320_list_items_lt',
            test=lambda: bool(
                len(rail.get_dag_run_var("schedule_entries") or []) < 1),
            yes_task="if_not_shift_schedule",
            no_task="if_office_schedule_uri_present",
        )

        if_not_shift_schedule = rail.IfOperator(
            task_id='if_not_shift_schedule',
            test='''{{ result('log_pluckifscheduleispresent') | matches('Shift Schedule') | is_falsy  and result('log_office_schedule_uri') | is_truthy }}''',
            yes_task="put_schedule_policy_1",
            no_task="if_scheduleispresent_304_contains_shiftschedule_1",
        )

        put_schedule_policy_1 = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_1',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "scheduleEntries": [
                    {
                        "schedulePolicy": {
                            "officeScheduleUri": "{{ result('log_office_schedule_uri') }}",
                            "name": null,
                            "officeSchedule": null,
                            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        if_scheduleispresent_304_contains_shiftschedule_1 = rail.IfOperator(
            task_id='if_scheduleispresent_304_contains_shiftschedule_1',
            test='''{{ result('log_pluckifscheduleispresent') | matches('Shift Schedule') }}''',
            yes_task="put_schedule_policy_2",
            no_task="if_office_schedule_uri_present",
        )

        put_schedule_policy_2 = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_2',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "scheduleEntries": [
                    {
                        "schedulePolicy": {
                            "officeScheduleUri": null,
                            "name": null,
                            "officeSchedule": null,
                            "scheduleTypeUri": "urn:replicon:schedule-type:shift"
                        },
                        "effectiveDate": null
                    }
                ]
            }
        )

        if_office_schedule_uri_present = rail.IfOperator(
            task_id='if_office_schedule_uri_present',
            test='''{{ result('log_office_schedule_uri') | is_truthy  and result('log_pluckifscheduleispresent') | matches('Shift Schedule') | is_falsy }}''',
            yes_task="insert_to_list_19",
            no_task="if_scheduleispresent_304_contains_shiftschedule_2",
        )

        insert_to_list_19 = rail.SetVariableOperator(
            task_id='insert_to_list_19',
            append=True,
            name='{{ result("declare_list_6").name }}',
            value={
                "effectiveDate": {
                    "year": "{{ result('get_today').year }}",
                    "month": "{{ result('get_today').month }}",
                    "day": "{{ result('get_today').day }}"
                },
                "schedulePolicy": {
                    "officeScheduleUri": "{{ result('log_office_schedule_uri') }}",
                    "name": null,
                    "officeSchedule": null,
                    "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                }
            }
        )

        log_getoffice_scheduleentriestobeassigned_1 = rail.PythonOperator(
            task_id='log_getoffice_scheduleentriestobeassigned_1',
            python_callable=lambda: _process_schedule_entries(json.loads(
                json.dumps(rail.get_dag_run_var(rail.result('declare_list_6')['name']) or [])
                .replace('"effectiveDate": {}', '"effectiveDate": null')
            ))
        )

        put_schedule_policy_3 = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_3',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf["useruri"],
                "scheduleEntries": rail.result('log_getoffice_scheduleentriestobeassigned_1')
            }
        )

        if_scheduleispresent_304_contains_shiftschedule_2 = rail.IfOperator(
            task_id='if_scheduleispresent_304_contains_shiftschedule_2',
            test='''{{ result('log_pluckifscheduleispresent') | matches('Shift Schedule') }}''',
            yes_task="insert_to_list_20",
            no_task="log_pluckifworkweekispresent",
        )

        insert_to_list_20 = rail.SetVariableOperator(
            task_id='insert_to_list_20',
            append=True,
            name='{{ result("declare_list_6").name }}',
            value={
                "effectiveDate": {
                    "year": "{{ result('get_today').year }}",
                    "month": "{{ result('get_today').month }}",
                    "day": "{{ result('get_today').day }}"
                },
                "schedulePolicy": {
                    "officeScheduleUri": null,
                    "name": null,
                    "officeSchedule": null,
                    "scheduleTypeUri": "urn:replicon:schedule-type:shift"
                }
            }
        )

        log_getoffice_scheduleentriestobeassigned_2 = rail.PythonOperator(
            task_id='log_getoffice_scheduleentriestobeassigned_2',
            python_callable=lambda: _process_schedule_entries(json.loads(
                json.dumps(rail.get_dag_run_var(rail.result('declare_list_6')['name']) or [])
                .replace('"effectiveDate": {}', '"effectiveDate": null')
            ))
        )

        put_schedule_policy_4 = rail.RepliconServiceOperator(
            task_id='put_schedule_policy_4',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf["useruri"],
                "scheduleEntries": rail.result('log_getoffice_scheduleentriestobeassigned_2')
            }
        )

        log_pluckifworkweekispresent = rail.PythonOperator(
            task_id='log_pluckifworkweekispresent',
            python_callable=lambda: next(
                (x['value'] for x in (rail.result('mapper_search_entries') or [])
                 if x["type"] == "Work Week" and x["employee_type"] == "All"),
                None
            )
        )

        if_pluckifworkweekispresent_present = rail.IfOperator(
            task_id='if_pluckifworkweekispresent_present',
            test='''{{ result('log_pluckifworkweekispresent') | is_truthy }}''',
            yes_task="log_getworkweek_uri_1",
            no_task="if_pluckifworkweekispresent_347_blank",
        )

        log_getworkweek_uri_1 = rail.PythonOperator(
            task_id='log_getworkweek_uri_1',
            python_callable=lambda:  str(rail.result(
                'log_pluckifworkweekispresent')).rsplit('|', maxsplit=1)[-1]
        )

        if_getworkweek_uri_present_1 = rail.IfOperator(
            task_id='if_getworkweek_uri_present_1',
            test='''{{ result('log_getworkweek_uri_1') | is_truthy  and result('log_getworkweek_uri_1') != result('bulk_get_users3')[0].userDetails.workWeekStartDay.uri }}''',
            yes_task="update_work_week_start_day_for_user_1",
            no_task="if_pluckifworkweekispresent_347_blank",
        )

        update_work_week_start_day_for_user_1 = rail.RepliconServiceOperator(
            task_id='update_work_week_start_day_for_user_1',
            endpoint="/services/UserService1.svc/UpdateWorkWeekStartDayForUser",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "dayOfWeekUri": "{{ result('log_getworkweek_uri_1') }}"
            }
        )

        if_pluckifworkweekispresent_347_blank = rail.IfOperator(
            task_id='if_pluckifworkweekispresent_347_blank',
            test='''{{ result('log_pluckifworkweekispresent') | is_falsy }}''',
            yes_task="get_work_week_start_day_for_new_users",
            no_task="log_pluckif_timezoneispresent",
        )

        get_work_week_start_day_for_new_users = rail.RepliconServiceOperator(
            task_id='get_work_week_start_day_for_new_users',
            endpoint="/services/UserService1.svc/GetWorkWeekStartDayForNewUsers",
            data=None
        )

        log_getworkweek_uri_2 = rail.PythonOperator(
            task_id='log_getworkweek_uri_2',
            python_callable=lambda: rail.result('get_work_week_start_day_for_new_users')['uri']
        )

        if_getworkweek_uri_present_2 = rail.IfOperator(
            task_id='if_getworkweek_uri_present_2',
            test='''{{ result('log_getworkweek_uri_2') | is_truthy  and result('log_getworkweek_uri_2') != result('bulk_get_users3')[0].userDetails.workWeekStartDay.uri }}''',
            yes_task="update_work_week_start_day_for_user_2",
            no_task="log_pluckif_timezoneispresent",
        )

        update_work_week_start_day_for_user_2 = rail.RepliconServiceOperator(
            task_id='update_work_week_start_day_for_user_2',
            endpoint="/services/UserService1.svc/UpdateWorkWeekStartDayForUser",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "dayOfWeekUri": "{{ result('log_getworkweek_uri_2') }}"
            }
        )

        log_pluckif_timezoneispresent = rail.PythonOperator(
            task_id='log_pluckif_timezoneispresent',
            python_callable=lambda: next(
                (x['value'] for x in (rail.result('mapper_search_entries') or [])
                 if x["type"] == "Time Zone" and x["employee_type"] == "All"),
                None
            )
        )

        if_pluckif_timezoneispresent_present = rail.IfOperator(
            task_id='if_pluckif_timezoneispresent_present',
            test='''{{ result('log_pluckif_timezoneispresent') | is_truthy }}''',
            yes_task="log_get_time_zone_uri",
            no_task="log_pluckiflicensesispresent",
        )

        log_get_time_zone_uri = rail.PythonOperator(
            task_id='log_get_time_zone_uri',
            python_callable=lambda:  str(rail.result(
                'log_pluckif_timezoneispresent')).rsplit('|', maxsplit=1)[-1]
        )

        if_get_time_zone_uri_present = rail.IfOperator(
            task_id='if_get_time_zone_uri_present',
            test='''{{ result('log_get_time_zone_uri') | is_truthy  and result('log_get_time_zone_uri') != result('bulk_get_users3')[0].timeZone.uri }}''',
            yes_task="update_time_zone_for_user",
            no_task="log_pluckiflicensesispresent",
        )

        update_time_zone_for_user = rail.RepliconServiceOperator(
            task_id='update_time_zone_for_user',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "timeZoneUri": "{{ result('log_get_time_zone_uri') }}"
            }
        )

        log_pluckiflicensesispresent = rail.PythonOperator(
            task_id='log_pluckiflicensesispresent',
            python_callable=lambda: next(
                (x['value'] for x in (rail.result('mapper_search_entries') or [])
                 if x["type"] == "License" and x["employee_type"] == "All"),
                None
            )
        )

        if_pluckiflicensesispresent_present = rail.IfOperator(
            task_id='if_pluckiflicensesispresent_present',
            test='''{{ result('log_pluckiflicensesispresent') | is_truthy }}''',
            yes_task="get_all_product_available",
            no_task="log_pluckifholidaycalendarispresent",
        )

        get_all_product_available = rail.RepliconServiceOperator(
            task_id='get_all_product_available',
            endpoint="/services/AccountManagementService1.svc/GetAllProductsAvailableForUserAssignment",
            data=None
        )

        log_licensestobeassigned = rail.PythonOperator(
            task_id='log_licensestobeassigned',
            python_callable=lambda:  str(rail.result(
                'log_pluckiflicensesispresent')).split("|")
        )

        # Replaced: create_list_2 + foreach_create_list_2 + accumulate_list_items_5
        #           + foreach_create_list_366_367_end
        # Builds license URI list directly from get_all_product_available API result
        log_getlicense_u_ris = rail.PythonOperator(
            task_id='log_getlicense_u_ris',
            python_callable=lambda: rail.smartjoin_by_delim(
                [
                    uri for uri in [
                        rail.find_first_by_attr_and_get_attr(
                            rail.result('get_all_product_available'), 'displayText', name, 'uri'
                        )
                        for name in rail.result('log_licensestobeassigned')
                    ] if uri
                ],
                ","
            ),
        )

        if_getlicense_u_ris_present = rail.IfOperator(
            task_id='if_getlicense_u_ris_present',
            test='''{{ result('log_getlicense_u_ris') | is_truthy }}''',
            yes_task="put_product_assignments_for_user",
            no_task="log_pluckifholidaycalendarispresent",
        )

        put_product_assignments_for_user = rail.RepliconServiceOperator(
            task_id='put_product_assignments_for_user',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf["useruri"],
                "productUris": rail.result('log_getlicense_u_ris').split(",") if rail.result('log_getlicense_u_ris') else []
            }
        )

        log_pluckifholidaycalendarispresent = rail.PythonOperator(
            task_id='log_pluckifholidaycalendarispresent',
            python_callable=lambda: next(
                (x['value'] for x in (rail.result('mapper_search_entries') or [])
                 if x["type"] == "Holiday Calendar" and x["employee_type"] == "All"),
                None
            )
        )

        if_pluckifholidaycalendarispresent_present = rail.IfOperator(
            task_id='if_pluckifholidaycalendarispresent_present',
            test='''{{ result('log_pluckifholidaycalendarispresent') | is_truthy  and result('log_pluckifholidaycalendarispresent') != result('bulk_get_users3')[0].holidayCalendar }}''',
            yes_task="get_all_holiday_calendars",
            no_task="trigger_timeoff_update379",
        )

        get_all_holiday_calendars = rail.RepliconServiceOperator(
            task_id='get_all_holiday_calendars',
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data=None
        )

        log_get_holiday_calendar_uri = rail.PythonOperator(
            task_id='log_get_holiday_calendar_uri',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_all_holiday_calendars'), 'displayText', rail.result('log_pluckifholidaycalendarispresent'), 'uri', "")
        )

        if_get_holiday_calendar_uri_present = rail.IfOperator(
            task_id='if_get_holiday_calendar_uri_present',
            test='''{{ result('log_get_holiday_calendar_uri') | is_truthy }}''',
            yes_task="update_holiday_calendar_for_user",
            no_task="trigger_timeoff_update379",
        )

        update_holiday_calendar_for_user = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar_for_user',
            endpoint="/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "holidayCalendarUri": "{{ result('log_get_holiday_calendar_uri') }}"
            }
        )

        trigger_timeoff_update379 = rail.TriggerDagRunOperator(
            task_id='trigger_timeoff_update379',
            retries=0,
            trigger_dag_id=config.timeoff_update_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf["parentjobid"],
                "userloginname": dag_run.conf["loginname"],
                "useruri": dag_run.conf["useruri"],
                "dateused": dag_run.conf["startdate"] if rail.get_dag_run_var('rehire_log') else datetime.utcnow().strftime("%m/%d/%Y"),
                "location": dag_run.conf["location"],
                "rehire": "yes" if rail.get_dag_run_var('rehire_log') else "no",
                "startdate": dag_run.conf["startdate"],
                "scheduledhours": dag_run.conf["udf"],
                "employeetype": dag_run.conf["employeetype"],
                "ascend_user_import_logs_lookuptable": dag_run.conf["ascend_user_import_logs_lookuptable"]
            }
        )

        wait_live_ascend_timeoff_update379 = rail.WaitForDagRunsSensor(
            task_id='wait_live_ascend_timeoff_update379',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_timeoff_update379") }}'
        )

        if_rehire_log_present = rail.IfOperator(
            task_id='if_rehire_log_present',
            test=lambda: bool(rail.get_dag_run_var('rehire_log')) and rail.get_dag_run_var('locationandemployeetypebasedchange') == 'no',
            yes_task="trigger_timeoff_update381",
            no_task="log_timeoff_assignment_exceptionlog",
        )

        trigger_timeoff_update381 = rail.TriggerDagRunOperator(
            task_id='trigger_timeoff_update381',
            retries=0,
            trigger_dag_id=config.timeoff_update_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": '{{ dag_run.conf["parentjobid"] }}',
                "userloginname": '{{ dag_run.conf["loginname"] }}',
                "useruri": '{{ dag_run.conf["useruri"] }}',
                "dateused": '{{ dag_run.conf["startdate"] }}',
                "location": '{{ dag_run.conf["location"] }}',
                "rehire": "yes",
                "startdate": '{{ dag_run.conf["startdate"] }}',
                "scheduledhours": '{{ dag_run.conf["udf"] }}',
                "employeetype": '{{ dag_run.conf["employeetype"] }}',
                "ascend_user_import_logs_lookuptable": '{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}'
            }
        )

        wait_live_ascend_timeoff_update381 = rail.WaitForDagRunsSensor(
            task_id='wait_live_ascend_timeoff_update381',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_timeoff_update381") }}'
        )

        log_timeoff_assignment_exceptionlog = rail.PythonOperator(
            task_id='log_timeoff_assignment_exceptionlog',
            python_callable=lambda dag_run: (
                f"Timeoff not assigned/updated as no timeoff is defined in mapper for "
                f"{dag_run.conf.get('location', '')}-{dag_run.conf.get('employeetype', '')}-{dag_run.conf.get('scheduledhours', '')}"
                if not rail.result('log_pluckiftimeofftemplateispresent')
                else None
            )
        )

        log_detailsnotavailableinmapperfile = rail.PythonOperator(
            task_id='log_detailsnotavailableinmapperfile',
            python_callable=lambda:  "Details not available for the said location in mapper"
        )

        log_entry_2 = rail.WriteLogOperator(
            task_id='log_entry_2',
            log='{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}',
            message="na",
            severity=lambda: str("Exception" if rail.result('log_start_dateupdate_skipped') else ("Exception" if rail.result('log_error_logfordepartmentnotpresent') else ("Exception" if rail.result('log_skipchecking_continuous_service_date') else ("Exception" if rail.result(
                'log_errormessageincasewhenlocationisnotavailable') else ("Exception" if rail.result('log_error_logfordepartmentnotpresent') else ("Exception" if rail.result('log_errormessageincasewhencostcenterisnotavailable') else ("Exception" if rail.result('log_detailsnotavailableinmapperfile') else "Success"))))))),
            properties=lambda dag_run: {
                "userloginname": dag_run.conf["loginname"],
                "username": str(dag_run.conf["employeefirstname"]) + " " + str(dag_run.conf["employeelastname"]),
                "action": "Update",
                "status": str("Exception" if rail.result('log_start_dateupdate_skipped') else ("Exception" if rail.result('log_error_logfordepartmentnotpresent') else ("Exception" if rail.result('log_skipchecking_continuous_service_date') else ("Exception" if rail.result('log_errormessageincasewhenlocationisnotavailable') else ("Exception" if rail.result('log_error_logfordepartmentnotpresent') else ("Exception" if rail.result('log_errormessageincasewhencostcenterisnotavailable') else ("Exception" if rail.result('log_detailsnotavailableinmapperfile') else "Success"))))))),
                "details": rail.smartjoin_by_delim([
                    "Updated Successfully",
                    str(rail.get_dag_run_var('rehire_log') or ''),
                    str(rail.result('log_error_logfordepartmentnotpresent') or ''),
                    str(rail.result('log_start_dateupdate_skipped') or ''),
                    str(rail.result('log_skipchecking_continuous_service_date') or ''),
                    str(rail.result('log_errormessageincasewhenlocationisnotavailable') or ''),
                    str(rail.result('log_timeoff_assignment_exceptionlog') or ''),
                    str(rail.result('log_errormessageincasewhencostcenterisnotavailable') or ''),
                    str(rail.result('log_detailsnotavailableinmapperfile') or ''),
                ], ";")
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}',
            message="na",
            severity="Error",
            trigger_rule='one_failed',
            properties=lambda dag_run: {
                "username": dag_run.conf.get('employeefirstname', '') + " " + dag_run.conf.get('employeelastname', ''),
                "userloginname": dag_run.conf.get('loginname', ''),
                "action": "Update",
                "status": "Error",
                "details": rail.render_template("{{ get_error_message() }}")
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> declare_variable
        declare_variable >> bulk_get_users3 >> if_userdetails_isenabled_is_true
        if_userdetails_isenabled_is_true >> rail.Label(
            'Yes') >> log_start_datefrom_replicon >> trigger_disable_user >> wait_live_ascend_disable_user >> stop_1 >> catch_and_log_errors
        if_userdetails_isenabled_is_true >> rail.Label(
            'No') >> if_userdetails_isenabled_is_not_true_1
        if_userdetails_isenabled_is_not_true_1 >> rail.Label(
            'Yes') >> log_entry_1 >> stop_2 >> catch_and_log_errors
        if_userdetails_isenabled_is_not_true_1 >> rail.Label(
            'No') >> declare_rehire_variable >> if_userdetails_isenabled_is_not_true_2
        if_userdetails_isenabled_is_not_true_2 >> rail.Label(
            'Yes') >> enable_login >> update_employment_date_rangetoremoveenddate >> log_rehire_log >> if_employeefirstname_present
        if_userdetails_isenabled_is_not_true_2 >> rail.Label(
            'No') >> if_employeefirstname_present
        if_employeefirstname_present >> rail.Label(
            'Yes') >> update_first_name >> if_employeelastname_present
        if_employeefirstname_present >> rail.Label(
            'No') >> if_employeelastname_present
        if_employeelastname_present >> rail.Label(
            'Yes') >> update_last_name >> if_emailaddress_contains
        if_employeelastname_present >> rail.Label(
            'No') >> if_emailaddress_contains
        if_emailaddress_contains >> rail.Label(
            'Yes') >> update_email >> if_employeeid_present
        if_emailaddress_contains >> rail.Label(
            'No') >> if_employeeid_present
        if_employeeid_present >> rail.Label(
            'Yes') >> update_employee_id >> if_startdate_present
        if_employeeid_present >> rail.Label(
            'No') >> if_startdate_present
        if_startdate_present >> rail.Label(
            'Yes') >> if_startdate_not_contains
        if_startdate_not_contains >> rail.Label(
            'Yes') >> log_start_dateupdate_skipped >> get_today
        if_startdate_not_contains >> rail.Label(
            'No') >> log_start_dateday >> log_start_dateasperfeedfile >> log_start_dateasper_repliconprofile >> if_startdate_changed_1
        if_startdate_changed_1 >> rail.Label(
            'Yes') >> update_employment_date_rangeforstartdate >> log_get_udf_uri_most_recent_hire_date >> if_get_udf_uri_most_recent_hire_date_present
        if_get_udf_uri_most_recent_hire_date_present >> rail.Label(
            'Yes') >> update_date_valuefor_most_recent_hire_date >> get_today
        if_get_udf_uri_most_recent_hire_date_present >> rail.Label(
            'No') >> get_today
        if_startdate_changed_1 >> rail.Label(
            'No') >> get_today
        if_startdate_present >> rail.Label(
            'No') >> get_today
        get_today >> if_timetype_present
        if_timetype_present >> rail.Label(
            'Yes') >> log_get_f_t_p_t_uri >> log_get_f_t_p_t_value >> if_get_f_t_p_t_value_41_ne_timetype
        if_get_f_t_p_t_value_41_ne_timetype >> rail.Label(
            'Yes') >> get_enabled_custom_field >> log_gettherequiredudfdropdownuri >> if_gettherequiredudfdropdownuri_present
        if_gettherequiredudfdropdownuri_present >> rail.Label(
            'Yes') >> update_dropdown_valuefor_f_t_p_t >> if_homecountry_present
        if_gettherequiredudfdropdownuri_present >> rail.Label(
            'No') >> if_homecountry_present
        if_get_f_t_p_t_value_41_ne_timetype >> rail.Label(
            'No') >> if_homecountry_present
        if_timetype_present >> rail.Label(
            'No') >> if_homecountry_present
        if_homecountry_present >> rail.Label(
            'Yes') >> log_get_udf_uri_home_country >> log_get_home_country_value >> if_get_home_country_value_49_ne_homecountry
        if_get_home_country_value_49_ne_homecountry >> rail.Label(
            'Yes') >> update_text_valuefor_home_country_1 >> if_udf_present
        if_get_home_country_value_49_ne_homecountry >> rail.Label(
            'No') >> if_udf_present
        if_homecountry_present >> rail.Label(
            'No') >> if_udf_present
        if_udf_present >> rail.Label(
            'Yes') >> log_get_udf_uri_scheduled_hours_1 >> log_get_scheduled_hours_value >> if_to_f_ne_udfto_f
        if_to_f_ne_udfto_f >> rail.Label(
            'Yes') >> update_numeric_valuefor_scheduled_hours_1 >> update_variable_1 >> if_udf_blank
        if_to_f_ne_udfto_f >> rail.Label(
            'No') >> if_udf_blank
        if_udf_present >> rail.Label(
            'No') >> if_udf_blank
        if_udf_blank >> rail.Label(
            'Yes') >> log_get_udf_uri_scheduled_hours_2 >> update_numeric_valuefor_scheduled_hours_2 >> update_variable_2 >> if_homestateprovince_present
        if_udf_blank >> rail.Label(
            'No') >> if_homestateprovince_present
        if_homestateprovince_present >> rail.Label(
            'Yes') >> log_get_udf_uri_home_state_province >> log_get_home_state_province_value >> if_home_state_changed
        if_home_state_changed >> rail.Label(
            'Yes') >> update_text_valuefor_home_country_2 >> if_homecity_present
        if_home_state_changed >> rail.Label(
            'No') >> if_homecity_present
        if_homestateprovince_present >> rail.Label(
            'No') >> if_homecity_present
        if_homecity_present >> rail.Label(
            'Yes') >> log_get_udf_uri_home_city >> log_get_home_city_value >> if_get_home_city_value_69_ne_homecity
        if_get_home_city_value_69_ne_homecity >> rail.Label(
            'Yes') >> update_text_valuefor_home_city >> if_continuousservicedate_present
        if_get_home_city_value_69_ne_homecity >> rail.Label(
            'No') >> if_continuousservicedate_present
        if_homecity_present >> rail.Label(
            'No') >> if_continuousservicedate_present
        if_continuousservicedate_present >> rail.Label(
            'Yes') >> if_continuousservicedate_not_contains
        if_continuousservicedate_not_contains >> rail.Label(
            'Yes') >> log_skipchecking_continuous_service_date >> if_employeetype_present_fulltimehourly
        if_continuousservicedate_not_contains >> rail.Label(
            'No') >> log_get_udf_uri_continuous_service_date >> log_get_continuous_service_date_value >> get_continuous_date_1 >> get_continuous_date_2 >> if_continuous_date_changed
        if_continuous_date_changed >> rail.Label(
            'Yes') >> update_date_valuefor_continuous_service_date >> if_employeetype_present_fulltimehourly
        if_continuous_date_changed >> rail.Label(
            'No') >> if_employeetype_present_fulltimehourly
        if_continuousservicedate_present >> rail.Label(
            'No') >> if_employeetype_present_fulltimehourly
        if_employeetype_present_fulltimehourly >> rail.Label(
            'Yes') >> get_all_employee_type_details >> log_employee_type_uri >> if_employee_type_uri_present
        if_employee_type_uri_present >> rail.Label(
            'Yes') >> update_employee_type_for_user >> update_variable_3 >> if_department_present
        if_employee_type_uri_present >> rail.Label(
            'No') >> if_department_present
        if_employeetype_present_fulltimehourly >> rail.Label(
            'No') >> if_department_present
        if_department_present >> rail.Label(
            'Yes') >> if_departmenturi_present
        if_departmenturi_present >> rail.Label(
            'Yes') >> update_department_for_user >> get_datafortherequireduser
        if_departmenturi_present >> rail.Label(
            'No') >> log_error_logfordepartmentnotpresent >> get_datafortherequireduser
        if_department_present >> rail.Label(
            'No') >> get_datafortherequireduser
        get_datafortherequireduser >> log_checkifsupervsorisassigned >> if_manger_present_1
        if_manger_present_1 >> rail.Label(
            'Yes') >> if_loginname_eq_manger
        if_loginname_eq_manger >> rail.Label(
            'Yes') >> log_error_supervisor_self >> if_loginname_ne_manger
        if_loginname_eq_manger >> rail.Label(
            'No') >> if_loginname_ne_manger
        if_loginname_ne_manger >> rail.Label(
            'Yes') >> get_all_permissionsets >> search_users >> if_getsupervisor_uri_101_present_1
        if_getsupervisor_uri_101_present_1 >> rail.Label(
            'Yes') >> get_assigned_permissionsets >> log_checkifsupervisorhassupervisorpermission >> if_supervisorhassupervisorpermission_105_blank
        if_supervisorhassupervisorpermission_105_blank >> rail.Label(
            'Yes') >> log_get_supervisor_permission >> assign_supervsior_permission_set_to_user >> if_supervisor_not_assigned
        if_supervisorhassupervisorpermission_105_blank >> rail.Label(
            'No') >> if_supervisor_not_assigned
        if_getsupervisor_uri_101_present_1 >> rail.Label(
            'No') >> if_supervisor_not_assigned
        if_supervisor_not_assigned >> rail.Label(
            'Yes') >> if_getsupervisor_status_102_eq_true_1
        if_getsupervisor_status_102_eq_true_1 >> rail.Label(
            'Yes') >> if_getsupervisor_uri_101_present_2
        if_getsupervisor_uri_101_present_2 >> rail.Label(
            'Yes') >> update_initial_supervisor >> if_getsupervisor_uri_101_blank
        if_getsupervisor_uri_101_present_2 >> rail.Label(
            'No') >> if_getsupervisor_uri_101_blank
        if_getsupervisor_uri_101_blank >> rail.Label(
            'Yes') >> ascend_supervisor_assignment_table_add_entry_1 >> if_supervisor_is_assigned
        if_getsupervisor_uri_101_blank >> rail.Label(
            'No') >> if_supervisor_is_assigned
        if_getsupervisor_status_102_eq_true_1 >> rail.Label(
            'No') >> ascend_supervisor_assignment_table_add_entry_2 >> if_supervisor_is_assigned
        if_supervisor_not_assigned >> rail.Label(
            'No') >> if_supervisor_is_assigned
        if_supervisor_is_assigned >> rail.Label(
            'Yes') >> log_current_supervisor_name >> log_getthesupervisorloginname >> if_manger_present_2
        if_manger_present_2 >> rail.Label(
            'Yes') >> if_getsupervisor_uri_101_present_3
        if_getsupervisor_uri_101_present_3 >> rail.Label(
            'Yes') >> if_getsupervisor_status_102_eq_true_2
        if_getsupervisor_status_102_eq_true_2 >> rail.Label(
            'Yes') >> update_supervisor_schedule >> ascend_supervisor_assignment_table_add_entry_4 >> if_costcenter_present
        if_getsupervisor_status_102_eq_true_2 >> rail.Label(
            'No') >> ascend_supervisor_assignment_table_add_entry_3 >> if_costcenter_present
        if_manger_present_2 >> rail.Label(
            'No') >> if_costcenter_present
        if_getsupervisor_uri_101_present_3 >> rail.Label(
            'No') >> ascend_supervisor_assignment_table_add_entry_4 >> if_costcenter_present
        if_supervisor_is_assigned >> rail.Label(
            'No') >> if_costcenter_present
        if_loginname_ne_manger >> rail.Label(
            'No') >> if_costcenter_present
        if_manger_present_1 >> rail.Label(
            'No') >> if_costcenter_present
        if_costcenter_present >> rail.Label(
            'Yes') >> log_checkifanycostcenterisassigned >> log_required_cost_center >> get_dataforcostcenters >> log_get_required_costcenter_uri >> if_costcenter_changed_1
        if_costcenter_changed_1 >> rail.Label(
            'Yes') >> log_errormessageincasewhencostcenterisnotavailable >> if_costcenter_changed_2
        if_costcenter_changed_1 >> rail.Label(
            'No') >> if_costcenter_changed_2
        if_costcenter_changed_2 >> rail.Label(
            'Yes') >> if_costcenter_not_assigned
        if_costcenter_not_assigned >> rail.Label(
            'Yes') >> put_cost_center_schedule_for_user_1 >> if_costcenter_is_assigned
        if_costcenter_not_assigned >> rail.Label(
            'No') >> if_costcenter_is_assigned
        if_costcenter_is_assigned >> rail.Label(
            'Yes') >> log_getthecurrentcostcenter_1 >> parse_json_1 >> log_getthecurrentcostcenter_2 >> log_getthecurrentcostcenteruri >> if_costcenter_changed_3
        if_costcenter_changed_3 >> rail.Label(
            'Yes') >> if_costcenter_changed_4
        if_costcenter_changed_4 >> rail.Label(
            'Yes') >> declare_list_1 >> foreach_response_1 >> foreach_foreach_response_1 >> if_effectivedate_day_blank_1
        if_effectivedate_day_blank_1 >> rail.Label(
            'Yes') >> insert_to_list_1 >> foreach_foreach_response_150_151_end
        if_effectivedate_day_blank_1 >> rail.Label(
            'No') >> log_effective_date_1 >> if_to_time_ne_todayto_time_1
        if_to_time_ne_todayto_time_1 >> rail.Label(
            'Yes') >> insert_to_list_2 >> foreach_foreach_response_150_151_end
        if_to_time_ne_todayto_time_1 >> rail.Label(
            'No') >> foreach_foreach_response_150_151_end
        foreach_foreach_response_1 >> foreach_foreach_response_150_151_end >> foreach_response_150_end
        foreach_response_1 >> foreach_response_150_end >> insert_to_list_3
        insert_to_list_3 >> log_cost_center_schedule >> if_cost_center_schedule_present
        if_cost_center_schedule_present >> rail.Label(
            'Yes') >> put_cost_center_schedule_for_user_2 >> if_location_present
        if_cost_center_schedule_present >> rail.Label(
            'No') >> if_location_present
        if_costcenter_changed_4 >> rail.Label(
            'No') >> if_location_present
        if_costcenter_changed_3 >> rail.Label(
            'No') >> if_location_present
        if_costcenter_is_assigned >> rail.Label(
            'No') >> if_location_present
        if_costcenter_changed_2 >> rail.Label(
            'No') >> if_location_present
        if_costcenter_present >> rail.Label(
            'No') >> if_location_present
        if_location_present >> rail.Label(
            'Yes') >> log_checkifanylocationisassigned >> get_all_locations >> log_get_required_location_uri >> if_location_changed_1
        if_location_changed_1 >> rail.Label(
            'Yes') >> if_location_not_assigned
        if_location_not_assigned >> rail.Label(
            'Yes') >> put_location_schedule_for_user_1 >> update_variable_4 >> log_location_assignment_done >> if_location_is_assigned
        if_location_not_assigned >> rail.Label(
            'No') >> if_location_is_assigned
        if_location_is_assigned >> rail.Label(
            'Yes') >> log_getthecurrent_location_1 >> parse_json_2 >> log_getthecurrent_location_2 >> log_getthecurrent_location_uri >> if_location_changed_2
        if_location_changed_2 >> rail.Label(
            'Yes') >> declare_list_2 >> foreach_response_2 >> foreach_foreach_response_2 >> if_effectivedate_day_blank_2
        if_effectivedate_day_blank_2 >> rail.Label(
            'Yes') >> insert_to_list_4 >> foreach_foreach_response_178_179_end
        if_effectivedate_day_blank_2 >> rail.Label(
            'No') >> log_effective_date_2 >> if_to_time_ne_todayto_time_2
        if_to_time_ne_todayto_time_2 >> rail.Label(
            'Yes') >> insert_to_list_5 >> foreach_foreach_response_178_179_end
        if_to_time_ne_todayto_time_2 >> rail.Label(
            'No') >> foreach_foreach_response_178_179_end
        foreach_foreach_response_2 >> foreach_foreach_response_178_179_end >> foreach_response_178_end
        foreach_response_2 >> foreach_response_178_end >> insert_to_list_6
        insert_to_list_6 >> log_location_schedule >> if_location_schedule_present
        if_location_schedule_present >> rail.Label(
            'Yes') >> put_location_schedule_for_user_2 >> update_variable_5 >> log_location_change_done >> if_location_changed_3
        if_location_schedule_present >> rail.Label(
            'No') >> if_location_changed_3
        if_location_changed_2 >> rail.Label(
            'No') >> if_location_changed_3
        if_location_is_assigned >> rail.Label(
            'No') >> if_location_changed_3
        if_location_changed_1 >> rail.Label(
            'No') >> if_location_changed_3
        if_location_changed_3 >> rail.Label(
            'Yes') >> log_errormessageincasewhenlocationisnotavailable >> if_hourlypayrollrate_present
        if_location_changed_3 >> rail.Label(
            'No') >> if_hourlypayrollrate_present
        if_location_present >> rail.Label(
            'No') >> if_hourlypayrollrate_present
        if_hourlypayrollrate_present >> rail.Label(
            'Yes') >> declare_list_3 >> foreach_response_3 >> foreach_foreach_response_3 >> if_effectivedate_day_present_1
        if_effectivedate_day_present_1 >> rail.Label(
            'Yes') >> log_effective_date_3 >> if_to_time_ne_todayto_time_3
        if_to_time_ne_todayto_time_3 >> rail.Label(
            'Yes') >> accumulate_list_items_2 >> insert_to_list_7 >> foreach_foreach_response_196_197_end
        if_to_time_ne_todayto_time_3 >> rail.Label(
            'No') >> foreach_foreach_response_196_197_end
        if_effectivedate_day_present_1 >> rail.Label(
            'No') >> log_initial_hourly_rate_1 >> log_initial_hourly_rate_2 >> foreach_foreach_response_196_197_end
        foreach_foreach_response_3 >> foreach_foreach_response_196_197_end >> foreach_response_196_end
        foreach_response_3 >> foreach_response_196_end >> log_getthemindaydiff
        if_hourlypayrollrate_present >> rail.Label(
            'No') >> log_getthemindaydiff
        log_getthemindaydiff >> log_get_employee_hourly_cost >> if_hourly_rate_changed
        if_hourly_rate_changed >> rail.Label(
            'Yes') >> if_hourlypayrollcurrency_present
        if_hourlypayrollcurrency_present >> rail.Label(
            'Yes') >> get_all_currencies >> log_get_currency_uri_1 >> if_get_currency_uri_present_1
        if_get_currency_uri_present_1 >> rail.Label(
            'Yes') >> insert_to_list_8 >> log_checkforadditionalschedule_1 >> put_user_payroll_rate_schedule_1 >> mapper_search_entries
        if_get_currency_uri_present_1 >> rail.Label(
            'No') >> mapper_search_entries
        if_hourlypayrollcurrency_present >> rail.Label(
            'No') >> get_base_currencies >> log_get_currency_uri_2 >> if_get_currency_uri_present_2
        if_get_currency_uri_present_2 >> rail.Label(
            'Yes') >> insert_to_list_9 >> log_checkforadditionalschedule_2 >> put_user_payroll_rate_schedule_2 >> mapper_search_entries
        if_get_currency_uri_present_2 >> rail.Label(
            'No') >> mapper_search_entries
        if_hourly_rate_changed >> rail.Label(
            'No') >> mapper_search_entries
        mapper_search_entries >> if_entry_col2_present
        if_entry_col2_present >> rail.Label(
            'Yes') >> if_declare_variable_2_value_eq_yes
        if_declare_variable_2_value_eq_yes >> rail.Label(
            'Yes') >> declare_effectivedate_var >> log_pluckif_pay_ruleispresent >> if_payruleispresent_present
        if_payruleispresent_present >> rail.Label(
            'Yes') >> foreach_foreach_response_4 >> if_effectivedate_day_present_2
        foreach_foreach_response_4 >> foreach_foreach_response_229_230_end
        if_effectivedate_day_present_2 >> rail.Label(
            'Yes') >> update_variable_6 >> if_value_to_date_less_than_today
        if_effectivedate_day_present_2 >> rail.Label(
            'No') >> if_value_to_date_less_than_today
        if_value_to_date_less_than_today >> rail.Label(
            'Yes') >> accumulate_list_items_3 >> foreach_foreach_response_229_230_end
        if_value_to_date_less_than_today >> rail.Label(
            'No') >> foreach_foreach_response_229_230_end
        foreach_foreach_response_229_230_end >> log_min_day_diff_1 >> log_current_payrule >> if_payrule_changed
        if_payrule_changed >> rail.Label(
            'Yes') >> get_all_payrule_scripts >> declare_list_4 >> log_get_pay_rule_script_uri >> foreach_foreach_response_5 >> if_effectivedate_day_blank_3
        if_effectivedate_day_blank_3 >> rail.Label(
            'Yes') >> insert_to_list_10 >> if_effectivedate_day_present_3
        if_effectivedate_day_blank_3 >> rail.Label(
            'No') >> if_effectivedate_day_present_3
        if_effectivedate_day_present_3 >> rail.Label(
            'Yes') >> log_effective_date_4 >> if_startdate_changed_2
        if_startdate_changed_2 >> rail.Label(
            'Yes') >> insert_to_list_11 >> foreach_foreach_response_241_242_end
        if_startdate_changed_2 >> rail.Label(
            'No') >> foreach_foreach_response_241_242_end
        if_effectivedate_day_present_3 >> rail.Label(
            'No') >> foreach_foreach_response_241_242_end
        foreach_foreach_response_5 >> foreach_foreach_response_241_242_end
        foreach_foreach_response_241_242_end >> if_declare_list_239_list_items_lt
        if_declare_list_239_list_items_lt >> rail.Label(
            'Yes') >> if_get_pay_rule_script_uri_240_present_enabled
        if_get_pay_rule_script_uri_240_present_enabled >> rail.Label(
            'Yes') >> put_payroll_assignment_1 >> foreach_response_241_end
        if_get_pay_rule_script_uri_240_present_enabled >> rail.Label(
            'No') >> if_location_change_done_present
        if_declare_list_239_list_items_lt >> rail.Label(
            'No') >> if_location_change_done_present
        if_location_change_done_present >> rail.Label(
            'Yes') >> if_get_pay_rule_script_uri_present
        if_get_pay_rule_script_uri_present >> rail.Label(
            'Yes') >> insert_to_list_12 >> log_get_existing_payrule_schedule >> put_payroll_assignment_2 >> foreach_response_241_end
        if_get_pay_rule_script_uri_present >> rail.Label(
            'No') >> foreach_response_241_end
        if_location_change_done_present >> rail.Label(
            'No') >> foreach_response_241_end
        foreach_response_241_end >> log_pluckif_activityispresent
        if_payrule_changed >> rail.Label(
            'No') >> log_pluckif_activityispresent
        if_payruleispresent_present >> rail.Label(
            'No') >> log_pluckif_activityispresent
        log_pluckif_activityispresent >> get_activity_assignments_for_user >> log_checkif_business_tripisassigned >> if_business_tripisassigned_present
        if_business_tripisassigned_present >> rail.Label('Yes') >> if_activityispresent_present
        if_business_tripisassigned_present >> rail.Label('No') >> if_activityispresent_present
        if_activityispresent_present >> rail.Label(
            'Yes') >> get_enabled_activities >> log_activitiestobeassigned >> log_activity_uristobeassigned >> if_activity_uristobeassigned_present
        if_activity_uristobeassigned_present >> rail.Label(
            'Yes') >> put_activity_assignments_for_user_1 >> if_activityispresent_258_blank_1
        if_activity_uristobeassigned_present >> rail.Label(
            'No') >> if_activityispresent_258_blank_1
        if_activityispresent_present >> rail.Label(
            'No') >> if_activityispresent_258_blank_1
        if_activityispresent_258_blank_1 >> rail.Label(
            'Yes') >> put_activity_assignments_for_user_2 >> if_activityispresent_258_blank_2
        if_activityispresent_258_blank_1 >> rail.Label(
            'No') >> if_activityispresent_258_blank_2
        if_activityispresent_258_blank_2 >> rail.Label(
            'Yes') >> put_activity_assignments_for_user_3 >> log_pluckiftimesheetapprovalpathispresent
        if_activityispresent_258_blank_2 >> rail.Label(
            'No') >> log_pluckiftimesheetapprovalpathispresent
        log_pluckiftimesheetapprovalpathispresent >> if_timesheetapprovalpathispresent_present
        if_timesheetapprovalpathispresent_present >> rail.Label(
            'Yes') >> get_all_timesheet_approval_paths >> log_timesheetapprovalpathuri >> if_timesheetapprovalpathuri_present
        if_timesheetapprovalpathuri_present >> rail.Label(
            'Yes') >> update_approval_path_for_userfortimesheet >> log_pluckiftimesheettemplateispresent
        if_timesheetapprovalpathuri_present >> rail.Label(
            'No') >> log_pluckiftimesheettemplateispresent
        if_timesheetapprovalpathispresent_present >> rail.Label(
            'No') >> log_pluckiftimesheettemplateispresent
        log_pluckiftimesheettemplateispresent >> log_pluckifpunchentrypolicyispresent >> log_pluckiftimeofftemplateispresent >> if_timesheettemplateispresent_285_blank
        if_timesheettemplateispresent_285_blank >> rail.Label(
            'Yes') >> put_policy_set_assignments_for_user_1 >> if_timesheettemplateispresent_present
        if_timesheettemplateispresent_285_blank >> rail.Label(
            'No') >> if_timesheettemplateispresent_present
        if_timesheettemplateispresent_present >> rail.Label(
            'Yes') >> get_all_policysets >> if_policysetstoassign_present
        if_policysetstoassign_present >> rail.Label(
            'Yes') >> put_policy_set_assignments_for_user_2 >> log_pluckiftimesoffapprovalpathispresent
        if_policysetstoassign_present >> rail.Label(
            'No') >> log_pluckiftimesoffapprovalpathispresent
        if_timesheettemplateispresent_present >> rail.Label(
            'No') >> log_pluckiftimesoffapprovalpathispresent
        log_pluckiftimesoffapprovalpathispresent >> if_timeoffapprovalpathispresent_present
        if_timeoffapprovalpathispresent_present >> rail.Label(
            'Yes') >> get_all_timeoff_approval_paths >> if_timeoffapprovalpathuri_present
        if_timeoffapprovalpathuri_present >> rail.Label(
            'Yes') >> update_approval_path_for_userfortimeoff >> log_pluckifscheduleispresent
        if_timeoffapprovalpathuri_present >> rail.Label(
            'No') >> log_pluckifscheduleispresent
        if_timeoffapprovalpathispresent_present >> rail.Label(
            'No') >> log_pluckifscheduleispresent
        log_pluckifscheduleispresent >> if_scheduleispresent_present
        if_scheduleispresent_present >> rail.Label(
            'Yes') >> foreach_foreach_response_6 >> if_effectivedate_day_present_4
        if_effectivedate_day_present_4 >> rail.Label(
            'Yes') >> log_schedule_effective_date_1 >> if_effectivedate_day_blank_4
        if_effectivedate_day_present_4 >> rail.Label(
            'No') >> if_effectivedate_day_blank_4
        if_effectivedate_day_blank_4 >> rail.Label(
            'Yes') >> log_schedule_effective_date_2 >> log_schedule_effective_date_3
        if_effectivedate_day_blank_4 >> rail.Label(
            'No') >> log_schedule_effective_date_3
        log_schedule_effective_date_3 >> if_to_date_less_than_today
        if_to_date_less_than_today >> rail.Label(
            'Yes') >> accumulate_list_items_4 >> foreach_foreach_response_306_307_end
        if_to_date_less_than_today >> rail.Label(
            'No') >> foreach_foreach_response_306_307_end
        foreach_foreach_response_6 >> foreach_foreach_response_306_307_end
        foreach_foreach_response_306_307_end >> log_min_day_diff_2 >> log_current_office_schedule >> if_schedule_changed
        if_schedule_changed >> rail.Label(
            'Yes') >> get_all_office_schedules >> log_office_schedule_uri >> declare_list_6 >> foreach_foreach_response_7 >> if_schedule_is_shift
        if_schedule_is_shift >> rail.Label(
            'Yes') >> if_effectivedate_day_blank_5
        if_effectivedate_day_blank_5 >> rail.Label(
            'Yes') >> insert_to_list_15 >> if_schedule_is_officeschedule
        if_effectivedate_day_blank_5 >> rail.Label(
            'No') >> insert_to_list_16 >> if_schedule_is_officeschedule
        if_schedule_is_shift >> rail.Label(
            'No') >> if_schedule_is_officeschedule
        if_schedule_is_officeschedule >> rail.Label(
            'Yes') >> if_effectivedate_day_blank_6
        if_effectivedate_day_blank_6 >> rail.Label(
            'Yes') >> insert_to_list_17 >> foreach_foreach_response_321_322_end
        if_effectivedate_day_blank_6 >> rail.Label(
            'No') >> insert_to_list_18 >> foreach_foreach_response_321_322_end
        if_schedule_is_officeschedule >> rail.Label(
            'No') >> foreach_foreach_response_321_322_end
        foreach_foreach_response_7 >> foreach_foreach_response_321_322_end
        foreach_foreach_response_321_322_end >> if_declare_list_320_list_items_lt
        if_declare_list_320_list_items_lt >> rail.Label(
            'Yes') >> if_not_shift_schedule
        if_not_shift_schedule >> rail.Label(
            'Yes') >> put_schedule_policy_1 >> if_scheduleispresent_304_contains_shiftschedule_1
        if_not_shift_schedule >> rail.Label(
            'No') >> if_scheduleispresent_304_contains_shiftschedule_1
        if_scheduleispresent_304_contains_shiftschedule_1 >> rail.Label(
            'Yes') >> put_schedule_policy_2 >> log_pluckifworkweekispresent
        if_scheduleispresent_304_contains_shiftschedule_1 >> rail.Label(
            'No') >> if_office_schedule_uri_present
        if_declare_list_320_list_items_lt >> rail.Label(
            'No') >> if_office_schedule_uri_present
        if_office_schedule_uri_present >> rail.Label(
            'Yes') >> insert_to_list_19 >> log_getoffice_scheduleentriestobeassigned_1 >> put_schedule_policy_3 >> if_scheduleispresent_304_contains_shiftschedule_2
        if_office_schedule_uri_present >> rail.Label(
            'No') >> if_scheduleispresent_304_contains_shiftschedule_2
        if_scheduleispresent_304_contains_shiftschedule_2 >> rail.Label(
            'Yes') >> insert_to_list_20 >> log_getoffice_scheduleentriestobeassigned_2 >> put_schedule_policy_4 >> log_pluckifworkweekispresent
        if_scheduleispresent_304_contains_shiftschedule_2 >> rail.Label(
            'No') >> log_pluckifworkweekispresent
        if_schedule_changed >> rail.Label(
            'No') >> log_pluckifworkweekispresent
        if_scheduleispresent_present >> rail.Label(
            'No') >> log_pluckifworkweekispresent
        log_pluckifworkweekispresent >> if_pluckifworkweekispresent_present
        if_pluckifworkweekispresent_present >> rail.Label(
            'Yes') >> log_getworkweek_uri_1 >> if_getworkweek_uri_present_1
        if_getworkweek_uri_present_1 >> rail.Label(
            'Yes') >> update_work_week_start_day_for_user_1 >> if_pluckifworkweekispresent_347_blank
        if_getworkweek_uri_present_1 >> rail.Label(
            'No') >> if_pluckifworkweekispresent_347_blank
        if_pluckifworkweekispresent_present >> rail.Label(
            'No') >> if_pluckifworkweekispresent_347_blank
        if_pluckifworkweekispresent_347_blank >> rail.Label(
            'Yes') >> get_work_week_start_day_for_new_users >> log_getworkweek_uri_2 >> if_getworkweek_uri_present_2
        if_getworkweek_uri_present_2 >> rail.Label(
            'Yes') >> update_work_week_start_day_for_user_2 >> log_pluckif_timezoneispresent
        if_getworkweek_uri_present_2 >> rail.Label(
            'No') >> log_pluckif_timezoneispresent
        if_pluckifworkweekispresent_347_blank >> rail.Label(
            'No') >> log_pluckif_timezoneispresent
        log_pluckif_timezoneispresent >> if_pluckif_timezoneispresent_present
        if_pluckif_timezoneispresent_present >> rail.Label(
            'Yes') >> log_get_time_zone_uri >> if_get_time_zone_uri_present
        if_get_time_zone_uri_present >> rail.Label(
            'Yes') >> update_time_zone_for_user >> log_pluckiflicensesispresent
        if_get_time_zone_uri_present >> rail.Label(
            'No') >> log_pluckiflicensesispresent
        if_pluckif_timezoneispresent_present >> rail.Label(
            'No') >> log_pluckiflicensesispresent
        log_pluckiflicensesispresent >> if_pluckiflicensesispresent_present
        if_pluckiflicensesispresent_present >> rail.Label(
            'Yes') >> get_all_product_available >> log_licensestobeassigned >> log_getlicense_u_ris >> if_getlicense_u_ris_present
        if_getlicense_u_ris_present >> rail.Label(
            'Yes') >> put_product_assignments_for_user >> log_pluckifholidaycalendarispresent
        if_getlicense_u_ris_present >> rail.Label(
            'No') >> log_pluckifholidaycalendarispresent
        if_pluckiflicensesispresent_present >> rail.Label(
            'No') >> log_pluckifholidaycalendarispresent
        log_pluckifholidaycalendarispresent >> if_pluckifholidaycalendarispresent_present
        if_pluckifholidaycalendarispresent_present >> rail.Label(
            'Yes') >> get_all_holiday_calendars >> log_get_holiday_calendar_uri >> if_get_holiday_calendar_uri_present
        if_get_holiday_calendar_uri_present >> rail.Label(
            'Yes') >> update_holiday_calendar_for_user >> trigger_timeoff_update379
        if_get_holiday_calendar_uri_present >> rail.Label(
            'No') >> trigger_timeoff_update379
        if_pluckifholidaycalendarispresent_present >> rail.Label(
            'No') >> trigger_timeoff_update379
        trigger_timeoff_update379 >> wait_live_ascend_timeoff_update379 >> if_rehire_log_present
        if_declare_variable_2_value_eq_yes >> rail.Label(
            'No') >> if_rehire_log_present
        if_rehire_log_present >> rail.Label(
            'Yes') >> trigger_timeoff_update381 >> wait_live_ascend_timeoff_update381 >> log_timeoff_assignment_exceptionlog
        if_rehire_log_present >> rail.Label(
            'No') >> log_timeoff_assignment_exceptionlog >> log_entry_2
        if_entry_col2_present >> rail.Label(
            'No') >> log_detailsnotavailableinmapperfile >> log_entry_2
        log_entry_2 >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
