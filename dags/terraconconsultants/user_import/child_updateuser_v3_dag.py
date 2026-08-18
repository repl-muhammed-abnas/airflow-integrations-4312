from datetime import datetime, timedelta
from airflow.models import Variable
import rail
from terraconconsultants.user_import.task.process_supervisor_assignment import process_supervisor_assignment_task_group
from terraconconsultants.user_import.utils import python_callable_method
from terraconconsultants.user_import.utils import request_payload
from terraconconsultants.user_import.utils import response_filter


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/terraconconsultants/user_import/config.py


# pylint: disable=too-many-statements
def create_updateuser_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'terraconconsultants_userimport_child_updateuser_v3_{config.instance}',
        description=f'TerraconConsultants User Sync Child Update User V3 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='user_field_exception'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='user_field_exception',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        user_field_exception = rail.PythonOperator(
            task_id='user_field_exception',
            python_callable=python_callable_method.get_user_field_exception
        )

        if_user_field_exception_present = rail.IfOperator(
            task_id='if_user_field_exception_present',
            test="{{ result('user_field_exception') | is_truthy }}",
            yes_task="write_updateuser_exception",
            no_task="get_user_report"
        )

        write_updateuser_exception = rail.WriteLogOperator(
            task_id='write_updateuser_exception',
            log="{{ dag_run.conf.log }}",
            message="User not updated, {{ result('user_field_exception') }}",
            severity="Exception",
            properties={
                "loginname": "{{ dag_run.conf.employeenumber }}",
                "uri": "{{ dag_run.conf.useruri }}",
                "action": "Update",
                "status": "Exception",
                "reason": "User not updated, {{ result('user_field_exception') }}"
            }
        )

        get_user_report = rail.RepliconServiceOperator(
            task_id='get_user_report',
            endpoint="/services/ReportService1.svc/GenerateReport",
            data=lambda dag_run: {
                "reportUri": dag_run.conf['reporturi'],
                "filterValues": [
                    {
                        "reportFilterUri": dag_run.conf['reportfilteruri'],
                        "value": dag_run.conf['useruri'].split(':')[-1]
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{ result('get_user_report').payload }}"
        )

        parse_csv_user_data = rail.PythonOperator(
            task_id='parse_csv_user_data',
            python_callable=lambda: rail.load_all_records(
                rail.result('parse_csv'))[0]
        )

        if_rehires_where_replicon_profile_disabled = rail.IfOperator(
            task_id='if_rehires_where_replicon_profile_disabled',
            test="{{ dag_run.conf.assignment_status != 'Terminate Assignment' and \
                result('parse_csv_user_data')['User Status'] != 'Enabled' }}",
            yes_task="process_rehiredate_blank",
            no_task="get_required_timezone_uri",
        )

        process_rehiredate_blank = rail.EmptyOperator(
            task_id='process_rehiredate_blank'
        )

        is_rehiredate_blank = rail.IfOperator(
            task_id='is_rehiredate_blank',
            test="{{ dag_run.conf.rehiredate | is_falsy }}",
            yes_task="write_rehiredate_exception",
            no_task="enable_login"
        )

        write_rehiredate_exception = rail.WriteLogOperator(
            task_id='write_rehiredate_exception',
            log="{{ dag_run.conf.log }}",
            message="User not enabled, Rehire date is blank in feed file",
            severity="Exception",
            properties={
                "loginname": "{{ dag_run.conf.employeenumber }}",
                "uri": "{{ dag_run.conf.useruri }}",
                "action": "Update",
                "status": "Exception",
                "reason": "User not enabled, Rehire date is blank in feed file"
            }
        )

        enable_login = rail.RepliconServiceOperator(
            task_id='enable_login',
            endpoint="/services/SecurityService1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        def compare_rehiredate(dag_run):
            rehire_date = dag_run.conf['rehiredate'].replace('-', '/')
            if rehire_date:
                rehire_datetime = datetime.strptime(rehire_date, '%m/%d/%Y')
                replicon_userdate = rail.result(
                    'parse_csv_user_data')['Rehire Date'] if rail.result(
                        'parse_csv_user_data')['Rehire Date'] else '01/01/2099'
                replicon_rehiredatetime = datetime.strptime(
                    replicon_userdate, '%m/%d/%Y') if replicon_userdate else ''
                return rehire_datetime.date() != replicon_rehiredatetime.date()
            return False
        is_rehiredate_changed = rail.IfOperator(
            task_id='is_rehiredate_changed',
            test=compare_rehiredate,
            yes_task="get_rehiredate_usercustom_field",
            no_task="get_required_timezone_uri",
        )

        get_rehiredate_usercustom_field = rail.RepliconServiceOperator(
            task_id='get_rehiredate_usercustom_field',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Rehire Date', 'uri', '')
        )

        update_rehiredate_customfield = rail.RepliconServiceOperator(
            task_id='update_rehiredate_customfield',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "customFieldUri": rail.result('get_rehiredate_usercustom_field'),
                "objectUri": dag_run.conf['useruri'],
                "value": request_payload.get_replicon_date(dag_run.conf['rehiredate'].replace('-', '/'))
            }
        )

        get_required_timezone_uri = rail.RepliconServiceOperator(
            task_id='get_required_timezone_uri',
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['timezone_code'], 'uri', '')
        )

        is_firstname_present = rail.IfOperator(
            task_id='is_firstname_present',
            test="{{ dag_run.conf.firstname | is_truthy and dag_run.conf.firstname != \
                result('parse_csv_user_data')['User First Name'] }}",
            yes_task="update_firstname",
            no_task="is_lastname_present",
        )

        update_firstname = rail.RepliconServiceOperator(
            task_id='update_firstname',
            endpoint="/services/UserService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.firstname }}"
            }
        )

        is_lastname_present = rail.IfOperator(
            task_id='is_lastname_present',
            test="{{ dag_run.conf.lastname | sn | is_truthy and \
                dag_run.conf.lastname != result('parse_csv_user_data')['User Last Name'] }}",
            yes_task="update_lastname",
            no_task="is_emailaddress_present",
        )

        update_lastname = rail.RepliconServiceOperator(
            task_id='update_lastname',
            endpoint="/services/UserService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.lastname }}"
            }
        )

        is_emailaddress_present = rail.IfOperator(
            task_id='is_emailaddress_present',
            test="{{ dag_run.conf.emailaddress | sn | is_truthy and \
                dag_run.conf.emailaddress != result('parse_csv_user_data')['User Email'] }}",
            yes_task="update_email",
            no_task="is_startdate_present",
        )

        update_email = rail.RepliconServiceOperator(
            task_id='update_email',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.emailaddress }}"
            }
        )

        def compare_startdate(dag_run):
            start_date = dag_run.conf['startdate'].replace('-', '/')
            if start_date:
                start_datetime = datetime.strptime(start_date, '%m/%d/%Y')
                replicon_userdate = rail.result(
                    'parse_csv_user_data')['User Start Date'] if rail.result(
                        'parse_csv_user_data')['User Start Date'] else '01/01/2099'
                replicon_startdatetime = datetime.strptime(
                    replicon_userdate, '%m/%d/%Y') if replicon_userdate else ''
                return start_datetime.date() != replicon_startdatetime.date()
            return False
        is_startdate_present = rail.IfOperator(
            task_id='is_startdate_present',
            test=compare_startdate,
            yes_task="update_startdate",
            no_task="is_remove_enddate",
        )

        update_startdate = rail.RepliconServiceOperator(
            task_id='update_startdate',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": request_payload.get_replicon_date(dag_run.conf['startdate'])
                }
            }
        )

        is_remove_enddate = rail.IfOperator(
            task_id='is_remove_enddate',
            test="{{ dag_run.conf.enddate | sn | is_falsy and \
                result('parse_csv_user_data')['User End Date'] | sn | is_truthy }}",
            yes_task="remove_enddate",
            no_task="is_enddate_present",
        )

        remove_enddate = rail.RepliconServiceOperator(
            task_id='remove_enddate',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": request_payload.get_replicon_date(dag_run.conf['startdate']),
                    "endDate": None
                }
            }
        )

        def compare_enddate(dag_run):
            end_date = dag_run.conf['enddate'].replace('-', '/')
            if end_date:
                end_datetime = datetime.strptime(end_date, '%m/%d/%Y')
                replicon_userdate = rail.result(
                    'parse_csv_user_data')['User End Date'] if rail.result(
                        'parse_csv_user_data')['User End Date'] else '01/01/2099'
                replicon_enddatetime = datetime.strptime(
                    replicon_userdate, '%m/%d/%Y') if replicon_userdate else ''
                return end_datetime.date() != replicon_enddatetime.date()
            return False
        is_enddate_present = rail.IfOperator(
            task_id='is_enddate_present',
            test=compare_enddate,
            yes_task="update_enddate",
            no_task="update_timezone",
        )

        update_enddate = rail.RepliconServiceOperator(
            task_id='update_enddate',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": request_payload.get_replicon_date(dag_run.conf['startdate']),
                    "endDate": request_payload.get_replicon_date(dag_run.conf['enddate'])
                }
            }
        )

        update_timezone = rail.RepliconServiceOperator(
            task_id='update_timezone',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "timeZoneUri": "{{ result('get_required_timezone_uri') }}"
            }
        )

        get_required_user_customfields = rail.RepliconServiceOperator(
            task_id='get_required_user_customfields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda response: {
                'service_date_udf': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Service Date', 'uri', ''),
                'chargeability_udf': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Chargeability %', 'uri', ''),
                'localtaxcode_udf': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Local Tax Code', 'uri', ''),
                'fulltimeavailability_udf': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Full Time Availability', 'uri', ''),
                'jobtitle_udf': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Job Title', 'uri', ''),
                'department_udf': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Department', 'uri', ''),
                'gre_udf': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'GRE', 'uri', ''),
                'floatingholiday_udf': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Floating Holiday', 'uri', '')
            }
        )

        def compare_servicedate(dag_run):
            service_date = dag_run.conf['service_date'].replace('-', '/')
            if service_date:
                service_datetime = datetime.strptime(service_date, '%m/%d/%Y')
                replicon_userdate = rail.result(
                    'parse_csv_user_data')['Service Date'] if rail.result(
                        'parse_csv_user_data')['Service Date'] else '01/01/2099'
                replicon_servicedatetime = datetime.strptime(
                    replicon_userdate, '%m/%d/%Y') if replicon_userdate else ''
                return service_datetime.date() != replicon_servicedatetime.date()
            return False
        is_servicedate_present = rail.IfOperator(
            task_id='is_servicedate_present',
            test=compare_servicedate,
            yes_task="update_servicedate_udf",
            no_task="is_chargeability_present",
        )

        update_servicedate_udf = rail.RepliconServiceOperator(
            task_id='update_servicedate_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['useruri'],
                "customFieldUri": rail.result('get_required_user_customfields')['service_date_udf'],
                "value": request_payload.get_replicon_date(dag_run.conf['service_date'].replace('-', '/'))
            }
        )

        is_chargeability_present = rail.IfOperator(
            task_id='is_chargeability_present',
            test="{{ dag_run.conf.chargeability | is_truthy and \
                dag_run.conf.chargeability != result('parse_csv_user_data')['Chargeability %'] and \
                    result('get_required_user_customfields').chargeability_udf | is_truthy }}",
            yes_task="update_chargeability_udf",
            no_task="is_localtaxcode_present",
        )

        update_chargeability_udf = rail.RepliconServiceOperator(
            task_id='update_chargeability_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').chargeability_udf }}",
                "value": "{{ dag_run.conf.chargeability }}"
            }
        )

        is_localtaxcode_present = rail.IfOperator(
            task_id='is_localtaxcode_present',
            test="{{ dag_run.conf.local_tax_code | is_truthy and \
                dag_run.conf.local_tax_code != result('parse_csv_user_data')['Local Tax Code'] and \
                    result('get_required_user_customfields').localtaxcode_udf | is_truthy }}",
            yes_task="update_localtaxcode_udf",
            no_task="is_full_time_availability_present",
        )

        update_localtaxcode_udf = rail.RepliconServiceOperator(
            task_id='update_localtaxcode_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').localtaxcode_udf }}",
                "value": "{{ dag_run.conf.local_tax_code }}"
            }
        )

        def compare_full_time_availability(dag_run):
            full_time_availability = dag_run.conf['full_time_availability']
            current_full_time_availability = round(float(rail.result(
                'parse_csv_user_data')['Full Time Availability']), 2) if rail.result(
                'parse_csv_user_data')['Full Time Availability'] else ''
            return full_time_availability and round(float(
                full_time_availability), 2) != current_full_time_availability
        is_full_time_availability_present = rail.IfOperator(
            task_id='is_full_time_availability_present',
            test=compare_full_time_availability,
            yes_task="update_full_time_availability",
            no_task="is_jobtitle_present",
        )

        update_full_time_availability = rail.RepliconServiceOperator(
            task_id='update_full_time_availability',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').fulltimeavailability_udf }}",
                "value": "{{ dag_run.conf.full_time_availability }}"
            }
        )

        is_jobtitle_present = rail.IfOperator(
            task_id='is_jobtitle_present',
            test="{{ dag_run.conf.job_title | is_truthy and \
                dag_run.conf.job_title != result('parse_csv_user_data')['Job Title'] and \
                    result('get_required_user_customfields').jobtitle_udf | is_truthy }}",
            yes_task="get_jobtitle_dropdown",
            no_task="is_department_present",
        )

        get_jobtitle_dropdown = rail.RepliconServiceOperator(
            task_id='get_jobtitle_dropdown',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_user_customfields').jobtitle_udf }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'job_title'], 'uri', '')
        )

        is_jobtitle_dropdown_present = rail.IfOperator(
            task_id='is_jobtitle_dropdown_present',
            test="{{ result('get_jobtitle_dropdown') | is_truthy }}",
            yes_task="update_jobtitle_udf",
            no_task="is_department_present",
        )

        update_jobtitle_udf = rail.RepliconServiceOperator(
            task_id='update_jobtitle_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').jobtitle_udf }}",
                "customFieldDropDownOptionUri": "{{ result('get_jobtitle_dropdown') }}"
            }
        )

        is_department_present = rail.IfOperator(
            task_id='is_department_present',
            test="{{ dag_run.conf.department | is_truthy and \
                dag_run.conf.department != result('parse_csv_user_data')['Department'] and \
                    result('get_required_user_customfields').department_udf | is_truthy }}",
            yes_task="get_department_dropdown",
            no_task="is_govt_reporting_entity_present",
        )

        get_department_dropdown = rail.RepliconServiceOperator(
            task_id='get_department_dropdown',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_user_customfields').department_udf }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'department'], 'uri', '')
        )

        is_department_dropdown_present = rail.IfOperator(
            task_id='is_department_dropdown_present',
            test="{{ result('get_department_dropdown') | is_truthy }}",
            yes_task="update_department_udf",
            no_task="is_govt_reporting_entity_present",
        )

        update_department_udf = rail.RepliconServiceOperator(
            task_id='update_department_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').department_udf }}",
                "customFieldDropDownOptionUri": "{{ result('get_department_dropdown') }}"
            }
        )

        is_govt_reporting_entity_present = rail.IfOperator(
            task_id='is_govt_reporting_entity_present',
            test="{{ dag_run.conf.govt_reporting_entity | is_truthy \
                and dag_run.conf.govt_reporting_entity != result('parse_csv_user_data')['GRE'] and \
                    result('get_required_user_customfields').gre_udf | is_truthy }}",
            yes_task="get_govt_reporting_entity_dropdown",
            no_task="is_floating_holiday_present",
        )

        get_govt_reporting_entity_dropdown = rail.RepliconServiceOperator(
            task_id='get_govt_reporting_entity_dropdown',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_user_customfields').gre_udf }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'govt_reporting_entity'], 'uri', '')
        )

        is_govt_reporting_entity_dropdown_present = rail.IfOperator(
            task_id='is_govt_reporting_entity_dropdown_present',
            test="{{ result('get_govt_reporting_entity_dropdown') | is_truthy }}",
            yes_task="update_govt_reporting_entity_udf",
            no_task="is_floating_holiday_present",
        )

        update_govt_reporting_entity_udf = rail.RepliconServiceOperator(
            task_id='update_govt_reporting_entity_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').gre_udf }}",
                "customFieldDropDownOptionUri": "{{ result('get_govt_reporting_entity_dropdown') }}"
            }
        )

        is_floating_holiday_present = rail.IfOperator(
            task_id='is_floating_holiday_present',
            test="{{ dag_run.conf.floating_holiday | is_truthy \
                and dag_run.conf.floating_holiday != result('parse_csv_user_data')['Floating Holiday'] and \
                    result('get_required_user_customfields').floatingholiday_udf | is_truthy }}",
            yes_task="get_floating_holiday_dropdown",
            no_task="should_update_supervisor",
        )

        get_floating_holiday_dropdown = rail.RepliconServiceOperator(
            task_id='get_floating_holiday_dropdown',
            endpoint="/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('get_required_user_customfields').floatingholiday_udf }}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(response, 'displayText', dag_run.conf[
                'floating_holiday'], 'uri', '')
        )

        is_floating_holiday_dropdown_present = rail.IfOperator(
            task_id='is_floating_holiday_dropdown_present',
            test="{{ result('get_floating_holiday_dropdown') | is_truthy }}",
            yes_task="update_floating_holiday_udf",
            no_task="should_update_supervisor",
        )

        update_floating_holiday_udf = rail.RepliconServiceOperator(
            task_id='update_floating_holiday_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').floatingholiday_udf }}",
                "customFieldDropDownOptionUri": "{{ result('get_floating_holiday_dropdown') }}"
            }
        )

        (should_update_supervisor, finish_supervisor_assignment) = process_supervisor_assignment_task_group(
            is_update_user=True)

        is_timesheettemplate_present = rail.IfOperator(
            task_id='is_timesheettemplate_present',
            test="{{ dag_run.conf.timesheettemplate | is_truthy \
                and dag_run.conf.timesheettemplate != result('parse_csv_user_data')['Timesheet Template'] }}",
            yes_task="get_required_policysets_to_assign",
            no_task="get_current_timesheeturi",
        )

        get_required_policysets_to_assign = rail.RepliconServiceOperator(
            task_id='get_required_policysets_to_assign',
            endpoint='/services/PolicySetService1.svc/GetAllPolicySets',
            data_handler=python_callable_method.get_required_policysets
        )

        is_policysets_to_assign = rail.IfOperator(
            task_id='is_policysets_to_assign',
            test="{{ result('get_required_policysets_to_assign') | length > 0 }}",
            yes_task="update_policysets",
            no_task="get_current_timesheeturi",
        )

        update_policysets = rail.RepliconServiceOperator(
            task_id='update_policysets',
            endpoint="/services/PolicySetService1.svc/PutPolicySetAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "policySetUris": rail.result('get_required_policysets_to_assign')
            }
        )

        get_current_timesheeturi = rail.RepliconServiceOperator(
            task_id='get_current_timesheeturi',
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate2",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "date": request_payload.get_today_date(),
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            },
            data_handler=lambda response: response['timesheet']['uri'] if response[
                'timesheet'] else ''
        )

        get_timesheet_startdate = rail.RepliconServiceOperator(
            task_id='get_timesheet_startdate',
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetails",
            data={
                "timesheetUri": "{{ result('get_current_timesheeturi') }}"
            },
            data_handler=lambda response: response['dateRange']['startDate']
        )

        get_required_locationname = rail.RepliconServicePageOperator(
            task_id='get_required_locationname',
            endpoint="/services/LocationListService1.svc/GetData",
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 1000,
                "columnUris": [
                    "urn:replicon:location-list-column:location",
                    "urn:replicon:location-list-column:code"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:location-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": dag_run.conf['employee_location_state']
                        }
                    }
                }
            },
            page_handler=response_filter.page_handler,
            all_result_data_handler=response_filter.get_required_location
        )

        is_location_present = rail.IfOperator(
            task_id='is_location_present',
            test="{{ result('get_required_locationname').required_locationname | is_truthy and \
                result('parse_csv_user_data')['Location (Current)'] != \
                    result('get_required_locationname').required_locationname }}",
            yes_task="get_locationschedule_to_assign",
            no_task="is_assignmentstatus_present",
        )

        get_locationschedule_to_assign = rail.RepliconServiceOperator(
            task_id='get_locationschedule_to_assign',
            endpoint="/services/LocationService1.svc/GetLocationScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=python_callable_method.get_locationschedule_assignments
        )

        is_locationschedule_to_assign = rail.IfOperator(
            task_id='is_locationschedule_to_assign',
            test="{{ result('get_locationschedule_to_assign') | is_truthy }}",
            yes_task="update_locationschedule",
            no_task="is_assignmentstatus_present",
        )

        update_locationschedule = rail.RepliconServiceOperator(
            task_id='update_locationschedule',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('get_locationschedule_to_assign')
            }
        )

        is_assignmentstatus_present = rail.IfOperator(
            task_id='is_assignmentstatus_present',
            test="{{ dag_run.conf.assignment_status | is_truthy \
                and dag_run.conf.assignment_status != result('parse_csv_user_data')['Assignment Status (Current)'] }}",
            yes_task="get_required_costcenter",
            no_task="is_principalstatus_present",
        )

        get_required_costcenter = rail.RepliconServiceOperator(
            task_id='get_required_costcenter',
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['assignment_status'], 'uri', '')
        )

        is_costcenter_uri_present = rail.IfOperator(
            task_id='is_costcenter_uri_present',
            test="{{ result('get_required_costcenter') | is_truthy }}",
            yes_task="get_costcenterschedule_to_assign",
            no_task="if_assignmentstatus_equals_leavefmla",
        )

        get_costcenterschedule_to_assign = rail.RepliconServiceOperator(
            task_id='get_costcenterschedule_to_assign',
            endpoint="/services/CostCenterService1.svc/GetCostCenterScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=python_callable_method.get_costcenterschedule_assignments
        )

        is_costcenter_schedule_present = rail.IfOperator(
            task_id='is_costcenter_schedule_present',
            test="{{ result('get_costcenterschedule_to_assign') | is_truthy }}",
            yes_task="update_costcenterschedule_assignments",
            no_task="if_assignmentstatus_equals_leavefmla",
        )

        update_costcenterschedule_assignments = rail.RepliconServiceOperator(
            task_id='update_costcenterschedule_assignments',
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('get_costcenterschedule_to_assign')
            }
        )

        if_assignmentstatus_equals_leavefmla = rail.IfOperator(
            task_id='if_assignmentstatus_equals_leavefmla',
            test=lambda dag_run: dag_run.conf['assignment_status'] in (
                'Leave FMLA', 'Leave NON-FMLA', 'Leave With Pay NTE', 'Leave Seasonal'),
            yes_task="trigger_delete_holiday_bookings",
            no_task="is_principalstatus_present",
        )

        trigger_delete_holiday_bookings = rail.TriggerDagRunOperator(
            task_id='trigger_delete_holiday_bookings',
            retries=0,
            trigger_dag_id=f'terraconconsultants_userimport_child_delete_holiday_bookings_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf={
                "useruri": "{{ dag_run.conf.useruri }}"
            }
        )

        is_principalstatus_present = rail.IfOperator(
            task_id='is_principalstatus_present',
            test="{{ dag_run.conf.principalstatus | is_truthy }}",
            yes_task="get_required_divisionname",
            no_task="is_assignment_category_present",
        )

        get_required_divisionname = rail.RepliconServicePageOperator(
            task_id='get_required_divisionname',
            endpoint="/services/DivisionListService1.svc/GetData",
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:division-list-column:division",
                    "urn:replicon:division-list-column:code"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:division-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "value": {
                            "text": dag_run.conf['principalstatus']
                        }
                    }
                }
            },
            page_handler=response_filter.page_handler,
            all_result_data_handler=response_filter.get_required_division
        )

        is_division_present = rail.IfOperator(
            task_id='is_division_present',
            test="{{ result('parse_csv_user_data')['Principal Status (Current)'] != \
                result('get_required_divisionname').required_divisionname and \
                    result('get_required_divisionname').required_divisionuri | is_truthy }}",
            yes_task="get_divisionschedule_to_assign",
            no_task="is_assignment_category_present",
        )

        get_divisionschedule_to_assign = rail.RepliconServiceOperator(
            task_id='get_divisionschedule_to_assign',
            endpoint="/services/DivisionService1.svc/GetDivisionScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=python_callable_method.get_divisionschedule_assignments
        )

        is_divisionschedule_to_assign = rail.IfOperator(
            task_id='is_divisionschedule_to_assign',
            test="{{ result('get_divisionschedule_to_assign') | is_truthy }}",
            yes_task="update_divisionschedule",
            no_task="is_assignment_category_present",
        )

        update_divisionschedule = rail.RepliconServiceOperator(
            task_id='update_divisionschedule',
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('get_divisionschedule_to_assign')
            }
        )

        is_assignment_category_present = rail.IfOperator(
            task_id='is_assignment_category_present',
            test="{{ dag_run.conf.assignment_category | is_truthy \
                and dag_run.conf.assignment_category != result('parse_csv_user_data')['Assignment Type (Current)'] }}",
            yes_task="get_required_servicecenter",
            no_task="is_hourly_salariedcode_present",
        )

        get_required_servicecenter = rail.RepliconServiceOperator(
            task_id='get_required_servicecenter',
            endpoint="/services/ServiceCenterService1.svc/GetAllServiceCenters",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['assignment_category'], 'uri', '')
        )

        is_servicecenter_uri_present = rail.IfOperator(
            task_id='is_servicecenter_uri_present',
            test="{{ result('get_required_servicecenter') | is_truthy }}",
            yes_task="get_servicecenterschedule_to_assign",
            no_task="is_assignmenttype_equals_regularfulltime",
        )

        get_servicecenterschedule_to_assign = rail.RepliconServiceOperator(
            task_id='get_servicecenterschedule_to_assign',
            endpoint="/services/ServiceCenterService1.svc/GetServiceCenterScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=python_callable_method.get_servicecenterschedule_assignments
        )

        is_servicecenter_schedule_present = rail.IfOperator(
            task_id='is_servicecenter_schedule_present',
            test="{{ result('get_servicecenterschedule_to_assign') | is_truthy }}",
            yes_task="update_servicecenterschedule_assignments",
            no_task="is_assignmenttype_equals_regularfulltime",
        )

        update_servicecenterschedule_assignments = rail.RepliconServiceOperator(
            task_id='update_servicecenterschedule_assignments',
            endpoint="/services/ServiceCenterService1.svc/PutServiceCenterScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('get_servicecenterschedule_to_assign')
            }
        )

        is_assignmenttype_equals_regularfulltime = rail.IfOperator(
            task_id='is_assignmenttype_equals_regularfulltime',
            test="{{ result('parse_csv_user_data')['Assignment Type (Current)'] == 'Regular, Full Time' \
                or result('parse_csv_user_data')['Assignment Type (Current)'] == 'Regular, Full Time' }}",
            yes_task="is_assignmentcategory_not_equals_regularfulltime",
            no_task="is_hourly_salariedcode_present",
        )

        is_assignmentcategory_not_equals_regularfulltime = rail.IfOperator(
            task_id='is_assignmentcategory_not_equals_regularfulltime',
            test="{{ dag_run.conf.assignment_category != 'Regular, <Full Time' \
                and dag_run.conf.assignment_category != 'Regular, Full Time' }}",
            yes_task="trigger_delete_holiday_bookings2",
            no_task="is_hourly_salariedcode_present",
        )

        trigger_delete_holiday_bookings2 = rail.TriggerDagRunOperator(
            task_id='trigger_delete_holiday_bookings2',
            retries=0,
            trigger_dag_id=f'terraconconsultants_userimport_child_delete_holiday_bookings_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf={
                "useruri": "{{ dag_run.conf.useruri }}"
            }
        )

        is_hourly_salariedcode_present = rail.IfOperator(
            task_id='is_hourly_salariedcode_present',
            test="{{ dag_run.conf.hourly_salaried_code | is_truthy \
                and dag_run.conf.hourly_salaried_code != result('parse_csv_user_data')['Employee Type (Current)'] }}",
            yes_task="get_required_employeetype",
            no_task="get_payrulename_from_salarycode",
        )

        get_required_employeetype = rail.RepliconServiceOperator(
            task_id='get_required_employeetype',
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['hourly_salaried_code'], 'uri', '')
        )

        is_employeetype_uri_present = rail.IfOperator(
            task_id='is_employeetype_uri_present',
            test="{{ result('get_required_employeetype') | is_truthy }}",
            yes_task="get_employeetypeschedule_to_assign",
            no_task="get_payrulename_from_salarycode",
        )

        get_employeetypeschedule_to_assign = rail.RepliconServiceOperator(
            task_id='get_employeetypeschedule_to_assign',
            endpoint="/services/EmployeeTypeGroupService1.svc/GetEmployeeTypeGroupScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=python_callable_method.get_employeetypeschedule_assignments
        )

        is_employeetype_schedule_present = rail.IfOperator(
            task_id='is_employeetype_schedule_present',
            test="{{ result('get_employeetypeschedule_to_assign') | is_truthy }}",
            yes_task="update_employeetypeschedule_assignments",
            no_task="get_payrulename_from_salarycode",
        )

        update_employeetypeschedule_assignments = rail.RepliconServiceOperator(
            task_id='update_employeetypeschedule_assignments',
            endpoint="/services/EmployeeTypeGroupService1.svc/PutEmployeeTypeGroupScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('get_employeetypeschedule_to_assign')
            }
        )

        get_payrulename_from_salarycode = rail.PythonOperator(
            task_id='get_payrulename_from_salarycode',
            python_callable=lambda dag_run: 'Custom Hourly Payrule' if 'Hourly' in dag_run.conf[
                'hourly_salaried_code'] else 'Custom Salaried Payrule'
        )

        is_payrule_equals_userpayrule = rail.IfOperator(
            task_id='is_payrule_equals_userpayrule',
            test="{{ result('get_payrulename_from_salarycode') != \
                result('parse_csv_user_data')['Pay Rule Name (Current)'] }}",
            yes_task="get_required_payrule",
            no_task="is_employeeorgcode_same",
        )

        get_required_payrule = rail.RepliconServiceOperator(
            task_id='get_required_payrule',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
            data_handler=lambda response: {
                'uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', rail.result('get_payrulename_from_salarycode'), 'uri', ''),
                'name': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', rail.result('get_payrulename_from_salarycode'), 'displayText', '')
            }
        )

        is_payrule_uri_present = rail.IfOperator(
            task_id='is_payrule_uri_present',
            test="{{ result('get_required_payrule').uri | is_truthy }}",
            yes_task="get_payruleschedule_to_assign",
            no_task="is_employeeorgcode_same",
        )

        get_payruleschedule_to_assign = rail.RepliconServiceOperator(
            task_id='get_payruleschedule_to_assign',
            endpoint="/services/PayRuleScriptService2.svc/GetPayRuleScriptAssignmentScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=python_callable_method.get_payruleschedule_assignments
        )

        is_payrulescript_schedule_present = rail.IfOperator(
            task_id='is_payrulescript_schedule_present',
            test="{{ result('get_payruleschedule_to_assign') | is_truthy }}",
            yes_task="update_payruleschedule_assignments",
            no_task="is_employeeorgcode_same",
        )

        update_payruleschedule_assignments = rail.RepliconServiceOperator(
            task_id='update_payruleschedule_assignments',
            endpoint="/services/PayRuleScriptService2.svc/PutPayRuleScriptAssignmentScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('get_payruleschedule_to_assign')
            }
        )

        is_employeeorgcode_same = rail.IfOperator(
            task_id='is_employeeorgcode_same',
            test="{{ dag_run.conf.departmentgroupuri | is_truthy and \
                dag_run.conf.employee_org_code | is_truthy \
                    and dag_run.conf.employee_org_code != \
                        result('parse_csv_user_data')['Department'] }}",
            yes_task="get_departmentgroupschedule_to_assign",
            no_task="is_schedule_not_same",
        )

        get_departmentgroupschedule_to_assign = rail.RepliconServiceOperator(
            task_id='get_departmentgroupschedule_to_assign',
            endpoint="/services/DepartmentGroupService1.svc/GetDepartmentGroupScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=python_callable_method.get_departmentgroupschedule_assignments
        )

        is_departmentgroup_schedule_present = rail.IfOperator(
            task_id='is_departmentgroup_schedule_present',
            test="{{ result('get_departmentgroupschedule_to_assign') | is_truthy }}",
            yes_task="update_departmentgroupschedule_assignments",
            no_task="is_schedule_not_same",
        )

        update_departmentgroupschedule_assignments = rail.RepliconServiceOperator(
            task_id='update_departmentgroupschedule_assignments',
            endpoint="/services/DepartmentGroupService1.svc/PutDepartmentGroupScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('get_departmentgroupschedule_to_assign')
            }
        )

        is_schedule_not_same = rail.IfOperator(
            task_id='is_schedule_not_same',
            test="{{ dag_run.conf.full_time_availability != \
                result('parse_csv_user_data')['Schedule Name (Current)'] }}",
            yes_task="get_required_office_schedule",
            no_task="get_variables"
        )

        get_required_office_schedule = rail.RepliconServiceOperator(
            task_id='get_required_office_schedule',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['full_time_availability'], 'uri', '')
        )

        is_scheduleuri_not_present = rail.IfOperator(
            task_id='is_scheduleuri_not_present',
            test="{{ result('get_required_office_schedule') | is_falsy }}",
            yes_task="create_officeschedule_draft",
            no_task="get_schedulepolicyschedule_to_assign",
        )

        create_officeschedule_draft = rail.RepliconServiceOperator(
            task_id='create_officeschedule_draft',
            endpoint="/services/OfficeScheduleService1.svc/CreateNewDraft"
        )

        update_schedule_name = rail.RepliconServiceOperator(
            task_id='update_schedule_name',
            endpoint="/services/OfficeScheduleService1.svc/UpdateName",
            data={
                "officeScheduleUri": "{{ result('create_officeschedule_draft') }}",
                "name": "{{ dag_run.conf.full_time_availability }}"
            }
        )

        put_simple_pattern = rail.RepliconServiceOperator(
            task_id='put_simple_pattern',
            endpoint="/services/OfficeScheduleService1.svc/PutSimpleSchedulePattern",
            data=request_payload.get_put_simplepattern_updateuser
        )

        publish_officeschedule = rail.RepliconServiceOperator(
            task_id='publish_officeschedule',
            endpoint="/services/OfficeScheduleService1.svc/PublishDraft",
            data={
                "officeScheduleDraftUri": "{{ result('create_officeschedule_draft') }}"
            }
        )

        get_schedulepolicyschedule_to_assign = rail.RepliconServiceOperator(
            task_id='get_schedulepolicyschedule_to_assign',
            endpoint="/services/SchedulingService2.svc/GetSchedulePolicyScheduleForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=python_callable_method.get_schedulepolicyschedule_assignments
        )

        is_schedulepolicy_schedule_present = rail.IfOperator(
            task_id='is_schedulepolicy_schedule_present',
            test="{{ result('get_schedulepolicyschedule_to_assign') | is_truthy }}",
            yes_task="update_schedulepolicychedule_assignments",
            no_task="get_variables",
        )

        update_schedulepolicychedule_assignments = rail.RepliconServiceOperator(
            task_id='update_schedulepolicychedule_assignments',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "scheduleEntries": rail.result('get_schedulepolicyschedule_to_assign')
            }
        )

        get_variables = rail.PythonOperator(
            task_id='get_variables',
            python_callable=python_callable_method.get_required_updateuser_vars
        )

        is_timeoff_trigger_yes = rail.IfOperator(
            task_id='is_timeoff_trigger_yes',
            test="{{ result('get_variables').timeofftrigger == 'yes' }}",
            yes_task="timeoff_mapper.download_csv_mapper_from_s3",
            no_task="is_assignmentstatus_equals_leavefmla",
        )

        entry, get_timeoff_mapper = rail.get_s3_csv_mapper(
            group_id='timeoff_mapper',
            mapper_s3_bucket=config.bucket_name,
            download_path=config.timeoff_mapper_key_name,
            filter_callable=python_callable_method.filter_records,
            aws_conn_id=config.aws_conn_id
        )

        get_timeoff_list = rail.RepliconServiceOperator(
            task_id='get_timeoff_list',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
            data_handler=response_filter.get_timeoff_list_from_mapper
        )

        is_timeoffs_to_assign = rail.IfOperator(
            task_id='is_timeoffs_to_assign',
            test="{{ result('get_timeoff_list') | is_truthy }}",
            yes_task="put_timeoff_type_assignments_user",
            no_task="is_assignmentstatus_equals_leavefmla",
        )

        put_timeoff_type_assignments_user = rail.RepliconServiceOperator(
            task_id='put_timeoff_type_assignments_user',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": [x['uri'] for x in rail.result('get_timeoff_list')]
            }
        )

        trigger_timeoff_update_rehire_v2 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_timeoff_update_rehire_v2',
            retries=0,
            items=lambda: rail.result('get_timeoff_list'),
            trigger_dag_id=f'terraconconsultants_userimport_child_timeoff_update_rehire_v2_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.get_timeoff_update_rehire_v2
        )

        is_assignmentstatus_equals_leavefmla = rail.IfOperator(
            task_id='is_assignmentstatus_equals_leavefmla',
            test=lambda dag_run: dag_run.conf['assignment_status'] in (
                'Leave FMLA', 'Leave NON-FMLA', 'Leave Seasonal', 'Leave With Pay NTE'),
            yes_task="is_activeassignment_same",
            no_task="write_updateuser_log",
        )

        is_activeassignment_same = rail.IfOperator(
            task_id='is_activeassignment_same',
            test="{{ result('parse_csv_user_data')['Assignment Status (Current)'] == \
                'Active Assignment' }}",
            yes_task="get_assigned_timeoff_types",
            no_task="write_updateuser_log",
        )

        get_assigned_timeoff_types = rail.RepliconServiceOperator(
            task_id='get_assigned_timeoff_types',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        trigger_blankpolicyline_with_validation = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_blankpolicyline_with_validation',
            retries=0,
            items=lambda: rail.result('get_assigned_timeoff_types'),
            trigger_dag_id=f'terraconconsultants_userimport_child_addblankpolicyline_with_validation_check_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "timeoffuri": "{{ item.uri }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "effectivedate": "{{ dag_run.conf.assignment_status_effective_date }}"
            }
        )

        write_updateuser_log = rail.WriteLogOperator(
            task_id='write_updateuser_log',
            log="{{ dag_run.conf.log }}",
            message="Updated User",
            severity="Info",
            properties=python_callable_method.write_updateuser_log_props
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.log }}",
            trigger_rule='one_failed',
            message="User partially updated, {{ get_error_message() }}",
            severity="Error",
            properties={
                "loginname": "{{ dag_run.conf.employeenumber }}",
                "uri": "{{ dag_run.conf.useruri }}",
                "action": "Rehire/Update",
                "status": "Error",
                "reason": "User partially updated, {{ get_error_message() }}"
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> user_field_exception
        user_field_exception >> if_user_field_exception_present
        if_user_field_exception_present >> rail.Label(
            'Yes') >> write_updateuser_exception >> catch_and_log_errors
        if_user_field_exception_present >> rail.Label(
            'No') >> get_user_report >> parse_csv >> parse_csv_user_data >> if_rehires_where_replicon_profile_disabled
        if_rehires_where_replicon_profile_disabled >> rail.Label(
            'Yes') >> process_rehiredate_blank >> is_rehiredate_blank
        is_rehiredate_blank >> rail.Label(
            'Yes') >> write_rehiredate_exception >> catch_and_log_errors
        is_rehiredate_blank >> rail.Label(
            'No') >> enable_login >> is_rehiredate_changed
        is_rehiredate_changed >> rail.Label(
            'Yes') >> get_rehiredate_usercustom_field >> update_rehiredate_customfield >> get_required_timezone_uri
        is_rehiredate_changed >> rail.Label(
            'No') >> get_required_timezone_uri
        if_rehires_where_replicon_profile_disabled >> rail.Label(
            'No') >> get_required_timezone_uri
        get_required_timezone_uri >> is_firstname_present
        is_firstname_present >> rail.Label(
            'Yes') >> update_firstname >> is_lastname_present
        is_firstname_present >> rail.Label(
            'No') >> is_lastname_present
        is_lastname_present >> rail.Label(
            'Yes') >> update_lastname >> is_emailaddress_present
        is_lastname_present >> rail.Label(
            'No') >> is_emailaddress_present
        is_emailaddress_present >> rail.Label(
            'Yes') >> update_email >> is_startdate_present
        is_emailaddress_present >> rail.Label(
            'No') >> is_startdate_present
        is_startdate_present >> rail.Label(
            'Yes') >> update_startdate >> is_remove_enddate
        is_startdate_present >> rail.Label(
            'No') >> is_remove_enddate
        is_remove_enddate >> rail.Label(
            'Yes') >> remove_enddate >> is_enddate_present
        is_remove_enddate >> rail.Label(
            'No') >> is_enddate_present
        is_enddate_present >> rail.Label(
            'Yes') >> update_enddate >> update_timezone
        is_enddate_present >> rail.Label(
            'No') >> update_timezone
        update_timezone >> get_required_user_customfields >> is_servicedate_present
        is_servicedate_present >> rail.Label(
            'Yes') >> update_servicedate_udf >> is_chargeability_present
        is_servicedate_present >> rail.Label(
            'No') >> is_chargeability_present
        is_chargeability_present >> rail.Label(
            'Yes') >> update_chargeability_udf >> is_localtaxcode_present
        is_chargeability_present >> rail.Label(
            'No') >> is_localtaxcode_present
        is_localtaxcode_present >> rail.Label(
            'Yes') >> update_localtaxcode_udf >> is_full_time_availability_present
        is_localtaxcode_present >> rail.Label(
            'No') >> is_full_time_availability_present
        is_full_time_availability_present >> rail.Label(
            'Yes') >> update_full_time_availability >> is_jobtitle_present
        is_full_time_availability_present >> rail.Label(
            'No') >> is_jobtitle_present
        is_jobtitle_present >> rail.Label(
            'Yes') >> get_jobtitle_dropdown >> is_jobtitle_dropdown_present
        is_jobtitle_dropdown_present >> rail.Label(
            'Yes') >> update_jobtitle_udf >> is_department_present
        is_jobtitle_dropdown_present >> rail.Label(
            'No') >> is_department_present
        is_jobtitle_present >> rail.Label(
            'No') >> is_department_present
        is_department_present >> rail.Label(
            'Yes') >> get_department_dropdown >> is_department_dropdown_present
        is_department_dropdown_present >> rail.Label(
            'Yes') >> update_department_udf >> is_govt_reporting_entity_present
        is_department_dropdown_present >> rail.Label(
            'No') >> is_govt_reporting_entity_present
        is_department_present >> rail.Label(
            'No') >> is_govt_reporting_entity_present
        is_govt_reporting_entity_present >> rail.Label(
            'Yes') >> get_govt_reporting_entity_dropdown >> is_govt_reporting_entity_dropdown_present
        is_govt_reporting_entity_dropdown_present >> rail.Label(
            'Yes') >> update_govt_reporting_entity_udf >> is_floating_holiday_present
        is_govt_reporting_entity_dropdown_present >> rail.Label(
            'No') >> is_floating_holiday_present
        is_govt_reporting_entity_present >> rail.Label(
            'No') >> is_floating_holiday_present
        is_floating_holiday_present >> rail.Label(
            'Yes') >> get_floating_holiday_dropdown >> is_floating_holiday_dropdown_present
        is_floating_holiday_dropdown_present >> rail.Label(
            'Yes') >> update_floating_holiday_udf >> should_update_supervisor
        is_floating_holiday_dropdown_present >> rail.Label(
            'No') >> should_update_supervisor
        is_floating_holiday_present >> rail.Label(
            'No') >> should_update_supervisor
        finish_supervisor_assignment >> is_timesheettemplate_present
        is_timesheettemplate_present >> rail.Label(
            'Yes') >> get_required_policysets_to_assign >> is_policysets_to_assign
        is_policysets_to_assign >> rail.Label(
            'Yes') >> update_policysets >> get_current_timesheeturi
        is_policysets_to_assign >> rail.Label(
            'No') >> get_current_timesheeturi
        is_timesheettemplate_present >> rail.Label(
            'No') >> get_current_timesheeturi
        get_current_timesheeturi >> get_timesheet_startdate >> get_required_locationname >> is_location_present
        is_location_present >> rail.Label(
            'Yes') >> get_locationschedule_to_assign >> is_locationschedule_to_assign
        is_locationschedule_to_assign >> rail.Label(
            'Yes') >> update_locationschedule >> is_assignmentstatus_present
        is_locationschedule_to_assign >> rail.Label(
            'No') >> is_assignmentstatus_present
        is_location_present >> rail.Label(
            'No') >> is_assignmentstatus_present
        is_assignmentstatus_present >> rail.Label(
            'Yes') >> get_required_costcenter >> is_costcenter_uri_present
        is_costcenter_uri_present >> rail.Label(
            'Yes') >> get_costcenterschedule_to_assign >> is_costcenter_schedule_present
        is_costcenter_schedule_present >> rail.Label(
            'Yes') >> update_costcenterschedule_assignments >> if_assignmentstatus_equals_leavefmla
        is_costcenter_schedule_present >> rail.Label(
            'No') >> if_assignmentstatus_equals_leavefmla
        is_costcenter_uri_present >> rail.Label(
            'No') >> if_assignmentstatus_equals_leavefmla
        if_assignmentstatus_equals_leavefmla >> rail.Label(
            'Yes') >> trigger_delete_holiday_bookings >> is_principalstatus_present
        if_assignmentstatus_equals_leavefmla >> rail.Label(
            'No') >> is_principalstatus_present
        is_assignmentstatus_present >> rail.Label(
            'No') >> is_principalstatus_present
        is_principalstatus_present >> rail.Label(
            'Yes') >> get_required_divisionname >> is_division_present
        is_division_present >> rail.Label(
            'Yes') >> get_divisionschedule_to_assign >> is_divisionschedule_to_assign
        is_divisionschedule_to_assign >> rail.Label(
            'Yes') >> update_divisionschedule >> is_assignment_category_present
        is_divisionschedule_to_assign >> rail.Label(
            'No') >> is_assignment_category_present
        is_division_present >> rail.Label(
            'No') >> is_assignment_category_present
        is_principalstatus_present >> rail.Label(
            'No') >> is_assignment_category_present
        is_assignment_category_present >> rail.Label(
            'Yes') >> get_required_servicecenter >> is_servicecenter_uri_present
        is_servicecenter_uri_present >> rail.Label(
            'Yes') >> get_servicecenterschedule_to_assign >> is_servicecenter_schedule_present
        is_servicecenter_schedule_present >> rail.Label(
            'Yes') >> update_servicecenterschedule_assignments >> is_assignmenttype_equals_regularfulltime
        is_servicecenter_schedule_present >> rail.Label(
            'No') >> is_assignmenttype_equals_regularfulltime
        is_assignmenttype_equals_regularfulltime >> rail.Label(
            'Yes') >> is_assignmentcategory_not_equals_regularfulltime
        is_servicecenter_uri_present >> rail.Label(
            'No') >> is_assignmenttype_equals_regularfulltime
        is_assignmentcategory_not_equals_regularfulltime >> rail.Label(
            'Yes') >> trigger_delete_holiday_bookings2 >> is_hourly_salariedcode_present
        is_assignmentcategory_not_equals_regularfulltime >> rail.Label(
            'No') >> is_hourly_salariedcode_present
        is_assignmenttype_equals_regularfulltime >> rail.Label(
            'No') >> is_hourly_salariedcode_present
        is_assignment_category_present >> rail.Label(
            'No') >> is_hourly_salariedcode_present
        is_hourly_salariedcode_present >> rail.Label(
            'Yes') >> get_required_employeetype >> is_employeetype_uri_present
        is_employeetype_uri_present >> rail.Label(
            'Yes') >> get_employeetypeschedule_to_assign >> is_employeetype_schedule_present
        is_employeetype_schedule_present >> rail.Label(
            'Yes') >> update_employeetypeschedule_assignments >> get_payrulename_from_salarycode
        is_employeetype_schedule_present >> rail.Label(
            'No') >> get_payrulename_from_salarycode
        is_employeetype_uri_present >> rail.Label(
            'No') >> get_payrulename_from_salarycode
        is_hourly_salariedcode_present >> rail.Label(
            'No') >> get_payrulename_from_salarycode
        get_payrulename_from_salarycode >> is_payrule_equals_userpayrule
        is_payrule_equals_userpayrule >> rail.Label(
            'Yes') >> get_required_payrule >> is_payrule_uri_present
        is_payrule_uri_present >> rail.Label(
            'Yes') >> get_payruleschedule_to_assign >> is_payrulescript_schedule_present
        is_payrulescript_schedule_present >> rail.Label(
            'Yes') >> update_payruleschedule_assignments >> is_employeeorgcode_same
        is_payrulescript_schedule_present >> rail.Label(
            'No') >> is_employeeorgcode_same
        is_payrule_uri_present >> rail.Label(
            'No') >> is_employeeorgcode_same
        is_payrule_equals_userpayrule >> rail.Label(
            'No') >> is_employeeorgcode_same
        is_employeeorgcode_same >> rail.Label(
            'Yes') >> get_departmentgroupschedule_to_assign >> is_departmentgroup_schedule_present
        is_departmentgroup_schedule_present >> rail.Label(
            'Yes') >> update_departmentgroupschedule_assignments >> is_schedule_not_same
        is_departmentgroup_schedule_present >> rail.Label(
            'No') >> is_schedule_not_same
        is_employeeorgcode_same >> rail.Label(
            'No') >> is_schedule_not_same
        is_schedule_not_same >> rail.Label(
            'Yes') >> get_required_office_schedule >> is_scheduleuri_not_present
        is_scheduleuri_not_present >> rail.Label(
            'Yes') >> create_officeschedule_draft >> update_schedule_name >> put_simple_pattern >> \
            publish_officeschedule >> get_schedulepolicyschedule_to_assign
        is_scheduleuri_not_present >> rail.Label(
            'No') >> get_schedulepolicyschedule_to_assign
        get_schedulepolicyschedule_to_assign >> is_schedulepolicy_schedule_present
        is_schedulepolicy_schedule_present >> rail.Label(
            'Yes') >> update_schedulepolicychedule_assignments >> get_variables
        is_schedulepolicy_schedule_present >> rail.Label(
            'No') >> get_variables
        is_schedule_not_same >> rail.Label(
            'No') >> get_variables
        get_variables >> is_timeoff_trigger_yes
        is_timeoff_trigger_yes >> rail.Label(
            'Yes') >> entry
        get_timeoff_mapper >> get_timeoff_list >> is_timeoffs_to_assign
        is_timeoffs_to_assign >> rail.Label(
            'Yes') >> put_timeoff_type_assignments_user >> trigger_timeoff_update_rehire_v2 >> \
            is_assignmentstatus_equals_leavefmla
        is_timeoffs_to_assign >> rail.Label(
            'No') >> is_assignmentstatus_equals_leavefmla
        is_timeoff_trigger_yes >> rail.Label(
            'No') >> is_assignmentstatus_equals_leavefmla
        is_assignmentstatus_equals_leavefmla >> rail.Label(
            'Yes') >> is_activeassignment_same
        is_activeassignment_same >> rail.Label(
            'Yes') >> get_assigned_timeoff_types >> trigger_blankpolicyline_with_validation >> \
            write_updateuser_log
        is_activeassignment_same >> rail.Label(
            'No') >> write_updateuser_log
        is_assignmentstatus_equals_leavefmla >> rail.Label(
            'No') >> write_updateuser_log
        write_updateuser_log >> catch_and_log_errors >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_updateuser_dag)
