from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null = None
# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/mccarthy/user_import/config.py


# pylint: disable=too-many-statements
def create_updateuser_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'mccarthy_user_import_update_user_child_{config.instance}',
        description=f'User Sync_Child_User Update {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='bulk_get_users3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='bulk_get_users3',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        bulk_get_users3 = rail.RepliconServiceOperator(
            task_id='bulk_get_users3',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ dag_run.conf.useruri }}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else ''
        )

        is_termination_user = rail.IfOperator(
            task_id='is_termination_user',
            test="{{ result('bulk_get_users3').userDetails.isEnabled | is_truthy \
                and dag_run.conf.Enddate | is_truthy }}",
            yes_task="trigger_termination_to_policy_update_child",
            no_task="is_rehire_user"
        )

        trigger_termination_to_policy_update_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_termination_to_policy_update_child',
            retries=0,
            items=[-1],
            trigger_dag_id=f'mccarthy_user_import_termination_to_policy_update_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "useruri": "{{ dag_run.conf.useruri }}",
                "enddate": "{{ dag_run.conf.Enddate }}",
                "loginname": "{{ dag_run.conf.Loginname }}",
                "Email": "{{ dag_run.conf.Email }}",
                "log": "{{ dag_run.conf.log }}"
            }
        )

        wait_for_termination_to_policy_update_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_termination_to_policy_update_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_termination_to_policy_update_child") }}'
        )

        is_rehire_user = rail.IfOperator(
            task_id='is_rehire_user',
            test="{{ result('bulk_get_users3').userDetails.isEnabled | is_falsy \
                and dag_run.conf.Enddate | is_falsy }}",
            yes_task="update_employment_daterange",
            no_task="should_update_firstname"
        )

        def get_replicon_date(date_str, fmt='%m/%d/%Y'):
            datetime_obj = datetime.strptime(date_str, fmt)
            return {
                'year': datetime_obj.year,
                'month': datetime_obj.month,
                'day': datetime_obj.day
            }
        update_employment_daterange = rail.RepliconServiceOperator(
            task_id='update_employment_daterange',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": get_replicon_date(dag_run.conf['Startdate'])
                }
            }
        )

        enable_login = rail.RepliconServiceOperator(
            task_id='enable_login',
            endpoint="/services/SecurityService1.svc/EnableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        trigger_rehire_usersync_child = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_rehire_usersync_child',
            retries=0,
            items=lambda dag_run: [dag_run.conf],
            trigger_dag_id=f'mccarthy_user_import_rehired_user_sync_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **{k: v for k, v in item.items() if k not in ('_ancestry', '_ecid', '_replication_position')}
            }
        )

        wait_for_rehire_usersync_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_rehire_usersync_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_rehire_usersync_child") }}'
        )

        should_update_firstname = rail.IfOperator(
            task_id='should_update_firstname',
            test="{{ dag_run.conf.Firstname | is_truthy and result('bulk_get_users3').userDetails.firstName | lower \
                != dag_run.conf.Firstname | lower }}",
            yes_task="update_first_name",
            no_task="should_update_lastname"
        )

        update_first_name = rail.RepliconServiceOperator(
            task_id='update_first_name',
            endpoint="/services/UserService1.svc/UpdateFirstName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "firstname": "{{ dag_run.conf.Firstname }}"
            }
        )

        should_update_lastname = rail.IfOperator(
            task_id='should_update_lastname',
            test="{{ dag_run.conf.Lastname | is_truthy and result('bulk_get_users3').userDetails.lastName | lower \
                != dag_run.conf.Lastname | lower }}",
            yes_task="update_lastname",
            no_task="is_email_changed"
        )

        update_lastname = rail.RepliconServiceOperator(
            task_id='update_lastname',
            endpoint="/services/UserService1.svc/UpdateLastName",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "lastname": "{{ dag_run.conf.Lastname }}"
            }
        )

        def is_email_changed_test(dag_run):
            if dag_run.conf['Email']:
                email_address = dag_run.conf['Email'].lower()
                replicon_user_email_address = rail.result(
                    'bulk_get_users3')['userDetails']['emailAddress'].lower() if rail.result(
                    'bulk_get_users3')['userDetails']['emailAddress'] else None
                return email_address != replicon_user_email_address
            return False
        is_email_changed = rail.IfOperator(
            task_id='is_email_changed',
            test=is_email_changed_test,
            yes_task="update_email",
            no_task="is_startdate_changed"
        )

        update_email = rail.RepliconServiceOperator(
            task_id='update_email',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "email": "{{ dag_run.conf.Email }}"
            }
        )

        def is_startdate_changed_test(dag_run):
            start_date = dag_run.conf['Startdate']
            replicon_month = rail.result('bulk_get_users3')[
                'userDetails']['employmentDateRange']['startDate']['month']
            replicon_day = rail.result('bulk_get_users3')[
                'userDetails']['employmentDateRange']['startDate']['day']
            replicon_year = rail.result('bulk_get_users3')[
                'userDetails']['employmentDateRange']['startDate']['year']
            replicon_start_date = f'{replicon_month}/{replicon_day}/{replicon_year}'
            return replicon_start_date != start_date if start_date else False
        is_startdate_changed = rail.IfOperator(
            task_id='is_startdate_changed',
            test=is_startdate_changed_test,
            yes_task="update_employment_daterange_2",
            no_task="get_user_customfields_to_update"
        )

        update_employment_daterange_2 = rail.RepliconServiceOperator(
            task_id='update_employment_daterange_2',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": get_replicon_date(dag_run.conf['Startdate'])
                }
            }
        )

        def get_customfields_to_update():
            dag_run_conf = rail.get_current_context()['dag_run'].conf
            custom_fields_to_update = []

            current_user_payrollname = rail.find_first_by_attr_and_get_attr(
                rail.result('bulk_get_users3')[
                    'userDetails']['customFieldValues'],
                'customField.displayText', "Payroll Name", 'text')
            payroll_name = dag_run_conf['Payrollname']
            if payroll_name and payroll_name != current_user_payrollname:
                custom_fields_to_update.append({
                    "customField": {
                        "uri": dag_run_conf['Payrollnameuri']
                    },
                    "dropDownOption": {
                        "name": payroll_name
                    }
                })
            current_user_employeecategory = rail.find_first_by_attr_and_get_attr(
                rail.result('bulk_get_users3')[
                    'userDetails']['customFieldValues'],
                'customField.displayText', "Employee Category", 'text')
            employeecategory = dag_run_conf['Employeecategory']
            if employeecategory and employeecategory != current_user_employeecategory:
                custom_fields_to_update.append({
                    "customField": {
                        "uri": dag_run_conf['Employeecategoryuri']
                    },
                    "dropDownOption": {
                        "name": employeecategory
                    }
                })
            current_user_employeeworkstate = rail.find_first_by_attr_and_get_attr(
                rail.result('bulk_get_users3')[
                    'userDetails']['customFieldValues'],
                'customField.displayText', "Employee Work State", 'text')
            employeeworkstate = dag_run_conf['Employeeworkstate']
            if employeeworkstate and employeeworkstate != current_user_employeeworkstate:
                custom_fields_to_update.append({
                    "customField": {
                        "uri": dag_run_conf['Employeeworkstateuri']
                    },
                    "dropDownOption": {
                        "name": employeeworkstate
                    }
                })
            current_user_legalentity = rail.find_first_by_attr_and_get_attr(
                rail.result('bulk_get_users3')[
                    'userDetails']['customFieldValues'],
                'customField.displayText', "Legal Entity", 'text')
            legalentity = dag_run_conf['Legalentity']
            if legalentity and legalentity != current_user_legalentity:
                custom_fields_to_update.append({
                    "customField": {
                        "uri": dag_run_conf['Legalentityuri']
                    },
                    "dropDownOption": {
                        "name": legalentity
                    }
                })
            current_user_jobtitle = rail.find_first_by_attr_and_get_attr(
                rail.result('bulk_get_users3')[
                    'userDetails']['customFieldValues'],
                'customField.displayText', "Job Title", 'text')
            jobtitle = dag_run_conf['Jobtitle']
            if jobtitle and jobtitle != current_user_jobtitle:
                custom_fields_to_update.append({
                    "customField": {
                        "uri": dag_run_conf['Jobtitleuri']
                    },
                    "text": jobtitle
                })
            current_user_organization = rail.find_first_by_attr_and_get_attr(
                rail.result('bulk_get_users3')[
                    'userDetails']['customFieldValues'],
                'customField.displayText', "Organization", 'text')
            organization = dag_run_conf['Organization']
            if organization and organization != current_user_organization:
                custom_fields_to_update.append({
                    "customField": {
                        "uri": dag_run_conf['Organizationuri']
                    },
                    "text": organization
                })
            return custom_fields_to_update
        get_user_customfields_to_update = rail.PythonOperator(
            task_id='get_user_customfields_to_update',
            python_callable=get_customfields_to_update
        )

        is_customfields_to_update = rail.IfOperator(
            task_id='is_customfields_to_update',
            test="{{ result('get_user_customfields_to_update') | length > 0 }}",
            yes_task="update_user_customfields",
            no_task="is_supervisor_assign_pending"
        )

        update_user_customfields = rail.RepliconServiceOperator(
            task_id='update_user_customfields',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "customFieldValuesToApply": rail.result('get_user_customfields_to_update')
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        is_supervisor_assign_pending = rail.IfOperator(
            task_id='is_supervisor_assign_pending',
            test=lambda dag_run: dag_run.conf['Supervisorid'] and rail.result('bulk_get_users3') and rail.result(
                'bulk_get_users3')['supervisorAssignmentSchedule'] and dag_run.conf['Supervisorid'] != rail.result(
                'bulk_get_users3')['supervisorAssignmentSchedule'][-1]['supervisor']['user']['loginName'] and
            dag_run.conf['Supervisorid'] != dag_run.conf['Employeeid'],
            yes_task="write_supervisor_pending_log",
            no_task="should_update_department"
        )

        write_supervisor_pending_log = rail.WriteLogOperator(
            task_id='write_supervisor_pending_log',
            log='{{ dag_run.conf.supervisor_log }}',
            message="na",
            severity="Pending",
            properties=lambda dag_run: {
                "userloginname": dag_run.conf['Loginname'],
                "useruri": dag_run.conf['useruri'],
                "supervisorloginname": dag_run.conf['Supervisorid'],
                "action": "Update",
                "status": "Pending",
                "emplid": dag_run.conf['Employeeid'],
                "effective_date": dag_run.conf['Supervisoreffectivedate'],
                "user_log": dag_run.conf['log']
            }
        )

        should_update_department = rail.IfOperator(
            task_id='should_update_department',
            test="{{ dag_run.conf.Departmenturi | is_truthy and dag_run.conf.Departmenturi != \
                    result('bulk_get_users3').departmentGroupSchedule | first_or_default | \
                        attr_or_default('departmentGroup.uri') }}",
            yes_task="update_departmentgroup",
            no_task="should_update_employeetype"
        )

        def get_beginning_of_week():
            beginning_of_week = datetime.today() - timedelta(days=datetime.today().weekday() % 7)
            return {
                'year': beginning_of_week.year,
                'month': beginning_of_week.month,
                'day': beginning_of_week.day
            }
        update_departmentgroup = rail.RepliconServiceOperator(
            task_id='update_departmentgroup',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "departmentGroupScheduleToApply": {
                        "userDepartmentGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "updateDepartmentGroupScheduleOverDateRange": {
                            "replacementDepartmentGroupScheduleEntries": [
                                {
                                    "departmentGroup": {
                                        "uri": dag_run.conf['Departmenturi']
                                    },
                                    "effectiveDate": get_beginning_of_week()
                                }
                            ]
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        should_update_employeetype = rail.IfOperator(
            task_id='should_update_employeetype',
            test="{{ dag_run.conf.Employeetypeuri | is_truthy and dag_run.conf.Employeetypeuri \
                != result('bulk_get_users3').employeeTypeGroupSchedule | first_or_default | \
                    attr_or_default('employeeTypeGroup.uri') }}",
            yes_task="update_employeetype_group",
            no_task="should_update_timezone"
        )

        update_employeetype_group = rail.RepliconServiceOperator(
            task_id='update_employeetype_group',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "employeeTypeGroupScheduleToApply": {
                        "userEmployeeTypeGroupScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "updateEmployeeTypeGroupScheduleOverDateRange": {
                            "replacementEmployeeTypeGroupScheduleEntries": [
                                {
                                    "employeeTypeGroup": {
                                        "uri": dag_run.conf['Employeetypeuri']
                                    },
                                    "effectiveDate": get_beginning_of_week()
                                }
                            ]
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        should_update_timezone = rail.IfOperator(
            task_id='should_update_timezone',
            test="{{ dag_run.conf.Timezoneuri | is_truthy and dag_run.conf.Timezoneuri != \
                result('bulk_get_users3').timeZone.uri }}",
            yes_task="update_user_timezone",
            no_task="should_update_timesheet_template"
        )

        update_user_timezone = rail.RepliconServiceOperator(
            task_id='update_user_timezone',
            endpoint="/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "timeZoneUri": "{{ dag_run.conf.Timezoneuri }}"
            }
        )

        should_update_timesheet_template = rail.IfOperator(
            task_id='should_update_timesheet_template',
            test="{{ result('bulk_get_users3').timesheetTemplate.name | is_falsy \
                or result('bulk_get_users3').timesheetTemplate.name != dag_run.conf.Timesheettemplate }}",
            yes_task="is_timesheet_templateuri_present",
            no_task="get_updateuser_exception_logs"
        )

        is_timesheet_templateuri_present = rail.IfOperator(
            task_id='is_timesheet_templateuri_present',
            test="{{ dag_run.conf.Timesheettemplateuri | is_truthy }}",
            yes_task="assign_timesheet_template",
            no_task="get_updateuser_exception_logs"
        )

        assign_timesheet_template = rail.RepliconServiceOperator(
            task_id='assign_timesheet_template',
            endpoint="/services/PolicySetService1.svc/UpdatePolicySetAssignmentScheduleOverDateRangeForUserAndPolicy",
            data={
                "target": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "employeeId": null,
                    "parameterCorrelationId": null
                },
                "policyUri": "urn:replicon:policy:timesheet",
                "policySetUri": "{{ dag_run.conf.Timesheettemplateuri }}",
                "dateRange": null
            }
        )

        def get_updateuser_exception():
            dag_run_conf = rail.get_current_context()['dag_run'].conf
            supervisor_assignment_schedule = rail.result(
                'bulk_get_users3')['supervisorAssignmentSchedule']
            current_supervisor = supervisor_assignment_schedule[0][
                'supervisor']['user']['loginName'] if supervisor_assignment_schedule else ''
            if dag_run_conf['Supervisorid'] and dag_run_conf['Supervisorid'] != current_supervisor and \
                    dag_run_conf['Supervisorid'] == dag_run_conf['Employeeid']:
                return 'supervisor could not be assigned as the supervisor ID received is same as user employee id'
            return ''
        get_updateuser_exception_logs = rail.PythonOperator(
            task_id='get_updateuser_exception_logs',
            python_callable=get_updateuser_exception
        )

        write_updateuser_log = rail.WriteLogOperator(
            task_id='write_updateuser_log',
            log="{{ dag_run.conf.log }}",
            message='\
                    {%- if result("get_updateuser_exception_logs") | is_truthy -%} \
                        Updated partially - {{ result("get_updateuser_exception_logs") }}\
                    {%- else -%} \
                        Updated successfully\
                    {%- endif -%}',
            severity='\
                    {%- if result("get_updateuser_exception_logs") | is_truthy -%} \
                        Exception\
                    {%- else -%} \
                        Success\
                    {%- endif -%}',
            properties={
                'loginname': '{{ dag_run.conf.Loginname }}',
                'email': '{{ dag_run.conf.Email }}',
                'action': 'Update',
                'status': '\
                    {%- if result("get_updateuser_exception_logs") | is_truthy -%} \
                        Exception\
                    {%- else -%} \
                        Success\
                    {%- endif -%}',
                'details': '\
                    {%- if result("get_updateuser_exception_logs") | is_truthy -%} \
                        Updated partially - {{ result("get_updateuser_exception_logs") }}\
                    {%- else -%} \
                        Updated successfully\
                    {%- endif -%}'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.log }}",
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity="Error",
            properties={
                'loginname': '{{ dag_run.conf.Loginname }}',
                'email': '{{ dag_run.conf.Email }}',
                'action': 'Update',
                'status': 'Error',
                'details': '{{ get_error_message() }}'
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> bulk_get_users3 >> is_termination_user
        is_termination_user >> rail.Label(
            'Yes') >> trigger_termination_to_policy_update_child >> wait_for_termination_to_policy_update_child >> \
            catch_and_log_errors
        is_termination_user >> rail.Label(
            'No') >> is_rehire_user
        is_rehire_user >> rail.Label(
            'Yes') >> update_employment_daterange >> enable_login >> trigger_rehire_usersync_child >> \
            wait_for_rehire_usersync_child >> catch_and_log_errors
        is_rehire_user >> rail.Label(
            'No') >> should_update_firstname
        should_update_firstname >> rail.Label(
            'Yes') >> update_first_name >> should_update_lastname
        should_update_firstname >> rail.Label(
            'No') >> should_update_lastname
        should_update_lastname >> rail.Label(
            'Yes') >> update_lastname >> is_email_changed
        should_update_lastname >> rail.Label(
            'No') >> is_email_changed
        is_email_changed >> rail.Label(
            'Yes') >> update_email >> is_startdate_changed
        is_email_changed >> rail.Label(
            'No') >> is_startdate_changed
        is_startdate_changed >> rail.Label(
            'Yes') >> update_employment_daterange_2 >> get_user_customfields_to_update
        is_startdate_changed >> rail.Label(
            'No') >> get_user_customfields_to_update
        get_user_customfields_to_update >> is_customfields_to_update
        is_customfields_to_update >> rail.Label(
            'Yes') >> update_user_customfields >> is_supervisor_assign_pending
        is_customfields_to_update >> rail.Label(
            'No') >> is_supervisor_assign_pending
        is_supervisor_assign_pending >> rail.Label(
            'Yes') >> write_supervisor_pending_log >> should_update_department
        is_supervisor_assign_pending >> rail.Label(
            'No') >> should_update_department
        should_update_department >> rail.Label(
            'Yes') >> update_departmentgroup >> should_update_employeetype
        should_update_department >> rail.Label(
            'No') >> should_update_employeetype
        should_update_employeetype >> rail.Label(
            'Yes') >> update_employeetype_group >> should_update_timezone
        should_update_employeetype >> rail.Label(
            'No') >> should_update_timezone
        should_update_timezone >> rail.Label(
            'Yes') >> update_user_timezone >> should_update_timesheet_template
        should_update_timezone >> rail.Label(
            'No') >> should_update_timesheet_template
        should_update_timesheet_template >> rail.Label(
            'Yes') >> is_timesheet_templateuri_present
        is_timesheet_templateuri_present >> rail.Label(
            'Yes') >> assign_timesheet_template >> get_updateuser_exception_logs
        is_timesheet_templateuri_present >> rail.Label(
            'No') >> get_updateuser_exception_logs
        should_update_timesheet_template >> rail.Label(
            'No') >> get_updateuser_exception_logs
        get_updateuser_exception_logs >> write_updateuser_log >> catch_and_log_errors
        catch_and_log_errors >> dagrun_log_to_sumo
    return dag


rail.for_each_instance(create_updateuser_dag)
