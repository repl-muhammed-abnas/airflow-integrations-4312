from datetime import timedelta
from airflow.models import Variable
import rail
from terraconconsultants.user_import.task.process_supervisor_assignment import process_supervisor_assignment_task_group
from terraconconsultants.user_import.utils import python_callable_method
from terraconconsultants.user_import.utils import request_payload
from terraconconsultants.user_import.utils import response_filter


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/terraconconsultants/user_import/config.py


# pylint: disable=too-many-statements
def create_adduser_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'terraconconsultants_userimport_child_adduser_{config.instance}',
        description=f'TerraconConsultants User Sync Add {config.instance}',
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
            yes_task="write_createuser_exception",
            no_task="get_required_employeetype"
        )

        write_createuser_exception = rail.WriteLogOperator(
            task_id='write_createuser_exception',
            log="{{ dag_run.conf.log }}",
            message="User not created, {{ result('user_field_exception') }}",
            severity="Exception",
            properties={
                "loginname": "{{ dag_run.conf.employeenumber }}",
                "uri": "NA",
                "action": "Add",
                "status": "Exception",
                "reason": "User not created, {{ result('user_field_exception') }}"
            }
        )

        get_required_employeetype = rail.RepliconServiceOperator(
            task_id='get_required_employeetype',
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['hourly_salaried_code'], 'uri', '')
        )

        is_employeetype_uri_present = rail.IfOperator(
            task_id='is_employeetype_uri_present',
            test="{{ result('get_required_employeetype') | is_truthy and \
                dag_run.conf.departmentgroupuri | is_truthy }}",
            yes_task="get_payrulename_from_salarycode",
            no_task="write_employeetype_uri_exception",
        )

        get_payrulename_from_salarycode = rail.PythonOperator(
            task_id='get_payrulename_from_salarycode',
            python_callable=lambda dag_run: 'Custom Hourly Payrule' if 'Hourly' in dag_run.conf[
                'hourly_salaried_code'] else 'Custom Salaried Payrule'
        )

        get_required_payrule_uri = rail.RepliconServiceOperator(
            task_id='get_required_payrule_uri',
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', rail.result('get_payrulename_from_salarycode'), 'uri', '')
        )

        get_required_timezone_uri = rail.RepliconServiceOperator(
            task_id='get_required_timezone_uri',
            endpoint="/services/InternationalizationService1.svc/GetAllTimeZones",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['timezone_code'], 'uri', '')
        )

        get_required_policysets_to_assign = rail.RepliconServiceOperator(
            task_id='get_required_policysets_to_assign',
            endpoint='/services/PolicySetService1.svc/GetAllPolicySets',
            data_handler=python_callable_method.get_required_policysets
        )

        create_user = rail.RepliconServiceOperator(
            task_id='create_user',
            endpoint="/services/ImportService1.svc/PutUser3",
            data=request_payload.get_createuser_payload
        )

        unassign_alltimeoffs_user = rail.RepliconServiceOperator(
            task_id='unassign_alltimeoffs_user',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "timeOffTypeUris": []
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

        is_servicedate_present = rail.IfOperator(
            task_id='is_servicedate_present',
            test="{{ dag_run.conf.service_date | is_truthy and \
                result('get_required_user_customfields').service_date_udf | is_truthy }}",
            yes_task="update_servicedate_udf",
            no_task="is_chargeability_present",
        )

        update_servicedate_udf = rail.RepliconServiceOperator(
            task_id='update_servicedate_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateDateValue",
            data=lambda dag_run: {
                "objectUri": rail.result('create_user')['uri'],
                "customFieldUri": rail.result('get_required_user_customfields')['service_date_udf'],
                "value": request_payload.get_replicon_date(dag_run.conf['service_date'].replace('-', '/'))
            }
        )

        is_chargeability_present = rail.IfOperator(
            task_id='is_chargeability_present',
            test="{{ dag_run.conf.chargeability | is_truthy and \
                    result('get_required_user_customfields').chargeability_udf | is_truthy }}",
            yes_task="update_chargeability_udf",
            no_task="is_localtaxcode_present",
        )

        update_chargeability_udf = rail.RepliconServiceOperator(
            task_id='update_chargeability_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').chargeability_udf }}",
                "value": "{{ dag_run.conf.chargeability }}"
            }
        )

        is_localtaxcode_present = rail.IfOperator(
            task_id='is_localtaxcode_present',
            test="{{ dag_run.conf.local_tax_code | is_truthy and \
                    result('get_required_user_customfields').localtaxcode_udf | is_truthy }}",
            yes_task="update_localtaxcode_udf",
            no_task="is_full_time_availability_present",
        )

        update_localtaxcode_udf = rail.RepliconServiceOperator(
            task_id='update_localtaxcode_udf',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').localtaxcode_udf }}",
                "value": "{{ dag_run.conf.local_tax_code }}"
            }
        )

        is_full_time_availability_present = rail.IfOperator(
            task_id='is_full_time_availability_present',
            test=lambda dag_run: dag_run.conf['full_time_availability'] and round(
                float(dag_run.conf['full_time_availability']), 2) and
            rail.result('get_required_user_customfields')[
                'fulltimeavailability_udf'],
            yes_task="update_full_time_availability",
            no_task="is_jobtitle_present",
        )

        update_full_time_availability = rail.RepliconServiceOperator(
            task_id='update_full_time_availability',
            endpoint="/services/CustomFieldService1.svc/UpdateNumericValue",
            data={
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').fulltimeavailability_udf }}",
                "value": "{{ dag_run.conf.full_time_availability }}"
            }
        )

        is_jobtitle_present = rail.IfOperator(
            task_id='is_jobtitle_present',
            test="{{ dag_run.conf.job_title | is_truthy and \
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
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').jobtitle_udf }}",
                "customFieldDropDownOptionUri": "{{ result('get_jobtitle_dropdown') }}"
            }
        )

        is_department_present = rail.IfOperator(
            task_id='is_department_present',
            test="{{ dag_run.conf.department | is_truthy and \
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
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').department_udf }}",
                "customFieldDropDownOptionUri": "{{ result('get_department_dropdown') }}"
            }
        )

        is_govt_reporting_entity_present = rail.IfOperator(
            task_id='is_govt_reporting_entity_present',
            test="{{ dag_run.conf.govt_reporting_entity | is_truthy \
                and result('get_required_user_customfields').gre_udf | is_truthy }}",
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
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').gre_udf }}",
                "customFieldDropDownOptionUri": "{{ result('get_govt_reporting_entity_dropdown') }}"
            }
        )

        is_floating_holiday_present = rail.IfOperator(
            task_id='is_floating_holiday_present',
            test="{{ dag_run.conf.floating_holiday | is_truthy \
                and result('get_required_user_customfields').floatingholiday_udf | is_truthy }}",
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
                "objectUri": "{{ result('create_user').uri }}",
                "customFieldUri": "{{ result('get_required_user_customfields').floatingholiday_udf }}",
                "customFieldDropDownOptionUri": "{{ result('get_floating_holiday_dropdown') }}"
            }
        )

        (should_update_supervisor,
         finish_supervisor_assignment) = process_supervisor_assignment_task_group()

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

        is_locationschedule_to_assign = rail.IfOperator(
            task_id='is_locationschedule_to_assign',
            test="{{ result('get_required_locationname')['required_locationuri'] | is_truthy }}",
            yes_task="update_locationschedule",
            no_task="is_assignmentstatus_present",
        )

        update_locationschedule = rail.RepliconServiceOperator(
            task_id='update_locationschedule',
            endpoint="/services/LocationService1.svc/PutLocationScheduleForUser",
            data=lambda: {
                "userUri": rail.result('create_user')['uri'],
                "scheduleEntries": [{
                    'location': {
                        'uri': rail.result('get_required_locationname')['required_locationuri']
                    }
                }]
            }
        )

        is_assignmentstatus_present = rail.IfOperator(
            task_id='is_assignmentstatus_present',
            test="{{ dag_run.conf.assignment_status | is_truthy }}",
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
            yes_task="update_costcenterschedule_assignments",
            no_task="is_principalstatus_present",
        )

        update_costcenterschedule_assignments = rail.RepliconServiceOperator(
            task_id='update_costcenterschedule_assignments',
            endpoint="/services/CostCenterService1.svc/PutCostCenterScheduleForUser",
            data=lambda dag_run: {
                "userUri": rail.result('create_user')['uri'],
                "scheduleEntries": [{
                    'costCenter': {
                        'uri': rail.result('get_required_costcenter')
                    },
                    'effectiveDate': request_payload.get_replicon_date(
                        dag_run.conf['assignment_status_effective_date'])
                }]
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
            test="{{ result('get_required_divisionname').required_divisionuri | \
                is_truthy }}",
            yes_task="update_divisionschedule",
            no_task="is_assignment_category_present",
        )

        update_divisionschedule = rail.RepliconServiceOperator(
            task_id='update_divisionschedule',
            endpoint="/services/DivisionService1.svc/PutDivisionScheduleForUser",
            data=lambda: {
                "userUri": rail.result('create_user')['uri'],
                "scheduleEntries": [{
                    'division': {
                        'uri': rail.result('get_required_divisionname')['required_divisionuri']
                    }
                }]
            }
        )

        is_assignment_category_present = rail.IfOperator(
            task_id='is_assignment_category_present',
            test="{{ dag_run.conf.assignment_category | is_truthy }}",
            yes_task="get_required_servicecenter",
            no_task="get_required_workweek_startdate",
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
            yes_task="update_servicecenterschedule_assignments",
            no_task="get_required_workweek_startdate",
        )

        update_servicecenterschedule_assignments = rail.RepliconServiceOperator(
            task_id='update_servicecenterschedule_assignments',
            endpoint="/services/ServiceCenterService1.svc/PutServiceCenterScheduleForUser",
            data=lambda dag_run: {
                "userUri": rail.result('create_user')['uri'],
                "scheduleEntries": [{
                    'serviceCenter': {
                        'uri': rail.result('get_required_servicecenter')
                    },
                    'effectiveDate': request_payload.get_replicon_date(
                        dag_run.conf['assignment_category_effective_date'])
                }]
            }
        )

        get_required_workweek_startdate = rail.RepliconServiceOperator(
            task_id='get_required_workweek_startdate',
            endpoint="/services/InternationalizationService1.svc/GetAllDaysOfWeek",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'name', 'Sunday', 'uri', '')
        )

        is_workweek_startdate_present = rail.IfOperator(
            task_id='is_workweek_startdate_present',
            test="{{ result('get_required_workweek_startdate') | is_truthy }}",
            yes_task="update_user_workweek",
            no_task="get_required_office_schedule",
        )

        update_user_workweek = rail.RepliconServiceOperator(
            task_id='update_user_workweek',
            endpoint="/services/UserService1.svc/UpdateWorkWeekStartDayForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "dayOfWeekUri": "{{ result('get_required_workweek_startdate') }}"
            }
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
            no_task="update_schedulepolicychedule_assignments",
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
            data=request_payload.get_put_simplepattern_adduser
        )

        publish_officeschedule = rail.RepliconServiceOperator(
            task_id='publish_officeschedule',
            endpoint="/services/OfficeScheduleService1.svc/PublishDraft",
            data={
                "officeScheduleDraftUri": "{{ result('create_officeschedule_draft') }}"
            }
        )

        update_schedulepolicychedule_assignments = rail.RepliconServiceOperator(
            task_id='update_schedulepolicychedule_assignments',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data=lambda: {
                "userUri": rail.result('create_user')['uri'],
                "scheduleEntries": [{
                    'schedulePolicy': {
                        'officeScheduleUri': rail.result('get_required_office_schedule') if rail.result(
                            'get_required_office_schedule') else rail.result('publish_officeschedule')['uri'],
                        'scheduleTypeUri': 'urn:replicon:schedule-type:office-schedule'
                    }
                }]
            }
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
            no_task="write_adduser_log",
        )

        put_timeoff_type_assignments_user = rail.RepliconServiceOperator(
            task_id='put_timeoff_type_assignments_user',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user')['uri'],
                "timeOffTypeUris": [x['uri'] for x in rail.result('get_timeoff_list')]
            }
        )

        trigger_timeoff_adduser = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_timeoff_adduser',
            retries=0,
            items=lambda: rail.result('get_timeoff_list'),
            trigger_dag_id=f'terraconconsultants_userimport_child_add_timeoff_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.get_timeoff_adduser_conf
        )

        write_adduser_log = rail.WriteLogOperator(
            task_id='write_adduser_log',
            log="{{ dag_run.conf.log }}",
            message="Added User",
            severity="Info",
            properties=python_callable_method.write_adduser_log_props
        )

        write_employeetype_uri_exception = rail.WriteLogOperator(
            task_id='write_employeetype_uri_exception',
            log="{{ dag_run.conf.log }}",
            message="Employee Type URI Exception",
            severity="Exception",
            properties=python_callable_method.write_employeetype_exception_log
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.log }}",
            trigger_rule='one_failed',
            message="\
                {%- if get_task_state('create_user') == 'success' -%} \
                    User created, but partially updated {{ get_error_message() }}\
                {%- else -%}\
                    User not created, {{ get_error_message() }}\
                {%- endif -%}",
            severity="Error",
            properties={
                "loginname": "{{ dag_run.conf.employeenumber }}",
                "uri": "\
                    {%- if get_task_state('create_user') == 'success' -%} \
                        {{ result('create_user').uri }}\
                    {%- else -%}\
                        NA\
                    {%- endif -%}",
                "action": "Add",
                "status": "Error",
                "reason": "\
                    {%- if get_task_state('create_user') == 'success' -%} \
                        User created, but partially updated {{ get_error_message() }}\
                    {%- else -%}\
                        User not created, {{ get_error_message() }}\
                    {%- endif -%}"
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
            'No') >> user_field_exception >> if_user_field_exception_present
        if_user_field_exception_present >> rail.Label(
            'Yes') >> write_createuser_exception >> catch_and_log_errors
        if_user_field_exception_present >> rail.Label(
            'No') >> get_required_employeetype >> is_employeetype_uri_present
        is_employeetype_uri_present >> rail.Label(
            'Yes') >> get_payrulename_from_salarycode >> get_required_payrule_uri >> \
            get_required_timezone_uri >> get_required_policysets_to_assign >> create_user >> \
            unassign_alltimeoffs_user >> get_required_user_customfields >> is_servicedate_present

        is_servicedate_present >> rail.Label(
            'Yes') >> update_servicedate_udf >> is_chargeability_present
        is_servicedate_present >> rail.Label(
            'No') >> is_chargeability_present
        is_chargeability_present >> rail.Label(
            'Yes') >> update_chargeability_udf >> is_localtaxcode_present
        is_chargeability_present >> rail.Label(
            'No') >> is_localtaxcode_present
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

        finish_supervisor_assignment >> get_required_locationname >> is_locationschedule_to_assign
        is_locationschedule_to_assign >> rail.Label(
            'Yes') >> update_locationschedule >> is_assignmentstatus_present
        is_locationschedule_to_assign >> rail.Label(
            'No') >> is_assignmentstatus_present
        is_assignmentstatus_present >> rail.Label(
            'Yes') >> get_required_costcenter >> is_costcenter_uri_present
        is_costcenter_uri_present >> rail.Label(
            'Yes') >> update_costcenterschedule_assignments >> is_principalstatus_present
        is_costcenter_uri_present >> rail.Label(
            'No') >> is_principalstatus_present
        is_assignmentstatus_present >> rail.Label(
            'No') >> is_principalstatus_present
        is_principalstatus_present >> rail.Label(
            'Yes') >> get_required_divisionname >> is_division_present
        is_division_present >> rail.Label(
            'Yes') >> update_divisionschedule >> is_assignment_category_present
        is_division_present >> rail.Label(
            'No') >> is_assignment_category_present
        is_principalstatus_present >> rail.Label(
            'No') >> is_assignment_category_present
        is_assignment_category_present >> rail.Label(
            'Yes') >> get_required_servicecenter >> is_servicecenter_uri_present
        is_servicecenter_uri_present >> rail.Label(
            'Yes') >> update_servicecenterschedule_assignments >> get_required_workweek_startdate
        is_servicecenter_uri_present >> rail.Label(
            'No') >> get_required_workweek_startdate
        is_assignment_category_present >> rail.Label(
            'No') >> get_required_workweek_startdate

        get_required_workweek_startdate >> is_workweek_startdate_present
        is_workweek_startdate_present >> rail.Label(
            'Yes') >> update_user_workweek >> get_required_office_schedule
        is_workweek_startdate_present >> rail.Label(
            'No') >> get_required_office_schedule
        get_required_office_schedule >> is_scheduleuri_not_present
        is_scheduleuri_not_present >> rail.Label(
            'Yes') >> create_officeschedule_draft >> update_schedule_name >> \
            put_simple_pattern >> publish_officeschedule >> update_schedulepolicychedule_assignments
        is_scheduleuri_not_present >> rail.Label(
            'No') >> update_schedulepolicychedule_assignments
        update_schedulepolicychedule_assignments >> entry
        get_timeoff_mapper >> get_timeoff_list >> is_timeoffs_to_assign
        is_timeoffs_to_assign >> rail.Label(
            'Yes') >> put_timeoff_type_assignments_user >> trigger_timeoff_adduser >> \
            write_adduser_log
        is_timeoffs_to_assign >> rail.Label(
            'No') >> write_adduser_log
        write_adduser_log >> catch_and_log_errors
        is_employeetype_uri_present >> rail.Label(
            'No') >> write_employeetype_uri_exception >> catch_and_log_errors
        catch_and_log_errors >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_adduser_child_dag)
