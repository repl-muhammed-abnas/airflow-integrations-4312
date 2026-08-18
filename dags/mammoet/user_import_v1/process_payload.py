from datetime import datetime, timedelta
import itertools
import rail
from rail.lib.ecid import get_dagrun_ecid
from mammoet.user_import_v1.utils.response_filter\
    import get_groups_data_handler, get_required_activities, get_all_holiday_calenders_data_handler
from mammoet.user_import_v1.utils import custom_methods
from airflow.models import Variable


null = None
# pylint: disable= too-many-statements


def create_process_payload_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.user_import_process_payload_child_dag_id,
        description="Mammoet Process Webhook Payload",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_payload_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='process_groups'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='process_groups',
            end_task='create_supervisor_log',
        )

        create_exception_log = rail.CreateLogOperator(
            task_id="create_exception_log"
        )

        compose_required_field = rail.DataAdaptorOperator(
            task_id="compose_required_field",
            source=lambda dag_run: dag_run.conf['users_data'],
            columns=['login_name', 'employee_status', 'employee_id',
                     'start_date', 'last_name', 'first_name', 'end_date',
                     'email_id', 'group_effective_start_date', 'time_zone',
                     'timeoff_profile_code', 'pay_grade_name', 'employee_type_name',
                     'location', 'location_code', 'country', 'country_code', 'country_iso_code', 'legal_entity', 'legal_entity_code',
                     'cost_center', 'cost_center_code', 'manager_id', 'office_schedule_name', 'payrule_name', 'voluntary_ot',
                     'legal_entity_full_path', 'voluntary_ot_effective_date','holiday_calendar_external_code'],
            data=lambda item: custom_methods.get_compose_required_field(
                item, config)
        )

        create_payload_collection = rail.CreateCollectionOperator(
            task_id="create_payload_collection",
            source=lambda: rail.result('compose_required_field'),
            name="payload_data",
            columns={
                "login_name": "login_name",
                "employee_status": "employee_status",
                "employee_id": "employee_id",
                "start_date": "start_date",
                "last_name": "last_name",
                "first_name": "first_name",
                "end_date": "end_date",
                "email_id": "email_id",
                "group_effective_start_date": "group_effective_start_date",
                "time_zone": "time_zone",
                "timeoff_profile_code": "timeoff_profile_code",
                "pay_grade_name": "pay_grade_name",
                "employee_type_name": "employee_type_name",
                "location": "location",
                "location_code": "location_code",
                "legal_entity": "legal_entity",
                "legal_entity_code": "legal_entity_code",
                "legal_entity_full_path": "legal_entity_full_path",
                "cost_center": "cost_center",
                "cost_center_code": "cost_center_code",
                "manager_id": "manager_id",
                "office_schedule_name": "office_schedule_name",
                "payrule_name": "payrule_name",
                "voluntary_ot": "voluntary_ot",
                'country': 'country',
                'country_code': 'country_code',
                'country_iso_code': 'country_iso_code',
                'voluntary_ot_effective_date': 'voluntary_ot_effective_date',
                'holiday_calendar_external_code': 'holiday_calendar_external_code'
            }
        )

        invalid_data_without_mandatory_fields = rail.QueryCollectionOperator(
            task_id="invalid_data_without_mandatory_fields",
            query="""SELECT * FROM payload_data pd
                    WHERE NULLIF(pd.login_name , '') IS NULL OR NULLIF(pd.employee_id , '') IS NULL OR
                    NULLIF(pd.last_name , '') IS NULL OR NULLIF(pd.first_name , '') IS NULL OR
                    NULLIF(pd.manager_id , '') IS NULL OR NULLIF(pd.payrule_name , '') IS NULL OR
                    NULLIF(pd.legal_entity , '') IS NULL OR NULLIF(pd.legal_entity_code , '') IS NULL OR
                    NULLIF(pd.country, '') IS NULL 
                """,
            name="invalid_payload_data"
        )

        MANDATORY_FIELDS = [('login_name', 'Login Name'), ('employee_id', 'Employee ID'), ('first_name', 'First Name'),
                            ('last_name', 'Last Name'), ('manager_id',
                                                         'Manager Id'), ('payrule_name', 'Payrule Name'),
                            ('legal_entity', 'Legal Entity'), ('legal_entity_code',
                                                               'Legal Entity Code'),
                            ('country', 'Country not found in mapper')]

        def get_missing_data_message(item):
            msg = []
            for key, log_msg in MANDATORY_FIELDS:
                if not item[key]:
                    if key == 'country':
                        log_msg = f"{log_msg} for code {item['legal_entity_code'][:2]}"
                    msg.append(log_msg)
            return f"{rail.smartjoin_by_delim(msg, ';')} missing in payload"

        log_invalid_data = rail.WriteLogOperator(
            task_id="log_invalid_data",
            log="{{result('create_exception_log')}}",
            severity="Exception",
            items="{{result('invalid_data_without_mandatory_fields')}}",
            message="Mandatory Field missing",
            properties=lambda dag_run, item: {
                "payload_identifier": dag_run.conf['payload_id'],
                "login_name": item['login_name'],
                "employee_id": item['employee_id'],
                "action": "Pre-Check",
                "status": "Exception",
                "details": get_missing_data_message(item)
            }
        )

        valid_data_with_mandatory_fields = rail.QueryCollectionOperator(
            task_id="valid_data_with_mandatory_fields",
            query="""SELECT * FROM payload_data pd
                    WHERE NULLIF(pd.login_name , '') IS NOT NULL AND NULLIF(pd.employee_id , '') IS NOT    NULL AND
                    NULLIF(pd.last_name , '') IS NOT NULL AND NULLIF(pd.first_name , '') IS NOT NULL AND
                    NULLIF(pd.manager_id , '') IS NOT NULL AND NULLIF(pd.payrule_name , '') IS NOT NULL AND
                    NULLIF(pd.legal_entity , '') IS NOT NULL AND NULLIF(pd.legal_entity_code , '') IS NOT NULL AND
                    NULLIF(pd.country, '') IS NOT NULL
                """,
            name="valid_payload_data"
        )

        has_any_valid_data = rail.IfOperator(
            task_id="has_any_valid_data",
            test="{{ result('valid_data_with_mandatory_fields', 'length') > 0 }}",
            yes_task="can_run_batch_task",
            no_task="start_log_generation"
        )

        process_groups = rail.TriggerDagRunOperator(
            task_id="process_groups",
            trigger_dag_id=config.user_import_process_groups_child_dag_id,
            retries=0,
            conf=lambda dag_run: {
                "payload_id": dag_run.conf['payload_id']
            }
        )

        wait_for_process_groups = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_groups",
            dag_runs="{{result('process_groups')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        query_valid_data_as_supervisor = rail.QueryCollectionOperator(
            task_id="query_valid_data_as_supervisor",
            query="""SELECT
                    vpd.*,
                    CASE WHEN vpd2.manager_id IS NOT NULL THEN 'yes' ELSE 'no' END AS is_supervisor
                FROM
                    valid_payload_data vpd
                LEFT JOIN
                    valid_payload_data vpd2  ON vpd.employee_id = vpd2.manager_id;""",
            name="valid_data_as_supervisor"
        )

        get_replicon_location_details = rail.RepliconServiceOperator(
            task_id="get_replicon_location_details",
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:location-list-column:location",
                    "urn:replicon:location-list-column:effectively-enabled",
                    "urn:replicon:location-list-column:full-path",
                    "urn:replicon:location-list-column:code"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_groups_data_handler
        )

        get_replicon_cost_center_details = rail.RepliconServiceOperator(
            task_id="get_replicon_cost_center_details",
            endpoint="/services/CostCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:cost-center-list-column:cost-center",
                    "urn:replicon:cost-center-list-column:effectively-enabled",
                    "urn:replicon:cost-center-list-column:full-path",
                    "urn:replicon:cost-center-list-column:code"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_groups_data_handler
        )

        get_replicon_legal_entities_details = rail.RepliconServiceOperator(
            task_id="get_replicon_legal_entities_details",
            endpoint="/services/DepartmentGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:department-group-list-column:department-group",
                    "urn:replicon:department-group-list-column:effectively-enabled",
                    "urn:replicon:department-group-list-column:full-path",
                    "urn:replicon:department-group-list-column:code"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_groups_data_handler
        )

        get_replicon_employee_type_details = rail.RepliconServiceOperator(
            task_id="get_replicon_employee_type_details",
            endpoint="/services/EmployeeTypeGroupListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:employee-type-group-list-column:employee-type-group",
                    "urn:replicon:employee-type-group-list-column:effectively-enabled",
                    "urn:replicon:employee-type-group-list-column:full-path",
                    "urn:replicon:employee-type-group-list-column:code"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_groups_data_handler
        )

        get_replicon_pay_grade_details = rail.RepliconServiceOperator(
            task_id="get_replicon_pay_grade_details",
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:service-center-list-column:service-center",
                    "urn:replicon:service-center-list-column:effectively-enabled",
                    "urn:replicon:service-center-list-column:full-path",
                    "urn:replicon:service-center-list-column:code"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_groups_data_handler
        )

        get_user_custom_field_group = rail.RepliconServiceOperator(
            task_id="get_user_custom_field_group",
            endpoint="/services/CustomFieldService1.svc/GetCustomFieldGroup",
            data={
                "objectTypeUri": "urn:replicon:object-type:user"
            }
        )

        get_all_user_custom_fields = rail.RepliconServiceOperator(
            task_id="get_all_user_custom_fields",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "{{ result('get_user_custom_field_group').uri }}"
            }
        )

        get_all_polices = rail.RepliconServiceOperator(
            task_id="get_all_polices",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets"
        )

        get_all_holiday_calenders = rail.RepliconServiceOperator(
            task_id="get_all_holiday_calenders",
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            data_handler=get_all_holiday_calenders_data_handler
        )

        get_all_timezones = rail.RepliconServiceOperator(
            task_id='get_all_timezones',
            endpoint='/services/InternationalizationService1.svc/GetAllTimeZones',
        )

        get_all_timeoffs = rail.RepliconServiceOperator(
            task_id="get_all_timeoffs",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id="get_all_permission_sets",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets"
        )

        get_all_activities = rail.RepliconServiceOperator(
            task_id="get_all_activities",
            endpoint="/services/ActivityService1.svc/GetAllActivities",
            data_handler=get_required_activities
        )

        get_all_payrule_scripts = rail.RepliconServiceOperator(
            task_id="get_all_payrule_scripts",
            endpoint="/services/PayRuleScriptService2.svc/GetAllScripts"
        )

        get_all_office_schedule = rail.RepliconServiceOperator(
            task_id="get_all_office_schedule",
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules"
        )

        create_supervisor_log = rail.CreateLogOperator(
            task_id="create_supervisor_log"
        )

        process_each_user = rail.trigger_parallel_dagrun(
            task_id="process_each_user",
            trigger_dag_id=config.user_import_process_users_child_dag_id,
            execution_timeout=timedelta(days=14),
            parallel_count=config.trigger_parallel_dagrun_count_process_users,
            items="{{result('query_valid_data_as_supervisor')}}",
            conf=lambda dag_run, item: custom_methods.process_each_user_conf(
                dag_run, item, config)
        )

        start_supervisor_processing = rail.EmptyOperator(
            task_id="start_supervisor_processing"
        )

        process_supervisor_assignment = rail.trigger_parallel_dagrun(
            task_id="process_supervisor_assignment",
            trigger_dag_id=config.user_import_process_supervisor_assignment_dag_id,
            execution_timeout=timedelta(days=14),
            parallel_count=config.trigger_parallel_dagrun_count_process_users,
            items="{{result('create_supervisor_log')}}",
            conf=lambda item: {
                **item['properties'],
                **{
                    "user_permissions": {
                        "supervisor": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'), 'name', 'Supervisor'),
                        "basic": rail.find_first_by_attr_and_get_attr(rail.result('get_all_permission_sets'), 'name', 'Project Resource with Reports'),
                    }
                }
            }
        )

        start_log_generation = rail.EmptyOperator(
            task_id="start_log_generation"
        )

        get_all_process_user_dag_runs = rail.PythonOperator(
            task_id="get_all_process_user_dag_runs",
            python_callable=lambda: list(itertools.chain(
                *list(filter(None, map(lambda x: rail.result(
                    f'process_each_user_{x+1}'), range(config.trigger_parallel_dagrun_count_process_users)))))),
            show_return_value_in_logs=False
        )

        get_process_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id="get_process_user_logs",
            dag_runs="{{result('get_all_process_user_dag_runs')}}",
            dagrun_task_id="create_user_log"
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation_dagid,
            conf=lambda dag_run: {
                "payload_id": dag_run.conf['payload_id'],
                'logs': rail.result('get_process_user_logs'),
                'exception_log': rail.result('create_exception_log'),
                # pylint: disable=line-too-long
                'log_filename': f'log_{ get_dagrun_ecid(dag_run).replace(":", "-")}_{datetime.now().strftime("%Y%m%dT%H%M%S")}.csv'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule="all_done"
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test="{{get_error_message() | is_truthy}}",
            yes_task="fail_dag"
        )

        fail_dag = rail.FailOperator(
            task_id="fail_dag",
            message="{{get_error_message()}}"
        )
        create_exception_log >> compose_required_field >> create_payload_collection >> invalid_data_without_mandatory_fields >> log_invalid_data
        log_invalid_data >> valid_data_with_mandatory_fields >> has_any_valid_data >> rail.Label(
            "No") >> start_log_generation

        has_any_valid_data >> rail.Label("Yes") >> can_run_batch_task >> rail.Label("No") \
            >> process_groups >> wait_for_process_groups
        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> create_supervisor_log

        wait_for_process_groups >> query_valid_data_as_supervisor >> get_replicon_location_details >> get_replicon_legal_entities_details \
            >> get_replicon_cost_center_details >> get_replicon_employee_type_details >> get_replicon_pay_grade_details >> get_user_custom_field_group\
            >> get_all_user_custom_fields >> get_all_polices \
                >> get_all_holiday_calenders >> get_all_payrule_scripts >> get_all_office_schedule >> get_all_timezones >> get_all_timeoffs \
            >> get_all_permission_sets >> get_all_activities >> create_supervisor_log >> process_each_user

        process_each_user >> start_supervisor_processing >> process_supervisor_assignment >> start_log_generation \
            >> get_all_process_user_dag_runs >> get_process_user_logs\
            >> process_log_generation >> log_to_sumo >> can_fail_dag >> rail.Label("Yes") >> fail_dag
    return dag


rail.for_each_instance(create_process_payload_dag)
