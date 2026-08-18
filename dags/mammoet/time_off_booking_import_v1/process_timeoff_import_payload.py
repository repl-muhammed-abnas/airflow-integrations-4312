from datetime import timedelta, datetime
import itertools
import rail
from rail.lib.ecid import get_dagrun_ecid

from mammoet.time_off_booking_import_v1.utils import request_payload, response_filter

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_timeoff_import_payload_dagid,
        description="Mammoet Time Off Booking Import Process Payload",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        max_active_runs=config.max_active_runs_process_timeoff_import_payload,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source=lambda dag_run: dag_run.conf['payload']['timeoffs'],
            name="input_data_collection",
            columns={
                'external_code': 'sf_booking_id',
                'start_date': 'start_date',
                'end_date': 'end_date',
                'time_type_external_code': 'time_off_type_description',
                'start_time': 'start_time',
                'end_time': 'end_time',
                'no_of_days': 'days',
                'user_id': 'employee_id',
                'approval_status': 'time_off_booking_status',
            }
        )

        has_input_data = rail.IfOperator(
            task_id='has_input_data',
            test="{{ result('create_input_data_collection','length') > 0 }}",
            yes_task='create_log',
            no_task='send_blank_payload_email'
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log',
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            subject='{{ get_company_key() }} | Time Off Booking Import - no records in payload - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html"
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            query="""SELECT * FROM input_data_collection WHERE NULLIF(sf_booking_id, '') IS NULL or
                    NULLIF(start_date, '') IS NULL or NULLIF(end_date, '') IS NULL
                    or NULLIF(time_off_type_description, '') IS NULL or NULLIF(employee_id, '') IS NULL
                    or NULLIF(time_off_booking_status, '') IS NULL or NULLIF(days, '') IS NULL"""
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            items='{{result("query_invalid_records")}}',
            log="{{result('create_log')}}",
            message=request_payload.get_mandatory_fields_exception_message,
            severity='Exception',
            properties=lambda item: {
                'sf_booking_id': item['sf_booking_id'],
                'employee_id': item['employee_id'],
                'time_off_type_description': item['time_off_type_description'],
                'action':'Validation',
                'status': 'Exception',
                "details": request_payload.get_mandatory_fields_exception_message(item)
            }
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            name='valid_records',
            query="""SELECT * FROM input_data_collection WHERE NULLIF(sf_booking_id, '') IS NOT NULL and
                    NULLIF(start_date, '') IS NOT NULL and NULLIF(end_date, '') IS NOT NULL
                    and NULLIF(time_off_type_description, '') IS NOT NULL and NULLIF(employee_id, '') IS NOT NULL
                    and NULLIF(time_off_booking_status, '') IS NOT NULL and NULLIF(days, '') IS NOT NULL"""
        )

        query_distinct_employees = rail.QueryCollectionOperator(
            task_id='query_distinct_employees',
            query='''SELECT DISTINCT employee_id FROM valid_records'''
        )

        get_hidden_oef_value = rail.RepliconServiceOperator(
            task_id='get_hidden_oef_value',
            endpoint='/services/ObjectExtensionDefinitionListService1.svc/GetData',
            data=request_payload.get_hidden_oef_value_payload,
            data_handler=response_filter.get_hidden_oef_value
        )

        get_all_time_off_types_uris = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types_uris',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            data_handler=response_filter.get_time_off_type_uris
        )

        get_timeoff_details = rail.RepliconServiceOperator(
            task_id='get_timeoff_details',
            endpoint='/services/TimeOffService1.svc/BulkGetTimeOffTypeDetails',
            data=lambda: {
                "timeOffTypeUris": rail.result('get_all_time_off_types_uris')
            },
            data_handler=response_filter.get_filtered_timeoff_details
        )

        process_distinct_employees = rail.trigger_parallel_dagrun(
            task_id='process_distinct_employees',
            items="{{ result('query_distinct_employees') }}",
            parallel_count=config.trigger_parallel_dagrun_count_process_distinct_employees,
            trigger_dag_id=config.process_distinct_employees_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item:{
                'employee_id': item['employee_id'],
                'timeoff_details': rail.result('get_timeoff_details'),
                'hidden_oef_value': rail.result('get_hidden_oef_value')['hidden_oef_value'],
                'employee_log': rail.result('create_employee_log')
            }
        )

        get_process_distinct_employees_dag_ids =rail.PythonOperator(
            task_id= 'get_process_distinct_employees_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_distinct_employees_{x+1}'), range(config.trigger_parallel_dagrun_count_process_distinct_employees))))),
            show_return_value_in_logs= False
        )

        gather_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_logs',
            dag_runs='{{ result("get_process_distinct_employees_dag_ids") }}',
            dagrun_task_id='create_employee_log',
            flatten=True
        )

        get_log_file_name = rail.PythonOperator(
            task_id = 'get_log_file_name',
            python_callable= lambda dag_run: f'log_{ get_dagrun_ecid(dag_run).split(":")[0]}_{datetime.now().strftime("%Y%m%dT%H%M%S")}.csv'
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation_dagid,
            conf=lambda :{
                'userlogs': rail.result('gather_logs'),
                'otherlogs': rail.result('create_log'),
                'log_filename': rail.result('get_log_file_name')
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "payload_identifer": "{{dag_run.conf.payload.payload_identifier}}",
                "no_of_timeoff_records_in_payload":  "{{result('create_input_data_collection','length')}}",
                "log_file_name": '{{result("get_log_file_name")}}'
            }
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

        create_input_data_collection >> has_input_data >> rail.Label('No') >> send_blank_payload_email
        has_input_data >> rail.Label('Yes') >> create_log >> query_invalid_records >> log_invalid_records >> query_valid_records
        query_valid_records >> query_distinct_employees >> get_hidden_oef_value >> get_all_time_off_types_uris
        get_all_time_off_types_uris >> get_timeoff_details >> process_distinct_employees >> get_process_distinct_employees_dag_ids
        get_process_distinct_employees_dag_ids >> gather_logs >> get_log_file_name >> process_log_generation >> log_to_sumo
        log_to_sumo >> can_fail_dag >> rail.Label("Yes") >> fail_dag

    return dag

rail.for_each_instance(create_child_dag)
