from datetime import timedelta, datetime as dt
from rail.lib.ecid import get_dagrun_ecid
import rail

from raynetsas.user_import.utils import request_payload,response_filter,custom_methods

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_user_child_dagid,
        description='raynetsas - User Import',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source=lambda dag_run: dag_run.conf['payload'],
            name="input_data_collection",
            columns={
                "first_name": "first_name",
                "last_name": "last_name",
                "email": "email",
                "country": "country",
                "cost_center": "cost_center"
            }
        )

        has_input_data = rail.IfOperator(
            task_id='has_input_data',
            test="{{ result('create_input_data_collection','length') > 0 }}",
            yes_task='create_log'
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log',
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            query="""SELECT * FROM input_data_collection WHERE NULLIF(first_name, '') IS NULL or
                    NULLIF(last_name, '') IS NULL or NULLIF(email, '') IS NULL
                    or NULLIF(country, '') IS NULL"""
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            items='{{result("query_invalid_records")}}',
            log="{{result('create_log')}}",
            message=request_payload.get_missing_field_message,
            severity='Exception',
            properties=lambda item: {
                'email': item['email'],
                'first_name': item['first_name'],
                'last_name': item['last_name'],
                "country": item['country'],
                'action':'Validation',
                'status': 'Exception',
                "details": request_payload.get_missing_field_message(item)
            }
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            name='valid_records',
            query="""SELECT ROW_NUMBER() OVER(ORDER BY ROWID) AS record_id,* FROM input_data_collection WHERE NULLIF(first_name, '') IS NOT NULL
                    and NULLIF(last_name, '') IS NOT NULL and NULLIF(email, '') IS NOT NULL and NULLIF(country, '') IS NOT NULL"""
        )

        has_valid_records = rail.IfOperator(
            task_id="has_valid_records",
            test="{{result('query_valid_records', 'length') > 0}}",
            yes_task='get_all_permission_set',
            no_task="format_logs"
        )

        get_all_permission_set = rail.RepliconServiceOperator(
            task_id="get_all_permission_set",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        get_all_policy_sets = rail.RepliconServiceOperator(
            task_id="get_all_policy_sets",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
        )

        get_all_locations = rail.RepliconServiceOperator(
            task_id="get_all_locations",
            endpoint="/services/LocationService1.svc/GetEnabledLocations",
        )

        get_all_service_centers = rail.RepliconServiceOperator(
            task_id="get_all_service_centers",
            endpoint="/services/ServiceCenterService1.svc/GetEnabledServiceCenters",
        )

        get_all_cost_centers = rail.RepliconServiceOperator(
            task_id="get_all_cost_centers",
            endpoint="/services/CostCenterService1.svc/GetEnabledCostCenters",
        )

        get_all_time_off_types = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types',
            endpoint='/services/TimeOffService1.svc/GetAllTimeOffTypes',
            data_handler=lambda response: response_filter.get_filtered_time_off_types(response, config.timeoff_types)
        )

        get_all_licenses = rail.RepliconServiceOperator(
            task_id="get_all_licenses",
            endpoint="/services/AccountManagementService1.svc/GetAllPublicLicensedProducts",
            data_handler=lambda response: response_filter.get_filtered_licenses(response, config.licenses)
        )

        process_users = rail.trigger_parallel_dagrun(
            task_id='process_users',
            items="{{ result('query_valid_records') }}",
            parallel_count=config.USER_BATCH_COUNT,
            trigger_dag_id=lambda item: f"{config.process_each_user_dagid}_batch_{str(int(item['record_id']) % config.USER_BATCH_COUNT)}",
            conf=lambda item: request_payload.get_child_conf(item,config),
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=custom_methods.do_format_logs,
            show_return_value_in_logs=False
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=[
                'Email',
                'First Name',
                'Last Name',
                'Country',
                'Action',
                'Status',
                'Details',
                'JobID'
            ],
            row=[
                '{{ item.properties.email }}',
                '{{ item.properties.first_name }}',
                '{{ item.properties.last_name }}',
                '{{ item.properties.country }}',
                '{{ item.properties.action }}',
                '{{ item.properties.status }}',
                '{{ item.properties.details }}',
                '{{ item.ecid }}'],
        )

        get_log_file_name = rail.PythonOperator(
            task_id = 'get_log_file_name',
            python_callable=lambda dag_run: get_dagrun_ecid(dag_run).split(":")[0] + '_' + dt.now().strftime('%m%d%YT%H%M%S')
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='Logs_{{result("get_log_file_name")}}.csv',
            expires_in_seconds=7*24*60*60,
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_logs', 'error') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon User Import - " }} \
                {%- if result("format_logs", key="get_errored_logs") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="get_exception_logs") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz() }}',
            html_content="templates/emails/import_complete_mail.html"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "no_of_user_records_in_payload":  "{{result('create_input_data_collection','length')}}",
                "no_of_valid_user_records": "{{result('query_valid_records','length')}}",
                "no_of_invalid_user_records": "{{result('query_invalid_records','length')}}",
                "log_file_name": '{{ result("get_log_file_name") }}'
            }
        )


        create_input_data_collection >> has_input_data >> rail.Label(
            'Yes') >> create_log >> query_invalid_records >> log_invalid_records >> query_valid_records

        query_valid_records >> has_valid_records >> rail.Label(
            'Yes') >> get_all_permission_set >> get_all_policy_sets >> get_all_locations >>\
                get_all_service_centers >> get_all_cost_centers >> get_all_time_off_types >> get_all_licenses >> process_users

        has_valid_records >> rail.Label(
            'No') >>  format_logs

        process_users >> format_logs >> render_logs_csv >> get_log_file_name >> generate_download_link >>\
            send_import_complete_email >> log_to_sumo

    return dag

rail.for_each_instance(create_child_dag)
