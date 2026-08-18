from datetime import datetime, timedelta, timezone
from os import path
from rail.filters import split
from rail.lib.artifact import existing_artifact
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/balparag3/project_import/config.py


# pylint: disable=too-many-statements
def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'balparag3_projectimport_add_master_{config.instance}',
        description=f'BalparaG3 Project Add master V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=30),
        max_active_runs=config.master_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_file',
            no_task='should_fail_dag'
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        download_email_file = rail.SFTPDownloadFileOperator(
            task_id='download_email_file',
            remote_filepath=config.fromaddress_filepath +
            "/{{ result('new_file_sensor') | file_name | replace('.csv', '.txt') }}"
        )

        def get_email_file_data():
            with existing_artifact(rail.result('download_email_file'), mode='r') as artifact:
                email_file_data = artifact.file.read()
                return email_file_data
        get_email_id = rail.PythonOperator(
            task_id='get_email_id',
            python_callable=get_email_file_data
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun'
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        log_date_time = rail.PythonOperator(
            task_id='log_date_time',
            python_callable=lambda: datetime.now(
                timezone.utc).strftime('%d/%m/%YT%H:%M:%S')
        )

        load_input_csv = rail.LoadCSVFileOperator(
            task_id='load_input_csv',
            document="{{ result('download_file') }}",
            encoding='utf-8-sig',
            headers=["Project Code", "Project Name", "Client Name", "Client Contact", "Project Managers", "Estimated Hours",
                     "Estimated Cost", "Department", "Billing Rates", "Actual Project Start Date", "Client Project #",
                     "PO #", "PMO #", "Work Order #", "Comments", "Invoice Client", "Invoice Client Contact",
                     "Invoice Balpara Contact", "Invoice Client Code", "Users", "Location", "Project Manager"]
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source="{{ result('load_input_csv') }}",
            name="inputdata",
            columns={
                'Project Code': 'projectcode',
                'Project Name': 'projectname',
                'Client Name': 'clientname',
                'Client Contact': 'clientcontact',
                'Project Managers': 'projectmanagers',
                'Estimated Hours': 'estimatedhours',
                'Estimated Cost': 'estimatedcost',
                'Department': 'department',
                'Billing Rates': 'billingrates',
                'Actual Project Start Date': 'actualprojectstartdate',
                'Client Project #': 'clientproject',
                'PO #': 'po',
                'PMO #': 'pmo',
                'Work Order #': 'workorder',
                'Comments': 'comments',
                'Invoice Client': 'invoiceclient',
                'Invoice Client Contact': 'invoiceclientcontact',
                'Invoice Balpara Contact': 'invoicebalparacontact',
                'Invoice Client Code': 'invoiceclientcode',
                'Users': 'users',
                'Location': 'location',
                'Project Manager': 'projectmanager'
            }
        )

        is_empty_payload = rail.IfOperator(
            task_id='is_empty_payload',
            test="{{ result('create_input_data_collection', 'length') < 1 }}",
            yes_task="send_blank_payload_email",
            no_task="query_requiredfields_not_available"
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to="{{ result('get_email_id') }}",
            bcc=config.internal_logs_email,
            subject="{{ get_company_key() }} | Project import (PIF Template) - no records in file {{ result('log_date_time') }}",
            html_content='email_templates/empty_payload.html'
        )

        query_requiredfields_not_available = rail.QueryCollectionOperator(
            task_id='query_requiredfields_not_available',
            query="""SELECT * FROM inputdata WHERE NULLIF(projectcode,'') IS NULL OR NULLIF(projectname,'') IS NULL OR
                    NULLIF(clientname,'') IS NULL OR NULLIF(clientcontact,'') IS NULL OR NULLIF(projectmanagers,'') IS NULL OR
                    NULLIF(estimatedhours,'') IS NULL OR NULLIF(estimatedcost,'') IS NULL OR NULLIF(billingrates,'') IS NULL OR
                    NULLIF(actualprojectstartdate,'') IS NULL OR NULLIF(invoiceclientcontact,'') IS NULL OR
                    NULLIF(invoiceclientcode,'') IS NULL OR NULLIF(projectmanager,'') IS NULL"""
        )

        is_requiredfields_not_available = rail.IfOperator(
            task_id='is_requiredfields_not_available',
            test="{{ result('query_requiredfields_not_available', 'length') > 0 }}",
            yes_task="create_requiredfields_not_available_log",
            no_task="get_validatedinputdata"
        )

        create_requiredfields_not_available_log = rail.CreateLogOperator(
            task_id='create_requiredfields_not_available_log'
        )

        def write_skipped_project(item):
            def get_exception_message(item):
                message = []
                projectcode = item['projectcode']
                projectname = item['projectname']
                clientname = item['clientname']
                clientcontact = item['clientcontact']
                projectmanagers = item['projectmanagers']
                estimatedhours = item['estimatedhours']
                estimatedcost = item['estimatedcost']
                billingrates = item['billingrates']
                actualprojectstartdate = item['actualprojectstartdate']
                invoiceclientcontact = item['invoiceclientcontact']
                invoiceclientcode = item['invoiceclientcode']
                projectmanager = item['projectmanager']
                if not projectcode:
                    message.append('Project code is blank')
                if not projectname:
                    message.append('Project name is blank')
                if not clientname:
                    message.append('Client name is blank')
                if not clientcontact:
                    message.append('Client contact is blank')
                if not projectmanagers:
                    message.append('Project manager (UDF) is blank')
                if not estimatedhours:
                    message.append('Estimated hours is blank')
                if not estimatedcost:
                    message.append('Estimated cost is blank')
                if not billingrates:
                    message.append('Billing rates is blank')
                if not actualprojectstartdate:
                    message.append('Actual project start date is blank')
                if not invoiceclientcontact:
                    message.append('Invoice client contact is blank')
                if not invoiceclientcode:
                    message.append('Invoice client code is blank')
                if not projectmanager:
                    message.append('Project manager (Default) is blank')
                return ','.join(message) if message else ''
            return {
                'project_code': item['projectcode'],
                'project_name': item['projectname'],
                'status': 'Skipped',
                'details': get_exception_message(item),
                'type': 'validation'
            }
        write_mandatory_fieldlog_exception = rail.WriteLogOperator(
            task_id='write_mandatory_fieldlog_exception',
            items="{{ result('query_requiredfields_not_available') }}",
            log="{{ result('create_requiredfields_not_available_log') }}",
            message="mandatory field log exception",
            severity="Skipped",
            properties=write_skipped_project
        )

        get_validatedinputdata = rail.QueryCollectionOperator(
            task_id='get_validatedinputdata',
            query="""SELECT * FROM inputdata WHERE
                    NULLIF(projectcode,'') IS NOT NULL AND NULLIF(projectname,'') IS NOT NULL 
                    AND NULLIF(clientname,'') IS NOT NULL AND NULLIF(clientcontact,'') IS NOT NULL 
                    AND NULLIF(projectmanagers,'') IS NOT NULL AND NULLIF(estimatedhours,'') IS NOT NULL 
                    AND NULLIF(estimatedcost,'') IS NOT NULL AND NULLIF(billingrates,'') IS NOT NULL 
                    AND NULLIF(actualprojectstartdate,'') IS NOT NULL AND NULLIF(invoiceclientcontact,'') IS NOT NULL 
                    AND NULLIF(invoiceclientcode,'') IS NOT NULL AND NULLIF(projectmanager,'') IS NOT NULL""",
            name='validatedinputdata'
        )

        is_query_requiredfields_greater_0 = rail.IfOperator(
            task_id='is_query_requiredfields_greater_0',
            test="{{ result('get_validatedinputdata', 'length') > 0 }}",
            yes_task=["get_base_currency", "get_enabled_locations", "get_department_client_custom_field",
                      "get_required_project_custom_fields", "get_required_project_oefs",
                      "query_unique_clients"],
            no_task="process_log_generation"
        )

        get_base_currency = rail.RepliconServiceOperator(
            task_id='get_base_currency',
            endpoint="/services/CurrencyService2.svc/GetBaseCurrency"
        )

        get_enabled_locations = rail.RepliconServiceOperator(
            task_id='get_enabled_locations',
            endpoint="/services/LocationService1.svc/GetEnabledLocations"
        )

        get_department_client_custom_field = rail.RepliconServiceOperator(
            task_id='get_department_client_custom_field',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                'objectUri': 'urn:replicon:object-type:client'
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Department', 'uri', '')
        )

        get_required_project_custom_fields = rail.RepliconServiceOperator(
            task_id='get_required_project_custom_fields',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:project"
            },
            data_handler=lambda response: {
                'project_manager_udf_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Project Managers', 'uri', ''),
                'actual_projectstartdate_udf_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Actual Project Start Date', 'uri', ''),
                'client_project_udf_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Client Project #', 'uri', ''),
                'po_udf_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'PO #', 'uri', ''),
                'comments_udf_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Comments', 'uri', ''),
                'workorder_udf_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Work Order #', 'uri', ''),
                'pmo_udf_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'PMO #', 'uri', ''),
                'invoiceclientcontact_udf_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Invoice Client Contact', 'uri', ''),
                'invoiceclientcode_udf_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Invoice Client Code', 'uri', '')
            }
        )

        get_required_project_oefs = rail.RepliconServiceOperator(
            task_id='get_required_project_oefs',
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:project"
            },
            data_handler=lambda response: {
                'invoicebalparacontact_oef_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'name', 'Invoice Balpara Contact', 'uri', ''),
                'projectcreatedate_oef_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'name', 'Project Created Date', 'uri', ''),
                'clientcontact_oef_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'name', 'Client Contact', 'uri', '')
            }
        )

        get_required_department_client_dropdown = rail.RepliconServiceOperator(
            task_id='get_required_department_client_dropdown',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                "customFieldUri": "{{ result('get_department_client_custom_field') }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Other', 'uri', '')
        )

        query_unique_clients = rail.QueryCollectionOperator(
            task_id='query_unique_clients',
            query="""SELECT DISTINCT clientname FROM validatedinputdata"""
        )

        process_client_creation = rail.TriggerDagRunForEachItemOperator(
            task_id='process_client_creation',
            retries=0,
            items="{{ result('query_unique_clients') }}",
            trigger_dag_id=f'balparag3_projectimport_client_validation_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf={
                "clientname": "{{ item.clientname }}",
                "department_udf_uri": "{{ result('get_department_client_custom_field') }}",
                "department_udf_dropdown_uri": "{{ result('get_required_department_client_dropdown') }}",
                "validated_input_data": "{{ result('get_validatedinputdata') }}"
            }
        )

        wait_for_process_client_creation = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_client_creation',
            dag_runs='{{ result("process_client_creation") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        query_unique_projects = rail.QueryCollectionOperator(
            task_id='query_unique_projects',
            query="""SELECT DISTINCT projectcode, projectname FROM validatedinputdata"""
        )

        def get_process_project_conf(item):
            return {
                **{k.lower(): v for k, v in item.items()},
                **{
                    'defaultcurrencysymbol': rail.result('get_base_currency')['symbol']
                },
                **dict(rail.result('get_required_project_custom_fields').items()),
                **dict(rail.result('get_required_project_oefs').items()),
                **{
                    'requester': rail.result('get_email_id'),
                    'get_enabled_locations': rail.result('get_enabled_locations')
                }
            }
        process_projects = rail.TriggerDagRunForEachItemOperator(
            task_id='process_projects',
            items="{{ result('query_unique_projects') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'balparag3_projectimport_child_process_project_{config.instance}',
            conf=get_process_project_conf
        )

        wait_for_process_projects = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_projects',
            dag_runs='{{ result("process_projects") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_logs',
            dag_runs="{{ result('process_projects') }}",
            dagrun_task_id='create_log',
            flatten=True
        )

        def get_all_userlogs():
            logs = []
            if rail.result('create_requiredfields_not_available_log'):
                logs.append(rail.result(
                    'create_requiredfields_not_available_log'))
            if rail.result('gather_logs'):
                logs.extend(rail.result('gather_logs'))
            return logs
        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            trigger_dag_id=f'balparag3_projectimport_child_log_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf=lambda: {
                "filename": split(string=path.split(rail.result('new_file_sensor'))[1], separator='.')[0],
                "user_logs": get_all_userlogs(),
                "tenant_email": rail.result('get_email_id')
            }
        )

        should_fail_dag = rail.IfOperator(
            task_id='should_fail_dag',
            trigger_rule='all_done',
            test="{{ get_failed_upstream_task_ids() | length > 0 }}",
            yes_task='fail_dag',
            no_task='process_logtosumo'
        )

        fail_dag = rail.FailOperator(
            task_id='fail_dag',
            message="{{ get_error_message() }}"
        )

        process_logtosumo = rail.EmptyOperator(
            task_id='process_logtosumo'
        )

        check_if_new_file_found = rail.IfOperator(
            task_id='check_if_new_file_found',
            test="{{ get_task_state('new_file_sensor') == 'success' }}",
            yes_task='log_dagrun_to_sumo'
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id=config.dagrun_log_conn_id,
            extra_info={
                'Filename': "{{ result('new_file_sensor') | file_base }}"
            }
        )

        new_file_sensor >> is_csv

        is_csv >> rail.Label(
            'Yes') >> download_file >> download_email_file >> get_email_id

        get_email_id >> rail.Label(
            'Always') >> was_new_file_found

        was_new_file_found >> rail.Label(
            'Yes') >> archive_file

        was_new_file_found >> rail.Label(
            'No') >> delete_this_dagrun

        get_email_id >> log_date_time >> load_input_csv >> create_input_data_collection >> is_empty_payload

        is_csv >> rail.Label(
            'No') >> should_fail_dag

        is_empty_payload >> rail.Label(
            'Yes') >> send_blank_payload_email >> should_fail_dag
        is_empty_payload >> rail.Label(
            'No') >> query_requiredfields_not_available >> is_requiredfields_not_available

        is_requiredfields_not_available >> rail.Label(
            'Yes') >> create_requiredfields_not_available_log >> \
            write_mandatory_fieldlog_exception >> get_validatedinputdata
        is_requiredfields_not_available >> rail.Label(
            'No') >> get_validatedinputdata

        get_validatedinputdata >> is_query_requiredfields_greater_0

        is_query_requiredfields_greater_0 >> rail.Label(
            'Yes') >> [get_base_currency, get_enabled_locations, get_department_client_custom_field,
                       get_required_project_custom_fields, get_required_project_oefs, query_unique_clients] >> \
            get_required_department_client_dropdown >> process_client_creation >> \
            wait_for_process_client_creation >> query_unique_projects >> \
            process_projects >> wait_for_process_projects >> \
            gather_logs >> process_log_generation
        is_query_requiredfields_greater_0 >> rail.Label(
            'No') >> process_log_generation

        process_log_generation >> should_fail_dag

        should_fail_dag >> rail.Label(
            'Yes') >> fail_dag

        should_fail_dag >> rail.Label(
            'No') >> process_logtosumo >> check_if_new_file_found

        check_if_new_file_found >> rail.Label(
            'Yes') >> log_dagrun_to_sumo

    return dag


rail.for_each_instance(create_main_airflow_dag)
