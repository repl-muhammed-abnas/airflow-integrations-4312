from datetime import timedelta
import rail
from matlensilver.client_project_task_sync import response_filter
from matlensilver.client_project_task_sync import request_payload
from matlensilver.client_project_task_sync.send_logs import get_send_logs

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/matlensilver/client_project_task_sync/config.py

# pylint: disable=too-many-statements


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'matlensilver_client_project_task_sync_master_{config.instance}',
        description=f'Matlen_Silver_Client_Project_Task_Sync_Automation Master - SFTP {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10),
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task='download_file',
            no_task='send_bad_file_format_email'
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Project sync into Replicon - Incorrect Format - {{ current_time_in_specified_tz() }}',
            html_content="email_bad_file_format.html"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            trigger_rule='all_done',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        load_csv_data = rail.LoadCSVFileOperator(
            task_id='load_csv_data',
            document="{{ result('download_file') }}",
            encoding='utf-8-sig'
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source="{{ result('load_csv_data') }}",
            name="inputdatacollection",
            columns={
                'Client Name': 'clientname',
                'ClientID': 'clientid',
                'Client Manager': 'clientmanager',
                'Description': 'description',
                'Currency': 'currency',
                'Tax': 'tax',
                'Client Street': 'clientstreet',
                'Client City': 'clientcity',
                'Client State': 'clientstate',
                'Client Country': 'clientcountry',
                'Client Zip': 'clientzip',
                'Billing Client': 'billingclient',
                'Project ID': 'projectid',
                'Project Name': 'projectname',
                'Project Status': 'projectstatus',
                'Project Start Date': 'projectstartdate',
                'Project End Date': 'projectenddate',
                'Project Type': 'projecttype',
                'Assignment ID': 'assignmentid',
                'Assignment Title': 'assignmenttitle',
                'Assignment Start Date': 'assignmentstartdate',
                'Assignment End Date': 'assignmentenddate',
                'Assignment Status': 'assignmentstatus',
                'Person ID': 'personid',
                'Solomon ID': 'solomonid',
                'Client Contact Assignment Level': 'clientcontactassignmentlevel',
                'Assignment Contact Email Assignment Level': 'assignmentcontactemail',
                'Assignment Contact Assignment Level': 'assignmentcontact',
                'Assignment Billing Client Assignment Level': 'assignmentbillingclient',
                'Billing Client Street Assignment Level': 'billingclientstreet',
                'Billing Client City Assignment Level': 'billingclientcity',
                'Billing Client State Assignment Level': 'billingclientstate',
                'Billing Client Country Assignment Level': 'billingclientcountry',
                'Billing Client Zip Assignment Level': 'billingclientzip',
                'Project Client Contact Assignment Level': 'projectclientcontact',
            }
        )

        has_input_data = rail.IfOperator(
            task_id='has_input_data',
            test="{{ result('create_input_data_collection','length') > 0 }}",
            yes_task='query_client_data',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Project sync into Replicon - no records in file - {{ current_time_in_specified_tz() }}',
            html_content="email_blank_payload.html"
        )

        query_client_data = rail.QueryCollectionOperator(
            task_id='query_client_data',
            query='''SELECT DISTINCT clientname, clientid, description,
            currency, tax, clientstreet, clientcity, clientstate, clientzip, billingclient FROM inputdatacollection'''
        )

        get_all_custom_fields = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields',
            endpoint='services/CustomFieldService1.svc/GetAllCustomFields',
            data={"objectUri": "urn:replicon:object-type:client"}
        )

        process_clients = rail.TriggerDagRunForEachItemOperator(
            task_id='process_clients',
            retries=0,
            items="{{ result('query_client_data') }}",
            execution_timeout=timedelta(days=14),
            trigger_dag_id=f'matlensilver_client_project_task_sync_process_clients_{config.instance}',
            conf=lambda item: {
                'clientname': item['clientname'],
                'clientid': item['clientid'],
                'description': item['description'],
                'currency': item['currency'],
                'tax': item['tax'],
                'clientstreet': item['clientstreet'],
                'clientcity': item['clientcity'],
                'clientstate': item['clientstate'],
                'clientzip': item['clientzip'],
                'billingclient': item['billingclient'],
                'billingclienturi': rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'Billing Client', 'uri'),
                'filename': (rail.result('new_file_sensor')).split('/')[-1]

            }
        )

        wait_for_process_clients = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_clients',
            dag_runs='{{ result("process_clients") }}',
            retries=0,
            execution_timeout=timedelta(days=14)
        )

        get_client_success = rail.GatherResultsFromDagRunsOperator(
            task_id='get_client_success',
            dag_runs="{{ result('process_clients') }}",
            dagrun_task_id='get_client_success_status',
            flatten=True,
        )

        get_client_error = rail.GatherResultsFromDagRunsOperator(
            task_id='get_client_error',
            dag_runs="{{ result('process_clients') }}",
            dagrun_task_id='get_client_errors_status',
            flatten=True,
        )

        query_project_data = rail.QueryCollectionOperator(
            task_id='query_project_data',
            query='''SELECT DISTINCT clientname,clientid,projectid,projectname,projectstatus,
            projectstartdate,projectenddate,projecttype FROM inputdatacollection'''
        )

        get_project_oefs = rail.RepliconServiceOperator(
            task_id="get_project_oefs",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={"bindingContextUri": "urn:replicon:object-type:project"},
            data_handler=lambda oefs: {
                'projectstatusuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Project Status', 'uri'),
            },
        )

        get_oef_dropdown_value = rail.RepliconServiceOperator(
            task_id="get_oef_dropdown_value",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data=lambda: {"objectExtensionTagDefinitionUri": rail.result('get_project_oefs')[
                'projectstatusuri']},
            response_filter=response_filter.get_filtered_tag_uri
        )

        process_projects = rail.TriggerDagRunForEachItemOperator(
            task_id='process_projects',
            retries=0,
            items="{{ result('query_project_data') }}",
            execution_timeout=timedelta(days=14),
            trigger_dag_id=f'matlensilver_client_project_task_sync_process_projects_{config.instance}',
            conf=lambda item: {
                'clientname': item['clientname'],
                'clientid': item['clientid'],
                'projectid': item['projectid'],
                'projectname': item['projectname'],
                'projectstatus': 'In Progress' if item['projectstatus'] == 'Active' else 'Completed' if item['projectstatus'] == 'Closed' else None,
                'projectstartdate': request_payload.get_datetime_object(item['projectstartdate']),
                'projectenddate': request_payload.get_datetime_object(item['projectenddate']),
                'projecttype': item['projecttype'],
                'projectstatusuri': rail.result('get_project_oefs')['projectstatusuri'],
                'tag_project_status_uri': rail.result('get_oef_dropdown_value')['open_uri'] if item['projectstatus'] == 'Active'
                else rail.result('get_oef_dropdown_value')['close_uri'],
                'client_success_log': rail.find_first_by_attr_and_get_attr(rail.result('get_client_success'), 'properties.clientid', item['clientid'])
                if rail.result('get_client_success') != [] else None,
                'client_error_log': rail.find_first_by_attr_and_get_attr(rail.result('get_client_error'), 'properties.clientid', item['clientid'])
                if rail.result('get_client_error') != [] else None,
                'filename': (rail.result('new_file_sensor')).split('/')[-1]
            }
        )

        wait_for_process_projects = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_projects',
            dag_runs='{{ result("process_projects") }}',
            retries=0,
            execution_timeout=timedelta(days=14)
        )

        get_project_status = rail.GatherResultsFromDagRunsOperator(
            task_id='get_project_status',
            dag_runs="{{ result('process_projects') }}",
            dagrun_task_id='get_project_success_status',
            flatten=True,
        )

        get_task_status = rail.GatherResultsFromDagRunsOperator(
            task_id='get_task_status',
            dag_runs="{{ result('process_projects') }}",
            dagrun_task_id='get_task_success_status',
            flatten=True,
        )

        send_logs_enter, send_logs_end = get_send_logs(config)

        can_log_to_sumo = rail.IfOperator(
            task_id="can_log_to_sumo",
            trigger_rule="all_done",
            test=lambda:  rail.get_current_context()['dag_run'].get_task_instance(
                delete_this_dagrun.task_id).current_state().lower() != "success",
            yes_task="log_to_sumo",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule="all_done",
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "file_name": "{{result('new_file_sensor')}}",
                "archive_file": "{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}",
                "log_file_name": 'log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv'
            }
        )

        can_fail_dag = rail.IfOperator(
            task_id = "can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task= "fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id = "fail_dagrun",
            message='{{ get_error_message() }}'
        )

        new_file_sensor >> is_csv >> rail.Label(
            'Yes') >> download_file >> was_new_file_found >> rail.Label('Yes') >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        is_csv >> rail.Label('No') >> send_bad_file_format_email
        download_file >> load_csv_data >> create_input_data_collection >> has_input_data
        has_input_data >> rail.Label(
            'Yes') >> query_client_data >> get_all_custom_fields >> process_clients >> wait_for_process_clients
        wait_for_process_clients >> get_client_success >> get_client_error >> query_project_data
        query_project_data >> get_project_oefs >> get_oef_dropdown_value >> process_projects
        process_projects >> wait_for_process_projects >> get_project_status >> get_task_status >> send_logs_enter
        has_input_data >> rail.Label('No') >> send_blank_payload_email
        send_logs_end >> can_log_to_sumo >> rail.Label("Yes") >> log_to_sumo >> can_fail_dag >> rail.Label('Yes') >> fail_dagrun

    return dag


rail.for_each_instance(create_main_dag)
