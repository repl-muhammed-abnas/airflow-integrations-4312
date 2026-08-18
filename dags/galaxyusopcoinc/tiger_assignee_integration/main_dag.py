from datetime import timedelta
import itertools
from os import path
from airflow.models import Variable
import rail
from rail.filters import split
from rail.lib.ecid import get_dagrun_ecid

from galaxyusopcoinc.tiger_assignee_integration.utils import request_payload
from galaxyusopcoinc.tiger_assignee_integration.tasks.generate_client_report_batch import report_batch


# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'vialtopartners_tiger_assignee_integration_master_{config.instance}',
        description='Vialto Partners Tiger Assignee Integration',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
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
            no_task='send_bad_file_format_email'
        )

        send_bad_file_format_email = rail.EmailOperator(
            task_id='send_bad_file_format_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            # pylint: disable=line-too-long
            subject='{{ get_company_key() }} | Tiger Assignee Integration - Incorrect Format - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/bad_file_format.html"
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        can_decrypt_file = rail.IfOperator(
            task_id ="can_decrypt_file",
            test=Variable.get(config.can_decrypt_file, default_var='true').lower() == 'true',
            yes_task='decrypt_file',
            no_task='dummy_load_data'
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            source='{{ result("download_file") }}',
            pgp_conn_id=config.pgp_conn_id
        )


        has_file_content = rail.IfOperator(
            task_id='has_file_content',
            test=request_payload.do_has_file_content,
            yes_task='dummy_load_data',
            no_task='send_blank_payload_email'
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            # yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            # trigger_rule='all_done',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        dummy_load_data = rail.PythonOperator(
            task_id= "dummy_load_data",
            python_callable= lambda: rail.result('decrypt_file') if Variable.get(config.can_decrypt_file).lower()== 'true' else rail.result('download_file'),
            show_return_value_in_logs= False
        )

        load_data = rail.LoadCSVFileOperator(
            task_id='load_data',
            document="{{ result('dummy_load_data') }}"
        )

        create_input_data_collection = rail.CreateCollectionOperator(
            task_id='create_input_data_collection',
            source="{{ result('load_data') }}",
            name="inputdatacollection",
            columns={
                'Tiger client long name': 'clientlongname',
                'Tiger short name': 'clientshortname',
                'assignee ID': 'assigneeid',
                'First Name': 'firstname',
                'Last Name': 'lastname',
                'Status': 'status'
            }
        )

        has_input_data = rail.IfOperator(
            task_id='has_input_data',
            test="{{ result('create_input_data_collection','length') > 0 }}",
            yes_task='create_skip_log',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='{{ get_company_key() }} | Tiger Assignee Integration - no records in file - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_payload.html"
        )

        create_skip_log = rail.CreateLogOperator(
            task_id='create_skip_log'
        )

        create_exception_record_log = rail.CreateLogOperator(
            task_id='create_exception_record_log'
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            name='validrecords',
            query="""SELECT * FROM inputdatacollection WHERE NULLIF(clientshortname, '') IS NOT NULL and
                    NULLIF(assigneeid, '') IS NOT NULL and NULLIF(firstname, '') IS NOT NULL and
                    NULLIF(lastname, '') IS NOT NULL and NULLIF(status, '') IS NOT NULL and status IN ('ACTIVE','EXPIRED')
                    and LENGTH(firstname || ' ' || lastname) <= 50"""
        )

        has_valid_records = rail.IfOperator(
            task_id="has_valid_records",
            test="{{result('query_valid_records', 'length') > 0}}",
            yes_task='query_unique_assignee_ids_active',
            no_task="no_valid_records_present"
        )

        no_valid_records_present = rail.EmptyOperator(
            task_id='no_valid_records_present'
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            name='invalidrecords',
            query="""SELECT * FROM inputdatacollection WHERE NULLIF(clientshortname, '') IS NULL or
                    NULLIF(assigneeid, '') IS NULL or NULLIF(firstname, '') IS NULL or
                    NULLIF(lastname, '') IS NULL or NULLIF(status, '') IS NULL or status NOT IN ('ACTIVE','EXPIRED') or
                    LENGTH(firstname || ' ' || lastname) > 50"""
        )

        has_invalid_records = rail.IfOperator(
            task_id="has_invalid_records",
            test="{{result('query_invalid_records', 'length') > 0}}",
            yes_task="log_invalid_records",
            no_task="no_invalid_records_present"
        )

        no_invalid_records_present = rail.EmptyOperator(
            task_id='no_invalid_records_present'
        )

        def get_invalid_message(item):
            if (len(f"{item['firstname']} {item['lastname']}")) > 50:
                return "Assignee name length is greater than 50 characters"

            if item['status'] == 'ACTIVE' or item['status'] == 'EXPIRED' or item['status'] =='':
                return "Mandatory fields are Missing"

            return "Mandatory fields are Missing/Status not in proposed format"

        log_invalid_records = rail.WriteLogOperator(
           task_id='log_invalid_records',
           log="{{result('create_exception_record_log')}}",
           items='{{result("query_invalid_records")}}',
           message=get_invalid_message,
           severity='Exception',
           properties=lambda item: {
               "projectname": '',
               "clientname": '',
               "clientshortname": item['clientshortname'] if item['clientshortname'] else '',
               "assigneeid": item['assigneeid'] if item['assigneeid'] else '',
               "assigneestatus": item['status'] if item['status'] else '',
               'details': get_invalid_message(item),
               'status': 'Exception',
              'jobid': get_dagrun_ecid(rail.get_current_context()['dag_run'])
           }
        )

        query_unique_assignee_ids_active = rail.QueryCollectionOperator(
            task_id="query_unique_assignee_ids_active",
            name='uniqueassigneeidsactive',
            query="""SELECT DISTINCT assigneeid, firstname, lastname FROM validrecords WHERE status = 'ACTIVE'"""
        )

        query_unique_assignee_ids_disabled = rail.QueryCollectionOperator(
            task_id="query_unique_assignee_ids_disabled",
            name='uniqueassigneeidstodisable',
            query="""SELECT DISTINCT assigneeid FROM validrecords WHERE status = 'EXPIRED'"""
        )

        get_timeentry_oefs = rail.RepliconServiceOperator(
            task_id="get_timeentry_oefs",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={"bindingContextUri": "urn:replicon:object-type:time-entry"},
            data_handler=lambda oefs: {
                'assigneenameuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Assignee Name', 'uri'),
            },
        )

        query_unique_assignee_ids = rail.QueryCollectionOperator(
            task_id="query_unique_assignee_ids",
            name='uniqueassigneeids',
            query="""SELECT DISTINCT assigneeid FROM validrecords"""
        )

        dummy_process_get_assignee_details =rail.EmptyOperator(
            task_id= "dummy_process_get_assignee_details"
        )

        process_get_assignee_details = rail.trigger_parallel_dagrun(
            task_id='process_get_assignee_details',
            items="{{ result('query_unique_assignee_ids') }}",
            parallel_count=config.trigger_parallel_dagrun_get_assignee_details,
            trigger_dag_id=f'vialtopartners_tiger_assignee_integration_child_process_get_assignee_details_{config.instance}',
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout_days),
            conf=request_payload.process_get_assignee_details_conf,
        )

        get_process_get_assignee_details_task_ids =rail.PythonOperator(
            task_id= 'get_process_get_assignee_details_task_ids',
            python_callable= lambda: list(itertools.chain(*list(map(lambda x: rail.result(
                    f'process_get_assignee_details_{x+1}'), range(config.trigger_parallel_dagrun_get_assignee_details))))),
            show_return_value_in_logs= False
        )

        get_all_assignee_details_from_child = rail.GatherResultsFromDagRunsOperator(
            task_id = "get_all_assignee_details_from_child",
            dag_runs= "{{result('get_process_get_assignee_details_task_ids')}}",
            flatten= True,
            dagrun_task_id= "get_assignee_details",
        )

        create_assignee_id_replicon_collection = rail.CreateCollectionOperator(
            task_id = "create_assignee_id_replicon_collection",
            source= lambda: rail.result('get_all_assignee_details_from_child'),
            name="repliconassigneeids",
            columns=['assigneeid','assigneename','assigneeuri','status']
        )

        query_not_available_assignee_id_disabled = rail.QueryCollectionOperator(
            task_id="query_not_available_assignee_id_disabled",
            name='notavailabledisabledassigneeids',
            query="""SELECT assigneeid FROM uniqueassigneeidstodisable
                    WHERE assigneeid  NOT IN (SELECT assigneeid FROM repliconassigneeids)
                    and assigneeid  NOT IN (SELECT assigneeid FROM uniqueassigneeidsactive)"""
        )

        has_not_available_disabled_assigneeid = rail.IfOperator(
            task_id='has_not_available_disabled_assigneeid',
            test="{{ result('query_not_available_assignee_id_disabled','length') > 0 }}",
            yes_task='log_assignee_id_not_available_disabled',
            no_task='query_update_assignee_ids'
        )

        log_assignee_id_not_available_disabled = rail.WriteLogOperator(
            task_id='log_assignee_id_not_available_disabled',
            log="{{result('create_skip_log')}}",
            items='{{result("query_not_available_assignee_id_disabled")}}',
            message='Required assignee id not available in replicon and expired in feed file',
            severity='Skipped',
            properties=lambda item: {
                'projectname': '',
                'clientname': '',
                'clientshortname': '',
                'assigneeid': item['assigneeid'],
                'assigneestatus': 'EXPIRED',
                'details': 'Required assignee id not available in replicon and expired in feed file',
                'status': 'Skipped',
                'jobid': get_dagrun_ecid(rail.get_current_context()['dag_run'])
            }
        )

        query_update_assignee_ids = rail.QueryCollectionOperator(
            task_id="query_update_assignee_ids",
            name='updateassigneeids',
            query="""SELECT DISTINCT v.assigneeid, v.firstname, v.lastname, r.assigneeuri  FROM validrecords v, repliconassigneeids r
                    WHERE v.assigneeid == r.assigneeid and ((v.firstname||' '||v.lastname) != r.assigneename or NULLIF(r.assigneename, '') IS NULL)"""
        )

        has_assignee_ids_for_update = rail.IfOperator(
            task_id='has_assignee_ids_for_update',
            test="{{ result('query_update_assignee_ids','length') > 0 }}",
            yes_task='process_assignee_ids_update',
            no_task='query_not_available_assignee_id'
        )

        process_assignee_ids_update = rail.TriggerDagRunForEachItemOperator(
            task_id='process_assignee_ids_update',
            items="{{ result('query_update_assignee_ids') }}",
            trigger_dag_id=f'vialtopartners_tiger_assignee_integration_child_process_assignee_update_{config.instance}',
            execution_timeout=timedelta(
                hours=config.child_execution_timeout_hours),
            conf=request_payload.get_process_assignee_ids_update_conf,
            retries=0,
        )

        wait_for_process_assignee_ids_update = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_assignee_ids_update',
            dag_runs='{{ result("process_assignee_ids_update") }}',
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout_days)
        )

        gather_assignee_update_error_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_assignee_update_error_logs',
            dag_runs='{{ result("process_assignee_ids_update") }}',
            dagrun_task_id='create_assignee_update_error_log',
            flatten=True,
        )

        query_not_available_assignee_id = rail.QueryCollectionOperator(
            task_id='query_not_available_assignee_id',
            name='notavailableassigneeids',
            query="""SELECT assigneeid, firstname, lastname FROM uniqueassigneeidsactive
                    WHERE assigneeid  NOT IN (SELECT assigneeid FROM repliconassigneeids)"""
        )

        has_assigneeids_for_add = rail.IfOperator(
            task_id='has_assigneeids_for_add',
            test="{{ result('query_not_available_assignee_id','length') > 0 }}",
            yes_task='process_assignee_ids_add',
            no_task='merge_new_assignee_id_with_previous_data'
        )

        process_assignee_ids_add = rail.TriggerDagRunForEachItemOperator(
            task_id='process_assignee_ids_add',
            items="{{ result('query_not_available_assignee_id') }}",
            trigger_dag_id=f'vialtopartners_tiger_assignee_integration_child_process_assignee_add_{config.instance}',
            execution_timeout=timedelta(
                hours=config.child_execution_timeout_hours),
            conf=request_payload.get_process_assignee_ids_add_conf,
            retries=0,
        )

        wait_for_process_assignee_ids_add = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_assignee_ids_add',
            dag_runs='{{ result("process_assignee_ids_add") }}',
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout_days)
        )

        gather_assignee_add_error_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_assignee_add_error_logs',
            dag_runs='{{ result("process_assignee_ids_add") }}',
            dagrun_task_id='create_assignee_add_error_log',
            flatten=True,
        )

        get_newly_created_assigneeid_from_child = rail.GatherResultsFromDagRunsOperator(
            task_id = "get_newly_created_assigneeid_from_child",
            dag_runs= '{{result("process_assignee_ids_add")}}',
            dagrun_task_id= "add_newly_created_assigneeid",
            flatten=True
        )

        merge_new_assignee_id_with_previous_data = rail.CreateCollectionOperator(
            task_id ="merge_new_assignee_id_with_previous_data",
            source= lambda: rail.load_all_records(rail.result("create_assignee_id_replicon_collection")) + (
            rail.result("get_newly_created_assigneeid_from_child") if rail.result("get_newly_created_assigneeid_from_child") else []),
            name='updatedrepliconassigneeids',
            columns=['assigneeid','assigneename','assigneeuri','status']
        )

        assignee_ids_available_in_updated_collection = rail.QueryCollectionOperator(
            task_id="assignee_ids_available_in_updated_collection",
            name='availableassigneeidsupdated',
            query="""SELECT DISTINCT assigneeid, status, clientshortname FROM validrecords
                    WHERE assigneeid IN (SELECT assigneeid FROM updatedrepliconassigneeids)"""
        )

        has_assignee_ids_available = rail.IfOperator(
            task_id='has_assignee_ids_available',
            test="{{ result('assignee_ids_available_in_updated_collection','length') > 0 }}",
            yes_task='valid_assignee_ids',
            no_task='no_assignee_ids_available'
        )

        no_assignee_ids_available = rail.EmptyOperator(
            task_id='no_assignee_ids_available'
        )

        valid_assignee_ids = rail.QueryCollectionOperator(
            task_id="valid_assignee_ids",
            name='validassigneeids',
            query="""SELECT DISTINCT assigneeid, status, clientshortname FROM availableassigneeidsupdated
                    WHERE assigneeid  NOT IN (SELECT assigneeid FROM notavailabledisabledassigneeids)"""
        )

        load_report, load_report_data = report_batch(config)

        create_report_collection = rail.CreateCollectionOperator(
            task_id='create_report_collection',
            name='reportdatacollection',
            source="{{ result('load_report_data') }}",
            columns={
                'Client Name': 'clientname',
                'Client URI': 'clienturi',
                'Tiger Client Name': 'tigerclientname'
            }
        )

        query_unique_clientshortnames = rail.QueryCollectionOperator(
            task_id='query_unique_clientshortnames',
            name='uniqueshortnames',
            query="""SELECT DISTINCT clientshortname FROM validrecords"""
        )

        query_clients_in_replicon = rail.QueryCollectionOperator(
            task_id="query_clients_in_replicon",
            name='clientsinreplicon',
            query="""SELECT r.clientname, r.clienturi, r.tigerclientname, v.clientshortname,v.assigneeid, v.status
                    FROM (SELECT DISTINCT * FROM reportdatacollection) as r, uniqueshortnames u,validassigneeids v
                    WHERE r.tigerclientname LIKE ('%|'||u.clientshortname||'|%') and v.clientshortname = u.clientshortname"""
        )

        query_unassociated_clientshortnames = rail.QueryCollectionOperator(
            task_id='query_unassociated_clientshortnames',
            name='unassociatedclientshortnames',
            query="""SELECT DISTINCT clientshortname, assigneeid FROM validassigneeids
                    WHERE clientshortname NOT IN (SELECT clientshortname FROM clientsinreplicon)"""
        )

        has_any_unassociated_clientshortnames = rail.IfOperator(
            task_id='has_any_unassociated_clientshortnames',
            test="{{ result('query_unassociated_clientshortnames','length') > 0 }}",
            yes_task='log_replicon_client_not_associated',
            no_task='query_unique_clients_in_replicon'
        )

        log_replicon_client_not_associated = rail.WriteLogOperator(
            task_id='log_replicon_client_not_associated',
            log="{{result('create_exception_record_log')}}",
            items='{{result("query_unassociated_clientshortnames")}}',
            message='No clients associated with clientshortname',
            severity='Exception',
            properties=lambda item: {
                'projectname': '',
                'clientname': '',
                'clientshortname': item['clientshortname'],
                'assigneeid': item['assigneeid'],
                'assigneestatus': '',
                'details':'No clients associated with clientshortname',
                'status': 'Exception',
                'jobid': get_dagrun_ecid(rail.get_current_context()['dag_run'])
            }
        )

        query_unique_clients_in_replicon = rail.QueryCollectionOperator(
            task_id='query_unique_clients_in_replicon',
            name='uniqueclientsinreplicon',
            query="""SELECT DISTINCT clientname, clienturi FROM clientsinreplicon"""
        )

        add_rowid_to_clients_query= rail.QueryCollectionOperator(
            task_id='add_rowid_to_clients_query',
            name='rowidclientsinreplicon',
            query="""SELECT ROW_NUMBER() OVER(ORDER BY ROWID) AS record_id,* FROM uniqueclientsinreplicon"""
        )

        has_replicon_clients = rail.IfOperator(
            task_id='has_replicon_clients',
            test="{{ result('query_unique_clients_in_replicon','length') > 0 }}",
            yes_task='create_exception_log',
            no_task='no_clients_present'
        )

        no_clients_present = rail.EmptyOperator(
            task_id='no_clients_present'
        )

        create_exception_log = rail.CreateLogOperator(
            task_id='create_exception_log'
        )

        create_error_log = rail.CreateLogOperator(
            task_id='create_error_log'
        )

        dummy_process_each_replicon_client =rail.EmptyOperator(
            task_id= "dummy_process_each_replicon_client"
        )

        def get_trigger_id_clients(item):
            if int(item['record_id'])%config.BATCH_SIZE_CLIENT == 0:
                return f'vialtopartners_tiger_assignee_integration_child_process_each_replicon_client_{config.instance}'
            else:
                return f'vialtopartners_tiger_assignee_integration_child_process_each_replicon_client_{config.instance}_batch_{int(item["record_id"])%config.BATCH_SIZE_CLIENT}'

        process_each_replicon_client = rail.trigger_parallel_dagrun(
            task_id='process_each_replicon_client',
            items="{{ result('add_rowid_to_clients_query') }}",
            parallel_count=config.trigger_parallel_dagrun_process_each_replicon_client,
            trigger_dag_id=lambda item:get_trigger_id_clients(item),
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout_days),
            conf=request_payload.get_process_each_replicon_client,
        )

        get_process_each_replicon_client_task_ids =rail.PythonOperator(
            task_id= 'get_process_each_replicon_client_task_ids',
            python_callable= lambda: list(itertools.chain(*list(map(lambda x: rail.result(
                    f'process_each_replicon_client_{x+1}'), range(config.trigger_parallel_dagrun_process_each_replicon_client))))),
            show_return_value_in_logs= False
        )

        # gather_success_logs = rail.GatherResultsFromDagRunsOperator(
        #     task_id='gather_success_logs',
        #     dag_runs='{{ result("get_process_each_replicon_client_task_ids") }}',
        #     dagrun_task_id='create_success_log',
        #     flatten=True,
        # )

        dummy_process_log_generation = rail.EmptyOperator(
            task_id="dummy_process_log_generation"
        )

        def get_startline():
            filename = split(string=path.split(rail.result('new_file_sensor'))[1], separator='.')[0]
            if len(str(filename).split('_'))==1:
                return "2"
            batch_number = str(filename).split('_')[-1]
            if batch_number =="0":
                return "2"
            else:
                return str((int(batch_number)*10000)+2)
            
        def get_endline():
            total_records = rail.result('create_input_data_collection',key='length')
            filename = split(string=path.split(rail.result('new_file_sensor'))[1], separator='.')[0]
            if len(str(filename).split('_'))==1:
                return str(total_records+1)
            batch_number = str(filename).split('_')[-1]
            if batch_number =="0":
                return str(total_records+1)
            else:
                return str((int(batch_number)*10000)+(total_records+1))

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout_days),
            trigger_dag_id=f'vialtopartners_tiger_assignee_integration_child_process_log_generation_{config.instance}',
            # pylint: disable=line-too-long
            conf=lambda: {
                'filename': split(string=path.split(rail.result('new_file_sensor'))[1], separator='.')[0],
                'log_filename': f"log_{get_dagrun_ecid(rail.get_current_context()['dag_run'])}_{split(string=path.split(rail.result('new_file_sensor'))[1], separator='.')[0]}.csv",
                'assigneeadd_errorlog': rail.result('gather_assignee_add_error_logs'),
                'assigneeupdate_errorlog': rail.result('gather_assignee_update_error_logs'),
                #'successlog': rail.result('gather_success_logs'),
                'errorlog': rail.result('create_error_log'),
                'exceptionlog': rail.result('create_exception_log'),
                'skipped_log':rail.result('create_skip_log'),
                'recordexceptionlog': rail.result('create_exception_record_log'),
                'merge_filepath': '/Tiger/Prod/Logs/MergeLogs',
                'merge_filename': f"Mergelog_{split(string=path.split(rail.result('new_file_sensor'))[1], separator='.')[0]}.csv",
                'startline': get_startline(),
                'endline': get_endline()
            }
        )

        can_log_to_sumo = rail.IfOperator(
            task_id="can_log_to_sumo",
            trigger_rule="all_done",
            test=lambda:  rail.get_current_context()['dag_run'].get_task_instance(
                delete_this_dagrun.task_id).current_state().lower() != "success" and
                rail.get_current_context()['dag_run'].get_task_instance(
                download_file.task_id).current_state().lower() == "success",
            yes_task="log_to_sumo",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "file_name": "{{result('new_file_sensor')}}",
                "archive_file": "{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}",
                "log_file_name": 'log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv'
            }
        )

        new_file_sensor >> is_csv >> rail.Label(
            'Yes') >> download_file >> was_new_file_found
        is_csv >> rail.Label('No') >> send_bad_file_format_email
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        download_file >> archive_file >> can_decrypt_file >> rail.Label('Yes') >> decrypt_file >> has_file_content
        can_decrypt_file >> rail.Label('No') >> dummy_load_data >> load_data
        has_file_content >> rail.Label('Yes') >> dummy_load_data
        has_file_content >> rail.Label('No') >> send_blank_payload_email
        load_data >> create_input_data_collection >> has_input_data
        has_input_data >> rail.Label('No') >> send_blank_payload_email
        has_input_data >> rail.Label(
            'Yes') >> create_skip_log >> create_exception_record_log >> [query_valid_records, query_invalid_records]
        query_valid_records >> has_valid_records >> rail.Label(
            'Yes') >> query_unique_assignee_ids_active
        has_valid_records >> rail.Label(
            'No') >> no_valid_records_present >> dummy_process_log_generation
        query_invalid_records >> has_invalid_records >> rail.Label(
            'No') >> no_invalid_records_present >> dummy_process_log_generation
        has_invalid_records >> rail.Label(
            'Yes') >> log_invalid_records >> dummy_process_log_generation
        query_unique_assignee_ids_active >> query_unique_assignee_ids_disabled >> get_timeentry_oefs >> query_unique_assignee_ids

        query_unique_assignee_ids >> dummy_process_get_assignee_details >> process_get_assignee_details >> get_process_get_assignee_details_task_ids
        get_process_get_assignee_details_task_ids >> get_all_assignee_details_from_child
        get_all_assignee_details_from_child >> create_assignee_id_replicon_collection
        create_assignee_id_replicon_collection >> query_not_available_assignee_id_disabled

        query_not_available_assignee_id_disabled >> has_not_available_disabled_assigneeid >> rail.Label(
            'Yes') >> log_assignee_id_not_available_disabled

        has_not_available_disabled_assigneeid >> rail.Label('No') >> query_update_assignee_ids >> has_assignee_ids_for_update >> rail.Label(
            'Yes') >> process_assignee_ids_update >> wait_for_process_assignee_ids_update
        wait_for_process_assignee_ids_update >> gather_assignee_update_error_logs >>  query_not_available_assignee_id
        has_assignee_ids_for_update >> rail.Label(
            'No') >> query_not_available_assignee_id
        log_assignee_id_not_available_disabled >> query_update_assignee_ids
        query_not_available_assignee_id >> has_assigneeids_for_add >> rail.Label(
            'No') >> merge_new_assignee_id_with_previous_data\
                 >> assignee_ids_available_in_updated_collection >> has_assignee_ids_available >> rail.Label(
            'No') >> no_assignee_ids_available >> dummy_process_log_generation
        has_assignee_ids_available >> rail.Label(
            'Yes') >> valid_assignee_ids >> load_report
        has_assigneeids_for_add >> rail.Label(
            'Yes') >> process_assignee_ids_add >> wait_for_process_assignee_ids_add >> gather_assignee_add_error_logs >> get_newly_created_assigneeid_from_child
        get_newly_created_assigneeid_from_child >> merge_new_assignee_id_with_previous_data >> assignee_ids_available_in_updated_collection

        load_report_data >> create_report_collection >> query_unique_clientshortnames >> query_clients_in_replicon >> query_unassociated_clientshortnames
        query_unassociated_clientshortnames >> has_any_unassociated_clientshortnames >> rail.Label(
            'Yes') >> log_replicon_client_not_associated >> query_unique_clients_in_replicon
        has_any_unassociated_clientshortnames >> rail.Label(
            'No') >> query_unique_clients_in_replicon
        query_unique_clients_in_replicon >> add_rowid_to_clients_query >> has_replicon_clients >> rail.Label(
            'No') >> no_clients_present >> dummy_process_log_generation
        has_replicon_clients >> rail.Label(
            'Yes') >> create_exception_log >> create_error_log >> dummy_process_each_replicon_client >> process_each_replicon_client
        process_each_replicon_client >> get_process_each_replicon_client_task_ids >> dummy_process_log_generation

        dummy_process_log_generation >> process_log_generation >> can_log_to_sumo >> rail.Label("Yes") >> log_to_sumo

    return dag


rail.for_each_instance(create_main_dag)
