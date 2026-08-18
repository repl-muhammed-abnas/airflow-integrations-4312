from datetime import datetime, timedelta, timezone
from os import path
import rail
from rail.filters import split
from dxctechnology.cwf_user_profile_v1.user_profile_sync.task.process_costcenters import process_costcenters_task_group
from dxctechnology.cwf_user_profile_v1.user_profile_sync.utils import request_payload
from dxctechnology.cwf_user_profile_v1.user_profile_sync.utils.response_filter import page_handler, map_employeetypes
from dxctechnology.cwf_user_profile_v1.user_profile_sync.utils.python_callable_method import get_blank_mandatory_field_log, get_cwf_user_integration_mapper_data


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/cwf_user_profile_v1/user_profile_sync/config.py


# pylint:disable = too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.cwf_main_dagid,
        description='DXC_Fieldglass CWFUserProfiles_Master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.master_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        get_time_for_file = rail.PythonOperator(
            task_id='get_time_for_file',
            python_callable=lambda: datetime.now(
                timezone.utc).strftime('%d%m%YT%H%M%S')
        )

        is_gsap = rail.IfOperator(
            task_id='is_gsap',
            test='{{ result("new_file_sensor") | file_base | starts_with("GSAP")}}',
            yes_task='process_gsap_main_dag',
            no_task='dummy_process_cwf',
        )

        process_gsap_main_dag = rail.TriggerDagRunOperator(
            task_id='process_gsap_main_dag',
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.gsap_main_dagid,
            conf=lambda: {
                'new_file_sensor': rail.result('new_file_sensor'),
                'download_file': rail.result('download_file'),
                'get_time_for_file': rail.result('get_time_for_file')
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        dummy_process_cwf = rail.EmptyOperator(
            task_id='dummy_process_cwf'
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test="{{ get_task_state('new_file_sensor') == 'success' }}",
            yes_task='archive_file',
            no_task='delete_this_dagrun'
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/{{ result('get_time_for_file') }}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            retries=0,
            source="{{ result('download_file') }}",
            pgp_conn_id=config.pgp_conn_id
        )

        process_file = rail.EmptyOperator(
            task_id='process_file',
            trigger_rule='one_success'
        )

        is_decrypted_file = rail.IfOperator(
            task_id='is_decrypted_file',
            test="{{ get_task_state('decrypt_file') == 'success' }}",
            yes_task='process_decrypted_file',
            no_task='create_rawdata_collection'
        )

        process_decrypted_file = rail.EmptyOperator(
            task_id='process_decrypted_file'
        )

        has_file_content = rail.IfOperator(
            task_id='has_file_content',
            test=request_payload.do_has_file_content,
            yes_task='load_decrypted_data',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon user sync for CWF worker profile - No records to process - {{ current_time_in_specified_tz() }}',
            html_content='templates/emails/blank_payload.html'
        )

        load_decrypted_data = rail.LoadCSVFileOperator(
            task_id='load_decrypted_data',
            document="{{ result('decrypt_file') }}",
            delimiter=config.delimiter
        )

        load_user_data = rail.LoadCSVFileOperator(
            task_id='load_user_data',
            trigger_rule='one_failed',
            document="{{ result('download_file') }}",
            delimiter=config.delimiter
        )

        create_rawdata_collection = rail.CreateCollectionOperator(
            task_id='create_rawdata_collection',
            source=lambda: rail.result(
                'load_decrypted_data') or rail.result('load_user_data'),
            name='rawdata',
            columns={
                'HPID': 'hpid',
                'FIRST_NAME': 'firstname',
                'LAST_NAME': 'lastname',
                'EMAIL_ADDRESS': 'emailaddress',
                'AFM_COST_CENTER': 'afmcostcenter',
                'MANAGER_EMAIL': 'manageremail',
                'MANAGER_HPID': 'managerid',
                'CONTRACT_END_DATE': 'contractenddate',
                'CONTRACT_BEGIN_DATE': 'contractstartdate',
                'WORKER_TYPE': 'workertype',
                'FINANCE_SYSTEM': 'financesystem',
                'Time_Tracking_Required': 'timetracking',
                'COMPANY_CODE': 'companycode'
            }
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test="{{ result('create_rawdata_collection', 'length') > 0 }}",
            yes_task='query_any_blankmandatory_check',
            no_task='send_blank_payload_email'
        )

        query_any_blankmandatory_check = rail.QueryCollectionOperator(
            task_id='query_any_blankmandatory_check',
            query="""SELECT * FROM rawdata
                    WHERE NULLIF(hpid,'') IS NULL OR NULLIF(firstname,'') IS NULL OR NULLIF(lastname,'') IS NULL OR NULLIF(emailaddress,'') IS NULL OR
                    NULLIF(manageremail,'') IS NULL OR NULLIF(managerid,'') IS NULL OR NULLIF(contractstartdate,'') IS NULL OR NULLIF(workertype,'') IS NULL OR
                    NULLIF(financesystem,'') IS NULL OR NULLIF(timetracking,'') IS NULL OR (financesystem != 'C1' AND financesystem != 'ES') OR
                    NULLIF(companycode,'') IS NULL"""
        )

        has_any_blankmandatory_field = rail.IfOperator(
            task_id='has_any_blankmandatory_field',
            test="{{ result('query_any_blankmandatory_check', 'length') > 0 }}",
            yes_task='create_blankmandatory_log',
            no_task='get_mapper_data'
        )

        create_blankmandatory_log = rail.CreateLogOperator(
            task_id='create_blankmandatory_log'
        )

        write_blankmandatory_field_log = rail.WriteLogOperator(
            task_id='write_blankmandatory_field_log',
            log="{{ result('create_blankmandatory_log') }}",
            items="{{ result('query_any_blankmandatory_check') }}",
            message='blank mandatory field log',
            severity='Skipped',
            properties=lambda item: {
                'userid': item['hpid'],
                'email': item['emailaddress'],
                'action': 'Validation',
                'status': 'Skipped',
                'details': get_blank_mandatory_field_log(item)
            }
        )

        get_mapper_data = rail.PythonOperator(
            task_id='get_mapper_data',
            python_callable=get_cwf_user_integration_mapper_data
        )

        load_inputdata_from_rawdata = rail.QueryCollectionOperator(
            task_id='load_inputdata_from_rawdata',
            name='inputdatacollection',
            query="""SELECT * FROM rawdata
                    WHERE NULLIF(hpid,'') IS NOT NULL AND NULLIF(firstname,'') IS NOT NULL AND NULLIF(lastname,'') IS NOT NULL AND NULLIF(emailaddress,'') IS NOT NULL AND
                    NULLIF(manageremail,'') IS NOT NULL AND NULLIF(managerid,'') IS NOT NULL AND NULLIF(contractstartdate,'') IS NOT NULL AND NULLIF(workertype,'') IS NOT NULL AND
                    NULLIF(financesystem,'') IS NOT NULL AND NULLIF(timetracking,'') IS NOT NULL AND (financesystem = 'C1' OR financesystem = 'ES') AND
                    NULLIF(companycode,'') IS NOT NULL"""
        )

        process_costcenters = rail.EmptyOperator(
            task_id='process_costcenters'
        )

        process_costcenters_task = process_costcenters_task_group(
            config.execution_timeout_days)

        should_process_userprofiles = rail.IfOperator(
            task_id='should_process_userprofiles',
            test="{{ result('load_inputdata_from_rawdata', 'length') > 0 }}",
            yes_task=['get_all_activities', 'get_all_permissionsets', 'get_financesystem_workertype_customfield',
                      'get_all_employeetypes', 'get_all_companycodes', 'get_all_policysets'],
            no_task='process_log_generation'
        )

        get_all_activities = rail.RepliconServiceOperator(
            task_id='get_all_activities',
            endpoint='/services/ActivityService1.svc/GetAllActivities'
        )

        get_all_permissionsets = rail.RepliconServiceOperator(
            task_id='get_all_permissionsets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets'
        )

        get_financesystem_workertype_customfield = rail.RepliconServiceOperator(
            task_id='get_financesystem_workertype_customfield',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                'objectUri': 'urn:replicon:object-type:user'
            },
            data_handler=lambda response: {
                'financesystemuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Finance System (CWF)', 'uri'),
                'workertypeuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Worker Type', 'uri'),
                'pernerudfuri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'PERNER', 'uri')
            }
        )

        get_all_employeetypes = rail.RepliconServicePageOperator(
            task_id='get_all_employeetypes',
            endpoint='/services/EmployeeTypeGroupListService1.svc/GetData',
            data=request_payload.get_all_employeetypes_payload,
            page_handler=page_handler,
            all_result_data_handler=map_employeetypes
        )

        get_all_companycodes = rail.RepliconServiceOperator(
            task_id='get_all_companycodes',
            endpoint='/services/DivisionService1.svc/GetEnabledDivisions'
        )

        get_all_policysets = rail.RepliconServiceOperator(
            task_id='get_all_policysets',
            endpoint='/services/PolicySetService1.svc/GetAllPolicySets'
        )

        get_financesystem_customfield_dropdowns = rail.RepliconServiceOperator(
            task_id='get_financesystem_customfield_dropdowns',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                'customFieldUri': "{{ result('get_financesystem_workertype_customfield').financesystemuri }}"
            }
        )

        get_workertype_customfield_dropdowns = rail.RepliconServiceOperator(
            task_id='get_workertype_customfield_dropdowns',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                'customFieldUri': "{{ result('get_financesystem_workertype_customfield').workertypeuri }}"
            }
        )

        process_userprofile_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='process_userprofile_child_dag',
            retries=0,
            items="{{ result('load_inputdata_from_rawdata') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.cwf_process_userprofiles_dagid,
            conf=request_payload.get_userprofile_child_dag_conf
        )

        wait_for_userprofile_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_userprofile_child_dag',
            dag_runs='{{ result("process_userprofile_child_dag") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        get_supervisorcheck_pending_logs = rail.FilterLogEntriesOperator(
            task_id='get_supervisorcheck_pending_logs',
            severity='Pending',
            remove_filtered_entries=True
        )

        is_supervisorcheck_pending = rail.IfOperator(
            task_id='is_supervisorcheck_pending',
            test="{{ result('get_supervisorcheck_pending_logs', 'length') > 0 }}",
            yes_task='process_supervisor_child_dag',
            no_task='gather_logs'
        )

        process_supervisor_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='process_supervisor_child_dag',
            retries=0,
            items="{{ result('get_supervisorcheck_pending_logs') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.cwf_supervisor_userprofiles_dagid,
            conf=request_payload.get_supervisor_child_dag_conf
        )

        wait_for_supervisor_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_supervisor_child_dag',
            dag_runs="{{ result('process_supervisor_child_dag') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_logs',
            dag_runs="{{ result('process_userprofile_child_dag') }}",
            dagrun_task_id='create_log',
            flatten=True
        )

        process_log_generation = rail.TriggerDagRunForEachItemOperator(
            task_id='process_log_generation',
            retries=0,
            items=lambda dag_run: [dag_run.conf],
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.cwf_log_userprofiles_dagid,
            conf=lambda: {
                'child_log': rail.result('gather_logs'),
                'filename': split(string=path.split(rail.result('new_file_sensor'))[1], separator='.')[0],
                'log_filename': f"Logs_{rail.result('get_time_for_file')}_{split(string=path.split(rail.result('new_file_sensor'))[1], separator='.')[0]}.csv",
                'input_filesize': rail.result('create_rawdata_collection', 'length'),
                'create_blankmandatory_log': rail.result('create_blankmandatory_log')
            }
        )

        should_fail_dag = rail.IfOperator(
            task_id='should_fail_dag',
            trigger_rule='all_done',
            test="{{ get_failed_upstream_task_ids() | length > 0 and get_failed_upstream_task_ids() | \
                first_or_default != 'decrypt_file' }}",
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
            test="{{ result('new_file_sensor') | is_truthy }}",
            yes_task='dagrun_log_to_sumo'
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            extra_info={
                'filename ': "{{ result('new_file_sensor') | file_name }}",
                'process_records': "{{ result('load_inputdata_from_rawdata', 'length') if result('load_inputdata_from_rawdata') else 'nil' }}",
                'skipped_invalidation_records': "{{ result('query_any_blankmandatory_check', 'length') if \
                    result('query_any_blankmandatory_check') else 'nil' }}"
            }
        )

        new_file_sensor >> download_file >> get_time_for_file >> is_gsap >> rail.Label(
            'Yes') >> process_gsap_main_dag

        process_gsap_main_dag >> finish

        is_gsap >> rail.Label('No') >> dummy_process_cwf

        dummy_process_cwf >> rail.Label(
            'Always') >> was_new_file_found

        was_new_file_found >> rail.Label(
            'Yes') >> archive_file

        was_new_file_found >> rail.Label(
            'No') >> delete_this_dagrun

        dummy_process_cwf >> decrypt_file >> rail.Label(
            'On Success') >> process_file

        decrypt_file >> rail.Label(
            'On Error') >> load_user_data

        load_user_data >> process_file >> is_decrypted_file

        is_decrypted_file >> rail.Label(
            'Yes') >> process_decrypted_file >> has_file_content

        is_decrypted_file >> rail.Label(
            'No') >> create_rawdata_collection

        has_file_content >> rail.Label(
            'Yes') >> load_decrypted_data >> create_rawdata_collection

        create_rawdata_collection >> has_data

        has_data >> rail.Label(
            'Yes') >> query_any_blankmandatory_check >> has_any_blankmandatory_field

        has_any_blankmandatory_field >> rail.Label(
            'Yes') >> create_blankmandatory_log >> write_blankmandatory_field_log >> get_mapper_data

        has_any_blankmandatory_field >> rail.Label(
            'No') >> get_mapper_data

        has_data >> rail.Label(
            'No') >> send_blank_payload_email

        has_file_content >> rail.Label(
            'No') >> send_blank_payload_email

        get_mapper_data >> load_inputdata_from_rawdata >> process_costcenters >> process_costcenters_task >> \
            should_process_userprofiles

        should_process_userprofiles >> rail.Label(
            'Yes') >> [get_all_activities, get_all_permissionsets, get_financesystem_workertype_customfield,
                       get_all_employeetypes, get_all_companycodes, get_all_policysets] >> \
            get_financesystem_customfield_dropdowns >> get_workertype_customfield_dropdowns >> \
            process_userprofile_child_dag >> wait_for_userprofile_child_dag >> \
            get_supervisorcheck_pending_logs >> is_supervisorcheck_pending

        is_supervisorcheck_pending >> rail.Label(
            'Yes') >> process_supervisor_child_dag >> wait_for_supervisor_child_dag >> gather_logs

        is_supervisorcheck_pending >> rail.Label(
            'No') >> gather_logs

        gather_logs >> process_log_generation

        should_process_userprofiles >> rail.Label(
            'No') >> process_log_generation

        process_log_generation >> should_fail_dag

        should_fail_dag >> rail.Label(
            'Yes') >> fail_dag

        should_fail_dag >> rail.Label(
            'No') >> process_logtosumo >> check_if_new_file_found

        check_if_new_file_found >> rail.Label(
            'Yes') >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_main_dag)
