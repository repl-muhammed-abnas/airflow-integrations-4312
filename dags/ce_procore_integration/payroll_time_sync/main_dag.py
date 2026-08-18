import rail
from datetime import date, datetime, timedelta, timezone
from rail.lib.last_sync_time_store import get_lastsync_time_variable, set_lastsync_time_variable
from ce_procore_integration.util_dags.utils import normalize_ce_identifier, get_tenant_email


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description='Computerease to Procore Payroll Time Sync',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.main_dag_max_active_runs,
        tags=['computerease_procore', 'payroll_time_sync'],
        schedule_interval=timedelta(
            minutes=config.payroll_time_sync_interval_minutes),
        default_args={
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        procore_company_id_template = "{{ conn." + config.procore_conn_id + ".extra_dejson.company_id }}"
        fetched_projects = {}

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_last_sync_time',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_last_sync_time = rail.PythonOperator(
            task_id='get_last_sync_time',
            python_callable=lambda: get_lastsync_time_variable(
                variable_name=config.payroll_time_last_sync_time_var,
                date_format=config.ce_time_format,
                initial_sync_time=config.initial_sync_time,
                reset_after_threshold=False
            ),
        )

        def filter_by_timestamp(response_data):
            """
            Note: CE time entries endpoint doesn't support server-side filtering
            (gt~updated_at not available). Once they start supporting it,
            we can remove this function and directly pass the filter in query params.
            """
            last_sync_time_str = rail.result('get_last_sync_time')['last_synctime']
            last_sync_time = datetime.strptime(last_sync_time_str, '%Y-%m-%dT%H:%M:%S.%f')
            last_sync_time_with_tz = last_sync_time.replace(tzinfo=timezone.utc)
            filtered_time_entries = []
            for entry in response_data:
                updated_at_str = entry.get('updated_at', '')
                updated_at = datetime.strptime(updated_at_str, config.ce_time_format)
                if updated_at > last_sync_time_with_tz and entry['timesheet_uuid'] == None and entry['job'] != None and entry['job'] != '':
                    filtered_time_entries.append(entry)

            return filtered_time_entries

        get_ce_time_entries = rail.ComputereaseAPIOperator(
            task_id='get_ce_time_entries',
            endpoint='/timesheet/entry',
            request_method='GET',
            query_params={
                'gte~date': date.today() - timedelta(days=config.lookback_days),
                'lte~date': date.today() + timedelta(days=config.lookahead_days),
                'source_id': 'Manual',
                'status': 'processed'
            },
            data_handler=lambda resp: filter_by_timestamp(resp['data'])
        )

        set_last_sync_time = rail.PythonOperator(
            task_id='set_last_sync_time',
            python_callable=lambda: set_lastsync_time_variable(
                variable_name=config.payroll_time_last_sync_time_var,
                value_to_set=rail.result('get_last_sync_time')['current_time']
            )
        )

        list_timecard_time_types = rail.ProcoreApiOperator(
            task_id='list_timecard_time_types',
            endpoint='/timecard_time_types',
            method='GET',
            query_params=lambda: {
                'company_id': rail.render_template(procore_company_id_template)
            },
            data_handler=lambda resp: { entry['time_type'] : entry['id'] for entry in resp }
        )

        get_procore_user_details = rail.ProcoreApiOperator(
            task_id='get_procore_user_details',
            endpoint=f'/companies/{procore_company_id_template}/users',
            method='GET',
            version='1.3',
            data_handler= lambda resp: { entry['origin_id'] : entry['id'] for entry in resp if entry['is_active'] and entry['is_employee']} if resp else {}
        )

        get_procore_party_ids = rail.ProcoreApiOperator(
            task_id='get_procore_party_ids',
            endpoint=f'/companies/{procore_company_id_template}/people',
            method='GET',
            data_handler= lambda resp: { entry['user_id'] : entry['id'] for entry in resp} if resp else {}
        )

        for_each_paryoll_time_entry = rail.ForEachOperator(
            task_id='for_each_paryoll_time_entry',
            items=lambda: rail.result("get_ce_time_entries"),
            start_task='check_if_employee_exist',
            end_task='end_for_each_payroll_time_entry_item'
        )

        end_for_each_payroll_time_entry_item = rail.EmptyOperator(
            task_id='end_for_each_payroll_time_entry_item'
        )

        def check_if_employee_exists(current_item, procore_employees):
            employee_origin_id =f'CE_{current_item["employee"]}'
            return employee_origin_id in procore_employees

        check_if_employee_exist = rail.IfOperator(
            task_id='check_if_employee_exist',
            test=lambda: check_if_employee_exists(rail.result("for_each_paryoll_time_entry"), rail.result("get_procore_user_details")),
            yes_task='check_if_project_details_exist',
            no_task='catch_error'
        )

        def check_if_project_exists(current_item):
            current_item = rail.result('for_each_paryoll_time_entry')
            project_origin_id =f'CE_{current_item["job"]}'
            return project_origin_id in fetched_projects and fetched_projects[project_origin_id]['status']

        check_if_project_details_exist = rail.IfOperator(
            task_id='check_if_project_details_exist',
            test=lambda: check_if_project_exists(rail.result("for_each_paryoll_time_entry")),
            yes_task='prepare_time_entry_payload',
            no_task='get_procore_project_details'
        )

        get_procore_project_details = rail.ProcoreApiOperator(
            task_id='get_procore_project_details',
            endpoint='/projects',
            method='GET',
            query_params=lambda: {
                'company_id': rail.render_template(procore_company_id_template),
                'filters[origin_id]': f'CE_{rail.result("for_each_paryoll_time_entry")["job"]}'
            }
        )

        def check_if_project_exists_in_procore(procore_project_details):
            if len(procore_project_details) > 0:
                fetched_projects[procore_project_details[0]['origin_id']] = {
                    'project_id' : procore_project_details[0]['id'],
                    'status' : procore_project_details[0]['active']
                }
                return procore_project_details[0]['active']
            return False
        
        check_if_project_exist_in_procore = rail.IfOperator(
            task_id='check_if_project_exist_in_procore',
            test=lambda: check_if_project_exists_in_procore(rail.result('get_procore_project_details')),
            yes_task='get_project_wbs_segments',
            no_task='catch_error'
        )

        def get_project_wbs_segments_endpoint(current_item):
            project_origin_id =f'CE_{current_item["job"]}'
            project_id = fetched_projects[project_origin_id]['project_id']
            return f'/projects/{project_id}/work_breakdown_structure/segments'


        get_project_wbs_segments = rail.ProcoreApiOperator(
            task_id='get_project_wbs_segments',
            endpoint= lambda: get_project_wbs_segments_endpoint(rail.result('for_each_paryoll_time_entry')),
            method='GET',
            data_handler= lambda response: next((seg['id'] for seg in response if seg.get('type') == config.cost_code_segment_type and seg.get(
                'name') == config.cost_code_segment_name and seg.get('tiered') == True), None)
        )

        def get_project_segment_items_endpoint(current_item,segment_id):
            project_origin_id =f'CE_{current_item["job"]}'
            project_id = fetched_projects[project_origin_id]['project_id']
            return f'/projects/{project_id}/work_breakdown_structure/segments/{segment_id}/segment_items'

        get_project_segment_items = rail.ProcoreApiOperator(
            task_id='get_project_segment_items',
            endpoint= lambda: get_project_segment_items_endpoint(rail.result('for_each_paryoll_time_entry'), rail.result("get_project_wbs_segments")),
            method='GET'
        )

        def update_fetched_projects_var(project_segment_items, current_item):
            project_origin_id =f'CE_{current_item["job"]}'
            fetched_projects[project_origin_id]['project_segment_items'] =  project_segment_items

        update_fetched_projects = rail.PythonOperator(
            task_id='update_fetched_projects',
            python_callable=lambda: update_fetched_projects_var(rail.result('get_project_segment_items'), rail.result('for_each_paryoll_time_entry'))
        )

        def get_timecard_time_type_id(pay_type):
            pay_types_mapper = config.pay_types
            timecard_time_types = rail.result('list_timecard_time_types')
            time_type_id_to_return = timecard_time_types.get(pay_types_mapper.get(pay_type))
            return time_type_id_to_return

        def get_wbs_code(phase, category):
            if phase and category:
                return f'{phase}-{category}'
            elif phase:
                return phase
            elif category:
                return category
        
        def get_cost_code_id(phase, category, project_segment_items):
            wbs_code = get_wbs_code(phase, category)
            for entry in project_segment_items:
                if 'path_code' in entry and normalize_ce_identifier(entry['path_code']) == wbs_code:
                    return entry['id']

        def get_party_id(employee_origin_id, procore_user_details, procore_party_ids):
            if employee_origin_id in procore_user_details:
                user_id = str(procore_user_details[employee_origin_id])
                if user_id in procore_party_ids:
                    return procore_party_ids[user_id]

        def prepare_time_entry_payload_data(current_item, procore_user_details, procore_party_ids):
            project_origin_id = f'CE_{current_item["job"]}'
            employee_origin_id = f'CE_{current_item["employee"]}'
            project_id = fetched_projects[project_origin_id]['project_id']
            date = current_item['date']
            hours = round(float(current_item['total_time']) / 3600, 2)
            billable = config.sync_time_entries_as_billable
            description = current_item['description']
            pay_type = current_item['pay_type'].lower()
            timecard_time_type_id = get_timecard_time_type_id(pay_type)
            cost_code_id = get_cost_code_id(current_item['phase'], current_item['category'], fetched_projects[project_origin_id]['project_segment_items'])
            party_id = get_party_id(employee_origin_id, procore_user_details, procore_party_ids)
            if party_id == None or pay_type == None or timecard_time_type_id == None or cost_code_id == None:
                return None
            
            origin_id = f'CE_{current_item["uuid"]}'

            return {
                "project_id": int(project_id),
                "timecard_entry": {
                    "date": date,
                    "hours": str(hours),
                    "billable": bool(billable),
                    "description": description,
                    "timecard_time_type_id": int(timecard_time_type_id),
                    "cost_code_id": int(cost_code_id),
                    "party_id": int(party_id),
                    "approval_status": "approved",
                    "origin_id": origin_id
                }
            }

        prepare_time_entry_payload = rail.PythonOperator(
            task_id='prepare_time_entry_payload',
            python_callable=lambda: prepare_time_entry_payload_data(rail.result('for_each_paryoll_time_entry'), rail.result('get_procore_user_details'), rail.result('get_procore_party_ids'))
        )

        check_if_valid_payload = rail.IfOperator(
            task_id='check_if_valid_payload',
            test=lambda: rail.result('prepare_time_entry_payload') != None,
            yes_task='sync_payroll_time_data',
            no_task='catch_error'
        )

        sync_payroll_time_data = rail.ProcoreApiOperator(
            task_id='sync_payroll_time_data',
            endpoint= '/companies/{procore_company_id_template}/timecard_entries',
            method='POST',
            data=lambda: rail.result('prepare_time_entry_payload')
        )

        def get_error_details():
            employee_origin_id = ''
            project_origin_id = ''
            time_entry_uuid = ''
            reason = ''
            status = ''
            try:
                current_item = rail.result('for_each_paryoll_time_entry')
                employee_origin_id =f'CE_{current_item["employee"]}'
                project_origin_id =f'CE_{current_item["job"]}'
                time_entry_uuid = current_item['uuid']
                procore_user_details = rail.result('get_procore_user_details')
                procore_party_ids = rail.result('get_procore_party_ids')


                if employee_origin_id not in procore_user_details or str(procore_user_details[employee_origin_id]) not in procore_party_ids:
                    reason = 'Employee does not exist in Procore or is inactive or the party_id is not found.'
                elif project_origin_id not in fetched_projects or fetched_projects[project_origin_id]['status'] == False:
                    reason = 'Project does not exist in Procore or is inactive'
                else:
                    reason = "Pay Type or Cost Code doesn't exist"                    
                
                err = rail.render_template('{{ get_error_message() }}')
                if type(err) == str:
                    status = 'Error'
                    reason += err
                else:
                    status = err['response']['status_code'] \
                        if err.get('response') else 'Error'
                    reason += err['response']['json']['error']['reason'] \
                        if err.get('response') else err
            except Exception as e:
                status = "Exception"
                reason += f"An exception occurred: {str(e)}"
            
            return {
                'employee_origin_id': employee_origin_id,
                'project_origin_id': project_origin_id,
                'time_entry_uuid': time_entry_uuid,
                'reason': reason,
                'status': status
            }

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=get_error_details
        )

        catch_unhandled_error = rail.WriteLogOperator(
            task_id='catch_unhandled_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=get_error_details
        )

        capture_time_sync_failure = rail.EmptyOperator(
            task_id='capture_time_sync_failure'
        )

        is_for_loop_execution_completed = rail.IfOperator(
                task_id='is_for_loop_execution_completed',
                test='{{ get_task_state("for_each_paryoll_time_entry") == "success"  or get_task_state("get_procore_party_ids") == "skipped" or get_task_state("get_procore_party_ids") == "failed"}}',
                yes_task='search_logs'
            )

        search_logs = rail.FilterLogEntriesOperator(
            task_id='search_logs',
            severity='Error/Exception',
            trigger_rule= 'all_done'
        )

        if_logs_present = rail.IfOperator(
            task_id='if_logs_present',
            test='{{ result("search_logs", "length") > 0 }}',
            yes_task='write_logs_into_csv',
            no_task='log_to_sumo'
        )

        write_logs_into_csv = rail.WriteCSVFileOperator(
            task_id='write_logs_into_csv',
            source='{{ result("search_logs") }}',
            header=['employee_origin_id', 'project_origin_id', 'time_entry_uuid',
                    'Reason', 'Status', 'ECID'],
            row=[
                "{{ item.properties | attr_or_default('employee_origin_id','') }}",
                "{{ item.properties | attr_or_default('project_origin_id','') }}",
                "{{ item.properties | attr_or_default('time_entry_uuid','') }}",
                "{{ item.properties | attr_or_default('reason','') }}",
                "{{ item.properties | attr_or_default('status','') }}",
                "{{ item | attr_or_default('ecid','') }}"
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name='{{ result("write_logs_into_csv") }}',
            output_file_name='ComputereaseProcore_PayrollTimeSyncLogs - {{ current_time() }}.csv',
            expires_in_seconds=60 * 60 * 24 * 7
        )

        send_email_alert = rail.EmailOperator(
            task_id='send_email_alert',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject='Computerease-Procore Integration: Payroll Time Sync completed with errors - {{ current_time() }}',
            html_content='/email_templates/payroll_time_sync.html'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> log_to_sumo
        batch_task >> get_last_sync_time >> get_ce_time_entries >> set_last_sync_time >> list_timecard_time_types \
            >> get_procore_user_details >> get_procore_party_ids >> for_each_paryoll_time_entry
                
        for_each_paryoll_time_entry >> check_if_employee_exist

        check_if_employee_exist >> rail.Label('Yes') >> check_if_project_details_exist
        check_if_employee_exist >> rail.Label('No') >> catch_error

        check_if_project_details_exist >> rail.Label('Yes') >> prepare_time_entry_payload
        check_if_project_details_exist >> rail.Label('No') >> get_procore_project_details >> check_if_project_exist_in_procore

        check_if_project_exist_in_procore >> rail.Label('Yes') >> get_project_wbs_segments >> get_project_segment_items >> update_fetched_projects >> prepare_time_entry_payload
        check_if_project_exist_in_procore >> rail.Label('No') >> catch_error

        prepare_time_entry_payload >> check_if_valid_payload

        check_if_valid_payload >> rail.Label('Yes') >> sync_payroll_time_data >> capture_time_sync_failure >> catch_unhandled_error >> end_for_each_payroll_time_entry_item
        check_if_valid_payload >> rail.Label('No') >> catch_error 

        catch_error >> end_for_each_payroll_time_entry_item

        for_each_paryoll_time_entry >> end_for_each_payroll_time_entry_item
        
        end_for_each_payroll_time_entry_item >> is_for_loop_execution_completed

        is_for_loop_execution_completed >> rail.Label('Yes') >> search_logs >> if_logs_present

        if_logs_present >> rail.Label(
            'Yes') >> write_logs_into_csv >> generate_download_link >> send_email_alert >> log_to_sumo
        if_logs_present >> rail.Label('No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
