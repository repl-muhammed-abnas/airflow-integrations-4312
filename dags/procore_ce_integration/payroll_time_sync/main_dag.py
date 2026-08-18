import rail
from datetime import date, timedelta
from rail.lib.last_sync_time_store import get_lastsync_time_variable, set_lastsync_time_variable
from procore_ce_integration.initial_setup_sync.shared_utils import get_tenant_email


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description='Procore To Computerease Payroll Time Sync',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.main_dag_max_active_runs,
        schedule_interval=timedelta(
            seconds=config.payroll_time_sync_interval_seconds),
        default_args={
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        procore_company_id_template = "{{ conn." + config.procore_conn_id + ".extra_dejson.company_id }}"
        fetched_procore_projects = {}

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
                date_format=config.procore_time_format,
                initial_sync_time=config.initial_sync_time,
                reset_after_threshold=False
            ),
        )

        def filter_by_employee_and_approved_status(response):
            time_entries_by_employee = {}
            procore_project_ids = []
            if response:
                for entry in response:
                    project_id = entry['project_id']
                    time_type = entry['timecard_time_type']['time_type'] if entry['timecard_time_type'] else 'Regular Time'
                    approval_status = entry['approval_status']
                    if approval_status == 'approved' and not entry['deleted_at'] and entry['party_id'] and project_id and time_type in config.pay_types and not entry['origin_id']:
                        id = str(entry['party']['user_id']) if config.employee_based_on_origin_id else str(entry['party']['employee_id'])                        
                        if project_id not in procore_project_ids:
                            procore_project_ids.append(project_id)
                        cost_code = {
                            'code': entry['cost_code'].get('code'),
                            'parent': {'id': entry['cost_code'].get('parent_id')}
                        } if entry['cost_code_id'] and entry.get('cost_code') else None
                        if id in time_entries_by_employee:
                            time_entries_by_employee[id].append({
                                'id': id,
                                'time_entry_id': entry['id'],
                                'cost_code_id': entry['cost_code_id'],
                                'cost_code_full_code': entry['cost_code']['full_code'] if entry['cost_code_id'] else None,
                                'cost_code': cost_code,
                                'approval_status': approval_status,
                                'billable': entry['billable'],
                                'date': entry['date'],
                                'description': entry['description'],
                                'hours': entry['hours'],
                                'party_id': entry['party_id'],
                                'project_id': str(project_id),
                                'job_code': entry['project']['name'].split(' - ')[0].strip(),
                                'time_type': time_type
                            })
                        else:
                            time_entries_by_employee[id]=[{
                                'id': id,
                                'time_entry_id': entry['id'],
                                'cost_code_id': entry['cost_code_id'],
                                'cost_code_full_code': entry['cost_code']['full_code'] if entry['cost_code_id'] else None,
                                'cost_code': cost_code,
                                'approval_status': approval_status,
                                'billable': entry['billable'],
                                'date': entry['date'],
                                'description': entry['description'],
                                'hours': entry['hours'],
                                'party_id': entry['party_id'],
                                'project_id': str(project_id),
                                'job_code': entry['project']['name'].split(' - ')[0].strip(),
                                'time_type': time_type
                            }]
                procore_project_ids = [procore_project_ids[i:i + config.project_chunk_size] for i in range(0, len(procore_project_ids), config.project_chunk_size)]
            return {
                'procore_project_ids': procore_project_ids,
                'time_entries_by_employee': time_entries_by_employee
            }

        get_procore_time_entries = rail.ProcoreApiOperator(
            task_id='get_procore_time_entries',
            endpoint= f'/companies/{procore_company_id_template}/timecard_entries',
            method='GET',
            query_params=lambda:{
                'page': 1,
                'per_page': 1000,
                'start_date': date.today() - timedelta(days=config.lookback_days),
                'end_date': date.today() + timedelta(days=config.lookahead_days),
                'filters[updated_at]': f'{rail.result("get_last_sync_time")["last_synctime"]} ... {rail.result("get_last_sync_time")["current_time"]}'
            },
            data_handler=filter_by_employee_and_approved_status
        )

        set_last_sync_time = rail.PythonOperator(
            task_id='set_last_sync_time',
            python_callable=lambda: set_lastsync_time_variable(
                variable_name=config.payroll_time_last_sync_time_var,
                value_to_set=rail.result('get_last_sync_time')['current_time']
            )
        )

        does_time_entry_exist = rail.IfOperator(
            task_id='does_time_entry_exist',
            test= lambda: len(rail.result("get_procore_time_entries")["time_entries_by_employee"]) > 0,
            yes_task='is_procore_user_details_required',
            no_task='log_to_sumo'
        )

        is_procore_user_details_required = rail.IfOperator(
            task_id='is_procore_user_details_required',
            test= config.employee_based_on_origin_id,
            yes_task='get_procore_user_details',
            no_task='is_procore_project_details_required'
        )

        get_procore_user_details = rail.ProcoreApiOperator(
            task_id='get_procore_user_details',
            endpoint=f'/companies/{procore_company_id_template}/users',
            method='GET',
            version='1.3',
            data_handler= lambda resp: {entry['id'] : entry['origin_id'] for entry in resp} if resp else {}
        )

        is_procore_project_details_required = rail.IfOperator(
            task_id='is_procore_project_details_required',
            test= config.project_based_on_origin_id,
            yes_task='for_each_procore_project',
            no_task='log_to_sumo'
        )

        for_each_procore_project = rail.ForEachOperator(
            task_id='for_each_procore_project',
            items= lambda: rail.result('get_procore_time_entries')['procore_project_ids'],
            start_task='get_procore_project_details',
            end_task='end_for_each_procore_project_item'
        )

        end_for_each_procore_project_item = rail.EmptyOperator(
            task_id='end_for_each_procore_project_item'
        )

        get_procore_project_details = rail.ProcoreApiOperator(
            task_id='get_procore_project_details',
            endpoint='/projects',
            method='GET',
            query_params=lambda: {
                'company_id': rail.render_template(procore_company_id_template),
                'filters[id]': str(rail.result("for_each_procore_project"))
            },
            data_handler= lambda resp: {entry['id'] : entry['origin_id'] for entry in resp} if resp else {}
        )

        def update_fetched_procore_projects():
            procore_project_details = rail.result('get_procore_project_details')
            fetched_procore_projects.update(procore_project_details)

        update_fetched_projects = rail.PythonOperator(
            task_id='update_fetched_projects',
            python_callable=lambda: update_fetched_procore_projects()
        )
        
        trigger_payroll_time_sync_child_dags = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_payroll_time_sync_child_dags',
            items=lambda: list(rail.result("get_procore_time_entries")["time_entries_by_employee"].values()),
            trigger_dag_id=config.child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'time_data': item,
                'fetched_procore_projects': fetched_procore_projects,
                'procore_user_details': rail.result("get_procore_user_details") if config.employee_based_on_origin_id else {},
                'project_based_on_origin_id': config.project_based_on_origin_id,
                'employee_based_on_origin_id': config.employee_based_on_origin_id,
                'pay_types': config.pay_types
            }
        )

        wait_for_payroll_time_sync = rail.WaitForDagRunsSensor(
            task_id='wait_for_payroll_time_sync',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_payroll_time_sync_child_dags") }}'
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
            header=['procore_time_entry_id', 'employee', 'job', 'phase', 'category', 'date', 'Reason', 'Status', 'ECID'],
            row=[
                "{{ item.properties | attr_or_default('procore_time_entry_id','') }}",
                "{{ item.properties | attr_or_default('employee','') }}",
                "{{ item.properties | attr_or_default('job','') }}",
                "{{ item.properties | attr_or_default('phase','') }}",
                "{{ item.properties | attr_or_default('category','') }}",
                "{{ item.properties | attr_or_default('date','') }}",
                "{{ item.properties | attr_or_default('reason','') }}",
                "{{ item.properties | attr_or_default('status','') }}",
                "{{ item | attr_or_default('ecid','') }}"
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name='{{ result("write_logs_into_csv") }}',
            output_file_name='ProcoreComputerease_PayrollTimeSyncLogs - {{ current_time() }}.csv',
            expires_in_seconds=60 * 60 * 24 * 7
        )

        send_email_alert = rail.EmailOperator(
            task_id='send_email_alert',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject='Procore-Computerease Integration: Payroll Time Sync completed with errors - {{ current_time() }}',
            html_content='/email_templates/payroll_time_sync.html'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> log_to_sumo
        batch_task >> get_last_sync_time >> get_procore_time_entries >> set_last_sync_time >> does_time_entry_exist
        
        does_time_entry_exist >> rail.Label('Yes') >> is_procore_user_details_required
        does_time_entry_exist >> rail.Label('No') >> log_to_sumo #catch_unhandled_error

        is_procore_user_details_required >> rail.Label('Yes') >> get_procore_user_details >> is_procore_project_details_required
        is_procore_user_details_required >> rail.Label('No') >> is_procore_project_details_required

        is_procore_project_details_required >> rail.Label('Yes') >> for_each_procore_project
        is_procore_project_details_required >> rail.Label('No') >> log_to_sumo
        
                
        for_each_procore_project >> get_procore_project_details >> update_fetched_projects >> end_for_each_procore_project_item

        for_each_procore_project >> end_for_each_procore_project_item

        end_for_each_procore_project_item >> trigger_payroll_time_sync_child_dags >> wait_for_payroll_time_sync >> search_logs >> if_logs_present

        if_logs_present >> rail.Label(
            'Yes') >> write_logs_into_csv >> generate_download_link >> send_email_alert >> log_to_sumo
        if_logs_present >> rail.Label('No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
