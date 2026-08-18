import json
import rail
from datetime import timedelta

from ce_procore_integration.change_orders_sync.utils.util import convert_ce_date_to_procore, build_flat_code
from ce_procore_integration.util_dags.utils import get_tenant_email


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description='ComputerEase to Procore Change Order Sync - Main DAG',
        schedule_interval='0 0 * * *' if config.input_source == 'email' else timedelta(
            minutes=config.interval_minutes),
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.main_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
            'imap_conn_id': config.imap_conn_id,
            'procore_conn_id': config.procore_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        if config.input_source == 'email':
            def extract_csv_attachments_func(response):
                if not response:
                    return

                change_order_file = ''
                job_cost_file = ''
                change_order_file_artifact = ''
                job_cost_file_artifact = ''
                for email in response:
                    if email.get('attachments'):
                        for attachment in email['attachments']:
                            filename = attachment['filename']
                            if filename == f'{config.co_report_filename}.csv':
                                change_order_file = filename
                                change_order_file_artifact = attachment['artifact']
                            elif filename == f'{config.job_cost_detail_report_filename}.csv':
                                job_cost_file = filename
                                job_cost_file_artifact = attachment['artifact']

                return {
                    'change_order_file': change_order_file,
                    'job_cost_file': job_cost_file,
                    'change_order_file_artifact': change_order_file_artifact,
                    'job_cost_file_artifact': job_cost_file_artifact,
                    'is_files_present': bool(change_order_file_artifact and job_cost_file_artifact)
                }

            read_emails_from_inbox = rail.ReadEmailOperator(
                task_id='read_emails_from_inbox',
                subject_pattern=config.email_subject_pattern,
                limit=config.email_limit,
                max_emails_to_check=config.email_max_to_check,
                data_handler=extract_csv_attachments_func
            )

            is_matching_email_found = rail.IfOperator(
                task_id='is_matching_email_found',
                test='{{ result("read_emails_from_inbox") | sn | is_truthy }}',
                yes_task='process_reports',
                no_task='delete_this_dagrun'
            )

            process_reports = rail.EmptyOperator(
                task_id='process_reports'
            )

        else:
            check_for_new_files = rail.SFTPAnyFileSensor(
                task_id='check_for_new_files',
                path=config.file_path,
                soft_fail_timeout=timedelta(
                    minutes=config.sftp_sensor_timeout_minutes)
            )

            list_all_files = rail.SFTPListFilesOperator(
                task_id='list_all_files',
                paths=[config.file_path]
            )

            def find_report_files_func():
                all_files = rail.result('list_all_files')[config.file_path]
                change_order_file = ''
                job_cost_file = ''

                for fileattributes in all_files:
                    if f'{config.co_report_filename}.csv' == fileattributes['name']:
                        change_order_file = f"{config.file_path}/{fileattributes['name']}"
                    elif f'{config.job_cost_detail_report_filename}.csv' == fileattributes['name']:
                        job_cost_file = f"{config.file_path}/{fileattributes['name']}"

                return {
                    'change_order_file': change_order_file,
                    'job_cost_file': job_cost_file,
                    'is_files_present': bool(change_order_file and job_cost_file)
                }

            find_report_files = rail.PythonOperator(
                task_id='find_report_files',
                python_callable=find_report_files_func
            )

            download_change_order_file = rail.SFTPDownloadFileOperator(
                task_id='download_change_order_file',
                remote_filepath="{{ result('find_report_files').change_order_file }}"
            )

            download_job_cost_file = rail.SFTPDownloadFileOperator(
                task_id='download_job_cost_file',
                remote_filepath="{{ result('find_report_files').job_cost_file }}"
            )

            was_file_found = rail.IfOperator(
                task_id='was_file_found',
                trigger_rule='all_done',
                test='{{ get_task_state("check_for_new_files") == "success" }}',
                yes_task='process_archive',
                no_task='delete_this_dagrun'
            )

            process_archive = rail.EmptyOperator(
                task_id='process_archive'
            )

            should_archive_change_order_file = rail.IfOperator(
                task_id='should_archive_change_order_file',
                test='{{ result("find_report_files").change_order_file | is_truthy }}',
                yes_task='archive_change_order_file',
                no_task='should_archive_job_cost_file'
            )

            archive_change_order_file = rail.SFTPMoveFileOperator(
                task_id='archive_change_order_file',
                existing_filename="{{ result('find_report_files').change_order_file }}",
                new_filename=config.archive_filepath +
                '/{{ dag_run_ecid() }}_{{ result("find_report_files").change_order_file | file_name }}'
            )

            should_archive_job_cost_file = rail.IfOperator(
                task_id='should_archive_job_cost_file',
                test='{{ result("find_report_files").job_cost_file | is_truthy }}',
                yes_task='archive_job_cost_file'
            )

            archive_job_cost_file = rail.SFTPMoveFileOperator(
                task_id='archive_job_cost_file',
                existing_filename="{{ result('find_report_files').job_cost_file }}",
                new_filename=config.archive_filepath +
                '/{{ dag_run_ecid() }}_{{ result("find_report_files").job_cost_file | file_name }}'
            )

        report_test_templated = "{{ result('read_emails_from_inbox').is_files_present | sn | is_truthy }}" if config.input_source == 'email' else "{{ result('find_report_files').is_files_present | sn | is_truthy }}"
        has_both_reports = rail.IfOperator(
            task_id='has_both_reports',
            test=report_test_templated,
            yes_task='load_change_order_csv' if config.input_source == 'email' else 'download_change_order_file',
            no_task='send_missing_files_notification'
        )

        send_missing_files_notification = rail.EmailOperator(
            task_id='send_missing_files_notification',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject="Computerease-Procore Integration: Change Order Sync - Missing Required Files - {{ current_time() }}",
            html_content='/email_templates/missing_file_failure.html',
            params={
                'file_result_task': 'read_emails_from_inbox' if config.input_source == 'email' else 'find_report_files'
            }
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        change_order_doc = "{{ result('read_emails_from_inbox').change_order_file_artifact }}" if config.input_source == 'email' else "{{ result('download_change_order_file') }}"
        load_change_order_csv = rail.LoadCSVFileOperator(
            task_id='load_change_order_csv',
            document=change_order_doc
        )

        create_change_order_collection = rail.CreateCollectionOperator(
            task_id='create_change_order_collection',
            source='{{ result("load_change_order_csv") }}',
            columns={
                "Budget Change": "budget_change",
                "C/O Number": "co_number",
                "Date": "date",
                "Description": "description",
                "Job": "job",
                "Job Class": "job_class",
                "Job Name": "job_name",
                "RFC Number": "rfc_number",
                "Status": "status",
                "Type": "type"
            }
        )

        is_input_data_present = rail.IfOperator(
            task_id='is_input_data_present',
            test="{{ result('create_change_order_collection', 'length') > 0 }}",
            yes_task='batch_task',
            no_task='delete_this_dagrun'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='load_job_cost_detail_csv',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        job_cost_detail_doc = "{{ result('read_emails_from_inbox').job_cost_file_artifact }}" if config.input_source == 'email' else "{{ result('download_job_cost_file') }}"
        load_job_cost_detail_csv = rail.LoadCSVFileOperator(
            task_id='load_job_cost_detail_csv',
            document=job_cost_detail_doc
        )

        create_job_cost_detail_collection = rail.CreateCollectionOperator(
            task_id='create_job_cost_detail_collection',
            name='job_cost_details',
            source='{{ result("load_job_cost_detail_csv") }}',
            columns={
                "Job": "job",
                "Job Name": "job_name",
                "Phase": "phase",
                "Phase Name": "phase_name",
                "Category": "category",
                "Category Name": "category_name",
                "Cost Type": "cost_type",
                "Cost Type Name": "cost_type_name",
                "P.O.": "po_number",
                "Cost Budget": "cost_budget",
                "Contract Amount": "contract_amount"
            }
        )

        filter_rfcs = rail.QueryCollectionOperator(
            task_id='filter_rfcs',
            name='change_orders',
            query='''SELECT * FROM create_change_order_collection
                  WHERE NULLIF(TRIM(rfc_number), '') IS NOT NULL
                  AND NULLIF(TRIM(co_number), '') IS NOT NULL
                  AND NULLIF(TRIM(job), '') IS NOT NULL
                  AND NULLIF(TRIM(status), '') = :status
                  AND NULLIF(TRIM(type), '') = :customer''',
            query_params={
                'status': config.status_to_sync,
                'customer': config.types_to_sync
            }
        )

        aggregate_job_cost_details = rail.QueryCollectionOperator(
            task_id='aggregate_job_cost_details',
            name='aggregated_job_costs',
            query='''SELECT
                  jcd.job,
                  jcd.po_number,
                  jcd.phase,
                  jcd.phase_name,
                  jcd.category,
                  jcd.category_name,
                  jcd.cost_type,
                  jcd.cost_type_name,
                  SUM(CAST(jcd.contract_amount AS DECIMAL)) as total_contract_amount,
                  SUM(CAST(jcd.cost_budget AS DECIMAL)) as total_cost_budget
                  FROM job_cost_details jcd
                  INNER JOIN change_orders co ON jcd.job = co.job AND jcd.po_number = co.rfc_number
                  WHERE NULLIF(TRIM(jcd.po_number), '') IS NOT NULL
                  GROUP BY jcd.job, jcd.po_number, jcd.phase, jcd.phase_name, jcd.category, jcd.category_name, jcd.cost_type, jcd.cost_type_name'''
        )

        def transform_rfcs():
            filtered_data = rail.load_all_records(rail.result('filter_rfcs'))
            job_cost_data = rail.load_all_records(
                rail.result('aggregate_job_cost_details'))

            job_cost_lookup = {}
            for jc in job_cost_data:
                key = (jc['job'].strip(), jc['po_number'].strip())
                if key not in job_cost_lookup:
                    job_cost_lookup[key] = []

                phase = (jc.get('phase') or '').strip()
                category = (jc.get('category') or '').strip()
                cost_type = (jc.get('cost_type') or '').strip()

                flat_code = build_flat_code(phase, category, cost_type)

                if flat_code:
                    job_cost_lookup[key].append({
                        'phase': phase,
                        'phase_name': (jc.get('phase_name') or '').strip(),
                        'category': category,
                        'category_name': (jc.get('category_name') or '').strip(),
                        'cost_type': cost_type,
                        'cost_type_name': (jc.get('cost_type_name') or '').strip(),
                        'flat_code': flat_code,
                        'contract_amount': str(jc.get('total_contract_amount', 0) or 0),
                        'cost_budget': str(jc.get('total_cost_budget', 0) or 0)
                    })

            rfcs = []
            for item in filtered_data:
                description = f"CE #{item.get('co_number', '').strip()} - {item.get('description', '').strip()}"

                job_code = item['job'].strip()
                rfc_number = item['rfc_number'].strip()

                budget_line_items = job_cost_lookup.get((job_code, rfc_number), [])

                has_financial_data = len(budget_line_items) > 0

                if has_financial_data:
                    rfc_data = {
                        'job_code': job_code,
                        'job_name': item['job_name'].strip(),
                        'job_class': (item.get('job_class') or '').strip(),
                        'rfc_number': rfc_number,
                        'co_number': item['co_number'].strip(),
                        'status': item['status'].strip(),
                        'description': description,
                        'date': convert_ce_date_to_procore(item['date']),
                        'budget_line_items': json.dumps(budget_line_items),
                    }

                    rfcs.append(rfc_data)

            return rfcs

        parse_rfcs = rail.PythonOperator(
            task_id='parse_rfcs',
            python_callable=transform_rfcs
        )

        has_rfcs_to_sync = rail.IfOperator(
            task_id='has_rfcs_to_sync',
            test="{{ result('parse_rfcs') | length > 0 }}",
            yes_task='create_rfcs_collection',
            no_task='log_to_sumo'
        )

        create_rfcs_collection = rail.CreateCollectionOperator(
            task_id='create_rfcs_collection',
            name='transformed_rfcs',
            source=lambda: rail.result('parse_rfcs')
        )

        get_distinct_jobs = rail.QueryCollectionOperator(
            task_id='get_distinct_jobs',
            name='distinct_jobs',
            query='''SELECT DISTINCT
                  job_code,
                  job_name,
                  job_class FROM transformed_rfcs'''
        )

        procore_company_id_template = '{{ conn.' + config.procore_conn_id + '.extra_dejson.company_id }}'

        fetch_procore_projects = rail.ProcoreApiOperator(
            task_id='fetch_procore_projects',
            endpoint='/projects',
            method='GET',
            query_params=lambda: {
                'company_id': rail.render_template(procore_company_id_template)
            },
            data_handler=lambda response: {
                project['origin_id']: {
                    'id': project['id'],
                    'display_name': project['display_name']
                } for project in response if project.get('origin_id')
            }
        )

        def get_aggregate_rfcs_by_job():
            distinct_jobs = rail.load_all_records(
                rail.result('get_distinct_jobs'))
            all_rfcs = rail.load_all_records(
                rail.result('create_rfcs_collection'))
            procore_projects = rail.result('fetch_procore_projects')

            jobs_dict = {}
            invalid_jobs = set()
            for job in distinct_jobs:
                job_code = job['job_code']
                if not procore_projects.get(f'CE_{job_code}'):
                    invalid_jobs.add(job_code)
                    continue
                jobs_dict[job_code] = {
                    'job_code': job_code,
                    'job_name': job['job_name'],
                    'job_class': job['job_class'],
                    'rfcs': []
                }

            job_wbs_codes = {}

            for rfc in all_rfcs:
                job_code = rfc['job_code']
                if job_code in jobs_dict:
                    jobs_dict[job_code]['rfcs'].append(rfc)

                    if job_code not in job_wbs_codes:
                        job_wbs_codes[job_code] = {}

                    budget_line_items = json.loads(
                        rfc.get('budget_line_items', '[]'))

                    if budget_line_items:
                        for budget_item in budget_line_items:
                            flat_code = budget_item.get('flat_code')
                            if flat_code and flat_code not in job_wbs_codes[job_code]:
                                job_wbs_codes[job_code][flat_code] = {
                                    'flat_code': flat_code,
                                    'phase_code': budget_item.get('phase', ''),
                                    'category_code': budget_item.get('category', ''),
                                    'cost_type': budget_item.get('cost_type', '')
                                }

            for job_code in jobs_dict:
                jobs_dict[job_code]['wbs_codes_to_check'] = list(
                    job_wbs_codes.get(job_code, {}).values()
                )

            return {
                'rfcs': list(jobs_dict.values()),
                'invalid_jobs': list(invalid_jobs)
            }

        aggregate_rfcs_by_job = rail.PythonOperator(
            task_id='aggregate_rfcs_by_job',
            python_callable=get_aggregate_rfcs_by_job
        )

        trigger_change_order_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_change_order_child_dag',
            trigger_dag_id=config.job_child_dag_id,
            items=lambda: rail.result('aggregate_rfcs_by_job')['rfcs'],
            conf=lambda item: {
                'batch': item,
                'company_id': rail.render_template(procore_company_id_template),
                'project_id': rail.result('fetch_procore_projects')[f'CE_{item["job_code"]}']['id']
            }
        )

        wait_for_change_order_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_change_order_completion',
            dag_runs='{{ result("trigger_change_order_child_dag") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        has_invalid_batches = rail.IfOperator(
            task_id='has_invalid_batches',
            test=lambda: len(rail.result('aggregate_rfcs_by_job').get(
                'invalid_jobs', [])) > 0,
            yes_task='write_exception',
            no_task='search_logs'
        )

        write_exception = rail.WriteLogOperator(
            task_id='write_exception',
            message='Job Exception',
            severity='Error/Exception',
            properties=lambda item: item,
            items=lambda: [
                {
                    'job_code': job,
                    'error_message': 'Change order not synced - Job not synced in procore'
                } for job in rail.result('aggregate_rfcs_by_job')['invalid_jobs']
            ]
        )

        search_logs = rail.FilterLogEntriesOperator(
            task_id='search_logs',
            severity='Error/Exception'
        )

        if_logs_present = rail.IfOperator(
            task_id='if_logs_present',
            test="{{ result('search_logs', 'length') > 0 }}",
            yes_task='write_logs_into_csv',
            no_task='log_to_sumo'
        )

        write_logs_into_csv = rail.WriteCSVFileOperator(
            task_id='write_logs_into_csv',
            source="{{ result('search_logs') }}"
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name='{{ result("write_logs_into_csv") }}',
            output_file_name='ComputereaseProcore_ChangeOrderSyncLogs - {{ current_time() }}.csv',
            expires_in_seconds=60 * 60 * 24 * 7
        )

        send_email_notification = rail.EmailOperator(
            task_id='send_email_notification',
            to=get_tenant_email(config),
            bcc=config.internal_email,
            subject="Computerease-Procore Integration: Change Order Sync completed with errors - {{ current_time() }}",
            html_content='/email_templates/change_order_failure.html'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        # Conditional branching based on input_source
        if config.input_source == 'email':
            # Email flow
            read_emails_from_inbox >> is_matching_email_found

            is_matching_email_found >> rail.Label(
                'Yes') >> process_reports >> has_both_reports

            has_both_reports >> rail.Label(
                'Yes') >> load_change_order_csv
            has_both_reports >> rail.Label(
                'No') >> send_missing_files_notification

            is_matching_email_found >> rail.Label(
                'No') >> delete_this_dagrun

        else:
            # SFTP flow
            check_for_new_files >> list_all_files >> find_report_files >> has_both_reports

            has_both_reports >> rail.Label(
                'Yes') >> download_change_order_file
            has_both_reports >> rail.Label(
                'No') >> send_missing_files_notification

            download_change_order_file >> download_job_cost_file
            download_job_cost_file >> rail.Label(
                'Always') >> was_file_found

            was_file_found >> rail.Label(
                'Yes') >> process_archive
            was_file_found >> rail.Label(
                'No') >> delete_this_dagrun

            process_archive >> should_archive_change_order_file

            should_archive_change_order_file >> rail.Label(
                'Yes') >> archive_change_order_file >> should_archive_job_cost_file

            should_archive_change_order_file >> rail.Label(
                'No') >> should_archive_job_cost_file

            should_archive_job_cost_file >> rail.Label(
                'Yes') >> archive_job_cost_file

            download_job_cost_file >> load_change_order_csv

        load_change_order_csv >> create_change_order_collection >> is_input_data_present

        is_input_data_present >> rail.Label(
            'Yes') >> batch_task >> load_job_cost_detail_csv >> create_job_cost_detail_collection \
            >> filter_rfcs >> aggregate_job_cost_details >> parse_rfcs >> has_rfcs_to_sync
        is_input_data_present >> rail.Label(
            'No') >> delete_this_dagrun

        has_rfcs_to_sync >> rail.Label(
            'Yes') >> create_rfcs_collection >> get_distinct_jobs >> fetch_procore_projects >> aggregate_rfcs_by_job >> \
            trigger_change_order_child_dag >> wait_for_change_order_completion >> has_invalid_batches
        
        has_invalid_batches >> rail.Label('Yes') >> write_exception >> search_logs
        has_invalid_batches >> rail.Label('No') >> search_logs >> if_logs_present

        has_rfcs_to_sync >> rail.Label(
            'No') >> log_to_sumo

        if_logs_present >> rail.Label(
            'Yes') >> write_logs_into_csv >> generate_download_link >> send_email_notification >> log_to_sumo
        if_logs_present >> rail.Label(
            'No') >> log_to_sumo

        batch_task >> log_to_sumo

    return dag


rail.for_each_instance(create_dag_instance)
