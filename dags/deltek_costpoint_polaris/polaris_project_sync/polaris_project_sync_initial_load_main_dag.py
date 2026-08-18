from datetime import datetime, timedelta
import itertools
from pytz import timezone
from airflow.models import Variable
import rail

# pylint:disable = too-many-statements, line-too-long
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_polaris_project_initial_load_{config.instance}',
        description=f'deltek_costpoint_polaris_project_initial_load_{config.instance}',
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        default_args={
            'deltek_costpoint_conn_id': config.deltek_cospoint_conn_id,
        }
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_last_run_date'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_last_run_date',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def do_get_last_run_date():
            current_time = datetime.now(timezone('UTC')) - timedelta(seconds=2)
            lookup_timestamp_value = Variable.get(
                config.last_run_date_var_name, default_var=None)
            last_run_date = (datetime.fromisoformat(
                lookup_timestamp_value) if lookup_timestamp_value else current_time).isoformat()
            # Variable.set(config.last_run_date_var_name,
            #              current_time.isoformat())
            rail.set_result(current_time.isoformat(), 'current_time')
            return last_run_date

        get_last_run_date = rail.PythonOperator(
            task_id='get_last_run_date',
            python_callable=do_get_last_run_date
        )

        def get_time():
            time_zone = timezone(config.time_zone)
            datetime_in_timezone = datetime.fromisoformat(
                rail.result('get_last_run_date')).astimezone(time_zone)
            return (datetime_in_timezone).replace(tzinfo=None).isoformat()

        def get_filters():
            return [
                {
                    "name": "ACTIVE_FL",
                    "relation": "=",
                    "value": "Y"
                },
                # TC_PROJ_FL Values:
                #     T=Time Collection
                #     E=Expense
                #     B=Time & Expense
                #     N=None
                {
                    "name": "TC_PROJ_FL",
                    "relation": "!=",
                    "value": "E"
                },
                {
                    "name": "TC_PROJ_FL",
                    "relation": "!=",
                    "value": "N"
                },
                {
                    "name": "PJMBASIC_PROJ_LAST_MODIFIED",
                    "relation": "gt=",
                    "value": get_time()
                },
                {
                    "name": "PJMBASIC_PROJ_LAST_MODIFIED",
                    "relation": "lt=",
                    "value": get_time()
                }
            ]

        def get_filters_for_range(start_date, end_date):
            return [
                {
                    "name": "ACTIVE_FL",
                    "relation": "=",
                    "value": "Y"
                },
                # TC_PROJ_FL Values:
                #     T=Time Collection
                #     E=Expense
                #     B=Time & Expense
                #     N=None
                {
                    "name": "TC_PROJ_FL",
                    "relation": "!=",
                    "value": "E"
                },
                {
                    "name": "TC_PROJ_FL",
                    "relation": "!=",
                    "value": "N"
                },
                {
                    "name": "PJMBASIC_PROJ_LAST_MODIFIED",
                    "relation": "gt=",
                    "value": start_date
                },
                {
                    "name": "PJMBASIC_PROJ_LAST_MODIFIED",
                    "relation": "lt=",
                    "value": end_date
                }
            ]

        can_load_data_in_chunks = rail.IfOperator(
            task_id='can_load_data_in_chunks',
            test=lambda: Variable.get(
                    config.get_data_in_chunk_var_name, default_var='false').lower() == 'true',
            yes_task='get_modified_projects_in_chunks',
            no_task='get_modified_projects'
        )

        def get_project_filter_items():
            items = []
            a_to_z_chars = list(map(chr, range(ord('A'), ord('Z')+1)))
            date_ranges = Variable.get(
                config.initial_load_date_ranges_var_name, deserialize_json=True, default_var=[])
            for date_range in date_ranges:
                start_date = date_range['from']
                end_date = date_range['to']
                last_item = []
                for char in a_to_z_chars:
                    items.append([
                        {
                            "name": "PROJ_NAME",
                            "relation": "like%",
                            "value": char
                        }
                    ] + get_filters_for_range(start_date, end_date))
                    last_item.append({
                        "name": "PROJ_NAME",
                        "relation": "not like%",
                        "value": char
                    })
                last_item = last_item + get_filters_for_range(start_date, end_date)
                items.append(last_item)
            return items

        get_modified_projects_in_chunks = rail.DeltekCostPointServiceCallForEachItemOperator(
            task_id='get_modified_projects_in_chunks',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_cospoint_company_ids,
            items=get_project_filter_items,
            data=lambda item: {
                "filter": {
                    "id": "polaris_exp_project",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "PJMBASIC_PROJ",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": item
                                    }
                                ]
                            }
                        }
                    ]
                }
            },
            data_handler=lambda data: data['document']['rows'],
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            flatten=True
        )

        get_modified_projects = rail.DeltekCostPointServiceOperator(
            task_id='get_modified_projects',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_cospoint_company_ids,
            data=lambda: {
                "filter": {
                    "id": "polaris_exp_project",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "PJMBASIC_PROJ",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": get_filters()
                                    }
                                ]
                            }
                        }
                    ]
                }
            },
            data_handler=lambda data: data['document']['rows'],
        )            
        
        def get_base_project_ids():
            projects = (rail.result('get_modified_projects') or
                        rail.result('get_modified_projects_in_chunks')) or []
            base_project_ids = [project['row']['data']['PROJ_ID'].split(".")[0]
                                for project in projects]
            chunk_size = int(Variable.get(
                config.project_chunk_number_var_name, default_var=10))
            return [base_project_ids[i:i + chunk_size]
                    for i in range(0, len(base_project_ids), chunk_size)]
            
        def get_date_range_filter(start_date, end_date):
            filters = [
                {
                    "name": "PJMBASIC_PROJ_LAST_MODIFIED",
                    "relation": "gt=",
                    "value": start_date
                },
                {
                    "name": "PJMBASIC_PROJ_LAST_MODIFIED",
                    "relation": "lt=",
                    "value": end_date
                }
            ]
            return filters

        def get_project_ids_filter_items():
            items = []
            date_ranges = Variable.get(
                config.initial_load_date_ranges_var_name, deserialize_json=True, default_var=[])
            for date_range in date_ranges:
                start_date = date_range['from']
                end_date = date_range['to']
                for chunk in get_base_project_ids():
                    items.append([
                        {
                            "name": "PROJ_ID",
                            "relation": "like%",
                            "value": project_id
                        }
                        for project_id in chunk
                    ] + get_date_range_filter(start_date, end_date))
            return items

        has_project_data = rail.IfOperator(
            task_id='has_project_data',
            test=lambda: bool((rail.result('get_modified_projects') or rail.result(
                'get_modified_projects_in_chunks'))),
            yes_task='group_data_by_root_project',
            no_task='delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        group_data_by_root_project = rail.PythonOperator(
            task_id='group_data_by_root_project',
            python_callable=lambda: [{'root_project_id': k, 'data': list(g)} for k, g in itertools.groupby(
                (rail.result('get_modified_projects') or rail.result('get_modified_projects_in_chunks')), lambda x: x['row']['data']['PROJ_ID'].split(".")[0])]
        )

        get_all_clients_from_replicon = rail.RepliconServiceOperator(
            task_id='get_all_clients_from_replicon',
            endpoint="/services/ClientListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "999999",
                "columnUris": [
                    "urn:replicon:client-list-column:client",
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda data: list(
                map(lambda x: x['cells'][0]['textValue'], data['rows']))
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
        )

        def project_role_list_input(response):
            rows = response.json()['d']['rows']
            return list(map(lambda row: {
                "name": row['cells'][0].get('textValue'),
                "code": row['cells'][1].get('textValue'),
                "uri": row['cells'][2].get('uri')
            }, rows)) if rows else []

        get_costpoint_plcs = rail.DeltekCostPointServiceOperator(
            task_id='get_costpoint_plcs',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_cospoint_company_ids,
            data={
                "filter": {
                    "id": "polaris_exp_plcs",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "ADMUDT07_HDR",
                                "conditions": [
                                ],
                                "children": [
                                ]
                            }
                        }
                    ]
                }
            },
            data_handler=lambda data: list(map(lambda x: {
                                           'name': x['row']['data']['UDT07_NAME'][0:50], 'code': x['row']['data']['UDT07_ID']}, data['document']['rows'])),
        )

        get_all_roles = rail.RepliconServiceOperator(
            task_id='get_all_roles',
            endpoint="/services/ProjectRoleListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000000",
                "columnUris":  [
                    "urn:replicon:project-role-list-column:name",
                    "urn:replicon:project-role-list-column:description",
                    "urn:replicon:project-role-list-column:project-role"
                ],
                "sort": [],
                "filterExpression": null
            },
            response_filter=project_role_list_input
        )

        get_replicon_divisions = rail.RepliconServiceOperator(
            task_id='get_replicon_divisions',
            endpoint='/services/DivisionListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "999999",
                "columnUris": [
                    "urn:replicon:division-list-column:division",
                    "urn:replicon:division-list-column:code"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda data: list(map(lambda x: {'name': x['cells'][0].get(
                'textValue'), 'code': x['cells'][1].get('textValue'), 'uri': x['cells'][0]['uri']}, data['rows']))
        )

        get_project_udfs = rail.RepliconServiceOperator(
            task_id='get_project_udfs',
            endpoint="/services/ProjectCustomFieldListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "999999",
                "columnUris": [
                    "urn:replicon:project-custom-field-list-column:project-custom-field",
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda data: list(
                map(lambda x: x['cells'][0], data['rows']))
        )

        process_clients = rail.RepliconServiceCallForEachItemOperator(
            task_id='process_clients',
            endpoint="/services/ClientService1.svc/PutClient",
            items=lambda: list(set(list(map(lambda x: x['row']['data'].get(
                'CUST_NAME'), (rail.result('get_modified_projects') or
                               rail.result('get_modified_projects_in_chunks')))))-set(rail.result('get_all_clients_from_replicon'))),
            data={
                "client": {
                    "target": {
                        "uri": null,
                        "name": "{{item}}",
                        "code": null,
                        "parameterCorrelationId": null
                    },
                    "name": "{{item}}",
                    "code": null,
                    "comment": null,
                    "clientManager": null,
                    "billingContact": null,
                    "clientAddress": null,
                    "billingAddress": null,
                    "isActive": "true",
                    "customFieldValues": [],
                    "billingRates": [],
                    "expenseCodesAllowedByDefaultOnNewProjects": [],
                    "defaultBillingCurrency": null
                }
            }
        )

        pick_chose_wbs_sync = rail.IfOperator(
            task_id='pick_chose_wbs_sync',
            test=lambda: bool(getattr(config, 'allow_only_chargeable', False)) or bool(getattr(config, 'enable_wbs_boundary_sync', False)),
            yes_task='process_each_root_project_pick_chose',
            no_task='process_each_root_project'
        )

        process_each_root_project = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_root_project',
            retries=0,
            items=lambda: rail.result('group_data_by_root_project'),
            trigger_dag_id=f'deltek_costpoint_polaris_project_sync_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            reset_count=10000,
            conf=lambda item: {'item': {**item},
                               'billing_rates': rail.result('get_all_roles'),
                               'divisions': rail.result('get_replicon_divisions'),
                               'permission_sets': rail.result('get_all_permission_sets'),
                               'project_udfs': rail.result('get_project_udfs'),
                               'allow_only_chargeable' : True
                               }
        )

        process_each_root_project_pick_chose = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_root_project_pick_chose',
            retries=0,
            items=lambda: rail.result('group_data_by_root_project'),
            trigger_dag_id=f'deltek_costpoint_pick_chose_project_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            reset_count=10000,
            conf=lambda item: {'item': {**item},
                               'billing_rates': rail.result('get_all_roles'),
                               'divisions': rail.result('get_replicon_divisions'),
                               'permission_sets': rail.result('get_all_permission_sets'),
                               'project_udfs': rail.result('get_project_udfs')
                               }
        )

        wait_for_process_each_root_project = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_root_project',
            trigger_rule='none_failed_min_one_success',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("process_each_root_project") or result("process_each_root_project_pick_chose") }}'
        )

        gather_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_logs',
            trigger_rule='none_failed_min_one_success',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_each_root_project") or result("process_each_root_project_pick_chose") }}',
            dagrun_task_id='create_log',
            flatten=True
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=lambda: list(list(itertools.chain(
                *list(map(rail.load_all_records, rail.result('gather_child_logs'))))))
        )

        get_logged_errors = rail.PythonOperator(
            task_id='get_logged_errors',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('format_logs'), 'properties.status', 'Error')
        )

        has_error_logs = rail.IfOperator(
            task_id='has_error_logs',
            test=lambda: bool(rail.result('get_logged_errors')),
            yes_task='create_csv_lines',
            no_task='finish'
        )

        create_csv_lines = rail.WriteCSVFileOperator(
            task_id='create_csv_lines',
            source="{{ result('format_logs') | to_json }}",
            header=['Parent Job ID',
                    'Project ID',
                    'Project Name',
                    'Status',
                    'Details',
                    'Job ID'],
            row=[
                "{{ dag_run_ecid() }}",
                "{{ item.properties.proj_id }}",
                "{{ item.properties.proj_name }}",
                "{{ item.properties.status }}",
                "{{ item.properties.get('details','') }}",
                "{{ item.ecid }}",
            ]
        )

        log_filename = rail.PythonOperator(
            task_id='log_filename',
            python_callable=lambda:  rail.render_template(
                "Log_{{ dag_run_ecid() }}_project_sync.csv")
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('create_csv_lines')}}",
            output_file_name='{{ result("log_filename") }}',
            expires_in_seconds=7*24*60*60,
        )

        send_mail_error = rail.EmailOperator(
            task_id='send_mail_error',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Deltek Costpoint Project sync Completed with Errors - {{ current_time() }}''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Deltek Costpoint Project sync is completed with failures based on the file - '{{ result('log_filename') }}'. Please find the  link below to download the logs.
            <br /> <br /> <a href="{{ result('generate_download_link') }}">Download log file</a><br /> <br /><em><span style="font-size: 9pt;">The download link is valid for 7 days.</span></em></p>
            <br />
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Replicon Inc.</p> ''',
            params=None,
        )

        update_last_run_date = rail.PythonOperator(
            task_id='update_last_run_date',
            python_callable=lambda: Variable.set(config.last_run_date_var_name,
                                                 rail.result('get_last_run_date', 'current_time'))
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> get_last_run_date
        get_last_run_date >> can_load_data_in_chunks
        can_load_data_in_chunks >> rail.Label(
            'yes') >> get_modified_projects_in_chunks >> update_last_run_date >> has_project_data
        can_load_data_in_chunks >> rail.Label(
            'no') >> get_modified_projects >> update_last_run_date >> has_project_data
        has_project_data >> rail.Label('yes') >> group_data_by_root_project
        has_project_data >> rail.Label(
            'no') >> delete_this_dagrun >> finish
        group_data_by_root_project >> get_all_clients_from_replicon >> get_costpoint_plcs >> \
            get_all_roles >> get_replicon_divisions >> \
            get_project_udfs >> get_all_permission_sets >> process_clients >> pick_chose_wbs_sync
        pick_chose_wbs_sync >> rail.Label(
            'Yes') >> process_each_root_project_pick_chose
        pick_chose_wbs_sync >> rail.Label(
            'No') >> process_each_root_project
        [process_each_root_project, process_each_root_project_pick_chose] >> \
            wait_for_process_each_root_project >> gather_child_logs >> format_logs >> get_logged_errors >> has_error_logs
        has_error_logs >> rail.Label(
            'yes') >> create_csv_lines >> log_filename >> generate_download_link >> send_mail_error >> finish
        has_error_logs >> rail.Label('no') >> finish

        return dag


rail.for_each_instance(create_dag)
