from datetime import datetime, timedelta
import itertools
from pytz import timezone
from airflow.models import Variable
import rail

# pylint:disable = too-many-statements, line-too-long
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.main_dag_id,
        description=f'deltek_costpoint_polaris_project_sync_main{config.instance}',
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        default_args={
            'deltek_costpoint_conn_id': config.deltek_cospoint_conn_id,
        }
    ) as dag:


        def do_get_last_run_date():
            current_time = datetime.now(timezone('UTC')) - timedelta(seconds=2)
            lookup_timestamp_value = Variable.get(
                config.last_run_date_var_name, default_var=None)
            last_run_date = (datetime.fromisoformat(
                lookup_timestamp_value) if lookup_timestamp_value else current_time).isoformat()
            Variable.set(config.last_run_date_var_name,
                         current_time.isoformat())
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

        get_modified_workforce_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='get_modified_workforce_costpoint',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_cospoint_company_ids,
            data=lambda: {
                "filter": {
                    "id": "POLARIS_EXP_PJM_WORK",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "PJM_PROJEMPL_HDR",
                                "conditions": [
                                    {
                                        "joinWithParent": "N",
                                        "relations": [
                                            {
                                                "name": "PJM_PROJEMPL_HDR_LAST_MODIFIED",
                                                "relation": "gt=",
                                                "value": get_time()
                                            }
                                        ]
                                    }
                                ]
                            }
                        }
                    ]
                }
            },
            data_handler=lambda data: data['document']['rows'],
        )

        get_modified_workforce_plc_costpoint = rail.DeltekCostPointServiceOperator(
            task_id='get_modified_workforce_plc_costpoint',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_cospoint_company_ids,
            data=lambda: {
                "filter": {
                    "id": "polaris_exp_project_wfc_plc",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "PJM_PROJEMPL_HDR",
                                "conditions": [],
                                "children": [
                                    {
                                        "rsWhere": {
                                            "rsId": "PJM_PROJEMPL_LABCAT_PLCWKFRCE",
                                            "conditions": [],
                                            "children": [
                                                {
                                                    "rsWhere": {
                                                        "rsId": "PJM_PROJEMPLLABCAT_PLCWK",
                                                        "conditions": [
                                                            {
                                                                "joinWithParent": "N",
                                                                "relations": [
                                                                    {
                                                                        "name": "PJM_PROJEMPLLABCAT_PLCWK_LAST_MODIFIED",
                                                                        "relation": "gt=",
                                                                        "value": get_time()
                                                                    }
                                                                ]
                                                            }
                                                        ],
                                                        "children": []
                                                    }
                                                }
                                            ]
                                        }
                                    }
                                ]
                            }
                        }
                    ]
                }
            },
            data_handler=lambda data: data['document']['rows'],
        )

        def get_workforce_proj_ids():
            if not getattr(config, 'workforce_change_detection_enabled', False):
                return []
            workforce = rail.result('get_modified_workforce_costpoint') or []
            workforce_plc = rail.result('get_modified_workforce_plc_costpoint') or []
            return list(set(
                [row['row']['data']['PROJ_ID'] for row in workforce if row.get('row', {}).get('data', {}).get('PROJ_ID')]
                + [row['row']['data']['PROJ_ID'] for row in workforce_plc
                if row.get('row', {}).get('data', {}).get('PROJ_ID')
                and row.get('row', {}).get('children')
                and row['row']['children'][0].get('row', {}).get('children')]
            ))

        def get_workforce_proj_id_conditions():
            return [
                {
                    "joinWithParent": "N",
                    "relations": [
                        {
                            "name": "PROJ_ID",
                            "relation": "=",
                            "value": proj_id
                        }
                    ]
                }
                for proj_id in get_workforce_proj_ids()
            ]

        def get_filters():
            filters = [
                
                {
                    "name": "PJMBASIC_PROJ_LAST_MODIFIED",
                    "relation": "gt=",
                    "value": get_time()
                }
            ]
            exclude_prefix = getattr(config, 'exclude_project_type_prefix', None)
            if exclude_prefix:
                # CP `like%` is case-sensitive; cover common case variants.
                variants = {exclude_prefix.upper(),
                            exclude_prefix.lower(),
                            exclude_prefix.capitalize()}
                for variant in variants:
                    filters.append({
                        "name": "PROJ_TYPE_DC",
                        "relation": "not like%",
                        "value": variant
                    })
                    filters.append({
                        "name": "PROJ_NAME",
                        "relation": "not like%",
                        "value": variant
                    })
            return filters

        check_project_workforce = rail.IfOperator(
            task_id='check_project_workforce',
            test=lambda: bool(getattr(config, 'workforce_change_detection_enabled', False)),
            yes_task='get_modified_workforce_costpoint',
            no_task='can_load_data_in_chunks'
        )
        can_load_data_in_chunks = rail.IfOperator(
            task_id='can_load_data_in_chunks',
            test=lambda: Variable.get(
                    config.get_data_in_chunk_var_name, default_var='false').lower() == 'true',
            yes_task='get_modified_projects_in_chunks',
            no_task='get_modified_projects'
        )

        def get_project_filter_items():
            items = []
            last_item = []
            a_to_z_chars = list(map(chr, range(ord('A'), ord('Z')+1)))
            for item in a_to_z_chars:
                items.append([
                    {
                        "name": "PROJ_NAME",
                        "relation": "like%",
                        "value": item
                    }
                ] + get_filters())
                last_item.append({
                    "name": "PROJ_NAME",
                    "relation": "not like%",
                    "value": item
                })
            last_item = last_item + get_filters()
            items.append(last_item)

            if getattr(config, 'workforce_change_detection_enabled', False):
                for proj_id in get_workforce_proj_ids():
                    items.append([
                        {
                            "name": "PROJ_ID",
                            "relation": "=",
                            "value": proj_id
                        }
                    ])

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
            flatten=True,
            extra_options={"verify": False}
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
            extra_options={"verify": False}
        )

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
            python_callable=lambda: [
                {'root_project_id': k, 'data': list(g)}
                for k, g in itertools.groupby(
                    sorted(
                        (rail.result('get_modified_projects') or rail.result('get_modified_projects_in_chunks') or []),
                        key=lambda x: x['row']['data']['PROJ_ID']
                    ),
                    lambda x: x['row']['data']['PROJ_ID'].split(".")[0]
                )
            ]
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
            extra_options={"verify": False}
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

        get_task_udfs = rail.RepliconServiceOperator(
            task_id='get_task_udfs',
            endpoint="/services/TaskCustomFieldListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "999999",
                "columnUris": [
                    "urn:replicon:task-custom-field-list-column:field-name",
                    "urn:replicon:task-custom-field-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda data: list(
                map(lambda x: x['cells'][0],
                    filter(lambda x: x['cells'][1].get('boolValue') == True, data['rows'])))
        )

        get_task_type_dropdown_options = rail.RepliconServiceOperator(
            task_id='get_task_type_dropdown_options',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_task_udfs') or [],
                    'textValue',
                    getattr(config, 'task_type_custom_field', None),
                    'uri'
                )
            },
            data_handler=lambda response: [item.get('displayText') for item in (response or [])]
        )

        def _normalize_client_name(name):
            # CP often pads CHAR(25) fields with trailing whitespace and may
            # return None for rows that have no CUST_NAME. Normalize so
            # set-diff against Replicon's client list doesn't create duplicates
            # (e.g. "Amtrak National Railroad " vs "Amtrak National Railroad")
            # or a literal "None" client (from BIDPRP.* rows).
            if name is None:
                return ''
            return str(name).strip()

        process_clients = rail.RepliconServiceCallForEachItemOperator(
            task_id='process_clients',
            endpoint="/services/ClientService1.svc/PutClient",
            items=lambda: sorted(
                {
                    _normalize_client_name(x['row']['data'].get('CUST_NAME'))
                    for x in (rail.result('get_modified_projects')
                              or rail.result('get_modified_projects_in_chunks')
                              or [])
                }
                - {_normalize_client_name(n) for n in (rail.result('get_all_clients_from_replicon') or [])}
                - {''}
            ),
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

        get_service_center = rail.RepliconServiceOperator(
            task_id='get_service_center',
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data={
                "page": 1,
                "pagesize": 200,
                "columnUris": [
                    "urn:replicon:service-center-list-column:service-center",
                    "urn:replicon:service-center-list-column:effectively-enabled",
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda data: next(
                (row['cells'][0].get('uri')
                 for row in (data or {}).get('rows', [])
                 if row['cells'][0].get('textValue') == config.proj_service_center_name
                 and row['cells'][1].get('textValue') in ('True', True)),
                null
            )
        )

        _shared_conf_cache = {}

        def build_child_conf(item, **_context):
            if not _shared_conf_cache:
                _shared_conf_cache.update({
                    'billing_rates': rail.result('get_all_roles'),
                    'divisions': rail.result('get_replicon_divisions'),
                    'permission_sets': rail.result('get_all_permission_sets'),
                    'project_udfs': rail.result('get_project_udfs'),
                    'task_udfs': rail.result('get_task_udfs'),
                    'task_type_dropdown_options': rail.result('get_task_type_dropdown_options'),
                    'service_center_uri': rail.result('get_service_center'),
                })
            return {'item': {**item}, **_shared_conf_cache, 'allow_only_chargeable': True}

        process_each_root_project = rail.trigger_parallel_dagrun(
            task_id='process_each_root_project',
            items=lambda: rail.result('group_data_by_root_project'),
            trigger_dag_id=config.child_dag_id,
            parallel_count=config.parallel_count,
            execution_timeout=timedelta(days=14),
            conf=build_child_conf
        )


        def gather_parallel_dagrun_ids():
            run_ids = []
            for i in range(1, config.parallel_count + 1):
                branch_result = rail.result(f'process_each_root_project_{i}')
                if branch_result:
                    run_ids.extend(branch_result)
            return run_ids

        gather_run_ids = rail.PythonOperator(
            task_id='gather_run_ids',
            python_callable=gather_parallel_dagrun_ids
        )

        gather_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("gather_run_ids") }}',
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
            no_task='update_last_run_date'
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

        get_last_run_date >> check_project_workforce
        check_project_workforce >> rail.Label('Yes') >> get_modified_workforce_costpoint >> get_modified_workforce_plc_costpoint >> can_load_data_in_chunks
        check_project_workforce >> rail.Label('No') >> can_load_data_in_chunks

        can_load_data_in_chunks >> rail.Label(
            'yes') >> get_modified_projects_in_chunks >> has_project_data
        can_load_data_in_chunks >> rail.Label(
            'no') >> get_modified_projects >> has_project_data
        has_project_data >> rail.Label('yes') >> group_data_by_root_project
        has_project_data >> rail.Label(
            'no') >> delete_this_dagrun >> update_last_run_date >> finish
        group_data_by_root_project >> get_all_clients_from_replicon >> get_costpoint_plcs >> get_all_roles >> \
            get_replicon_divisions >> \
            get_project_udfs >> get_task_udfs >> get_task_type_dropdown_options >> get_all_permission_sets >> process_clients >> get_service_center >> process_each_root_project >> \
            gather_run_ids >> gather_child_logs >> format_logs >> get_logged_errors >> has_error_logs
        has_error_logs >> rail.Label(
            'yes') >> create_csv_lines >> log_filename >> generate_download_link >> send_mail_error >> update_last_run_date >> finish
        has_error_logs >> rail.Label('no') >> update_last_run_date >> finish

        return dag


rail.for_each_instance(create_dag)
