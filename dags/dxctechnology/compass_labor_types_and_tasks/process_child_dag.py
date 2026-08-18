
from datetime import datetime, timedelta
import json
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_labor_types_and_tasks_process_child_{config.sub_erp_name}_{config.instance}',
        description=f'DXC_COMPASS_Labour Types and Task Automation Child {config.sub_erp_name}_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_dag_run_child_process,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        query_tasks = rail.QueryCollectionOperator(
            task_id='query_tasks',
            query='''SELECT * FROM task WHERE wbs=:wbs''',
            query_params={
                "wbs": "{{dag_run.conf.wbs}}"
            }
        )

        query_billing_rates = rail.QueryCollectionOperator(
            task_id='query_billing_rates',
            query='''SELECT * FROM labourtype WHERE wbs=:wbs''',
            query_params={
                "wbs": "{{dag_run.conf.wbs}}"
            }
        )

        load_billing_rates = rail.PythonOperator(
            task_id='load_billing_rates',
            python_callable=lambda:  rail.load_all_records(
                rail.result('query_billing_rates'))
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data={
                "projects": [
                    {
                        "uri": null,
                        "name": "{{dag_run.conf.wbs}}",
                        "code": null,
                        "parameterCorrelationId": null
                    }
                ]
            }
        )

        has_project_uri = rail.IfOperator(
            task_id='has_project_uri',
            test=lambda: bool(rail.result('get_project_details')[
                              0]['projectDetails']),
            yes_task="create_csv_lines_datesvalidationand_update",
            no_task="log_invalid_project",
        )

        log_invalid_project = rail.WriteLogOperator(
            task_id='log_invalid_project',
            log="{{ result('create_log') }}",
            message='WBS Element not present in Replicon',
            severity='Exception',
            properties={
                'wbs': '{{dag_run.conf.wbs}}',
                'task': '',
                'billingrate': '',
                'message': 'WBS Element not present in Replicon',
                'status': 'Exception',
            }
        )

        create_csv_lines_datesvalidationand_update = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_datesvalidationand_update',
            source="{{ result('query_billing_rates') }}",
            header=[
                    'name',
                    'personnelnumber',
                    'taskassignmentstartdate',
                    'taskassignmentenddate',
                    'billabledefault',
                    'blanklabortype'
            ],
            row=lambda item: {
                "column_0": item['name'],
                "column_1": item['personnelnumber'],
                "column_2": item['taskassignmentstartdate'],
                "column_3": item['taskassignmentenddate'],
                "column_4": item['billabledefault'],
                "column_5": bool(item['name'])
            }.values()
        )

        load_csv_datesvalidationand_update = rail.LoadCSVFileOperator(
            task_id="load_csv_datesvalidationand_update",
            document="{{result('create_csv_lines_datesvalidationand_update')}}",
        )

        create_collection_datesvalidationand_update = rail.CreateCollectionOperator(
            task_id='create_collection_datesvalidationand_update',
            source="{{ result('load_csv_datesvalidationand_update') }}",
            name="billingratestoassign",
            columns={
                'name': 'name',
                'personnelnumber': 'personnelnumber',
                'taskassignmentstartdate': 'taskassignmentstartdate',
                'taskassignmentenddate': 'taskassignmentenddate',
                'billabledefault': 'billabledefault',
                'blanklabortype': 'blanklabortype'
            }
        )

        bulk_getproject = rail.RepliconServiceOperator(
            task_id='bulk_getproject',
            endpoint="/services/ImportService1.svc/BulkGetProjects",
            data={
                "projects": [
                    {
                        "uri": null,
                        "name": "{{dag_run.conf.wbs}}",
                        "code": null,
                        "parameterCorrelationId": null
                    }
                ]
            }
        )

        log_message_key_name_space_labour_type = rail.PythonOperator(
            task_id='log_message_key_name_space_labour_type',
            python_callable=lambda:  'DXC_CompassWBSLabourTypeDetails'
        )

        get_key_value_labour_type = rail.RepliconServiceOperator(
            task_id='get_key_value_labour_type',
            endpoint="/services/GenericKeyValueStoreService1.svc/GetKeyValue",
            data={
                "keyNamespace": "{{ result('log_message_key_name_space_labour_type') }}",
                "key": " {{dag_run.conf.wbs}}"
            }
        )

        get_billing_rate_query_param = rail.PythonOperator(
            task_id='get_billing_rate_query_param',
            python_callable=lambda: f'''{ ",".join(list(map(lambda x:f'"{get_labour_type_name(x)}"',rail.result('load_billing_rates')))) }''',
        )

        billing_rate_query = rail.QueryCollectionOperator(
            task_id='billing_rate_query',
            query='''SELECT * FROM billingratesinreplicon WHERE name IN ({{ result('get_billing_rate_query_param') }})''',
        )

        def get_labour_type_name(item):
            return item['name'] if item['name'] else ''

        def get_billing_rates_to_assign():
            billing_rates = rail.result('load_billing_rates')
            billing_rates_to_assign = list(map(lambda item: {
                "displaytext": item['displayText'],
                "name": item['name'],
                "uri": item['uri'],
                "availableinproject": "No" if len(list(filter(lambda x: x['billingRate']['displayText'] == item['displayText'],
                                                              rail.result('bulk_getproject')[0]['timeAndMaterials']['projectBillingRates'])
                                                       )) == 0 else "Yes",
                "requiredtoassign": "No" if len(list(filter(lambda x: get_labour_type_name(x) == get_labour_type_name(item),
                                                            billing_rates))) == 0 else "Yes",
            },  rail.load_all_records(rail.result('billing_rate_query'))))
            return list(filter(
                lambda item: item['requiredtoassign'] == "Yes", billing_rates_to_assign))

        billing_rates_to_assign = rail.PythonOperator(
            task_id='billing_rates_to_assign',
            python_callable=get_billing_rates_to_assign
        )

        has_no_key_value_labour_type = rail.IfOperator(
            task_id='has_no_key_value_labour_type',
            test=lambda: not bool(rail.result('get_key_value_labour_type')),
            yes_task="add_key_value_labour_type_details",
            no_task="can_update_key_value_labour_type_details",
        )

        def map_date(date_str):
            if not date_str:
                return ''
            return datetime.strptime(date_str, '%Y%m%d').strftime("%m/%d/%Y")

        def map_replicon_date_to_str(date):
            if not date:
                return ''
            return datetime(day=date['day'], month=date['month'], year=date['year']).strftime("%m/%d/%Y")

        def get_billing_rate_jsonvalue():
            billing_rates = rail.result('load_billing_rates')
            return json.dumps(list(map(lambda item: {
                "wbsUri": rail.result('bulk_getproject')[0]['project']['uri'],
                "wbsName": rail.get_current_context()['dag_run'].conf['wbs'],
                "labourType": item['displaytext'],
                "labourTypeUri": item['uri'],
                "startDate": map_date(list(filter(lambda x: get_labour_type_name(x) == get_labour_type_name(item),
                                                  billing_rates))[0]['taskassignmentstartdate']) or
                map_replicon_date_to_str(
                    rail.result('get_project_details')[0]['projectDetails']['timeEntryDateRange']['startDate']),
                "endDate": map_date(list(filter(lambda x:  get_labour_type_name(x) == get_labour_type_name(item),
                                                billing_rates))[0]['taskassignmentenddate']) or
                map_replicon_date_to_str(rail.result('get_project_details')[
                                         0]['projectDetails']['timeEntryDateRange']['endDate']),
            }, rail.result('billing_rates_to_assign'))))

        add_key_value_labour_type_details = rail.RepliconServiceOperator(
            task_id='add_key_value_labour_type_details',
            endpoint="/services/GenericKeyValueStoreService1.svc/PutKeyValue",
            data=lambda: {
                "keyNamespace": rail.result('log_message_key_name_space_labour_type'),
                "keyValue": {
                    "key":  rail.get_current_context()['dag_run'].conf['wbs'],
                    "jsonValue": get_billing_rate_jsonvalue()
                },
            }
        )

        can_update_key_value_labour_type_details = rail.IfOperator(
            task_id='can_update_key_value_labour_type_details',
            test=lambda: len(get_billingrates_to_update()[1]) > 0,
            yes_task="update_key_value_labour_type_details",
            no_task="get_all_project_tasks",
        )

        def get_updated_jsonvalue():
            json_value, new_billing_rates_to_update = get_billingrates_to_update()
            if new_billing_rates_to_update:
                json_value.extend(new_billing_rates_to_update)
            return json.dumps(json_value)

        def get_billingrates_to_update():
            billing_rates = rail.result('load_billing_rates')
            json_value = json.loads(rail.result(
                'get_key_value_labour_type')['jsonValue'])
            new_billing_rates_to_update = list(filter(lambda new_item: len(list(
                filter(lambda old_item:
                       new_item['wbsName']+new_item['labourType']+new_item['startDate']+new_item['endDate'] ==
                       old_item['wbsName']+old_item['labourType'] +
                       old_item['startDate'] +
                       old_item['endDate'],
                       json_value)
            )) == 0,
                list(map(lambda item: {
                    "wbsUri": rail.result('bulk_getproject')[0]['project']['uri'],
                    "wbsName": rail.get_current_context()['dag_run'].conf['wbs'],
                    "labourType": item['displaytext'],
                    "labourTypeUri": item['uri'],
                    "startDate": map_date(list(filter(lambda x: get_labour_type_name(x) == get_labour_type_name(item),
                                                      billing_rates))[0]['taskassignmentstartdate']) or
                    map_replicon_date_to_str(
                        rail.result('get_project_details')[0]['projectDetails']['timeEntryDateRange']['startDate']),
                    "endDate": map_date(list(filter(lambda x: get_labour_type_name(x) == get_labour_type_name(item),
                                                    billing_rates))[0]['taskassignmentenddate']) or
                    map_replicon_date_to_str(rail.result('get_project_details')[
                        0]['projectDetails']['timeEntryDateRange']['endDate']),
                }, rail.result('billing_rates_to_assign')))))
            return json_value, new_billing_rates_to_update

        update_key_value_labour_type_details = rail.RepliconServiceOperator(
            task_id='update_key_value_labour_type_details',
            endpoint="/services/GenericKeyValueStoreService1.svc/PutKeyValue",
            data=lambda: {
                "keyNamespace": rail.result('log_message_key_name_space_labour_type'),
                "keyValue": {
                    "key":  rail.get_current_context()['dag_run'].conf['wbs'],
                    "jsonValue": get_updated_jsonvalue()
                },
            }
        )

        get_all_project_tasks = rail.RepliconServiceOperator(
            task_id='get_all_project_tasks',
            endpoint="/services/TaskService1.svc/GetDescendantTaskDetails",
            data=lambda: {
                "parentUri": rail.result('bulk_getproject')[0]['project']['uri'],
            },
        )

        has_project_task = rail.IfOperator(
            task_id='has_project_task',
            test=lambda: bool(rail.result('get_all_project_tasks')),
            yes_task='create_project_task_collection',
            no_task='process_resources'
        )

        create_project_task_collection = rail.CreateCollectionOperator(
            task_id='create_project_task_collection',
            name='project_task',
            source=lambda:  list(map(lambda item: {
                "taskname": item['task']['name'],
                "taskuri": item['task']['uri'],
                "tasktype": rail.find_first_by_attr_and_get_attr(item['task']['customFields'], 'customField.name', 'Task Type', 'text')
            }, rail.result('get_all_project_tasks')))
        )

        process_resources = rail.TriggerDagRunForEachItemOperator(
            task_id='process_resources',
            retries=0,
            items="{{ result('query_billing_rates') }}",
            trigger_dag_id=f'dxctechnology_compass_labor_types_and_tasks_process_resource_child_{config.sub_erp_name}_{config.instance}',
            conf=lambda item: {
                'personnelnumber': item['personnelnumber'],
                'project_info': rail.result('bulk_getproject')[0],
                'log': rail.result('create_log'),
                'wbs': rail.get_current_context()['dag_run'].conf['wbs'],
                'project_tasks': rail.result('get_all_project_tasks')
            },

            execution_timeout=timedelta(days=14),
        )

        wait_for_process_resources = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_resources',
            dag_runs='{{ result("process_resources") }}',
            execution_timeout=timedelta(days=14),
        )

        process_tasks = rail.TriggerDagRunForEachItemOperator(
            task_id='process_tasks',
            retries=0,
            items="{{ result('query_tasks') }}",
            trigger_dag_id=f'dxctechnology_compass_labor_types_and_tasks_process_task_child_{config.sub_erp_name}_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                **item,
                'name': item['task'],
                'project_info': rail.result('bulk_getproject')[0],
                'log': rail.result('create_log'),
                'wbs': rail.get_current_context()['dag_run'].conf['wbs'],
                'billingrates': rail.result('load_billing_rates'),
                'project_tasks': rail.result('get_all_project_tasks'),
            },

        )

        wait_for_process_tasks = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_tasks',
            dag_runs='{{ result("process_tasks") }}',
            execution_timeout=timedelta(days=14),
        )

        process_billing_rate = rail.TriggerDagRunForEachItemOperator(
            task_id='process_billing_rate',
            retries=0,
            items="{{ result('query_billing_rates') }}",
            trigger_dag_id=f'dxctechnology_compass_labor_types_and_tasks_process_billing_rate_child_{config.sub_erp_name}_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                **item,
                'name': item['labourtypes'],
                'project_info': rail.result('bulk_getproject')[0],
                'log': rail.result('create_log'),
                'wbs': rail.get_current_context()['dag_run'].conf['wbs'],
                'billingrates': rail.result('load_billing_rates'),
                'project_tasks': rail.result('get_all_project_tasks'),
            },

        )

        wait_for_process_billing_rate = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_billing_rate',
            dag_runs='{{ result("process_billing_rate") }}',
            execution_timeout=timedelta(days=14),
        )

        get_duplicate_billingrate_records = rail.PythonOperator(
            task_id='get_duplicate_billingrate_records',
            python_callable=lambda: list(filter(
                lambda item: item['requiredtoassign'] == "Yes" and item['availableinproject'] == "Yes", get_billing_rates_to_assign()))
        )

        add_duplicate_log_entry = rail.WriteLogOperator(
            task_id='add_duplicate_log_entry',
            log="{{ result('create_log') }}",
            items="{{ result('get_duplicate_billingrate_records') | to_json }}",
            message='Billing Rate Already available in WBS',
            severity='Info',
            properties={
                'wbs': '{{dag_run.conf.wbs}}',
                'task': '',
                'billingrate': '{{item.name}}',
                'message': 'Billing Rate Already available in WBS',
                'status': 'Skipped',
            }

        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message()}}',
            properties={
                'wbs': '{{dag_run.conf.wbs}}',
                'task': '',
                'billingrate': '',
                'message': '{{ get_error_message()}}',
                'status': 'Error',
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        create_log >> query_tasks >> query_billing_rates >> load_billing_rates >> \
            get_project_details >> has_project_uri
        has_project_uri >> rail.Label(
            'Yes') >> create_csv_lines_datesvalidationand_update
        has_project_uri >> rail.Label(
            'No') >> log_invalid_project >> catch_and_log_errors
        create_csv_lines_datesvalidationand_update >> load_csv_datesvalidationand_update >> \
            create_collection_datesvalidationand_update >> bulk_getproject >> log_message_key_name_space_labour_type >> get_key_value_labour_type >> \
            get_billing_rate_query_param >> billing_rate_query >> billing_rates_to_assign >> has_no_key_value_labour_type
        has_no_key_value_labour_type >> rail.Label(
            'Yes') >> add_key_value_labour_type_details >> get_all_project_tasks
        has_no_key_value_labour_type >> rail.Label(
            'No') >> can_update_key_value_labour_type_details
        can_update_key_value_labour_type_details >> rail.Label(
            'yes') >> update_key_value_labour_type_details >> get_all_project_tasks
        can_update_key_value_labour_type_details >> rail.Label(
            'no') >> get_all_project_tasks
        get_all_project_tasks >> has_project_task
        has_project_task >> rail.Label(
            'yes') >> create_project_task_collection >> process_resources
        has_project_task >> rail.Label('no') >> process_resources
        process_resources >> wait_for_process_resources >> process_tasks >> \
            wait_for_process_tasks >> process_billing_rate >> wait_for_process_billing_rate >> get_duplicate_billingrate_records >> \
            add_duplicate_log_entry >> catch_and_log_errors
        catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
