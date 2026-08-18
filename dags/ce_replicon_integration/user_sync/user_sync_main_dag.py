from datetime import timedelta
from ce_replicon_integration.user_sync.utils.python_callable_method import extract_user_details, get_dropdown_value_by_union_class
from rail.lib.last_sync_time_store import get_lastsync_time_variable, set_lastsync_time_variable
import rail
null = None
# pylint:disable = too-many-statements, line-too-long


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.user_sync_main_dag_id,
        description=f'{config.company_key } Sync users from Deltek Computerease to Replicon',
        company_key=config.company_key,
        max_active_runs=config.max_active_runs,
        multi_tenant=True
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_last_sync_time',
            end_task='should_log_history',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_employee_last_sync_time():
            company_key = rail.get_current_context()['dag_run'].conf['company_key']
            return get_lastsync_time_variable(
                variable_name=f'{config.employee_last_sync_time_var}_{company_key}',
                date_format=config.ce_time_format,
                initial_sync_time='1970-01-01T00:00:00Z',
                reset_after_threshold=False
            )

        get_last_sync_time = rail.PythonOperator(
            task_id='get_last_sync_time',
            python_callable=get_employee_last_sync_time
        )

        def filter_ce_users_by_timestamp(response_data, last_sync_time):
            filtered_ce_users = []
            for employee in response_data:
                employee_updated_at = employee.get('updated_at', '')
                if employee_updated_at > last_sync_time:
                    filtered_ce_users.append(employee)

            return filtered_ce_users

        def filter_with_sync_time(employees_data):
            last_sync_time = rail.result('get_last_sync_time')['last_synctime']
            return filter_ce_users_by_timestamp(employees_data, last_sync_time)

        get_all_users_from_ce = rail.ComputereaseAPIOperator(
            task_id='get_all_users_from_ce',
            endpoint='/catalog/employee',
            request_method='GET',
            computerease_conn_id='{{ dag_run.conf.computerease_conn_id }}',
            data_handler=lambda employees: filter_with_sync_time(
                employees['data'])
        )

        def set_employee_last_sync_time():
            company_key = rail.get_current_context()['dag_run'].conf['company_key']
            return set_lastsync_time_variable(
                variable_name=f'{config.employee_last_sync_time_var}_{company_key}',
                value_to_set=rail.result('get_last_sync_time')['current_time']
            )

        set_last_sync_time = rail.PythonOperator(
            task_id='set_last_sync_time',
            python_callable=set_employee_last_sync_time
        )

        has_ce_users_to_sync = rail.IfOperator(
            task_id='has_ce_users_to_sync',
            test='{{ result("get_all_users_from_ce") | length > 0 }}',
            yes_task='search_user_in_replicon',
            no_task='should_log_history'
        )

        def store_ce_users_in_array():
            ce_response = rail.result('get_all_users_from_ce')

            required_fields = [
                field.strip() for field in config.employee_required_fields.split(',')]

            users_array = []
            for user in ce_response:
                user_data = {field: user.get(field, '')
                             for field in required_fields}
                user_data["loginName"] = user.get("code", "")
                users_array.append(user_data)

            return {'users': users_array}

        search_user_in_replicon = rail.RepliconServiceOperator(
            task_id='search_user_in_replicon',
            endpoint='/services/UserService1.svc/BulkGetUsers2',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda: store_ce_users_in_array(),
            data_handler=lambda response: [user['uri']
                                           for user in response if user]
        )

        get_existing_user_details = rail.RepliconServiceOperator(
            task_id='get_existing_user_details',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda: {
                'users': [
                    {"uri": uri} for uri in (rail.result('search_user_in_replicon') or []) if uri
                ]
            },
            data_handler=lambda response: extract_user_details(response)
        )

        create_group_options_list = rail.SetVariableOperator(
            task_id='create_group_options_list',
            name='group_options_list',
            append=False,
            value=[]
        )

        for_each_group = rail.ForEachOperator(
            task_id='for_each_group',
            items=config.groups,
            start_task='get_all_group_options',
            end_task='for_each_group_end',
        )

        def get_getdata_payload():
            group_var = rail.result('for_each_group')['getdataservicevariable']
            return {
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    f"urn:replicon:{group_var}-list-column:name",
                    f"urn:replicon:{group_var}-list-column:code",
                    f"urn:replicon:{group_var}-list-column:{group_var}"
                ],
                "sort": [],
                "filterExpression": null
            }

        get_all_group_options = rail.RepliconServiceOperator(
            task_id='get_all_group_options',
            endpoint="{{result('for_each_group').optionsendpoint}}",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=get_getdata_payload,
            data_handler=lambda response: list(map(lambda dept: {
                'name': dept['cells'][0]['textValue'] if 'textValue' in dept['cells'][0] else '',
                'code': dept['cells'][1]['textValue'] if 'textValue' in dept['cells'][1] else '',
                'uri': dept['cells'][2]['uri'] if 'uri' in dept['cells'][2] else ''
            }, response['rows']))
        )

        add_group_options_to_list = rail.SetVariableOperator(
            task_id='add_group_options_to_list',
            name='group_options_list',
            append=True,
            value=lambda: {
                rail.result('for_each_group')['type']: rail.result(
                    'get_all_group_options')
            }
        )

        for_each_group_end = rail.EmptyOperator(
            task_id='for_each_group_end'
        )

        def get_group_value_by_code(user, group_options, exception_messages):
            union = user.get('union', '')
            local = user.get('local', '')

            if not isinstance(union, str) or not union.strip():
                return None

            ce_local = local.strip().upper()
            if not ce_local:
                return None

            for group_item in group_options:
                group_code = group_item.get('code', '').strip().upper()

                if ce_local == group_code:
                    return group_item.get('uri')

            return None

        def get_oef_definition_uris(oefs_response):
            oef_definitions = []
            missing_oefs = []
            for oef in config.oefs:
                definition_uri = rail.find_first_by_attr_and_get_attr(oefs_response, 'name', oef['name'], 'uri')
                if not definition_uri:
                    missing_oefs.append(oef['name'])
                oef_definitions.append({
                    'id': oef['id'],
                    'definitionuri': definition_uri,
                    'type': oef['type']
                })

            if missing_oefs:
                raise ValueError(f"OEF fields not found in Replicon: {', '.join(missing_oefs)}. Please verify OEF names.")

            return oef_definitions

        get_user_oefs = rail.RepliconServiceOperator(
            task_id="get_user_oefs",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data={
                "bindingContextUri": "urn:replicon:object-type:user"
            },
            # pylint: disable=unnecessary-lambda
            data_handler=lambda oefs: get_oef_definition_uris(oefs)
        )

        def format_tags(response):
            tag_options = list(map(lambda row: {
                "name": row['cells'][0]['textValue'],
                "uri": row['cells'][1]['uri'],
                "is_enabled": row['cells'][2]['textValue']
            }, response['rows']))
            return tag_options if tag_options else None

        def get_tags_object(response, item):
            return {
                **item,
                'tags': format_tags(response)
            }

        get_tags_for_each_dropdown_oef = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_tags_for_each_dropdown_oef',
            endpoint='services/ObjectExtensionTagListService1.svc/GetData',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            items=lambda: list(filter(lambda oef: (
                oef['type'] == 'dropdown' and oef['definitionuri']), rail.result('get_user_oefs'))),
            data=lambda item: {
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:object-extension-tag-list-column:name",
                    "urn:replicon:object-extension-tag-list-column:object-extension-tag",
                    "urn:replicon:object-extension-tag-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:definition"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "value": {
                            "uri": item['definitionuri']
                        }
                    }
                }
            },
            # pylint: disable=unnecessary-lambda
            data_handler=lambda response, item: get_tags_object(response, item)
        )

        def get_oef_definition_and_value_with_exceptions(user, exception_messages):
            oefs = []

            for oef in config.oefs:
                oef_definition = rail.find_first_by_attr_and_get_attr(
                    rail.result('get_user_oefs'), 'id', oef['id'], 'definitionuri')
                if oef_definition:
                    if oef['type'] == 'dropdown':
                        dropdown_tags_data = rail.result(
                            'get_tags_for_each_dropdown_oef') or []
                        value = get_dropdown_value_by_union_class(
                            oef['id'], user, dropdown_tags_data, exception_messages)
                    else:
                        value = user[oef['input']]
                    oefs.append({
                        'id': oef['id'],
                        'def': oef_definition,
                        'value': value,
                        'type': oef['type'],
                        'input': user[oef['input']],
                        'name': oef['name']
                    })
                else:
                    exception_messages.append(
                        f"{oef['name']} not assigned since the field is not present in Replicon. ")
            return oefs

        get_all_permissionsets = rail.RepliconServiceOperator(
            task_id='get_all_permissionsets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        def get_current_details(user):
            existing_users = rail.result('get_existing_user_details') or {}
            return existing_users.get(user['loginName'], None)

        def build_permissions_list(permissions_config):
            return list(map(lambda permission_name: {
                "permissionSetPolicy": {
                    "name": permission_name
                }
            }, permissions_config))

        def build_user_basic_info(user, exception_messages):
            return {
                "loginname": user['code'],
                "loginenabled": user['active'],
                "startdate": null,
                "firstname": user['first_name'],
                "lastname": user['last_name'],
                "displayname": f"{user['first_name']} {user['last_name']}",
                "email": None
            }

        def build_user_configuration_data(user_configuration):
            return {
                **user_configuration,
                "permissions": build_permissions_list(user_configuration['permissions'])
            }

        def build_single_user_data(user, user_configuration, group_options):
            current_details = get_current_details(user)
            exception_messages = []

            group_assignments = {}
            for group_config in config.groups:
                group_type = group_config['type']
                group_value = get_group_value_by_code(user, group_options, exception_messages)
                if group_value:
                    group_assignments[group_type] = group_value

            oefs = get_oef_definition_and_value_with_exceptions(user, exception_messages)

            user_data = {
                **build_user_basic_info(user, exception_messages),
                "currentdetails": current_details,
                **group_assignments,
                **build_user_configuration_data(user_configuration),
                "oefs": oefs,
                "exception_messages": exception_messages if exception_messages else None
            }

            return user_data

        def parse_users():
            users = store_ce_users_in_array()['users']
            user_configuration = (config.user_configuration)[0]
            group_options_raw = rail.get_dag_run_var('group_options_list')
            existing_users = rail.result('get_existing_user_details') or {}

            group_options = []
            for group_type_obj in group_options_raw:

                for group_config in config.groups:
                    group_type = group_config['type']
                    if group_type in group_type_obj:
                        group_options.extend(group_type_obj[group_type])

            all_users = []
            for user in users:
                is_active_in_ce = user.get('active')
                exists_in_replicon = user['loginName'] in existing_users

                if not is_active_in_ce and not exists_in_replicon:
                    continue

                if not is_active_in_ce and exists_in_replicon and not existing_users[user['loginName']].get('active'):
                    continue

                user_data = build_single_user_data(
                    user, user_configuration, group_options)
                all_users.append(user_data)

            return all_users

        process_each_user = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_user',
            retries=0,
            items=parse_users,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_each_user_child_dag_id,
            conf=lambda dag_run, item: {
                **item,
                'computerease_conn_id': dag_run.conf['computerease_conn_id'],
                'replicon_conn_id': dag_run.conf['replicon_conn_id']
            }
        )

        wait_for_completion_trigger_process_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_process_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_each_user") }}'
        )

        gather_user_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_errors',
            dag_runs="{{ result('process_each_user') }}",
            dagrun_task_id='catch_user_error',
            flatten=True
        )

        is_user_error = rail.IfOperator(
            task_id='is_user_error',
            test="{{ (get_task_state('gather_user_errors') == 'success' and result('gather_user_errors') | length > 0) }}",
            yes_task='fail_user_error',
            no_task='should_log_history'
        )

        fail_user_error = rail.FailOperator(
            task_id='fail_user_error',
            message="{{ result('gather_user_errors') | map_to_attr('error') | join('|') }}"
        )

        should_log_history = rail.IfOperator(
            task_id='should_log_history',
            test="{{ not(get_task_state('has_ce_users_to_sync') == 'success' and \
                    result('has_ce_users_to_sync') != 'search_user_in_replicon') }}",
            trigger_rule='all_done',
            yes_task='log_dagrun_details_to_table',
            no_task='delete_this_dagrun'
        )

        log_dagrun_details_to_table = rail.PostDagRunDetailsToRepliconOperator(
            task_id='log_dagrun_details_to_table',
            required_configs={
                'airflow_connector_ui_connid': config.airflow_connector_ui_connid,
                'hmac_secret_var': config.hmac_secret
            },
            company_key='{{ dag_run.conf.company_key }}',
            connector_name=config.provider,
            integration_type=config.workflow
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        batch_task >> get_last_sync_time >> get_all_users_from_ce >> set_last_sync_time
        batch_task >> should_log_history
        set_last_sync_time >> has_ce_users_to_sync
        has_ce_users_to_sync >> rail.Label(
            'Yes') >> search_user_in_replicon
        search_user_in_replicon >> get_existing_user_details
        get_existing_user_details >> create_group_options_list >> for_each_group >> get_all_group_options >> add_group_options_to_list >> for_each_group_end
        for_each_group >> for_each_group_end >> get_user_oefs >> get_tags_for_each_dropdown_oef >> get_all_permissionsets >> process_each_user
        process_each_user >> wait_for_completion_trigger_process_user >> gather_user_errors >> is_user_error

        is_user_error >> rail.Label('Yes') >> fail_user_error >> should_log_history
        is_user_error >> rail.Label('No') >> should_log_history
        has_ce_users_to_sync >> rail.Label('No') >> should_log_history

        should_log_history >> rail.Label('Yes') >> log_dagrun_details_to_table
        should_log_history >> rail.Label('No') >> delete_this_dagrun

        return dag


rail.for_each_instance(create_dag)
