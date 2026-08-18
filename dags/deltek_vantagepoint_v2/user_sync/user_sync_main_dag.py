from datetime import datetime, timedelta
from uuid import uuid4
from airflow.models import Variable
from deltek_vantagepoint_v2.user_sync.utils.python_callable_method import get_userlist_request, get_user_data_from_list
from deltek_vantagepoint_v2.initial_setup.utils import get_filters_from_custom_settings, build_company_name_to_code_map, build_org_name_to_path_map, build_country_name_to_code_map, build_state_name_to_code_map, build_laborcode_name_to_code_map, resolve_field_values, has_translatable_filters, filters_need_lookups
import rail


null = None


# pylint:disable = too-many-statements, line-too-long
def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.user_sync_dag_id,
        description=f'{config.company_key} Sync users from Deltek Vantagepoint to Replicon',
        company_key=config.company_key,
        max_active_runs=config.max_active_runs,
        multi_tenant=True,
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_initial_run'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_initial_run',
            end_task='should_log_history',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def check_initial_run(dag_run):
            if dag_run.conf.get('force_initial_run'):
                return True
            var = f'{config.user_sync_initial_run_var}_{dag_run.conf["company_key"]}'
            is_initial_run = Variable.get(
                var, default_var='true').lower() == 'true'
            if is_initial_run:
                Variable.set(var, False)
            return is_initial_run

        is_initial_run = rail.IfOperator(
            task_id='is_initial_run',
            test=check_initial_run,
            yes_task='needs_filter_lookups',
            no_task='is_user_deleted_in_vp'
        )

        needs_filter_lookups = rail.IfOperator(
            task_id='needs_filter_lookups',
            test=lambda dag_run: filters_need_lookups(dag_run.conf.get('customSettings', {})),
            yes_task='get_all_companies_for_filter',
            no_task='get_all_users_from_vp'
        )

        def build_field_filter_params(field_name, field_values, start_index, add_and_after_group=False):
            filter_params = []
            for i, value in enumerate(field_values):
                filter_index = start_index + i
                filter_params.extend([
                    f'filterHash[{filter_index}][name]={field_name}',
                    f'filterHash[{filter_index}][value]={value}',
                    f'filterHash[{filter_index}][opp]==',
                    f'filterHash[{filter_index}][seq]={filter_index}'
                ])
                # Add OR condition for all except last
                if len(field_values) > 1 and i < len(field_values) - 1:
                    filter_params.append(f'filterHash[{filter_index}][condition]=OR')
                # Add AND condition after last item if another group follows
                elif add_and_after_group and i == len(field_values) - 1:
                    filter_params.append(f'filterHash[{filter_index}][condition]=AND')
            return filter_params

        def build_filter_params(dag_run):
            custom_settings = dag_run.conf.get('customSettings', {})
            employee_filters = get_filters_from_custom_settings(custom_settings) if custom_settings.get('userSyncFilters') else {}

            # On force_initial_run (integration test path only), if an Employee filter is present,
            # use it exclusively — bypasses status/other filters to scope the sync to one employee.
            # Not applied on the normal production path where Employee + other filters should combine.
            if dag_run.conf.get('force_initial_run') and employee_filters.get('Employee'):
                params = build_field_filter_params('Employee', employee_filters['Employee'], 0)
                return ('&' + '&'.join(params)) if params else ''
            # Lookup tasks only run when a translatable filter is present (gated by needs_filter_lookups).
            # When skipped, leave the maps empty so resolve_field_values passes values through unchanged.
            if has_translatable_filters(employee_filters):
                company_name_to_code = build_company_name_to_code_map(rail.result('get_all_companies_for_filter'))
                org_name_to_path = build_org_name_to_path_map(rail.result('get_all_orgs_for_filter'))
                country_name_to_code = build_country_name_to_code_map(rail.result('get_all_countries_for_filter'))
                state_name_to_code = build_state_name_to_code_map(rail.result('get_all_states_for_filter'))
                laborcode_name_to_code = build_laborcode_name_to_code_map(rail.result('get_all_labor_codes_for_filter'))
            else:
                company_name_to_code = org_name_to_path = country_name_to_code = state_name_to_code = laborcode_name_to_code = {}

            all_params = []
            current_index = 0
            for field_name, field_values in employee_filters.items():
                if field_name in ('Status', 'ReadyForProcessing') or not isinstance(field_values, list) or not field_values:
                    continue
                # UI sends display names: HomeCompany->code, Org->path, Country/State->code, DefaultLC<N>->code.
                resolved_values = resolve_field_values(field_name, field_values, company_name_to_code, org_name_to_path, country_name_to_code, state_name_to_code, laborcode_name_to_code)
                all_params.extend(build_field_filter_params(field_name, resolved_values, current_index))
                current_index += len(resolved_values)

            return ('&' + '&'.join(all_params)) if all_params else ''

        get_all_companies_for_filter = rail.VantagepointAPIOperator(
            task_id='get_all_companies_for_filter',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            endpoint='/Settings/Company',
            request_method='GET'
        )

        get_all_orgs_for_filter = rail.VantagepointAPIOperator(
            task_id='get_all_orgs_for_filter',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            endpoint='/organization',
            request_method='GET'
        )

        get_all_countries_for_filter = rail.VantagepointAPIOperator(
            task_id='get_all_countries_for_filter',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            endpoint='/codeTable/FW_CFGCountry',
            request_method='GET'
        )

        get_all_states_for_filter = rail.VantagepointAPIOperator(
            task_id='get_all_states_for_filter',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            endpoint='/codeTable/CFGStates',
            request_method='GET'
        )

        get_all_labor_codes_for_filter = rail.VantagepointAPIOperator(
            task_id='get_all_labor_codes_for_filter',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            endpoint='/accountConfiguration/laborCode',
            request_method='GET'
        )

        get_all_users_from_vp = rail.VantagepointAPIOperator(
            task_id='get_all_users_from_vp',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            endpoint='/employee',
            request_method='GET',
            filters=lambda dag_run: '?fieldFilter=' + config.employee_required_fields + build_filter_params(dag_run),
            trigger_rule='one_success'
        )

        is_user_deleted_in_vp = rail.IfOperator(
            task_id = 'is_user_deleted_in_vp',
            test = lambda dag_run: (dag_run.conf.get('webhook', {}).get('data', {}).get('Action', '')).lower() == 'delete',
            yes_task = 'search_user_in_replicon',
            no_task = 'get_user_details_for_webhook'
        )

        search_user_in_replicon = rail.RepliconServiceOperator(
            task_id = 'search_user_in_replicon',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint='/services/UserService1.svc/BulkGetUsers2',
            data={
                "users": [
                    {
                        "loginName": "{{dag_run.conf.webhook.data['Employee Number']}}"
                    }
                ]
            },
            data_handler=lambda response: response and response[0] and response[0]['uri']
        )

        if_user_found = rail.IfOperator(
            task_id = 'if_user_found',
            test = lambda: bool(rail.result('search_user_in_replicon')),
            yes_task='disable_user',
            no_task='should_log_history'
        )

        disable_user = rail.RepliconServiceOperator(
            task_id = 'disable_user',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint='/services/ImportService2.svc/CreateUserOrApplyModifications',
            data=lambda dag_run:{
                "target": {
                    "loginName": dag_run.conf['webhook']['data']['Employee Number']
                },
                "modifications": {
                    "securitySettings": {
                        "value": {
                            "loginEnabled": {
                                "value": False
                            }
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save",
                "unitOfWorkId": str(uuid4())
            }
        )

        get_user_details_for_webhook = rail.VantagepointAPIOperator(
            task_id='get_user_details_for_webhook',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            endpoint='/employee/{{dag_run.conf.webhook.data["Employee Number"]}}',
            request_method='GET',
            filters='?fieldFilter=' + config.employee_required_fields
        )

        search_vp_users_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id='search_vp_users_in_replicon',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint='/services/UserListService1.svc/GetData',
            items=lambda: get_userlist_request(config),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            flatten=True,
            data=lambda item: item,
            all_result_data_handler=lambda response: get_user_data_from_list(response, config)
        )

        def should_not_sync_webhook_user():
            webhook_user = rail.result('get_user_details_for_webhook')
            if not webhook_user:
                return False
            is_approved_for_processing = webhook_user[0]['ReadyForProcessing'] == config.YES
            is_status_not_in_allowed = webhook_user[0]['Status'] not in config.sync_users_by_status
            is_user_present_in_replicon = rail.find_first_by_attr_and_get_attr(rail.result(
                'search_vp_users_in_replicon'), 'loginname', rail.result('get_user_details_for_webhook')[0]['Employee'])
            current_status = is_user_present_in_replicon.get('status') if is_user_present_in_replicon else None
            new_status = 'True' if webhook_user[0]['Status'] in config.sync_users_by_status else 'False'
            is_status_not_changed = (not current_status) or (current_status == new_status)
            should_not_sync_user = (not(config.sync_users_not_allowed_for_use_in_processing) and not is_approved_for_processing) or (is_status_not_in_allowed and is_status_not_changed)
            return should_not_sync_user

        if_user_not_to_process = rail.IfOperator(
            task_id = 'if_user_not_to_process',
            test=should_not_sync_webhook_user,
            yes_task='should_log_history',
            no_task='if_supervisor_user_present'
        )

        if_supervisor_user_present = rail.IfOperator(
            task_id='if_supervisor_user_present',
            test=lambda: bool(rail.result('get_all_users_from_vp')) or not (rail.result(
                'get_user_details_for_webhook')[0]['Supervisor']) or bool(rail.find_first_by_attr_and_get_attr(rail.result(
                    'search_vp_users_in_replicon'), 'loginname', rail.result('get_user_details_for_webhook')[0]['Supervisor'])),
            yes_task='get_list_of_users_to_process',
            no_task='get_supervisor_user_details_from_vp'
        )

        get_supervisor_user_details_from_vp = rail.VantagepointAPIOperator(
            task_id='get_supervisor_user_details_from_vp',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            endpoint="/employee/{{result('get_user_details_for_webhook')[0].Supervisor}}",
            request_method='GET',
            filters='?fieldFilter=' + config.employee_required_fields
        )

        def get_users_list():
            webhook_user = rail.result('get_user_details_for_webhook')
            supervisor_user = rail.result(
                'get_supervisor_user_details_from_vp')
            if supervisor_user:
                webhook_user.extend(supervisor_user)
            bulk_users = rail.result('get_all_users_from_vp')
            is_bulk = bulk_users is not None
            users_list = bulk_users if is_bulk else (webhook_user or [])
            return list(filter(lambda user: (
                (not is_bulk or user['Status'] in config.sync_users_by_status)
                and (config.sync_users_not_allowed_for_use_in_processing or user['ReadyForProcessing'] == config.YES)
            ), users_list))

        get_list_of_users_to_process = rail.PythonOperator(
            task_id='get_list_of_users_to_process',
            python_callable=get_users_list
        )

        def get_payload_batches():
            chunk_size = 50
            all_existing_users = [user['uri'] for user in rail.result('search_vp_users_in_replicon')]
            user_chunks = [all_existing_users[i:i + chunk_size] for i in range(0, len(all_existing_users), chunk_size)]
            return user_chunks

        get_existing_schedules_for_users = rail.RepliconServiceCallForEachItemOperator(
            task_id = 'get_existing_schedules_for_users',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            items= get_payload_batches,
            endpoint='/services/SchedulingService2.svc/BulkGetGetSchedulePolicyScheduleForUsers',
            flatten=True,
            data= lambda item: {
                "userUris": item
            },
            all_result_data_handler=lambda response: list(map(lambda user: {
                "uri": user['userUri'],
                "schedule": user['schedule'] and len(user['schedule']) > 0 and user['schedule'][-1]['officeSchedule']['displayText']
            }, response))
        )

        create_group_options_list = rail.SetVariableOperator(
            task_id = 'create_group_options_list',
            name='group_options_list',
            append=False,
            value=[]
        )

        for_each_group = rail.ForEachOperator(
            task_id = 'for_each_group',
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
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint="{{result('for_each_group').optionsendpoint}}",
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
            value=lambda:{
                rail.result('for_each_group')['type']: rail.result('get_all_group_options')
            }
        )

        for_each_group_end = rail.EmptyOperator(
            task_id = 'for_each_group_end'
        )


        get_all_permissionsets = rail.RepliconServiceOperator(
            task_id='get_all_permissionsets',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets'
        )

        def _resolve_dynamic_name(cfg_oef):
            if not cfg_oef['id'].startswith('laborcodelevel'):
                return cfg_oef['name']
            dag_run = rail.get_current_context()['dag_run']
            initial_cs = dag_run.conf.get('initial_custom_settings', {})
            if 'M' in initial_cs and isinstance(initial_cs.get('M'), dict):
                initial_cs = initial_cs['M']
            labor_code_setting = initial_cs.get('laborCodeSetting', {}) or {}
            if not labor_code_setting.get('configureLaborCode', False):
                return cfg_oef['name']
            levels = labor_code_setting.get('levels', [])
            level_num = int(cfg_oef['id'].replace('laborcodelevel', ''))
            if level_num <= len(levels):
                return str(levels[level_num - 1])
            return cfg_oef['name']

        def get_oef_definition_uris(oefs):
            oef_definitions = []
            for cfg_oef in config.oefs:
                name = _resolve_dynamic_name(cfg_oef)
                replicon_oef = rail.find_first_by_attr_and_get_attr(oefs, 'name', name)
                oef_definitions.append({
                    'id': cfg_oef['id'],
                    'definitionuri': replicon_oef and replicon_oef.get('uri'),
                    'name': name,
                    'type': cfg_oef['type'],
                    'input': cfg_oef.get('input'),
                    'bind': cfg_oef.get('bind', [])
                })
            return oef_definitions

        get_user_oefs = rail.RepliconServiceOperator(
            task_id="get_user_oefs",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:user"
            },
            # pylint: disable=unnecessary-lambda
            data_handler=lambda oefs: get_oef_definition_uris(oefs)
        )

        def format_tags(response):
            tag_options = list(map(lambda row: {
                "name": row['cells'][0]['textValue'],
                "code": row['cells'][1].get('textValue'),
                "uri": row['cells'][2]['uri'],
                "is_enabled": row['cells'][3]['textValue']
            }, response['rows']))
            return tag_options if tag_options else None

        def get_tags_object(response, item):
            return {
                **item,
                'tags': format_tags(response)
            }

        get_tags_for_each_dropdown_oef = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_tags_for_each_dropdown_oef',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint='services/ObjectExtensionTagListService1.svc/GetData',
            items=lambda: list(filter(lambda oef: (
                oef['type'] == 'dropdown' and oef['definitionuri']), rail.result('get_user_oefs'))),
            data=lambda item: {
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:object-extension-tag-list-column:name",
                    "urn:replicon:object-extension-tag-list-column:code",
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

        create_supervisor_processing_log = rail.CreateLogOperator(
            task_id='create_supervisor_processing_log',
        )

        def get_date(date):
            return rail.get_replicon_date(datetime.strptime(date, "%Y-%m-%dT%H:%M:%S.%f")) if date else null

        def get_oef_definition_and_value_with_exceptions(user):
            oefs = []
            exception_messages = []
            for oef in rail.result('get_user_oefs'):
                if oef['definitionuri']:
                    oefs.append({
                        'id': oef['id'],
                        'def': oef['definitionuri'],
                        'value': rail.find_first_by_attr_and_get_attr(rail.find_first_by_attr_and_get_attr(rail.result(
                            'get_tags_for_each_dropdown_oef'), 'id', oef['id'], 'tags'), 'code', str(user[oef[
                                'input']]), 'uri') if oef['type'] == 'dropdown' else user[oef['input']],
                        'type': oef['type'],
                        'input': user[oef['input']],
                        'name': oef['name']
                    })
                else:
                    exception_messages.append(f"{oef['name']} not assigned since the field is not present in Replicon. ")
            return {
                "oefs": oefs,
                "exception_messages": exception_messages
            }

        def get_supervisor_details(user):
            supervisor_user = rail.find_first_by_attr_and_get_attr(rail.result('search_vp_users_in_replicon'), 'loginname', user['Supervisor'])
            return {
                "supervisor": user['Supervisor'],
                "is_user_own_supervisor": user['Supervisor'] == user['Employee'],
                "supervisoruri": supervisor_user and supervisor_user.get('uri'),
                "is_supervisor_enabled": supervisor_user and (supervisor_user.get('status') == 'True'),
            }

        def get_groups(user, group_options):
            group_values = {}
            for group in config.groups:
                if group['input'] == 'PayType':
                    value = user.get('PayType')
                    value = config.paytype_employeetype_map.get(value, value) if value is not None else None
                else:
                    value = user[group['input']]

                group_values[group['type']] = rail.find_first_by_attr_and_get_attr(
                    group_options[group['type']], group['assignby'], value, 'uri'
                ) if value is not None else None
                group_values[group['type'] + 'name'] = value
            return group_values

        def get_current_details(user):
            current_details = rail.find_first_by_attr_and_get_attr(rail.result('search_vp_users_in_replicon'), 'loginname', user['Employee'])
            if current_details and current_details['uri']:
                current_details['scheduletype'] = rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_existing_schedules_for_users'), 'uri',current_details['uri'], 'schedule')
            return current_details

        def parse_options(all_groups):
            group_options = {}
            for group in all_groups:
                group_options.update(group)
            return group_options

        def parse_users():
            users = rail.result('get_list_of_users_to_process')
            user_configuration = (config.user_configuration)[0]
            group_options = parse_options(rail.get_dag_run_var('group_options_list'))
            return list(map(lambda user: {
                "loginname": user['Employee'],
                "currentdetails": get_current_details(user),
                **get_groups(user, group_options),
                **get_supervisor_details(user),
                "loginenabled": user['Status'] in config.sync_users_by_status,
                "startdate": get_date(user['HireDate']),
                "enddate": get_date(user['TerminationDate']),
                "firstname": user['FirstName'],
                "lastname": user['LastName'],
                "displayname": user['PreferredName'],
                "email": user['EMail'],
                "supervisorpermissionuri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_all_permissionsets'), 'name', user_configuration['supervisorpermission'], 'uri'),
                "timesheettemplate": user_configuration['timesheettemplate'],
                "timesheetperiod": user_configuration['timesheetperiod'],
                "timesheetapprovalpath": user_configuration['timesheetapprovalpath'],
                "scheduletype": user_configuration['scheduletype'],
                "workweek": user_configuration['workweek'],
                "timezone": user_configuration['timezone'],
                "permissions": list(map(lambda permission_name: {
                    "permissionSetPolicy": {
                        "name": permission_name
                    }
                }, user_configuration['permissions'])),
                "modDate": rail.get_replicon_date(datetime.fromisoformat(user['ModDate'])),
                **get_oef_definition_and_value_with_exceptions(user)
            }, users))

        process_each_user = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_user',
            retries=0,
            items=parse_users,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_each_user_child_dag_id,
            conf=lambda item, dag_run: {
                **item,
                **{
                    "supervisor_processing_log": rail.result('create_supervisor_processing_log'),
                    "replicon_conn_id": dag_run.conf['replicon_conn_id'],
                    "vantagepoint_conn_id": dag_run.conf['vantagepoint_conn_id'],
                    "company_key": dag_run.conf['company_key']
                }
            }
        )

        wait_for_completion_trigger_process_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_process_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_each_user") }}'
        )

        def get_data_from_document(document):
            with rail.lib.readers.get_data_reader(document) as reader:
                return list(reader)

        def get_supervisor_entries():
            supervisor_details = []
            supervisor_log_informations = get_data_from_document(
                rail.result('create_supervisor_processing_log'))
            for supervisor_info in supervisor_log_informations:
                if supervisor_info['properties']:
                    supervisor_details.append({
                        "loginname": supervisor_info['properties'].get('loginname'),
                        "useruri": supervisor_info['properties'].get('useruri'),
                        "supervisor": supervisor_info['properties'].get('supervisor'),
                        "currentsupervisor": supervisor_info['properties'].get('currentsupervisor'),
                        "supervisorpermissionuri": supervisor_info['properties'].get('supervisorpermissionuri'),
                        "effectivedate": supervisor_info['properties'].get('effectivedate'),
                    })
            return supervisor_details

        process_supervisor_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id='process_supervisor_assignment',
            retries=0,
            items=get_supervisor_entries,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.supervisor_assignment_child_dag_id,
            conf=lambda item, dag_run: {
                **item,
                **{
                    "replicon_conn_id": dag_run.conf['replicon_conn_id'],
                    "vantagepoint_conn_id": dag_run.conf['vantagepoint_conn_id'],
                    "company_key": dag_run.conf['company_key']
                }
            }
        )

        wait_for_supervisor_assignment_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_supervisor_assignment_completion',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_supervisor_assignment") }}'
        )

        gather_child_dag_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_dag_errors',
            dag_runs="{{ [result('process_each_user'), result('process_supervisor_assignment')] }}",
            dagrun_task_id='catch_error',
            flatten=True
        )

        is_child_dag_error = rail.IfOperator(
            task_id='is_child_dag_error',
            test="{{ (get_task_state('gather_child_dag_errors') == 'success' and result('gather_child_dag_errors') | length > 0) }}",
            yes_task='fail_child_dag_error',
            no_task='should_log_history'
        )

        fail_child_dag_error = rail.FailOperator(
            task_id='fail_child_dag_error',
            message="{{ result('gather_child_dag_errors') | map_to_attr('error') | join('|') }}"
        )

        should_log_history = rail.IfOperator(
            task_id='should_log_history',
            test="{{ get_task_state('gather_child_dag_errors') == 'success' }}",
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

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> should_log_history
        can_run_batch_task >> rail.Label('No') >> is_initial_run
        is_initial_run >> rail.Label('Yes') >> needs_filter_lookups
        needs_filter_lookups >> rail.Label('Yes') >> get_all_companies_for_filter >> get_all_orgs_for_filter >> get_all_countries_for_filter >> get_all_states_for_filter >> get_all_labor_codes_for_filter >> get_all_users_from_vp >> search_vp_users_in_replicon
        needs_filter_lookups >> rail.Label('No') >> get_all_users_from_vp
        is_initial_run >> rail.Label('No') >> is_user_deleted_in_vp
        is_user_deleted_in_vp >> rail.Label('Yes') >> search_user_in_replicon >> if_user_found
        if_user_found >> rail.Label('Yes') >> disable_user >> should_log_history
        if_user_found >> rail.Label('No') >> should_log_history
        is_user_deleted_in_vp >> rail.Label('No') >> get_user_details_for_webhook >> search_vp_users_in_replicon >> if_user_not_to_process
        if_user_not_to_process >> rail.Label('Yes') >> should_log_history
        if_user_not_to_process >> rail.Label('No') >> if_supervisor_user_present
        if_supervisor_user_present >> rail.Label('Yes') >> get_list_of_users_to_process
        if_supervisor_user_present >> rail.Label('No') >> get_supervisor_user_details_from_vp
        get_supervisor_user_details_from_vp >> get_list_of_users_to_process >> get_existing_schedules_for_users >> create_group_options_list
        create_group_options_list >> for_each_group >> get_all_group_options >> add_group_options_to_list >> for_each_group_end
        for_each_group >> for_each_group_end >> get_all_permissionsets >> get_user_oefs >> get_tags_for_each_dropdown_oef
        get_tags_for_each_dropdown_oef >> create_supervisor_processing_log >> process_each_user
        process_each_user >> wait_for_completion_trigger_process_user >> process_supervisor_assignment
        process_supervisor_assignment >> wait_for_supervisor_assignment_completion >> gather_child_dag_errors >> is_child_dag_error
        is_child_dag_error >> rail.Label('Yes') >> fail_child_dag_error >> should_log_history
        is_child_dag_error >> rail.Label('No') >> should_log_history
        should_log_history >> rail.Label('Yes') >> log_dagrun_details_to_table
        should_log_history >> rail.Label('No') >> delete_this_dagrun

        return dag


rail.for_each_instance(create_dag)
