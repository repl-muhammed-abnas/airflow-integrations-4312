from datetime import datetime, timedelta
from uuid import uuid4
from airflow.models import Variable
from deltek_vantagepoint.user_sync.utils.python_callable_method import get_userlist_request, get_user_data_from_list
import rail


null = None


# pylint:disable = too-many-statements, line-too-long
def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_vantagepoint_user_sync_main_{config.instance}',
        description='Sync users from Deltek Vantagepoint to Replicon',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        webhook_conf=rail.WebhookConf(
            basic_auth_username_var=config.webhook_basicauth_username,
            basic_auth_password_var=config.webhook_basicauth_password
        ),
        default_args={
            'vp_conn_id': config.deltek_vantagepoint_conn_id
        }
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
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def check_initial_run():
            var = f'{config.initial_run_flag}_{config.company_key}'
            is_initial_run = Variable.get(
                var, default_var='true').lower() == 'true'
            if is_initial_run:
                Variable.set(var, False)
            return is_initial_run

        is_initial_run = rail.IfOperator(
            task_id='is_initial_run',
            test=check_initial_run,
            yes_task='get_all_users_from_vp',
            no_task='is_user_deleted_in_vp'
        )

        def build_field_filter_params(field_name, field_values, start_index):
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
            return filter_params

        def build_filter_params():

            def build_status_filter_params():
                if not config.sync_users_by_status:
                    return [], 0
                return build_field_filter_params('Status', config.sync_users_by_status, 0), len(config.sync_users_by_status)

            status_params, status_count = build_status_filter_params()

            def build_employee_filter_params():
                employee_filters = Variable.get(config.usersync_filter_var, deserialize_json=True, default_var={})
                employee_filter_params = []
                current_index = status_count

                for field_name, field_values in employee_filters.items():
                    if field_name in ('Status', 'ReadyForProcessing') or not isinstance(field_values, list) or not field_values:
                        continue
                    params = build_field_filter_params(field_name, field_values, current_index)
                    employee_filter_params.extend(params)
                    current_index += len(field_values)

                return employee_filter_params

            employee_filter_params = build_employee_filter_params()

            all_params = status_params + employee_filter_params
            return ('&' + '&'.join(all_params)) if all_params else ''

        get_all_users_from_vp = rail.VantagepointAPIOperator(
            task_id='get_all_users_from_vp',
            endpoint='/employee',
            request_method='GET',
            filters='?fieldFilter=' + config.employee_required_fields + build_filter_params()
        )

        is_user_deleted_in_vp = rail.IfOperator(
            task_id = 'is_user_deleted_in_vp',
            test = lambda dag_run: (dag_run.conf['webhook']['data'].get('Action', '')).lower() == 'delete',
            yes_task = 'search_user_in_replicon',
            no_task = 'get_user_details_for_webhook'
        )

        search_user_in_replicon = rail.RepliconServiceOperator(
            task_id = 'search_user_in_replicon',
            endpoint='/services/UserService1.svc/BulkGetUsers2',
            data={
                "users": [
                    {
                        "loginName": "{{dag_run.conf.webhook.data.EmployeeNumber}}"
                    }
                ]
            },
            data_handler=lambda response: response and response[0] and response[0]['uri']
        )

        if_user_found = rail.IfOperator(
            task_id = 'if_user_found',
            test = lambda: rail.result('search_user_in_replicon'),
            yes_task='disable_user',
            no_task='log_to_sumo'
        )

        disable_user = rail.RepliconServiceOperator(
            task_id = 'disable_user',
            endpoint='/services/ImportService2.svc/CreateUserOrApplyModifications',
            data=lambda dag_run:{
                "target": {
                    "loginName": dag_run.conf['webhook']['data']['EmployeeNumber']
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
            endpoint='/employee/{{dag_run.conf.webhook.data.EmployeeNumber}}',
            request_method='GET',
            filters='?fieldFilter=' + config.employee_required_fields
        )

        search_vp_users_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id='search_vp_users_in_replicon',
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
            yes_task='log_to_sumo',
            no_task='if_supervisor_user_present'
        )

        if_supervisor_user_present = rail.IfOperator(
            task_id='if_supervisor_user_present',
            test=lambda: rail.result('get_all_users_from_vp') or not (rail.result(
                'get_user_details_for_webhook')[0]['Supervisor']) or rail.find_first_by_attr_and_get_attr(rail.result(
                    'search_vp_users_in_replicon'), 'loginname', rail.result('get_user_details_for_webhook')[0]['Supervisor']),
            yes_task='get_list_of_users_to_process',
            no_task='get_supervisor_user_details_from_vp'
        )

        get_supervisor_user_details_from_vp = rail.VantagepointAPIOperator(
            task_id='get_supervisor_user_details_from_vp',
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
            users_list =  rail.result('get_all_users_from_vp') or webhook_user
            return list(filter(lambda user: (config.sync_users_not_allowed_for_use_in_processing or user['ReadyForProcessing'] == config.YES), users_list))

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
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets'
        )

        def get_oef_definition_uris(oefs):
            oef_definitions = []
            for oef in config.oefs:
                oef_definitions.append({
                    'id': oef['id'],
                    'definitionuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', oef['name'], 'uri'),
                    'type': oef['type']
                })
            return oef_definitions

        get_user_oefs = rail.RepliconServiceOperator(
            task_id="get_user_oefs",
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
            for oef in config.oefs:
                oef_definition = rail.find_first_by_attr_and_get_attr(rail.result('get_user_oefs'), 'id', oef['id'], 'definitionuri')
                if oef_definition:
                    oefs.append({
                        'id': oef['id'],
                        'def': oef_definition,
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
                value = user[group['input']]
                if group['input'] == 'PayType':
                    value = config.paytype_employeetype_map.get(value, value)
                group_values[group['type']] = rail.find_first_by_attr_and_get_attr(group_options[group['type']], group['assignby'], value, 'uri')
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
            trigger_dag_id=f'deltek_vantagepoint_process_each_user_child_{config.instance}',
            conf=lambda item: {
                **item,
                **{
                    "supervisor_processing_log": rail.result('create_supervisor_processing_log')
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
            trigger_dag_id=f'deltek_vantagepoint_process_supervisor_assignment_child_{config.instance}',
            conf=lambda item: {
                **item
            }
        )

        wait_for_supervisor_assignment_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_supervisor_assignment_completion',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_supervisor_assignment") }}'
        )

        search_logs = rail.FilterLogEntriesOperator(
            task_id='search_logs',
            severity='Error/Exception'
        )

        if_logs_present = rail.IfOperator(
            task_id='if_logs_present',
            test=lambda: rail.result('search_logs', 'length') > 0,
            yes_task='write_logs_to_csv',
            no_task='log_to_sumo'
        )

        write_logs_to_csv = rail.WriteCSVFileOperator(
            task_id='write_logs_to_csv',
            source='{{result("search_logs")}}',
            header=["Employee",
                    "Action",
                    "Status",
                    "Details",
                    "Ecid"
                    ],
            row=[
                '{{item.properties| attr_or_default("loginname","")}}',
                '{{item.properties| attr_or_default("action","")}}',
                '{{item.properties| attr_or_default("status","")}}',
                '{{item.properties| attr_or_default("reason","")}}',
                '{{item| attr_or_default("ecid","")}}'
            ]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('write_logs_to_csv')}}",
            output_file_name='UserSyncLogs - {{ current_time() }}.csv',
            expires_in_seconds=7*24*60*60,
        )

        send_mail_error = rail.EmailOperator(
            task_id='send_mail_error',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject='''{{ get_company_key() }} | Deltek Vantagepoint User sync Completed with Errors/Exceptions - {{ current_time() }}''',
            html_content="templates/failure_email.html",
            params=None,
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> is_initial_run
        is_initial_run >> rail.Label(
            'Yes') >> get_all_users_from_vp >> search_vp_users_in_replicon
        is_initial_run >> rail.Label(
            'No') >> is_user_deleted_in_vp
        is_user_deleted_in_vp >> rail.Label('Yes') >> search_user_in_replicon >> if_user_found
        if_user_found >> rail.Label('Yes') >> disable_user >> log_to_sumo
        if_user_found >> rail.Label('No') >> log_to_sumo
        is_user_deleted_in_vp >> rail.Label('No') >> get_user_details_for_webhook >> search_vp_users_in_replicon >> if_user_not_to_process
        if_user_not_to_process >> rail.Label('Yes') >> log_to_sumo
        if_user_not_to_process >> rail.Label('No') >> if_supervisor_user_present
        if_supervisor_user_present >> rail.Label(
            'Yes') >> get_list_of_users_to_process
        if_supervisor_user_present >> rail.Label(
            'No') >> get_supervisor_user_details_from_vp
        get_supervisor_user_details_from_vp >> get_list_of_users_to_process >> get_existing_schedules_for_users >> create_group_options_list
        create_group_options_list >> for_each_group >> get_all_group_options >> add_group_options_to_list >> for_each_group_end
        for_each_group >> for_each_group_end >> get_all_permissionsets >> get_user_oefs >> get_tags_for_each_dropdown_oef
        get_tags_for_each_dropdown_oef >> create_supervisor_processing_log >> process_each_user
        process_each_user >> wait_for_completion_trigger_process_user >> process_supervisor_assignment
        process_supervisor_assignment >> wait_for_supervisor_assignment_completion >> search_logs >> if_logs_present
        if_logs_present >> rail.Label(
            'Yes') >> write_logs_to_csv >> generate_download_link >> send_mail_error >> log_to_sumo
        if_logs_present >> rail.Label('No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
