from datetime import timedelta
import rail
from galaxyusopcoinc.workday_user_sync.user_import_v2.utils import request_payload, response_filter
from galaxyusopcoinc.workday_user_sync.user_import_v2.tasks.update_supervisor import get_update_supervisor
from airflow.models import Variable

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.user_update_dag_id,
        description=f'VialtoPartners_User Import_Child_User Update {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.user_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        null = None


        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='bulk_get_user3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="bulk_get_user3",
            end_task="catch_and_log_errors"
        )

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        bulk_get_user3 = rail.RepliconServiceOperator(
            task_id='bulk_get_user3',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=lambda dag_run: {
                "users": [
                    {
                        "uri": dag_run.conf['useruri'],
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            response_filter=lambda res: res.json()['d'][0]
        )

        get_assigned_policysets_for_user = rail.RepliconServiceOperator(
            task_id='get_assigned_policysets_for_user',
            endpoint='/services/policySetService1.svc/GetAssignedPolicySetsForUser',
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
            }
        )

        is_user_already_disabled = rail.IfOperator(
            task_id='is_user_already_disabled',
            test=lambda: not rail.result(bulk_get_user3.task_id)[
                'userDetails']['isEnabled'],
            yes_task='update_empid',
            no_task="get_current_timesheet"
        )

        update_empid = rail.RepliconServiceOperator(
            task_id='update_empid',
            endpoint='/services/userService1.svc/UpdateEmployeeId',
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "employeeId": request_payload.employeeid_toupdate()
            }
        )

        update_login_name = rail.RepliconServiceOperator(
            task_id='update_login_name',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri'],
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications":  {
                    "securitySettingsToApply": {
                        "loginName": request_payload.loginname_toupdate(),
                        "ssoName": request_payload.loginname_toupdate(),
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        add_user = rail.TriggerDagRunForEachItemOperator(
            task_id='add_user',
            retries=0,
            items=lambda dag_run: [dag_run.conf],
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.user_add_dag_id,
            conf=lambda item, dag_run: {
                **item, **{
                    'validationlog': dag_run.conf['validationlog'],
                    'log': dag_run.conf.get('log'),
                    'rehire': "yes"
                }
            }
        )

        wait_for_process_add_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_add_user',
            dag_runs="{{ result('add_user') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        log_user_already_disabled = rail.WriteLogOperator(
            task_id='log_user_already_disabled',
            message='User already disabled. Updated existing user Employee ID',
            severity='Success',
            log="{{dag_run.conf.create_user_log}}",
            properties=lambda dag_run : {
                'employeeid': dag_run.conf['employeeid'],
                'username': f"{dag_run.conf['legalfirstname']} {dag_run.conf['legallastname']}",
                'loginname': dag_run.conf['workemail'],
                'status': 'Success',
                'action': 'Update',
                'message': 'User already disabled. Updated existing user Employee ID',
                "allowed_for_supervisor_dag": "False",
                "user_uri": "",
                "managerid": dag_run.conf['managerid'],
                "is_add_and_errored": "False"
            }
        )

        get_effective_user_groupmembership = rail.RepliconServiceOperator(
            task_id='get_effective_user_groupmembership',
            endpoint='/services/UserGroupService1.svc/GetEffectiveUserGroupMembership',
            data={
                "userUri": "{{dag_run.conf.useruri}}",
                "dateRange": null
            },
            data_handler=response_filter.get_effective_user_groupmembership_filter

        )

        get_current_timesheet = rail.RepliconServiceOperator(
            task_id="get_current_timesheet",
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetailsForDate",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "date": request_payload.get_today_date(),
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            }
        )

        prepare_update_payload = rail.PythonOperator(
            task_id="prepare_update_payload",
            python_callable=request_payload.get_update_user_payload
        )

        update_user_details = rail.RepliconServiceOperator(
            task_id="update_user_details",
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data="{{result('prepare_update_payload') | to_json }}"
        )

        is_update_failed = rail.IfOperator(
            task_id="is_update_failed",
            test=lambda: bool(rail.result(
                'update_user_details').get('errors')),
            yes_task="log_update_failed",
            no_task="is_country_changed"
        )

        log_update_failed = rail.WriteLogOperator(
            task_id="log_update_failed",
            message="{{ result('update_user_details').errors }}",
            log="{{dag_run.conf.create_user_log}}",
            properties=lambda dag_run:{
                'employeeid': '{{dag_run.conf.employeeid}}',
                'username':  f'{dag_run.conf["legalfirstname"]} {dag_run.conf["legallastname"]}',
                'loginname': dag_run.conf['workemail'],
                'status': 'Error',
                'action': 'Update',
                'message': rail.result('update_user_details').get('errors', "Unknown error occurred"),
                "allowed_for_supervisor_dag": "False",
                "user_uri": dag_run.conf["useruri"],
                "managerid": dag_run.conf['managerid'],
                "is_add_and_errored": "False",
                "user_effective_date": request_payload.get_new_effective_date()

            },
        )

        def is_location_updated_test(dag_run):
            return dag_run.conf['country_location_uri'] != rail.result('get_effective_user_groupmembership', 'location').get('uri', '')

        def is_country_updated(dag_run):
            return (rail.find_first_by_attr_and_get_attr(
                rail.result(bulk_get_user3.task_id)['userDetails']['extensionFieldValues'], 'definition.displayText', 'Country')
                    ) and (rail.find_first_by_attr_and_get_attr(
                rail.result(bulk_get_user3.task_id)['userDetails']['extensionFieldValues'], 'definition.displayText', 'Country')
                           )['textValue'] != dag_run.conf['country']

        is_country_changed = rail.IfOperator(
            task_id='is_country_changed',
            test=lambda dag_run: is_country_updated(dag_run) or is_location_updated_test(dag_run),
            yes_task='get_timeoff_toassign',
            no_task='update_supervisor_task_start'
        )

        get_timeoff_toassign = rail.PythonOperator(
            task_id='get_timeoff_toassign',
            python_callable=response_filter.map_timeoff_data

        )

        has_timeofftype = rail.IfOperator(
            task_id='has_timeofftype',
            test=lambda: len(rail.result('get_timeoff_toassign')) > 0,
            yes_task='get_users_assigned_timeoff',
            no_task='update_supervisor_task_start',
        )

        get_users_assigned_timeoff = rail.RepliconServiceOperator(
            task_id="get_users_assigned_timeoff",
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{dag_run.conf.useruri}}"
            },
            data_handler=response_filter.get_users_assigned_timeoff_filter
        )

        process_zero_line_policy = rail.TriggerDagRunForEachItemOperator(
            task_id="process_zero_line_policy",
            trigger_dag_id=config.add_zero_line_policy_dag_id,
            items=lambda: rail.result(
                "get_users_assigned_timeoff", 'timeoff_to_remove_with_zero_line_policy'),
            conf=lambda item, dag_run: {
                **item,
                **{
                    "prevent_balance_overdraw_uri": dag_run.conf['prevent_balance_overdraw_uri']['uri'],
                    "starting_balance_set_to_uri": dag_run.conf["starting_balance_set_to_uri"]['uri'],
                    "user_uri": dag_run.conf['useruri'],
                    "user_employeeid": dag_run.conf['employeeid'],
                    "timeoff_uris": rail.result('get_timeoff_toassign'),
                    "timeoff_names": rail.result('get_timeoff_toassign', 'timeoff_names'),
                    "employeeid": dag_run.conf["employeeid"],
                    "username": f'{dag_run.conf["legalfirstname"]} {dag_run.conf["legallastname"]}',
                    "workemail": dag_run.conf['workemail'],
                    "managerid": dag_run.conf["managerid"]

                }
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_process_zero_line_policy = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_zero_line_policy',
            dag_runs="{{ result('process_zero_line_policy') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        add_timeofftype = rail.RepliconServiceOperator(
            task_id='add_timeofftype',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data=request_payload.get_timeoff_payload_update
        )

        process_timeoff_policies = rail.TriggerDagRunForEachItemOperator(
            task_id="process_timeoff_policies",
            items=lambda: rail.result("get_timeoff_toassign"),
            trigger_dag_id=config.process_timeoff_dag_id,
            conf=lambda dag_run, item: {
                **{
                    "timeoff_to_process": item,
                    "action": "update"
                },
                **{
                    "user_uri": dag_run.conf['useruri'],
                    "effective_date_to_use": request_payload.get_new_effective_date()
                },
                **dag_run.conf
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        wait_for_process_timeoff_policies = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_timeoff_policies",
            dag_runs="{{result('process_timeoff_policies')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        update_supervisor_task_start = rail.EmptyOperator(
            task_id="update_supervisor_task_start"
        )

        update_supervisor_task, update_supervisor_assignmentschedule_overdaterange = get_update_supervisor(
            caller="update")

        logs_map = {
            update_supervisor_assignmentschedule_overdaterange.task_id: 'Supervisor updated',
        }

        def do_map_logs(dag_run):
            logs = rail.result('prepare_update_payload', 'updated_fields').split(
                ";") if rail.result('prepare_update_payload', 'updated_fields') else []
            if len(dag_run.conf.get('validationlog', [])) > 0:
                logs.extend(list(
                    map(lambda item: item['message'], dag_run.conf['validationlog'])))
            success_tasks = list(map(lambda x: x.task_id, filter(lambda x: x.state == 'success',
                                                                 rail.get_current_context()['dag_run'].get_task_instances())))
            for key in [*logs_map]:
                if key in success_tasks:
                    logs.append(logs_map[key])

            return logs

        allowed_for_supervisor_dag = rail.PythonOperator(
            task_id="allowed_for_supervisor_dag",
            # if the supervisor is not found then it will be true
            python_callable=lambda: not bool(
                rail.result("search_supervisor_by_employeeid"))
        )

        map_logs = rail.PythonOperator(
            task_id='map_logs',
            python_callable=do_map_logs
        )

        write_update_logs = rail.WriteLogOperator(
            task_id='write_update_logs',
            log="{{dag_run.conf.create_user_log}}",
            message=' {%- if dag_run.conf.validationlog | length > 0 -%} \
                    User partially updated [{{ result("map_logs") | join("; ") }}] \
                {%- else -%} \
                    Successfully updated [{{ result("map_logs") | join("; ") }}] \
                {%- endif -%} ',
            severity='Success',
            properties=lambda dag_run: {
                'employeeid': dag_run.conf['employeeid'],
                'username':  f"{dag_run.conf['legalfirstname']} {dag_run.conf['legallastname']}",
                'loginname': dag_run.conf["workemail"],
                'status': 'Success',
                'action': 'Update',
                'message': rail.render_template(' {%- if dag_run.conf.validationlog | length > 0 -%} \
                    User partially updated [{{ result("map_logs") | join(", ") }}] \
                {%- else -%} \
                    Successfully updated [{{ result("map_logs") | join(", ") }}] \
                {%- endif -%} '),
                "allowed_for_supervisor_dag": rail.result('allowed_for_supervisor_dag'),
                "user_uri": dag_run.conf['useruri'],
                "managerid": dag_run.conf['managerid'],
                "is_add_and_errored": "False",
                "create_user_log": dag_run.conf['create_user_log'],
                "user_effective_date": request_payload.get_new_effective_date()
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{dag_run.conf.create_user_log}}",
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties=lambda dag_run : {
                'employeeid': dag_run.conf['employeeid'],
                'username':  f"{dag_run.conf['legalfirstname']} {dag_run.conf['legallastname']}",
                'loginname': dag_run.conf['workemail'],
                'status': 'Error',
                'action': 'Update',
                'message': rail.render_template('{{ get_error_message() }}'),
                "allowed_for_supervisor_dag": rail.render_template("{{result('allowed_for_supervisor_dag') if get_task_state('add_user') == 'success' else  False}}"),
                "user_uri": dag_run.conf['useruri'],
                "managerid": dag_run.conf['managerid'],
                "is_add_and_errored": "False",
                "user_effective_date": request_payload.get_new_effective_date(),
                "create_user_log": dag_run.conf['create_user_log']
            },
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> bulk_get_user3

        bulk_get_user3 >> get_assigned_policysets_for_user >> get_effective_user_groupmembership
        get_effective_user_groupmembership >> is_user_already_disabled >> rail.Label(
            'Yes') >> update_empid >> update_login_name >> add_user >> wait_for_process_add_user >> log_user_already_disabled >> catch_and_log_errors >> log_to_sumo

        is_user_already_disabled >> rail.Label(
            'No') >> get_current_timesheet >> prepare_update_payload >> update_user_details >> is_update_failed >> rail.Label("No") >> is_country_changed

        is_update_failed >> rail.Label("Yes") >> log_update_failed >> rail.Label(
            "On Error") >> catch_and_log_errors
        is_country_changed >> rail.Label(
            'Yes') >> get_timeoff_toassign >> has_timeofftype
        is_country_changed >> rail.Label(
            'No') >> update_supervisor_task_start >> update_supervisor_task >> allowed_for_supervisor_dag >> map_logs
        map_logs >> write_update_logs >> catch_and_log_errors

        has_timeofftype >> rail.Label(
            'Yes') >> get_users_assigned_timeoff >> add_timeofftype >> process_zero_line_policy >> wait_for_process_zero_line_policy
        wait_for_process_zero_line_policy >> process_timeoff_policies >> wait_for_process_timeoff_policies\
            >> update_supervisor_task_start >> update_supervisor_task >> allowed_for_supervisor_dag >> map_logs >>\
            write_update_logs >> catch_and_log_errors
        has_timeofftype >> rail.Label(
            'no') >> update_supervisor_task_start >> update_supervisor_task >> allowed_for_supervisor_dag >> map_logs
        map_logs >> write_update_logs >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
