
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'hawaiigas_user_import_supervisor_assignment_{config.instance}',
        description=f'HawaiiGas Supervisor Assignment {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_today_date_obj'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_today_date_obj',
            end_task='catch_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_today_date_obj=rail.PythonOperator(
            task_id='get_today_date_obj',
            python_callable= lambda: {
                'day': datetime.now().day,
                'month': datetime.now().month,
                'year': datetime.now().year
            }
        )

        getallpermissionsets_6=rail.RepliconServiceOperator(
            task_id='getallpermissionsets_6',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        if_request_supervisorid_present_7=rail.IfOperator(
            task_id='if_request_supervisorid_present_7',
            test='''{{ dag_run.conf.supervisorid | is_truthy }}''',
            yes_task="getsupervisordetailsbasedon_employeeid_8",
            no_task="catch_log_error",
        )

        def get_uri_and_status(response,dag_run):
            users_found = response['rows']
            supervisor = {}
            for user in users_found:
                if user['cells'][2]['textValue'] == dag_run.conf['supervisorid']:
                    supervisor = user
                    break
            return {
                'uri': supervisor['cells'][0]['uri'] if supervisor else '',
                'status': supervisor['cells'][1]['textValue'] if supervisor else ''
            }

        getsupervisordetailsbasedon_employeeid_8=rail.RepliconServiceOperator(
            task_id='getsupervisordetailsbasedon_employeeid_8',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:enabled",
                    "urn:replicon:user-list-column:employee-id"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": null,
                    "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                    "leftExpression": null,
                    "operatorUri": null,
                    "rightExpression": null,
                    "value": {
                        "uri": null,
                        "uris": [],
                        "bool": null,
                        "date": null,
                        "money": null,
                        "number": null,
                        "text": "{{ dag_run.conf.supervisorid }}",
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "dateTimeUtc": null
                    },
                    "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            data_handler=get_uri_and_status
        )

        if_log_get_supervisor_uri_11_present_13=rail.IfOperator(
            task_id='if_log_get_supervisor_uri_11_present_13',
            test='''{{ result('getsupervisordetailsbasedon_employeeid_8').uri | is_truthy }}''',
            yes_task="get_assigned_permission_sets_for_supervisor_14",
            no_task="hawaiigas_userimport_logs_prod_search_entries_27",
        )

        get_assigned_permission_sets_for_supervisor_14=rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_supervisor_14',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri":"{{ result('getsupervisordetailsbasedon_employeeid_8').uri }}"
            }
        )

        if_log_checkif_supervisorpermissionisassigned_15_blank_16=rail.IfOperator(
            task_id='if_log_checkif_supervisorpermissionisassigned_15_blank_16',
            test=lambda: not bool(rail.find_first_by_attr_and_get_attr(rail.result(
                'get_assigned_permission_sets_for_supervisor_14'),'policyUri','urn:replicon:policy:supervision','user.uri','')),
            yes_task="get_all_permission_sets_required_uri",
            no_task="update_initial_supervisor_25",
        )

        def get_required_permission_sets_uri():
            assigned_permissions = rail.result('get_assigned_permission_sets_for_supervisor_14')
            required_permissions = [{
                'uri': permission['permissionSet']['uri']
            } for permission in assigned_permissions if permission['policyUri'] != 'urn:replicon:policy:user']
            required_permissions.append({
                'uri':rail.find_first_by_attr_and_get_attr(rail.result('getallpermissionsets_6'),'displayText','Gen3 Supervisor','uri','')
            })
            required_permissions.append({
                'uri':rail.find_first_by_attr_and_get_attr(rail.result('getallpermissionsets_6'),'displayText','Gen3 User - Substitute User Access','uri','')
            })
            return [permission['uri'] for permission in required_permissions if permission['uri'] != '']

        get_all_permission_sets_required_uri=rail.PythonOperator(
            task_id='get_all_permission_sets_required_uri',
            python_callable=get_required_permission_sets_uri
        )

        put_permission_set_assignments_forsupervisorofthe_user_24=rail.RepliconServiceOperator(
            task_id='put_permission_set_assignments_forsupervisorofthe_user_24',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('getsupervisordetailsbasedon_employeeid_8')['uri'],
                "permissionSetUris": rail.result('get_all_permission_sets_required_uri')
            }
        )

        update_initial_supervisor_25=rail.RepliconServiceOperator(
            task_id='update_initial_supervisor_25',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ result('getsupervisordetailsbasedon_employeeid_8').uri }}",
                "dateRange": {
                    "startDate": {
                    "year": "{{ result('get_today_date_obj').year }}",
                    "month": "{{ result('get_today_date_obj').month }}",
                    "day": "{{ result('get_today_date_obj').day }}"
                    },
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        hawaiigas_userimport_logs_prod_search_entries_27=rail.FilterLogEntriesOperator(
            task_id='hawaiigas_userimport_logs_prod_search_entries_27',
            log="{{dag_run.conf.logslookuptable}}",
            properties={
                "employeeid": "{{dag_run.conf.loginname}}",
                "jobid": "{{dag_run.conf.callerjobid}}"
            },
            remove_filtered_entries=True
        )

        load_found_entry = rail.PythonOperator(
            task_id = 'load_found_entry',
            python_callable=lambda: rail.load_all_records(rail.result('hawaiigas_userimport_logs_prod_search_entries_27'))
        )

        if_first_id_present_28=rail.IfOperator(
            task_id='if_first_id_present_28',
            test='''{{ result('hawaiigas_userimport_logs_prod_search_entries_27','length') > 0 }}''',
            yes_task="hawaiigas_userimport_logs_prod_update_entry_29",
            no_task="catch_log_error",
        )

        hawaiigas_userimport_logs_prod_update_entry_29=rail.WriteLogOperator(
            task_id='hawaiigas_userimport_logs_prod_update_entry_29',
            log="{{dag_run.conf.logslookuptable}}",
            message="na",
            severity="na",
            properties=lambda:{
                "employeeid": rail.result('load_found_entry')[0]['properties']['employeeid'],
                "action": rail.result('load_found_entry')[0]['properties']['action'],
                "status": rail.result('load_found_entry')[0]['properties']['status'],
                "details": ((rail.result('load_found_entry')[0]['properties']['details']).split("|"))[0] + "," +
                "Supervisor not updated since Supervisor profile is not available in Replicon|" + "|" +
                ((rail.result('load_found_entry')[0]['properties']['details']).split("|"))[-1] + "," + rail.render_template('{{dag_run_ecid()}}')
            }
        )

        catch_log_error=rail.FilterLogEntriesOperator(
            task_id='catch_log_error',
            trigger_rule='one_failed',
            log="{{dag_run.conf.logslookuptable}}",
            properties={
                "employeeid": "{{dag_run.conf.loginname}}",
                "jobid": "{{dag_run.conf.callerjobid}}"
            },
            remove_filtered_entries=True
        )

        load_foundentry = rail.PythonOperator(
            task_id = 'load_foundentry',
            python_callable=lambda: rail.load_all_records(rail.result('catch_log_error'))
        )

        if_first_id_present_32=rail.IfOperator(
            task_id='if_first_id_present_32',
            test='''{{ result('catch_log_error','length') > 0 }}''',
            yes_task="hawaiigas_userimport_logs_prod_update_entry_33",
            no_task="log_to_sumo",
        )

        hawaiigas_userimport_logs_prod_update_entry_33=rail.WriteLogOperator(
            task_id='hawaiigas_userimport_logs_prod_update_entry_33',
            log="{{dag_run.conf.logslookuptable}}",
            message="na",
            severity="Error",
            properties=lambda:{
                "employeeid": rail.result('load_foundentry')[0]['properties']['employeeid'],
                "action": rail.result('load_foundentry')[0]['properties']['action'],
                "status": "Error",
                "details": ((rail.result('load_foundentry')[0]['properties']['details']).split("|"))[0] + "," +
                "Supervisor not updated,{{get_error_message()}}|" + ((rail.result('load_foundentry')[0]['properties']['details']).split("|"))[-1] + "," +
                rail.render_template('{{dag_run_ecid()}}'),
                "jobid": rail.result('load_foundentry')[0]['properties']['jobid'],
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_log_error
        can_run_batch_task >> rail.Label('No') >> get_today_date_obj
        get_today_date_obj >> getallpermissionsets_6 >> if_request_supervisorid_present_7
        if_request_supervisorid_present_7 >> rail.Label('Yes')  >> getsupervisordetailsbasedon_employeeid_8 >> if_log_get_supervisor_uri_11_present_13
        if_log_get_supervisor_uri_11_present_13 >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_supervisor_14 >> if_log_checkif_supervisorpermissionisassigned_15_blank_16
        if_log_checkif_supervisorpermissionisassigned_15_blank_16 >> rail.Label(
            'Yes') >> get_all_permission_sets_required_uri >> put_permission_set_assignments_forsupervisorofthe_user_24 >> update_initial_supervisor_25
        if_log_checkif_supervisorpermissionisassigned_15_blank_16 >> rail.Label('No') >> update_initial_supervisor_25 >> catch_log_error
        if_log_get_supervisor_uri_11_present_13 >> rail.Label(
            'No') >> hawaiigas_userimport_logs_prod_search_entries_27 >> load_found_entry >> if_first_id_present_28
        if_first_id_present_28 >> rail.Label('Yes')  >> hawaiigas_userimport_logs_prod_update_entry_29 >> catch_log_error
        if_first_id_present_28 >> rail.Label('No') >> catch_log_error
        if_request_supervisorid_present_7 >> rail.Label('No') >> catch_log_error >> load_foundentry >> if_first_id_present_32
        if_first_id_present_32 >> rail.Label('Yes')  >> hawaiigas_userimport_logs_prod_update_entry_33 >> log_to_sumo
        if_first_id_present_32 >> rail.Label('No') >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
