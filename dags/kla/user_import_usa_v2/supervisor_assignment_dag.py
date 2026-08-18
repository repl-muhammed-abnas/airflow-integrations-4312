
from datetime import datetime, timedelta
import pytz

from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'kla_user_import_usa_supervisor_assignment_v2_{config.instance}',
        description=f'KLATencor Supervisor Assignment V2 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        def get_conf():
            return rail.get_current_context()['dag_run'].conf

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_message_today'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_message_today',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_message_today = rail.PythonOperator(
            task_id='log_message_today',
            python_callable=lambda: {
                'year': datetime.now(tz=pytz.UTC).year,
                'month': datetime.now(tz=pytz.UTC).month,
                'day': datetime.now(tz=pytz.UTC).day,
            }
        )

        get_all_permissionsets = rail.RepliconServiceOperator(
            task_id='get_all_permissionsets',
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
        )

        log_message_urifor_managers_supervisor = rail.PythonOperator(
            task_id='log_message_urifor_managers_supervisor',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_permissionsets'), 'displayText', 'Manager\'s Supervisor', 'uri')
        )

        log_message_urifor_manager_basic_user = rail.PythonOperator(
            task_id='log_message_urifor_manager_basic_user',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_permissionsets'), 'displayText', 'Manager Basic User', 'uri')
        )

        has_supervisor_id = rail.IfOperator(
            task_id='has_supervisor_id',
            test="{{ dag_run.conf.supervisorid | is_truthy }}",
            yes_task="getsupervisordetailsbasedon_employeeid",
            no_task='finish'
        )

        getsupervisordetailsbasedon_employeeid = rail.RepliconServiceOperator(
            task_id='getsupervisordetailsbasedon_employeeid',
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
            data_handler=lambda data: next(iter(filter(lambda x: x['employeeid'] == get_conf()['supervisorid'],
                                                       map(lambda x: {
                                                           "employeeid": x['cells'][2].get('textValue'),
                                                           "uri": x['cells'][0].get('uri'),
                                                           "status": x['cells'][1].get('textValue'),
                                                       }, data['rows']))), None)


        )

        has_supervisor_uri = rail.IfOperator(
            task_id='has_supervisor_uri',
            test="{{ result('getsupervisordetailsbasedon_employeeid') | is_truthy and result('getsupervisordetailsbasedon_employeeid').uri != dag_run.conf.useruri }}",
            yes_task="get_assigned_permission_sets_for_supervisor",
            no_task='add_supervisor_not_found_log',
        )

        get_assigned_permission_sets_for_supervisor = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_supervisor',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('getsupervisordetailsbasedon_employeeid').uri }}"
            }
        )

        log_message_checkif_supervisorpermissionisassigned = rail.PythonOperator(
            task_id='log_message_checkif_supervisorpermissionisassigned',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(get_assigned_permission_sets_for_supervisor.task_id), 'policyUri',
                                                                         'urn:replicon:policy:supervision')
        )

        has_no_supervisor_permission = rail.IfOperator(
            task_id='has_no_supervisor_permission',
            test="{{ result('log_message_checkif_supervisorpermissionisassigned') | is_falsy }}",
            yes_task="get_all_non_user_permission_policy",
            no_task="update_initial_supervisor",
        )

        get_all_non_user_permission_policy = rail.PythonOperator(
            task_id='get_all_non_user_permission_policy',
            python_callable=lambda: list(map(lambda x: x['permissionSet']['uri'],
                                             filter(lambda x: x['policyUri'] != 'urn:replicon:policy:user',
                                                    rail.result('get_assigned_permission_sets_for_supervisor'))))
        )

        log_message_new_permission_setforsupervisor = rail.PythonOperator(
            task_id='log_message_new_permission_setforsupervisor',
            python_callable=lambda: [rail.result('log_message_urifor_managers_supervisor'), rail.result(
                'log_message_urifor_manager_basic_user')]
        )

        log_message_permission_setsfor_user = rail.PythonOperator(
            task_id='log_message_permission_setsfor_user',
            python_callable=lambda:  rail.result('get_all_non_user_permission_policy') +
            rail.result('log_message_new_permission_setforsupervisor')
        )

        put_permission_set_assignments_for_supervisorofthe_user = rail.RepliconServiceOperator(
            task_id='put_permission_set_assignments_for_supervisorofthe_user',
            endpoint="/services/PermissionSetService1.svc/PutPermissionSetAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('getsupervisordetailsbasedon_employeeid')['uri'],
                "permissionSetUris": rail.result('log_message_permission_setsfor_user')

            }
        )

        update_initial_supervisor = rail.RepliconServiceOperator(
            task_id='update_initial_supervisor',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda: {
                "userUri": get_conf()['useruri'],
                "supervisorUri": rail.result('getsupervisordetailsbasedon_employeeid')['uri'],
                "dateRange": {
                    "startDate": rail.result('log_message_today'),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        add_supervisor_not_found_log = rail.WriteLogOperator(
            task_id='add_supervisor_not_found_log',
            log="{{ dag_run.conf.log }}",
            message="Supervisor not assigned, since Supervisor is not present in Replicon",
            severity="Exception",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "action": "Supervisor Assignment |{{dag_run.conf.supervisorid }}",
                "status": "Exception",
                "message": "Supervisor not assigned, since Supervisor is not present in Replicon"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.log }}",
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "action": "Supervisor Assignment |{{dag_run.conf.supervisorid }}",
                'status': 'Error',
                'message': '{{ get_error_message() }}',

            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> log_message_today

        log_message_today >> get_all_permissionsets >> log_message_urifor_managers_supervisor >> log_message_urifor_manager_basic_user >> has_supervisor_id

        has_supervisor_id >> rail.Label(
            'Yes') >> getsupervisordetailsbasedon_employeeid >> has_supervisor_uri
        has_supervisor_id >> rail.Label(
            'No') >> finish

        has_supervisor_uri >> rail.Label(
            'No') >> add_supervisor_not_found_log >> finish
        has_supervisor_uri >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_supervisor >> log_message_checkif_supervisorpermissionisassigned >> has_no_supervisor_permission
        has_no_supervisor_permission >> rail.Label(
            'Yes') >> get_all_non_user_permission_policy >> log_message_new_permission_setforsupervisor >> log_message_permission_setsfor_user >> put_permission_set_assignments_for_supervisorofthe_user >> update_initial_supervisor
        has_no_supervisor_permission >> rail.Label(
            'No') >> update_initial_supervisor
        update_initial_supervisor >> finish

        finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
