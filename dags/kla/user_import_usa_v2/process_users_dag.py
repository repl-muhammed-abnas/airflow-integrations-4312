
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'kla_user_import_usa_process_users_v2_{config.instance}',
        description=f'KLATencor User Import USA process Users v2 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_log',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        get_user_databasedonemployeeid = rail.RepliconServiceOperator(
            task_id='get_user_databasedonemployeeid',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:user"
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
                            "text": "{{ dag_run.conf.employeeid }}",
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
            data_handler=lambda data: list(filter(lambda x: x['empid'] == rail.get_current_context()['dag_run'].conf['employeeid'],
                                                  map(lambda x: {
                                                      "empid": x['cells'][0].get('textValue'),
                                                      "uri": x['cells'][1].get('uri')
                                                  }, data['rows'])))
        )

        has_user_in_replicon = rail.IfOperator(
            task_id='has_user_in_replicon',
            test="{{ result('get_user_databasedonemployeeid') | is_truthy }}",
            yes_task="process_update_user",
            no_task="has_active_user",
        )

        process_update_user = rail.TriggerDagRunForEachItemOperator(
            task_id='process_update_user',
            retries=0,
            items=[1],
            trigger_dag_id=f'kla_user_import_usa_update_rehire_disable_user_v2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf={
                "log": "{{result('create_log')}}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}",
                "employeetype": "{{ dag_run.conf.employee_type }}",
                "department": "{{ dag_run.conf.deptname + '-' + dag_run.conf.deptid }}",
                "location": "{{ dag_run.conf.campusstate }}",
                "useruri": "{{ result('get_user_databasedonemployeeid')[0].uri }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "lasthiredate2": "{{ dag_run.conf.lasthiredate2 }}",
                "supervisorid": "{{ dag_run.conf.supervisorid }}",
                "costcenter": "{{ dag_run.conf.costcenter }}",
                "company": "{{ dag_run.conf.company }}",
                "empl_status": "{{ dag_run.conf.empl_status }}",
                "loginname": "{{ dag_run.conf.email }}",
                "startdate": "{{ dag_run.conf.lasthiredate1 }}",
                "returntoworkdate": "{{ dag_run.conf.leave_end_date }}",
                "enddate": "{{ dag_run.conf.terminationdate }}",
                "emailaddress": "{{ dag_run.conf.email }}",
                "lastrecordupdate": "{{ dag_run.conf.last_upd_dt }}",
                "firstdayofleave": "{{ dag_run.conf.leave_begin_date }}",
                "pdrcountry": "{{ dag_run.conf.campuscountry }}",
                "hasusdirectreport": "{{ dag_run.conf.hasusadirectreports }}",
                "wfn_id": "{{ dag_run.conf | attr_or_default('wfn_id', '') }}",
            }
        )

        wait_for_process_update_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_update_user',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("process_update_user") }}'
        )

        has_active_user = rail.IfOperator(
            task_id='has_active_user',
            test="{{ dag_run.conf.empl_status == 'Active' }}",
            yes_task="process_add_user",
            no_task="add_disabled_user_log",
        )

        process_add_user = rail.TriggerDagRunForEachItemOperator(
            task_id='process_add_user',
            retries=0,
            items=[1],
            trigger_dag_id=f'kla_user_import_usa_add_user_v2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf={
                "log": "{{ result('create_log') }}",
                "firstname": "{{ dag_run.conf.firstname }}",
                "lastname": "{{ dag_run.conf.lastname }}",
                "employeetype": "{{ dag_run.conf.employee_type }}",
                "department": "{{ dag_run.conf.deptname + '-' + dag_run.conf.deptid }}",
                "location": "{{ dag_run.conf.campusstate }}",
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "loginname": "{{ dag_run.conf.email }}",
                "lasthiredate2": "{{ dag_run.conf.lasthiredate2 }}",
                "supervisorid": "{{ dag_run.conf.supervisorid }}",
                "costcenter": "{{ dag_run.conf.costcenter }}",
                "company": "{{ dag_run.conf.company }}",
                "startdate": "{{ dag_run.conf.lasthiredate1 }}",
                "returntoworkdate": "{{ dag_run.conf.leave_end_date }}",
                "firstdayofleave": "{{ dag_run.conf.leave_begin_date }}",
                "enddate": "{{ dag_run.conf.terminationdate }}",
                "emailaddress": "{{ dag_run.conf.email }}",
                "lastrecordupdate": "{{ dag_run.conf.last_upd_dt }}",
                "pdrcountry": "{{ dag_run.conf.campuscountry }}",
                "userstatus": "{{ dag_run.conf.empl_status }}",
                "hasusdirectreport": "{{ dag_run.conf.hasusadirectreports }}",
                "wfn_id": "{{ dag_run.conf | attr_or_default('wfn_id', '') }}",
            }
        )

        wait_for_process_add_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_add_user',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("process_add_user") }}'
        )

        add_disabled_user_log = rail.WriteLogOperator(
            task_id='add_disabled_user_log',
            log="{{ result('create_log') }}",
            message="User is not present in Replicon and is in terminated status in PDR system.",
            severity='Exception',
            properties={
                "loginname": "{{dag_run.conf.email}}",
                "action": "Ignored",
                "status": "Exception",
                "message": "User is not present in Replicon and is in terminated status in PDR system."
            }
        )

        get_supervisor_assignment = rail.GatherResultsFromDagRunsOperator(
            task_id='get_supervisor_assignment',
            dag_runs="{{ result('process_add_user') or result('process_update_user') }}",
            dagrun_task_id='queue_supervisor_assignment',
            flatten=True,
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> create_log
        create_log >> get_user_databasedonemployeeid >> has_user_in_replicon
        has_user_in_replicon >> rail.Label(
            'yes') >> process_update_user >> wait_for_process_update_user >> get_supervisor_assignment >> log_to_sumo
        has_user_in_replicon >> rail.Label('no') >> has_active_user
        has_active_user >> rail.Label(
            'yes') >> process_add_user >> wait_for_process_add_user >> get_supervisor_assignment >> log_to_sumo
        has_active_user >> rail.Label(
            'no') >> add_disabled_user_log >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
