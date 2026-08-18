
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.update_task_child_dagid,
        description=f'VJTechnologies_{config.entity_name}_Update_Task_child_{config.instance}',
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
            no_task='create_timeentrystartdatetoapply_variable'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_timeentrystartdatetoapply_variable',
            end_task='catch_and_return_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_timeentrystartdatetoapply_variable=rail.SetVariableOperator(
            task_id='create_timeentrystartdatetoapply_variable',
            append=False,
            name='timeentrystartdatetoapply',
            value=None
        )

        create_timeentryenddatetoapply_variable=rail.SetVariableOperator(
            task_id='create_timeentryenddatetoapply_variable',
            append=False,
            name='timeentryenddatetoapply',
            value=None
        )

        create_estimatedhours_variable=rail.SetVariableOperator(
            task_id='create_estimatedhours_variable',
            append=False,
            name='estimatedhours',
            value=None
        )

        def get_task_daterange(dag_run):
            taskstartdate = datetime.strptime(dag_run.conf['taskstartdate'],'%Y/%m/%d') if dag_run.conf['taskstartdate'] else null
            taskenddate = datetime.strptime(dag_run.conf['taskenddate'],'%Y/%m/%d') if dag_run.conf['taskenddate'] else null
            print(taskstartdate)
            return {
                "taskstartdateday": taskstartdate.day if taskstartdate else null,
                "taskstartdatemonth": taskstartdate.month if taskstartdate else null,
                "taskstartdateyear": taskstartdate.year if taskstartdate else null,
                "taskenddateday": taskenddate.day if taskenddate else null,
                "taskenddatemonth": taskenddate.month if taskenddate else null,
                "taskenddateyear": taskenddate.year if taskenddate else null
            }

        get_task_date_range=rail.PythonOperator(
            task_id='get_task_date_range',
            python_callable=get_task_daterange
        )

        get_task_name=rail.PythonOperator(
            task_id='get_task_name',
            python_callable= lambda dag_run: dag_run.conf['taskname'] + '-' + dag_run.conf['taskcode']
        )

        if_estimatedefforthours_present=rail.IfOperator(
            task_id='if_estimatedefforthours_present',
            test='''{{ dag_run.conf.estimatedefforthours | is_truthy }}''',
            yes_task="convert_decimal_into_hours_minutes",
            no_task="if_task_startdate_present",
        )

        def get_hours_minutes_from_decimal(dag_run):
            hours = int(((dag_run.conf['estimatedefforthours']).split('.'))[0])
            minutes = 60 * float('0.' + ((dag_run.conf['estimatedefforthours']).split('.'))[-1]) if len(
                ((dag_run.conf['estimatedefforthours']).split('.'))) > 1 else 0
            final_minutes = minutes if len(str(minutes).split('.')) == 1 else str(minutes).split('.', maxsplit=1)[0]
            seconds = 60 * float('0.' + str(minutes).rsplit('.', maxsplit=1)[-1]) if len(str(minutes).split('.')) > 1 else 0
            final_seconds = seconds if len(str(seconds).split('.')) == 1 else str(seconds).split('.', maxsplit=1)[0]
            return {
                'hours': hours,
                'minutes': str(final_minutes).rjust(2,'0'),
                'seconds': str(final_seconds).rjust(2,'0')
            }

        convert_decimal_into_hours_minutes=rail.PythonOperator(
            task_id='convert_decimal_into_hours_minutes',
            python_callable=get_hours_minutes_from_decimal
        )

        update_estimatedhours_variable=rail.SetVariableOperator(
            task_id='update_estimatedhours_variable',
            append=False,
            name='{{ result("create_estimatedhours_variable").name }}',
            value={
                "duration": {
                    "hours": "{{ result('convert_decimal_into_hours_minutes').hours }}",
                    "minutes": "{{ result('convert_decimal_into_hours_minutes').minutes }}",
                    "seconds": "{{ result('convert_decimal_into_hours_minutes').seconds }}"
                }
            }
        )

        if_task_startdate_present=rail.IfOperator(
            task_id='if_task_startdate_present',
            test='''{{ dag_run.conf.taskstartdate | is_truthy }}''',
            yes_task="update_timeentrystartdatetoapply_variable",
            no_task="if_task_enddate_present",
        )

        update_timeentrystartdatetoapply_variable=rail.SetVariableOperator(
            task_id='update_timeentrystartdatetoapply_variable',
            append=False,
            name='{{ result("create_timeentrystartdatetoapply_variable").name }}',
            value=lambda: {
                    "date": {
                    "year": rail.result('get_task_date_range')['taskstartdateyear'],
                    "month": rail.result('get_task_date_range')['taskstartdatemonth'],
                    "day": rail.result('get_task_date_range')['taskstartdateday']
                }
            }
        )

        if_task_enddate_present=rail.IfOperator(
            task_id='if_task_enddate_present',
            test='''{{ dag_run.conf.taskenddate | is_truthy }}''',
            yes_task="update_timeentryenddatetoapply_variable",
            no_task="create_task_or_apply_modifications",
        )

        update_timeentryenddatetoapply_variable=rail.SetVariableOperator(
            task_id='update_timeentryenddatetoapply_variable',
            append=False,
            name='{{ result("create_timeentryenddatetoapply_variable").name }}',
            value=lambda: {
                    "date": {
                    "year": rail.result('get_task_date_range')['taskenddateyear'],
                    "month": rail.result('get_task_date_range')['taskenddatemonth'],
                    "day": rail.result('get_task_date_range')['taskenddateday']
                }
            }
        )

        create_task_or_apply_modifications=rail.RepliconServiceOperator(
            task_id='create_task_or_apply_modifications',
            endpoint="/services/TaskService1.svc/CreateTaskOrApplyModifications",
            data=lambda dag_run:{
            "target": {
                "uri": dag_run.conf['taskuri'],
                "name": null,
                "parent": null,
                "parameterCorrelationId": null
            },
            "project": {
                "uri": dag_run.conf['projecturi'],
                "name": null,
                "code": null,
                "parameterCorrelationId": null
            },
            "modifications": {
                "name": rail.result('get_task_name'),
                "codeToApply":null,
                "descriptionToApply": {
                "value": dag_run.conf['taskdescription']
                },
                "isClosed": "false",
                "timeEntryStartDateToApply": rail.get_dag_run_var('timeentrystartdatetoapply'),
                "timeEntryEndDateToApply": rail.get_dag_run_var('timeentryenddatetoapply'),
                "timeAndExpenseEntryTypeToApply": null,
                "isTimeEntryAllowed": "true",
                "costTypeToApply": null,
                "estimatedHoursToApply": rail.get_dag_run_var('estimatedhours'),
                "estimatedCostToApply": null,
                "resourceAssignmentModifications": null,
                "customFieldsToApply": [],
                "keyValuesToApply": [],
                "objectExtensionFieldsToApply": []
            },
            "unitOfWorkId": rail.render_template("{{ dag_run_ecid() }} _task")
            }
        )

        if_resourceuris_present=rail.IfOperator(
            task_id='if_resourceuris_present',
            test='''{{ dag_run.conf.resourceuris | is_truthy }}''',
            yes_task="get_current_resource_assignments",
            no_task="catch_and_return_error",
        )

        get_current_resource_assignments=rail.RepliconServiceOperator(
            task_id='get_current_resource_assignments',
            endpoint="/services/TaskService1.svc/BulkGetResourceAssignments",
            data=lambda dag_run:{
                "taskUris": [dag_run.conf['taskuri']],
                "asOfDate": null
            }
        )

        get_resources_to_remove=rail.PythonOperator(
            task_id='get_resources_to_remove',
            python_callable= lambda: ([assignment['resource']['uri'] for assignment in rail.result('get_current_resource_assignments')[0]['assignments']]) if
                                (rail.result('get_current_resource_assignments') and rail.result('get_current_resource_assignments')[0]['assignments'] and
                                rail.result('get_current_resource_assignments')[0]['assignments'][0]['resource'] and
                                rail.result('get_current_resource_assignments')[0]['assignments'][0]['resource']['uri']) else null
        )

        if_resources_to_remove_present=rail.IfOperator(
            task_id='if_resources_to_remove_present',
            test='''{{ result('get_resources_to_remove') | is_truthy }}''',
            yes_task="remove_project_team_members",
            no_task="update_resource_assignments",
        )

        remove_project_team_members=rail.RepliconServiceOperator(
            task_id='remove_project_team_members',
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            data=lambda dag_run: {
                "taskUri": dag_run.conf['taskuri'],
                "resourceUris": rail.result('get_resources_to_remove'),
                "isAssigned": "false"
            }
        )

        update_resource_assignments=rail.RepliconServiceOperator(
            task_id='update_resource_assignments',
            endpoint="/services/TaskService1.svc/BulkUpdateResourceAssignments",
            data=lambda dag_run:{
                "taskUri": dag_run.conf['taskuri'],
                "resourceUris": dag_run.conf['resourceuris'],
                "isAssigned": "true"
            }
        )

        catch_and_return_error=rail.PythonOperator(
            task_id='catch_and_return_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template('{{get_error_message}}')
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_return_error
        can_run_batch_task >> rail.Label('No') >> create_timeentrystartdatetoapply_variable
        create_timeentrystartdatetoapply_variable >> create_timeentryenddatetoapply_variable >> create_estimatedhours_variable >> get_task_date_range
        get_task_date_range >> get_task_name >> if_estimatedefforthours_present
        if_estimatedefforthours_present >> rail.Label(
            'Yes') >> convert_decimal_into_hours_minutes >> update_estimatedhours_variable >> if_task_startdate_present
        if_estimatedefforthours_present >> rail.Label('No') >> if_task_startdate_present
        if_task_startdate_present >> rail.Label('Yes')  >> update_timeentrystartdatetoapply_variable >> if_task_enddate_present
        if_task_startdate_present >> rail.Label('No') >> if_task_enddate_present
        if_task_enddate_present >> rail.Label('Yes')  >> update_timeentryenddatetoapply_variable >> create_task_or_apply_modifications
        if_task_enddate_present >> rail.Label('No') >> create_task_or_apply_modifications >> if_resourceuris_present
        if_resourceuris_present >> rail.Label('Yes')  >> get_current_resource_assignments >> get_resources_to_remove >> if_resources_to_remove_present
        if_resources_to_remove_present >> rail.Label('Yes')  >> remove_project_team_members >> update_resource_assignments
        if_resources_to_remove_present >> rail.Label('No') >> update_resource_assignments >> catch_and_return_error
        if_resourceuris_present >> rail.Label('No') >> catch_and_return_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
