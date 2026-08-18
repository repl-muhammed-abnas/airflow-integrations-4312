
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'momentive_annual_leave_policy_update_south_korea_master_{config.instance}',
        description=f'Momentive Anual Leave Policy Update_KOR Master{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_all_divisions'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_divisions',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_divisions=rail.RepliconServiceOperator(
            task_id='get_all_divisions',
            endpoint="/services/DivisionService1.svc/GetAllDivisions",
        )

        get_all_timeoff_types=rail.RepliconServiceOperator(
            task_id='get_all_timeoff_types',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
        )

        get_timeoff_and_division_uris=rail.PythonOperator(
            task_id='get_timeoff_and_division_uris',
            python_callable= lambda: {
                'timeoffuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_timeoff_types'),'displayText','KOR_Annual Leave 연차휴가','uri',''),
                'divisionuri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_divisions'),
                                'displayText','MOMENTIVE PERFORMANCE MATERIALS KOREA CO., LTD.','uri','')
            }
        )

        if_timeoff_or_division_uri_not_present=rail.IfOperator(
            task_id='if_timeoff_or_division_uri_not_present',
            test='''{{ result('get_timeoff_and_division_uris').timeoffuri | is_falsy  or result('get_timeoff_and_division_uris').divisionuri | is_falsy }}''',
            yes_task="stop_with_error",
            no_task="get_all_users_tobe_processed",
        )

        stop_with_error=rail.FailOperator(
            task_id='stop_with_error',
            message='''Timeoff URI or Division (Legal Entity uri) is not present'''
        )

        get_all_users_tobe_processed=rail.RepliconServiceOperator(
            task_id='get_all_users_tobe_processed',
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:enabled",
                    "urn:replicon:user-list-column:start-date"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:division"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                        "uri": "{{ result('get_timeoff_and_division_uris').divisionuri }}",
                        "uris": [],
                        "bool": null,
                        "date": null,
                        "money": null,
                        "number": null,
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "dateTimeUtc": null,
                        "dateTimeUtcRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:time-off-type"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                        "uri": "{{ result('get_timeoff_and_division_uris').timeoffuri }}",
                        "uris": [],
                        "bool": null,
                        "date": null,
                        "money": null,
                        "number": null,
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "dateTimeUtc": null,
                        "dateTimeUtcRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            data_handler= lambda response: response['rows']
        )

        def create_user_list():
            users = rail.result('get_all_users_tobe_processed')
            user_list = []
            user_list = [ {
                'name': user['cells'][0]['textValue'],
                'status': user['cells'][1]['textValue'],
                'uri': user['cells'][0]['uri'],
                'startdate': user['cells'][2]['textValue'],
                'tenure': round((datetime(((datetime.today() - timedelta(days=1)) + relativedelta(months = 12)).year,1,1).date() -
                            datetime(datetime.strptime(user['cells'][2]['textValue'],"%Y/%m/%d").year,1,1).date()).days/365,2)
            } for user in users]
            return user_list

        get_user_list=rail.PythonOperator(
            task_id='get_user_list',
            python_callable= create_user_list
        )

        create_userlist_collection = rail.CreateCollectionOperator(
            task_id='create_userlist_collection',
            source = lambda: rail.result('get_user_list'),
            name = "userlist",
        )

        query_eligible_users=rail.QueryCollectionOperator(
            task_id='query_eligible_users',
            query="""SELECT * FROM  userlist WHERE  userlist.status='True' AND  userlist.tenure > 1.99""",
        )

        process_each_user=rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_user',
            retries=0,
            items="{{ result('query_eligible_users') }}",
            trigger_dag_id=f'momentive_annual_leave_policy_update_south_korea_child_dag_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf={
                "useruri": "{{ item.uri }}",
                "timeoffuri": "{{ result('get_timeoff_and_division_uris').timeoffuri }}",
                "tenure": "{{ item.tenure }}",
                "startdate": "{{ item.startdate }}",
                "username": "{{ item.name }}"
            }
        )

        wait_for_process_each_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_each_user") }}'
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> get_all_divisions
        get_all_divisions >> get_all_timeoff_types >> get_timeoff_and_division_uris >> if_timeoff_or_division_uri_not_present
        if_timeoff_or_division_uri_not_present >> rail.Label('Yes')  >> stop_with_error >> log_to_sumo
        if_timeoff_or_division_uri_not_present >> rail.Label('No') >> get_all_users_tobe_processed >> get_user_list >> create_userlist_collection
        create_userlist_collection >> query_eligible_users >> process_each_user >> wait_for_process_each_user >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
