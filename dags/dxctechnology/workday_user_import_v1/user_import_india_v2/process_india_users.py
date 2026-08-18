from datetime import timedelta
from pendulum import datetime
import pendulum
import rail
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable


null = None 
DATE_FORMAT = "%Y-%d-%m"
TIMEZONE = 'America/Los_Angeles'

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.india_process_users_child_dag_id,
        description="DXC Workday User Import iNDIA - Process Each Users",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.max_active_run_process_each_users_india
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name_india, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="create_user_log"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="create_user_log",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )
        
        create_user_log = rail.CreateLogOperator(
            task_id = "create_user_log"
        )

        def get_user_details_payload():
            return {
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:enabled"
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
                        "text": "{{ dag_run.conf.file_data.emp_id }}",
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
                }
                }
        
        def get_value(data, index, pluck_key):
            return data['cells'][index].get(pluck_key)

        def get_filtered_user_data(response,dag_run):
            return list(filter(lambda x: bool(x['employeeid']) and x['employeeid'] == dag_run.conf['file_data']['emp_id'], map(lambda row: {
                "name": get_value(row, 0, 'textValue'),
                'loginname': get_value(row, 1, 'textValue'),
                "uri": get_value(row, 0, 'uri'),
                "employeeid": get_value(row, 2, 'textValue'),
                "status": get_value(row, 3, 'textValue')
            }, response['rows'])))

        get_user_details_via_emp_id = rail.RepliconServiceOperator(
            task_id = "get_user_details_via_emp_id",
            endpoint="/services/UserListService1.svc/GetData",
            data = get_user_details_payload(),
            data_handler= lambda response, dag_run:get_filtered_user_data(response, dag_run)
        )

        is_user_found = rail.IfOperator(
            task_id = "is_user_found",
            test= "{{ result('get_user_details_via_emp_id') | is_truthy}}",
            yes_task="is_multiple_users_found",
            no_task="trigger_add_user"
        )

        is_multiple_users_found = rail.IfOperator(
            task_id = "is_multiple_users_found",
            test= lambda: len(rail.result('get_user_details_via_emp_id'))>1,
            yes_task="log_multiple_users_found",
            no_task="trigger_update_user"
        )

        log_multiple_users_found = rail.WriteLogOperator(
            task_id = "log_multiple_users_found",
            log="{{result('create_user_log')}}",
            message = lambda dag_run :f'''Multiple users available with employee id "{dag_run.conf['file_data']['emp_id']}"''',
            severity = "Exception",
            properties = lambda dag_run:{
                "Jobid": "",
                "Userid": dag_run.conf['file_data']['emp_id'],
                "Email": dag_run.conf['file_data']['email_id'],
                "Action": "Update",
                "Status": "Exception",
                "Details": f'''Multiple users available with employee id "{dag_run.conf['file_data']['emp_id']}"'''
            }
        )

        def get_work_week_date(timezone, work_week, return_format= "str"):
            # workato wday function -> (sunday as 0 to saturday as 6) -> range(0 to 6)
            # pendulum day_of_week function - > (monday as 1 to sunday as 7) -> range(1 to 7)
            # Comparision in above both function -> only value for sunday is different (workato->0 and pendulum->7)

            def get_required_days_to_subtract():
                if pendulum.now(timezone).day_of_week==7:
                    if work_week.lower().split(" ")[0] == "saturday":
                        return 1
                    if work_week.lower().split(" ")[0] == "sunday":
                        return 0
                    return 7

                elif pendulum.now(timezone).day_of_week==1:
                        if work_week.lower().split(" ")[0] == "saturday":
                            return 2
                        if work_week.lower().split(" ")[0] == "sunday":
                            return 1
                        return 0
                
                elif pendulum.now(timezone).day_of_week==2:
                    if work_week.lower().split(" ")[0] == "saturday":
                        return 3
                    if work_week.lower().split(" ")[0] == "sunday":
                        return 2
                    return 1
                
                elif pendulum.now(timezone).day_of_week==3:
                    if work_week.lower().split(" ")[0] == "saturday":
                        return 4
                    if work_week.lower().split(" ")[0] == "sunday":
                        return 3
                    return 2
                
                elif pendulum.now(timezone).day_of_week==4:
                    if work_week.lower().split(" ")[0] == "saturday":
                        return 5
                    if work_week.lower().split(" ")[0] == "sunday":
                        return 4
                    return 3
                
                elif pendulum.now(timezone).day_of_week==5:
                    if work_week.lower().split(" ")[0] == "saturday":
                        return 6
                    if work_week.lower().split(" ")[0] == "sunday":
                        return 5
                    return 4
                
                else :
                    if work_week.lower().split(" ")[0] == "saturday":
                        return 0
                    if work_week.lower().split(" ")[0] == "sunday":
                        return 6
                    return 5
            
            result = ((pendulum.now(timezone).date()).subtract(days=get_required_days_to_subtract()))
            if return_format == "dict":
                return {
                    "day": result.day,
                    "month": result.month,
                    "year": result.year
                }
            return result.strftime(DATE_FORMAT)
        
        def get_trigger_update_user_conf(dag_run):
            _work_week_date = get_work_week_date(TIMEZONE, dag_run.conf['mapper_data']['workweek'], return_format="dict")
            run_conf = dag_run.conf
            run_conf['json_formatted_dates']['work_week_date'] = _work_week_date
            return {
                **{
                    "user_uri": rail.result('get_user_details_via_emp_id')[0]['uri'],
                    "loginname": rail.result('get_user_details_via_emp_id')[0]['loginname'],
                    "replicon_field": "true" if dag_run.conf['file_data']['status'] == "1" else "false",
                    "location": (dag_run.conf['file_data']['country']+ "/"+ dag_run.conf['file_data']['state']) if dag_run.conf['file_data']['state'] else dag_run.conf['file_data']['country'],
                    'todays_date': (pendulum.now('America/Los_Angeles')).strftime(DATE_FORMAT),
                    "work_week_date": f"{_work_week_date['year']}-{_work_week_date['day']}-{_work_week_date['month']}",
                    "user_log": rail.result('create_user_log')
                },
                **run_conf
            }

        trigger_update_user = rail.TriggerDagRunOperator(
            task_id = "trigger_update_user",
            trigger_dag_id=config.india_update_user_dag_id,
            conf=get_trigger_update_user_conf
        )

        wait_for_update_user_completion = rail.WaitForDagRunsSensor(
            task_id = "wait_for_update_user_completion",
            dag_runs="{{result('trigger_update_user')}}",
            execution_timeout = timedelta(days=config.execution_timeout_days)
        )

        def get_trigger_add_user_conf(dag_run):
            _work_week_date = get_work_week_date(TIMEZONE, dag_run.conf['mapper_data']['workweek'], return_format="dict")
            run_conf = dag_run.conf
            run_conf['json_formatted_dates']['work_week_date'] = _work_week_date
            return {
                **{
                   "replicon_field": "true" if dag_run.conf['file_data']['status'] == "1" else "false",
                    'todays_date': (pendulum.now('America/Los_Angeles')).strftime(DATE_FORMAT),
                    "work_week_date": f"{_work_week_date['year']}-{_work_week_date['day']}-{_work_week_date['month']}",
                    "user_log": rail.result('create_user_log')
                },
                **run_conf
            }

        trigger_add_user = rail.TriggerDagRunOperator(
            task_id = "trigger_add_user",
            trigger_dag_id=config.india_add_user_dag_id,
            conf=get_trigger_add_user_conf
        )

        wait_for_add_user_completion = rail.WaitForDagRunsSensor(
            task_id = "wait_for_add_user_completion",
            dag_runs="{{result('trigger_add_user')}}",
            execution_timeout = timedelta(days=config.execution_timeout_days)
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            trigger_rule = "one_failed",
            log="{{result('create_user_log')}}",
            message = "User processing Error",
            severity = "Error",
            properties = lambda dag_run: {                
                # WriteLogOperator ecid has ecid | run_id
                "Jobid": "",
                "Userid": dag_run.conf['file_data']['emp_id'],
                "Email": dag_run.conf['file_data']['email_id'],
                "Action": "Add",
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> create_user_log

        create_user_log >> get_user_details_via_emp_id >> is_user_found
        is_user_found >> rail.Label("Yes") >> is_multiple_users_found >> rail.Label('Yes') >> log_multiple_users_found >> catch_and_log_error
        is_multiple_users_found >> rail.Label('No') >> trigger_update_user
        
        trigger_update_user >> wait_for_update_user_completion >> catch_and_log_error
        is_user_found >> rail.Label("No") >> trigger_add_user >> wait_for_add_user_completion >> catch_and_log_error

    return dag
    
rail.for_each_instance(create_dag)
