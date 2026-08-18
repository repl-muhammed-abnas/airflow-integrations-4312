from datetime import timedelta
from json import dumps, loads
from pendulum import datetime
import rail
from airflow.models import Variable
from dxctechnology.workday_user_import_v1.user_import_global.utils import request_payload


# Non- Canada 
def create_add_user_timeoff_assignment_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.usa_lse_add_user_timeoff_assignment_dag_id,
        description="dxctechnology workday user sync process users child",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=10
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
            config.can_run_batch_task_var_name_usa_les, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="set_variable_to_store_run_id"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="set_variable_to_store_run_id",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )

        set_variable_to_store_run_id = rail.GetVariableOperator(
            task_id = "set_variable_to_store_run_id",
            name="variable_to_store_run_id"
        )

        get_all_timeoffs_types = rail.RepliconServiceOperator(
            task_id = "get_all_timeoffs_types",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes"
        )

        assign_timeoff_to_user = rail.RepliconServiceOperator(
            task_id="assign_timeoff_to_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "timeOffTypeUris": dag_run.conf['timeoffs']
            }
        )

        # process_normal_timeoffs in for loop
        for_each_timeoff = rail.ForEachOperator(
            task_id = "for_each_timeoff",
            items=lambda dag_run: [timeoff for timeoff in dag_run.conf["map_mapper_replicon_timeoff"] if timeoff['uri']],
            start_task="timeoff_name_not_es_holiday_or_es_sick_time",
            end_task="for_each_timeoff_end"
        )

        def timeoff_name_not_es_holiday_or_es_sick_time_test():
            return rail.result("for_each_timeoff")['name'] not in ['[USA] ES Holiday', '[USA] ES Sick Time']

        timeoff_name_not_es_holiday_or_es_sick_time = rail.IfOperator(
            task_id = "timeoff_name_not_es_holiday_or_es_sick_time",
            test=timeoff_name_not_es_holiday_or_es_sick_time_test,
            yes_task="get_default_timeoff_policy_schedule_for_user",
            no_task="timeoff_name_is_es_sick_time"
        )

        get_default_timeoff_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id = "get_default_timeoff_policy_schedule_for_user",
            endpoint = "/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff")['uri']
                }
            }
        )

        has_any_policy_to_assign = rail.IfOperator(
            task_id = "has_any_policy_to_assign",
            test=lambda: rail.result("get_default_timeoff_policy_schedule_for_user") and rail.result("get_default_timeoff_policy_schedule_for_user")[0]['policySet'],
            yes_task="put_default_policy_to_user",
            no_task="for_each_timeoff_end"
        )

        def get_put_default_policy_to_user_payload(dag_run):
            policy = loads(dumps(rail.result("get_default_timeoff_policy_schedule_for_user")
                    ).replace("null", "\"effective\""
                ).replace("\"script\"", "\"scriptTarget\""
                ))
            return {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff")['uri']
                },
                "policySetScheduleEntries": policy
            }

        put_default_policy_to_user = rail.RepliconServiceOperator(
            task_id = "put_default_policy_to_user",
            endpoint = "/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data = get_put_default_policy_to_user_payload
        )

        def timeoff_name_is_es_sick_time_test():
            return rail.result("for_each_timeoff")['name'] == '[USA] ES Sick Time'

        timeoff_name_is_es_sick_time = rail.IfOperator(
            task_id = "timeoff_name_is_es_sick_time",
            test=timeoff_name_is_es_sick_time_test,
            yes_task="is_state_california",
            no_task="timeoff_name_is_es_holiday"
        )

        def is_state_california_test(dag_run):
            return dag_run.conf['state'] == "California"

        is_state_california = rail.IfOperator(
            task_id = "is_state_california",
            test=is_state_california_test,
            yes_task="trigger_us_sick_california_timeoff_assignment",
            no_task="trigger_us_sick_non_california_timeoff_assignment"
        )

        trigger_us_sick_california_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_us_sick_california_timeoff_assignment",
            items = [1],
            trigger_dag_id = config.usa_les_us_sick_leave_california_user_timeoff_assignment_dag_id,
            conf=lambda dag_run: {
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['emp_id'],
                "email_id": dag_run.conf['email_id'],
                "user_uri": dag_run.conf['user_uri'],
                "loginName": dag_run.conf['loginName'],
                "company_code": dag_run.conf['company_code'],
                "source": dag_run.conf['parent_company_code'],
                "start_date": dag_run.conf['start_date'],
                "country": dag_run.conf['country'],
                "contineous_service_date": dag_run.conf['contineous_service_date'],
                "timeoff_uri": rail.result("for_each_timeoff")['uri'],
                "timeoff_name": rail.result("for_each_timeoff")['name'],
                "secondary_timeoff_uri": rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeoffs_types"), "name", "[USA] ES CA Sick Time", 'uri', ''),
                "Secondarytimeoffname": "[USA] ES CA Sick Time",
                "caller": "Add",
                "policy_sets": [],
                "schedule_changed_date": None,
                "action": "Add"
            }
        )

        add_dag_run_id_to_wait1 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait1",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_us_sick_california_timeoff_assignment"),
            append=True
        )

        trigger_us_sick_non_california_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_us_sick_non_california_timeoff_assignment",
            items = [1],
            trigger_dag_id = config.usa_les_us_sick_leave_non_california_user_timeoff_assignment_dag_id,
            conf=lambda dag_run: {
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['emp_id'],
                "email_id": dag_run.conf['email_id'],
                "user_uri": dag_run.conf['user_uri'],
                "loginName": dag_run.conf['loginName'],
                "company_code": dag_run.conf['company_code'],
                "source": dag_run.conf['parent_company_code'],
                "start_date": dag_run.conf['start_date'],
                "country": dag_run.conf['country'],
                "contineous_service_date": dag_run.conf['contineous_service_date'],
                "timeoff_uri": rail.result("for_each_timeoff")['uri'],
                "timeoff_name": rail.result("for_each_timeoff")['name'],
                "schedule": dag_run.conf['work_schedule'],
                "caller": "Add",
                "policy_sets": [],
                "schedule_changed_date": None,
                "fte": dag_run.conf['fte'],
                "action": "Add"
            }
        )

        add_dag_run_id_to_wait2 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait2",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_us_sick_non_california_timeoff_assignment"),
            append=True
        )

        def timeoff_name_is_es_holiday_test():
            return rail.result("for_each_timeoff")['name'] == '[USA] ES Holiday'

        timeoff_name_is_es_holiday = rail.IfOperator(
            task_id = "timeoff_name_is_es_holiday",
            test=timeoff_name_is_es_holiday_test,
            yes_task="trigger_us_holiday_timeoff_assignment",
            no_task="for_each_timeoff_end"
        )

        trigger_us_holiday_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_us_holiday_timeoff_assignment",
            items = [1],
            trigger_dag_id = config.usa_les_us_holiday_user_timeoff_assignment_dag_id,
            conf=lambda dag_run: {
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['emp_id'],
                "email_id": dag_run.conf['email_id'],
                "user_uri": dag_run.conf['user_uri'],
                "loginName": dag_run.conf['loginName'],
                "company_code": dag_run.conf['company_code'],
                "source": dag_run.conf['parent_company_code'],
                "start_date": dag_run.conf['start_date'],
                "country": dag_run.conf['country'],
                "contineous_service_date": dag_run.conf['contineous_service_date'],
                "timeoff_uri": rail.result("for_each_timeoff")['uri'],
                "timeoff_name": rail.result("for_each_timeoff")['name'],
                "schedule": dag_run.conf['work_schedule'],
                "caller": "Add",
                "policy_sets": [],
                "schedule_changed_date": None,
                "fte": dag_run.conf['fte'],
                "action": "Add"
            }
        )

        add_dag_run_id_to_wait3 = rail.SetVariableOperator(
            task_id = "add_dag_run_id_to_wait3",
            name= lambda: rail.result('set_variable_to_store_run_id')['name'],
            value=lambda: rail.result("trigger_us_holiday_timeoff_assignment"),
            append=True
        )

        for_each_timeoff_end = rail.EmptyOperator(
            task_id = "for_each_timeoff_end"
        )

        def gather_all_runids_to_wait_callable():
            dag_run_ids = rail.get_dag_run_var(rail.result("set_variable_to_store_run_id")['name'])
            if not dag_run_ids:
                dag_run_ids = []
            if rail.result("process_timeoff_disable"):
                dag_run_ids.extend(rail.result("process_timeoff_disable"))
            return dag_run_ids

        gather_all_runids_to_wait = rail.PythonOperator(
            task_id = "gather_all_runids_to_wait",
            python_callable=gather_all_runids_to_wait_callable
        )

        wait_for_dag_run_to_complete = rail.WaitForDagRunsSensor(
            task_id = "wait_for_dag_run_to_complete",
            dag_runs="{{result('gather_all_runids_to_wait')}}",
            retries = 0,
            execution_timeout = timedelta(days=1)
        )

        catch_and_log_error =  rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            log = "{{dag_run.conf.user_log}}",
            trigger_rule = "one_failed",
            message="User Add",
            severity="Error",
            properties=lambda dag_run: {
                "Jobid": "",
                "Userid": dag_run.conf["emp_id"],
                "Email": dag_run.conf["email_id"],
                "Action": 'Add',
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> set_variable_to_store_run_id
        set_variable_to_store_run_id >> get_all_timeoffs_types >> assign_timeoff_to_user >> for_each_timeoff

        for_each_timeoff >> timeoff_name_not_es_holiday_or_es_sick_time >> rail.Label(
            "Yes") >> get_default_timeoff_policy_schedule_for_user >> has_any_policy_to_assign >> rail.Label("Yes") >> put_default_policy_to_user >> for_each_timeoff_end
        has_any_policy_to_assign >> rail.Label("No") >> for_each_timeoff_end
        timeoff_name_not_es_holiday_or_es_sick_time >> rail.Label("No") >> timeoff_name_is_es_sick_time >> for_each_timeoff_end
        timeoff_name_is_es_sick_time >> rail.Label("Yes") >> is_state_california >> rail.Label("Yes") >> trigger_us_sick_california_timeoff_assignment >> add_dag_run_id_to_wait1 >> for_each_timeoff_end
        is_state_california >> rail.Label("No") >> trigger_us_sick_non_california_timeoff_assignment >> add_dag_run_id_to_wait2 >> for_each_timeoff_end


        timeoff_name_is_es_sick_time >> rail.Label("No") >> timeoff_name_is_es_holiday >> rail.Label("Yes") >> trigger_us_holiday_timeoff_assignment >> add_dag_run_id_to_wait3 >> for_each_timeoff_end

        timeoff_name_is_es_holiday >> rail.Label("No") >> for_each_timeoff_end

        for_each_timeoff >> for_each_timeoff_end >> gather_all_runids_to_wait >> wait_for_dag_run_to_complete >> catch_and_log_error

        return dag

rail.for_each_instance(create_add_user_timeoff_assignment_dag)
