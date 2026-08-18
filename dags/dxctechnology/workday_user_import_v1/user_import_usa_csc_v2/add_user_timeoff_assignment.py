from datetime import timedelta
from json import dumps, loads
from pendulum import datetime
import rail
from airflow.models import Variable
from dxctechnology.workday_user_import_v1.user_import_global.utils import request_payload


# Non- Canada
def create_add_user_timeoff_assignment_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.usa_csc_add_user_timeoff_assignment_dag_id,
        description="dxctechnology workday user sync process users child",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.max_run_add_to_assignment
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name_us_csc, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="get_all_timeoffs_types"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="get_all_timeoffs_types",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
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
                "timeOffTypeUris": dag_run.conf["timeoffs"]
            }
        )

        # process_normal_timeoffs in for loop
        for_each_timeoff = rail.ForEachOperator(
            task_id = "for_each_timeoff",
            items=lambda dag_run: [timeoff for timeoff in dag_run.conf["map_mapper_replicon_timeoff"] if timeoff['uri']],
            start_task="is_timeoff_name_not_holiday_or_sick_time_or_vacation_accrued",
            end_task="for_each_timeoff_end"
        )

        def timeoff_name_not_holiday_or_sick_time_or_vacation_accrued_test():
            return rail.result("for_each_timeoff")['name'] not in ['[USA] 02-CSC Holiday', '[USA] 03-CSC Sick Time', '[PR] 04-Vacation Accrued']

        is_timeoff_name_not_holiday_or_sick_time_or_vacation_accrued = rail.IfOperator(
            task_id = "is_timeoff_name_not_holiday_or_sick_time_or_vacation_accrued",
            test=timeoff_name_not_holiday_or_sick_time_or_vacation_accrued_test,
            yes_task="get_default_timeoff_policy_schedule_for_user",
            no_task="timeoff_name_is_sick_time"
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
            test=lambda: bool(rail.result("get_default_timeoff_policy_schedule_for_user")),
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

        def timeoff_name_is_sick_time_test():
            return rail.result("for_each_timeoff")['name'] == '[USA] 03-CSC Sick Time'

        timeoff_name_is_sick_time = rail.IfOperator(
            task_id = "timeoff_name_is_sick_time",
            test=timeoff_name_is_sick_time_test,
            yes_task="is_state_california",
            no_task="timeoff_name_is_es_holiday"
        )

        def is_state_california_test(dag_run):
            return dag_run.conf['state'] == 'California'

        is_state_california = rail.IfOperator(
            task_id = "is_state_california",
            test=is_state_california_test,
            yes_task="trigger_us_sick_california_timeoff_assignment",
            no_task="trigger_us_sick_non_california_timeoff_assignment"
        )

        trigger_us_sick_california_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_us_sick_california_timeoff_assignment",
            items = [1],
            trigger_dag_id = config.usa_csc_us_sick_leave_california_user_timeoff_assignment_dag_id,
            conf=lambda dag_run: {
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
                "secondary_timeoff_uri": rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeoffs_types"), "name", "[USA] CSC CA Sick Time", 'uri', ''),
                "Secondarytimeoffname": "[USA] CSC CA Sick Time",
                "caller": "Add",
                "policy_sets": [],
                "schedule_changed_date": None
            }
        )

        trigger_us_sick_non_california_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_us_sick_non_california_timeoff_assignment",
            items = [1],
            trigger_dag_id = config.usa_csc_us_sick_leave_non_california_user_timeoff_assignment_dag_id,
            conf=lambda dag_run: {
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
                "fte": dag_run.conf['fte']
            }
        )

        def timeoff_name_is_es_holiday_test():
            return rail.result("for_each_timeoff")['name'] == '[USA] 02-CSC Holiday'

        timeoff_name_is_es_holiday = rail.IfOperator(
            task_id = "timeoff_name_is_es_holiday",
            test=timeoff_name_is_es_holiday_test,
            yes_task="trigger_us_holiday_timeoff_assignment",
            no_task="timeoff_name_is_vacation_accrued"
        )

        trigger_us_holiday_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_us_holiday_timeoff_assignment",
            items = [1],
            trigger_dag_id = config.usa_csc_us_holiday_user_timeoff_assignment_dag_id,
            conf=lambda dag_run: {
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
                "fte": dag_run.conf['fte']
            }
        )

        def timeoff_name_is_es_vacation_accrued():
            return rail.result("for_each_timeoff")['name'] == '[PR] 04-Vacation Accrued'

        timeoff_name_is_vacation_accrued = rail.IfOperator(
            task_id = "timeoff_name_is_vacation_accrued",
            test=timeoff_name_is_es_vacation_accrued,
            yes_task="trigger_vacation_accrued_timeoff_assignment",
            no_task="for_each_timeoff_end"
        )

        trigger_vacation_accrued_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_vacation_accrued_timeoff_assignment",
            items = [1],
            trigger_dag_id = config.usa_csc_us_puerto_rico_user_timeoff_assignment_dag_id,
            conf=lambda dag_run: {
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
                "fte": dag_run.conf['fte']
            }
        )

        for_each_timeoff_end = rail.EmptyOperator(
            task_id = "for_each_timeoff_end"
        )

        is_wait_required =  rail.IfOperator(
            task_id = "is_wait_required",
            test=lambda: rail.result("trigger_vacation_accrued_timeoff_assignment") or rail.result("trigger_us_holiday_timeoff_assignment") or rail.result("trigger_us_sick_california_timeoff_assignment")
                or rail.result("trigger_us_sick_non_california_timeoff_assignment"),
            yes_task="wait_for_timeoff_triggers",
            no_task="catch_and_log_error"
        )

        wait_for_timeoff_triggers = rail.WaitForDagRunsSensor(
            task_id = "wait_for_timeoff_triggers",
            dag_runs="""{{ result('trigger_vacation_accrued_timeoff_assignment') or result('trigger_us_holiday_timeoff_assignment') or result('trigger_us_sick_california_timeoff_assignment') or result('trigger_us_sick_non_california_timeoff_assignment')}}""",
            execution_timeout=timedelta(
                    days=config.execution_timeout_days),
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
        can_run_batch_task >> rail.Label("No") >> get_all_timeoffs_types

        get_all_timeoffs_types >> assign_timeoff_to_user >> for_each_timeoff

        for_each_timeoff >> is_timeoff_name_not_holiday_or_sick_time_or_vacation_accrued >> rail.Label(
            "Yes") >> get_default_timeoff_policy_schedule_for_user >> has_any_policy_to_assign >> rail.Label("Yes") >> put_default_policy_to_user >> for_each_timeoff_end
        has_any_policy_to_assign >> rail.Label("No") >> for_each_timeoff_end

        is_timeoff_name_not_holiday_or_sick_time_or_vacation_accrued >> rail.Label("No") >> timeoff_name_is_sick_time
        timeoff_name_is_sick_time >> rail.Label("Yes") >> is_state_california >> rail.Label("Yes") >> trigger_us_sick_california_timeoff_assignment >> timeoff_name_is_es_holiday
        is_state_california >> rail.Label("No") >> trigger_us_sick_non_california_timeoff_assignment >> timeoff_name_is_es_holiday

        timeoff_name_is_sick_time >> rail.Label("No") >> timeoff_name_is_es_holiday >> rail.Label("Yes") >> trigger_us_holiday_timeoff_assignment >> timeoff_name_is_vacation_accrued

        timeoff_name_is_es_holiday >> rail.Label("No") >> timeoff_name_is_vacation_accrued >> rail.Label('Yes') >> trigger_vacation_accrued_timeoff_assignment >> for_each_timeoff_end

        timeoff_name_is_vacation_accrued >> rail.Label('No') >> for_each_timeoff_end

        for_each_timeoff >> for_each_timeoff_end >> is_wait_required >> rail.Label('Yes') >> wait_for_timeoff_triggers >> catch_and_log_error
        is_wait_required >> rail.Label('No') >>catch_and_log_error

        return dag

rail.for_each_instance(create_add_user_timeoff_assignment_dag)
