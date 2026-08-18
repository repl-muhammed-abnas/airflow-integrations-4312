from datetime import timedelta
import rail
from galaxyusopcoinc.workday_user_sync.user_import_v2.utils import request_payload
from galaxyusopcoinc.workday_user_sync.user_import_v2.utils import custom_methods
from airflow.models import Variable
from pandas import DateOffset, to_datetime


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.process_timeoff_dag_id,
        description='User Sync Process Time off Policy New User',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_time_off_policy_new_user,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="get_default_time_off_policy_schedule"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_default_time_off_policy_schedule',
            end_task="catch_and_log_errors",
        )

        def get_policy_to_assign_for_timeoff(response, dag_run):
            rail.set_result(key="res", val=response)
            if not response:
                return None

            present = request_payload.get_date_from_json_date(
                dag_run.conf["effective_date_to_use"])

            def get_offset(item):
                if (item['startOffset']['offsetUnitUri']).split(':')[-1] == 'years':
                    return DateOffset(years=int(item['startOffset']['offsetValue']))
                if (item['startOffset']['offsetUnitUri']).split(':')[-1] == 'months':
                    return DateOffset(months=int(item['startOffset']['offsetValue']))
                return DateOffset(days=int(item['startOffset']['offsetValue']))

            def get_effective_date(item):
                current_date = (to_datetime(present) + get_offset(item))
                return {
                    'day': current_date.day,
                    'month': current_date.month,
                    'year': current_date.year
                }

            return list(map(lambda item: {
                'description': 'effective on ' + dag_run.conf['hiredate'] if dag_run.conf['action'].lower() == 'add' else present.strftime("%y-%m-%d"),
                'effectiveDate': get_effective_date(item),
                'policySet': item['policySet']
            }, response))

        get_default_time_off_policy_schedule = rail.RepliconServiceOperator(
            task_id="get_default_time_off_policy_schedule",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data=request_payload.get_default_timeoff_policy_schedule_payload,
            data_handler=get_policy_to_assign_for_timeoff
        )

        is_update_user = rail.IfOperator(
            task_id="is_update_user",
            test=custom_methods.is_user_update_callable,
            yes_task="get_all_policies_assigned",
            no_task="policy_to_assign"
        )

        def get_specific_user_time_off_assigned(response, dag_run):
            if dag_run.conf.get('rehire', 'no').lower() == "yes":
                return []
            data = response['policiesByTimeOffType']
            timeoff_data = list(filter(lambda x: x['timeOffType']['displayText'] == dag_run.conf['timeoff_to_process']['name'], data))
            if timeoff_data:
                return timeoff_data[0]['policySetSchedule']
            return []

        get_all_policies_assigned = rail.RepliconServiceOperator(
            task_id="get_all_policies_assigned",
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri']
            },
            data_handler=get_specific_user_time_off_assigned
        )

        policy_to_assign = rail.PythonOperator(
            task_id="policy_to_assign",
            python_callable=custom_methods.get_policy_to_assign
        )

        is_policy_present = rail.IfOperator(
            task_id='is_policy_present',
            test=lambda: bool(rail.result('policy_to_assign')),
            yes_task='put_user_timeoff_policy',
            no_task='no_default_timeoff_policy_available'
        )

        put_user_timeoff_policy = rail.RepliconServiceOperator(
            task_id="put_user_timeoff_policy",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=request_payload.get_user_timeoff_policy_payload
        )

        no_default_timeoff_policy_available = rail.EmptyOperator(
            task_id='no_default_timeoff_policy_available'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{dag_run.conf.create_user_log}}",
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'employeeid': '{{dag_run.conf.employeeid}}',
                'username': '{{dag_run.conf.legalfirstname}} {{dag_run.conf.legallastname}}',
                'loginname': '{{dag_run.conf.workemail}}',
                'status': "Error",
                'action': '{{dag_run.conf.action}}',
                'message': '{{ get_error_message() }}',
                "allowed_for_supervisor_dag": "NA",
                "user_uri": "{{dag_run.conf.user_uri}}",
                "managerid": "{{dag_run.conf.managerid}}",
                "is_add_and_errored": "NA"
            },
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_default_time_off_policy_schedule \
            >> is_update_user >> rail.Label("No") >> policy_to_assign >> is_policy_present
        is_update_user >> rail.Label(
            "Yes") >> get_all_policies_assigned >> policy_to_assign
        is_policy_present >> rail.Label(
            'Yes') >> put_user_timeoff_policy >> catch_and_log_errors
        is_policy_present >> rail.Label(
            'No') >> no_default_timeoff_policy_available >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag_wbs)
