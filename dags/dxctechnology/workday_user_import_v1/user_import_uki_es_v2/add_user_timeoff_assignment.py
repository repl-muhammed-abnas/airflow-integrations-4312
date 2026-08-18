import json
import ast
from pandas import DateOffset, to_datetime
from pendulum import datetime
import rail
from dxctechnology.workday_user_import_v1.user_import_uki_es_v2.utils import request_payload, custom_methods
from airflow.models import Variable
from datetime import timedelta

def create_add_user_timeoff_assignment_dag(config):
    _dags = []
    for batch_index in range(1, config.DAG_BATCH_COUNT + 1):
        prefix = f"_{batch_index}"
        if batch_index == 1:
            prefix = ""
        with rail.create_airflow_dag(
            dag_id=f"{config.workday_user_import_uki_es_add_user_timeoff_assignment_dag}{prefix}",
            description="DXC Technology Workday User Sync UK&I CSC Add User Timeoff Assignment",
            replicon_conn_id=config.replicon_conn_id,
            company_key=config.company_key,
            start_date=datetime(2025, 4, 1),
            max_active_runs=config.max_active_run_add_user_timeoff_assignment_uki_es
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

            can_run_batch_task = rail.IfOperator(
                task_id="can_run_batch_task",
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name_uki_es, default_var='true').lower() == 'true',
                yes_task="batch_task",
                no_task="is_the_timeoff_is_special"
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id="batch_task",
                start_task="is_the_timeoff_is_special",
                end_task="catch_and_log_error",
                execution_timeout=timedelta(days=14)
            )

            is_the_timeoff_is_special = rail.IfOperator(
                task_id = "is_the_timeoff_is_special",
                test = lambda dag_run: dag_run.conf['special_timeoff'] == 'yes',
                yes_task="process_special_timeoff",
                no_task="get_default_timeoff_policy"
            )

            process_special_timeoff = rail.EmptyOperator(
                task_id = "process_special_timeoff"
            )

            def get_offset(item):
                if (item['startOffset']['offsetUnitUri']).split(':')[-1] == 'years':
                    return DateOffset(years=int(item['startOffset']['offsetValue']))
                if (item['startOffset']['offsetUnitUri']).split(':')[-1] == 'months':
                    return DateOffset(months=int(item['startOffset']['offsetValue']))
                return DateOffset(days=int(item['startOffset']['offsetValue']))

            def get_policy_to_assign_for_vacation_add(response,dag_run):
                if not response:
                    return None

                rail.set_result(key = "response", val = response)
                def get_effective_date(item):
                    if item['startOffset']['offsetValue'] == 0:
                        return request_payload.get_replicon_date(dag_run.conf['effective_date'])

                    _date = to_datetime(dag_run.conf['effective_date'], format=request_payload.INPUT_DATE_FORMAT) + get_offset(item)
                    return {
                            'year': _date.year,
                            'month': 1,
                            'day': 1
                        }

                res= list(map(lambda item: {
                    'description': 'effective on '+ str(dag_run.conf['effective_date']),
                    'effectiveDate': get_effective_date(item),
                    'policySet': item['policySet']
                }, response))

                return json.dumps(ast.literal_eval(str(res).replace("'script'", "'scriptTarget'")))

            get_default_time_off_policy_schedule = rail.RepliconServiceOperator(
                task_id="get_default_time_off_policy_schedule",
                endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
                data={
                    "timeOffTypeUri": "{{ dag_run.conf.timeoff_uri }}"
                },
                data_handler=get_policy_to_assign_for_vacation_add
            )

            is_policy_present = rail.IfOperator(
                task_id='is_policy_present',
                test=lambda: bool(rail.result(
                    'get_default_time_off_policy_schedule')),
                yes_task='put_user_timeoff_policy',
                no_task='catch_and_log_error'
            )

            put_user_timeoff_policy = rail.RepliconServiceOperator(
                task_id="put_user_timeoff_policy",
                endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
                data=lambda dag_run:{
                    "timeOffAccount": {
                        "userUri": dag_run.conf['user_uri'],
                        "timeOffTypeUri": dag_run.conf['timeoff_uri']
                    },
                    "policySetScheduleEntries": json.loads(rail.result('get_default_time_off_policy_schedule'))
                }
            )

            # Original flow for standard timeoffs
            get_default_timeoff_policy = rail.RepliconServiceOperator(
                task_id="get_default_timeoff_policy",
                endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
                data=request_payload.get_default_timeoff_policy_payload_uki_es
            )

            policy_set_to_assign = rail.PythonOperator(
                task_id='policy_set_to_assign',
                python_callable=custom_methods.get_policy_set_to_assign_uki_es
            )

            def has_valid_policies():
                # Check both possible sources of policy data
                policy_data = rail.result("policy_set_to_assign", None)
                return policy_data is not None and bool(policy_data)

            has_any_policy_to_assign = rail.IfOperator(
                task_id="has_any_policy_to_assign",
                test=has_valid_policies,
                yes_task="put_user_timeoff_policy_set",
                no_task="catch_and_log_error"
            )

            def get_put_user_timeoff_policy_set_data(dag_run):
                return {
                    "timeOffAccount": {
                        "userUri": dag_run.conf['user_uri'],
                        "timeOffTypeUri": dag_run.conf['timeoff_uri']
                    },
                    "policySetScheduleEntries": rail.result('policy_set_to_assign')
                }

            put_user_timeoff_policy_set = rail.RepliconServiceOperator(
                task_id="put_user_timeoff_policy_set",
                endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
                data=get_put_user_timeoff_policy_set_data
            )

            # UK&I specific: Log successful timeoff assignment with UK&I details
            log_timeoff_success = rail.WriteLogOperator(
                task_id="log_timeoff_success",
                log="{{ dag_run.conf.user_log }}",
                message="Timeoff Assignment Success",
                severity="Success",
                properties=lambda dag_run: {
                    "Jobid": "",
                    "Userid": dag_run.conf.get('emp_id', 'Unknown'),
                    "Email": dag_run.conf.get('email_id', 'Unknown'),
                    "Action": "Add Timeoff",
                    "Status": "Success",
                    "Details": f"Timeoff {dag_run.conf.get('timeoff_name', '')} assigned successfully",
                    "TimeoffType": dag_run.conf.get('timeoff_name', ''),
                    "Country": dag_run.conf.get('file_data', {}).get('country', ''),
                    "WorkerCategory": dag_run.conf.get('file_data', {}).get('worker_category', ''),
                    "CostCenter": dag_run.conf.get('file_data', {}).get('cost_center_name', '')
                }
            )

            def get_timeoff_error_log_properties(dag_run):
                # Check if we have valid dag_run.conf
                if not dag_run or not hasattr(dag_run, 'conf') or not dag_run.conf:
                    return {
                        "Jobid": "",
                        "Userid": "Unknown",
                        "Email": "Unknown",
                        "Action": "Add Timeoff",
                        "Status": "Error",
                        "Details": "Missing dag_run.conf data"
                    }

                emp_id = dag_run.conf.get('emp_id', 'Unknown')
                email_id = dag_run.conf.get('email_id', 'Unknown')
                error_message = rail.render_template("{{get_error_message()}}")
                timeoff_name = dag_run.conf.get('timeoff_name', 'Unknown')

                return {
                    "Jobid": "",
                    "Userid": emp_id,
                    "Email": email_id,
                    "Action": "Add Timeoff",
                    "Status": "Error",
                    "Details": f"Failed to assign timeoff {timeoff_name}: {error_message}",
                    "TimeoffType": timeoff_name,
                    "Country": dag_run.conf.get('file_data', {}).get('country', ''),
                    "WorkerCategory": dag_run.conf.get('file_data', {}).get('worker_category', '')
                }

            catch_and_log_error = rail.WriteLogOperator(
                task_id="catch_and_log_error",
                log="{{ dag_run.conf.user_log }}",
                trigger_rule="one_failed",
                message="User Add Timeoff Error",
                severity="Error",
                properties=get_timeoff_error_log_properties
            )

            # Task dependencies
            can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
            can_run_batch_task >> rail.Label("No") >> is_the_timeoff_is_special

            is_the_timeoff_is_special >> rail.Label("No") >> get_default_timeoff_policy
            is_the_timeoff_is_special >> rail.Label("Yes") >> process_special_timeoff >> get_default_time_off_policy_schedule >> is_policy_present
            
            is_policy_present >> rail.Label("No") >> catch_and_log_error
            is_policy_present >> rail.Label("Yes") >> put_user_timeoff_policy >> catch_and_log_error

            
            get_default_timeoff_policy >> policy_set_to_assign >> has_any_policy_to_assign

            # Company validation and assignment
            has_any_policy_to_assign >> rail.Label('Yes') >> put_user_timeoff_policy_set >> log_timeoff_success >> rail.Label("On Error") >> catch_and_log_error
            
            has_any_policy_to_assign >> rail.Label("On Error") >> catch_and_log_error

        _dags.append(dag)
    return _dags

rail.for_each_instance(create_add_user_timeoff_assignment_dag)