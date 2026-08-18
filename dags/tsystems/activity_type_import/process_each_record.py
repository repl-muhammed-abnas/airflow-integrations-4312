from airflow.models import Variable
import rail

from tsystems.activity_type_import.utils import request_payload
from tsystems.activity_type_import.utils import custom_methods
from tsystems.activity_type_import.utils import response_filter

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_record_dagid,
        description='T-Systems Activity Type Import - Process Each Record DAG',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        max_active_runs=config.max_active_runs_child,
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task= 'create_log',
            end_task='catch_and_log_errors',
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        is_valid_record= rail.IfOperator(
            task_id='is_valid_record',
            test=custom_methods.validate_record,
            yes_task='search_user_in_replicon',
            no_task='log_invalid_record'
        )

        log_invalid_record = rail.WriteLogOperator(
            task_id='log_invalid_record',
            log="{{result('create_log')}}",
            message=custom_methods.get_invalid_record_msg_child,
            severity='Exception',
            properties=lambda dag_run:{
                'employee_id': dag_run.conf['employee_id'],
                'action': 'Validation',
                'status': 'Exception',
                'details': custom_methods.get_invalid_record_msg_child(dag_run)
            }
        )

        search_user_in_replicon = rail.RepliconServiceOperator(
            task_id="search_user_in_replicon",
            endpoint="/services/UserService1.svc/BulkGetUsers2",
            data={
                "users": [
                    {
                    "uri": null,
                    "loginName": null,
                    "employeeId": "{{dag_run.conf.employee_id}}",
                    "parameterCorrelationId": null
                    }
                ]
            },
            data_handler=lambda response: [] if response == [None] else response
        )

        is_user_available = rail.IfOperator(
            task_id='is_user_available',
            test=lambda: bool(rail.result('search_user_in_replicon')),
            yes_task='get_user_details',
            no_task='log_user_not_available'
        )

        get_user_details = rail.RepliconServiceOperator(
                task_id="get_user_details",
                endpoint="/services/ImportService1.svc/BulkGetUsers3",
                data=lambda dag_run: {
                    "users": [
                        {
                            "uri": null,
                            "loginName": null,
                            "employeeId": dag_run.conf["employee_id"],
                            "parameterCorrelationId": null
                        }
                    ],
                    "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
                },
                data_handler=lambda response: response[0] if response else null
            )

        log_user_not_available = rail.WriteLogOperator(
            task_id='log_user_not_available',
            log="{{result('create_log')}}",
            message="User with Employee ID {{ dag_run.conf.employee_id }} not found in replicon.",
            severity='Exception',
            properties={
                'employee_id':  '{{ dag_run.conf.employee_id }}',
                'action': 'Validation',
                'status': 'Exception',
                'details':  "User with Employee ID {{ dag_run.conf.employee_id }} not found in replicon."
            }
        )

        is_user_enabled = rail.IfOperator(
            task_id='is_user_enabled',
            test=lambda: rail.result('get_user_details')['userDetails']['isEnabled'] in [True,"true"],
            yes_task='get_effective_user_groupmembership',
            no_task='log_user_disabled'
        )

        log_user_disabled = rail.WriteLogOperator(
            task_id='log_user_disabled',
            log="{{result('create_log')}}",
            message="User with Employee ID {{ dag_run.conf.employee_id }} is disabled in Replicon.",
            severity='Exception',
            properties={
                'employee_id': '{{ dag_run.conf.employee_id }}',
                'action': 'Validation',
                'status': 'Exception',
                'details': "User with Employee ID {{ dag_run.conf.employee_id }} is disabled in Replicon."
            }
        )

        get_effective_user_groupmembership = rail.RepliconServiceOperator(
            task_id='get_effective_user_groupmembership',
            endpoint='/services/UserGroupService1.svc/GetEffectiveUserGroupMembership',
            data={
                "userUri": "{{result('get_user_details').userDetails.uri}}",
                "dateRange": None
            },
            data_handler=response_filter.get_effective_user_groupmembership_filter
        )

        apply_user_modifications = rail.RepliconServiceOperator(
            task_id='apply_user_modifications',
            endpoint='services/ImportService2.svc/CreateUserOrApplyModifications',
            data=request_payload.get_apply_user_modifications_payload,
        )

        def get_completion_msg():
            if rail.result('apply_user_modifications', key='exception_msg'):
                return  rail.result('apply_user_modifications', key='exception_msg')
            return "Activity Type and Cost Rate updated successfully."
    
        log_completion = rail.WriteLogOperator(
            task_id='log_completion',
            log='{{ result("create_log") }}',
            message=get_completion_msg,
            severity=lambda: 'Success' if not rail.result('apply_user_modifications', key='exception_msg') else 'Exception',
            properties=lambda :{
                'employee_id': '{{ dag_run.conf.employee_id }}',
                'action': 'Update',
                'status': 'Success' if not rail.result('apply_user_modifications', key='exception_msg') else 'Exception',
                'details': get_completion_msg()
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{ result('create_log') }}",
            severity="Error",
            message="{{ get_error_message() }}",
            properties=lambda item: {
                'employee_id':  '{{ dag_run.conf.employee_id }}',
                'action': 'Update',
                'status': 'Error',
                'details': rail.render_template("{{ get_error_message() }}"),
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> create_log

        create_log >> is_valid_record >> rail.Label('No') >> log_invalid_record >> catch_and_log_errors
        create_log >> is_valid_record >> rail.Label('Yes') >> search_user_in_replicon >> is_user_available
        is_user_available >> rail.Label('No') >> log_user_not_available >> catch_and_log_errors
        is_user_available >> rail.Label('Yes') >> get_user_details >> is_user_enabled >> rail.Label('No') >> log_user_disabled >> catch_and_log_errors
        is_user_enabled >> rail.Label('Yes') >> get_effective_user_groupmembership
        get_effective_user_groupmembership >> apply_user_modifications >> log_completion
        log_completion >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)