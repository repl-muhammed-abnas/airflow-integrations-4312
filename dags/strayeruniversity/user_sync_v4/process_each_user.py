from datetime import timedelta
from airflow.models import Variable
import rail
from strayeruniversity.user_sync_v4.utils import request_payload

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_proecss_each_user_dag_id,
        description=f'strayeruniversity_usersync_proecss_each_user_child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_eachuser_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_user_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_user_log',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_user_log = rail.CreateLogOperator(
            task_id="create_user_log"
        )

        if_division_present = rail.IfOperator(
            task_id='if_division_present',
            test='''{{ dag_run.conf.division | is_truthy }}''',
            yes_task="get_user_data_based_on_emplid",
            no_task="catch_and_log_error",
        )

        get_user_data_based_on_emplid = rail.RepliconServiceOperator(
            task_id="get_user_data_based_on_emplid",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": null,
                        "loginName": null,
                        "employeeId": "{{dag_run.conf.emplid}}",
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: [] if response == [None] else response
        )

        check_user_in_replicon_present = rail.IfOperator(
            task_id='check_user_in_replicon_present',
            test=lambda: bool(rail.result('get_user_data_based_on_emplid')),
            yes_task="if_empstatus_is_T",
            no_task="process_add_user",
        )

        if_empstatus_is_T = rail.IfOperator(
            task_id='if_empstatus_is_T',
            test='''{{ dag_run.conf.employeestatus == 'T' }}''',
            yes_task="process_disable_user",
            no_task="get_current_groups_data",
        )

        process_disable_user = rail.TriggerDagRunOperator(
            task_id='process_disable_user',
            trigger_dag_id=config.child_disable_user_dag_id,
            conf=request_payload.process_disable_user_payload,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_process_disable_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_disable_user',
            dag_runs='{{ result("process_disable_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_current_groups_data = rail.RepliconServiceOperator(
            task_id="get_current_groups_data",
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data=lambda: {
                "userUri": rail.result('get_user_data_based_on_emplid')[0]['userDetails']['uri'],
                "dateRange": null
            },
            data_handler=lambda response: {
                'current_location': response['locations'][0]['location']['location']['displayText'] if response['locations'] else '',
                'current_division': response['divisions'][0]['division']['division']['displayText'] if response['divisions'] else '',
                'current_scheduledhour': response['serviceCenters'][0]['serviceCenter']['serviceCenter']['displayText'] if response['serviceCenters'] else ''
            }
        )

        process_update_user = rail.TriggerDagRunOperator(
            task_id='process_update_user',
            trigger_dag_id=config.child_update_user_dag_id,
            conf=request_payload.process_update_user_payload,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_process_update_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_update_user',
            dag_runs='{{ result("process_update_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        process_add_user = rail.TriggerDagRunOperator(
            task_id='process_add_user',
            trigger_dag_id=config.child_add_user_dag_id,
            conf=request_payload.process_add_user_payload,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_process_add_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_add_user',
            dag_runs='{{ result("process_add_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log='{{ dag_run.conf.logger}}',
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "username": "{{ dag_run.conf.username }}" + "|" + "{{ dag_run.conf.emplid }}",
                "action": "Process Each User",
                "status": "Error",
                "details": "{{ dag_run_ecid() }}" + "-" + "{{ get_error_message() }}"
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> create_user_log

        create_user_log >> if_division_present

        if_division_present >> rail.Label(
            'Yes') >> get_user_data_based_on_emplid >> check_user_in_replicon_present
        if_division_present >> rail.Label('No') >> catch_and_log_error

        check_user_in_replicon_present >> rail.Label(
            'Yes') >> if_empstatus_is_T
        check_user_in_replicon_present >> rail.Label(
            'No') >> process_add_user >> wait_for_process_add_user >> catch_and_log_error

        if_empstatus_is_T >> rail.Label(
            'Yes') >> process_disable_user >> wait_for_process_disable_user >> catch_and_log_error
        if_empstatus_is_T >> rail.Label(
            'No') >> get_current_groups_data >> process_update_user >> wait_for_process_update_user >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
