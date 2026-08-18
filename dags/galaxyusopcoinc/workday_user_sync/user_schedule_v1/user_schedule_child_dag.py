import rail
from galaxyusopcoinc.workday_user_sync.user_schedule_v1.utils import request_payload
from galaxyusopcoinc.workday_user_sync.user_schedule_v1.utils import python_callable_method


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'vialtopartners_user_schedule_child_dag_{config.dag_id_postfix}',
        description=f'Vialto Partners User Schedule Child V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_process_user_schedule_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_user_info_from_user_service = rail.RepliconServiceOperator(
            task_id='get_user_info_from_user_service',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=request_payload.get_user_details3_payload
        )

        is_user_present = rail.IfOperator(
            task_id="is_user_present",
            test="{{ result('get_user_info_from_user_service') | length > 0}}",
            yes_task="is_user_enable",
            no_task="log_user_not_present",
        )

        is_user_enable = rail.IfOperator(
            task_id="is_user_enable",
            # to be updated
            test=lambda: bool(rail.result('get_user_info_from_user_service')[0]['userDetails']['isEnabled']),
            yes_task="is_schedule_all_blank_zero",
            no_task="log_user_disable",
        )

        log_user_not_present = rail.WriteLogOperator(
            task_id='log_user_not_present',
            message='User not present in Replicon',
            severity='Exception',
            properties={
                'schedulename': "{{dag_run.conf.replicon_schedule_type}}",
                'employeeid': "{{dag_run.conf.employee_id}}",
                'status': 'Exception',
                'message': "User not present in Replicon"
            }
        )

        log_user_disable = rail.WriteLogOperator(
            task_id='log_user_disable',
            message='User is disable in Replicon',
            severity='Exception',
            properties={
                'schedulename': "{{dag_run.conf.replicon_schedule_type}}",
                'employeeid': "{{dag_run.conf.employee_id}}",
                'status': 'Exception',
                'message': "User is disable in Replicon"
            }
        )

        is_schedule_all_blank_zero = rail.IfOperator(
            task_id='is_schedule_all_blank_zero',
            test=lambda: python_callable_method.schedule_all_blank_zero(
                request_payload.get_dag_run_conf()['replicon_schedule_type']),
            yes_task='update_intial_schedule',
            no_task='has_valid_scheduletype'
        )

        update_intial_schedule = rail.RepliconServiceOperator(
            task_id='update_intial_schedule',
            endpoint='/services/ImportService1.svc/ApplyUserModifications3',
            # uri to be updated
            data=lambda: request_payload.apply_user_modification_payload(True)
        )

        log_success_initial_schedule = rail.WriteLogOperator(
            task_id='log_success_initial_schedule',
            message='Initial Schedule is assigned to user',
            severity='Success',
            properties={
                'schedulename': "{{ dag_run.conf.replicon_schedule_type}}",
                'employeeid': "{{ dag_run.conf.employee_id}}",
                'status': 'Success',
                'message': "8 hours/day; Mon-Fri Schedule is assigned to user as Initial Schedule"
            }
        )

        has_valid_scheduletype = rail.IfOperator(
            task_id='has_valid_scheduletype',
            test=lambda: python_callable_method.valid_schedule(
                request_payload.get_dag_run_conf()['replicon_schedule_type']),
            yes_task='update_user_office_schedule',
        )

        update_user_office_schedule = rail.RepliconServiceOperator(
            task_id='update_user_office_schedule',
            endpoint='/services/ImportService1.svc/ApplyUserModifications3',
            data=lambda: request_payload.apply_user_modification_payload(False)
        )

        log_success_schedule = rail.WriteLogOperator(
            task_id='log_success_schedule',
            message='Schedule is assigned to user',
            severity='Success',
            properties={
                'schedulename': "{{ dag_run.conf.replicon_schedule_type}}",
                'employeeid': "{{ dag_run.conf.employee_id}}",
                'status': 'Success',
                'message': "Schedule is assigned to user"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'schedulename': "{{ dag_run.conf.replicon_schedule_type}}",
                'employeeid': "{{ dag_run.conf.employee_id}}",
                'status': 'Error',
                'message': '{{ get_error_message() }}',

            },
        )

        get_user_info_from_user_service >> is_user_present >> rail.Label(
            "NO") >> log_user_not_present >> catch_and_log_errors
        is_user_present >> rail.Label("YES") >> is_user_enable
        is_user_enable >> rail.Label("YES") >> is_schedule_all_blank_zero
        is_user_enable >> rail.Label("NO") >> log_user_disable >> catch_and_log_errors
        is_schedule_all_blank_zero >> rail.Label(
            "YES") >> update_intial_schedule >> log_success_initial_schedule >> catch_and_log_errors
        is_schedule_all_blank_zero >> rail.Label(
            "NO") >> has_valid_scheduletype
        has_valid_scheduletype >> rail.Label(
            "YES") >> update_user_office_schedule
        update_user_office_schedule >> log_success_schedule >> catch_and_log_errors

        return dag


rail.for_each_instance(create_child_dag)
