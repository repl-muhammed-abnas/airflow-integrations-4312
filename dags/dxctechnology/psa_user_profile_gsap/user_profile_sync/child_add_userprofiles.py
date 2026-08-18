from datetime import timedelta
import rail
from dxctechnology.psa_user_profile_gsap.user_profile_sync.task.process_supervisor_assignment import process_supervisor_assignment_task_group
from dxctechnology.psa_user_profile_gsap.user_profile_sync.utils.python_callable_method import get_contract_dates, check_company_code, \
    get_user_exception_log_message
from dxctechnology.psa_user_profile_gsap.user_profile_sync.utils.request_payload import get_put_user_payload, validate_enddate_with_startdate
from airflow.models import Variable

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/psa_user_profile_gsap/user_profile_sync/config.py


def create_add_userprofile_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_psa_userprofiles_add_child_gsap_{config.instance}',
        description=f'DXC_PSA_Child_Add User GSAP {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_user_profile_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='check_company_code_for_gsap'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='check_company_code_for_gsap',
            end_task='catch_and_log_errors',
        )

        check_company_code_for_gsap = rail.IfOperator(
            task_id='check_company_code_for_gsap',
            test=check_company_code,
            yes_task='validate_user_enddate_with_startdate',
            no_task='log_add_user_exception'
        )

        log_add_user_exception = rail.WriteLogOperator(
            task_id='log_add_user_exception',
            log='{{ dag_run.conf.log }}',
            message='User is not Created in Replicon as received company code is not GSAP contractor company code',
            properties={
                'userid': '{{ dag_run.conf.contractorpern }}',
                'email': '{{ dag_run.conf.email }}',
                'action': 'Add',
                'status': 'Exception',
                'details': 'User is not Created in Replicon as received company code is not GSAP contractor company code'
            }
        )

        validate_user_enddate_with_startdate = rail.IfOperator(
            task_id='validate_user_enddate_with_startdate',
            test=lambda dag_run: not validate_enddate_with_startdate(
                dag_run.conf['contractstartdate'], dag_run.conf['contractenddate']),
            yes_task='get_contract_startdate_enddate',
            no_task='log_user_exception'
        )

        log_user_exception = rail.WriteLogOperator(
            task_id='log_user_exception',
            log='{{ dag_run.conf.log }}',
            message='User already disabled in Replicon',
            properties={
                'userid': '{{ dag_run.conf.contractorpern }}',
                'email': '{{ dag_run.conf.email }}',
                'action': 'Update',
                'status': 'Skipped',
                'details': 'User Enddate is before the Startdate'
            }
        )

        get_contract_startdate_enddate = rail.PythonOperator(
            task_id='get_contract_startdate_enddate',
            python_callable=get_contract_dates
        )

        create_user_in_replicon = rail.RepliconServiceOperator(
            task_id='create_user_in_replicon',
            endpoint=config.put_user_service,
            data=lambda dag_run: get_put_user_payload(
                dag_run, config.should_add_emailaddress)
        )

        is_timeentry_approvalpath_exist = rail.IfOperator(
            task_id='is_timeentry_approvalpath_exist',
            test='{{ dag_run.conf.timeentry_approval_path | sn | is_truthy }}',
            yes_task='update_timeentry_approval_path',
            no_task='remove_timeoff_assignment'
        )

        update_timeentry_approval_path = rail.RepliconServiceOperator(
            task_id='update_timeentry_approval_path',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data={
                'user': {
                    'uri': '{{ result("create_user_in_replicon").uri }}'
                },
                'modifications': {
                    'timeEntryRevisionGroupApprovalPathToApply': {
                        'name': '{{ dag_run.conf.timeentry_approval_path }}'
                    }
                },
                'userModificationOptionUri': 'urn:replicon:user-modification-option:save'
            }
        )

        remove_timeoff_assignment = rail.RepliconServiceOperator(
            task_id='remove_timeoff_assignment',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data={
                'userUri': "{{ result('create_user_in_replicon').uri }}",
                'timeOffTypeUris': []
            }
        )

        put_product_assignments = rail.RepliconServiceOperator(
            task_id='put_product_assignments',
            endpoint='/services/AccountManagementService1.svc/PutProductAssignmentsForUser',
            data=lambda dag_run: {
                'userUri': rail.result('create_user_in_replicon')['uri'],
                'productUris': dag_run.conf['product_uri']
            }
        )

        should_update_supervisor = rail.IfOperator(
            task_id='should_update_supervisor',
            test=lambda dag_run: not (
                dag_run.conf['managerid'] == dag_run.conf['contractorpern']) and dag_run.conf['managerid'] != '',
            yes_task='get_data_for_supervisor',
            no_task='get_exception_logs'
        )

        (get_data_for_supervisor, assign_initial_supervisor, log_supervisor_check, get_exception_logs) = process_supervisor_assignment_task_group(
            config.execution_timeout_days)

        # pylint: disable=line-too-long
        log_add_user = rail.WriteLogOperator(
            task_id='log_add_user',
            log='{{ dag_run.conf.log }}',
            message='\
                {%- if result("get_exception_logs") | is_truthy -%} \
                    User Created Partialy - {{ result("get_exception_logs") }}\
                {%- else -%} \
                    User Created successfully\
                {%- endif -%}',
            properties={
                'userid': '{{ dag_run.conf.contractorpern }}',
                'email': '{{ dag_run.conf.email }}',
                'action': 'Add',
                'status': '\
                    {%- if result("get_exception_logs") | is_truthy -%} \
                        Exception\
                    {%- else -%} \
                        Success\
                    {%- endif -%}',
                'details': '\
                    {%- if result("get_exception_logs") | is_truthy -%} \
                        User Created Partialy - {{ result("get_exception_logs") }}\
                    {%- else -%} \
                        User Created successfully\
                    {%- endif -%}'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.log }}',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            properties={
                'userid': '{{ dag_run.conf.contractorpern }}',
                'email': '{{ dag_run.conf.email }}',
                'action': 'Add',
                'status': 'Error',
                'details': "{{ get_error_message() }}"
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label(
            'No') >> check_company_code_for_gsap

        check_company_code_for_gsap >> rail.Label(
            "Yes") >> validate_user_enddate_with_startdate

        check_company_code_for_gsap >> rail.Label(
            "No") >> log_add_user_exception >> catch_and_log_errors

        validate_user_enddate_with_startdate >> rail.Label(
            "Yes") >> get_contract_startdate_enddate

        validate_user_enddate_with_startdate >> rail.Label(
            "No") >> log_user_exception >> catch_and_log_errors

        get_contract_startdate_enddate >> create_user_in_replicon >> is_timeentry_approvalpath_exist

        is_timeentry_approvalpath_exist >> rail.Label(
            "Yes") >> update_timeentry_approval_path >> remove_timeoff_assignment

        is_timeentry_approvalpath_exist >> rail.Label(
            "No") >> remove_timeoff_assignment >> put_product_assignments >> should_update_supervisor

        assign_initial_supervisor >> get_exception_logs

        log_supervisor_check >> get_exception_logs

        should_update_supervisor >> rail.Label(
            'Yes') >> get_data_for_supervisor

        should_update_supervisor >> rail.Label(
            'No') >> get_exception_logs

        get_exception_logs >> log_add_user >> catch_and_log_errors >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_add_userprofile_child_dag)
