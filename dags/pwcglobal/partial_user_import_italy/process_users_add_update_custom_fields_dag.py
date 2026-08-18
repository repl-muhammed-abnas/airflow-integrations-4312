from datetime import timedelta
import rail
from airflow.models import Variable
from pwcglobal.partial_user_import_italy.utils import request_payload, response_filter

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.process_add_update_custom_field_values_child_dag_id,
        description=f'PwC - Partial User Import Process Users Add Update Custom Field Values Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='search_user_by_partyid_and_legal_entity'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_user_by_partyid_and_legal_entity',
            end_task='log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        search_user_by_partyid_and_legal_entity = rail.RepliconServiceOperator(
            task_id='search_user_by_partyid_and_legal_entity',
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_user_by_partyid_and_legal_entity_uri_payload,
            data_handler=lambda response, dag_run: response_filter.search_user_response_filter(
                dag_run, response)
        )

        if_user_found = rail.IfOperator(
            task_id='if_user_found',
            test=lambda: bool(rail.result(
                'search_user_by_partyid_and_legal_entity')),
            yes_task='if_multiple_users_found',
            no_task='log_exception_user_not_found_entry_user_import'
        )

        log_exception_user_not_found_entry_user_import = rail.WriteLogOperator(
            task_id='log_exception_user_not_found_entry_user_import',
            log="{{ dag_run.conf.userimportlogs}}",
            severity='Exception',
            message='na',
            properties={
                'jobid': '{{dag_run.conf.jobid}}',
                'user_party_id': '{{dag_run.conf.user_party_id}}',
                'legal_entity_party_id': '{{dag_run.conf.legal_entity_party_id}}',
                'status': 'Exception',
                'details': "User not found in replicon",
                'child_job_id': '{{dag_run_ecid()}}'
            }
        )

        if_multiple_users_found = rail.IfOperator(
            task_id='if_multiple_users_found',
            test=lambda: len(rail.result(
                'search_user_by_partyid_and_legal_entity')) > 1,
            yes_task='log_exception_multiple_users_entry_user_import',
            no_task='update_user_custom_fields'
        )

        log_exception_multiple_users_entry_user_import = rail.WriteLogOperator(
            task_id='log_exception_multiple_users_entry_user_import',
            log="{{ dag_run.conf.userimportlogs}}",
            severity='Exception',
            message='na',
            properties={
                'jobid': '{{dag_run.conf.jobid}}',
                'user_party_id': '{{dag_run.conf.user_party_id}}',
                'legal_entity_party_id': '{{dag_run.conf.legal_entity_party_id}}',
                'status': 'Exception',
                'details': "Multiple Users found in replicon for the same 'User Party ID' and 'Legal Entity Party ID'",
                'child_job_id': '{{dag_run_ecid()}}'
            }
        )

        update_user_custom_fields = rail.RepliconServiceOperator(
            task_id='update_user_custom_fields',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: request_payload.applyusermodification3_payload(
                dag_run, rail.result('search_user_by_partyid_and_legal_entity')[0]['user_uri'])
        )

        log_successful_user_import = rail.WriteLogOperator(
            task_id='log_successful_user_import',
            log="{{ dag_run.conf.userimportlogs}}",
            severity='na',
            message='na',
            properties={
                'jobid': '{{dag_run.conf.jobid}}',
                'user_party_id': '{{dag_run.conf.user_party_id}}',
                'legal_entity_party_id': '{{dag_run.conf.legal_entity_party_id}}',
                'status': 'Success',
                'details': 'Remote work contract status updated.',
                'child_job_id': '{{dag_run_ecid()}}'
            }
        )

        log_errors = rail.WriteLogOperator(
            task_id='log_errors',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.userimportlogs}}",
            severity='Error',
            message='na',
            properties={
                'jobid': '{{dag_run.conf.jobid}}',
                'user_party_id': '{{dag_run.conf.user_party_id}}',
                'legal_entity_party_id': '{{dag_run.conf.legal_entity_party_id}}',
                'status': 'Error',
                'details': '{{get_error_message()}}',
                'child_job_id': '{{dag_run_ecid()}}'
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_errors
        can_run_batch_task >> rail.Label(
            'No') >> search_user_by_partyid_and_legal_entity
        search_user_by_partyid_and_legal_entity >> if_user_found
        if_user_found >> rail.Label(
            "No") >> log_exception_user_not_found_entry_user_import >> log_errors
        if_user_found >> rail.Label(
            "Yes") >> if_multiple_users_found
        if_multiple_users_found >> rail.Label(
            "Yes") >> log_exception_multiple_users_entry_user_import >> log_errors
        if_multiple_users_found >> rail.Label(
            'No') >> update_user_custom_fields
        update_user_custom_fields >> log_successful_user_import >> log_errors

    return dag


rail.for_each_instance(create_dag)
