from datetime import timedelta
import rail
from airflow.models import Variable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'b2g_assign_remove_billing_rate_child_{config.instance}',
        description=f'B2g_assign_remove_billing_rate_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_useruri_not_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_useruri_not_present',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_useruri_not_present = rail.IfOperator(
            task_id='if_useruri_not_present',
            test="{{dag_run.conf.billing_rate_items.user_uri | is_falsy}}",
            yes_task="add_exception_entries_for_useruri",
            no_task="if_projecturi_not_present",
        )

        add_exception_entries_for_useruri = rail.WriteLogOperator(
            task_id='add_exception_entries_for_useruri',
            log="{{dag_run.conf.lookup_table}}",
            message="na",
            severity="exception",
            properties=lambda dag_run: {
                'loginname': dag_run.conf['billing_rate_items']['login_name'],
                'projectname': dag_run.conf['billing_rate_items']['project_name'],
                'billingrate': dag_run.conf['billing_rate_items']['billing_rate'],
                'action': dag_run.conf['billing_rate_items']['action'],
                'status': 'exception',
                'details': 'User is not enabled or not available in Replicon',
                'jobid': dag_run.conf['jobid'],
                'childjobid': rail.render_template("{{dag_run_ecid()}}")
            }
        )

        if_projecturi_not_present = rail.IfOperator(
            task_id='if_projecturi_not_present',
            test="{{dag_run.conf.billing_rate_items.project_uri | is_falsy}}",
            yes_task="add_exception_entries_for_projecturi",
            no_task="if_billingrateuri_not_present",
        )

        add_exception_entries_for_projecturi = rail.WriteLogOperator(
            task_id='add_exception_entries_for_projecturi',
            log="{{dag_run.conf.lookup_table}}",
            message="na",
            severity="exception",
            properties=lambda dag_run: {
                'loginname': dag_run.conf['billing_rate_items']['login_name'],
                'projectname': dag_run.conf['billing_rate_items']['project_name'],
                'billingrate': dag_run.conf['billing_rate_items']['billing_rate'],
                'action': dag_run.conf['billing_rate_items']['action'],
                'status': 'exception',
                'details': 'Project  is not available in Replicon',
                'jobid': dag_run.conf['jobid'],
                'childjobid': rail.render_template("{{dag_run_ecid()}}")
            }
        )

        if_billingrateuri_not_present = rail.IfOperator(
            task_id='if_billingrateuri_not_present',
            test="{{dag_run.conf.billingrateuri | is_falsy}}",
            yes_task="add_exception_entries_for_billingrate_uri",
            no_task="if_action_not_present",
        )

        add_exception_entries_for_billingrate_uri = rail.WriteLogOperator(
            task_id='add_exception_entries_for_billingrate_uri',
            log="{{dag_run.conf.lookup_table}}",
            message="na",
            severity="exception",
            properties=lambda dag_run: {
                'loginname': dag_run.conf['billing_rate_items']['login_name'],
                'projectname': dag_run.conf['billing_rate_items']['project_name'],
                'billingrate': dag_run.conf['billing_rate_items']['billing_rate'],
                'action': dag_run.conf['billing_rate_items']['action'],
                'status': 'exception',
                'details': 'Billing Rate is  not available in Replicon',
                'jobid': dag_run.conf['jobid'],
                'childjobid': rail.render_template("{{dag_run_ecid()}}")
            }
        )

        if_action_not_present = rail.IfOperator(
            task_id='if_action_not_present',
            test=lambda dag_run: (dag_run.conf['billing_rate_items']['action'] != 'Add') and (
                dag_run.conf['billing_rate_items']['action'] != 'Remove'),
            yes_task="add_exception_entries_for_action",
            no_task="update_billing_rate",
        )

        add_exception_entries_for_action = rail.WriteLogOperator(
            task_id='add_exception_entries_for_action',
            log="{{dag_run.conf.lookup_table}}",
            message="na",
            severity="exception",
            properties=lambda dag_run: {
                'loginname': dag_run.conf['billing_rate_items']['login_name'],
                'projectname': dag_run.conf['billing_rate_items']['project_name'],
                'billingrate': dag_run.conf['billing_rate_items']['billing_rate'],
                'action': dag_run.conf['billing_rate_items']['action'],
                'status': 'exception',
                'details': 'Invalid Status value',
                'jobid': dag_run.conf['jobid'],
                'childjobid': rail.render_template("{{dag_run_ecid()}}")
            }
        )

        update_billing_rate = rail.RepliconServiceOperator(
            task_id='update_billing_rate',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/UpdateBillingRateIsAvailableForAssignmentToTeamMembers",
            data=lambda dag_run: {
                "projectUri": dag_run.conf['billing_rate_items']['project_uri'],
                "billingRateUri": dag_run.conf['billingrateuri'],
                "billingRateAvailableForAssignmentOptionUri": "urn:replicon:billing-rate-available-for-assignment-option:available"
            }
        )

        assign_user_to_project = rail.RepliconServiceOperator(
            task_id='assign_user_to_project',
            endpoint="/services/ProjectService1.svc/AssignResourceToProject",
            data=lambda dag_run: {
                "projectUri": dag_run.conf['billing_rate_items']['project_uri'],
                "resourceUri": dag_run.conf['billing_rate_items']['user_uri']

            }
        )

        if_action_equals_add = rail.IfOperator(
            task_id='if_action_equals_add',
            test=lambda dag_run: dag_run.conf['billing_rate_items']['action'] == 'Add',
            yes_task="put_project_team_member_billing_rates_allowed_for_billing_time",
            no_task="if_action_equals_remove",
        )

        put_project_team_member_billing_rates_allowed_for_billing_time = rail.RepliconServiceOperator(
            task_id='put_project_team_member_billing_rates_allowed_for_billing_time',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/PutProjectTeamMemberBillingRatesAllowedForBillingTime",
            data=lambda dag_run: {
                "projectUri": dag_run.conf['billing_rate_items']['project_uri'],
                "resourceUri": dag_run.conf['billing_rate_items']['user_uri'],
                "billingRateUris": [dag_run.conf['billingrateuri']]
            }
        )

        add_success_entries_for_add_action = rail.WriteLogOperator(
            task_id='add_success_entries_for_add_action',
            log="{{dag_run.conf.lookup_table}}",
            message="na",
            severity="success",
            properties=lambda dag_run: {
                'loginname': dag_run.conf['billing_rate_items']['login_name'],
                'projectname': dag_run.conf['billing_rate_items']['project_name'],
                'billingrate': dag_run.conf['billing_rate_items']['billing_rate'],
                'action': dag_run.conf['billing_rate_items']['action'],
                'status': 'success',
                'details': 'Billing rate assigned to user successfully',
                'jobid': dag_run.conf['jobid'],
                'childjobid': rail.render_template("{{dag_run_ecid()}}")
            }
        )

        if_action_equals_remove = rail.IfOperator(
            task_id='if_action_equals_remove',
            test=lambda dag_run: dag_run.conf['billing_rate_items']['action'] == 'Remove',
            yes_task="put_project_team_member_billing_rates",
            no_task="add_error_entries",
        )

        put_project_team_member_billing_rates = rail.RepliconServiceOperator(
            task_id='put_project_team_member_billing_rates',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/PutProjectTeamMemberBillingRatesAllowedForBillingTime",
            data=lambda dag_run: {
                "projectUri": dag_run.conf['billing_rate_items']['project_uri'],
                "resourceUri": dag_run.conf['billing_rate_items']['user_uri'],
                "billingRateUris": []
            }
        )

        add_success_entries_for_add_remove_action = rail.WriteLogOperator(
            task_id='add_success_entries_for_remove_action',
            log="{{dag_run.conf.lookup_table}}",
            message="na",
            severity="success",
            properties=lambda dag_run: {
                'loginname': dag_run.conf['billing_rate_items']['login_name'],
                'projectname': dag_run.conf['billing_rate_items']['project_name'],
                'billingrate': dag_run.conf['billing_rate_items']['billing_rate'],
                'action': dag_run.conf['billing_rate_items']['action'],
                'status': 'success',
                'details': 'Billing rate removed successfully',
                'jobid': dag_run.conf['jobid'],
                'childjobid': rail.render_template("{{dag_run_ecid()}}")
            }
        )

        add_error_entries = rail.WriteLogOperator(
            task_id='add_error_entries',
            log="{{dag_run.conf.lookup_table}}",
            trigger_rule='one_failed',
            message="na",
            severity="error",
            properties=lambda dag_run: {
                'loginname': dag_run.conf['billing_rate_items']['login_name'],
                'projectname': dag_run.conf['billing_rate_items']['project_name'],
                'billingrate': dag_run.conf['billing_rate_items']['billing_rate'],
                'action': dag_run.conf['billing_rate_items']['action'],
                'status': 'error',
                'details': '{{get_error_message()}}',
                'jobid': dag_run.conf['jobid'],
                'childjobid': rail.render_template("{{dag_run_ecid()}}")
            }
        )

        finish_job = rail.EmptyOperator(
            task_id='finish_job'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> if_useruri_not_present
        if_useruri_not_present >> rail.Label(
            'Yes') >> add_exception_entries_for_useruri >> finish_job
        if_useruri_not_present >> rail.Label(
            'No') >> if_projecturi_not_present >> rail.Label('Yes') >> add_exception_entries_for_projecturi >> finish_job
        if_projecturi_not_present >> rail.Label(
            'No') >> if_billingrateuri_not_present >> rail.Label('Yes') >> add_exception_entries_for_billingrate_uri >> finish_job
        if_billingrateuri_not_present >> rail.Label(
            'No') >> if_action_not_present >> rail.Label('Yes') >> add_exception_entries_for_action >> finish_job
        if_action_not_present >> rail.Label(
            'No') >> update_billing_rate >> assign_user_to_project >> if_action_equals_add
        if_action_equals_add >> rail.Label(
            'Yes') >> put_project_team_member_billing_rates_allowed_for_billing_time >> add_success_entries_for_add_action
        add_success_entries_for_add_action >> finish_job
        if_action_equals_add >> rail.Label(
            'No') >> if_action_equals_remove >> rail.Label('Yes') >> put_project_team_member_billing_rates
        put_project_team_member_billing_rates >> add_success_entries_for_add_remove_action >> finish_job
        if_action_equals_remove >> rail.Label(
            'No') >> add_error_entries >> finish_job >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
