from datetime import timedelta
import rail
from airflow.models import Variable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'allenphilip_project_rate_update_supplied_users_child_{config.instance}',
        description=f'Allenphilip__project_rate_update_supplied_users_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child_supplier,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_amount_and_currencysymbol_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_amount_and_currencysymbol_present',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_amount_and_currencysymbol_present = rail.IfOperator(
            task_id='if_amount_and_currencysymbol_present',
            test=lambda dag_run: dag_run.conf['user_rate'] and dag_run.conf['currency_symbol'],
            yes_task="if_defaultbillingrate_equals_currencysymbol",
            no_task="put_project_to_unassign_other_billing_rates",
        )

        if_defaultbillingrate_equals_currencysymbol = rail.IfOperator(
            task_id='if_defaultbillingrate_equals_currencysymbol',
            test=lambda dag_run: dag_run.conf['suppliedusers'][
                'defaultbillingrate'][:4] == dag_run.conf['currency_symbol'],
            yes_task="if_split_last_equals_to_log_userratein_project",
            no_task="add_entry_for_defaultbillingrate_not_equals_currencysymbol",
        )

        if_split_last_equals_to_log_userratein_project = rail.IfOperator(
            task_id='if_split_last_equals_to_log_userratein_project',
            test=lambda dag_run: bool(float((dag_run.conf['suppliedusers'][
                'defaultbillingrate'].split('$')[-1]).replace(",", "")) == float(dag_run.conf['user_rate'])),
            yes_task="put_project_for_team_member_billing_rates",
            no_task="add_entry_for_defaultbillingrate_not_equals_userrate",
        )

        put_project_for_team_member_billing_rates = rail.RepliconServiceOperator(
            task_id='put_project_for_team_member_billing_rates',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/PutProjectTeamMemberBillingRatesAllowedForBillingTime",
            data=lambda dag_run: {
                "projectUri": dag_run.conf['project_uri'],
                "resourceUri": dag_run.conf['suppliedusers']['useruri'],
                "billingRateUris": ["urn:replicon:user-specific-billing-rate"]
            }
        )

        insert_to_users_to_be_added_to_tasks_list = rail.WriteLogOperator(
            task_id='insert_to_users_to_be_added_to_tasks_list',
            log="{{ dag_run.conf.user_lookuptable }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "loginname": dag_run.conf['suppliedusers']['loginname'],
                "useruri": dag_run.conf['suppliedusers']['useruri']
            }
        )

        add_entry_for_defaultbillingrate_not_equals_userrate = rail.WriteLogOperator(
            task_id='add_entry_for_defaultbillingrate_not_equals_userrate',
            log="{{ dag_run.conf.log_table }}",
            message="na",
            severity="Ignored",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['job_id'],
                "loginname": dag_run.conf['suppliedusers']['loginname'],
                "projectname": dag_run.conf['project_name'],
                "defaultbillingrate": dag_run.conf['suppliedusers']['defaultbillingrate'],
                "status": "Ignored",
                "details": "User Billing rate is different in Project -" + str(dag_run.conf['currency_symbol']) + str(dag_run.conf['user_rate']),
                "childjobid": rail.render_template("{{dag_run_ecid()}}")
            }
        )

        add_entry_for_defaultbillingrate_not_equals_currencysymbol = rail.WriteLogOperator(
            task_id='add_entry_for_defaultbillingrate_not_equals_currencysymbol',
            log="{{ dag_run.conf.log_table }}",
            message="na",
            severity="Ignored",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['job_id'],
                "loginname": dag_run.conf['suppliedusers']['loginname'],
                "projectname": dag_run.conf['project_name'],
                "defaultbillingrate": dag_run.conf['suppliedusers']['defaultbillingrate'],
                "status": "Ignored",
                "details": "User Billing Rate Currency is different in Project -" + str(dag_run.conf['currency_symbol']),
                "childjobid": rail.render_template("{{dag_run_ecid()}}"),
            }
        )

        put_project_to_unassign_other_billing_rates = rail.RepliconServiceOperator(
            task_id='put_project_to_unassign_other_billing_rates',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/PutProjectTeamMemberBillingRatesAllowedForBillingTime",
            data=lambda dag_run: {
                "projectUri": dag_run.conf['project_uri'],
                "resourceUri": dag_run.conf['suppliedusers']['useruri'],
                "billingRateUris": ["urn:replicon:user-specific-billing-rate"]
            }
        )

        insert_to_users_to_be_added = rail.WriteLogOperator(
            task_id='insert_to_users_to_be_added',
            log="{{ dag_run.conf.user_lookuptable }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "loginname": dag_run.conf['suppliedusers']['loginname'],
                "useruri": dag_run.conf['suppliedusers']['useruri']
            }
        )

        stop = rail.EmptyOperator(
            task_id='stop'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> if_amount_and_currencysymbol_present
        if_amount_and_currencysymbol_present >> rail.Label(
            'Yes') >> if_defaultbillingrate_equals_currencysymbol
        if_defaultbillingrate_equals_currencysymbol >> rail.Label(
            'Yes') >> if_split_last_equals_to_log_userratein_project
        if_split_last_equals_to_log_userratein_project >> rail.Label(
            'Yes') >> put_project_for_team_member_billing_rates >> insert_to_users_to_be_added_to_tasks_list
        insert_to_users_to_be_added_to_tasks_list >> stop
        if_split_last_equals_to_log_userratein_project >> rail.Label(
            'No') >> add_entry_for_defaultbillingrate_not_equals_userrate >> stop
        if_defaultbillingrate_equals_currencysymbol >> rail.Label(
            'No') >> add_entry_for_defaultbillingrate_not_equals_currencysymbol >> stop
        if_amount_and_currencysymbol_present >> rail.Label(
            'No') >> put_project_to_unassign_other_billing_rates >> insert_to_users_to_be_added
        insert_to_users_to_be_added >> stop >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
