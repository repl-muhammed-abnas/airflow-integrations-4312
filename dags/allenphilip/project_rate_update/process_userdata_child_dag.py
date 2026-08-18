from datetime import timedelta
import rail
from airflow.models import Variable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'allenphilip_project_rate_update_userdata_child_{config.instance}',
        description=f'Allenphilip__project_rate_update_userdata_child {config.instance}',
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
                config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='allocate_user_to_project'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='allocate_user_to_project',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        allocate_user_to_project = rail.RepliconServiceOperator(
            task_id='allocate_user_to_project',
            endpoint="/services/ProjectService1.svc/AssignResourceToProject",
            data=lambda dag_run: {
                "projectUri": dag_run.conf['project_uri'],
                "resourceUri": dag_run.conf['userdata_items']['useruri'],

            }
        )

        put_project_to_teammembers_for_billing_rates = rail.RepliconServiceOperator(
            task_id='put_project_to_teammembers_for_billing_rates',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/PutProjectTeamMemberBillingRatesAllowedForBillingTime",
            data=lambda dag_run: {
                "projectUri": dag_run.conf['project_uri'],
                "resourceUri": dag_run.conf['userdata_items']['useruri'],
                "billingRateUris": ["urn:replicon:user-specific-billing-rate"]
            }
        )

        add_item_to_list = rail.WriteLogOperator(
            task_id='add_item_to_list',
            log="{{ dag_run.conf.user_lookuptable}}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "loginname": dag_run.conf['userdata_items']['loginname'],
                "useruri": dag_run.conf['userdata_items']['useruri'],
            }
        )

        log_success_entries = rail.WriteLogOperator(
            task_id='log_success_entries',
            log="{{ dag_run.conf.log_table }}",
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['job_id'],
                "loginname": dag_run.conf['userdata_items']['loginname'],
                "projectname":  dag_run.conf['project_name'],
                "defaultbillingrate": dag_run.conf['userdata_items']['defaultbillingrate'],
                "status": "Success",
                "details": "Assigned to Project",
                "childjobid": rail.render_template("{{dag_run_ecid()}}")
            }
        )

        end_task = rail.EmptyOperator(
            task_id='end_task'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> allocate_user_to_project
        allocate_user_to_project >> put_project_to_teammembers_for_billing_rates
        put_project_to_teammembers_for_billing_rates >> add_item_to_list >> log_success_entries >> end_task >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
