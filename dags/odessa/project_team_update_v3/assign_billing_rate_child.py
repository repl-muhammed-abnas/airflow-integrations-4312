from datetime import timedelta
import rail

from odessa.project_team_update_v3.utils import custom_method
from odessa.project_team_update_v3.utils import request_payload


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.assign_billing_rate_child_dag_id,
        description=f"odessa_project_team_update_assign_billing_rate_child_v3_{config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.billing_rate_child_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        has_customerrole_and_rate = rail.IfOperator(
            task_id="has_customerrole_and_rate",
            test=lambda dag_run: custom_method.is_meaningful(dag_run.conf.get("customerrole"))
            and custom_method.is_meaningful(dag_run.conf.get("billingrateuri")),
            yes_task="update_default_billing_rate",
            no_task="end",
        )

        update_default_billing_rate = rail.RepliconServiceOperator(
            task_id="update_default_billing_rate",
            endpoint="/services/ClientService1.svc/UpdateBillingRateIsAllowedByDefaultOnNewProjects",
            retries=3,
            retry_delay=timedelta(seconds=10),
            data=lambda dag_run: request_payload.update_default_billing_rate(
                dag_run.conf["clienturi"], dag_run.conf["billingrateuri"]),
        )

        put_member_billing_rates = rail.RepliconServiceOperator(
            task_id="put_member_billing_rates",
            endpoint="/services/TimeAndMaterialsProjectService1.svc/PutProjectTeamMemberBillingRatesAllowedForBillingTime",
            retries=3,
            retry_delay=timedelta(seconds=10),
            data=lambda dag_run: request_payload.put_member_billing_rates(
                dag_run.conf["projecturi"],
                dag_run.conf["useruri"],
                dag_run.conf["billingrateuri"]),
        )

        end = rail.EmptyOperator(task_id="end")

        has_customerrole_and_rate >> rail.Label("Yes") >> update_default_billing_rate \
            >> put_member_billing_rates >> end
        has_customerrole_and_rate >> rail.Label("No") >> end

    return dag


rail.for_each_instance(create_child_dag)
