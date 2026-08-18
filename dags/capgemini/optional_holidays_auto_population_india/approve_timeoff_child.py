import uuid
import rail

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'capgemini_book_optional_holiday_approve_timeoff_child_{config.instance}',
        description=f'Capgemini Auto Population of Optional Holidays India Approve Timeoff Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_approve_timeoff_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        rail.RepliconServiceOperator(
            task_id='approve_timeoff',
            endpoint='/services/TimeoffapprovalService1.svc/ForceApprove',
            data={
                "timeOffUri": "{{ dag_run.conf.timeoff_uri }}",
                "unitOfWorkId": str(uuid.uuid4()),
                "comments": "Approved by Optional Holiday Admin"
            }
        )

    return dag


rail.for_each_instance(create_child_dag)
