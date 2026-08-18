import rail
from tsystems.project_team_assignment_v3.utils import request_payload

def create_child_dag(config):

    add_dags = []

    for idx in range(0, config.ALLOCATION_BATCH_COUNT):
        get_postfix = "" if idx == 0 else f'_batch_{idx}'

        with rail.create_airflow_dag(
            dag_id=f"{config.individual_allocation_per_day_dag_id}{get_postfix}",
            description=f'T-Systems Project Team Assignment - process individual allocation {config.instance}',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.allocation_child_max_active_runs,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            allocate_user_per_day = rail.RepliconServiceOperator(
                task_id='allocate_user_per_day',
                endpoint='/services/ResourceService1.svc/PutProjectResourceAllocation',
                data=request_payload.per_day_allocation_payload
            )

            log_allocation_successfull = rail.WriteLogOperator(
                task_id='log_allocation_successfull',
                log='{{ dag_run.conf.logger }}',
                message='Allocation processed successfully',
                severity='Success',
                properties={
                    'assignment_id':'{{ dag_run.conf.assignment_id }}',
                    'decidalo_project_id': '{{ dag_run.conf.decidalo_project_id }}',
                    'individual_id': '{{ dag_run.conf.individual_id }}',
                    'cost_object_id': '{{ dag_run.conf.cost_object_id }}',
                    'search_period_start': '{{ dag_run.conf.allocation_date }}',
                    'search_period_end': '{{ dag_run.conf.allocation_date }}',
                    'hours': '{{ dag_run.conf.capacity_amount }}', 
                    'status': 'Success',
                    'details': 'Allocation processed successfully',
                }
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                log='{{ dag_run.conf.logger }}',
                trigger_rule='one_failed',
                message='{{ get_error_message() }}',
                severity='Error',
                properties={
                    'assignment_id':'{{ dag_run.conf.assignment_id }}',
                    'decidalo_project_id': '{{ dag_run.conf.decidalo_project_id }}',
                    'individual_id': '{{ dag_run.conf.individual_id }}',
                    'cost_object_id': '{{ dag_run.conf.cost_object_id }}',
                    'search_period_start': '{{ dag_run.conf.allocation_date }}',
                    'search_period_end': '{{ dag_run.conf.allocation_date }}',
                    'hours': '{{ dag_run.conf.capacity_amount }}',
                    'status': "Error",
                    'details': '{{ get_error_message() }}'
                }
            )

            allocate_user_per_day >> log_allocation_successfull >> catch_and_log_errors

        add_dags.append(dag)

    return add_dags

rail.for_each_instance(create_child_dag)
