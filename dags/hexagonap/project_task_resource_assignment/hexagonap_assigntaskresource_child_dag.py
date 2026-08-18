
from datetime import timedelta
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'hexagonap_project_task_resource_assignment_assign_task_resource_child_{config.instance}',
        description=f'hexagonap_assign task resource {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='update_resource_assignment'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='update_resource_assignment',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        update_resource_assignment=rail.RepliconServiceOperator(
            task_id='update_resource_assignment',
            endpoint="/services/TaskService1.svc/UpdateResourceAssignment",
            data=lambda dag_run:{
                "taskUri": dag_run.conf['taskuri'],
                "resourceUri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:department:1",
                "isAssigned": "true"
            }
        )

        add_log_resources_assigned=rail.WriteLogOperator(
            task_id='add_log_resources_assigned',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Success",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "projectname": "{{ dag_run.conf.projectname }}",
                "taskname": "{{ dag_run.conf.taskname }}",
                "taskuri": "{{ dag_run.conf.taskuri }}",
                "status": "Success",
                "details": '',
                "childjob": ''
            }
        )

        catch_and_log_error=rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.logslookuptable  }}",
            trigger_rule='one_failed',
            message="na",
            severity="Failed",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "projectname": "{{ dag_run.conf.projectname }}",
                "taskname": "{{ dag_run.conf.taskname }}",
                "taskuri": "{{ dag_run.conf.taskuri }}",
                "status": "Failed",
                "details": "{{get_error_message()}}",
                "childjob": ''
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> update_resource_assignment
        update_resource_assignment >> add_log_resources_assigned >> catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
