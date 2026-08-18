import rail
from pimco.market_rate_projects.utils import python_callable_method
from pimco.market_rate_projects.utils import request_payload


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'pimco_market_rate_project_child_{config.instance}',
        description='PIMCO_Market_Rate_Project_Automation Update Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_task_details = rail.RepliconServiceOperator(
            task_id="get_task_details",
            endpoint="/services/ProjectService1.svc/BulkGetTaskDetails2",
            data=request_payload.get_task_details,
        )

        get_task_heirarchy_details = rail.PythonOperator(
            task_id='get_task_heirarchy_details',
            python_callable=python_callable_method.get_task_heirarchy_details
        )

        update_project_task_market_rate = rail.RepliconServiceOperator(
            task_id="update_project_task_market_rate",
            endpoint="/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications",
            data=request_payload.update_project_task_market_rate,
        )

        is_update_project_task_market_rate_successfull = rail.IfOperator(
            task_id='is_update_project_task_market_rate_successfull',
            test="{{result('update_project_task_market_rate') | filter_by_attr('error', 'does-not-equal', None)|is_falsy}}",
            yes_task='update_project_market_rate_success',
            no_task='fail_market_rate_update'
        )

        fail_market_rate_update = rail.FailOperator(
            task_id="fail_market_rate_update",
            message="Update Market Rate Unsucessfull",
        )

        update_project_market_rate_success = rail.EmptyOperator(
            task_id='update_project_market_rate_success'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'projectname': '{{dag_run.conf.projectname}}',
                'runid': '{{dag_run.run_id}}',
                'status': 'Error',
            },
        )

        get_task_details >> get_task_heirarchy_details >> update_project_task_market_rate
        update_project_task_market_rate >> is_update_project_task_market_rate_successfull >> rail.Label('No') >> fail_market_rate_update >> catch_and_log_errors
        is_update_project_task_market_rate_successfull >> rail.Label('Yes') >> update_project_market_rate_success >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag_wbs)
