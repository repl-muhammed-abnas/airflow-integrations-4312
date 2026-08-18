from airflow.models import Variable
from datetime import timedelta
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.child_service_center_add_dag_id,
        description=f'Assured Partners User Import Service Centre Add Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_add_groups,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_service_centre_blank'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_service_centre_blank',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_service_centre_blank = rail.IfOperator(
            task_id='if_service_centre_blank',
            test="{{dag_run.conf.servicecenter | is_falsy}}",
            yes_task="catch_and_log_error",
            no_task="create_service_center_or_apply_modification_3",
        )

        create_service_center_or_apply_modification_3 = rail.RepliconServiceOperator(
            task_id='create_service_center_or_apply_modification_3',
            endpoint="/services/ServiceCenterService1.svc/CreateServiceCenterOrApplyModification",
            data={
                "serviceCenter": {
                    "name": null,
                    "uri": null,
                    "parent": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": "{{ dag_run.conf.servicecenter }}",
                    "codeToApply": null,
                    "descriptionToApply": {
                        "value": "{{ dag_run.conf.servicecenterdescription }}"
                    },
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ dag_run_ecid() }}"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{dag_run.conf.groups_table}}",
            message='na',
            severity='Error',
            properties={
                "jobid": "{{dag_run.conf.jobid}}",
                "name": "{{dag_run.conf.servicecenter}}",
                "details": "Error in creating Service center - {{dag_run.conf.servicecenter}} ; {{get_error_message()}} "
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> if_service_centre_blank
        
        if_service_centre_blank >> rail.Label('No') >> catch_and_log_error
        if_service_centre_blank >> rail.Label('Yes') >> create_service_center_or_apply_modification_3

        create_service_center_or_apply_modification_3 >> catch_and_log_error

        return dag


rail.for_each_instance(create_dag)
