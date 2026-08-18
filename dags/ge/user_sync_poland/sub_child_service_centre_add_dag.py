from datetime import timedelta
from airflow.models import Variable
import rail
import uuid

null = None


def create_dag(config):
    # pylnot: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.sub_child_legacy_payroll_id_servicecenter_add_dag_id,
        description=f'GE POLAND User Import Add Legacy payroll ID Service Center Sub-Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_sub_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_request_type_equals_to_enable_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_request_type_equals_to_enable_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_request_type_equals_to_enable_3 = rail.IfOperator(
            task_id='if_request_type_equals_to_enable_3',
            test='''{{ dag_run.conf.type == 'enable' }}''',
            yes_task="enable_servicecenter_4",
            no_task="create_service_center_or_apply_modification_6",
        )

        enable_servicecenter_4 = rail.RepliconServiceOperator(
            task_id='enable_servicecenter_4',
            endpoint="/services/ServiceCenterService1.svc/Enable",
            data={
                "serviceCenterUri": "{{ dag_run.conf.servicecentreuri }}"
            }
        )

        create_service_center_or_apply_modification_6 = rail.RepliconServiceOperator(
            task_id='create_service_center_or_apply_modification_6',
            endpoint="/services/ServiceCenterService1.svc/CreateServiceCenterOrApplyModification",
            data=lambda dag_run: {
                "serviceCenter": {
                    "name": null,
                    "uri": null,
                    "parent": {
                        "name": null,
                        "uri": dag_run.conf['parenturi'],
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": dag_run.conf['servicecenter'],
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> if_request_type_equals_to_enable_3

        if_request_type_equals_to_enable_3 >> rail.Label(
            'Yes') >> enable_servicecenter_4 >> finish
        if_request_type_equals_to_enable_3 >> rail.Label(
            'No') >> create_service_center_or_apply_modification_6 >> finish

    return dag


rail.for_each_instance(create_dag)
