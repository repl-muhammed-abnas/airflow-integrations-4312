
from datetime import timedelta
import uuid
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.employee_type_add_child_dag_id,
        description=f'VelawG3 Child_employee type add V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_employeetype_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_employeetype_present',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        is_employeetype_present = rail.IfOperator(
            task_id='is_employeetype_present',
            test='''{{ dag_run.conf.employeetype | is_truthy }}''',
            yes_task="create_employee_type_group_or_apply_modification_3",
            no_task="log_to_sumo",
        )

        create_employee_type_group_or_apply_modification_3 = rail.RepliconServiceOperator(
            task_id='create_employee_type_group_or_apply_modification_3',
            endpoint="/services/EmployeeTypeGroupService1.svc/CreateEmployeeTypeGroupOrApplyModification",
            data={
                "employeeTypeGroup": {
                    "uri": null,
                    "parent": null,
                    "name": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": "{{ dag_run.conf.employeetype }}",
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> is_employeetype_present
        is_employeetype_present >> rail.Label(
            'Yes') >> create_employee_type_group_or_apply_modification_3 >> log_to_sumo
        is_employeetype_present >> rail.Label(
            'No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
