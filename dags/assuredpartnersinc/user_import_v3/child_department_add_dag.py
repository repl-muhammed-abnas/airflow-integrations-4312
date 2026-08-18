from airflow.models import Variable
from datetime import timedelta
import uuid
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_department_add_dag_id,
        description=f'Assured Partners User Import Department add Child {config.instance}',
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
            no_task='create_department_group_or_apply_modification_level2_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_department_group_or_apply_modification_level2_3',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_department_group_or_apply_modification_level2_3 = rail.RepliconServiceOperator(
            task_id='create_department_group_or_apply_modification_level2_3',
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=lambda dag_run: {
                "departmentGroup": {
                    "uri": null,
                    "parent": {
                        "uri": dag_run.conf['compaydepturi']
                    },
                    "name": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": dag_run.conf['department'],
                    "descriptionToApply": {
                        "value": dag_run.conf['description']
                    },
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4())
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
                "name": "{{dag_run.conf.department}}",
                "details": "Error in creating Department Group - {{dag_run.conf.department}} ; {{get_error_message()}}"
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> create_department_group_or_apply_modification_level2_3

        create_department_group_or_apply_modification_level2_3 >> catch_and_log_error

        return dag


rail.for_each_instance(create_dag)
