from airflow.models import Variable
from datetime import timedelta
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_employee_type_add_dag_id,
        description=f'Assured Partners User Import Employee Type Add Child {config.instance}',
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
            no_task='if_employeetype_blank'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_employeetype_blank',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )
        
        if_employeetype_blank = rail.IfOperator(
            task_id='if_employeetype_blank',
            test="{{dag_run.conf.employeetype | is_falsy}}",
            yes_task="catch_and_log_error",
            no_task="create_employee_type_group_or_apply_modification_3",
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
                    "descriptionToApply": {
                        "value": "{{ dag_run.conf.description }}"
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
                "name": "{{dag_run.conf.employeetype}}",
                "details": "Error creating employee type group - {{dag_run.conf.employeetype}}; {{get_error_message()}} "
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> if_employeetype_blank
        
        if_employeetype_blank >> rail.Label('No') >> catch_and_log_error
        if_employeetype_blank >> rail.Label('Yes') >> create_employee_type_group_or_apply_modification_3

        create_employee_type_group_or_apply_modification_3 >> catch_and_log_error

        return dag


rail.for_each_instance(create_dag)
