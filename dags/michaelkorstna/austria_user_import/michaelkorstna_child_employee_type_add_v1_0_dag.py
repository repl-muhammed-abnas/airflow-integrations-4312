
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'michaelkorstna_austria_user_import_employee_type_add_child_{config.instance}',
        description=f'MichaelKorsTnA Child_employee type add V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_groups,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_employee_type_group_or_apply_modification_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_employee_type_group_or_apply_modification_3',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
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
                "unitOfWorkId": "{{ dag_run_ecid() }}"
            }
        )

        michael_kors_gmbh_groups_table_add_entry_4 = rail.WriteLogOperator(
            task_id='michael_kors_gmbh_groups_table_add_entry_4',
            log="{{ dag_run.conf.groupslookuptable }}",
            message="na",
            severity="na",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "name": "{{ result('create_employee_type_group_or_apply_modification_3').displayText }}",
                "uri": "{{ result('create_employee_type_group_or_apply_modification_3').uri }}",
                "fullpath": "{{ result('create_employee_type_group_or_apply_modification_3').displayText }}",
                "type": "employeetype"
            }
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "{{get_error_message()}}")
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label(
            'No') >> create_employee_type_group_or_apply_modification_3
        create_employee_type_group_or_apply_modification_3 >> michael_kors_gmbh_groups_table_add_entry_4 >> catch_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
