from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_location_add_dag_id,
        description=f'Assured Partners User Import Location Add Child {config.instance}',
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
            no_task='check_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='check_logs',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        check_logs = rail.FilterLogEntriesOperator(
            task_id='check_logs',
            properties={
                "jobid": "{{dag_run.conf.jobid}}",
                "fullpath": "{{dag_run.conf.location}}",
                "type": "location"
            }
        )

        if_costcentertypecostcenteruri_presence_blank_2 = rail.IfOperator(
            task_id='if_costcentertypecostcenteruri_presence_blank_2',
            test=lambda dag_run: bool(
                not (rail.result("check_logs", "length") > 0)),
            yes_task="if_location_blank",
            no_task="catch_and_log_error",
        )
        
        if_location_blank = rail.IfOperator(
            task_id='if_location_blank',
            test="{{dag_run.conf.location | is_falsy}}",
            yes_task="catch_and_log_error",
            no_task="create_location_or_apply_modification_level1_4",
        )

        create_location_or_apply_modification_level1_4 = rail.RepliconServiceOperator(
            task_id='create_location_or_apply_modification_level1_4',
            endpoint="/services/LocationService1.svc/CreateLocationOrApplyModification",
            data={
                "location": {
                    "name": null,
                    "uri": null,
                    "parent": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": "{{ dag_run.conf.location }}",
                    "codeToApply": null,
                    "descriptionToApply": {
                        "value": "{{ dag_run.conf.locationdescription }}"
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
                "name": "{{dag_run.conf.location}}",
                "details": "Error in creating Location - {{dag_run.conf.location}} ; {{get_error_message()}}"
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> check_logs

        check_logs >> if_costcentertypecostcenteruri_presence_blank_2

        if_costcentertypecostcenteruri_presence_blank_2 >> rail.Label(
            'No') >> catch_and_log_error
        if_costcentertypecostcenteruri_presence_blank_2 >> rail.Label(
            'No') >> if_location_blank 
        
        if_location_blank >> rail.Label('No') >> catch_and_log_error
        if_location_blank >> rail.Label('Yes') >> create_location_or_apply_modification_level1_4
        
        create_location_or_apply_modification_level1_4 >> catch_and_log_error

        return dag


rail.for_each_instance(create_dag)
