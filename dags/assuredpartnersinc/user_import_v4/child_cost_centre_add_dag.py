from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_cost_center_add_dag_id,
        description=f'Assured Partners User Import Cost Centre Add Child {config.instance}',
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
            log="{{dag_run.conf.groups_table}}",
            properties={
                "jobid": "{{dag_run.conf.jobid}}",
                "fullpath": "{{dag_run.conf.costcenter}}",
                "type": "costcenter"
            }
        )

        if_costcentertypecostcenteruri_presence_blank_2 = rail.IfOperator(
            task_id='if_costcentertypecostcenteruri_presence_blank_2',
            test=lambda dag_run: bool(
                not (rail.result("check_logs", "length") > 0) and not (dag_run.conf['type'])),
            yes_task="if_costcentre_blank",
            no_task="if_request_type_present_costcenter_8",
        )

        if_costcentre_blank = rail.IfOperator(
            task_id='if_costcentre_blank',
            test="{{dag_run.conf.costcenter | is_falsy}}",
            yes_task="if_request_type_present_costcenter_8",
            no_task="create_cost_center_or_apply_modification_level1_4",
        )

        create_cost_center_or_apply_modification_level1_4 = rail.RepliconServiceOperator(
            task_id='create_cost_center_or_apply_modification_level1_4',
            endpoint="/services/CostCenterService1.svc/CreateCostCenterOrApplyModification",
            data={
                "costCenter": {
                    "name": null,
                    "uri": null,
                    "parent": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": "{{ dag_run.conf.costcenter }}",
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ dag_run_ecid() }}"
            }
        )

        add_entry_groups_table = rail.WriteLogOperator(
            task_id='add_entry_groups_table',
            log="{{dag_run.conf.groups_table}}",
            message='na',
            severity='na',
            properties={
                "jobid": "{{dag_run.conf.jobid}}",
                "name": "{{dag_run.conf.costcenter}}",
                "uri": "{{result('create_cost_center_or_apply_modification_level1_4').uri}}",
                "fullpath": "{{dag_run.conf.costcenter}}",
                "type": "costcenter"
            }
        )

        if_request_type_present_costcenter_8 = rail.IfOperator(
            task_id='if_request_type_present_costcenter_8',
            test='''{{ dag_run.conf.type | is_truthy }}''',
            yes_task="enable_10",
            no_task="catch_and_log_error",
        )

        enable_10 = rail.RepliconServiceOperator(
            task_id='enable_10',
            endpoint="/services/{{dag_run.conf.type}}Service1.svc/Enable",
            data={
                "{{ dag_run.conf.type }}Uri": "{{ dag_run.conf.uri }}"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{dag_run.conf.groups_table}}",
            message='na',
            severity='Error',
            properties=lambda dag_run: {
                "jobid": dag_run.conf['jobid'],
                "name": dag_run.conf['costcenter'],
                'details': rail.render_template(
                    "Error in enabling {{dag_run.conf.type}} - {{dag_run.conf.costcenter}} ; {{get_error_message()}}") if dag_run.conf['type'] else rail.render_template(
                    "Error in creating CostCentre - {{dag_run.conf.costcenter}} ; {{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> check_logs

        check_logs >> if_costcentertypecostcenteruri_presence_blank_2

        if_costcentertypecostcenteruri_presence_blank_2 >> rail.Label(
            'Yes') >> if_costcentre_blank
        if_costcentertypecostcenteruri_presence_blank_2 >> rail.Label(
            'No') >> if_request_type_present_costcenter_8

        if_costcentre_blank >> rail.Label(
            'Yes') >> if_request_type_present_costcenter_8
        if_costcentre_blank >> rail.Label(
            'No') >> create_cost_center_or_apply_modification_level1_4 >> add_entry_groups_table >> if_request_type_present_costcenter_8

        if_request_type_present_costcenter_8 >> rail.Label(
            'Yes') >> enable_10 >> catch_and_log_error
        if_request_type_present_costcenter_8 >> rail.Label(
            'No') >> catch_and_log_error

        return dag


rail.for_each_instance(create_dag)
