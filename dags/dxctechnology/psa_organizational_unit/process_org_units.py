from datetime import timedelta
import rail
from dxctechnology.psa_organizational_unit.utils import request_payload
from dxctechnology.psa_organizational_unit.utils import python_callable_method
from airflow.models import Variable
from rail.lib.ecid import get_dagrun_ecid

# pylint: disable=too-many-statements
def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.dxctechnology_psa_process_organizational_units_child,
        description='DXC PSA Organization Unit Import',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_org_units,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_log',
            end_task='catch_and_log_errors',
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        is_org_unit_available = rail.IfOperator(
            task_id='is_org_unit_available',
            test='{{ dag_run.conf.organization_unit_uri | is_truthy }}',
            yes_task='is_org_unit_under_psa',
            no_task='create_organizational_unit'
        )

        create_organizational_unit = rail.RepliconServiceOperator(
            task_id="create_organizational_unit",
            endpoint="/services/DepartmentGroupService1.svc/CreateDepartmentGroupOrApplyModification",
            data=request_payload.create_organizational_unit,
        )

        is_org_unit_under_psa = rail.IfOperator(
            task_id='is_org_unit_under_psa',
            test=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                dag_run.conf['current_organization_unit_parent'], 'textValue', 'PSA Org Unit', 'textValue') == 'PSA Org Unit',
            yes_task='log_completion',
            no_task='move_under_psa'
        )

        move_under_psa = rail.RepliconServiceOperator(
            task_id="move_under_psa",
            endpoint="/services/DepartmentGroupService1.svc/MoveDepartmentGroup",
            data=request_payload.move_under_psa,
        )

        log_completion = rail.WriteLogOperator(
            task_id='log_completion',
            log='{{ result("create_log") }}',
            message=lambda dag_run: 'Organization Unit Added to PSA Org Unit Hierarchy'
                if not rail.find_first_by_attr_and_get_attr(dag_run.conf['current_organization_unit_parent'], 'textValue', 'PSA Org Unit')
                    else "Organization Unit already exists as part of PSA Org Unit Hierarchy",
            severity=python_callable_method.get_status,
            properties=lambda dag_run: {
                'organization_unit_cd': dag_run.conf["organization_unit_cd"],
                'status': python_callable_method.get_status(dag_run),
                'details': 'Organization Unit Added to PSA Org Unit Hierarchy'
                    if not rail.find_first_by_attr_and_get_attr(dag_run.conf['current_organization_unit_parent'], 'textValue', 'PSA Org Unit')
                        else "Organization Unit already exists as part of PSA Org Unit Hierarchy",
                'ecid': get_dagrun_ecid(dag_run)
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("create_log") }}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'organization_unit_cd': '{{ dag_run.conf.organization_unit_cd }}',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
                'ecid': '{{ dag_run_ecid() }}'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> create_log

        create_log >> is_org_unit_available
        is_org_unit_available >> rail.Label(
            'No') >> create_organizational_unit >> log_completion
        is_org_unit_available >> rail.Label(
            'Yes') >> is_org_unit_under_psa >> rail.Label('Yes') >> log_completion
        is_org_unit_under_psa >> rail.Label(
            'No') >> move_under_psa >> log_completion >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag_wbs)
