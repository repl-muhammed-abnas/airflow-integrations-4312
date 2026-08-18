from datetime import timedelta
import rail
from pwcglobal.supervisor_org_group_import.utils import python_callable, request_payload


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.add_dagid,
        description=f'PwC Supervisory Org Custom Import Add Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_add_child_logs = rail.CreateLogOperator(
            task_id = 'create_add_child_logs'
        )

        if_no_supervisory_org_to_add = rail.IfOperator(
            task_id='if_no_supervisory_org_to_add',
            test=lambda dag_run: not bool(dag_run.conf['child']),
            yes_task="log_no_supervisory_org_to_add",
            no_task="create_supervisory_org",
        )

        create_supervisory_org = rail.RepliconServiceOperator(
            task_id='create_supervisory_org',
            endpoint='/services/CostCenterService1.svc/CreateCostCenterHierarchyOrApplyModifications',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            data=request_payload.get_add_conf
        )

        prepare_log_to_add = rail.PythonOperator(
            task_id='prepare_log_to_add',
            python_callable=python_callable.prepare_log_to_add
        )

        success_error_log = rail.WriteLogOperator(
            task_id='success_error_log',
            log="{{ result('create_add_child_logs') }}",
            message="Success",
            severity="Success",
            properties={
                "Supervisory Org": "{{ result('prepare_log_to_add').added_levels }}",
                "Action": "Add",
                "Status": "{{ result('prepare_log_to_add').status }}",
                "Details": "{{ result('prepare_log_to_add').details }}"
            }
        )

        log_no_supervisory_org_to_add = rail.WriteLogOperator(
            task_id='log_no_supervisory_org_to_add',
            log="{{ result('create_add_child_logs') }}",
            message="Exception",
            severity="Exception",
            properties={
                "Supervisory Org": "{{ dag_run.conf.parents }}",
                "Action": "Add",
                "Status": "Exception",
                "Details": "Supervisory Org already present in replicon."
            }
        )

        finish =  rail.EmptyOperator(
            task_id='finish'
        )

        create_add_child_logs >> if_no_supervisory_org_to_add >> rail.Label("Yes") >> log_no_supervisory_org_to_add >> finish
        if_no_supervisory_org_to_add >> rail.Label("No") >> create_supervisory_org

        create_supervisory_org >> prepare_log_to_add >> success_error_log >> finish

    return dag

rail.for_each_instance(create_dag)
