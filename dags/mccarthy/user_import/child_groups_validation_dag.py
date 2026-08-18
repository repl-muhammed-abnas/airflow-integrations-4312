from datetime import timedelta
import uuid
from airflow.models import Variable
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/mccarthy/user_import/config.py


def create_groups_validation_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'mccarthy_user_import_groups_validation_child_{config.instance}',
        description=f'LIVE | Mccarthy_User_Import_Groups_Validation- Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_grouptype_employeetype'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_grouptype_employeetype',
            end_task='dagrun_log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        is_grouptype_employeetype = rail.IfOperator(
            task_id='is_grouptype_employeetype',
            test="{{ dag_run.conf.grouptype == 'Employee Type' }}",
            yes_task="get_enabled_employeetype_groups",
            no_task="dagrun_log_to_sumo"
        )

        get_enabled_employeetype_groups = rail.RepliconServiceOperator(
            task_id='get_enabled_employeetype_groups',
            endpoint="/services/EmployeeTypeGroupService1.svc/GetEnabledEmployeeTypeGroups"
        )

        create_repliconemployeetype_group_collection = rail.CreateCollectionOperator(
            task_id='create_repliconemployeetype_group_collection',
            source=lambda: rail.result('get_enabled_employeetype_groups'),
            name="groupvaluesinreplicon"
        )

        get_new_employeetypes_to_create = rail.QueryCollectionOperator(
            task_id='get_new_employeetypes_to_create',
            query="""SELECT * FROM groupvaluesfromfeedfile WHERE
                    displayText NOT IN (SELECT DISTINCT displayText FROM groupvaluesinreplicon)"""
        )

        is_new_employeetypes_to_create = rail.IfOperator(
            task_id='is_new_employeetypes_to_create',
            test="{{ result('get_new_employeetypes_to_create', 'length') > 0 }}",
            yes_task="create_employeetype_group_or_apply_modification",
            no_task="dagrun_log_to_sumo"
        )

        create_employeetype_group_or_apply_modification = rail.RepliconServiceCallForEachItemOperator(
            task_id='create_employeetype_group_or_apply_modification',
            items="{{ result('get_new_employeetypes_to_create') }}",
            endpoint="/services/EmployeeTypeGroupService1.svc/CreateEmployeeTypeGroupOrApplyModification",
            data=lambda item: {
                "modifications": {
                    "name": item['displayText'],
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> is_grouptype_employeetype
        is_grouptype_employeetype >> rail.Label(
            'Yes') >> get_enabled_employeetype_groups >> create_repliconemployeetype_group_collection >> \
            get_new_employeetypes_to_create >> is_new_employeetypes_to_create
        is_new_employeetypes_to_create >> rail.Label(
            'Yes') >> create_employeetype_group_or_apply_modification >> dagrun_log_to_sumo
        is_new_employeetypes_to_create >> rail.Label(
            'No') >> dagrun_log_to_sumo
        is_grouptype_employeetype >> rail.Label(
            'No') >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_groups_validation_dag)
