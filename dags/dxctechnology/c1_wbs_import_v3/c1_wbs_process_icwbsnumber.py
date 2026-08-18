import rail
from dxctechnology.c1_wbs_import_v3.utils import request_payload

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_wbs_import_v3/config.py


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id_icwbsnumber,
        description=f'DXC_C1_WBS_Automation Can Update ICWBSNumber {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.client_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_project_info_based_on_icwbsnumber = rail.RepliconServiceOperator(
            task_id='get_project_info_based_on_icwbsnumber',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={
                "projects": [
                    {
                        "name": "{{ dag_run.conf.ICWBSNumber }}"
                    }
                ]
            }
        )

        is_project_exist = rail.IfOperator(
            task_id="is_project_exist",
            test=request_payload.is_icwbs_project_exist,
            yes_task="update_iwo_wbs_oef",
            no_task="end",
        )

        update_iwo_wbs_oef = rail.RepliconServiceOperator(
            task_id='update_iwo_wbs_oef',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.get_icwbs_iwo_oef_update_param
        )

        end = rail.EmptyOperator(
            task_id='end'
        )

        get_project_info_based_on_icwbsnumber >> is_project_exist >> rail.Label(
            "Yes") >> update_iwo_wbs_oef >> end

        is_project_exist >> rail.Label(
            "No") >> end

    return dag


rail.for_each_instance(create_child_dag)
