import rail
from dxctechnology.compass_iwo_details_v1.utils import request_payload
from dxctechnology.compass_iwo_details_v1.utils import python_callable_method


def create_process_iwowbselement_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_iwo_process_iwowbselement_child_{config.dag_id_postfix}',
        description=f'DXC COMPASS IWO process iwo wbs element Update {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        is_parent_present = rail.IfOperator(
            task_id="is_parent_present",
            test="{{dag_run.conf.parent | is_truthy}}",
            yes_task="get_parent_project_details"
        )
        get_parent_project_details = rail.RepliconServiceOperator(
            task_id='get_parent_project_details',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=lambda dag_run: {
                "projects": [
                    {
                        "name": dag_run.conf['parent'],
                    }
                ]
            },
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                {"projectDetails": None}])[0]['projectDetails']
        )

        does_parent_project_exist = rail.IfOperator(
            task_id='does_parent_project_exist',
            test=lambda: bool(rail.result(
                'get_parent_project_details') and rail.result(
                'get_parent_project_details')['uri']),
            yes_task='get_iwo_wbs_element_details'
        )

        get_iwo_wbs_element_details = rail.PythonOperator(
            task_id='get_iwo_wbs_element_details',
            python_callable=python_callable_method.get_iwo_wbs_element_fields
        )

        update_iwo_wbs_element = rail.RepliconServiceOperator(
            task_id='update_iwo_wbs_element',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=request_payload.get_update_iwo_wbs_element_payload
        )

        is_parent_present >> rail.Label("Yes") >> get_parent_project_details >> does_parent_project_exist >> rail.Label(
            "Yes") >> get_iwo_wbs_element_details >> update_iwo_wbs_element

    return dag


rail.for_each_instance(create_process_iwowbselement_child_dag)
