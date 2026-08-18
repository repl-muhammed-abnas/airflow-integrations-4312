
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'siliconvalleycleanwater_workorder_sync_svc_workordersync_child_v1_0_{config.instance}',
        description=f'SVC_workordersync_Child - V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_log',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_request_enddate_present_2 = rail.IfOperator(
            task_id='if_request_enddate_present_2',
            test='''{{ dag_run.conf.enddate | is_truthy }}''',
            yes_task="log_formattheenddateas_y_y_y_y_m_m_d_d_3",
            no_task="get_all_object_extension_field_details_6",
        )

        log_formattheenddateas_y_y_y_y_m_m_d_d_3 = rail.PythonOperator(
            task_id='log_formattheenddateas_y_y_y_y_m_m_d_d_3',
            python_callable=lambda:  rail.parse_date(
                rail.get_dag_run_conf()['enddate'], "%Y-%m-%d")
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        log_formattheenddateas_y_y_y_y_m_m_d_d_4 = rail.PythonOperator(
            task_id='log_formattheenddateas_y_y_y_y_m_m_d_d_4',
            python_callable=lambda: rail.parse_date(
                rail.get_dag_run_conf()['enddate'], "%Y-%m-%d")
        )

        get_all_object_extension_field_details_6 = rail.RepliconServiceOperator(
            task_id='get_all_object_extension_field_details_6',
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:project"
            }
        )

        bulk_get_project_details2_7 = rail.RepliconServiceOperator(
            task_id='bulk_get_project_details2_7',
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails2",
            data={
                "projects": [
                    {
                        "uri": null,
                        "name": null,
                        "code": "{{ dag_run.conf.projectnumber }}",
                        "parameterCorrelationId": null
                    }
                ]
            }
        )

        if_projectdetails_uri_present_8 = rail.IfOperator(
            task_id='if_projectdetails_uri_present_8',
            test='''{{ result('bulk_get_project_details2_7')[0].projectDetails | is_truthy }}''',
            yes_task="trigger_dag_run_live_svc_update_workorder_child_v1_0async_9",
            no_task="trigger_dag_run_live_svc_create_workorder_child_v1_0async_11",
        )

        trigger_dag_run_live_svc_update_workorder_child_v1_0async_9 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_svc_update_workorder_child_v1_0async_9',
            retries=0,
            items=[1],
            trigger_dag_id=f'siliconvalleycleanwater_workorder_sync_svc_update_workorder_child_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda: {
                "log": rail.render_template("{{ result('create_log') }}"),
                "projectnumber": rail.render_template("{{ dag_run.conf.projectnumber | sn }}"),
                "projectname": rail.render_template("{{ dag_run.conf.projectname | sn }}"),
                "Status": rail.render_template("{{ dag_run.conf.Status | sn }}"),
                "startdate": rail.render_template("{{ dag_run.conf.startdate | sn }}"),
                "enddate": rail.render_template("{{ dag_run.conf.enddate | sn }}"),
                "projecturi": rail.render_template("{{ result('bulk_get_project_details2_7')[0].projectDetails.uri }}"),
                "projectdata": rail.get_dag_run_conf()['projectdata'],
                "masterjobid": rail.render_template("{{ dag_run.conf.masterjobid }}"),
                "requestidvalue": rail.render_template("{{ dag_run.conf.requestid | sn }}"),
                "physicallocationvalue": rail.render_template("{{ dag_run.conf.physicallocation | sn }}"),
                "euquipmentvalue": rail.render_template("{{ dag_run.conf.equipmentposition | sn }}"),
                "requestiduri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_object_extension_field_details_6'), 'name', "Request / Template", 'uri'),
                "physicallocationuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_object_extension_field_details_6'), 'name', "WO - Physical Location", 'uri'),
                "equipmenturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_object_extension_field_details_6'), 'name', "Equipment Position", 'uri'),
            }
        )

        wait_for_completion_trigger_dag_run_live_svc_update_workorder_child_v1_0async_9 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_svc_update_workorder_child_v1_0async_9',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_svc_update_workorder_child_v1_0async_9") }}'
        )

        trigger_dag_run_live_svc_create_workorder_child_v1_0async_11 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_svc_create_workorder_child_v1_0async_11',
            retries=0,
            items=[1],
            trigger_dag_id=f'siliconvalleycleanwater_workorder_sync_svc_create_workorder_child_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda: {
                "log": rail.render_template("{{ result('create_log') }}"),
                "projectnumber": rail.render_template("{{ dag_run.conf.projectnumber | sn }}"),
                "projectname": rail.render_template("{{ dag_run.conf.projectname| sn  }}"),
                "Status": rail.render_template("{{ dag_run.conf.Status | sn }}"),
                "startdate": rail.render_template("{{ dag_run.conf.startdate | sn }}"),
                "enddate": rail.render_template("{{ dag_run.conf.enddate| sn  }}"),
                "projectdata": rail.get_dag_run_conf()['projectdata'],
                "masterjobid": rail.render_template("{{ dag_run.conf.masterjobid }}"),
                "requestidvalue": rail.render_template("{{ dag_run.conf.requestid|sn }}"),
                "physicallocationvalue": rail.render_template("{{ dag_run.conf.physicallocation|sn }}"),
                "euquipmentvalue": rail.render_template("{{ dag_run.conf.equipmentposition|sn }}"),
                "requestiduri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_object_extension_field_details_6'), 'name', "Request / Template", 'uri'),
                "physicallocationuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_object_extension_field_details_6'), 'name', "WO - Physical Location", 'uri'),
                "equipmenturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_object_extension_field_details_6'), 'name', "Equipment Position", 'uri'),
            }
        )

        wait_for_completion_trigger_dag_run_live_svc_create_workorder_child_v1_0async_11 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_svc_create_workorder_child_v1_0async_11',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_svc_create_workorder_child_v1_0async_11") }}'
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> create_log >> if_request_enddate_present_2
        if_request_enddate_present_2
        if_request_enddate_present_2 >> rail.Label(
            'Yes') >> log_formattheenddateas_y_y_y_y_m_m_d_d_3 >> log_formattheenddateas_y_y_y_y_m_m_d_d_4 >> get_all_object_extension_field_details_6
        if_request_enddate_present_2 >> rail.Label(
            'No') >> get_all_object_extension_field_details_6 >> bulk_get_project_details2_7 >> if_projectdetails_uri_present_8
        if_projectdetails_uri_present_8 >> rail.Label(
            'Yes') >> trigger_dag_run_live_svc_update_workorder_child_v1_0async_9 >> wait_for_completion_trigger_dag_run_live_svc_update_workorder_child_v1_0async_9 >> finish
        if_projectdetails_uri_present_8 >> rail.Label(
            'No') >> trigger_dag_run_live_svc_create_workorder_child_v1_0async_11 >> wait_for_completion_trigger_dag_run_live_svc_create_workorder_child_v1_0async_11 >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
