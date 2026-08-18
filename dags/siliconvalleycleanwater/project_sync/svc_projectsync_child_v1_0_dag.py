
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'siliconvalleycleanwater_project_sync_svc_projectsync_child_v1_0_{config.instance}',
        description=f'SVC_projectsync_Child - V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
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
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='bulk_get_project_details2_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='bulk_get_project_details2_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        bulk_get_project_details2_3 = rail.RepliconServiceOperator(
            task_id='bulk_get_project_details2_3',
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

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        if_projectdetails_uri_present_4 = rail.IfOperator(
            task_id='if_projectdetails_uri_present_4',
            test='''{{ result('bulk_get_project_details2_3')[0].projectDetails | is_truthy }}''',
            yes_task="trigger_dag_run_live_svc_update_project_child_v1_0async_5",
            no_task="trigger_dag_run_live_svc_create_project_child_v1_0async_7",
        )

        trigger_dag_run_live_svc_update_project_child_v1_0async_5 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_svc_update_project_child_v1_0async_5',
            retries=0,
            items=[1],
            trigger_dag_id=f'siliconvalleycleanwater_project_sync_svc_update_project_child_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda: {
                "log": rail.render_template("{{ result('create_log') }}"),
                "projectnumber": rail.render_template("{{ dag_run.conf.projectnumber | sn}}"),
                "projectname": rail.render_template("{{ dag_run.conf.projectname | sn}}"),
                "Status": rail.render_template("{{ dag_run.conf.Status | sn}}"),
                "startdate": rail.render_template("{{ dag_run.conf.startdate | sn}}"),
                "enddate": rail.render_template("{{ dag_run.conf.enddate| sn }}"),
                "projecturi": rail.render_template("{{ result('bulk_get_project_details2_3')[0].projectDetails.uri }}"),
                "projectdata": rail.get_dag_run_conf()['projectdata'],
                "client": rail.get_dag_run_conf()['projectnumber'].split("-")[1],
            }
        )

        wait_for_completion_trigger_dag_run_live_svc_update_project_child_v1_0async_5 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_svc_update_project_child_v1_0async_5',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_svc_update_project_child_v1_0async_5") }}'
        )

        trigger_dag_run_live_svc_create_project_child_v1_0async_7 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_svc_create_project_child_v1_0async_7',
            retries=0,
            items=[1],
            trigger_dag_id=f'siliconvalleycleanwater_project_sync_svc_create_project_child_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda: {
                "log": rail.render_template("{{ result('create_log') }}"),
                "projectnumber": rail.render_template("{{ dag_run.conf.projectnumber | sn}}"),
                "projectname": rail.render_template("{{ dag_run.conf.projectname | sn}}"),
                "Status": rail.render_template("{{ dag_run.conf.Status| sn }}"),
                "startdate": rail.render_template("{{ dag_run.conf.startdate | sn}}"),
                "enddate": rail.render_template("{{ dag_run.conf.enddate | sn}}"),
                "projectdata": rail.get_dag_run_conf()['projectdata'],
                "client": rail.get_dag_run_conf()['projectnumber'].split("-")[1],
            }
        )

        wait_for_completion_trigger_dag_run_live_svc_create_project_child_v1_0async_7 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_svc_create_project_child_v1_0async_7',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_svc_create_project_child_v1_0async_7") }}'
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> bulk_get_project_details2_3
        bulk_get_project_details2_3 >> create_log >> if_projectdetails_uri_present_4
        if_projectdetails_uri_present_4 >> rail.Label(
            'Yes') >> trigger_dag_run_live_svc_update_project_child_v1_0async_5 >> wait_for_completion_trigger_dag_run_live_svc_update_project_child_v1_0async_5 >> finish
        if_projectdetails_uri_present_4 >> rail.Label(
            'No') >> trigger_dag_run_live_svc_create_project_child_v1_0async_7 >> wait_for_completion_trigger_dag_run_live_svc_create_project_child_v1_0async_7 >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
