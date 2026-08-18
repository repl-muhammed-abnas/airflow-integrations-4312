from datetime import timedelta
import rail
from rail.lib.ecid import get_dagrun_ecid
from pimco.project_import.utils import request_payload, response_filter
from pimco.project_import.utils.custom_method import validate_project, get_log_message
from airflow.models import Variable

# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pimco_update_project_child_{config.instance}",
        description=f"PIMCO Update Project Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="check_project_name_and_status"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task="check_project_name_and_status",
            end_task="catch_and_log_error"
        )
        check_project_name_and_status = rail.IfOperator(
            task_id = 'check_project_name_and_status',
            test= validate_project,
            yes_task= 'log_field_not_present',
            no_task= 'update_project'
        )

        log_field_not_present = rail.WriteLogOperator(
            task_id="log_field_not_present",
            severity='Skipped',
            message=get_log_message,
            properties= lambda dag_run: {
                'Projectcode': dag_run.conf['Projectcode'],
                'Projectname': dag_run.conf['Projectname'],
                'Status': "Skipped",
                'JobId': get_dagrun_ecid(dag_run),
                'details': get_log_message(dag_run),
                'flag': dag_run.conf['flag'],
            }
        )

        update_project = rail.RepliconServiceOperator(
            task_id= 'update_project',
            endpoint= '/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data= request_payload.get_update_project_payload
        )

        get_all_oefs = rail.RepliconServiceOperator(
            task_id = 'get_all_oefs',
            endpoint= '/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldBindings',
            data= {
                    "bindingContextUri": "urn:replicon:object-type:project"
                },
            response_filter= response_filter.get_oef_uris
        )

        get_oef_drop_down_values_eligibility = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_values_eligibility",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.result("get_all_oefs")[0]['uri'],
            }
        )

        update_oef= rail.RepliconServiceOperator(
            task_id = 'update_oef',
            endpoint= '/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=lambda dag_run: request_payload.get_oef_update_payload(dag_run, 'update')
        )

        log_success = rail.WriteLogOperator(
            task_id="log_success",
            severity='Success',
            message="Project updated Successfully",
            properties={
                'Projectcode': '{{ dag_run.conf.Projectcode }}',
                'Projectname': "{{ dag_run.conf.Projectname }}",
                'Status': "success",
                'JobId': '{{ dag_run_ecid() }}',
                'details': 'Project updated Successfully',
                'flag': "{{ dag_run.conf.flag }}",
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'Projectcode': '{{ dag_run.conf.Projectcode }}',
                'Projectname': "{{ dag_run.conf.Projectname }}",
                'Status': "failed",
                'JobId': '{{ dag_run_ecid() }}',
                'details': '{{ get_error_message() }}',
                'flag': "{{ dag_run.conf.flag }}",
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error >> log_to_sumo
        can_run_batch_task >> rail.Label("No") >> check_project_name_and_status >> rail.Label(
            "Yes") >> log_field_not_present >> rail.Label("On Error") >> catch_and_log_error

        check_project_name_and_status >> rail.Label(
            "No") >> update_project >> get_all_oefs >> get_oef_drop_down_values_eligibility >> update_oef >> log_success >> catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_main_dag)
