from datetime import timedelta
import rail
from cbrefcg.oef_update.mapper.allowed_oefs import Allowed_Project_Oef
from cbrefcg.oef_update.utils import custom_method, request_payload


def process_oef(config,name):
    with rail.TaskGroup(group_id=f"process_oef_{name}", prefix_group_id=False) as process_user_oefs:

        get_all_oef_details= rail.RepliconServiceOperator(
            task_id=f'get_all_oef_details_{name}',
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:project"
            }
        )

        search_entries_in_oef_mapper = rail.PythonOperator(
            task_id=f'search_entries_in_oef_mapper_{name}',
            python_callable=lambda: request_payload.oefs_from_mapper(Allowed_Project_Oef)
        )

        process_each_oef= rail.TriggerDagRunForEachItemOperator(
            task_id=f'process_each_oef_{name}',
            retries=0,
            items= lambda: rail.result(f"search_entries_in_oef_mapper_{name}"),
            trigger_dag_id=config.child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf= lambda item, dag_run : request_payload.get_child_config(item,dag_run, name)
        )

        user_data_to_log = rail.WriteLogOperator(
            task_id=f'user_data_to_log_{name}',
            log="{{ result('create_log') }}",
            message="Add User To Log",
            severity="add",
            properties=lambda dag_run:{
                "loginName": dag_run.conf['webhook']['data']['user']['loginName'],
                "useruri": dag_run.conf['webhook']['data']['user']['uri'],
                "broker_value": custom_method.check_custom_fielddata()['custom_filed'],
                "jobdatetime": "{{ current_time_in_specified_tz() }}",
                "jobid": "{{ dag_run_ecid() }}"
            }
        )

        get_all_oef_details >> search_entries_in_oef_mapper >> process_each_oef >> user_data_to_log

    return process_user_oefs
