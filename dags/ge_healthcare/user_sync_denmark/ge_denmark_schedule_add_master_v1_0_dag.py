
from datetime import timedelta
from airflow.models import Variable
from ge_healthcare.user_sync_denmark.denmark_master_mapper import denmark_master_mapper
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'gehealthcare_user_sync_denmark_ge_denmark_schedule_add_master_v1_0_{config.instance}',
        description=f'GE_Denmark Schedule Add Master V1.0 {config.instance}',
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
            no_task='_adhoc_http_action_6'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='_adhoc_http_action_6',
            end_task='catch_7_7_21',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_data_from_document(document):
            with rail.lib.readers.get_data_reader(document) as reader:
                return list(reader)

        def get_input_data(dag_run):
            return get_data_from_document(dag_run.conf['inputdata'])

        _adhoc_http_action_6 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_6',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
        )

        ge_denmark_user_sync_master_mapper_v2_0_search_entries_8 = rail.PythonOperator(
            task_id='ge_denmark_user_sync_master_mapper_v2_0_search_entries_8',
            python_callable=lambda: list(
                filter(lambda x: x['type'] == 'Default Schedule', denmark_master_mapper))
        )

        def get_value_from_mapper(entity_type):
            emp_types = list(filter(
                lambda x: x['type'] == entity_type, denmark_master_mapper))
            return emp_types[0]['value'] if emp_types else None

        def get_schedule_uri(schedule_to_assign):
            return rail.find_first_by_attr_and_get_attr(rail.result(
                '_adhoc_http_action_6'), 'displayText', schedule_to_assign, 'uri')
        # pylint: disable=too-many-boolean-expressions

        def get_schedule_to_assign(dag_run):
            schedule_to_assign = []
            schedules_from_input = get_input_data(dag_run)
            schedule_name_mapper = get_value_from_mapper("Default Schedule")
            for schedule in schedules_from_input:
                schedule_name_to_assign = schedule['DWSMonday'] + "|" + schedule['DWSTuesday'] + "|" + schedule['DWSWednesday'] + "|" + \
                    schedule['DWSThursday'] + "|" + schedule['DWSFriday'] + \
                    "|" + schedule['DWSSaturday'] + \
                    "|" + schedule['DWSSunday']
                print("schedule_name_to_assign_name", schedule_name_to_assign)
                if schedule['DWSMonday'] == 0 and schedule['DWSTuesday'] == 0 and schedule['DWSWednesday'] == 0 \
                    and schedule['DWSThursday'] == 0 and schedule['DWSFriday'] == 0 and schedule['DWSSaturday'] == 0 \
                        and schedule['DWSSunday'] == 0:
                    schedule_name_to_assign = schedule_name_mapper
                if schedule['DWSMonday'] is None or schedule['DWSTuesday'] is None or schedule['DWSWednesday'] is None \
                    or schedule['DWSThursday'] is None or schedule['DWSFriday'] is None or schedule['DWSSaturday'] is None \
                        or schedule['DWSSunday'] is None:
                    schedule_name_to_assign = schedule_name_mapper
                schedule_to_assign.append({
                    "Schedulename": schedule_name_to_assign,
                    "Scheduleuri": get_schedule_uri(schedule_name_to_assign)
                })
            return schedule_to_assign

        create_list_17 = rail.CreateCollectionOperator(
            task_id='create_list_17',
            # pylint: disable=unnecessary-lambda
            source=lambda dag_run: get_schedule_to_assign(dag_run),
            name="schedulelist",
        )

        query_list_get_distinct_schedulewhereno_uriisfound_18 = rail.QueryCollectionOperator(
            task_id='query_list_get_distinct_schedulewhereno_uriisfound_18',
            query="""SELECT DISTINCT schedulelist.schedulename FROM  schedulelist WHERE  NULLIF(schedulelist.scheduleuri, '') IS NULL""",
        )

        trigger_dag_run_ge_user_sync_denmark_ge_denmark_child_schedule_add_v1_020 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_user_sync_denmark_ge_denmark_child_schedule_add_v1_020',
            retries=0,
            items="{{ result('query_list_get_distinct_schedulewhereno_uriisfound_18') }}",
            trigger_dag_id=f'gehealthcare_user_sync_denmark_ge_denmark_child_schedule_add_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda item: {
                "name": item['Schedulename'],
                "monday": item['Schedulename'].split("|")[0],
                "tuesday": item['Schedulename'].split("|")[1],
                "wednesday": item['Schedulename'].split("|")[2],
                "thursday": item['Schedulename'].split("|")[3],
                "friday": item['Schedulename'].split("|")[4],
                "saturday": item['Schedulename'].split("|")[5],
                "sunday": item['Schedulename'].split("|")[6]
            }
        )

        wait_for_completion_trigger_dag_run_ge_user_sync_denmark_ge_denmark_child_schedule_add_v1_020 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_user_sync_denmark_ge_denmark_child_schedule_add_v1_020',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_user_sync_denmark_ge_denmark_child_schedule_add_v1_020") }}'
        )

        catch_7_7_21 = rail.EmptyOperator(
            task_id='catch_7_7_21',
            trigger_rule='one_failed',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_7_7_21
        can_run_batch_task >> rail.Label('No') >> _adhoc_http_action_6 >> ge_denmark_user_sync_master_mapper_v2_0_search_entries_8 >> \
            create_list_17 >> query_list_get_distinct_schedulewhereno_uriisfound_18 >> \
            trigger_dag_run_ge_user_sync_denmark_ge_denmark_child_schedule_add_v1_020 >> \
            wait_for_completion_trigger_dag_run_ge_user_sync_denmark_ge_denmark_child_schedule_add_v1_020 >> \
            catch_7_7_21 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
