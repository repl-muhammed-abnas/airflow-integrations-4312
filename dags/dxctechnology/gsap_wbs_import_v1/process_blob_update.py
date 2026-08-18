from datetime import timedelta
import json
from airflow.models import Variable
import rail
from dxctechnology.gsap_wbs_import_v1.utils import request_payload

null = None

# pylint: disable=too-many-statements
def create_child_dag_wbs(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsap_wbs_import_child_process_blob_{config.instance}_v1',
        description='DXC_GSAP_WBS_Automation Process Blob',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_blob,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_parent_labourtype_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_parent_labourtype_details',
            end_task='catch_and_log_errors',
        )

        get_parent_labourtype_details = rail.RepliconServiceOperator(
            task_id='get_parent_labourtype_details',
            endpoint='services/GenericKeyValueStoreService1.svc/GetKeyValue',
            data={
                "keyNamespace": "DXC_WBSLabourTypeDetails",
                "key": '{{ dag_run.conf.parentwbs }}'
            }
        )

        does_key_value_exist = rail.IfOperator(
            task_id='does_key_value_exist',
            test=lambda: bool(rail.result(
                'get_parent_labourtype_details')),
            yes_task='write_existing_blob_records',
            no_task='finish',
        )

        write_existing_blob_records = rail.WriteCSVFileOperator(
            task_id='write_existing_blob_records',
            source=lambda: json.loads(rail.result('get_parent_labourtype_details')[
                'jsonValue']),
            header=['wbsuri',
                     'wbsname',
                     'labourtype',
                     'labourtypeuri',
                     'startdate',
                     'enddate'],
            row=request_payload.get_blob_rows
        )

        existing_blob_records = rail.CreateCollectionOperator(
            task_id='existing_blob_records',
            source='{{result("write_existing_blob_records")}}'
        )

        put_labourtype_details_child = rail.RepliconServiceOperator(
            task_id='put_labourtype_details_child',
            endpoint='/services/GenericKeyValueStoreService1.svc/PutKeyValue',
            data=lambda dag_run: {
                "keyNamespace": "DXC_CompassWBSLabourTypeDetails",
                "keyValue": {
                    "key": dag_run.conf['wbs'],
                    "jsonValue": request_payload.get_json_value_payload(dag_run)
                }
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'projectname': '{{ dag_run.conf.wbs }}',
                'status': 'Error',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> get_parent_labourtype_details

        get_parent_labourtype_details >> does_key_value_exist

        does_key_value_exist >> rail.Label(
            'Yes') >> write_existing_blob_records >> existing_blob_records >> put_labourtype_details_child >> finish
        does_key_value_exist >> rail.Label('No') >> finish

        finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag_wbs)
