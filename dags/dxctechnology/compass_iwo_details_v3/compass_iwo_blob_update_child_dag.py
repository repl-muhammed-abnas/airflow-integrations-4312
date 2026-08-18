from datetime import timedelta
import rail
from dxctechnology.compass_iwo_details_v3.utils import request_payload
from dxctechnology.compass_iwo_details_v3.utils import custom_methods
from airflow.models import Variable

null = None

# pylint: disable=too-many-statements


def create_iwo_details_blob_update_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_iwo_blob_update_child_{config.dag_id_postfix}',
        description=f'DXC_COMPASS_Labour Types IWO Child Blob Update V2.0 {config.dag_id_postfix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_dxc_compass_wbs_labourtype_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_dxc_compass_wbs_labourtype_details',
            end_task='catch_and_log_errors',
        )

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_dxc_compass_wbs_labourtype_details = rail.RepliconServiceOperator(
            task_id='get_dxc_compass_wbs_labourtype_details',
            endpoint='services/GenericKeyValueStoreService1.svc/GetKeyValue',
            data={
                "keyNamespace": "DXC_WBSLabourTypeDetails",
                "key": '{{ dag_run.conf.parentwbs }}'
            }
        )

        does_key_value_exist = rail.IfOperator(
            task_id='does_key_value_exist',
            test=lambda: bool(rail.result(
                'get_dxc_compass_wbs_labourtype_details')),
            yes_task='write_existing_blob_records',
            no_task='finish',
        )

        write_existing_blob_records = rail.DataAdaptorOperator(
            task_id='write_existing_blob_records',
            source='{{result("get_dxc_compass_wbs_labourtype_details").jsonValue}}',
            columns=['wbsuri',
                     'wbsname',
                     'labourtype',
                     'labourtypeuri',
                     'startdate',
                     'enddate'],
            data=custom_methods.get_create_existing_blob
        )

        existing_blob_records = rail.CreateCollectionOperator(
            task_id='existing_blob_records',
            source='{{result("write_existing_blob_records")}}'
        )

        put_dxc_compass_wbs_labourtype_details = rail.RepliconServiceOperator(
            task_id='put_dxc_compass_wbs_labourtype_details',
            endpoint='/services/GenericKeyValueStoreService1.svc/PutKeyValue',
            data=lambda dag_run: {
                "keyNamespace": "DXC_CompassWBSLabourTypeDetails",
                "keyValue": {
                    "key": dag_run.conf['wbs'],
                    "jsonValue": request_payload.get_json_value_payload(dag_run.conf['wbs'], dag_run.conf['wbsuri'])
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
                'wbs': '{{ dag_run.conf.wbs }}',
                'employeeid': '',
                'status': 'Error',
                # pylint: disable=line-too-long
                'details': '{{ get_error_message() }}',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'wbs ': '{{ dag_run.conf.wbs }}',
                'parentwbs': '{{ dag_run.conf.parentwbs }}',
                'billingratesizeinblob': '{{ result("existing_blob_records" , "length") }}',
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_dxc_compass_wbs_labourtype_details

        get_dxc_compass_wbs_labourtype_details >> does_key_value_exist

        does_key_value_exist >> rail.Label(
            'Yes') >> write_existing_blob_records >> existing_blob_records >> put_dxc_compass_wbs_labourtype_details >> finish
        does_key_value_exist >> rail.Label('No') >> finish

        finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_iwo_details_blob_update_child_dag)