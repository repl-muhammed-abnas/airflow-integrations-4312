
from datetime import timedelta
import uuid
from airflow.models import Variable
from frontdoorinc.user_import.utils.response_filter import get_costcenter
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'frontdoorinc_process_costcenter_groups_child_{config.instance}',
        description=f'Frontdoorinc_process_costcenter_groups_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
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
            no_task='create_costcenterlist'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_costcenterlist',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_costcenterlist = rail.CreateCollectionOperator(
            task_id='create_costcenterlist',
            source="{{ dag_run.conf.allcostcenters}}",
            name="groupvaluesfromfeedfile",
            columns={
                'displaytext': 'displaytext',
                'code': 'code',
            }
        )

        get_data_cost_center_list_service1_5 = rail.RepliconServiceOperator(
            task_id='get_data_cost_center_list_service1_5',
            endpoint="/services/CostCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:cost-center-list-column:code",
                    "urn:replicon:cost-center-list-column:cost-center"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=get_costcenter
        )

        create_list_7 = rail.CreateCollectionOperator(
            task_id='create_list_7',
            source=lambda: rail.result('get_data_cost_center_list_service1_5')[
                'costcenter'],
            name="groupvaluesinreplicon",
        )

        query_list_checkfornewdropdownvalues_8 = rail.QueryCollectionOperator(
            task_id='query_list_checkfornewdropdownvalues_8',
            query="""SELECT * FROM  groupvaluesfromfeedfile WHERE  groupvaluesfromfeedfile.code NOT IN ( SELECT DISTINCT  groupvaluesinreplicon.code FROM  groupvaluesinreplicon )""",
        )

        if_query_list_checkfornewdropdownvalues_8_rows_greater_than_0_9 = rail.IfOperator(
            task_id='if_query_list_checkfornewdropdownvalues_8_rows_greater_than_0_9',
            test='''{{ result('query_list_checkfornewdropdownvalues_8','length') > 0 }}''',
            yes_task="foreach_query_list_checkfornewdropdownvalues_8_10",
            no_task="log_to_sumo",
        )

        foreach_query_list_checkfornewdropdownvalues_8_10 = rail.ForEachOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_8_10',
            items="{{ result('query_list_checkfornewdropdownvalues_8') }}",
            start_task='create_cost_center_or_apply_modification_11',
            end_task='foreach_query_list_checkfornewdropdownvalues_8_10_end'
        )

        create_cost_center_or_apply_modification_11 = rail.RepliconServiceOperator(
            task_id='create_cost_center_or_apply_modification_11',
            endpoint="/services/CostCenterService1.svc/CreateCostCenterOrApplyModification",
            data={
                "costCenter": null,
                "modifications": {
                    "name": "{{ result('foreach_query_list_checkfornewdropdownvalues_8_10').displaytext }}",
                    "codeToApply": {
                        "value": "{{ result('foreach_query_list_checkfornewdropdownvalues_8_10').code }}"
                    },
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        foreach_query_list_checkfornewdropdownvalues_8_10_end = rail.EmptyOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_8_10_end',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> create_costcenterlist
        create_costcenterlist >> get_data_cost_center_list_service1_5 >> create_list_7
        create_list_7 >> query_list_checkfornewdropdownvalues_8
        query_list_checkfornewdropdownvalues_8 >> if_query_list_checkfornewdropdownvalues_8_rows_greater_than_0_9
        if_query_list_checkfornewdropdownvalues_8_rows_greater_than_0_9 >> rail.Label(
            'Yes') >> foreach_query_list_checkfornewdropdownvalues_8_10 >> create_cost_center_or_apply_modification_11
        create_cost_center_or_apply_modification_11 >> foreach_query_list_checkfornewdropdownvalues_8_10_end
        foreach_query_list_checkfornewdropdownvalues_8_10 >> foreach_query_list_checkfornewdropdownvalues_8_10_end
        foreach_query_list_checkfornewdropdownvalues_8_10_end >> log_to_sumo
        if_query_list_checkfornewdropdownvalues_8_rows_greater_than_0_9 >> rail.Label(
            'No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
