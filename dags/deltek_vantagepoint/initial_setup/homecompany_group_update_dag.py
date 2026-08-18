from datetime import timedelta
import uuid
from airflow.models import Variable
import rail
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_vantagepoint_homecompany_group_sync_child_{config.instance}',
        description='Syncs the list of Home Companies from Vantagepoint to Replicon',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        default_args={
            'vp_conn_id': config.deltek_vantagepoint_conn_id
        }
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_all_companies_from_vp'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_companies_from_vp',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_companies_from_vp = rail.VantagepointAPIOperator(
            task_id='get_all_companies_from_vp',
            endpoint='/Settings/Company',
            request_method='GET'
        )

        def get_getdata_payload(dag_run):
            group_var = dag_run.conf['getdataservicevariable']
            return {
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    f"urn:replicon:{group_var}-list-column:name",
                    f"urn:replicon:{group_var}-list-column:code",
                    f"urn:replicon:{group_var}-list-column:{group_var}"
                ],
                "sort": [],
                "filterExpression": null
            }

        get_all_group_options_in_replicon = rail.RepliconServiceOperator(
            task_id='get_all_group_options_in_replicon',
            endpoint="{{dag_run.conf.optionsendpoint}}",
            data=get_getdata_payload,
            data_handler=lambda response: list(map(lambda option: {
                'name': option['cells'][0]['textValue'] if 'textValue' in option['cells'][0] else '',
                'code': option['cells'][1]['textValue'] if 'textValue' in option['cells'][1] else '',
                'uri': option['cells'][2]['uri'] if 'uri' in option['cells'][2] else ''
            }, response['rows']))
        )

        def get_option_name(option, existing_options, vp_companies):
            is_option_present_by_name = rail.find_first_by_attr_and_get_attr(existing_options, 'name', option['name'])
            if is_option_present_by_name and is_option_present_by_name['code'] == option['code']:
                return option['name']
            options_to_create_by_this_name = list(
                filter(lambda company: company['name'] == option['name'], vp_companies))
            if is_option_present_by_name or len(options_to_create_by_this_name) > 1:
                return f"{option['name']} - {option['code']}"
            return option['name']

        def get_options_delta():
            vp_companies = list(map(lambda company: {
                'name': company['FirmName'],
                'code': company['Company']
            }, rail.result('get_all_companies_from_vp')))
            existing_options = rail.result(
                'get_all_group_options_in_replicon')
            options_to_disable = list(filter(lambda option: (not (option['name'] == config.root_department or rail.find_first_by_attr_and_get_attr(
                vp_companies, 'code', option['code']))), existing_options))
            options_to_add = list(map(lambda option: {
                **option,
                'uri': rail.find_first_by_attr_and_get_attr(existing_options, 'code', option['code'], 'uri'),
                'name': get_option_name(option, existing_options, vp_companies)
            }, vp_companies))
            return {
                'options_to_disable': options_to_disable,
                'options_to_add': options_to_add
            }

        get_options_to_add_disable = rail.PythonOperator(
            task_id='get_options_to_add_disable',
            python_callable=get_options_delta
        )

        disable_options = rail.RepliconServiceCallForEachItemOperator(
            task_id='disable_options',
            items=lambda: rail.result('get_options_to_add_disable')[
                'options_to_disable'],
            endpoint="{{dag_run.conf.disableendpoint}}",
            data=lambda dag_run, item: {
                f"{dag_run.conf['grouptypevariable']}Uri": item['uri']
            }
        )

        create_modify_options = rail.RepliconServiceCallForEachItemOperator(
            task_id='create_modify_options',
            items=lambda: rail.result('get_options_to_add_disable')[
                'options_to_add'],
            endpoint="{{dag_run.conf.creategroupendpoint}}",
            data=lambda dag_run, item: {
                dag_run.conf['grouptypevariable']: {
                    "uri": item['uri'] or null,
                    "parent": {
                        "name": config.root_department
                    } if (not item['uri'] and dag_run.conf['type'] == 'department') else null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": item['name'],
                    "codeToApply": {
                        "value": item['code']
                    },
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_all_companies_from_vp >> get_all_group_options_in_replicon >> get_options_to_add_disable >> disable_options
        disable_options >> create_modify_options >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
