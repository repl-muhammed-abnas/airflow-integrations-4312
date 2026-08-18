from datetime import timedelta
import uuid
import rail
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.homecompany_group_child_dag_id,
        description=f'{config.company_key} Syncs the list of Home Companies from Vantagepoint to Replicon',
        company_key=config.company_key,
        max_active_runs=1,
        multi_tenant=True
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_companies_from_vp',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_companies_from_vp = rail.VantagepointAPIOperator(
            task_id='get_all_companies_from_vp',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
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
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
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

        def get_options_delta(dag_run):
            vp_companies = list(map(lambda company: {
                'name': company['FirmName'],
                'code': company['Company']
            }, rail.result('get_all_companies_from_vp')))
            existing_options = rail.result(
                'get_all_group_options_in_replicon')

            # Dual lookup: by code first, then by name
            enriched_options = []
            for option in vp_companies:
                existing_by_code = rail.find_first_by_attr_and_get_attr(
                    existing_options, 'code', option['code'])

                existing_by_name = None
                if not existing_by_code:
                    existing_by_name = rail.find_first_by_attr_and_get_attr(
                        existing_options, 'name', option['name'])

                existing_uri = (existing_by_code or existing_by_name or {}).get('uri')

                enriched_options.append({
                    **option,
                    'uri': existing_uri,
                    'name': get_option_name(option, existing_options, vp_companies)
                })

            # Disable based on URI matching
            matched_uris = {opt['uri'] for opt in enriched_options if opt.get('uri')}
            root_dept_uri = rail.find_first_by_attr_and_get_attr(
                existing_options, 'name', config.root_department, 'uri')

            options_to_disable = [
                option for option in existing_options
                if option['uri'] not in matched_uris and option['uri'] != root_dept_uri
            ]

            return {
                'options_to_disable': options_to_disable,
                'options_to_add': enriched_options
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
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda dag_run, item: {
                f"{dag_run.conf['grouptypevariable']}Uri": item['uri']
            }
        )

        create_modify_options = rail.RepliconServiceCallForEachItemOperator(
            task_id='create_modify_options',
            items=lambda: rail.result('get_options_to_add_disable')[
                'options_to_add'],
            endpoint="{{dag_run.conf.creategroupendpoint}}",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
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

        def get_downstreamtasks_error(error_message):
            return {
                'error': f'Error in homecompany group sync - {error_message}'
            }

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ get_error_message() }}']
        )

        batch_task >> catch_error
        batch_task >> get_all_companies_from_vp >> get_all_group_options_in_replicon >> get_options_to_add_disable >> disable_options
        disable_options >> create_modify_options >> catch_error

        return dag


rail.for_each_instance(create_dag)
