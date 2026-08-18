from datetime import timedelta
import rail
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.laborcategory_options_child_dag_id,
        description=f'{config.company_key} Syncs the list of Labor Category options from Vantagepoint to Replicon',
        company_key=config.company_key,
        max_active_runs=1,
        multi_tenant=True
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_labor_categories',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_labor_categories = rail.VantagepointAPIOperator(
            task_id='get_all_labor_categories',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            endpoint='/codeTable/LaborCategory',
            request_method='GET'
        )

        get_user_oefs = rail.RepliconServiceOperator(
            task_id="get_user_oefs",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data={
                "bindingContextUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda oefs: {
                'laborcategoryoefdefinition': rail.find_first_by_attr_and_get_attr(oefs, 'name', rail.find_first_by_attr_and_get_attr(
                    config.oefs, 'id', 'laborcategory', 'name'), 'uri')
            }
        )

        trigger_update_tag_oef_options = rail.TriggerDagRunOperator(
            task_id='trigger_update_tag_oef_options',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.tag_oef_options_update_child_dag_id,
            conf=lambda dag_run: {
                'options': rail.result('get_all_labor_categories'),
                'definition_uri': rail.result('get_user_oefs')['laborcategoryoefdefinition'],
                'oef_id': 'laborcategory',
                'replicon_conn_id': dag_run.conf['replicon_conn_id'],
                'vantagepoint_conn_id': dag_run.conf['vantagepoint_conn_id'],
                'company_key': dag_run.conf['company_key']
            }
        )

        wait_for_tag_options_update = rail.WaitForDagRunsSensor(
            task_id='wait_for_tag_options_update',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_update_tag_oef_options") }}'
        )

        def get_downstreamtasks_error(error_message):
            return {
                'error': f'Error in labor category options update - {error_message}'
            }

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ get_error_message() }}']
        )

        batch_task >> catch_error
        batch_task >> get_all_labor_categories >> get_user_oefs >> trigger_update_tag_oef_options >> wait_for_tag_options_update >> catch_error

        return dag


rail.for_each_instance(create_dag)
