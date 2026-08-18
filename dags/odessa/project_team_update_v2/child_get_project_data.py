import rail
from odessa.project_team_update_v2.utils import python_callable_method
from odessa.project_team_update_v2.utils import request_payload
from odessa.project_team_update_v2.utils import response_filter


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"odessa_jira_import_child_get_project_data_v2_{config.instance}",
        description=f"odessa jira import child get project data V2 {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_process_wbs_max_active_runs
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        search_clients = rail.RepliconServiceOperator(
            task_id='search_clients',
            endpoint='/services/ClientListService1.svc/GetData',
            data=request_payload.search_client_data,
            response_filter=response_filter.client_uri_check
        )

        check_client_uri = rail.IfOperator(
            task_id='check_client_uri',
            test=lambda: bool(rail.result('search_clients')),
            yes_task='get_client_data',
            no_task='creat_client'
        )

        creat_client = rail.RepliconServiceOperator(
            task_id='creat_client',
            endpoint='/services/ClientService1.svc/PutClient',
            data=request_payload.create_client_payload,
            response_filter=response_filter.put_client
        )

        update_billing_rate_is_alloed_by_default = rail.RepliconServiceOperator(
            task_id='update_billing_rate_is_alloed_by_default',
            endpoint='/services/ClientService1.svc/UpdateBillingRateIsAllowedByDefaultOnNewProjects',
            data=lambda: request_payload.billing_rate_update(
                rail.result("creat_client"))
        )

        get_client_data = rail.RepliconServiceOperator(
            task_id='get_client_data',
            endpoint='/services/ClientService1.svc/GetClientDetails',
            data={
                "clientUri": '{{result("search_clients")[0]["uri"]}}'
            }
        )

        check_is_client_active = rail.IfOperator(
            task_id='check_is_client_active',
            test=python_callable_method.is_required_custom_fields_present,
            yes_task='get_all_jira_for_specified_customer',
            no_task='end'
        )

        get_all_jira_for_specified_customer = rail.QueryCollectionOperator(
            task_id='get_all_jira_for_specified_customer',
            query="""SELECT * FROM jiraupdatedata{{dag_run.conf.page_number}}
                    WHERE customer == :Customer
                    """,
            query_params={
                "Customer": "{{dag_run.conf.customer}}"
            }
        )

        custom_fields_data = rail.PythonOperator(
            task_id='custom_fields_data',
            python_callable=python_callable_method.get_required_client_custom_fields,
        )

        has_time_and_material_phases = rail.IfOperator(
            task_id='has_time_and_material_phases',
            test=lambda: bool(rail.result("custom_fields_data")[
                              'time_and_material'][0]['text']),
            yes_task='add_time_and_materials_data',
            no_task='has_fixedbid_phases',
        )

        all_jira_for_specified_customer = rail.PythonOperator(
            task_id='all_jira_for_specified_customer',
            python_callable=lambda: rail.load_all_records(
                rail.result("get_all_jira_for_specified_customer"))
        )

        add_time_and_materials_data = rail.RenderTemplateOperator(
            task_id='add_time_and_materials_data',
            template="""
                {% set list = [] %}
                {% set data= result("all_jira_for_specified_customer") %}
                {% for j in result("custom_fields_data")['time_and_material'][0]['text'].split(",") %}
                    {% for i in data %}
                        {% do list.append({ 'Clienturi': result("search_clients")[0]["uri"],
                                'Client': dag_run.conf.customer,
                                'Projectname': dag_run.conf.customer +' | '+ i['wing']+' - '+ j.strip(),
                                'Key': i['key'],
                                'Summary': i['summary'],
                                'Customer': i['customer'],
                                'Wing': i['wing'],
                                'Billingtype': 'Time and Material',
                                'Issuetype': i['task_type'],
                                'Parentjira': i['parent_jira'],
                                'Epicid': i['epic_id']}) if j else None %}
                    {% endfor %}
                {% endfor %}
                {{ list | tojson }}
                """,
            target='result',
            json=True,
        )

        has_fixedbid_phases = rail.IfOperator(
            task_id='has_fixedbid_phases',
            test=lambda: bool(rail.result("custom_fields_data")[
                              'fixed_bid'][0]['text']),
            yes_task='add_fixed_bid_items_data',
            no_task='end'
        )

        add_fixed_bid_items_data = rail.RenderTemplateOperator(
            task_id='add_fixed_bid_items_data',
            template="""
                {% set list = [] %}
                {% set data= result("all_jira_for_specified_customer") %}
                {% for j in result("custom_fields_data")['fixed_bid'][0]['text'].split(",") %}
                    {% for i in data %}
                        {% do list.append({ 'Clienturi': result("search_clients")[0]["uri"],
                                'Client': dag_run.conf.customer,
                                'Projectname': dag_run.conf.customer +' | '+ i['wing']+' - '+ j.strip(),
                                'Key': i['key'],
                                'Summary': i['summary'],
                                'Customer': i['customer'],
                                'Wing': i['wing'],
                                'Billingtype': 'Fixed Bid',
                                'Issuetype': i['task_type'],
                                'Parentjira': i['parent_jira'],
                                'Epicid': i['epic_id']}) if j else None %}
                        {% endfor %}
                {% endfor %}
                {{ list | tojson }}
                """,
            target='result',
            json=True,
        )

        end = rail.EmptyOperator(
            task_id='end'
        )

        search_clients >> check_client_uri

        check_client_uri >> \
            rail.Label(
                "No") >> creat_client >> update_billing_rate_is_alloed_by_default >> end

        check_client_uri >> \
            rail.Label("Yes") >> get_client_data >> check_is_client_active

        check_is_client_active >> \
            rail.Label(
                "Yes") >> get_all_jira_for_specified_customer >> all_jira_for_specified_customer >> custom_fields_data

        check_is_client_active >> \
            rail.Label("No") >> end

        custom_fields_data >> has_time_and_material_phases >> \
            rail.Label(
                "Yes") >> add_time_and_materials_data >> has_fixedbid_phases

        has_time_and_material_phases >> \
            rail.Label("No") >> has_fixedbid_phases

        has_fixedbid_phases >> \
            rail.Label("Yes") >> add_fixed_bid_items_data >> end

        has_fixedbid_phases >> \
            rail.Label("No") >> end

    return dag


rail.for_each_instance(create_child_dag)
