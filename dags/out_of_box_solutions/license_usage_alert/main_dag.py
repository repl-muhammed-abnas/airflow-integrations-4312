import rail
from out_of_box_solutions.license_usage_alert.utils import python_callable_method
from out_of_box_solutions.license_usage_alert.utils import custom_methods
from out_of_box_solutions.license_usage_alert.utils import response_filter


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'{config.company_key}_license_usage_alert_{config.instance}',
        description=f'{config.company_key} License Usage Alert {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
    ) as dag:

        get_all_public_licennsed_products = rail.RepliconServiceOperator(
            task_id='get_all_public_licennsed_products',
            endpoint='/services/AccountManagementService1.svc/GetAllPublicLicensedProducts',
            response_filter=lambda response: list(map(lambda item: {
                "uri": item['uri']
            }, response.json()['d']))
        )
        get_product_licensing_summary = rail.RepliconServiceOperator(
            task_id='get_product_licensing_summary',
            endpoint='/services/AccountManagementService1.svc/GetProductLicensingSummary',
            response_filter=response_filter.get_product_licensing_summary_response_filter
        )

        check_for_the_threshold = rail.IfOperator(
            task_id='check_threshold_type',
            test=custom_methods.check_for_threshold,
            yes_task="encode_base64",
            no_task="end"
        )

        encode_base64 = rail.PythonOperator(
            task_id='encode_base64',
            python_callable=lambda: python_callable_method.encode(config)
        )

        get_html_table_data = rail.RenderTemplateOperator(
            task_id='get_html_table_data',
            target='result',
            template_file='templates/emails/alert_email.html',
            dataset="{{result('get_product_licensing_summary') | to_json}}"
        )

        get_company_and_threshold_details = rail.PythonOperator(
            task_id='get_company_and_threshold_details',
            python_callable=lambda: custom_methods.get_companykey_threshold(
                config)
        )

        send_email = rail.EmailOperator(
            task_id='send_email',
            to="{{result('get_company_and_threshold_details').emailto}}",
            bcc=config.internal_logs_email,
            subject=f'{config.company_key} | Replicon Licenses Usage Alert',
            html_content="{{result('get_html_table_data')}}",
        )

        end = rail.EmptyOperator(
            task_id="end"
        )

        get_all_public_licennsed_products >> get_product_licensing_summary >> get_company_and_threshold_details >> check_for_the_threshold

        check_for_the_threshold >> rail.Label(
            'Yes') >> encode_base64 >> get_html_table_data >> send_email

        check_for_the_threshold >> rail.Label('No') >> end

    return dag


rail.for_each_instance(create_dag)
