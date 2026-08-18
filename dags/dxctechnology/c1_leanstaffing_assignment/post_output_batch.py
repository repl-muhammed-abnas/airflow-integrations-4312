import datetime
import rail

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_leanstaffing_assignment/config.py


def create_dag(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_c1_leanstaffassignment_post_output{dag_id_postfix}',
        description=f'DXC C1 Leanstaff Assignment - Post Output Batch {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime.datetime(2022, 1, 1),
        default_args={
            "http_conn_id": config.http_conn_id,
        }
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        write_xml_file = rail.RenderTemplateOperator(
            task_id='write_xml_file',
            target='artifact',
            template_file='output_template.xml',
            dataset="{{ dag_run.conf['items'] | tojson }}",
        )

        post_to_target = rail.HTTPUploadFileOperator(
            task_id='post_to_target',
            content_type='application/xml',
            content="{{ result('write_xml_file') }}",
            retries = 0,
            extra_options= {
                'verify': False
            } if config.instance == "sandbox" else None
        )

        write_xml_file >> post_to_target
    return dag


rail.for_each_instance(create_dag)
