import rail
from dxctechnology.gsap_wbs_import_file_merger.utils import python_callable_method

# pylint: disable=too-many-statements
def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_gsap_wbs_file_merger_child_{config.instance}',
        description='DXCtechnology GSAP wbs file merger child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        download_wbs_file = rail.SFTPDownloadFileOperator(
            task_id='download_wbs_file',
            remote_filepath=config.input_filepath +
            '/' + "{{ dag_run.conf.file_name }}",
        )

        parse_xml = rail.LoadXMLFileOperator(
            task_id="parse_xml",
            document="{{result('download_wbs_file')}}",
            xsd_document="./dags/dxctechnology/gsap_wbs_import_file_merger/xml_schema/input_schema.xsd"
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("parse_xml") | xpath("Records") | length > 0 }}',
            yes_task='create_project_collection',
            no_task='move_files_to_archive',
        )

        create_project_collection = rail.CreateCollectionOperator(
            task_id='create_project_collection',
            name='inputdata',
            source="{{ result('parse_xml') | xpath('Records') }}",
        )

        get_query_to_merge = rail.PythonOperator(
            task_id='get_query_to_merge',
            python_callable=python_callable_method.get_query
        )

        query_wbs_data = rail.QueryCollectionOperator(
            task_id='query_wbs_data',
            query='{{result("get_query_to_merge")}}'
        )

        move_files_to_archive = rail.SFTPMoveFileOperator(
            task_id='move_files_to_archive',
            existing_filename=config.input_filepath +
            '/{{ dag_run.conf.file_name }}',
            new_filename=config.archive_filepath +
            "/{{dag_run.conf.file_index}}_{{current_time('%Y%m%dT%H%M%S')}}_{{dag_run.conf.file_name }}"
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        download_wbs_file >> parse_xml >> has_data
        has_data >> rail.Label(
            "Yes") >> create_project_collection >> get_query_to_merge >> query_wbs_data >> move_files_to_archive >> finish
        has_data >> rail.Label(
            "No") >> move_files_to_archive
        move_files_to_archive >> finish
    return dag


rail.for_each_instance(create_dag)
