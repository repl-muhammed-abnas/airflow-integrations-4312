
from datetime import timedelta
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'cbreinc_add_clients_to_projects_get_new_clients_master_{config.instance}',
        description=f'CBREInc - Get new clients from Replicon Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(minutes=30),
        max_active_runs=1,
        default_args={
        },
    ) as dag:

        get_all_clients = rail.RepliconServiceOperator(
            task_id='get_all_clients',
            endpoint="/services/ClientService1.svc/GetActiveClients",
            data_handler=lambda data: list(map(lambda x: {'client_name': x['name'],
                                                          'client_uri': x['uri']}, data))
        )

        create_replicon_client_collection = rail.CreateCollectionOperator(
            task_id='create_replicon_client_collection',
            source=lambda: rail.result('get_all_clients'),
            name='replicon_client'
        )

        # for migration make sure we create new empty data csv file in this s3 path
        download_client_reference_s3_file = rail.S3DownloadFileOperator(
            task_id='download_client_reference_s3_file',
            bucket_name=config.s3_bucket,
            key_name=config.client_reference_s3_file_path,
            aws_conn_id=config.aws_conn_id
        )

        load_reference_file = rail.LoadCSVFileOperator(
            task_id='load_reference_file',
            document="{{ result('download_client_reference_s3_file') }}",
        )

        create_reference_client_collection = rail.CreateCollectionOperator(
            task_id='create_reference_client_collection',
            source="{{ result('load_reference_file') }}",
            name='reference_client'
        )

        query_new_client = rail.QueryCollectionOperator(
            task_id='query_new_client',
            query='''SELECT client_name, client_uri FROM replicon_client WHERE client_name NOT IN ( SELECT client_name from reference_client)''',
            name='new_client'
        )

        has_new_clients = rail.IfOperator(
            task_id='has_new_clients',
            test="{{ result('query_new_client','length') > 0 }}",
            yes_task='get_new_clients_log',
            no_task='log_to_sumo'
        )

        get_new_clients_log = rail.CreateLogOperator(
            task_id="get_new_clients_log",
            tenant_wide_name="cbreinc_add_clients_to_projects_new_clients",
            existing_log_mode="append",
        )

        new_clients_list_add_entry_2 = rail.WriteLogOperator(
            task_id='new_clients_list_add_entry_2',
            log="{{ result('get_new_clients_log') }}",
            items="{{ result('query_new_client') }}",
            message="na",
            severity="na",
            properties={
                "uri": "{{ item.client_uri}}",
                "name": "{{ item.client_name}}",
                "jobdatetime": "{{ current_time() }}",
                "data": "yes"
            }
        )

        create_new_reference_file = rail.WriteCSVFileOperator(
            task_id='create_new_reference_file',
            source="{{ result('create_replicon_client_collection') }}",
            header=['client_name', 'client_uri'],
            row=["{{ item.client_name }}", "{{ item.client_uri }}"]
        )

        upload_reference_s3_file = rail.S3UploadFileOperator(
            task_id='upload_reference_s3_file',
            aws_conn_id=config.aws_conn_id,
            source="{{ result('create_new_reference_file') }}",
            bucket_name=config.s3_bucket,
            key_name=config.client_reference_s3_file_path,
            replace=True,
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        get_all_clients >> create_replicon_client_collection >> download_client_reference_s3_file >> load_reference_file >> create_reference_client_collection >> query_new_client >> has_new_clients
        has_new_clients >> rail.Label(
            'yes') >> get_new_clients_log >> new_clients_list_add_entry_2 >> create_new_reference_file >> upload_reference_s3_file >> log_to_sumo
        has_new_clients >> rail.Label('no') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
