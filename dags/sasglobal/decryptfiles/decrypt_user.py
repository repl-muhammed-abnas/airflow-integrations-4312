from datetime import timedelta
import rail
from sasglobal.decryptfiles.task.decrypt_file import decrypt_files
def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"sasglobal_decryptfile_user_master_{config.instance}",
        description=f"Sasglobal User {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_schedule_interval),
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
        max_active_runs=config.max_active_runs_master
    ) as dag:

        decrypt_files(config,config.user_input_filepath,config.user_processing_filepath,config.user_archivepath)

    return dag


rail.for_each_instance(create_main_dag)
