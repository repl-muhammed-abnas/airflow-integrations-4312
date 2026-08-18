from datetime import timedelta
from pendulum import datetime
import rail
from airflow.models import Variable


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"vialtopartners_decrypt_input_files_master{config.instance}",
        description=f'vialtopartners_decrypt_input_files_master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1),
        max_active_runs=config.master_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        def get_variable_value():
            return Variable.get(config.vialtopartners_decrypt_files_path)

        get_file_path= rail.PythonOperator(
            task_id = "get_file_path",
            python_callable= get_variable_value
        )

        list_dir = rail.SFTPListFilesOperator(
            task_id = "list_dir",
            paths=lambda : [rail.result('get_file_path')]
        )

        trigger_child_for_decryption = rail.trigger_parallel_dagrun(
            task_id = "trigger_child_for_decryption",
            parallel_count= 20,
            trigger_dag_id= f"vialtopartners_decrypt_input_files_child{config.instance}",
            conf=lambda item:{
                "file_name": item['name'],
                "file_full_path": f"{rail.result('get_file_path')}/{item['name']}"
            },
            items=lambda : rail.result('list_dir')[rail.result('get_file_path')],
            execution_timeout=timedelta(hours=12)
        )

        get_file_path >> list_dir >> trigger_child_for_decryption

    return dag

rail.for_each_instance(create_main_dag)
