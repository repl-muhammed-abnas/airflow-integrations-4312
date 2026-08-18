
from datetime import timedelta
from momentive.payroll_export_file_transfer.mapper.payroll_details_mapper import momentive_payroll_export_mapper
import rail

null=None

def create_dag(config):
    dags_generated = []
    for payroll_name in config.payroll_names:
        location_code = rail.find_first_by_attr_and_get_attr(momentive_payroll_export_mapper,
                                "payroll_name", payroll_name, "3-digit_iso_country_code")
        payroll_name_lower = payroll_name.replace(" ", "_").lower()
        input_file_path = rail.find_first_by_attr_and_get_attr(momentive_payroll_export_mapper,
                                "payroll_name", payroll_name, "replicon_sftp_folder")
        with rail.create_airflow_dag(
            dag_id=f'momentive_payroll_export_{payroll_name_lower}_{location_code}_{config.instance}',
            description=f'Momentive Payroll Export - {payroll_name_lower} {location_code} {config.instance}',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            schedule_interval = timedelta(seconds=config.schedule_interval),
            max_active_runs=config.max_active_runs,
            default_args={
                'sftp_conn_id': config.sftp_conn_id,
            },
        ) as dag:

            new_file_sensor = rail.SFTPAnyFileSensor(
                task_id='new_file_sensor',
                path=input_file_path,
                sftp_conn_id=config.secondary_sftp_conn_id,
                soft_fail_timeout=timedelta(minutes=10),
            )

            download_file = rail.SFTPDownloadFileOperator(
                task_id = 'download_file',
                sftp_conn_id= config.secondary_sftp_conn_id,
                remote_filepath = "{{ result('new_file_sensor') }}",
            )

            was_new_file_found = rail.IfOperator(
                task_id = 'was_new_file_found',
                trigger_rule = 'all_done',
                test = '{{ get_task_state("new_file_sensor") == "success" }}',
                yes_task = 'is_data_exists_in_mapper',
                no_task = 'delete_this_dagrun'
            )

            is_data_exists_in_mapper = rail.IfOperator(
                task_id='is_data_exists_in_mapper',
                # pylint: disable=cell-var-from-loop
                test=lambda: rail.find_first_by_attr_and_get_attr(momentive_payroll_export_mapper, "replicon_sftp_folder", input_file_path) != null,
                yes_task="archive_to_secondary_sftp",
            )

            delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
                task_id='delete_this_dagrun'
            )

            archive_to_secondary_sftp = rail.SFTPMoveFileOperator(
                task_id='archive_to_secondary_sftp',
                sftp_conn_id=config.secondary_sftp_conn_id,
                new_filename=input_file_path+'/Archive/{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv',
                existing_filename=input_file_path+'/{{ result("new_file_sensor") | file_base }}.csv',
            )

            filename = rail.find_first_by_attr_and_get_attr(momentive_payroll_export_mapper, "3-digit_iso_country_code",
                            location_code, "safeguard_sftp_folder")+\
                                '/MPMS_'+location_code+'_'+payroll_name+"_TIME_"+'{{ current_time("%y%m%d") }}.csv'

            upload_to_sftp = rail.SFTPUploadFileOperator(
                task_id='upload_to_sftp',
                content="{{ result('download_file') }}",
                remote_filepath=filename,
            )

            new_file_sensor >> download_file >> was_new_file_found
            was_new_file_found >> rail.Label("Yes") >> is_data_exists_in_mapper
            was_new_file_found >> rail.Label("No") >> delete_this_dagrun
            is_data_exists_in_mapper >> rail.Label('Yes') >> archive_to_secondary_sftp >> upload_to_sftp

        dags_generated.append(dag)

    return dags_generated

rail.for_each_instance(create_dag)
