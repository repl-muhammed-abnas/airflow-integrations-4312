import os
from datetime import datetime
import rail


def cleanup_file(context):

    try:
        local_file = rail.result('download_file', context=context)
        if local_file and os.path.exists(local_file):
            os.remove(local_file)
            return f"Cleaned up local file: {local_file}"
    except Exception as e:
        # Non-critical error, just log
        return f"Could not clean up local file: {str(e)}"


def create_log_file(config, execution_date, **context):
    # Get task instance and dag_run from context
    ti = context['ti']
    dag_run = context['dag_run']

    # Prepare log content
    log_lines = []
    log_lines.append("=" * 80)
    log_lines.append("CARGOLUX SFTP FILE TRANSFER LOG")
    log_lines.append("=" * 80)
    log_lines.append(f"Execution Date: {execution_date}")
    log_lines.append(f"Execution Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log_lines.append(f"DAG Run ID: {dag_run.run_id}")
    log_lines.append(f"Environment: {config.environment}")
    log_lines.append("")
    log_lines.append("-" * 80)

    try:
        # Check if files were found by looking at task states
        from airflow.models import TaskInstance
        from airflow.utils.state import State

        check_files_ti = ti.get_dagrun().get_task_instance('check_for_files')

        if check_files_ti and check_files_ti.state == State.SUCCESS:
            # Get list of files that were processed
            files_list = ti.xcom_pull(task_ids='list_files') or []

            if files_list:
                log_lines.append(f"FILES PROCESSED: {len(files_list)}")
                log_lines.append("")

                # Log each file transfer
                for idx, filename in enumerate(files_list, 1):
                    log_lines.append(f"{idx}. {filename}")
                    log_lines.append(f"   Source: {config.source_sftp_input_path}/{filename}")
                    log_lines.append(f"   Destination: {config.dest_sftp_output_path}/{filename}")
                    log_lines.append(f"   Status: Transferred")
                    log_lines.append("")
            else:
                log_lines.append("STATUS: NO FILES FOUND")
                log_lines.append("The source directory was checked but contained no files.")
        else:
            log_lines.append("STATUS: NO FILES TO TRANSFER")
            log_lines.append("No new files were found in the source SFTP directory.")

    except Exception as e:
        log_lines.append(f"STATUS: LOG GENERATION ERROR")
        log_lines.append(f"Could not retrieve transfer details: {str(e)}")

    log_lines.append("-" * 80)
    log_lines.append("END OF LOG")
    log_lines.append("=" * 80)

    # Write log to temporary file
    log_content = "\n".join(log_lines)
    log_file_path = f"/tmp/cargolux_transfer_log_{dag_run.run_id}.txt"

    with open(log_file_path, 'w') as f:
        f.write(log_content)

    return log_file_path
