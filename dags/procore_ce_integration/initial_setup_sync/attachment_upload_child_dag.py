import base64
from datetime import timedelta
import rail
from rail.hooks.procore_hook import ProcoreHook

MIME_TYPES = {
    'jpg': 'image/jpeg', 'jpeg': 'image/jpeg', 'pdf': 'application/pdf',
    'doc': 'application/msword',
    'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    'xls': 'application/vnd.ms-excel',
    'xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    'ppt': 'application/vnd.ms-powerpoint',
    'pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    'png': 'image/png', 'gif': 'image/gif', 'mp4': 'video/mp4',
    '3gp': 'video/3gpp', 'msg': 'application/vnd.ms-outlook',
    'txt': 'text/plain', 'csv': 'text/csv'
}
MAX_SIZE_BYTES = 20 * 1024 * 1024
SIMPLE_UPLOAD_MAX_BYTES = 5 * 1024 * 1024


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.attachment_child_dag_id,
        description='Procore to Computerease - Attachment Upload Child DAG',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.max_active_runs_attachment_child,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'computerease_conn_id': config.computerease_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='download_file_from_procore',
            end_task='set_dag_result',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def download_file(dag_run):
            filename = dag_run.conf['filename']
            ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
            if ext not in MIME_TYPES:
                raise ValueError(f"File extension '{ext}' is not allowed for CE attachment upload")

            hook = ProcoreHook(procore_conn_id=config.procore_conn_id)
            session = hook.get_conn()
            response = session.get(
                dag_run.conf['url'],
                headers={
                    'Authorization': f'Bearer {session.access_token}',
                    'Procore-Company-Id': session.company_id
                }
            )
            if not response.ok:
                raise RuntimeError(
                    f"Failed to download attachment from Procore: {response.status_code}"
                )

            file_bytes = response.content
            size = len(file_bytes)
            if size > MAX_SIZE_BYTES:
                raise ValueError(f"File '{filename}' exceeds the 20MB CE attachment size limit")

            return {'data': base64.b64encode(file_bytes).decode('utf-8'), 'size': size}

        download_file_from_procore = rail.PythonOperator(
            task_id='download_file_from_procore',
            show_return_value_in_logs=False,
            python_callable=download_file
        )

        if_small_file = rail.IfOperator(
            task_id='if_small_file',
            test=lambda: (rail.result('download_file_from_procore') or {}).get('size', 0) <= SIMPLE_UPLOAD_MAX_BYTES,
            yes_task='simple_ce_upload',
            no_task='initiate_ce_upload'
        )

        simple_ce_upload = rail.ComputereaseAPIOperator(
            task_id='simple_ce_upload',
            endpoint='/import/attachments',
            request_method='POST',
            request_body=lambda dag_run: {
                'filename': dag_run.conf['filename'],
                'data': (rail.result('download_file_from_procore') or {}).get('data')
            }
        )

        initiate_ce_upload = rail.ComputereaseAPIOperator(
            task_id='initiate_ce_upload',
            endpoint='/import/attachments/initiate',
            request_method='POST',
            request_body=lambda dag_run: {'filename': dag_run.conf['filename']}
        )

        def upload_to_s3(dag_run):
            import requests as req

            upload_url = (rail.result('initiate_ce_upload') or {}).get('data', {}).get('upload_url')
            if not upload_url:
                raise ValueError("No upload URL returned from CE initiate")

            filename = dag_run.conf['filename']
            ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''
            file_bytes = base64.b64decode((rail.result('download_file_from_procore') or {}).get('data', ''))

            s3_response = req.put(
                upload_url,
                data=file_bytes,
                headers={'Content-Type': MIME_TYPES.get(ext, 'application/octet-stream')}
            )
            if not s3_response.ok:
                raise RuntimeError(f"Failed to upload file to S3: {s3_response.status_code}")

        upload_to_computerease_s3 = rail.PythonOperator(
            task_id='upload_to_computerease_s3',
            python_callable=upload_to_s3
        )

        confirm_ce_upload = rail.ComputereaseAPIOperator(
            task_id='confirm_ce_upload',
            endpoint=lambda: f"/import/attachments/{(rail.result('initiate_ce_upload') or {}).get('data', {}).get('uuid')}/confirm",
            request_method='POST'
        )

        def set_result_data(dag_run, error_message):
            simple_result = rail.result('simple_ce_upload')
            if simple_result is not None:
                uuid = (simple_result or {}).get('data', {}).get('uuid')
            else:
                uuid = (
                    (rail.result('initiate_ce_upload') or {}).get('data', {}).get('uuid')
                    if rail.result('confirm_ce_upload') is not None else None
                )
            result = {
                'attachment_id': dag_run.conf['attachment_id'],
                'filename': dag_run.conf.get('filename', '')
            }
            if uuid:
                result['uuid'] = uuid
            else:
                result['error'] = error_message
            return result

        set_dag_result = rail.PythonOperator(
            task_id='set_dag_result',
            trigger_rule='all_done',
            python_callable=set_result_data,
            op_kwargs={'error_message': '{{ get_error_message() }}'}
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        # Task dependencies
        batch_task >> set_dag_result
        batch_task >> download_file_from_procore >> if_small_file
        if_small_file >> rail.Label('Yes') >> simple_ce_upload >> set_dag_result
        if_small_file >> rail.Label('No') >> initiate_ce_upload >> upload_to_computerease_s3 >> confirm_ce_upload >> set_dag_result
        set_dag_result >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
