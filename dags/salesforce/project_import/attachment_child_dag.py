from datetime import timedelta
import rail
from airflow.models import Variable
from salesforce.project_import.utils.request_payload import get_binaryobject_payload


REPLICON_MAX_FILE_SIZE = 5242880
ALLOWED_FILE_EXTENSIONS = ('txt', 'jpg', 'png', 'pdf', 'tiff', 'html', 'doc', 'docx', 'bmp', 'gif', 'aac', 'avi', 'bmp', 'eml',
                           'heic', 'heif', 'jpeg', 'm4a', 'm4v', 'mkv', 'mov', 'mp3', 'mp4', 'mpe', 'mpeg', 'mpg', 'odp', 'ods',
                           'odt', 'oga', 'ogg', 'ogm', 'ogv', 'ppt', 'pptx', 'rtf', 'svg', 'tif', 'weba', 'webm', 'webp', 'xls',
                           'xlsx')


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_salesforce_{config.region.replace('-', '_')}_project_import_attachment_child_dag_{config.instance}",
        description=f'Salesforce {config.region} Project Import Attachment Child DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='valid_contentversions_in_salesforce'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='valid_contentversions_in_salesforce',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def get_content_version(response):
            records = response.get('records', [])
            return records[0] if records else ''
        valid_contentversions_in_salesforce = rail.SalesforceQueryOperator2(
            task_id='valid_contentversions_in_salesforce',
            salesforce_conn_id='{{ dag_run.conf.salesforce_conn_id }}',
            query="SELECT Id, ContentSize, CreatedDate, FileExtension, PathOnClient, Title, VersionNumber FROM ContentVersion \
                WHERE ContentDocumentId = '{{ dag_run.conf.content_document_id }}' AND IsLatest = True AND \
                ContentSize <= %s AND FileExtension IN %s" % (REPLICON_MAX_FILE_SIZE, ALLOWED_FILE_EXTENSIONS),
            data_handler=get_content_version
        )

        invalid_contentversions_in_salesforce = rail.SalesforceQueryOperator2(
            task_id='invalid_contentversions_in_salesforce',
            salesforce_conn_id='{{ dag_run.conf.salesforce_conn_id }}',
            query="SELECT Id, ContentSize, CreatedDate, FileExtension, PathOnClient, Title, VersionNumber FROM ContentVersion \
                WHERE ContentDocumentId = '{{ dag_run.conf.content_document_id }}' AND IsLatest = True AND \
                (ContentSize > %s OR FileExtension NOT IN %s)" % (REPLICON_MAX_FILE_SIZE, ALLOWED_FILE_EXTENSIONS),
            data_handler=get_content_version
        )

        is_valid_contentversion_present = rail.IfOperator(
            task_id='is_valid_contentversion_present',
            test="{{ result('valid_contentversions_in_salesforce', 'length') > 0 }}",
            yes_task='getfile_mimetype_encoding',
            no_task='finish'
        )

        getfile_mimetype_encoding = rail.SalesforceBase64ContentOperator2(
            task_id='getfile_mimetype_encoding',
            salesforce_conn_id='{{ dag_run.conf.salesforce_conn_id }}',
            object_name='ContentVersion',
            file_name="{{ result('valid_contentversions_in_salesforce').PathOnClient }}",
            record_id="{{ result('valid_contentversions_in_salesforce').Id }}",
            base64_field='VersionData'
        )

        put_binary_object = rail.RepliconServiceOperator(
            task_id='put_binary_object',
            endpoint='/services/BinaryObjectService1.svc/PutBinaryObject',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=get_binaryobject_payload
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish

        can_run_batch_task >> rail.Label(
            'No') >> valid_contentversions_in_salesforce >> invalid_contentversions_in_salesforce >> \
            is_valid_contentversion_present

        is_valid_contentversion_present >> rail.Label(
            'Yes') >> getfile_mimetype_encoding >> put_binary_object >> finish

        is_valid_contentversion_present >> rail.Label(
            'No') >> finish

    return dag


rail.for_each_instance(create_child_dag)
