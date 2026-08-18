from datetime import timedelta
import rail
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.file_format_creation_child_dag_id,
        description=f'{config.company_key} Creates the File Format for exporting Time Data from Replicon to ComputerEase',
        company_key=config.company_key,
        max_active_runs=config.child_dag_max_active_runs,
        multi_tenant=True
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_time_data_columns',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_time_data_columns = rail.RepliconServiceOperator(
            task_id='get_time_data_columns',
            endpoint="/services/TimeDataExportService1.svc/GetAllColumns",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data_handler=lambda response: {
                "timeentryoef": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Time Entry Object Extension Field ', 'columns'),
                "useroef": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'User Object Extension Field', 'columns')
            }
        )

        get_my_identity = rail.RepliconServiceOperator(
            task_id='get_my_identity',
            endpoint="/services/UserAccessControlService1.svc/GetMyIdentity",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data_handler=lambda response: response['actualUser']['uri']
        )

        def add_column(uri, label=False):
            collection = [
                {
                    "collection": [{
                        "text": "uri"
                    }, {
                        "uri": uri
                    }],
                    "uri": "urn:replicon:script-type:parameter:time-data-download:column-uri"
                }
            ]
            if ('entry-date' in uri) or ('timesheet-period' in uri):
                collection.append({
                    "collection": [{
                        "text": "text"
                    }, {
                        "text": config.date_format_for_file_format
                    }],
                    "uri": "urn:replicon:script-type:parameter:time-data-download:column-date-format"
                })
            if label:
                collection.append({
                    "collection": [{
                        "text": "text"
                    }, {
                        "text": label
                    }],
                    "uri": "urn:replicon:script-type:parameter:time-data-download:column-name"
                })
            return {
                "collection": [{
                    "text": "dictionary"
                }, {
                    "collection": collection
                }]
            }

        def get_columns_collection():
            columns_collection = []
            timeentryoefcolumns = rail.result('get_time_data_columns').get('timeentryoef')

            # Standard columns
            for uri in [
                "urn:replicon:time-data-export-column:user-login-name",
                "urn:replicon:time-data-export-column:entry-date",
                "urn:replicon:time-data-export-column:comments",
                "urn:replicon:time-data-export-totals-column:hours",
                "urn:replicon:time-data-export-column:in-time",
                "urn:replicon:time-data-export-column:out-time",
                "urn:replicon:time-data-export-column:punch-in-time",
                "urn:replicon:time-data-export-column:punch-out-time",
                "urn:replicon:time-data-export-column:break-type-name",
                "urn:replicon:time-data-export-column:project-name",
                "urn:replicon:time-data-export-column:project-code",
                "urn:replicon:time-data-export-column:task-code",
                "urn:replicon:time-data-export-column:task-hierarchy-code",
                "urn:replicon:time-data-export-column:time-off-code-name",
                "urn:replicon:time-data-export-column:pay-code-name"
            ]:
                columns_collection.append(add_column(uri))

            # Add Time Entry OEFs dynamically from config.oefs
            timesheet_oefs = [oef for oef in config.oefs if 'timesheet' in oef.get('bind', [])]
            for oef in timesheet_oefs:
                oef_uri = rail.find_first_by_attr_and_get_attr(
                    timeentryoefcolumns, 'displayText', oef['name'], 'uri')
                if oef_uri:
                    columns_collection.append(add_column(oef_uri))

            return columns_collection

        def get_file_format_key_values():
            key_values = [
                {
                    "keyUri": "urn:replicon:csv-file-settings:custom-delimiter",
                    "value": {
                        "text": ","
                    }
                },
                {
                    "keyUri": "urn:replicon:script-key:basis-script-identifier",
                    "value": {
                        "text": "CsvGeneration"
                    }
                },
                {
                    "keyUri": "urn:replicon:script-key:creator",
                    "value": {
                        "uri": rail.result('get_my_identity')
                    }
                },
                {
                    "keyUri": "urn:replicon:script-key:custom-script",
                    "value": {
                        "bool": "false"
                    }
                },
                {
                    "keyUri": "urn:replicon:script-key:name",
                    "value": {
                        "text": config.replicon_export_file_format_name
                    }
                },
                {
                    "keyUri": "urn:replicon:script-key:owner",
                    "value": {
                        "uri": "urn:replicon:script-key:owner:tenant"
                    }
                },
                {
                    "keyUri": "urn:replicon:script-key:parameter:time-data-download:columns",
                    "value": {
                        "collection": [{
                            "text": "collection"
                        },
                        {
                            "collection": get_columns_collection()
                        }]
                    }
                },
                {
                    "keyUri": "urn:replicon:script-key:script-filename",
                    "value": {
                        "text": "@replicon/script-library/TimeDataDownload/CsvGeneration.py"
                    }
                },
                {
                    "keyUri": "urn:replicon:script-key:script-version",
                    "value": {
                        "text": "0.1"
                    }
                },
                {
                    "keyUri": "urn:replicon:script-key:system-format",
                    "value": {
                        "text": "csv"
                    }
                },
                {
                    "keyUri": "urn:replicon:script-key:time-data-download-script:transport-method",
                    "value": {
                        "uri": "urn:replicon:time-data-download-transport-method:file"
                    }
                }
            ]

            return key_values

        put_file_format = rail.RepliconServiceOperator(
            task_id='put_file_format',
            endpoint="/services/TimeDataExportService1.svc/PutFileFormat",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda: {
                "fileFormat": {
                    "target": {
                        "uri": null,
                        "slug": null,
                        "name": config.replicon_export_file_format_name
                    },
                    "name": config.replicon_export_file_format_name,
                    "description": null,
                    "detailedDescription": null,
                    "isActive": "true",
                    "keyValues": get_file_format_key_values()
                }
            }
        )

        def get_downstreamtasks_error(error_message):
            return {
                'error': f'Error in file format creation - {error_message}'
            }

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ get_error_message() }}']
        )

        batch_task >> catch_error
        batch_task >> get_time_data_columns >> get_my_identity
        get_my_identity >> put_file_format >> rail.Label('On Error') >> catch_error

        return dag


rail.for_each_instance(create_dag)
