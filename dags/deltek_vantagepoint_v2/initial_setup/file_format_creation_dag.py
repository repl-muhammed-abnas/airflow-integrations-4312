from datetime import timedelta
import rail
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.file_format_creation_child_dag_id,
        description=f'{config.company_key} Creates the File Format for exporting Time Data from Vantagepoint to Replicon',
        company_key=config.company_key,
        max_active_runs=1,
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
            task_id = 'get_time_data_columns',
            endpoint="/services/TimeDataExportService1.svc/GetAllColumns",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data_handler=lambda response: {
                "userproperties": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'User Properties', 'columns'),
                "timeentryoef": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Time Entry Object Extension Field ', 'columns'),
                "useroef": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'User Object Extension Field', 'columns')
            }
        )

        get_my_identity = rail.RepliconServiceOperator(
            task_id = 'get_my_identity',
            endpoint="/services/UserAccessControlService1.svc/GetMyIdentity",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data_handler=lambda response: response['actualUser']['uri']
        )

        def add_column(uri, label = False):
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
                },{
                    "collection": collection
                }]
            }


        get_all_oef_definitions = rail.RepliconServiceOperator(
            task_id='get_all_oef_definitions',
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data={
                "bindingContextUri": "urn:replicon:object-type:user"
            }
        )

        def get_columns_collection():
            columns_collection = []
            timeentryoefcolumns = rail.result('get_time_data_columns').get('timeentryoef')
            useroefcolumns = rail.result('get_time_data_columns').get('useroef')
            userpropertiescolumns = rail.result('get_time_data_columns').get('userproperties')
            oef_definitions = rail.result('get_all_oef_definitions')

            def get_laborcode_oef_name(oef):
                if oef['id'].startswith('laborcodelevel'):
                    level_num = oef['id'].replace('laborcodelevel', '')
                    replicon_name = rail.find_first_by_attr_and_get_attr(oef_definitions, 'code', f'Labor Code Level {level_num}', 'name')
                    if replicon_name:
                        return replicon_name
                return oef['name']

            dag_run = rail.get_current_context()['dag_run']
            expected_level_count = dag_run.conf.get('expected_level_count', 0)

            laborcodeoefs = []
            default_laborcodeoefs = []
            laborcode_oefs_in_config = [oef for oef in config.oefs if 'laborcodelevel' in oef.get('id')]
            for oef in laborcode_oefs_in_config[:expected_level_count]:
                name = get_laborcode_oef_name(oef)
                te_uri = rail.find_first_by_attr_and_get_attr(timeentryoefcolumns, 'displayText', name + ' (Code)', 'uri')
                user_uri = rail.find_first_by_attr_and_get_attr(useroefcolumns, 'displayText', name + ' (Code)', 'uri')
                if not (te_uri and user_uri):
                    raise RuntimeError(
                        f"Labor code level '{name}' was expected (per customSettings) but its Replicon "
                        f"columns could not be found (time-entry URI: {te_uri}, user-oef URI: {user_uri}). "
                        f"Initial-setup may have failed to create this OEF — please check Replicon."
                    )
                laborcodeoefs.append(te_uri)
                default_laborcodeoefs.append({'uri': user_uri, 'label': 'Default ' + name + ' (Code)'})
            homecompany_config = rail.find_first_by_attr_and_get_attr(config.groups, 'id', 'homecompany')
            homecompany_code_uri = rail.find_first_by_attr_and_get_attr(userpropertiescolumns, 'displayText', (homecompany_config.get('name') + ' Code'), 'uri') if homecompany_config.get('type') != 'department' else 'urn:replicon:time-data-export-column:department-group-code'
            allow_lc_update_oef_uri = rail.find_first_by_attr_and_get_attr(useroefcolumns, 'displayText', rail.find_first_by_attr_and_get_attr(config.oefs, 'id', 'allowlcupdate', 'name'), 'uri')
            laborcategory_oef_uri = rail.find_first_by_attr_and_get_attr(timeentryoefcolumns, 'displayText', (rail.find_first_by_attr_and_get_attr(config.oefs, 'id', 'laborcategory', 'name') + ' (Code)'), 'uri')
            workdistribution_oef_uri = rail.find_first_by_attr_and_get_attr(timeentryoefcolumns, 'displayText', (rail.find_first_by_attr_and_get_attr(config.oefs, 'id', 'workdistribution', 'name')), 'uri')
            timesheet_field_lc_oef_name = getattr(config, 'timesheet_field_oef_name_for_lc', None)
            timesheet_field_lc_oef_uri = rail.find_first_by_attr_and_get_attr(
                timeentryoefcolumns, 'displayText', (timesheet_field_lc_oef_name + ' (Code)'), 'uri') if timesheet_field_lc_oef_name else None
            for uri in [
                "urn:replicon:time-data-export-column:user-login-name",
                "urn:replicon:time-data-export-column:entry-date",
                "urn:replicon:time-data-export-totals-column:hours",
                "urn:replicon:time-data-export-column:time-entry-id",
                "urn:replicon:time-data-export-column:comments",
                "urn:replicon:time-data-export-column:timesheet-period",
                "urn:replicon:time-data-export-column:project-name",
                "urn:replicon:time-data-export-column:project-code",
                "urn:replicon:time-data-export-column:task-name",
                "urn:replicon:time-data-export-column:task-hierarchy-name",
                "urn:replicon:time-data-export-column:task-code",
                "urn:replicon:time-data-export-column:task-hierarchy-code",
                "urn:replicon:time-data-export-column:time-off-code-name",
                "urn:replicon:time-data-export-column:time-off-code-description",
                "urn:replicon:time-data-export-column:pay-code-name"
                ]:
                columns_collection.append(add_column(uri))
            for uri in laborcodeoefs:
                columns_collection.append(add_column(uri))
            for item in default_laborcodeoefs:
                columns_collection.append(add_column(item.get('uri'), item.get('label')))
            columns_collection.append(add_column(allow_lc_update_oef_uri))
            columns_collection.append(add_column(laborcategory_oef_uri))
            columns_collection.append(add_column(workdistribution_oef_uri))
            columns_collection.append(add_column(homecompany_code_uri))
            if timesheet_field_lc_oef_uri:
                columns_collection.append(add_column(timesheet_field_lc_oef_uri))
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
            task_id = 'put_file_format',
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
        batch_task >> get_time_data_columns >> get_all_oef_definitions >> get_my_identity >> put_file_format >> catch_error

        return dag

rail.for_each_instance(create_dag)
