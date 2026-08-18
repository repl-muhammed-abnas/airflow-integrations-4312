import rail
from lendingclub.user_import.utils.request_payload import get_customfield_dropdown_option_uris

def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'lendingclub_user_import_update_vendor_dropdown_child_{config.instance}',
        description=f'lendingclub_user_import_update_vendor_dropdown_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.vendor_dropdown_update_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        get_all_custom_field_dropdown_option = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_dropdown_option',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ dag_run.conf.vendor_uri }}"
            },
        )

        create_collection_from_vendor_inputfile_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_vendor_inputfile_csv',
            source="{{ dag_run.conf.vendor }}",
            name="contractorslist_inputfile",
            columns={
                "vendor" : "vendor"
            }
        )

        current_vendor_values = rail.CreateCollectionOperator(
            task_id='current_vendor_values',
            source=lambda: rail.result('get_all_custom_field_dropdown_option'),
            name='vendorvalues'
        )

        new_vendor_values = rail.QueryCollectionOperator(
            task_id='new_vendor_values',
            query="""SELECT * FROM contractorslist_inputfile WHERE
                    lower(vendor) NOT IN (SELECT DISTINCT LOWER(displayText) FROM vendorvalues)""",
            name='newvendorvalues'
        )

        is_vendor_new_values_present = rail.IfOperator(
            task_id='is_vendor_new_values_present',
            test="{{ result('new_vendor_values', 'length') > 0 }}",
            yes_task='put_dropdown_options_vendor',
            no_task='catch_and_log_error'
        )

        put_dropdown_options_vendor = rail.RepliconServiceOperator(
            task_id='put_dropdown_options_vendor',
            endpoint='/services/CustomFieldService1.svc/PutDropDownOptions',
            data=lambda dag_run: {
                'customFieldUri':dag_run.conf['vendor_uri'],
                'customFieldDropDownOptionUris': get_customfield_dropdown_option_uris()
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log = '{{ dag_run.conf.logger}}',
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "UserID": "",
                "Action": "Vendor Dropdown Update",
                "Status": "Error",
                "Details": "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        get_all_custom_field_dropdown_option >> create_collection_from_vendor_inputfile_csv >> \
        current_vendor_values >> new_vendor_values >> is_vendor_new_values_present

        is_vendor_new_values_present >> rail.Label('Yes') >> put_dropdown_options_vendor >> catch_and_log_error
        is_vendor_new_values_present >> rail.Label('No') >> catch_and_log_error

        catch_and_log_error >> log_to_sumo


    return dag

rail.for_each_instance(create_dag)
