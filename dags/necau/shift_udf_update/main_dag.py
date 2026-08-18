import hashlib
from pendulum import datetime
import rail

from necau.shift_udf_update.utils import request_payload
from necau.shift_udf_update.utils import python_callable_method

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f"necau_shift_udf_update_{config.instance}",
        description=f"NECAU Shift UDF update {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date= datetime(2022, 4, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        def map_shift_list(resp):
            if not resp:
                return None
            return list(
                map(lambda row:
                    {
                        "name": row['cells'][0]['textValue'],
                        "uri":  row['cells'][2]['uri'],
                        "status": (row['cells'][1]['textValue']).lower(),
                        "reference": row['cells'][2]['uri'].split(':')[-1],
                        # pylint: disable=line-too-long
                        "md5": hashlib.md5((str(row['cells'][0]['textValue'])+"," \
                                            + str(row['cells'][2]['uri'])+"," \
                                            + str((row['cells'][1]['textValue']).lower()) + "," \
                                            + str(row['cells'][2]['uri'].split(':')[-1])).encode()).hexdigest()
                    },
                    resp['rows'])) if resp['rows'] else []

        get_all_shift_data = rail.RepliconServiceOperator(
            task_id='get_all_shift_data',
            endpoint='/services/ShiftListService1.svc/GetData',
            data=request_payload.get_all_shift_data,
            data_handler=map_shift_list,
        )

        write_csv_file = rail.WriteCSVFileOperator(
            task_id="write_csv_file",
            source=lambda: rail.result('get_all_shift_data'),
            header=[
                    "name",
                    "uri",
                    "status",
                    "reference",
                    "md5"],
            row=["{{item.name}}", "{{ item.uri }}", "{{ item.status }}",
                 "{{ item.reference }}", "{{ item.md5 }}"],
        )

        create_shift_list_collection = rail.CreateCollectionOperator(
            task_id='create_shift_list_collection',
            source=lambda: rail.result('write_csv_file'),
            name="shiftlist"
        )

        download_reference_file = rail.SFTPDownloadFileOperator(
            task_id='download_reference_file',
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.reference_filepath,
        )

        parse_reference_file = rail.LoadCSVFileOperator(
            task_id="parse_reference_file",
            document="{{result('download_reference_file')}}",
            headers=["name", "uri", "status",
                     "reference", "md5"]
        )

        create_reference_shift_list_collection = rail.CreateCollectionOperator(
            task_id='create_reference_shift_list_collection',
            name="referenceshiftlist",
            source="{{result('parse_reference_file')}}",
            columns=["name", "uri", "status",
                     "reference", "md5"]
        )

        get_new_shift_to_add_or_update = rail.QueryCollectionOperator(
            task_id='get_new_shift_to_add_or_update',
            query="""SELECT * FROM shiftlist WHERE md5 NOT IN \
                (SELECT DISTINCT md5 FROM referenceshiftlist)""",
        )

        has_new_data = rail.IfOperator(
            task_id='has_new_data',
            test='{{ result("get_new_shift_to_add_or_update", "length") > 0 }}',
            yes_task="get_all_user_custom_fields",
            no_task="finish",
        )

        get_all_user_custom_fields = rail.RepliconServiceOperator(
            task_id="get_all_user_custom_fields",
            endpoint="services/CustomFieldService1.svc/GetAllCustomFields",
            data={'objectUri': 'urn:replicon:object-type:user'},
        )

        get_autoschedule_assignment_uri = rail.PythonOperator(
            task_id='get_autoschedule_assignment_uri',
            python_callable=python_callable_method.get_auto_schedule_assignment_uri,
            op_args=[config.auto_schedule_assignment_displaytext]
        )

        get_custom_field_details = rail.RepliconServiceOperator(
            task_id='get_custom_field_details',
            endpoint='/services/CustomFieldService1.svc/GetCustomFieldDetails',
            data=request_payload.get_custom_field_data
        )

        get_all_custom_field_dropdown_options = rail.RepliconServiceOperator(
            task_id='get_all_custom_field_dropdown_options',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                "customFieldUri": "{{ result('get_autoschedule_assignment_uri')}}"
            }
        )

        get_drop_down_option_detail_in_csv = rail.WriteCSVFileOperator(
            task_id='get_drop_down_option_detail_in_csv',
            source="{{ result('get_all_custom_field_dropdown_options') | to_json }}",
            header=[
                    'name',
                    'isdefault',
                    'status',
                    'uri',
                    'reference'],

            row=['{{ item.displayText }}', '{{ item.isDefaultValue }}', '{{ item.isEnabled }}',
                 '{{ item.uri }}', '{{ item.displayText.split("|")[-1] }}']
        )

        create_dropdown_collection = rail.CreateCollectionOperator(
            task_id='create_dropdown_collection',
            name='dropdownlist',
            source="{{ result('get_drop_down_option_detail_in_csv') }}",
            columns=[
                    'name',
                    'isdefault',
                    'status',
                    'uri',
                    'reference'
            ]
        )

        get_all_dropdown_options = rail.QueryCollectionOperator(
            task_id='get_all_dropdown_options',
            query="""SELECT * FROM dropdownlist"""
        )

        get_shifts_not_in_dropdown_list = rail.QueryCollectionOperator(
            task_id='get_shifts_not_in_dropdown_list',
            query="""SELECT * FROM shiftlist \
                WHERE reference\
                NOT IN (SELECT DISTINCT reference FROM dropdownlist)"""
        )

        get_all_dropdown_not_present_as_shift = rail.QueryCollectionOperator(
            task_id='get_all_dropdown_not_present_as_shift',
            query="""SELECT * FROM dropdownlist \
                WHERE reference\
                NOT IN (SELECT DISTINCT reference\
                     FROM shiftlist ) """
        )

        get_all_dropdown_present_as_shift = rail.QueryCollectionOperator(
            task_id='get_all_dropdown_present_as_shift',
            query="""SELECT * FROM dropdownlist\
                 WHERE reference IN \
                    (SELECT DISTINCT reference \
                        FROM shiftlist)"""
        )

        has_extra_dropdown_not_present_as_shift = rail.IfOperator(
            task_id='has_extra_dropdown_not_present_as_shift',
            test='{{result("get_all_dropdown_not_present_as_shift", "length") > 0}}',
            yes_task='add_dropdown_not_present_in_shift_to_dropdown_list',
            no_task='has_dropdown_option_present_as_shift'
        )

        has_dropdown_option_present_as_shift = rail.IfOperator(
            task_id='has_dropdown_option_present_as_shift',
            test='{{ result("get_all_dropdown_present_as_shift", "length") > 0}}',
            yes_task='query_all_shift_data_in_list',
            no_task='has_shift_not_present_in_dropdown'
        )

        has_shift_not_present_in_dropdown = rail.IfOperator(
            task_id='has_shift_not_present_in_dropdown',
            test='{{ result("get_shifts_not_in_dropdown_list", "length") > 0 }}',
            yes_task="add_shift_not_in_dropdown_option_to_dropdown_list",
            no_task='get_final_dropdown_list'
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        query_all_shift_data_in_list = rail.QueryCollectionOperator(
            task_id='query_all_shift_data_in_list',
            query="""SELECT * FROM shiftlist"""
        )
        # pylint: disable=line-too-long
        add_dropdown_not_present_in_shift_to_dropdown_list = rail.PythonOperator(
            task_id="add_dropdown_not_present_in_shift_to_dropdown_list",
            python_callable=python_callable_method.add_dropdown_not_present_in_shift_to_dropdown_list
        )

        add_dropdown_present_as_shift_to_dropdown_list = rail.PythonOperator(
            task_id="add_dropdown_present_as_shift_to_dropdown_list",
            python_callable=python_callable_method.add_dropdown_present_as_shift_to_dropdown_list
        )

        add_shift_not_in_dropdown_option_to_dropdown_list = rail.PythonOperator(
            task_id="add_shift_not_in_dropdown_option_to_dropdown_list",
            python_callable=python_callable_method.add_shift_not_in_dropdown_option_to_dropdown_list
        )

        get_final_dropdown_list = rail.PythonOperator(
            task_id='get_final_dropdown_list',
            python_callable=python_callable_method.get_final_dropdown_list,
        )

        put_dropdown_options = rail.RepliconServiceOperator(
            task_id='put_dropdown_options',
            endpoint='/services/CustomFieldService1.svc/PutDropDownOptions',
            data=request_payload.get_request_payload
        )

        get_default_office_schedule_uri = rail.PythonOperator(
            task_id='get_default_office_schedule_uri',
            python_callable=request_payload.get_default_office_schedule_uri,
            op_args=[config.default_office_schedule_name]
        )

        update_dropdown_default_value = rail.RepliconServiceOperator(
            task_id='update_dropdown_default_value',
            endpoint='/services/CustomFieldService1.svc/UpdateDropDownDefaultValue',
            data=request_payload.get_request_payload_to_update_dropdown_default_value
        )

        upload_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_file_to_sftp',
            content="{{ result('write_csv_file')}}",
            remote_filepath=config.reference_filepath
        )

        get_all_shift_data >> write_csv_file >> create_shift_list_collection >> download_reference_file >> parse_reference_file >> create_reference_shift_list_collection >> get_new_shift_to_add_or_update >> has_new_data

        has_new_data >> rail.Label(
            "No") >> finish

        has_new_data >> rail.Label(
            "Yes") >> get_all_user_custom_fields >> get_autoschedule_assignment_uri >> get_custom_field_details >> get_all_custom_field_dropdown_options >> get_drop_down_option_detail_in_csv >> create_dropdown_collection >> get_all_dropdown_options >> get_shifts_not_in_dropdown_list >> get_all_dropdown_not_present_as_shift >> get_all_dropdown_present_as_shift >> has_extra_dropdown_not_present_as_shift
        has_extra_dropdown_not_present_as_shift >> rail.Label(
            "Yes") >> add_dropdown_not_present_in_shift_to_dropdown_list
        has_extra_dropdown_not_present_as_shift >> rail.Label(
            "No") >> has_dropdown_option_present_as_shift
        add_dropdown_not_present_in_shift_to_dropdown_list >> has_dropdown_option_present_as_shift >> rail.Label(
            "Yes") >> query_all_shift_data_in_list >> add_dropdown_present_as_shift_to_dropdown_list
        has_dropdown_option_present_as_shift >> rail.Label(
            "No") >> has_shift_not_present_in_dropdown
        add_dropdown_present_as_shift_to_dropdown_list >> has_shift_not_present_in_dropdown >> rail.Label(
            "Yes") >> add_shift_not_in_dropdown_option_to_dropdown_list
        has_shift_not_present_in_dropdown >> rail.Label(
            "No") >> get_final_dropdown_list

        add_shift_not_in_dropdown_option_to_dropdown_list >> get_final_dropdown_list >> rail.Label(
            "Yes") >> put_dropdown_options >> get_default_office_schedule_uri >> update_dropdown_default_value >> upload_file_to_sftp >> finish
        get_final_dropdown_list >> rail.Label("No") >> finish
    return dag


rail.for_each_instance(create_dag)
