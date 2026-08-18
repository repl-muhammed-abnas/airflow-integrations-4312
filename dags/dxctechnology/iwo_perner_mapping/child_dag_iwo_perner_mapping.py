import rail
from dxctechnology.iwo_perner_mapping import request_payload
from dxctechnology.iwo_perner_mapping import response_filter


def create_child_dag_wbs(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id = f'dxctechnology_iwo_perner_mapping_{config.sub_erp_name}_child{dag_id_postfix}',
        description = f'DXC_IWO Perner Mapping Automation Child V1.0 - {config.sub_erp_name}',
        company_key = config.company_key,
        replicon_conn_id = config.replicon_conn_id,
        max_active_runs = config.max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_config")

        has_personnel_number = rail.IfOperator(
            task_id = "has_personnel_number",
            test = "{{dag_run.conf.COMPASSPersonnelNumber | is_truthy and dag_run.conf.C1GSAPPersonnelNumber | is_truthy}}",
            yes_task = "has_c1useruri",
            no_task = "log_no_personnel_number"
        )

        has_c1useruri = rail.IfOperator(
            task_id = "has_c1useruri",
            test = '{{ dag_run.conf.C1useruri | is_truthy}}',
            yes_task = "get_user_on_useruri",
            no_task = "get_user_on_empid"
        )

        get_user_on_useruri = rail.RepliconServiceOperator(
            task_id="get_user_on_useruri",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_user_on_useruri_payload,
            response_filter=response_filter.get_filtered_output_useruri
        )

        get_user_on_empid = rail.RepliconServiceOperator(
            task_id="get_user_on_empid",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_user_on_empid_payload,
            response_filter=response_filter.get_filtered_output_empid
        )

        user_details = rail.PythonOperator(
            task_id='user_details',
            python_callable=request_payload.get_user_details,
        )

        has_user_uri = rail.IfOperator(
            task_id = "has_user_uri",
            test =  lambda: bool(rail.result('user_details')['useruri']) and rail.result('user_details')['status']=='True',
            yes_task = "has_length",
            no_task = "log_notfounddisable",
        )

        has_length = rail.IfOperator(
            task_id = "has_length",
            test = lambda: rail.result('user_details')['length'] == 1,
            yes_task = "has_type",
            no_task = "log_multiple_users",
        )

        has_type = rail.IfOperator(
            task_id = "has_type",
            test = lambda: bool(rail.result('user_details')['type']),
            yes_task = "check_types_presence",
            no_task = "log_ccode_not_assigned",
        )

        check_types_presence = rail.IfOperator(
            task_id = "check_types_presence",
            test = lambda: bool(rail.result('user_details')['type'] not in ['COMPASS','C1']),
            yes_task = "log_not_c1_compass",
            no_task = "check_c1_or_compass",
        )

        check_c1_or_compass =rail.IfOperator(
            task_id = "check_c1_or_compass",
            test = lambda: rail.result('user_details')['type'] == 'COMPASS',
            yes_task = "update_perner_udf_compass",
            no_task = "update_perner_udf_c1",
        )

        update_perner_udf_compass = rail.RepliconServiceOperator(
            task_id="update_perner_udf_compass",
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('user_details') | attr_or_default('useruri','')}}",
                "customFieldUri": "{{dag_run.conf.Udfuri}}",
                "value": "{{dag_run.conf.C1GSAPPersonnelNumber}}"
            },
        )

        update_perner_udf_c1 = rail.RepliconServiceOperator(
            task_id="update_perner_udf_c1",
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data={
                "objectUri": "{{ result('user_details') | attr_or_default('useruri','')}}",
                "customFieldUri": "{{dag_run.conf.Udfuri}}",
                "value": "{{ dag_run.conf.COMPASSPersonnelNumber}}"
            },
        )

        log_notfounddisable = rail.WriteLogOperator(
            task_id = 'log_notfounddisable',
            message = 'User not found/disabled in Replicon',
            severity='Exception',
            properties = request_payload.get_properties_exception
        )

        log_multiple_users = rail.WriteLogOperator(
            task_id = 'log_multiple_users',
            message = 'Multiple users found in Replicon with same employee id',
            severity='Exception',
            properties = request_payload.get_properties_exception
        )

        log_ccode_not_assigned= rail.WriteLogOperator(
            task_id = 'log_ccode_not_assigned',
            message = 'Company code is not assigned to user {{result("user_details") | attr_or_default("name","")}}',
            severity='Exception',
            properties = request_payload.get_properties_exception
        )

        log_not_c1_compass = rail.WriteLogOperator(
            task_id = 'log_not_c1_compass',
            # pylint: disable=line-too-long
            message = 'Company code assigned to user {{result("user_details") | attr_or_default("name","")}} does not belong to C1 or COMPASS. Current company code assigned is {{result("user_details") | attr_or_default("companycode","")}}',
            severity='Exception',
            properties = request_payload.get_properties_exception
        )

        log_success_compass = rail.WriteLogOperator(
            task_id = 'log_success_compass',
            message = 'Perner UDF is updated successfully for COMPASS user {{result("user_details") | attr_or_default("name","")}}',
            severity='Success',
            properties = {
                'employeeid': "{{ dag_run.conf.COMPASSPersonnelNumber }}",
                'value': "{{ dag_run.conf.C1GSAPPersonnelNumber }}",
                'status': 'Success',
            }
        )

        log_success_c1 = rail.WriteLogOperator(
            task_id = 'log_success_c1',
            message = 'Perner UDF is updated successfully for C1 user {{result("user_details") | attr_or_default("name","")}}',
            severity='Success',
            properties = {
                'employeeid': "{{ dag_run.conf.C1GSAPPersonnelNumber }}",
                'value': "{{ dag_run.conf.COMPASSPersonnelNumber }}",
                'status': 'Success',
            }
        )

        log_no_personnnel_number = rail.WriteLogOperator(
            task_id = 'log_no_personnel_number',
            message = '\
                {%- if dag_run.conf.COMPASSPersonnelNumber | is_falsy -%} \
                    COMPASSPersonnelNumber is blank \
                {%- endif -%}\
                {%- if dag_run.conf.C1GSAPPersonnelNumber | is_falsy -%} \
                    C1GSAPPersonnelNumber is blank \
                {%- endif -%}',
            severity='Exception',
            properties = {
                'employeeid': '{{ dag_run.conf.COMPASSPersonnelNumber }}',
                'value': '{{ dag_run.conf.C1GSAPPersonnelNumber }}',
                'status': 'Exception',
            }
        )

        has_personnel_number >> rail.Label("Yes") >> has_c1useruri >> rail.Label("Yes") >> get_user_on_useruri >> user_details >> has_user_uri
        has_c1useruri >> rail.Label("No") >> get_user_on_empid >> user_details >> has_user_uri
        has_user_uri >> rail.Label("Yes") >> has_length >> rail.Label("Yes") >> has_type
        has_user_uri >> rail.Label("No") >> log_notfounddisable
        has_length >> rail.Label("No") >> log_multiple_users
        has_personnel_number >> rail.Label("No") >> log_no_personnnel_number
        has_type >> rail.Label("No") >> log_ccode_not_assigned
        has_type >> rail.Label("Yes") >> check_types_presence >> rail.Label("NotCompass/C1") >> log_not_c1_compass
        check_types_presence >> rail.Label("Compass/C1") >> check_c1_or_compass >> rail.Label("Compass") >> update_perner_udf_compass >> log_success_compass
        check_c1_or_compass >> rail.Label("C1") >> update_perner_udf_c1 >> log_success_c1

    return dag

rail.for_each_instance(create_child_dag_wbs)
