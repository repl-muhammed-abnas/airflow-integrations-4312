import rail
from dxctechnology.ftp_sales_order_import import request_payload
from dxctechnology.ftp_sales_order_import import response_filter
from dxctechnology.ftp_sales_order_import import python_callable_method


def validate_persons_responsible():
    with rail.TaskGroup(group_id='validate_persons_responsible', prefix_group_id=False):

        does_projectmanger_eql_comanager = rail.IfOperator(
            task_id ='does_projectmanger_eql_comanager',
            test = "{{dag_run.conf.Projectmanager|is_truthy and dag_run.conf.Coprojectmanager | is_truthy and \
             dag_run.conf.Projectmanager == dag_run.conf.Coprojectmanager}}",
            yes_task="log_same_manager_comanager",
            no_task="has_coprojectmanger"
        )

        log_same_manager_comanager = rail.WriteLogOperator(
            task_id='log_same_manager_comanager',
            log='{{ result("create_exception_log") }}',
            message='Project Manager and Project CoManager have same user',
        )

        has_coprojectmanger = rail.IfOperator(
            task_id ='has_coprojectmanger',
            test = "{{dag_run.conf.Coprojectmanager | is_truthy}}",
            yes_task="has_both_managers",
            no_task="log_no_comanager"
        )

        log_no_comanager = rail.WriteLogOperator(
            task_id='log_no_comanager',
            log='{{ result("create_exception_log") }}',
            message='Project Comanager is not present',
        )

        has_both_managers = rail.IfOperator(
            task_id ='has_both_managers',
            test = "{{dag_run.conf.Coprojectmanager | is_truthy and dag_run.conf.Projectmanager | is_truthy }}",
            yes_task="get_user_on_empid_both",
            no_task="has_projectmanger_not_coprojectmanger"
        )

        get_user_on_empid_both = rail.RepliconServiceOperator(
            task_id="get_user_on_empid_both",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_user_on_empid_payload,
            response_filter=response_filter.get_filtered_output_empid
        )

        has_projectmanger_not_coprojectmanger = rail.IfOperator(
            task_id ='has_projectmanger_not_coprojectmanger',
            test = "{{dag_run.conf.Coprojectmanager | is_falsy and dag_run.conf.Projectmanager | is_truthy}}",
            yes_task="get_user_on_empid_single",
            no_task="user_details"
        )

        get_user_on_empid_single = rail.RepliconServiceOperator(
            task_id="get_user_on_empid_single",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_user_on_empid_payload_2,
            response_filter=response_filter.get_filtered_output_empid
        )

        user_details = rail.PythonOperator(
            task_id='user_details',
            python_callable= python_callable_method.get_user_details,
        )

        did_both_persons_load_successfully = rail.IfOperator(
            task_id='did_both_persons_load_successfully',
            test = lambda: bool(rail.result('user_details')['useruri']) and bool(rail.result('user_details')['comanageruri']),
            yes_task='check_contractor_projectmanger',
            no_task='log_user_unavailable'
        )

        log_user_unavailable = rail.WriteLogOperator(
            task_id='log_user_unavailable',
            log='{{ result("create_exception_log") }}',
            message=request_payload.get_unavailable_meassage,
        )

        check_contractor_projectmanger = rail.IfOperator(
            task_id='check_contractor_projectmanger',
            test = lambda: rail.result('user_details')['employeegroup']=='Contractor',
            yes_task='log_empgrp_contractor',
            no_task='check_contractor_coprojectmanger'
        )

        log_empgrp_contractor = rail.WriteLogOperator(
            task_id = 'log_empgrp_contractor',
            message = 'Project Manager employee type is contractor',
            severity='Error',
            properties = request_payload.get_properties_error
        )

        check_contractor_coprojectmanger = rail.IfOperator(
            task_id='check_contractor_coprojectmanger',
            test = lambda: rail.result('user_details')['comanageremployeegroup']=='Contractor',
            yes_task='log_coempgrp_contractor',
            no_task='userinfo_loaded'
        )

        log_coempgrp_contractor = rail.WriteLogOperator(
            task_id='log_coempgrp_contractor',
            log='{{ result("create_exception_log") }}',
            message='Project CoManager employee type is Contractor',
        )

        userinfo_loaded = rail.EmptyOperator(task_id='userinfo_loaded')

    does_projectmanger_eql_comanager >> rail.Label("Yes") >> log_same_manager_comanager >> has_coprojectmanger
    does_projectmanger_eql_comanager >> rail.Label("No") >> has_coprojectmanger >> rail.Label("Yes") >> has_both_managers
    has_both_managers >> rail.Label("Yes") >> get_user_on_empid_both >> user_details
    has_coprojectmanger >> rail.Label("No") >> log_no_comanager >> has_both_managers
    has_both_managers >> rail.Label("No") >> has_projectmanger_not_coprojectmanger >> rail.Label("Yes") >> get_user_on_empid_single
    get_user_on_empid_single >> user_details >> did_both_persons_load_successfully >> rail.Label("Yes") >> check_contractor_projectmanger
    check_contractor_projectmanger >> rail.Label('Yes') >> log_empgrp_contractor
    check_contractor_projectmanger >> rail.Label("No") >> check_contractor_coprojectmanger >> rail.Label("No") >> userinfo_loaded
    check_contractor_coprojectmanger >> rail.Label("Yes") >> log_coempgrp_contractor >> userinfo_loaded
    has_projectmanger_not_coprojectmanger >> rail.Label("No") >> user_details
    did_both_persons_load_successfully >> rail.Label("No") >> log_user_unavailable >> check_contractor_projectmanger

    return does_projectmanger_eql_comanager, (userinfo_loaded,log_empgrp_contractor)
