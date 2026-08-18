import rail


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_c1_cwf_purchase_order_user_udf_update_child_{config.instance}",
        description=f"DXCTechnology C1 CWF Purchase order User UDF update child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
        max_active_runs=config.max_active_runs_child

    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_conf")

        can_ignore = rail.IfOperator(
            task_id="can_ignore",
            test="{{dag_run.conf.purchase_order_user_profile == dag_run.conf.purchase_order_input_file}}",
            yes_task="log_success",
            no_task="update_c1_purchase_order_udf"
        )

        update_c1_purchase_order_udf = rail.RepliconServiceOperator(
            task_id="update_c1_purchase_order_udf",
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['user_uri'],
                "customFieldUri": dag_run.conf["c1_purchase_order_udf_uri"],
                    "value": dag_run.conf['purchase_order_input_file']
            }
        )
        log_success = rail.WriteLogOperator(
            task_id="log_success",
            message="Updated user attributes related to Purhaseorder balance in Replicon",
            severity="success",
            properties={
                "workordernumber": "{{dag_run.conf.work_order_number}}",
                "personnelnumber": "{{dag_run.conf.personnel_number}}",
                "companycode": "{{dag_run.conf.company_code}}",
                "purchaseorder": "{{dag_run.conf.purchase_order_input_file}}",
                "status": "success",
                "details": "Updated user attributes related to Purhaseorder balance in Replicon",
                "action": "User_attributes_update"
            }
        )
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='failed',
            message='{{ get_error_message() }}',
            properties={
                "work_order_number": "{{dag_run.conf.work_order_number}}",
                "personnel_number": "{{dag_run.conf.personnel_number}}",
                "company_code": "{{dag_run.conf.company_code}}",
                "purchase_order": "{{dag_run.conf.purchase_order_input_file}}",
                "status": "failed",
                "details": '{{ get_error_message() }}',
                "action": "User_attributes_update"
            }
        )
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )
        can_ignore >> rail.Label("Yes") >> log_success
        can_ignore >> rail.Label("No") >> update_c1_purchase_order_udf >> log_success >> rail.Label("On error") >> catch_and_log_errors \
            >> log_to_sumo
    return dag


rail.for_each_instance(create_child_dag)
