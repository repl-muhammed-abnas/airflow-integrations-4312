
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'dtna_child_update_task_eng_prod_{config.instance}',
        description=f'DTNA_Child_Update_Task_ENG_Prod {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=10,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_request_isclosed_equals_to_1_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_request_isclosed_equals_to_1_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_request_isclosed_equals_to_1_3 = rail.IfOperator(
            task_id='if_request_isclosed_equals_to_1_3',
            test='''{{ dag_run.conf.isclosed == -1 }}''',
            yes_task="_adhoc_http_action_4",
            no_task="if_request_isclosed_equals_to_1_5",
        )

        _adhoc_http_action_4 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_4',
            endpoint="/services/TaskService1.svc/Close",
            data={
                "taskUri": "{{ dag_run.conf.taskuri }}"
            }
        )

        if_request_isclosed_equals_to_1_5 = rail.IfOperator(
            task_id='if_request_isclosed_equals_to_1_5',
            test='''{{ dag_run.conf.isclosed == 1 }}''',
            yes_task="_adhoc_http_action_6",
            no_task="if_request_taskdescription_present_7",
        )

        _adhoc_http_action_6 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_6',
            endpoint="/services/TaskService1.svc/Open",
            data={
                "taskUri": "{{ dag_run.conf.taskuri }}"
            }
        )

        if_request_taskdescription_present_7 = rail.IfOperator(
            task_id='if_request_taskdescription_present_7',
            test='''{{ dag_run.conf.taskdescription | is_truthy }}''',
            yes_task="update_descriptionoftherequiredtask_8",
            no_task="if_request_customfield_dept_cntl_cd_present_11",
        )

        update_descriptionoftherequiredtask_8 = rail.RepliconServiceOperator(
            task_id='update_descriptionoftherequiredtask_8',
            endpoint="/services/TaskService1.svc/UpdateDescription",
            data={
                "taskUri": "{{ dag_run.conf.taskuri }}",
                "description": "{{ dag_run.conf.taskdescription }}"
            }
        )

        log_required_namefor_task_9 = rail.PythonOperator(
            task_id='log_required_namefor_task_9',
            python_callable=lambda dag_run:  dag_run.conf['taskcode'] +
            "-" + dag_run.conf['taskdescription']
        )

        update_nameoftherequiredtask_10 = rail.RepliconServiceOperator(
            task_id='update_nameoftherequiredtask_10',
            endpoint="/services/TaskService1.svc/UpdateName",
            data={
                "taskUri": "{{ dag_run.conf.taskuri }}",
                "name": "{{ result('log_required_namefor_task_9') }}"
            }
        )

        if_request_customfield_dept_cntl_cd_present_11 = rail.IfOperator(
            task_id='if_request_customfield_dept_cntl_cd_present_11',
            test='''{{ dag_run.conf.customfield_dept_cntl_cd | is_truthy }}''',
            yes_task="update_custom_field_d_e_p_t_c_n_t_l_c_d_12",
            no_task="if_request_customfield_job_work_type_present_13",
        )

        update_custom_field_d_e_p_t_c_n_t_l_c_d_12 = rail.RepliconServiceOperator(
            task_id='update_custom_field_d_e_p_t_c_n_t_l_c_d_12',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['taskuri'],
                "customFieldUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:474cdfb3-4e1d-4e87-b0c5-453559721e76",
                "value": dag_run.conf['customfield_dept_cntl_cd']
            }
        )

        if_request_customfield_job_work_type_present_13 = rail.IfOperator(
            task_id='if_request_customfield_job_work_type_present_13',
            test='''{{ dag_run.conf.customfield_job_work_type | is_truthy }}''',
            yes_task="update_custom_field_j_o_b_w_o_r_k_t_y_p_e_14",
            no_task="if_request_customfield_projectengineer_present_15",
        )

        update_custom_field_j_o_b_w_o_r_k_t_y_p_e_14 = rail.RepliconServiceOperator(
            task_id='update_custom_field_j_o_b_w_o_r_k_t_y_p_e_14',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['taskuri'],
                "customFieldUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:52eb35da-d6d2-4e21-bd7e-d469cd5f5cb5",
                "value": dag_run.conf['customfield_job_work_type']
            }
        )

        if_request_customfield_projectengineer_present_15 = rail.IfOperator(
            task_id='if_request_customfield_projectengineer_present_15',
            test='''{{ dag_run.conf.customfield_projectengineer | is_truthy }}''',
            yes_task="update_custom_field_project_engineer_16",
            no_task="if_request_customfield_ewrcondition_present_17",
        )

        update_custom_field_project_engineer_16 = rail.RepliconServiceOperator(
            task_id='update_custom_field_project_engineer_16',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['taskuri'],
                "customFieldUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:cfac323e-088b-4124-abbf-43a4b56bfe72",
                "value": dag_run.conf['customfield_projectengineer']
            }
        )

        if_request_customfield_ewrcondition_present_17 = rail.IfOperator(
            task_id='if_request_customfield_ewrcondition_present_17',
            test='''{{ dag_run.conf.customfield_ewrcondition | is_truthy }}''',
            yes_task="update_custom_field_e_w_rcondition_18",
            no_task="catch_19_19_19",
        )

        update_custom_field_e_w_rcondition_18 = rail.RepliconServiceOperator(
            task_id='update_custom_field_e_w_rcondition_18',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['taskuri'],
                "customFieldUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:851755a0-e004-485b-93a9-5c1997381852",
                "value": dag_run.conf['customfield_ewrcondition']
            }
        )

        catch_19_19_19 = rail.EmptyOperator(
            task_id='catch_19_19_19',
            trigger_rule='one_failed',
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> if_request_isclosed_equals_to_1_3
        if_request_isclosed_equals_to_1_3
        if_request_isclosed_equals_to_1_3 >> rail.Label(
            'Yes') >> _adhoc_http_action_4 >> if_request_isclosed_equals_to_1_5
        if_request_isclosed_equals_to_1_3 >> rail.Label(
            'No') >> if_request_isclosed_equals_to_1_5
        if_request_isclosed_equals_to_1_5 >> rail.Label(
            'Yes') >> _adhoc_http_action_6 >> if_request_taskdescription_present_7
        if_request_isclosed_equals_to_1_5 >> rail.Label(
            'No') >> if_request_taskdescription_present_7
        if_request_taskdescription_present_7 >> rail.Label(
            'Yes') >> update_descriptionoftherequiredtask_8 >> log_required_namefor_task_9 >> \
            update_nameoftherequiredtask_10 >> if_request_customfield_dept_cntl_cd_present_11
        if_request_taskdescription_present_7 >> rail.Label(
            'No') >> if_request_customfield_dept_cntl_cd_present_11
        if_request_customfield_dept_cntl_cd_present_11 >> rail.Label(
            'Yes') >> update_custom_field_d_e_p_t_c_n_t_l_c_d_12 >> if_request_customfield_job_work_type_present_13
        if_request_customfield_dept_cntl_cd_present_11 >> rail.Label(
            'No') >> if_request_customfield_job_work_type_present_13
        if_request_customfield_job_work_type_present_13 >> rail.Label(
            'Yes') >> update_custom_field_j_o_b_w_o_r_k_t_y_p_e_14 >> if_request_customfield_projectengineer_present_15
        if_request_customfield_job_work_type_present_13 >> rail.Label(
            'No') >> if_request_customfield_projectengineer_present_15
        if_request_customfield_projectengineer_present_15 >> rail.Label(
            'Yes') >> update_custom_field_project_engineer_16 >> if_request_customfield_ewrcondition_present_17
        if_request_customfield_projectengineer_present_15 >> rail.Label(
            'No') >> if_request_customfield_ewrcondition_present_17
        if_request_customfield_ewrcondition_present_17 >> rail.Label(
            'Yes') >> update_custom_field_e_w_rcondition_18 >> finish
        if_request_customfield_ewrcondition_present_17 >> rail.Label(
            'No') >> catch_19_19_19 >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
