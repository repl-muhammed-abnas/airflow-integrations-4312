import rail
from dxctechnology.adhoc.ftp_wbs_master_adhoc import request_payload

def create_child_dag_wbs(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id = f'dxctechnology_ftp_wbs_master_child_adhoc{dag_id_postfix}',
        description = 'DXC_FTP_WBS_MASTER_Automation Child ADHOC',
        company_key = config.company_key,
        replicon_conn_id = config.replicon_conn_id,
        max_active_runs = config.max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_config")

        has_mandatory_fields = rail.IfOperator(
            task_id ='has_mandatory_fields',
            test = request_payload.get_all_mandatory_check,
            yes_task="load_project",
            no_task="log_madatory_fields_not_present"
        )

        log_madatory_fields_not_present =rail.WriteLogOperator(
            task_id = 'log_madatory_fields_not_present',
            message = '\
                {%- if dag_run.conf.wbs | is_falsy -%} \
                    Project Name is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.projectcode | is_falsy -%} \
                    Project code is not present in payload, \
                {%- endif -%}',
            severity='Exception',
            properties = request_payload.get_properties_exception
        )

        load_project = rail.RepliconServiceOperator(
            task_id='load_project',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data= request_payload.get_project_payload,
            response_filter=lambda resp: resp.json()['d'][0]['projectDetails'] if resp.json()['d'][0]['projectDetails'] else None,
        )

        does_project_exist = rail.IfOperator(
            task_id="does_project_exist",
            test="{{ result('load_project') | is_truthy }}",
            yes_task="apply_project_modifications",
            no_task="log_not_present",
        )

        log_not_present =rail.WriteLogOperator(
            task_id = 'log_not_present',
            message = 'WBS is not present in Replicon',
            severity='Exception',
            properties = request_payload.get_properties_exception
        )

        apply_project_modifications = rail.RepliconServiceOperator(
            task_id = 'apply_project_modifications',
            endpoint ='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data = request_payload.get_project_modifications,
        )

        log_success =rail.WriteLogOperator(
            task_id = 'log_success',
            message = 'WBS Successfully Updated',
            severity='Success',
            properties = request_payload.get_properties_success
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        has_mandatory_fields >> rail.Label('Yes') >> load_project >> does_project_exist >> rail.Label('Yes') >> apply_project_modifications
        apply_project_modifications >> log_success >> finish
        has_mandatory_fields >> rail.Label('No') >> log_madatory_fields_not_present
        does_project_exist >> rail.Label('No') >> log_not_present >> finish

    return dag

rail.for_each_instance(create_child_dag_wbs)
