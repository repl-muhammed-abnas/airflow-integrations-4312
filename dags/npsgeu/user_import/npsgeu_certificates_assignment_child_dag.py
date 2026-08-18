
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'npsgeu_user_import_certificates_assignment_child_{config.instance}',
        description=f'NPSGEU_Skills_certificates_assignment_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_skills,
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
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_foreach_query_list_usersforcertifacateassignment_12_35_useruri_present_37'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_foreach_query_list_usersforcertifacateassignment_12_35_useruri_present_37',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_foreach_query_list_usersforcertifacateassignment_12_35_useruri_present_37 = rail.IfOperator(
            task_id='if_foreach_query_list_usersforcertifacateassignment_12_35_useruri_present_37',
            test='''{{ dag_run.conf.useruri | is_truthy }}''',
            yes_task="put_certificate_for_user_38",
            no_task="npsgeu_skillassignment_logs_add_entry_41",
        )

        put_certificate_for_user_38 = rail.RepliconServiceOperator(
            task_id='put_certificate_for_user_38',
            endpoint="/services/SkillService1.svc/PutCertificateForUser",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri'],
                    "employeeId": null,
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "userCertificate": {
                    "target": null,
                    "name": dag_run.conf['certificate'],
                    "issuer": dag_run.conf['issuingorganization'],
                    "issueDate": rail.parse_date(dag_run.conf['issuedate'],"%m/%d/%Y"),
                    "expiryDate": rail.parse_date(dag_run.conf['expirydate'],"%m/%d/%Y"),
                    "customMetadata": []
                }
            }
        )

        npsgeu_skillassignment_logs_add_entry_39 = rail.WriteLogOperator(
            task_id='npsgeu_skillassignment_logs_add_entry_39',
            log="{{ dag_run.conf.skillslogtable }}",
            message="na",
            severity="success",
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "skills|certificates": "{{ dag_run.conf.certificate }}",
                "status": "success",
                "details": "Certificates updated successfully",
                "parentjobid": "{{dag_run.conf.parentjobid}}",
                "childjobid": "{{ dag_run.conf.childjobid }}"
            }
        )

        npsgeu_skillassignment_logs_add_entry_41 = rail.WriteLogOperator(
            task_id='npsgeu_skillassignment_logs_add_entry_41',
            log="{{ dag_run.conf.skillslogtable }}",
            message="na",
            severity="Exception",
            properties={
                "loginname": "{{dag_run.conf.loginname}}",
                "skills|certificates": "{{ dag_run.conf.certificate }}",
                "status": "Exception",
                "details": "User is not present in Replicon",
                "parentjobid": "{{dag_run.conf.parentjobid}}",
                "childjobid": "{{ dag_run.conf.childjobid }}"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.skillslogtable }}",
            message="na",
            trigger_rule='one_failed',
            severity="error",
            properties={
                "loginname": "{{ dag_run.conf.loginname}}",
                "skills|certificates": "{{ dag_run.conf.certificate }}",
                "status": "error",
                "details": "{{get_error_message()}}",
                "parentjobid": "{{dag_run.conf.parentjobid}}",
                "childjobid": "{{ dag_run.conf.childjobid }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> if_foreach_query_list_usersforcertifacateassignment_12_35_useruri_present_37 >> rail.Label('Yes') >> put_certificate_for_user_38
        put_certificate_for_user_38 >> npsgeu_skillassignment_logs_add_entry_39 >> catch_and_log_error
        if_foreach_query_list_usersforcertifacateassignment_12_35_useruri_present_37 >> rail.Label('No') >> npsgeu_skillassignment_logs_add_entry_41
        npsgeu_skillassignment_logs_add_entry_41 >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
