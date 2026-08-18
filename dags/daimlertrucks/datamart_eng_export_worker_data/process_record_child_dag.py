
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long too-many-branches
    with rail.create_airflow_dag(
        dag_id=f'daimlertrucks_datamart_eng_export_worker_data_process_item_child_{config.instance}',
        description=f'DTNA process item record {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_198'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_198',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_198 = rail.PythonOperator(
            task_id='log_198',
            python_callable=lambda:  rail.render_template(
                "{{ dag_run.conf.item.repliconworkerid }}{{ dag_run.conf.item.hiringmanagerid }}{{ dag_run.conf.item.costcenter }}{{ dag_run.conf.item.costcentereffectivedate }}")
        )

        query_list_199 = rail.QueryCollectionOperator(
            task_id='query_list_199',
            query="""SELECT * FROM  referencefile WHERE  referencefile.reference = "{{ result('log_198') }}" """,
        )

        query_list_200 = rail.QueryCollectionOperator(
            task_id='query_list_200',
            query="""SELECT * FROM  useremployeeid_list WHERE  useremployeeid_list.employeeid="{{ dag_run.conf.item.repliconworkerid}}" """,
        )

        log_201 = rail.PythonOperator(
            task_id='log_201',
            python_callable=lambda:   "Employee ID, HIRING_MANAGER_ID, COST_CENTER_NAME and COST_CENTER Effective Date combination is not unique;" if rail.result(
                'query_list_199', 'length') > 1 else "Employee ID is not unique;" if rail.result('query_list_200', 'length') > 1 else ''
        )

        def get_validation_message(item):
            logs = []

            if item['repliconworkerid'] and "," in item['repliconworkerid']:
                logs.append("Replicon Worker ID include ',';")

            if item['hiringmanagerid'] and "," in item['hiringmanagerid']:
                logs.append("Hiring Manager ID include ',';")

            if item['costcenter'] and "," in item['costcenter']:
                logs.append("Cost Center include ',';")

            if item['status'] and "," in item['status']:
                logs.append("Status include ',';")

            if item['clientworkerid'] and "," in item['clientworkerid']:
                logs.append("Client Worker ID include ',';")

            if item['workertype'] and "," in item['workertype']:
                logs.append("Worker type include ',';")

            if item['firstname'] and "," in item['firstname']:
                logs.append("Worker First name include ',';")

            if item['lastname'] and "," in item['lastname']:
                logs.append("Worker Last name include ',';")

            if item['repliconworkerid'] and len(item['repliconworkerid']) > 50:
                logs.append(
                    "Replicon Worker ID is greater than 50 characters;")

            if item['hiringmanagerid'] and len(item['hiringmanagerid']) > 50:
                logs.append("Hiring Manager ID is greater than 50 characters;")

            if item['costcenter'] and len(item['costcenter']) > 100:
                logs.append("Cost Center is greater than 100 characters;")

            if item['status'] and len(item['status']) > 30:
                logs.append("Status is greater than 30 characters;")

            if item['loginname'] and len(item['loginname']) > 50:
                logs.append(
                    "Replicon Login Name is greater than 50 characters;")

            if item['clientworkerid'] and len(item['clientworkerid']) > 50:
                logs.append("Client Worker ID is greater than 50 characters;")

            if item['workertype'] and len(item['workertype']) > 30:
                logs.append("worker type is greater than 50 characters;")

            if item['firstname'] and len(item['firstname']) > 50:
                logs.append("Worker first name is greater than 50 characters;")
            if item['lastname'] and len(item['lastname']) > 50:
                logs.append("Worker last name is greater than 50 characters;")
            if item['email'] and len(item['email']) > 150:
                logs.append("email is greater than 150 characters;")
            if item['approverid'] and len(item['approverid']) > 50:
                logs.append("approverid is greater than 50 characters;")

            if item['initialseng'] and len(item['initialseng']) > 10:
                logs.append("Initials -Eng is greater than 10 characters;")
            if item['managereng'] and len(item['managereng']) > 30:
                logs.append("Manager - Eng  is greater than 30 characters;")

            if item['email'] and "," in item['email']:
                logs.append("Worker Email Address include ',';")

            if item['approverid'] and "," in item['approverid']:
                logs.append("Approver ID include ',';")

            if item['initialseng'] and "," in item['initialseng']:
                logs.append("Initials-Eng include ',';")

            if item['managereng'] and "," in item['managereng']:
                logs.append("Manager-Eng include ',';")

            if item['loginname'] and "," in item['loginname']:
                logs.append("Replicon Login Name include ',';")

            return "".join(logs)

        log_202 = rail.PythonOperator(
            task_id='log_202',
            python_callable=lambda:  get_validation_message(
                rail.get_dag_run_conf()['item'])
        )

        if_log_202_present_203 = rail.IfOperator(
            task_id='if_log_202_present_203',
            test='''{{ result('log_202') | is_truthy  or result('log_201') | is_truthy }}''',
            yes_task="insert_to_reject_list_204",
            no_task="insert_to_valid_list_206",
        )

        insert_to_reject_list_204 = rail.SetVariableOperator(
            task_id='insert_to_reject_list_204',
            append=False,
            name='reject_entry',
            value={
                "repliconworkerid": "{{ dag_run.conf.item.repliconworkerid }}",
                "hiringmanagerid": "{{ dag_run.conf.item.hiringmanagerid }}",
                "costcenter": "{{ dag_run.conf.item.costcenter }}",
                "costcentereffectivedate": "{{ dag_run.conf.item.costcentereffectivedate }}",
                "activedate": "{{ dag_run.conf.item.activedate }}",
                "terminationdate": "{{ dag_run.conf.item.terminationdate }}",
                "status": "{{ dag_run.conf.item.status }}",
                "loginname": "{{ dag_run.conf.item.loginname }}",
                "clientworkerid": "{{ dag_run.conf.item.clientworkerid }}",
                "workertype": "{{ dag_run.conf.item.workertype }}",
                "firstname": "{{ dag_run.conf.item.firstname }}",
                "lastname": "{{ dag_run.conf.item.lastname }}",
                "email": "{{ dag_run.conf.item.email }}",
                "approverid": "{{ dag_run.conf.item.approverid }}",
                "initialseng": "{{ dag_run.conf.item.initialseng }}",
                "managereng": "{{ dag_run.conf.item.managereng }}",
                "reason": "{{ result('log_202') + result('log_201') }}"
            }
        )

        insert_to_valid_list_206 = rail.SetVariableOperator(
            task_id='insert_to_valid_list_206',
            append=False,
            name='valid_entry',
            value={
                "repliconworkerid": "{{ dag_run.conf.item.repliconworkerid }}",
                "hiringmanagerid": "{{ dag_run.conf.item.hiringmanagerid }}",
                "costcenter": "{{ dag_run.conf.item.costcenter }}",
                "costcentereffectivedate": "{{ dag_run.conf.item.costcentereffectivedate }}",
                "activedate": "{{ dag_run.conf.item.activedate }}",
                "terminationdate": "{{ dag_run.conf.item.terminationdate }}",
                "status": "{{ dag_run.conf.item.status }}",
                "loginname": "{{ dag_run.conf.item.loginname }}",
                "clientworkerid": "{{ dag_run.conf.item.clientworkerid }}",
                "workertype": "{{ dag_run.conf.item.workertype }}",
                "firstname": "{{ dag_run.conf.item.firstname }}",
                "lastname": "{{ dag_run.conf.item.lastname }}",
                "email": "{{ dag_run.conf.item.email }}",
                "approverid": "{{ dag_run.conf.item.approverid }}",
                "initialseng": "{{ dag_run.conf.item.initialseng }}",
                "managereng": "{{ dag_run.conf.item.managereng }}"
            }
        )

        log_final_valid_entry = rail.PythonOperator(
            task_id='log_final_valid_entry',
            python_callable=lambda:  rail.get_dag_run_var('valid_entry')
        )

        log_final_reject_entry = rail.PythonOperator(
            task_id='log_final_reject_entry',
            python_callable=lambda:  rail.get_dag_run_var('reject_entry')
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> log_198
        log_198 >> query_list_199 >> query_list_200 >> log_201 >> log_202 >> if_log_202_present_203
        if_log_202_present_203 >> rail.Label(
            'Yes') >> insert_to_reject_list_204 >> log_final_reject_entry >> finish
        if_log_202_present_203 >> rail.Label(
            'No') >> insert_to_valid_list_206 >> log_final_valid_entry >> finish
        finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
