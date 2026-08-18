
import itertools
from datetime import datetime, timedelta, timezone
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid
null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'nrdc_updating_c3_c4_values_{config.instance}',
        description=f'Live|NRDC Updating C3/C4 values {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
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
            no_task='declare_variable_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_variable_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_variable_3 = rail.SetVariableOperator(
            task_id='declare_variable_3',
            append=False,
            name='status',
            value='{{ dag_run.conf.accountstatus }}'
        )

        if_request_memberof_not_contains_c4_4 = rail.IfOperator(
            task_id='if_request_memberof_not_contains_c4_4',
            # pylint: disable=line-too-long
            test="{{ dag_run.conf.memberof | matches('C4') | is_falsy  and dag_run.conf.memberof | matches('C3') | is_falsy  and dag_run.conf.memberof | matches('Delegate') | is_falsy }}",
            yes_task="update_variable_5",
            no_task="log_accountstatus_6",
        )

        update_variable_5 = rail.SetVariableOperator(
            task_id="update_variable_5",
            append=False,
            name='{{ result("declare_variable_3").name }}',
            value="Disabled"
        )

        log_accountstatus_6 = rail.PythonOperator(
            task_id='log_accountstatus_6',
            python_callable=lambda dag_run:  "false" if dag_run.conf['accountstatus'].lower(
            ) == "disabled" else "true"
        )

        if_request_department_blank_7 = rail.IfOperator(
            task_id='if_request_department_blank_7',
            test="{{ dag_run.conf.department | is_falsy  or dag_run.conf.emailaddress | is_falsy  or dag_run.conf.logonname | is_falsy }}",
            yes_task="nrdc_user_import_logs_add_entry_8",
            no_task="log_toidentify_c4or_delegateprimaryprofileexistingusers_10",
        )

        nrdc_user_import_logs_add_entry_8 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_8',
            message="User not Updated",
            severity="Error",
            properties={
                "user": "{{ dag_run.conf.logonname }}",
                "action": "Update",
                "status": "Failed",
                "details": "User not Updated, login name/email or Employee ID or department or employee type not present|{{ dag_run_ecid() }}",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        stop_9 = rail.EmptyOperator(
            task_id='stop_9',
        )

        log_toidentify_c4or_delegateprimaryprofileexistingusers_10 = rail.PythonOperator(
            task_id='log_toidentify_c4or_delegateprimaryprofileexistingusers_10',
            # pylint: disable=line-too-long
            python_callable=lambda dag_run:  "C4" if "C4" in dag_run.conf[
                'currenttype'] else "Delegate" if "Delegate" in dag_run.conf['currenttype'] else "No Primary"
        )

        def get_profile_list(dag_run):
            profile_list = []
            user_uris = dag_run.conf['useruris']
            loginnames = dag_run.conf['loginnames']
            user_types = dag_run.conf['currenttype']
            # pylint: disable=consider-using-enumerate
            for x in range(len(user_uris)):
                profile_list.append({
                    "uri": user_uris[x],
                    "userloginname": loginnames[x],
                    "type": user_types[x]
                })

            return profile_list

        create_list_14 = rail.PythonOperator(
            task_id='create_list_14',
            # pylint: disable=unnecessary-lambda
            python_callable=lambda dag_run: get_profile_list(dag_run)
        )

        create_list_17 = rail.CreateCollectionOperator(
            task_id='create_list_17',
            source=lambda: rail.result('create_list_14'),
            name="existinguserdata",
        )

        log_todayin_m_m_d_d_y_y_y_y_18 = rail.PythonOperator(
            task_id='log_todayin_m_m_d_d_y_y_y_y_18',
            python_callable=lambda: datetime.now(
                timezone.utc).strftime("%m/%d/%Y")
        )

        log_todays_year_19 = rail.PythonOperator(
            task_id='log_todays_year_19',
            python_callable=lambda:  datetime.now(timezone.utc).year
        )

        log_todays_month_20 = rail.PythonOperator(
            task_id='log_todays_month_20',
            python_callable=lambda:  datetime.now(timezone.utc).month
        )

        log_todays_day_21 = rail.PythonOperator(
            task_id='log_todays_day_21',
            python_callable=lambda:  datetime.now(timezone.utc).day
        )

        get_all_policy_sets_22 = rail.RepliconServiceOperator(
            task_id='get_all_policy_sets_22',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",

        )

        get_all_custom_fields_23 = rail.RepliconServiceOperator(
            task_id='get_all_custom_fields_23',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data=lambda: {
                "objectUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user:1"
            }
        )

        def get_customoef_uri(custom_field_info):
            existing_customoefs = rail.result('get_all_custom_fields_23')
            input_department_info = list(filter(
                lambda item: item['displayText'] == custom_field_info, existing_customoefs))
            return input_department_info[0]['uri'] if input_department_info else None

        log_type_u_ri_24 = rail.PythonOperator(
            task_id='log_type_u_ri_24',
            # pylint: disable=line-too-long
            python_callable=lambda: get_customoef_uri("Type")
        )

        if_request_currentprofilecount_equals_to_1_25 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_1_25',
            test='''{{ dag_run.conf.currentprofilecount == 1 }}''',
            yes_task="trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile26",
            no_task="declare_rehire_list_dag_runs",
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile26 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile26',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                # pylint: disable=line-too-long
                "firstname": "Action Fund" if "C4" in rail.result('create_list_14')[0]['type'] else rail.result('create_list_14')[0]['type'],
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": rail.result('create_list_14')[0]['userloginname'],
                "accountstatus": rail.get_dag_run_var(rail.result('declare_variable_3')['name']),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": "zakhter",
                "title": dag_run.conf['title'],
                "useruri": rail.result('create_list_14')[0]['uri'],
                "locationuri": dag_run.conf['locationuri'],
                "type": rail.result('create_list_14')[0]['type'],
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile26 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile26',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile26") }}'
        )

        declare_rehire_list_dag_runs = rail.SetVariableOperator(
            task_id='declare_rehire_list_dag_runs',
            name='rehire_user_process_dag_runs',
            value=[]
        )

        if_request_currentprofilecount_equals_to_2_27 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_2_27',
            test='''{{ dag_run.conf.currentprofilecount == 2 }}''',
            yes_task="foreach_accumulate_list_items_16_28",
            no_task="if_request_currentprofilecount_equals_to_5_30",
        )

        foreach_accumulate_list_items_16_28 = rail.ForEachOperator(
            task_id='foreach_accumulate_list_items_16_28',
            items="{{ result('create_list_14') | to_json}}",
            start_task='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile29',
            end_task='foreach_accumulate_list_items_16_28_end'
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile29 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile29',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund" if "C4" in rail.result('foreach_accumulate_list_items_16_28')[
                    'type'] else rail.result('foreach_accumulate_list_items_16_28')['type'],
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": rail.result('foreach_accumulate_list_items_16_28')['userloginname'],
                "accountstatus": rail.get_dag_run_var(rail.result('declare_variable_3')['name']),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('foreach_accumulate_list_items_16_28')['uri'],
                "locationuri": dag_run.conf['locationuri'],
                "type": rail.result('foreach_accumulate_list_items_16_28')['type'],
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        insert_to_rehire_user_dag_run_list_2 = rail.SetVariableOperator(
            task_id='insert_to_rehire_user_dag_run_list_2',
            append=True,
            name='{{ result("declare_rehire_list_dag_runs").name }}',
            value='{{(result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile29"))[0]}}'
        )

        foreach_accumulate_list_items_16_28_end = rail.EmptyOperator(
            task_id='foreach_accumulate_list_items_16_28_end',
        )

        wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile29 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile29',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_rehire_user_dag_run_list_2").value | to_json }}'
        )

        if_request_currentprofilecount_equals_to_5_30 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_5_30',
            test='''{{ dag_run.conf.currentprofilecount == 5 }}''',
            yes_task="foreach_accumulate_list_items_16_31",
            no_task="if_request_currentprofilecount_equals_to_6_33",
        )

        foreach_accumulate_list_items_16_31 = rail.ForEachOperator(
            task_id='foreach_accumulate_list_items_16_31',
            items="{{ result('create_list_14') | to_json}}",
            start_task='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile32',
            end_task='foreach_accumulate_list_items_16_31_end'
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile32 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile32',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund" if "C4" in rail.result('foreach_accumulate_list_items_16_31')[
                    'type'] else rail.result('foreach_accumulate_list_items_16_31')['type'],
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": rail.result('foreach_accumulate_list_items_16_31')['userloginname'],
                "accountstatus": rail.get_dag_run_var(rail.result('declare_variable_3')['name']),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('foreach_accumulate_list_items_16_31')['uri'],
                "locationuri": dag_run.conf['locationuri'],
                "type": rail.result('foreach_accumulate_list_items_16_31')['type'],
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        insert_to_rehire_user_dag_run_list_5 = rail.SetVariableOperator(
            task_id='insert_to_rehire_user_dag_run_list_5',
            append=True,
            name='{{ result("declare_rehire_list_dag_runs").name }}',
            value='{{(result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile32"))[0]}}'
        )

        foreach_accumulate_list_items_16_31_end = rail.EmptyOperator(
            task_id='foreach_accumulate_list_items_16_31_end',
        )

        wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile32 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile32',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_rehire_user_dag_run_list_5").value | to_json }}'
        )

        if_request_currentprofilecount_equals_to_6_33 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_6_33',
            test='''{{ dag_run.conf.currentprofilecount == 6 }}''',
            yes_task="foreach_accumulate_list_items_16_34",
            no_task="if_request_currentprofilecount_equals_to_7_36",
        )

        foreach_accumulate_list_items_16_34 = rail.ForEachOperator(
            task_id='foreach_accumulate_list_items_16_34',
            items="{{ result('create_list_14') | to_json}}",
            start_task='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile35',
            end_task='foreach_accumulate_list_items_16_34_end'
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile35 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile35',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund" if "C4" in rail.result('foreach_accumulate_list_items_16_34')[
                    'type'] else rail.result('foreach_accumulate_list_items_16_34')['type'],
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": rail.result('foreach_accumulate_list_items_16_34')['userloginname'],
                "accountstatus": rail.get_dag_run_var(rail.result('declare_variable_3')['name']),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('foreach_accumulate_list_items_16_34')['uri'],
                "locationuri": dag_run.conf['locationuri'],
                "type": rail.result('foreach_accumulate_list_items_16_34')['type'],
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        insert_to_rehire_user_dag_run_list_6 = rail.SetVariableOperator(
            task_id='insert_to_rehire_user_dag_run_list_6',
            append=True,
            name='{{ result("declare_rehire_list_dag_runs").name }}',
            value='{{(result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile35"))[0]}}'
        )

        wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile35 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile35',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_rehire_user_dag_run_list_6").value | to_json }}'
        )

        foreach_accumulate_list_items_16_34_end = rail.EmptyOperator(
            task_id='foreach_accumulate_list_items_16_34_end',
        )

        if_request_currentprofilecount_equals_to_7_36 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_7_36',
            test='{{ dag_run.conf.currentprofilecount == 7 }}',
            yes_task="foreach_accumulate_list_items_16_37",
            no_task="if_request_accountstatus_equals_to_disabled_39",
        )

        foreach_accumulate_list_items_16_37 = rail.ForEachOperator(
            task_id='foreach_accumulate_list_items_16_37',
            items="{{ result('create_list_14') | to_json}}",
            start_task='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile38',
            end_task='foreach_accumulate_list_items_16_37_end'
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile38 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile38',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund" if "C4" in rail.result('foreach_accumulate_list_items_16_37')[
                    'type'] else rail.result('foreach_accumulate_list_items_16_37')['type'],
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": rail.result('foreach_accumulate_list_items_16_37')['userloginname'],
                "accountstatus": rail.get_dag_run_var(rail.result('declare_variable_3')['name']),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('foreach_accumulate_list_items_16_37')['uri'],
                "locationuri": dag_run.conf['locationuri'],
                "type": rail.result('foreach_accumulate_list_items_16_37')['type'],
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        insert_to_rehire_user_dag_run_list_7 = rail.SetVariableOperator(
            task_id='insert_to_rehire_user_dag_run_list_7',
            append=True,
            name='{{ result("declare_rehire_list_dag_runs").name }}',
            value='{{(result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile38"))[0]}}'
        )

        wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile38 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile38',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_rehire_user_dag_run_list_7").value | to_json }}'
        )

        foreach_accumulate_list_items_16_37_end = rail.EmptyOperator(
            task_id='foreach_accumulate_list_items_16_37_end',
        )

        if_request_accountstatus_equals_to_disabled_39 = rail.IfOperator(
            task_id='if_request_accountstatus_equals_to_disabled_39',
            test='''{{ dag_run.conf.accountstatus | lower == 'disabled' }}''',
            yes_task="stop_40",
            no_task="declare_variable_41",
        )

        stop_40 = rail.EmptyOperator(
            task_id='stop_40',

        )

        declare_variable_41 = rail.SetVariableOperator(
            task_id='declare_variable_41',
            append=False,
            name='requiredcount',
            value=1
        )

        if_request_currentprofilecount_equals_to_1_c4to_delegateor_delegateto_c41profiles_42 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_1_c4to_delegateor_delegateto_c41profiles_42',
            test='''{{ dag_run.conf.currentprofilecount == 1  and dag_run.conf.memberof | matches('C3') | is_falsy}}''',
            yes_task="update_variable_43",
            no_task="if_request_currentprofilecount_equals_to_1_delegateto_delegateand_c4or_c4to_delegateand_c42profiles_89",
        )

        update_variable_43 = rail.SetVariableOperator(
            task_id='update_variable_43',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=1
        )

        if_request_memberof_contains_c4_44 = rail.IfOperator(
            task_id='if_request_memberof_contains_c4_44',
            test='''{{ dag_run.conf.memberof | matches('C4')  and dag_run.conf.memberof | matches('Delegate') }}''',
            yes_task="update_variable_45",
            no_task="if_declare_variable_41_value_equals_to_1_46",
        )

        update_variable_45 = rail.SetVariableOperator(
            task_id='update_variable_45',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=2
        )

        if_declare_variable_41_value_equals_to_1_46 = rail.IfOperator(
            task_id='if_declare_variable_41_value_equals_to_1_46',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_41')['name']) == 1,
            yes_task="query_list_whereexistingprimaryprofilevalueis_c4or_delegate_47",
            no_task="if_request_currentprofilecount_equals_to_1_delegateto_delegateand_c4or_c4to_delegateand_c42profiles_89",
        )

        query_list_whereexistingprimaryprofilevalueis_c4or_delegate_47 = rail.QueryCollectionOperator(
            task_id='query_list_whereexistingprimaryprofilevalueis_c4or_delegate_47',
            query="""SELECT * FROM  existinguserdata WHERE  existinguserdata.type = 'C4' OR  existinguserdata.type='Delegate'""",
        )

        declare_variable_48 = rail.SetVariableOperator(
            task_id='declare_variable_48',
            append=False,
            name='useruri',
            value=None
        )

        def get_data_from_document(document):
            with rail.lib.readers.get_data_reader(document) as reader:
                return list(reader)

        def get_profilevalueis_c4or_delegate_47(task_name):
            user_profiles = get_data_from_document(rail.result(task_name))
            return user_profiles[0] if user_profiles else {}

        profilevalueis_c4or_delegate_47 = rail.PythonOperator(
            task_id='profilevalueis_c4or_delegate_47',
            python_callable=lambda: get_profilevalueis_c4or_delegate_47(
                'query_list_whereexistingprimaryprofilevalueis_c4or_delegate_47')
        )

        if_request_memberof_contains_c4_delegateto_c4_49 = rail.IfOperator(
            task_id='if_request_memberof_contains_c4_delegateto_c4_49',
            # pylint: disable=line-too-long
            test="{{ dag_run.conf.memberof | matches('C4') and dag_run.conf.memberof |  matches('Delagate') | is_falsy  and result('profilevalueis_c4or_delegate_47').type == 'Delegate' }}",
            yes_task="updateuserloginname_set_replicon_authentication_for_user_50",
            no_task="if_request_memberof_contains_delegate_c4to_delegate_69",
        )

        updateuserloginname_set_replicon_authentication_for_user_50 = rail.RepliconServiceOperator(
            task_id='updateuserloginname_set_replicon_authentication_for_user_50',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ result('profilevalueis_c4or_delegate_47').uri }}",
                "loginName": "{{ result('profilevalueis_c4or_delegate_47').userloginname }}d",
                "password": "Replicon@12",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        update_email_removingemail_51 = rail.RepliconServiceOperator(
            task_id='update_email_removingemail_51',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('profilevalueis_c4or_delegate_47').uri }}",
                "email": null
            }
        )

        disable_loginoldprimaryprofile_52 = rail.RepliconServiceOperator(
            task_id='disable_loginoldprimaryprofile_52',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ result('profilevalueis_c4or_delegate_47').uri }}"
            }
        )

        update_user_end_date_53 = rail.RepliconServiceOperator(
            task_id='update_user_end_date_53',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('profilevalueis_c4or_delegate_47').uri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": {
                                "year": "{{result('log_todays_year_19')}}",
                                "month": "{{result('log_todays_month_20')}}",
                                "day": "{{result('log_todays_day_21')}}"
                            }
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        log_loginnamewithaf_54 = rail.PythonOperator(
            task_id='log_loginnamewithaf_54',
            python_callable=lambda:  rail.result(
                'profilevalueis_c4or_delegate_47')['userloginname'] + "af"
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def all_result_data_handler(result, loginname):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], result))))
            return list(filter(lambda x: x['loginname'] == loginname, map(lambda row: {
                'username': row['cells'][0]['textValue'] if 'textValue' in row['cells'][0] else None,
                'employeeid': row['cells'][2]['textValue'] if 'textValue' in row['cells'][2] else None,
                'status': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
                'loginname': row['cells'][1]['textValue'],
                'useruri': row['cells'][1]['uri']
            }, flaten_rows)))

        search_users_55 = rail.RepliconServicePageOperator(
            task_id="search_users_55",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('log_loginnamewithaf_54')
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, rail.result('log_loginnamewithaf_54'))
        )

        if_search_users_55_users_less_than_1_56 = rail.IfOperator(
            task_id='if_search_users_55_users_less_than_1_56',
            test="{{result('search_users_55') | length == 0}}",
            yes_task="trigger_dag_run_live_nrdc_basic_add_update57",
            no_task="if_search_users_55_users_greater_than_0_58",
        )

        trigger_dag_run_live_nrdc_basic_add_update57 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update57',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('profilevalueis_c4or_delegate_47')['userloginname'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "C4",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "NA",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "C4 Timesheet",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update57 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update57',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update57") }}'
        )

        if_search_users_55_users_greater_than_0_58 = rail.IfOperator(
            task_id='if_search_users_55_users_greater_than_0_58',
            test="{{result('search_users_55') | length > 0}}",
            yes_task="log_useruribasedonthesuffix_59",
            no_task="if_request_memberof_contains_delegate_c4to_delegate_69",
        )

        def get_existing_user_uri(task_name):
            profile_name = rail.result(task_name)
            user_info = list(filter(
                lambda item: item['loginname'] == profile_name, rail.result('search_users_55')))
            return user_info[0]['useruri'] if user_info else None

        log_useruribasedonthesuffix_59 = rail.PythonOperator(
            task_id='log_useruribasedonthesuffix_59',
            python_callable=lambda:  get_existing_user_uri(
                'log_loginnamewithaf_54')
        )

        if_log_useruribasedonthesuffix_59_present_60 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_59_present_60',
            test='''{{ result('log_useruribasedonthesuffix_59') | is_truthy }}''',
            yes_task="log_userstatusbasedonthesuffix_61",
            no_task="if_search_users_55_users_blank_67",
        )

        def get_existing_user_status(prof_task_name, user_list_task_name):
            profile_name = rail.result(prof_task_name)
            user_info = list(filter(
                lambda item: item['loginname'] == profile_name, rail.result(user_list_task_name)))
            return user_info[0]['status'] if user_info else 'False'

        log_userstatusbasedonthesuffix_61 = rail.PythonOperator(
            task_id='log_userstatusbasedonthesuffix_61',
            python_callable=lambda:  get_existing_user_status(
                'log_loginnamewithaf_54', 'search_users_55')
        )

        if_log_userstatusbasedonthesuffix_61_equals_to_false_62 = rail.IfOperator(
            task_id='if_log_userstatusbasedonthesuffix_61_equals_to_false_62',
            test='''{{ result('log_userstatusbasedonthesuffix_61') == 'False' }}''',
            yes_task="re_enable_userprofile_63",
            no_task="if_search_users_55_users_blank_67",
        )

        re_enable_userprofile_63 = rail.RepliconServiceOperator(
            task_id='re_enable_userprofile_63',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_59') }}"
            }
        )

        removeenddate_64 = rail.RepliconServiceOperator(
            task_id='removeenddate_64',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('log_useruribasedonthesuffix_59') }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": null
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        updatetoprimaryprofile_set_s_s_o_authentication_for_user_65 = rail.RepliconServiceOperator(
            task_id='updatetoprimaryprofile_set_s_s_o_authentication_for_user_65',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_59') }}",
                "loginName": "{{ result('profilevalueis_c4or_delegate_47').userloginname }}"
            }
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile66 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile66',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": dag_run.conf['logonname'],
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('log_useruribasedonthesuffix_59'),
                "locationuri": dag_run.conf['locationuri'],
                "type": "C4",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile66 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile66',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile66") }}'
        )

        if_search_users_55_users_blank_67 = rail.IfOperator(
            task_id='if_search_users_55_users_blank_67',
            test='''{{result('search_users_55') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update68",
            no_task="if_request_memberof_contains_delegate_c4to_delegate_69",
        )

        trigger_dag_run_live_nrdc_basic_add_update68 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update68',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('profilevalueis_c4or_delegate_47')['userloginname'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "C4",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "NA",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "C4 Timesheet",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update68 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update68',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update68") }}'
        )

        if_request_memberof_contains_delegate_c4to_delegate_69 = rail.IfOperator(
            task_id='if_request_memberof_contains_delegate_c4to_delegate_69',
            # pylint: disable=line-too-long
            test='''{{ dag_run.conf.memberof | matches('Delegate')  and dag_run.conf.memberof | matches('C4') | is_falsy and result('profilevalueis_c4or_delegate_47').type == 'C4' }}''',
            yes_task="updateuserloginname_set_replicon_authentication_for_user_70",
            no_task="if_request_currentprofilecount_equals_to_1_delegateto_delegateand_c4or_c4to_delegateand_c42profiles_89",
        )

        updateuserloginname_set_replicon_authentication_for_user_70 = rail.RepliconServiceOperator(
            task_id='updateuserloginname_set_replicon_authentication_for_user_70',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ result('profilevalueis_c4or_delegate_47').uri }}",
                "loginName": "{{ result('profilevalueis_c4or_delegate_47').userloginname }}",
                "password": "Replicon@12",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        update_email_removingemail_71 = rail.RepliconServiceOperator(
            task_id='update_email_removingemail_71',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('profilevalueis_c4or_delegate_47').uri }}",
                "email": null
            }
        )

        disable_loginoldprimaryprofile_72 = rail.RepliconServiceOperator(
            task_id='disable_loginoldprimaryprofile_72',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ result('profilevalueis_c4or_delegate_47').uri }}"
            }
        )

        update_user_end_date_73 = rail.RepliconServiceOperator(
            task_id='update_user_end_date_73',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('profilevalueis_c4or_delegate_47').uri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": {
                                "year": "{{result('log_todays_year_19')}}",
                                "month": "{{result('log_todays_month_20')}}",
                                "day": "{{result('log_todays_day_21')}}"
                            }
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        log_loginnamewithd_74 = rail.PythonOperator(
            task_id='log_loginnamewithd_74',
            python_callable=lambda:  rail.result('profilevalueis_c4or_delegate_47')[
                'userloginname'] + 'd'
        )

        search_users_75 = rail.RepliconServicePageOperator(
            task_id="search_users_75",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('log_loginnamewithd_74')
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, rail.result('log_loginnamewithd_74'))
        )

        if_search_users_75_users_less_than_1_76 = rail.IfOperator(
            task_id='if_search_users_75_users_less_than_1_76',
            test="{{result('search_users_75') | length == 0}}",
            yes_task="trigger_dag_run_live_nrdc_basic_add_update77",
            no_task="if_search_users_75_users_greater_than_0_78",
        )

        trigger_dag_run_live_nrdc_basic_add_update77 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update77',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Delegate",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('profilevalueis_c4or_delegate_47')['userloginname'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "Delegate",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "NA",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "NA",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update77 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update77',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update77") }}'
        )

        if_search_users_75_users_greater_than_0_78 = rail.IfOperator(
            task_id='if_search_users_75_users_greater_than_0_78',
            test="{{result('search_users_75') | length > 0}}",
            yes_task="log_useruribasedonthesuffix_79",
            no_task="if_request_currentprofilecount_equals_to_1_delegateto_delegateand_c4or_c4to_delegateand_c42profiles_89",
        )

        def get_existing_user_uri_79(task_name):
            profile_name = rail.result(task_name)
            user_info = list(filter(
                lambda item: item['loginname'] == profile_name, rail.result('search_users_75')))
            return user_info[0]['useruri'] if user_info else None

        def get_existing_user_status_81(task_name):
            profile_name = rail.result(task_name)
            user_info = list(filter(
                lambda item: item['loginname'] == profile_name, rail.result('search_users_75')))
            return user_info[0]['status'] if user_info else 'False'

        log_useruribasedonthesuffix_79 = rail.PythonOperator(
            task_id='log_useruribasedonthesuffix_79',
            python_callable=lambda:  get_existing_user_uri_79(
                'log_loginnamewithd_74')
        )

        if_log_useruribasedonthesuffix_79_present_80 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_79_present_80',
            test='''{{ result('log_useruribasedonthesuffix_79') | is_truthy }}''',
            yes_task="log_userstatusbasedonthesuffix_81",
            no_task="if_log_useruribasedonthesuffix_79_blank_87",
        )

        log_userstatusbasedonthesuffix_81 = rail.PythonOperator(
            task_id='log_userstatusbasedonthesuffix_81',
            python_callable=lambda: get_existing_user_status_81(
                'log_loginnamewithd_74')
        )

        if_log_userstatusbasedonthesuffix_81_equals_to_false_82 = rail.IfOperator(
            task_id='if_log_userstatusbasedonthesuffix_81_equals_to_false_82',
            test='''{{ result('log_userstatusbasedonthesuffix_81') == 'False' }}''',
            yes_task="re_enable_userprofile_83",
            no_task="if_log_useruribasedonthesuffix_79_blank_87",
        )

        re_enable_userprofile_83 = rail.RepliconServiceOperator(
            task_id='re_enable_userprofile_83',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_79') }}"
            }
        )

        removeenddate_84 = rail.RepliconServiceOperator(
            task_id='removeenddate_84',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('log_useruribasedonthesuffix_79') }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": null
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        updatetoprimaryprofile_set_s_s_o_authentication_for_user_85 = rail.RepliconServiceOperator(
            task_id='updatetoprimaryprofile_set_s_s_o_authentication_for_user_85',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_79') }}",
                "loginName": "{{ result('profilevalueis_c4or_delegate_47').userloginname }}"
            }
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile86 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile86',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Delegate",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": dag_run.conf['logonname'],
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('log_useruribasedonthesuffix_79'),
                "locationuri": dag_run.conf['locationuri'],
                "type": "Delegate",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile86 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile86',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile86") }}'
        )

        if_log_useruribasedonthesuffix_79_blank_87 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_79_blank_87',
            test='''{{ result('log_useruribasedonthesuffix_79') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update88",
            no_task="if_request_currentprofilecount_equals_to_1_delegateto_delegateand_c4or_c4to_delegateand_c42profiles_89",
        )

        trigger_dag_run_live_nrdc_basic_add_update88 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update88',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Delegate",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('profilevalueis_c4or_delegate_47')['userloginname'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "Delegate",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "NA",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "NA",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update88 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update88',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update88") }}'
        )

        if_request_currentprofilecount_equals_to_1_delegateto_delegateand_c4or_c4to_delegateand_c42profiles_89 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_1_delegateto_delegateand_c4or_c4to_delegateand_c42profiles_89',
            # pylint: disable=line-too-long
            test="{{ dag_run.conf.currentprofilecount == 1 and dag_run.conf.memberof | matches('C3') | is_falsy  and dag_run.conf.memberof | matches('Delegate')  and dag_run.conf.memberof | matches('C4') }}",
            yes_task="update_variable_90",
            no_task="if_request_currentprofilecount_equals_to_1_c4to_c4_c3or_delegateto_delegate_c36profiles_147",
        )

        update_variable_90 = rail.SetVariableOperator(
            task_id='update_variable_90',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=2
        )

        if_declare_variable_41_value_equals_to_2_91 = rail.IfOperator(
            task_id='if_declare_variable_41_value_equals_to_2_91',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_41')['name']) == 2,
            yes_task="if_request_currenttype_contains_c4_92",
            no_task="if_request_currentprofilecount_equals_to_1_c4to_c4_c3or_delegateto_delegate_c36profiles_147",
        )

        def is_current_type_matches(dag_run, current_type):
            input_current_type = "|".join(dag_run.conf['currenttype'])
            return current_type in input_current_type

        if_request_currenttype_contains_c4_92 = rail.IfOperator(
            task_id='if_request_currenttype_contains_c4_92',
            test=lambda dag_run: is_current_type_matches(dag_run, "C4"),
            yes_task="log_loginnameprimaryprofile_93",
            no_task="if_request_currenttype_contains_delegate_123",
        )

        log_loginnameprimaryprofile_93 = rail.PythonOperator(
            task_id='log_loginnameprimaryprofile_93',
            python_callable=lambda dag_run:  dag_run.conf['logonname'].split(
                "@")[0]
        )

        getuserdata_94 = rail.RepliconServicePageOperator(
            task_id='getuserdata_94',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('log_loginnameprimaryprofile_93')
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, rail.result('log_loginnameprimaryprofile_93'))
        )

        def get_existing_user_uri_98(task_name):
            profile_name = rail.result(task_name)
            user_info = list(filter(
                lambda item: item['loginname'] == profile_name, rail.result('getuserdata_94')))
            return user_info[0]['useruri'] if user_info else None

        log_useruri_primaryprofile_98 = rail.PythonOperator(
            task_id='log_useruri_primaryprofile_98',
            python_callable=lambda:  get_existing_user_uri_98(
                'log_loginnameprimaryprofile_93')
        )

        if_log_useruri_primaryprofile_98_present_99 = rail.IfOperator(
            task_id='if_log_useruri_primaryprofile_98_present_99',
            test='''{{ result('log_useruri_primaryprofile_98') | is_truthy }}''',
            yes_task="updateuserloginname_set_replicon_authentication_for_user_100",
            no_task="log_loginnamewithsuffix_102",
        )

        updateuserloginname_set_replicon_authentication_for_user_100 = rail.RepliconServiceOperator(
            task_id='updateuserloginname_set_replicon_authentication_for_user_100',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ result('log_useruri_primaryprofile_98') }}",
                "loginName": "{{ result('log_loginnameprimaryprofile_93') }}af",
                "password": "Replicon@12",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        update_email_removingemail_101 = rail.RepliconServiceOperator(
            task_id='update_email_removingemail_101',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('log_useruri_primaryprofile_98') }}",
                "email": null
            }
        )

        log_loginnamewithsuffix_102 = rail.PythonOperator(
            task_id='log_loginnamewithsuffix_102',
            python_callable=lambda:  rail.result(
                'log_loginnameprimaryprofile_93') + "d"
        )

        log_t_y_p_e_103 = rail.PythonOperator(
            task_id='log_t_y_p_e_103',
            python_callable=lambda:  '''Delegate'''
        )

        log_timesheet_t_y_p_e_104 = rail.PythonOperator(
            task_id='log_timesheet_t_y_p_e_104',
            python_callable=lambda:  '''No timesheet'''
        )

        def get_existing_user_uri_105(task_name):
            profile_name = rail.result(task_name)
            user_info = list(filter(
                lambda item: item['loginname'] == profile_name, rail.result('getuserdata_94')))
            return user_info[0]['useruri'] if user_info else None

        log_uri_105 = rail.PythonOperator(
            task_id='log_uri_105',
            python_callable=lambda:  get_existing_user_uri_105(
                'log_loginnamewithsuffix_102')
        )

        if_log_uri_105_present_106 = rail.IfOperator(
            task_id='if_log_uri_105_present_106',
            test='''{{ result('log_uri_105') | is_truthy }}''',
            yes_task="log_status_107",
            no_task="if_log_uri_105_blank_113",
        )

        def get_existing_user_status_107():
            profile_name = rail.result('log_loginnamewithsuffix_102')
            user_info = list(filter(
                lambda item: item['displayText'] == profile_name, rail.result('getuserdata_94')))
            return user_info[0]['status'] if user_info else 'False'

        log_status_107 = rail.PythonOperator(
            task_id='log_status_107',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_existing_user_status_107
        )

        if_log_status_107_not_equals_to_true_108 = rail.IfOperator(
            task_id='if_log_status_107_not_equals_to_true_108',
            test='''{{ result('log_status_107') != 'True' }}''',
            yes_task="re_enable_userprofile_109",
            no_task="if_log_uri_105_blank_113",
        )

        re_enable_userprofile_109 = rail.RepliconServiceOperator(
            task_id='re_enable_userprofile_109',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('log_uri_105') }}"
            }
        )

        remove_user_end_date_110 = rail.RepliconServiceOperator(
            task_id='remove_user_end_date_110',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('log_uri_105') }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": null,
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        updatetoprimaryprofile_set_s_s_o_authentication_for_user_111 = rail.RepliconServiceOperator(
            task_id='updatetoprimaryprofile_set_s_s_o_authentication_for_user_111',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('log_uri_105') }}",
                "loginName": "{{ result('log_loginnameprimaryprofile_93') }}"
            }
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile112 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile112',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": rail.result('log_t_y_p_e_103'),
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": "cg",
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('log_uri_105'),
                "locationuri": dag_run.conf['locationuri'],
                "type": rail.result('log_t_y_p_e_103'),
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile112 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile112',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile112") }}'
        )

        if_log_uri_105_blank_113 = rail.IfOperator(
            task_id='if_log_uri_105_blank_113',
            test='''{{ result('log_uri_105') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update114",
            no_task="log_primaryuseruri_115",
        )

        trigger_dag_run_live_nrdc_basic_add_update114 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update114',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Delegate",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_loginnameprimaryprofile_93'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": rail.result('log_t_y_p_e_103'),
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "na",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": rail.result('log_timesheet_t_y_p_e_104'),
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update114 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update114',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update114") }}'
        )

        gather_user_uri = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_uri',
            dag_runs="{{ result('trigger_dag_run_live_nrdc_basic_add_update114')}}",
            dagrun_task_id='log_useruri_71',
            flatten=True
        )

        log_primaryuseruri_115 = rail.PythonOperator(
            task_id='log_primaryuseruri_115',
            python_callable=lambda:  rail.result('gather_user_uri') and rail.result(
                'gather_user_uri')[0] or rail.result('log_uri_105')
        )

        declare_substitute_user_dag_runs = rail.SetVariableOperator(
            task_id='declare_substitute_user_dag_runs',
            name='sub_user_process_dag_runs',
            value=[]
        )

        foreach_accumulate_list_items_16_116 = rail.ForEachOperator(
            task_id='foreach_accumulate_list_items_16_116',
            items="{{ result('create_list_14')| to_json }}",
            start_task='get_all_substitute_user_assignments_for_user_117',
            end_task='foreach_accumulate_list_items_16_116_end'
        )

        get_all_substitute_user_assignments_for_user_117 = rail.RepliconServiceOperator(
            task_id='get_all_substitute_user_assignments_for_user_117',
            endpoint="/services/SubstituteUserAssignmentService1.svc/GetAllSubstituteUserAssignmentsForUser",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_116').uri }}"
            }
        )

        def get_substitueUserUris(primary_user_task, substitute_user_task):
            existing_substitute_users = rail.result(substitute_user_task)
            primary_user_uri = rail.result(primary_user_task)
            user_info = list(filter(
                lambda item: item['user'] and item['user']['uri'] == primary_user_uri, existing_substitute_users))
            return user_info[0]['user']['uri'] if user_info else None

        def get_substitueUserUris_first(first_primary_user_task, substitute_user_task):
            existing_substitute_users = rail.result(substitute_user_task)
            primary_user = rail.result(first_primary_user_task)
            user_info = list(filter(
                lambda item: item['user'] and item['user']['uri'] == primary_user['uri'], existing_substitute_users))
            return user_info[0]['user']['uri'] if user_info else None

        def get_substitueUserUrisbyname(primary_user_task, substitute_user_task):
            existing_substitute_users = rail.result(substitute_user_task)
            primary_user_login = rail.result(primary_user_task)
            user_info = list(filter(
                lambda item: item['user'] and item['user']['loginName'] == primary_user_login, existing_substitute_users))
            return user_info[0]['user']['uri'] if user_info else None

        log_substituteuserassigned_118 = rail.PythonOperator(
            task_id='log_substituteuserassigned_118',
            python_callable=lambda:  get_substitueUserUrisbyname(
                'log_loginnameprimaryprofile_93', 'get_all_substitute_user_assignments_for_user_117')
        )

        if_log_substituteuserassigned_118_blank_119 = rail.IfOperator(
            task_id='if_log_substituteuserassigned_118_blank_119',
            test='''{{ result('log_substituteuserassigned_118') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_assign_substitute_usersv2122",
            no_task="foreach_accumulate_list_items_16_116_end",
        )

        trigger_dag_run_live_nrdc_assign_substitute_usersv2122 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_assign_substitute_usersv2122',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_assignsubstituteusersv2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "suburi": rail.result('log_primaryuseruri_115'),
                "actualuri": rail.result('foreach_accumulate_list_items_16_116')['uri'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        insert_substitute_user_dag_run_list = rail.SetVariableOperator(
            task_id='insert_substitute_user_dag_run_list',
            append=True,
            name='{{ result("declare_substitute_user_dag_runs").name }}',
            value='{{(result("trigger_dag_run_live_nrdc_assign_substitute_usersv2122"))[0]}}'
        )

        foreach_accumulate_list_items_16_116_end = rail.EmptyOperator(
            task_id='foreach_accumulate_list_items_16_116_end',
        )

        wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2122 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2122',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_substitute_user_dag_run_list").value | to_json }}'
        )

        if_request_currenttype_contains_delegate_123 = rail.IfOperator(
            task_id='if_request_currenttype_contains_delegate_123',
            test=lambda dag_run: is_current_type_matches(dag_run, "Delegate"),
            yes_task="log_loginnameprimaryprofile_124",
            no_task="if_request_currentprofilecount_equals_to_1_c4to_c4_c3or_delegateto_delegate_c36profiles_147",
        )

        log_loginnameprimaryprofile_124 = rail.PythonOperator(
            task_id='log_loginnameprimaryprofile_124',
            python_callable=lambda dag_run:  dag_run.conf['logonname'].split(
                "@")[0]
        )

        def get_user_uri_from_list(login_name_task, users_list_task):
            profile_name = rail.result(login_name_task)
            user_info = list(filter(
                lambda item: item['userloginname'] == profile_name, rail.result(users_list_task)))
            return user_info[0]['uri'] if user_info else None

        def get_user_from_list_by_type(type_name, users_list_task):
            user_info = list(filter(
                lambda item: item['type'] == type_name, rail.result(users_list_task)))
            return user_info[0]['userloginname'] if user_info else None

        def get_user_uri_from_list_by_type(type_name, users_list_task):
            user_info = list(filter(
                lambda item: item['type'] == type_name, rail.result(users_list_task)))
            return user_info[0]['uri'] if user_info else None

        def get_user_uri_125(login_name_task, users_list_task):
            profile_name = rail.result(login_name_task)
            user_info = list(filter(
                lambda item: item['loginname'] == profile_name, rail.result(users_list_task)))
            return user_info[0]['useruri'] if user_info else None

        def get_user_status(login_name_task, users_list_task):
            profile_name = rail.result(login_name_task)
            user_info = list(filter(
                lambda item: item['loginname'] == profile_name, rail.result(users_list_task)))
            return user_info[0]['status'] if user_info else 'False'

        log_useruri_primaryprofile_125 = rail.PythonOperator(
            task_id='log_useruri_primaryprofile_125',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_user_uri_from_list(
                'log_loginnameprimaryprofile_124', 'create_list_14')
        )

        log_loginnamewithsuffix_126 = rail.PythonOperator(
            task_id='log_loginnamewithsuffix_126',
            python_callable=lambda:  rail.result(
                'log_loginnameprimaryprofile_124') + "af"
        )

        log_t_y_p_e_127 = rail.PythonOperator(
            task_id='log_t_y_p_e_127',
            python_callable=lambda:  '''Action Fund'''
        )

        log_timesheet_t_y_p_e_128 = rail.PythonOperator(
            task_id='log_timesheet_t_y_p_e_128',
            python_callable=lambda:  '''C4 Timesheet'''
        )

        search_users_129 = rail.RepliconServicePageOperator(
            task_id="search_users_129",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('log_loginnamewithsuffix_126')
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, rail.result('log_loginnamewithsuffix_126'))
        )

        if_search_users_129_users_less_than_1_130 = rail.IfOperator(
            task_id='if_search_users_129_users_less_than_1_130',
            test="{{result('search_users_129') | length == 0}}",
            yes_task="trigger_dag_run_live_nrdc_basic_add_update131",
            no_task="if_search_users_129_users_greater_than_0_132",
        )

        trigger_dag_run_live_nrdc_basic_add_update131 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update131',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_loginnamewithsuffix_126'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "C4",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": rail.result('log_useruri_primaryprofile_125'),
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "C4 Timesheet",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update131 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update131',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update131") }}'
        )

        if_search_users_129_users_greater_than_0_132 = rail.IfOperator(
            task_id='if_search_users_129_users_greater_than_0_132',
            test="{{result('search_users_129') | length > 0}}",
            yes_task="log_useruribasedonthesuffix_133",
            no_task="if_request_currentprofilecount_equals_to_1_c4to_c4_c3or_delegateto_delegate_c36profiles_147",
        )

        log_useruribasedonthesuffix_133 = rail.PythonOperator(
            task_id='log_useruribasedonthesuffix_133',
            # pylint: disable=line-too-long
            python_callable=lambda: get_user_uri_125(
                'log_loginnamewithsuffix_126', 'search_users_129')
        )

        if_log_useruribasedonthesuffix_133_present_134 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_133_present_134',
            test='''{{ result('log_useruribasedonthesuffix_133') | is_truthy }}''',
            yes_task="log_userstatusbasedonthesuffix_135",
            no_task="if_log_useruribasedonthesuffix_133_blank_145",
        )

        log_userstatusbasedonthesuffix_135 = rail.PythonOperator(
            task_id='log_userstatusbasedonthesuffix_135',
            # pylint: disable=line-too-long
            python_callable=lambda: get_user_status(
                'log_loginnamewithsuffix_126', 'search_users_129')
        )

        if_log_userstatusbasedonthesuffix_135_equals_to_false_136 = rail.IfOperator(
            task_id='if_log_userstatusbasedonthesuffix_135_equals_to_false_136',
            test='''{{ result('log_userstatusbasedonthesuffix_135') == 'False' }}''',
            yes_task="re_enable_userprofile_137",
            no_task="if_log_useruribasedonthesuffix_133_blank_145",
        )

        re_enable_userprofile_137 = rail.RepliconServiceOperator(
            task_id='re_enable_userprofile_137',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_133') }}"
            }
        )

        get_all_substitute_user_assignments_for_user_138 = rail.RepliconServiceOperator(
            task_id='get_all_substitute_user_assignments_for_user_138',
            endpoint="/services/SubstituteUserAssignmentService1.svc/GetAllSubstituteUserAssignmentsForUser",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_133') }}"
            }
        )

        log_substituteuserassigned_139 = rail.PythonOperator(
            task_id='log_substituteuserassigned_139',
            # pylint: disable=line-too-long
            python_callable=lambda: get_substitueUserUris(
                'log_useruri_primaryprofile_125', 'get_all_substitute_user_assignments_for_user_138')
        )

        if_log_substituteuserassigned_139_blank_140 = rail.IfOperator(
            task_id='if_log_substituteuserassigned_139_blank_140',
            test='''{{ result('log_substituteuserassigned_139') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_assign_substitute_usersv2143",
            no_task="trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile144",
        )

        trigger_dag_run_live_nrdc_assign_substitute_usersv2143 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_assign_substitute_usersv2143',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_assignsubstituteusersv2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "suburi": rail.result('log_useruri_primaryprofile_125'),
                "actualuri": rail.result('log_useruribasedonthesuffix_133'),
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2143 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2143',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_assign_substitute_usersv2143") }}'
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile144 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile144',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": "f",
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('log_useruribasedonthesuffix_133'),
                "locationuri": dag_run.conf['locationuri'],
                "type": "C4",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile144 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile144',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile144") }}'
        )

        if_log_useruribasedonthesuffix_133_blank_145 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_133_blank_145',
            test='''{{ result('log_useruribasedonthesuffix_133') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update146",
            no_task="if_request_currentprofilecount_equals_to_1_c4to_c4_c3or_delegateto_delegate_c36profiles_147",
        )

        trigger_dag_run_live_nrdc_basic_add_update146 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update146',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": rail.result('log_t_y_p_e_127'),
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_loginnamewithsuffix_126'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": rail.result('log_t_y_p_e_127'),
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": rail.result('log_useruri_primaryprofile_125'),
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "C4 Timesheet",
                "authtype": "Replicon",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update146 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update146',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update146") }}'
        )

        if_request_currentprofilecount_equals_to_1_c4to_c4_c3or_delegateto_delegate_c36profiles_147 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_1_c4to_c4_c3or_delegateto_delegate_c36profiles_147',
            test='''{{ dag_run.conf.currentprofilecount == 1  and dag_run.conf.memberof | matches('C3') }}''',
            yes_task="update_variable_148",
            no_task="if_request_currentprofilecount_equals_to_1_c4to_c4_c3anddelegateor_delegateto_c4_c3and_delegate1to7profiles_230",
        )

        update_variable_148 = rail.SetVariableOperator(
            task_id='update_variable_148',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=6
        )

        def get_primary_user_info(task_name):
            primary_profiles = get_data_from_document(rail.result(task_name))
            return primary_profiles[0] if primary_profiles else {}

        query_list_whereexistingprimaryprofilevalueis_delegate_149 = rail.QueryCollectionOperator(
            task_id='query_list_whereexistingprimaryprofilevalueis_delegate_149',
            query="SELECT * FROM  existinguserdata WHERE existinguserdata.type='Delegate' OR existinguserdata.type = 'C4'",
        )

        get_first_primary_records_149 = rail.PythonOperator(
            task_id='get_first_primary_records_149',
            python_callable=lambda:  get_primary_user_info(
                'query_list_whereexistingprimaryprofilevalueis_delegate_149')
        )

        declare_variable_150 = rail.SetVariableOperator(
            task_id='declare_variable_150',
            append=False,
            name='useruri',
            value="{{ result('get_first_primary_records_149').uri}}"
        )

        if_request_memberof_contains_c4_delegateto_c4_c3addingnewprimaryprofileanddisablingold_151 = rail.IfOperator(
            task_id='if_request_memberof_contains_c4_delegateto_c4_c3addingnewprimaryprofileanddisablingold_151',
            # pylint: disable=line-too-long
            test='''{{ dag_run.conf.memberof | matches('C4')  and dag_run.conf.memberof | matches('C3')  and dag_run.conf.memberof | matches('Delegate') | is_falsy and result('get_first_primary_records_149') | is_truthy and result('get_first_primary_records_149').type == 'Delegate' }}''',
            yes_task="update_variable_152",
            no_task="if_request_memberof_contains_delegate_c4to_delegate_c3addingnewprimaryprofileanddisablingold_174",
        )

        update_variable_152 = rail.SetVariableOperator(
            task_id='update_variable_152',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=6
        )

        if_declare_variable_41_value_equals_to_6_153 = rail.IfOperator(
            task_id='if_declare_variable_41_value_equals_to_6_153',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_41')['name']) == 6,
            yes_task="updateuserloginname_set_replicon_authentication_for_user_154",
            no_task="if_request_memberof_contains_delegate_c4to_delegate_c3addingnewprimaryprofileanddisablingold_174",
        )

        updateuserloginname_set_replicon_authentication_for_user_154 = rail.RepliconServiceOperator(
            task_id='updateuserloginname_set_replicon_authentication_for_user_154',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ result('get_first_primary_records_149').uri }}",
                "loginName": "{{ result('get_first_primary_records_149').userloginname }}d",
                "password": "Replicon@12",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        update_email_removingemail_155 = rail.RepliconServiceOperator(
            task_id='update_email_removingemail_155',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('get_first_primary_records_149').uri }}",
                "email": null
            }
        )

        disable_loginoldprimaryprofile_156 = rail.RepliconServiceOperator(
            task_id='disable_loginoldprimaryprofile_156',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ result('get_first_primary_records_149').uri }}"
            }
        )

        update_user_end_date_157 = rail.RepliconServiceOperator(
            task_id='update_user_end_date_157',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('get_first_primary_records_149').uri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": {
                                "year": "{{ result('log_todays_year_19') }}",
                                "month": "{{ result('log_todays_month_20') }}",
                                "day": "{{ result('log_todays_day_21') }}"
                            }
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        log_loginnamewithaf_158 = rail.PythonOperator(
            task_id='log_loginnamewithaf_158',
            python_callable=lambda:  rail.result('get_first_primary_records_149')[
                'userloginname'] + "af"
        )

        search_users_159 = rail.RepliconServicePageOperator(
            task_id="search_users_159",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('log_loginnamewithaf_158')
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, rail.result('log_loginnamewithaf_158'))
        )

        if_search_users_159_users_less_than_1_160 = rail.IfOperator(
            task_id='if_search_users_159_users_less_than_1_160',
            test="{{ result('search_users_159') | length == 0}}",
            yes_task="trigger_dag_run_live_nrdc_basic_add_update161",
            no_task="if_search_users_159_users_greater_than_0_162",
        )

        trigger_dag_run_live_nrdc_basic_add_update161 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update161',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('get_first_primary_records_149')['userloginname'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "C4",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "NA",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "C4 Timesheet",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update161 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update161',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update161") }}'
        )

        if_search_users_159_users_greater_than_0_162 = rail.IfOperator(
            task_id='if_search_users_159_users_greater_than_0_162',
            test="{{result('search_users_159') | length > 0}}",
            yes_task="log_useruribasedonthesuffix_163",
            no_task="update_variable_173",
        )

        log_useruribasedonthesuffix_163 = rail.PythonOperator(
            task_id='log_useruribasedonthesuffix_163',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_user_uri_125(
                'log_loginnamewithaf_158', 'search_users_159')
        )

        if_log_useruribasedonthesuffix_163_present_164 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_163_present_164',
            test='''{{ result('log_useruribasedonthesuffix_163') | is_truthy }}''',
            yes_task="log_userstatusbasedonthesuffix_165",
            no_task="if_log_useruribasedonthesuffix_163_blank_171",
        )

        log_userstatusbasedonthesuffix_165 = rail.PythonOperator(
            task_id='log_userstatusbasedonthesuffix_165',
            # pylint: disable=line-too-long
            python_callable=lambda: get_user_status(
                'log_loginnamewithaf_158', 'search_users_159')
        )

        if_log_userstatusbasedonthesuffix_165_equals_to_false_166 = rail.IfOperator(
            task_id='if_log_userstatusbasedonthesuffix_165_equals_to_false_166',
            test='''{{ result('log_userstatusbasedonthesuffix_165') == 'False' }}''',
            yes_task="re_enable_userprofile_167",
            no_task="if_log_useruribasedonthesuffix_163_blank_171",
        )

        re_enable_userprofile_167 = rail.RepliconServiceOperator(
            task_id='re_enable_userprofile_167',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_163') }}"
            }
        )

        removeenddate_168 = rail.RepliconServiceOperator(
            task_id='removeenddate_168',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('log_useruribasedonthesuffix_163') }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": null
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        updatetoprimaryprofile_set_s_s_o_authentication_for_user_169 = rail.RepliconServiceOperator(
            task_id='updatetoprimaryprofile_set_s_s_o_authentication_for_user_169',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_163') }}",
                "loginName": "{{ result('get_first_primary_records_149').userloginname }}"
            }
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile170 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile170',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": dag_run.conf['logonname'],
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('log_useruribasedonthesuffix_163'),
                "locationuri": dag_run.conf['locationuri'],
                "type": "C4",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile170 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile170',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile170") }}'
        )

        if_log_useruribasedonthesuffix_163_blank_171 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_163_blank_171',
            test='''{{ result('log_useruribasedonthesuffix_163') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update172",
            no_task="update_variable_173",
        )

        trigger_dag_run_live_nrdc_basic_add_update172 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update172',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('get_first_primary_records_149')['userloginname'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "C4",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": rail.result('log_useruri_primaryprofile_125'),
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "C4 Timesheet",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update172 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update172',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update172") }}'
        )

        update_variable_173 = rail.SetVariableOperator(
            task_id='update_variable_173',
            append=False,
            name='{{ result("declare_variable_150").name }}',
            value="{{ result('log_useruribasedonthesuffix_163') }}"
        )

        query_list_whereexistingprimaryprofilevalueis_c4_174 = rail.QueryCollectionOperator(
            task_id='query_list_whereexistingprimaryprofilevalueis_c4_174',
            query="SELECT * FROM  existinguserdata WHERE existinguserdata.type='Delegate' OR existinguserdata.type = 'C4'",
        )

        get_first_primary_records_174 = rail.PythonOperator(
            task_id='get_first_primary_records_174',
            python_callable=lambda:  get_primary_user_info(
                'query_list_whereexistingprimaryprofilevalueis_c4_174')
        )

        if_request_memberof_contains_delegate_c4to_delegate_c3addingnewprimaryprofileanddisablingold_174 = rail.IfOperator(
            task_id='if_request_memberof_contains_delegate_c4to_delegate_c3addingnewprimaryprofileanddisablingold_174',
            # pylint: disable=line-too-long
            test='''{{ dag_run.conf.memberof | matches('Delegate')  and dag_run.conf.memberof | matches('C3')  and dag_run.conf.memberof | matches('C4') | is_falsy  and result('get_first_primary_records_149').type == 'C4' }}''',
            yes_task="update_variable_175",
            no_task="if_request_memberof_contains_c4_197",
        )

        update_variable_175 = rail.SetVariableOperator(
            task_id='update_variable_175',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=6
        )

        if_declare_variable_41_value_equals_to_6_176 = rail.IfOperator(
            task_id='if_declare_variable_41_value_equals_to_6_176',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_41')['name']) == 6,
            yes_task="updateuserloginname_set_replicon_authentication_for_user_177",
            no_task="if_request_memberof_contains_c4_197",
        )

        updateuserloginname_set_replicon_authentication_for_user_177 = rail.RepliconServiceOperator(
            task_id='updateuserloginname_set_replicon_authentication_for_user_177',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ result('get_first_primary_records_149').uri }}",
                "loginName": "{{ result('get_first_primary_records_149').userloginname }}af",
                "password": "Replicon@12",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        update_email_removingemail_178 = rail.RepliconServiceOperator(
            task_id='update_email_removingemail_178',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('get_first_primary_records_149').uri }}",
                "email": null
            }
        )

        disable_loginoldprimaryprofile_179 = rail.RepliconServiceOperator(
            task_id='disable_loginoldprimaryprofile_179',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ result('get_first_primary_records_149').uri }}"
            }
        )

        update_user_end_date_180 = rail.RepliconServiceOperator(
            task_id='update_user_end_date_180',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('get_first_primary_records_149').uri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": {
                                "year": "{{result('log_todays_year_19')}}",
                                "month": "{{result('log_todays_month_20')}}",
                                "day": "{{result('log_todays_day_21')}}"
                            }
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        log_loginnamewithd_181 = rail.PythonOperator(
            task_id='log_loginnamewithd_181',
            python_callable=lambda:  rail.result('get_first_primary_records_149')[
                'userloginname'] + "d"
        )

        search_users_182 = rail.RepliconServicePageOperator(
            task_id="search_users_182",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('log_loginnamewithd_181')
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, rail.result('log_loginnamewithd_181'))
        )

        if_search_users_182_users_less_than_1_183 = rail.IfOperator(
            task_id='if_search_users_182_users_less_than_1_183',
            test="{{result('search_users_182') | length == 0}}",
            yes_task="trigger_dag_run_live_nrdc_basic_add_update184",
            no_task="if_search_users_182_users_greater_than_0_185",
        )

        trigger_dag_run_live_nrdc_basic_add_update184 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update184',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Delegate",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('get_first_primary_records_149')['userloginname'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "Delegate",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "NA",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "NA",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update184 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update184',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update184") }}'
        )

        if_search_users_182_users_greater_than_0_185 = rail.IfOperator(
            task_id='if_search_users_182_users_greater_than_0_185',
            test="{{result('search_users_182') | length > 0}}",
            yes_task="log_useruribasedonthesuffix_186",
            no_task="update_variable_196",
        )

        log_useruribasedonthesuffix_186 = rail.PythonOperator(
            task_id='log_useruribasedonthesuffix_186',
            python_callable=lambda:  get_user_uri_125(
                'log_loginnamewithd_181', 'search_users_182')
        )

        if_log_useruribasedonthesuffix_186_present_187 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_186_present_187',
            test='''{{ result('log_useruribasedonthesuffix_186') | is_truthy }}''',
            yes_task="log_userstatusbasedonthesuffix_188",
            no_task="if_log_useruribasedonthesuffix_186_blank_194",
        )

        log_userstatusbasedonthesuffix_188 = rail.PythonOperator(
            task_id='log_userstatusbasedonthesuffix_188',
            # pylint: disable=line-too-long
            python_callable=lambda: get_user_status(
                'log_loginnamewithd_181', 'search_users_182')
        )

        if_log_userstatusbasedonthesuffix_188_equals_to_false_189 = rail.IfOperator(
            task_id='if_log_userstatusbasedonthesuffix_188_equals_to_false_189',
            test='''{{ result('log_userstatusbasedonthesuffix_188') == 'False' }}''',
            yes_task="re_enable_userprofile_190",
            no_task="if_log_useruribasedonthesuffix_186_blank_194",
        )

        re_enable_userprofile_190 = rail.RepliconServiceOperator(
            task_id='re_enable_userprofile_190',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_186') }}"
            }
        )

        removeenddate_191 = rail.RepliconServiceOperator(
            task_id='removeenddate_191',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('log_useruribasedonthesuffix_186') }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": null
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        updatetoprimaryprofile_set_s_s_o_authentication_for_user_192 = rail.RepliconServiceOperator(
            task_id='updatetoprimaryprofile_set_s_s_o_authentication_for_user_192',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_186') }}",
                "loginName": "{{ result('get_first_primary_records_149').userloginname }}"
            }
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile193 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile193',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Delegate",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": dag_run.conf['logonname'],
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('log_useruribasedonthesuffix_186'),
                "locationuri": dag_run.conf['locationuri'],
                "type": "Delegate",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile193 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile193',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile193") }}'
        )

        if_log_useruribasedonthesuffix_186_blank_194 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_186_blank_194',
            test='''{{ result('log_useruribasedonthesuffix_186') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update195",
            no_task="update_variable_196",
        )

        trigger_dag_run_live_nrdc_basic_add_update195 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update195',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Delegate",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('get_first_primary_records_149')['userloginname'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "Delegate",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "NA",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "NA",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update195 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update195',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update195") }}'
        )

        update_variable_196 = rail.SetVariableOperator(
            task_id='update_variable_196',
            append=False,
            name='{{ result("declare_variable_150").name }}',
            # pylint: disable=line-too-long
            value="_('data.workato_service.trigger_dag_run_live_nrdc_basic_add_update184.reply.useruri.present? ? _('data.workato_service.trigger_dag_run_live_nrdc_basic_add_update184.reply.useruri : (result('log_useruribasedonthesuffix_186')').present? ? result('log_useruribasedonthesuffix_186')') : _('data.workato_service.trigger_dag_run_live_nrdc_basic_add_update195.reply.useruri)"
        )

        if_request_memberof_contains_c4_197 = rail.IfOperator(
            task_id='if_request_memberof_contains_c4_197',
            test='''{{ dag_run.conf.memberof | matches('C4') and dag_run.conf.memberof | matches('Delegate') }}''',
            yes_task="update_variable_198",
            no_task="if_declare_variable_41_value_equals_to_6_199",
        )

        update_variable_198 = rail.SetVariableOperator(
            task_id='update_variable_198',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=7
        )

        if_declare_variable_41_value_equals_to_6_199 = rail.IfOperator(
            task_id='if_declare_variable_41_value_equals_to_6_199',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_41')['name']) == 6,
            yes_task="query_list_whereexistingprimaryprofilevalueis_delegate_c4_200",
            no_task="if_request_currentprofilecount_equals_to_1_c4to_c4_c3anddelegateor_delegateto_c4_c3and_delegate1to7profiles_230",
        )

        query_list_whereexistingprimaryprofilevalueis_delegate_c4_200 = rail.QueryCollectionOperator(
            task_id='query_list_whereexistingprimaryprofilevalueis_delegate_c4_200',
            query="SELECT * FROM  existinguserdata WHERE  existinguserdata.type = 'C4' OR  existinguserdata.type='Delegate'",
        )

        get_first_primary_records_200 = rail.PythonOperator(
            task_id='get_first_primary_records_200',
            python_callable=lambda:  get_primary_user_info(
                'query_list_whereexistingprimaryprofilevalueis_delegate_c4_200')
        )

        if_first_type_present_c4_c4and_c3_200 = rail.IfOperator(
            task_id='if_first_type_present_c4_c4and_c3_200',
            test='''{{ result('get_first_primary_records_149') | is_truthy }}''',
            yes_task="log_loginnameprimaryprofile_201",
            no_task="if_request_currentprofilecount_equals_to_1_c4to_c4_c3anddelegateor_delegateto_c4_c3and_delegate1to7profiles_230",
        )

        log_loginnameprimaryprofile_201 = rail.PythonOperator(
            task_id='log_loginnameprimaryprofile_201',
            python_callable=lambda:  rail.result(
                'get_first_primary_records_149')['userloginname']
        )

        log_useruri_primaryprofile_202 = rail.PythonOperator(
            task_id='log_useruri_primaryprofile_202',
            python_callable=lambda:  rail.get_dag_run_var(
                rail.result('declare_variable_150')['name'])
        )

        getuserdata_203 = rail.RepliconServicePageOperator(
            task_id="getuserdata_203",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('log_loginnameprimaryprofile_201')
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, rail.result('log_loginnameprimaryprofile_201'))
        )

        create_list_size5_207 = rail.EmptyOperator(
            task_id='create_list_size5_207',
        )

        declare_list_208 = rail.SetVariableOperator(
            task_id='declare_list_208',
            append=False,
            name='c3c4list',
            value=[]
        )

        def create_c3_c4_list(login_task):
            c3_c4_list = []
            c3_c4_list.append({
                "loginname": rail.result(login_task) + "fl",
                "type": "Federal Legislative",
                "timesheetpolicy": "C3 - Federal Legislative"
            })
            c3_c4_list.append({
                "loginname": rail.result(login_task) + "la",
                "type": "Local administrative",
                "timesheetpolicy": "C3 - Local Administrative"
            })
            c3_c4_list.append({
                "loginname": rail.result(login_task) + "ll",
                "type": "Local legislative",
                "timesheetpolicy": "C3 - Local Legislative"
            })
            c3_c4_list.append({
                "loginname": rail.result(login_task) + "sa",
                "type": "State administrative",
                "timesheetpolicy": "C3 - State Administrative"
            })
            c3_c4_list.append({
                "loginname": rail.result(login_task) + "sl",
                "type": "State legislative",
                "timesheetpolicy": "C3 - State Legislative"
            })

            return c3_c4_list

        get_c3_c4_list = rail.PythonOperator(
            task_id='get_c3_c4_list',
            # pylint: disable=line-too-long
            python_callable=lambda:  create_c3_c4_list(
                'log_loginnameprimaryprofile_201')
        )

        declare_list_update_dag_runs_214 = rail.SetVariableOperator(
            task_id='declare_list_update_dag_runs_214',
            name='user_process_update_dag_runs_214',
            value=[]
        )

        foreach_declare_list_208_214 = rail.ForEachOperator(
            task_id='foreach_declare_list_208_214',
            items="{{ result('get_c3_c4_list') | to_json }}",
            start_task='log_uriuser_215',
            end_task='foreach_declare_list_208_214_end'
        )

        def get_user_uri_215(for_each_task, users_list_task):
            profile_name = rail.result(for_each_task)['loginname']
            user_info = list(filter(
                lambda item: item['loginname'] == profile_name, rail.result(users_list_task)))
            return user_info[0]['useruri'] if user_info else None

        def get_user_status_215(for_each_task, users_list_task):
            profile_name = rail.result(for_each_task)['loginname']
            user_info = list(filter(
                lambda item: item['loginname'] == profile_name, rail.result(users_list_task)))
            return user_info[0]['status'] if user_info else 'False'

        log_uriuser_215 = rail.PythonOperator(
            task_id='log_uriuser_215',
            # pylint: disable=line-too-long
            python_callable=lambda: get_user_uri_215(
                'foreach_declare_list_208_214', 'getuserdata_203')
        )

        if_log_uriuser_215_present_216 = rail.IfOperator(
            task_id='if_log_uriuser_215_present_216',
            test='''{{ result('log_uriuser_215') | is_truthy }}''',
            yes_task="log_status_217",
            no_task="if_log_uriuser_215_blank_228",
        )

        log_status_217 = rail.PythonOperator(
            task_id='log_status_217',
            python_callable=lambda: get_user_status_215(
                'foreach_declare_list_208_214', 'getuserdata_203')
        )

        if_log_status_217_not_equals_to_true_218 = rail.IfOperator(
            task_id='if_log_status_217_not_equals_to_true_218',
            test='''{{ result('log_status_217') != 'True' }}''',
            yes_task="re_enable_userprofile_219",
            no_task="if_log_uriuser_215_blank_228",
        )

        re_enable_userprofile_219 = rail.RepliconServiceOperator(
            task_id='re_enable_userprofile_219',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('log_uriuser_215') }}"
            }
        )

        remove_user_end_date_220 = rail.RepliconServiceOperator(
            task_id='remove_user_end_date_220',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('log_uriuser_215') }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": null,
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile221 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile221',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": rail.result('foreach_declare_list_208_214')['type'],
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": dag_run.conf['logonname'],
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('log_uriuser_215'),
                "locationuri": dag_run.conf['locationuri'],
                "type": rail.result('foreach_declare_list_208_214')['type'],
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        insert_to_user_rehire_disable_user_list = rail.SetVariableOperator(
            task_id='insert_to_user_rehire_disable_user_list',
            append=True,
            name='{{ result("declare_list_update_dag_runs_214").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile221"))[0]}}'
        )

        get_all_substitute_user_assignments_for_user_222 = rail.RepliconServiceOperator(
            task_id='get_all_substitute_user_assignments_for_user_222',
            endpoint="/services/SubstituteUserAssignmentService1.svc/GetAllSubstituteUserAssignmentsForUser",
            data={
                "userUri": "{{ result('log_uriuser_215') }}"
            }
        )

        log_substituteuserassigned_223 = rail.PythonOperator(
            task_id='log_substituteuserassigned_223',
            # pylint: disable=line-too-long
            python_callable=lambda: get_substitueUserUrisbyname(
                'log_loginnameprimaryprofile_201', 'get_all_substitute_user_assignments_for_user_222')
        )

        if_log_substituteuserassigned_223_blank_224 = rail.IfOperator(
            task_id='if_log_substituteuserassigned_223_blank_224',
            test='''{{ result('log_substituteuserassigned_223') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_assign_substitute_usersv2227",
            no_task="if_log_uriuser_215_blank_228",
        )

        trigger_dag_run_live_nrdc_assign_substitute_usersv2227 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_assign_substitute_usersv2227',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_assignsubstituteusersv2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "suburi": rail.result('log_useruri_primaryprofile_202'),
                "actualuri": rail.result('log_uriuser_215'),
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        insert_to_assign_substitute_user_list = rail.SetVariableOperator(
            task_id='insert_to_assign_substitute_user_list',
            append=True,
            name='{{ result("declare_list_update_dag_runs_214").name }}',
            value='{{(result("trigger_dag_run_live_nrdc_assign_substitute_usersv2227"))[0]}}'
        )

        if_log_uriuser_215_blank_228 = rail.IfOperator(
            task_id='if_log_uriuser_215_blank_228',
            test='''{{ result('log_uriuser_215') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update229",
            no_task="foreach_declare_list_208_214_end",
        )

        trigger_dag_run_live_nrdc_basic_add_update229 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update229',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": rail.result('foreach_declare_list_208_214')['type'],
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('foreach_declare_list_208_214')['loginname'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": rail.result('foreach_declare_list_208_214')['type'],
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": rail.result('log_useruri_primaryprofile_202'),
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": rail.result('foreach_declare_list_208_214')['timesheetpolicy'],
                "authtype": "replicon",
                "status": dag_run.conf['accountstatus']
            }
        )

        insert_to_user_basic_add_list = rail.SetVariableOperator(
            task_id='insert_to_user_basic_add_list',
            append=True,
            name='{{ result("declare_list_update_dag_runs_214").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_nrdc_basic_add_update229"))[0]}}'
        )

        foreach_declare_list_208_214_end = rail.EmptyOperator(
            task_id='foreach_declare_list_208_214_end',
        )

        get_dag_run_ids_229 = rail.PythonOperator(
            task_id='get_dag_run_ids_229',
            python_callable=lambda: rail.get_dag_run_var(
                rail.result('declare_list_update_dag_runs_214')['name'])
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update229 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update229',
            execution_timeout=timedelta(days=14),
            dag_runs="{{ result('get_dag_run_ids_229') | to_json}}"
        )

        if_request_currentprofilecount_equals_to_1_c4to_c4_c3anddelegateor_delegateto_c4_c3and_delegate1to7profiles_230 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_1_c4to_c4_c3anddelegateor_delegateto_c4_c3and_delegate1to7profiles_230',
            # pylint: disable=line-too-long
            test='''{{ dag_run.conf.currentprofilecount == 1  and dag_run.conf.memberof | matches('Delegate')  and dag_run.conf.memberof | matches('C4')  and dag_run.conf.memberof | matches('C3') }}''',
            yes_task="update_variable_231",
            no_task="if_request_currentprofilecount_equals_to_1_c4to_c3or_delegateto_c35profiles_313",
        )

        update_variable_231 = rail.SetVariableOperator(
            task_id='update_variable_231',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=7
        )

        if_declare_variable_41_value_equals_to_7_232 = rail.IfOperator(
            task_id='if_declare_variable_41_value_equals_to_7_232',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_41')['name']) == 7,
            yes_task="query_list_whereexistingprimaryprofilevalueis_delegate_233",
            no_task="if_request_currentprofilecount_equals_to_1_c4to_c3or_delegateto_c35profiles_313",
        )

        query_list_whereexistingprimaryprofilevalueis_delegate_233 = rail.QueryCollectionOperator(
            task_id='query_list_whereexistingprimaryprofilevalueis_delegate_233',
            query="SELECT * FROM  existinguserdata WHERE  existinguserdata.type='Delegate' OR existinguserdata.type = 'C4'",
        )

        get_first_primary_records_233 = rail.PythonOperator(
            task_id='get_first_primary_records_233',
            python_callable=lambda:  get_primary_user_info(
                'query_list_whereexistingprimaryprofilevalueis_delegate_233')
        )

        declare_variable_234 = rail.SetVariableOperator(
            task_id='declare_variable_234',
            append=False,
            name='useruri',
            value="{{ result('get_first_primary_records_233').uri }}"
        )

        if_first_type_equals_to_delegate_keeping_delegateas_primary_235 = rail.IfOperator(
            task_id='if_first_type_equals_to_delegate_keeping_delegateas_primary_235',
            test='''{{ result('get_first_primary_records_233').type == 'Delegate' }}''',
            yes_task="log_loginnamewithaf_236",
            no_task="if_first_type_equals_to_c4_c4to_delegate_258",
        )

        log_loginnamewithaf_236 = rail.PythonOperator(
            task_id='log_loginnamewithaf_236',
            python_callable=lambda:  rail.result('get_first_primary_records_233')[
                'userloginname'] + "af"
        )

        search_users_237 = rail.RepliconServicePageOperator(
            task_id="search_users_237",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('log_loginnamewithaf_236')
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, rail.result('log_loginnamewithaf_236'))
        )

        if_search_users_237_users_less_than_1_238 = rail.IfOperator(
            task_id='if_search_users_237_users_less_than_1_238',
            test="{{result('search_users_237') | length == 0}}",
            yes_task="trigger_dag_run_live_nrdc_basic_add_update239",
            no_task="if_search_users_237_users_greater_than_0_240",
        )

        trigger_dag_run_live_nrdc_basic_add_update239 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update239',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_loginnamewithaf_236'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "C4",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": rail.result('log_useruri_primaryprofile_202'),
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "C4 Timesheet",
                "authtype": "replicon",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update239 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update239',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update239") }}'
        )

        gather_user_uri_239 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_uri_239',
            dag_runs="{{ result('trigger_dag_run_live_nrdc_basic_add_update239')}}",
            dagrun_task_id='log_useruri_71',
            flatten=True
        )

        if_search_users_237_users_greater_than_0_240 = rail.IfOperator(
            task_id='if_search_users_237_users_greater_than_0_240',
            test="{{result('search_users_237') | length > 0}}",
            yes_task="log_useruribasedonthesuffix_241",
            no_task="update_variable_250",
        )

        log_useruribasedonthesuffix_241 = rail.PythonOperator(
            task_id='log_useruribasedonthesuffix_241',
            # pylint: disable=line-too-long
            python_callable=lambda: get_user_uri_125(
                'log_loginnamewithaf_236', 'search_users_237')
        )

        if_log_useruribasedonthesuffix_241_present_242 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_241_present_242',
            test='''{{ result('log_useruribasedonthesuffix_241') | is_truthy }}''',
            yes_task="log_userstatusbasedonthesuffix_243",
            no_task="if_log_useruribasedonthesuffix_241_blank_248",
        )

        log_userstatusbasedonthesuffix_243 = rail.PythonOperator(
            task_id='log_userstatusbasedonthesuffix_243',
            python_callable=lambda: get_user_status(
                'log_loginnamewithaf_236', 'search_users_237')
        )

        if_log_userstatusbasedonthesuffix_243_equals_to_false_244 = rail.IfOperator(
            task_id='if_log_userstatusbasedonthesuffix_243_equals_to_false_244',
            test='''{{ result('log_userstatusbasedonthesuffix_243') == 'False' }}''',
            yes_task="re_enable_userprofile_245",
            no_task="if_log_useruribasedonthesuffix_241_blank_248",
        )

        re_enable_userprofile_245 = rail.RepliconServiceOperator(
            task_id='re_enable_userprofile_245',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_241') }}"
            }
        )

        removeenddate_246 = rail.RepliconServiceOperator(
            task_id='removeenddate_246',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('log_useruribasedonthesuffix_241') }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": null
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile247 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile247',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": dag_run.conf['logonname'],
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('log_useruribasedonthesuffix_241'),
                "locationuri": dag_run.conf['locationuri'],
                "type": "C4",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile247 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile247',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile247") }}'
        )

        if_log_useruribasedonthesuffix_241_blank_248 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_241_blank_248',
            test='''{{ result('log_useruribasedonthesuffix_241') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update249",
            no_task="update_variable_250",
        )

        trigger_dag_run_live_nrdc_basic_add_update249 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update249',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_loginnamewithaf_236'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "C4",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": rail.result('get_first_primary_records_233')['uri'],
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "C4 Timesheet",
                "authtype": "replicon",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update249 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update249',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update249") }}'
        )

        gather_user_uri_249 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_uri_249',
            dag_runs="{{ result('trigger_dag_run_live_nrdc_basic_add_update249')}}",
            dagrun_task_id='log_useruri_71',
            flatten=True
        )

        update_variable_250 = rail.SetVariableOperator(
            task_id='update_variable_250',
            append=False,
            name='{{ result("declare_variable_234").name }}',
            value="{{ result('get_first_primary_records_233').uri }}"
        )

        def get_newly_created_from_basic_add_task(task1, task2, task3):
            user_uri_1 = rail.result(task1)[
                0] if rail.result(task1) else None
            user_uri_2 = rail.result(task2)
            user_uri_3 = rail.result(task3)[
                0] if rail.result(task3) else None
            return user_uri_1 or user_uri_2 or user_uri_3

        log_a_fchilduseruri_251 = rail.PythonOperator(
            task_id='log_a_fchilduseruri_251',
            python_callable=lambda:  get_newly_created_from_basic_add_task(
                'gather_user_uri_239', 'log_useruribasedonthesuffix_241', 'gather_user_uri_249')
        )

        get_all_substitute_user_assignments_for_user_252 = rail.RepliconServiceOperator(
            task_id='get_all_substitute_user_assignments_for_user_252',
            endpoint="/services/SubstituteUserAssignmentService1.svc/GetAllSubstituteUserAssignmentsForUser",
            data={
                "userUri": "{{ result('log_a_fchilduseruri_251') }}"
            }
        )

        log_substituteuserassigned_253 = rail.PythonOperator(
            task_id='log_substituteuserassigned_253',
            python_callable=lambda: get_substitueUserUris_first(
                'get_first_primary_records_233', 'get_all_substitute_user_assignments_for_user_252')
        )

        if_log_substituteuserassigned_253_blank_254 = rail.IfOperator(
            task_id='if_log_substituteuserassigned_253_blank_254',
            test='''{{ result('log_substituteuserassigned_253') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_assign_substitute_usersv2257",
            no_task="if_first_type_equals_to_c4_c4to_delegate_258",
        )

        trigger_dag_run_live_nrdc_assign_substitute_usersv2257 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_assign_substitute_usersv2257',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_assignsubstituteusersv2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "suburi": rail.get_dag_run_var(rail.result('declare_variable_234')['name']),
                "actualuri": rail.result('log_a_fchilduseruri_251'),
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2257 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2257',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_assign_substitute_usersv2257") }}'
        )

        if_first_type_equals_to_c4_c4to_delegate_258 = rail.IfOperator(
            task_id='if_first_type_equals_to_c4_c4to_delegate_258',
            test='''{{ result('get_first_primary_records_233').type == 'C4' }}''',
            yes_task="updateuserloginname_set_replicon_authentication_for_user_259",
            no_task="query_list_whereexistingprimaryprofilevalueis_c4_delegate_283",
        )

        updateuserloginname_set_replicon_authentication_for_user_259 = rail.RepliconServiceOperator(
            task_id='updateuserloginname_set_replicon_authentication_for_user_259',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ result('get_first_primary_records_233').uri }}",
                "loginName": "{{ result('get_first_primary_records_233').userloginname }}af",
                "password": "Replicon@12",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        update_email_removingemail_260 = rail.RepliconServiceOperator(
            task_id='update_email_removingemail_260',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('get_first_primary_records_233').uri }}",
                "email": null
            }
        )

        log_loginnamewithd_261 = rail.PythonOperator(
            task_id='log_loginnamewithd_261',
            python_callable=lambda:  rail.result('get_first_primary_records_233')[
                'userloginname'] + "d"
        )

        search_users_262 = rail.RepliconServicePageOperator(
            task_id="search_users_262",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('log_loginnamewithd_261')
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, rail.result('log_loginnamewithd_261'))
        )

        if_search_users_262_users_less_than_1_263 = rail.IfOperator(
            task_id='if_search_users_262_users_less_than_1_263',
            test="{{result('search_users_262') | length == 0}}",
            yes_task="trigger_dag_run_live_nrdc_basic_add_update264",
            no_task="if_search_users_262_users_greater_than_0_265",
        )

        trigger_dag_run_live_nrdc_basic_add_update264 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update264',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Delegate",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('get_first_primary_records_233')['userloginname'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "Delegate",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "NA",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "NA",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update264 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update264',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update264") }}'
        )

        if_search_users_262_users_greater_than_0_265 = rail.IfOperator(
            task_id='if_search_users_262_users_greater_than_0_265',
            test="{{result('search_users_262') | length > 0}}",
            yes_task="log_useruribasedonthesuffix_266",
            no_task="update_variable_276",
        )

        log_useruribasedonthesuffix_266 = rail.PythonOperator(
            task_id='log_useruribasedonthesuffix_266',
            # pylint: disable=line-too-long
            python_callable=lambda: get_user_uri_125(
                'log_loginnamewithd_261', 'search_users_262')
        )

        if_log_useruribasedonthesuffix_266_present_267 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_266_present_267',
            test='''{{ result('log_useruribasedonthesuffix_266') | is_truthy }}''',
            yes_task="log_userstatusbasedonthesuffix_268",
            no_task="if_log_useruribasedonthesuffix_266_blank_274",
        )

        log_userstatusbasedonthesuffix_268 = rail.PythonOperator(
            task_id='log_userstatusbasedonthesuffix_268',
            # pylint: disable=line-too-long
            python_callable=lambda: get_user_status(
                'log_loginnamewithd_261', 'search_users_262')
        )

        if_log_userstatusbasedonthesuffix_268_equals_to_false_269 = rail.IfOperator(
            task_id='if_log_userstatusbasedonthesuffix_268_equals_to_false_269',
            test='''{{ result('log_userstatusbasedonthesuffix_268') == 'False' }}''',
            yes_task="re_enable_userprofile_270",
            no_task="if_log_useruribasedonthesuffix_266_blank_274",
        )

        re_enable_userprofile_270 = rail.RepliconServiceOperator(
            task_id='re_enable_userprofile_270',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_266') }}"
            }
        )

        removeenddate_271 = rail.RepliconServiceOperator(
            task_id='removeenddate_271',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('log_useruribasedonthesuffix_266') }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": null
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        updatetoprimaryprofile_set_s_s_o_authentication_for_user_272 = rail.RepliconServiceOperator(
            task_id='updatetoprimaryprofile_set_s_s_o_authentication_for_user_272',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_266') }}",
                "loginName": "{{ result('get_first_primary_records_233').userloginname }}"
            }
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile273 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile273',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Delegate",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": dag_run.conf['logonname'],
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('log_useruribasedonthesuffix_266'),
                "locationuri": dag_run.conf['locationuri'],
                "type": "Delegate",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile273 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile273',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile273") }}'
        )

        if_log_useruribasedonthesuffix_266_blank_274 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_266_blank_274',
            test='''{{ result('log_useruribasedonthesuffix_266') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update275",
            no_task="update_variable_276",
        )

        trigger_dag_run_live_nrdc_basic_add_update275 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update275',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Delegate",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('get_first_primary_records_233')['userloginname'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "Delegate",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "NA",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "NA",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update275 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update275',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update275") }}'
        )

        update_variable_276 = rail.SetVariableOperator(
            task_id='update_variable_276',
            append=False,
            name='{{ result("declare_variable_234").name }}',
            # pylint: disable=line-too-long
            value="data.workato_service.trigger_dag_run_live_nrdc_basic_add_update264.reply.useruri.present? ? _('data.workato_service.trigger_dag_run_live_nrdc_basic_add_update264.reply.useruri : (result('log_useruribasedonthesuffix_266')').present? ? result('log_useruribasedonthesuffix_266')') : _('data.workato_service.trigger_dag_run_live_nrdc_basic_add_update275.reply.useruri)"
        )

        get_all_substitute_user_assignments_for_user_277 = rail.RepliconServiceOperator(
            task_id='get_all_substitute_user_assignments_for_user_277',
            endpoint="/services/SubstituteUserAssignmentService1.svc/GetAllSubstituteUserAssignmentsForUser",
            data={
                "userUri": "{{ result('get_first_primary_records_233').uri }}"
            }
        )

        log_substituteuserassigned_278 = rail.PythonOperator(
            task_id='log_substituteuserassigned_278',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_substitueUserUris_first(
                'get_first_primary_records_233', 'get_all_substitute_user_assignments_for_user_277')
        )

        if_log_substituteuserassigned_278_blank_279 = rail.IfOperator(
            task_id='if_log_substituteuserassigned_278_blank_279',
            test='''{{ result('log_substituteuserassigned_278') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_assign_substitute_usersv2282",
            no_task="if_first_type_present_c4_c4and_c3_283",
        )

        trigger_dag_run_live_nrdc_assign_substitute_usersv2282 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_assign_substitute_usersv2282',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_assignsubstituteusersv2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "suburi": rail.get_dag_run_var(rail.result('declare_variable_234')['name']),
                "actualuri": rail.result('get_first_primary_records_233')['uri'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2282 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2282',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_assign_substitute_usersv2282") }}'
        )

        query_list_whereexistingprimaryprofilevalueis_c4_delegate_283 = rail.QueryCollectionOperator(
            task_id='query_list_whereexistingprimaryprofilevalueis_c4_delegate_283',
            query="SELECT * FROM  existinguserdata WHERE  existinguserdata.type = 'C4' OR  existinguserdata.type='Delegate'",
        )

        get_first_primary_records_283 = rail.PythonOperator(
            task_id='get_first_primary_records_283',
            python_callable=lambda:  get_primary_user_info(
                'query_list_whereexistingprimaryprofilevalueis_c4_delegate_283')
        )

        if_first_type_present_c4_c4and_c3_283 = rail.IfOperator(
            task_id='if_first_type_present_c4_c4and_c3_283',
            test="{{ result('get_first_primary_records_233').type | is_truthy }}",
            yes_task="log_loginnameprimaryprofile_284",
            no_task="if_request_currentprofilecount_equals_to_1_c4to_c3or_delegateto_c35profiles_313",
        )

        log_loginnameprimaryprofile_284 = rail.PythonOperator(
            task_id='log_loginnameprimaryprofile_284',
            python_callable=lambda:  rail.result(
                'get_first_primary_records_233')['userloginname']
        )

        log_useruri_primaryprofile_285 = rail.PythonOperator(
            task_id='log_useruri_primaryprofile_285',
            python_callable=lambda:  rail.get_dag_run_var(
                rail.result('declare_variable_150')['name'])
        )

        getuserdata_286 = rail.RepliconServicePageOperator(
            task_id="getuserdata_286",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('log_loginnameprimaryprofile_284')
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, rail.result('log_loginnameprimaryprofile_284'))
        )

        create_list_size5_290 = rail.EmptyOperator(
            task_id='create_list_size5_290',
        )

        declare_list_291 = rail.SetVariableOperator(
            task_id='declare_list_291',
            append=False,
            name='c3c4list',
            value=[]
        )

        get_c3_c4_list_297 = rail.PythonOperator(
            task_id='get_c3_c4_list_297',
            python_callable=lambda:  create_c3_c4_list(
                'log_loginnameprimaryprofile_284')
        )

        declare_list_update_dag_runs_297 = rail.SetVariableOperator(
            task_id='declare_list_update_dag_runs_297',
            name='user_process_update_dag_runs_297',
            value=[]
        )

        foreach_declare_list_291_297 = rail.ForEachOperator(
            task_id='foreach_declare_list_291_297',
            items="{{ result('get_c3_c4_list_297') | to_json }}",
            start_task='log_uriuser_298',
            end_task='foreach_declare_list_291_297_end'
        )

        log_uriuser_298 = rail.PythonOperator(
            task_id='log_uriuser_298',
            python_callable=lambda:  get_user_uri_215(
                'foreach_declare_list_291_297', 'getuserdata_286')
        )

        if_log_uriuser_298_present_299 = rail.IfOperator(
            task_id='if_log_uriuser_298_present_299',
            test='''{{ result('log_uriuser_298') | is_truthy }}''',
            yes_task="log_status_300",
            no_task="if_log_uriuser_298_blank_311",
        )

        log_status_300 = rail.PythonOperator(
            task_id='log_status_300',
            python_callable=lambda: get_user_status_215(
                'foreach_declare_list_291_297', 'getuserdata_286')
        )

        if_log_status_300_not_equals_to_true_301 = rail.IfOperator(
            task_id='if_log_status_300_not_equals_to_true_301',
            test='''{{ result('log_status_300') != 'True' }}''',
            yes_task="re_enable_userprofile_302",
            no_task="if_log_uriuser_298_blank_311",
        )

        re_enable_userprofile_302 = rail.RepliconServiceOperator(
            task_id='re_enable_userprofile_302',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('log_uriuser_298') }}"
            }
        )

        remove_user_end_date_303 = rail.RepliconServiceOperator(
            task_id='remove_user_end_date_303',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('log_uriuser_298') }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": null,
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile304 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile304',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": rail.result('foreach_declare_list_291_297')['type'],
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": dag_run.conf['logonname'],
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('log_uriuser_298'),
                "locationuri": dag_run.conf['locationuri'],
                "type": rail.result('foreach_declare_list_291_297')['type'],
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        insert_to_user_rehire_disable_user_list_304 = rail.SetVariableOperator(
            task_id='insert_to_user_rehire_disable_user_list_304',
            append=True,
            name='{{ result("declare_list_update_dag_runs_297").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile304"))[0]}}'
        )

        get_all_substitute_user_assignments_for_user_305 = rail.RepliconServiceOperator(
            task_id='get_all_substitute_user_assignments_for_user_305',
            endpoint="/services/SubstituteUserAssignmentService1.svc/GetAllSubstituteUserAssignmentsForUser",
            data={
                "userUri": "{{ result('log_uriuser_298') }}"
            }
        )

        log_substituteuserassigned_306 = rail.PythonOperator(
            task_id='log_substituteuserassigned_306',
            # pylint: disable=line-too-long
            python_callable=lambda: get_substitueUserUris_first(
                'get_first_primary_records_233', 'get_all_substitute_user_assignments_for_user_305')
        )

        if_log_substituteuserassigned_306_blank_307 = rail.IfOperator(
            task_id='if_log_substituteuserassigned_306_blank_307',
            test='''{{ result('log_substituteuserassigned_306') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_assign_substitute_usersv2310",
            no_task="if_log_uriuser_298_blank_311",
        )

        trigger_dag_run_live_nrdc_assign_substitute_usersv2310 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_assign_substitute_usersv2310',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_assignsubstituteusersv2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "suburi": rail.result('log_useruri_primaryprofile_285'),
                "actualuri": rail.result('log_uriuser_298'),
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        insert_to_user_rehire_disable_user_list_310 = rail.SetVariableOperator(
            task_id='insert_to_user_rehire_disable_user_list_310',
            append=True,
            name='{{ result("declare_list_update_dag_runs_297").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_nrdc_assign_substitute_usersv2310"))[0]}}'
        )

        if_log_uriuser_298_blank_311 = rail.IfOperator(
            task_id='if_log_uriuser_298_blank_311',
            test='''{{ result('log_uriuser_298') | is_truthy }}''',
            yes_task="foreach_declare_list_291_297_end",
            no_task="trigger_dag_run_live_nrdc_basic_add_update312",
        )

        trigger_dag_run_live_nrdc_basic_add_update312 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update312',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": rail.result('foreach_declare_list_291_297')['type'],
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('foreach_declare_list_291_297')['loginname'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": rail.result('foreach_declare_list_291_297')['type'],
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": rail.result('log_useruri_primaryprofile_285'),
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": rail.result('foreach_declare_list_291_297')['timesheetpolicy'],
                "authtype": "replicon",
                "status": dag_run.conf['accountstatus']
            }
        )

        insert_to_user_rehire_disable_user_list_312 = rail.SetVariableOperator(
            task_id='insert_to_user_rehire_disable_user_list_312',
            append=True,
            name='{{ result("declare_list_update_dag_runs_297").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_nrdc_basic_add_update312"))[0]}}'
        )

        foreach_declare_list_291_297_end = rail.EmptyOperator(
            task_id='foreach_declare_list_291_297_end',
        )

        get_dag_run_ids_310 = rail.PythonOperator(
            task_id='get_dag_run_ids_310',
            python_callable=lambda: rail.get_dag_run_var(
                rail.result('declare_list_update_dag_runs_297')['name'])
        )

        wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2310 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2310',
            execution_timeout=timedelta(days=14),
            dag_runs="{{ result('get_dag_run_ids_310') | to_json}}"
        )

        if_request_currentprofilecount_equals_to_1_c4to_c3or_delegateto_c35profiles_313 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_1_c4to_c3or_delegateto_c35profiles_313',
            # pylint: disable=line-too-long
            test='''{{ dag_run.conf.currentprofilecount == 1  and dag_run.conf.memberof | matches('C3')  and dag_run.conf.memberof | matches('C4') | is_falsy  and dag_run.conf.memberof | matches('Delegate') | is_falsy }}''',
            yes_task="update_variable_314",
            no_task="if_request_currentprofilecount_equals_to_2_c4delegateto_c4_c3and_delegate2to7profiles_383",
        )

        update_variable_314 = rail.SetVariableOperator(
            task_id='update_variable_314',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=5
        )

        if_declare_variable_41_value_equals_to_5_315 = rail.IfOperator(
            task_id='if_declare_variable_41_value_equals_to_5_315',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_41')['name']) == 5,
            yes_task="log_primaryloginname_316",
            no_task="if_request_currentprofilecount_equals_to_2_c4delegateto_c4_c3and_delegate2to7profiles_383",
        )

        log_primaryloginname_316 = rail.PythonOperator(
            task_id='log_primaryloginname_316',
            python_callable=lambda:  rail.result('create_list_14')[
                0]['userloginname']
        )

        log_primary_uri_317 = rail.PythonOperator(
            task_id='log_primary_uri_317',
            python_callable=lambda:  rail.result('create_list_14')[0]['uri']
        )

        log_primary_user_type_318 = rail.PythonOperator(
            task_id='log_primary_user_type_318',
            python_callable=lambda:  rail.result('create_list_14')[0]['type']
        )

        search_users_319 = rail.RepliconServicePageOperator(
            task_id="search_users_319",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('log_primaryloginname_316')
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, rail.result('log_primaryloginname_316'))
        )

        declare_list_320 = rail.SetVariableOperator(
            task_id='declare_list_320',
            append=False,
            name='availableuserprofiles',
            value=[]
        )

        declare_variable_321 = rail.SetVariableOperator(
            task_id='declare_variable_321',
            append=False,
            name='requiredextention',
            value="af"
        )

        declare_list_update_dag_runs_322 = rail.SetVariableOperator(
            task_id='declare_list_update_dag_runs_322',
            name='user_process_update_dag_runs_322',
            value=[]
        )

        foreach_search_users_319_322 = rail.ForEachOperator(
            task_id='foreach_search_users_319_322',
            items="{{result('search_users_319') | to_json}}",
            start_task='if_login_name_textvalue_equals_to_datalogger38faa588message_323',
            end_task='foreach_search_users_319_322_end'
        )

        if_login_name_textvalue_equals_to_datalogger38faa588message_323 = rail.IfOperator(
            task_id='if_login_name_textvalue_equals_to_datalogger38faa588message_323',
            test='''{{ result('foreach_search_users_319_322').loginname == result('log_primaryloginname_316') }}''',
            yes_task="if_log_primary_user_type_318_equals_to_delegate_324",
            no_task="if_login_name_textvalue_equals_to_datalogger38faa588messagefl_329",
        )

        if_log_primary_user_type_318_equals_to_delegate_324 = rail.IfOperator(
            task_id='if_log_primary_user_type_318_equals_to_delegate_324',
            test='''{{ result('log_primary_user_type_318') == 'Delegate' }}''',
            yes_task="update_variable_325",
            no_task="update_loginname_326",
        )

        update_variable_325 = rail.SetVariableOperator(
            task_id='update_variable_325',
            append=False,
            name='{{ result("declare_variable_321").name }}',
            value="d"
        )

        update_loginname_326 = rail.RepliconServiceOperator(
            task_id='update_loginname_326',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data=lambda: {
                "userUri": rail.result('log_primary_uri_317'),
                "loginName": rail.result('log_primaryloginname_316') + "" + rail.get_dag_run_var(rail.result('declare_variable_321')['name']),
                "password": "Replicon@12",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        add_end_dateandemail_327 = rail.RepliconServiceOperator(
            task_id='add_end_dateandemail_327',
            endpoint="/services/importservice1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('log_primary_uri_317') }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "departmentGroupScheduleToApply": null,
                    "employeeTypeGroupScheduleToApply": null,
                    "timesheetPeriodScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": {
                            "emailAddress": null
                        },
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": {
                                "year": "{{result('log_todays_year_19')}}",
                                "month": "{{result('log_todays_month_20')}}",
                                "day": "{{result('log_todays_day_21')}}"
                            }
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRulesScheduleModifications": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null,
                    "resourceAllocationAfterUserEndDateOptionUri": null
                }
            }
        )

        disable_userprofile_328 = rail.RepliconServiceOperator(
            task_id='disable_userprofile_328',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ result('log_primary_uri_317') }}"
            }
        )

        def is_login_matching_329():
            return bool(rail.result('foreach_search_users_319_322')['loginname'] == rail.result('log_primaryloginname_316') + "fl")

        if_login_name_textvalue_equals_to_datalogger38faa588messagefl_329 = rail.IfOperator(
            task_id='if_login_name_textvalue_equals_to_datalogger38faa588messagefl_329',
            test=is_login_matching_329,
            yes_task="insert_to_list_330",
            no_task="if_login_name_textvalue_equals_to_datalogger38faa588messagell_335",
        )

        insert_to_list_330 = rail.SetVariableOperator(
            task_id='insert_to_list_330',
            append=True,
            name='{{ result("declare_list_320").name }}',
            value={
                "userloginname": "{{ result('foreach_search_users_319_322').loginname }}",
                "useruri": "{{ result('foreach_search_users_319_322').useruri }}",
                "status": "{{ result('foreach_search_users_319_322').status }}",
                "type": "Federal Legislative"
            }
        )

        update_loginnameandmakeprimaryprofile_331 = rail.RepliconServiceOperator(
            task_id='update_loginnameandmakeprimaryprofile_331',
            endpoint="/services/securityservice1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('log_primary_uri_317') }}",
                "loginName": "{{ result('log_primaryloginname_316') }}"
            }
        )

        if_enabled_boolvalue_is_not_true_332 = rail.IfOperator(
            task_id='if_enabled_boolvalue_is_not_true_332',
            test='''{{ result('foreach_search_users_319_322').status == 'False' }}''',
            yes_task="remove_end_date_333",
            no_task="trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile334",
        )

        remove_end_date_333 = rail.RepliconServiceOperator(
            task_id='remove_end_date_333',
            endpoint="/services/importservice1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('foreach_search_users_319_322').useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "departmentGroupScheduleToApply": null,
                    "employeeTypeGroupScheduleToApply": null,
                    "timesheetPeriodScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": {
                                "year": null,
                                "month": null,
                                "day": null
                            }
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRulesScheduleModifications": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null,
                    "resourceAllocationAfterUserEndDateOptionUri": null
                }
            }
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile334 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile334',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Federal Legislative",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": dag_run.conf['logonname'],
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('foreach_search_users_319_322')['useruri'],
                "locationuri": dag_run.conf['locationuri'],
                "type": "Federal Legislative",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        insert_to_user_dag_run_list_334 = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list_334',
            append=True,
            name='{{ result("declare_list_update_dag_runs_322").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile334"))[0]}}'
        )

        def is_login_matching_335(user_suffix):
            return bool(rail.result('foreach_search_users_319_322')['loginname'] == rail.result('log_primaryloginname_316') + user_suffix)

        if_login_name_textvalue_equals_to_datalogger38faa588messagell_335 = rail.IfOperator(
            task_id='if_login_name_textvalue_equals_to_datalogger38faa588messagell_335',
            test=lambda: is_login_matching_335('ll'),
            yes_task="insert_to_list_336",
            no_task="if_login_name_textvalue_equals_to_datalogger38faa588messagela_340",
        )

        insert_to_list_336 = rail.SetVariableOperator(
            task_id='insert_to_list_336',
            append=True,
            name='{{ result("declare_list_320").name }}',
            value={
                "userloginname": "{{ result('foreach_search_users_319_322').loginname }}",
                "useruri": "{{ result('foreach_search_users_319_322').useruri }}",
                "status": "{{ result('foreach_search_users_319_322').status }}",
                "type": "Local legislative"
            }
        )

        if_enabled_boolvalue_is_not_true_337 = rail.IfOperator(
            task_id='if_enabled_boolvalue_is_not_true_337',
            test='''{{ result('foreach_search_users_319_322').status  | is_falsy }}''',
            yes_task="remove_end_date_338",
            no_task="trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile339",
        )

        remove_end_date_338 = rail.RepliconServiceOperator(
            task_id='remove_end_date_338',
            endpoint="/services/importservice1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('foreach_search_users_319_322').loginname }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "departmentGroupScheduleToApply": null,
                    "employeeTypeGroupScheduleToApply": null,
                    "timesheetPeriodScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": {
                                "year": null,
                                "month": null,
                                "day": null
                            }
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRulesScheduleModifications": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null,
                    "resourceAllocationAfterUserEndDateOptionUri": null
                }
            }
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile339 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile339',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Local legislative",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": dag_run.conf['logonname'],
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri":  rail.result('foreach_search_users_319_322')['useruri'],
                "locationuri": dag_run.conf['locationuri'],
                "type": "Local legislative",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        insert_to_user_dag_run_list_339 = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list_339',
            append=True,
            name='{{ result("declare_list_update_dag_runs_322").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile339"))[0]}}'
        )

        if_login_name_textvalue_equals_to_datalogger38faa588messagela_340 = rail.IfOperator(
            task_id='if_login_name_textvalue_equals_to_datalogger38faa588messagela_340',
            test=lambda: is_login_matching_335('la'),
            yes_task="insert_to_list_341",
            no_task="if_login_name_textvalue_equals_to_datalogger38faa588messagesl_345",
        )

        insert_to_list_341 = rail.SetVariableOperator(
            task_id='insert_to_list_341',
            append=True,
            name='{{ result("declare_list_320").name }}',
            value={
                "userloginname": "{{ result('foreach_search_users_319_322').loginname }}",
                "useruri": "{{ result('foreach_search_users_319_322').useruri }}",
                "status": "{{ result('foreach_search_users_319_322').status }}",
                "type": "Local administrative"
            }
        )

        if_enabled_boolvalue_is_not_true_342 = rail.IfOperator(
            task_id='if_enabled_boolvalue_is_not_true_342',
            test='''{{ result('foreach_search_users_319_322').status | is_falsy }}''',
            yes_task="remove_end_date_343",
            no_task="trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile344",
        )

        remove_end_date_343 = rail.RepliconServiceOperator(
            task_id='remove_end_date_343',
            endpoint="/services/importservice1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('foreach_search_users_319_322').useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "departmentGroupScheduleToApply": null,
                    "employeeTypeGroupScheduleToApply": null,
                    "timesheetPeriodScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": {
                                "year": null,
                                "month": null,
                                "day": null
                            }
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRulesScheduleModifications": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null,
                    "resourceAllocationAfterUserEndDateOptionUri": null
                }
            }
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile344 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile344',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Local administrative",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": dag_run.conf['logonname'],
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('foreach_search_users_319_322')['useruri'],
                "locationuri": dag_run.conf['locationuri'],
                "type": "Local administrative",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        insert_to_user_dag_run_list_345 = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list_345',
            append=True,
            name='{{ result("declare_list_update_dag_runs_322").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile344"))[0]}}'
        )

        if_login_name_textvalue_equals_to_datalogger38faa588messagesl_345 = rail.IfOperator(
            task_id='if_login_name_textvalue_equals_to_datalogger38faa588messagesl_345',
            test=lambda: is_login_matching_335('sl'),
            yes_task="insert_to_list_346",
            no_task="if_login_name_textvalue_equals_to_datalogger38faa588messagesa_350",
        )

        insert_to_list_346 = rail.SetVariableOperator(
            task_id='insert_to_list_346',
            append=True,
            name='{{ result("declare_list_320").name }}',
            value={
                "userloginname": "{{ result('foreach_search_users_319_322').loginname }}",
                "useruri": "{{ result('foreach_search_users_319_322').useruri }}",
                "status": "{{ result('foreach_search_users_319_322').status }}",
                "type": "State legislative"
            }
        )

        if_enabled_boolvalue_is_not_true_347 = rail.IfOperator(
            task_id='if_enabled_boolvalue_is_not_true_347',
            test='''{{ result('foreach_search_users_319_322').status | is_falsy }}''',
            yes_task="remove_end_date_348",
            no_task="trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile349",
        )

        remove_end_date_348 = rail.RepliconServiceOperator(
            task_id='remove_end_date_348',
            endpoint="/services/importservice1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('foreach_search_users_319_322').useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "departmentGroupScheduleToApply": null,
                    "employeeTypeGroupScheduleToApply": null,
                    "timesheetPeriodScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": {
                                "year": null,
                                "month": null,
                                "day": null
                            }
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRulesScheduleModifications": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null,
                    "resourceAllocationAfterUserEndDateOptionUri": null
                }
            }
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile349 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile349',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "State legislative",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": dag_run.conf['logonname'],
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('foreach_search_users_319_322')['useruri'],
                "locationuri": dag_run.conf['locationuri'],
                "type": "State legislative",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        insert_to_user_dag_run_list_349 = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list_349',
            append=True,
            name='{{ result("declare_list_update_dag_runs_322").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile349"))[0]}}'
        )

        if_login_name_textvalue_equals_to_datalogger38faa588messagesa_350 = rail.IfOperator(
            task_id='if_login_name_textvalue_equals_to_datalogger38faa588messagesa_350',
            test=lambda: is_login_matching_335('sa'),
            yes_task="insert_to_list_351",
            no_task="foreach_search_users_319_322_end",
        )

        insert_to_list_351 = rail.SetVariableOperator(
            task_id='insert_to_list_351',
            append=True,
            name='{{ result("declare_list_320").name }}',
            value={
                "userloginname": "{{ result('foreach_search_users_319_322').loginname }}",
                "useruri": "{{ result('foreach_search_users_319_322').useruri }}",
                "status": "{{ result('foreach_search_users_319_322').status }}",
                "type": "State administrative"
            }
        )

        if_enabled_boolvalue_is_not_true_352 = rail.IfOperator(
            task_id='if_enabled_boolvalue_is_not_true_352',
            test='''{{ result('foreach_search_users_319_322').status | is_falsy }}''',
            yes_task="remove_end_date_353",
            no_task="trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile354",
        )

        remove_end_date_353 = rail.RepliconServiceOperator(
            task_id='remove_end_date_353',
            endpoint="/services/importservice1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('foreach_search_users_319_322').useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "departmentGroupScheduleToApply": null,
                    "employeeTypeGroupScheduleToApply": null,
                    "timesheetPeriodScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": {
                                "year": null,
                                "month": null,
                                "day": null
                            }
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRulesScheduleModifications": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null,
                    "resourceAllocationAfterUserEndDateOptionUri": null
                }
            }
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile354 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile354',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "State administrative",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": dag_run.conf['logonname'],
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('foreach_search_users_319_322')['useruri'],
                "locationuri": dag_run.conf['locationuri'],
                "type": "State administrative",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        insert_to_user_dag_run_list_354 = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list_354',
            append=True,
            name='{{ result("declare_list_update_dag_runs_322").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile354"))[0]}}'
        )

        foreach_search_users_319_322_end = rail.EmptyOperator(
            task_id='foreach_search_users_319_322_end',
        )

        get_dag_run_ids_354 = rail.PythonOperator(
            task_id='get_dag_run_ids_354',
            python_callable=lambda: rail.get_dag_run_var(
                rail.result('declare_list_update_dag_runs_322')['name'])
        )

        wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile354 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile354',
            execution_timeout=timedelta(days=14),
            dag_runs="{{ result('get_dag_run_ids_354') | to_json}}"
        )

        def get_federal_legislative_355(type_name, users_list_task):
            available_user_profiles = rail.get_dag_run_var(
                rail.result(users_list_task)['name'])
            user_info = list(filter(
                lambda item: item['type'] == type_name, available_user_profiles))
            return user_info[0]['type'] if user_info else None

        log_checkif_federal_legislativeisavailable_355 = rail.PythonOperator(
            task_id='log_checkif_federal_legislativeisavailable_355',
            python_callable=lambda: get_federal_legislative_355(
                'Federal Legislative', 'declare_list_320')
        )

        if_log_checkif_federal_legislativeisavailable_355_blank_356 = rail.IfOperator(
            task_id='if_log_checkif_federal_legislativeisavailable_355_blank_356',
            test='''{{ result('log_checkif_federal_legislativeisavailable_355') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update357",
            no_task="log_checkif_statelegislativeisavailable_359",
        )

        trigger_dag_run_live_nrdc_basic_add_update357 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update357',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Federal Legislative",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_primaryloginname_316'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "Federal Legislative",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "na",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "C3 - Federal Legislative",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update357 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update357',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update357") }}'
        )

        gather_user_uri_358 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_uri_358',
            dag_runs="{{ result('trigger_dag_run_live_nrdc_basic_add_update357')}}",
            dagrun_task_id='log_useruri_71',
            flatten=True
        )

        insert_to_list_358 = rail.SetVariableOperator(
            task_id='insert_to_list_358',
            append=True,
            name='{{ result("declare_list_320").name }}',
            value={
                "userloginname": "{{ result('log_primaryloginname_316') }}",
                "useruri": "{{ result('gather_user_uri_358')[0] }}",
                "status": "{{ dag_run.conf.accountstatus }}",
                "type": "Federal Legislative"
            }
        )

        log_checkif_statelegislativeisavailable_359 = rail.PythonOperator(
            task_id='log_checkif_statelegislativeisavailable_359',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_federal_legislative_355(
                'State legislative', 'declare_list_320')
        )

        if_log_checkif_statelegislativeisavailable_359_blank_360 = rail.IfOperator(
            task_id='if_log_checkif_statelegislativeisavailable_359_blank_360',
            test='''{{ result('log_checkif_statelegislativeisavailable_359') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update361",
            no_task="log_checkif_locallegislativeisavailable_363",
        )

        trigger_dag_run_live_nrdc_basic_add_update361 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update361',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "State legislative",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_primaryloginname_316') + "sl",
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "State legislative",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "na",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "C3 - State Legislative",
                "authtype": "replicon",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update361 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update361',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update361") }}'
        )

        gather_user_uri_362 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_uri_362',
            dag_runs="{{ result('trigger_dag_run_live_nrdc_basic_add_update361')}}",
            dagrun_task_id='log_useruri_71',
            flatten=True
        )

        insert_to_list_362 = rail.SetVariableOperator(
            task_id='insert_to_list_362',
            append=True,
            name='{{ result("declare_list_320").name }}',
            value={
                "userloginname": "{{ result('log_primaryloginname_316') }}",
                "useruri": "{{ result('gather_user_uri_362')[0] }}",
                "status": "{{ dag_run.conf.accountstatus }}",
                "type": "State legislative"
            }
        )

        log_checkif_locallegislativeisavailable_363 = rail.PythonOperator(
            task_id='log_checkif_locallegislativeisavailable_363',
            # pylint: disable=line-too-long
            python_callable=lambda: get_federal_legislative_355(
                'Local legislative', 'declare_list_320')
        )

        if_log_checkif_locallegislativeisavailable_363_blank_364 = rail.IfOperator(
            task_id='if_log_checkif_locallegislativeisavailable_363_blank_364',
            test='''{{ result('log_checkif_locallegislativeisavailable_363') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update365",
            no_task="log_checkif_localadministrativeisavailable_367",
        )

        trigger_dag_run_live_nrdc_basic_add_update365 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update365',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Local legislative",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_primaryloginname_316') + "ll",
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "Local legislative",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "na",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "C3 - Local Legislative",
                "authtype": "replicon",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update365 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update365',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update365") }}'
        )

        gather_user_uri_366 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_uri_366',
            dag_runs="{{ result('trigger_dag_run_live_nrdc_basic_add_update365')}}",
            dagrun_task_id='log_useruri_71',
            flatten=True
        )

        insert_to_list_366 = rail.SetVariableOperator(
            task_id='insert_to_list_366',
            append=True,
            name='{{ result("declare_list_320").name }}',
            value={
                "userloginname": "{{ result('log_primaryloginname_316') }}",
                "useruri": "{{ result('gather_user_uri_366')[0] }}",
                "status": "{{ dag_run.conf.accountstatus }}",
                "type": "Local legislative"
            }
        )

        log_checkif_localadministrativeisavailable_367 = rail.PythonOperator(
            task_id='log_checkif_localadministrativeisavailable_367',
            # pylint: disable=line-too-long
            python_callable=lambda: get_federal_legislative_355(
                'Local administrative', 'declare_list_320')
        )

        if_log_checkif_localadministrativeisavailable_367_blank_368 = rail.IfOperator(
            task_id='if_log_checkif_localadministrativeisavailable_367_blank_368',
            test='''{{ result('log_checkif_localadministrativeisavailable_367') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update369",
            no_task="log_checkif_stateadministrativeisavailable_371",
        )

        trigger_dag_run_live_nrdc_basic_add_update369 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update369',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Local administrative",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_primaryloginname_316') + "la",
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "Local administrative",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "na",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "C3 - Local Administrative",
                "authtype": "replicon",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update369 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update369',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update369") }}'
        )

        gather_user_uri_370 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_uri_370',
            dag_runs="{{ result('trigger_dag_run_live_nrdc_basic_add_update369')}}",
            dagrun_task_id='log_useruri_71',
            flatten=True
        )

        insert_to_list_370 = rail.SetVariableOperator(
            task_id='insert_to_list_370',
            append=True,
            name='{{ result("declare_list_320").name }}',
            value={
                "userloginname": "{{ result('log_primaryloginname_316') }}",
                "useruri": "{{ result('gather_user_uri_370')[0] }}",
                "status": "{{ dag_run.conf.accountstatus }}",
                "type": "Local administrative"
            }
        )

        log_checkif_stateadministrativeisavailable_371 = rail.PythonOperator(
            task_id='log_checkif_stateadministrativeisavailable_371',
            # pylint: disable=line-too-long
            python_callable=lambda: get_federal_legislative_355(
                'State administrative', 'declare_list_320')
        )

        if_log_checkif_stateadministrativeisavailable_371_blank_372 = rail.IfOperator(
            task_id='if_log_checkif_stateadministrativeisavailable_371_blank_372',
            test='''{{ result('log_checkif_stateadministrativeisavailable_371') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update373",
            no_task="log_f_l_user_profile_uri_375",
        )

        trigger_dag_run_live_nrdc_basic_add_update373 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update373',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "State administrative",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_primaryloginname_316') + "sa",
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "State administrative",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "na",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "C3 - State Administrative",
                "authtype": "replicon",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update373 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update373',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update373") }}'
        )

        gather_user_uri_374 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_uri_374',
            dag_runs="{{ result('trigger_dag_run_live_nrdc_basic_add_update373')}}",
            dagrun_task_id='log_useruri_71',
            flatten=True
        )

        insert_to_list_374 = rail.SetVariableOperator(
            task_id='insert_to_list_374',
            append=True,
            name='{{ result("declare_list_320").name }}',
            value={
                "userloginname": "{{ result('log_primaryloginname_316') }}",
                "useruri": "{{ result('gather_user_uri_374')[0] }}",
                "status": "{{ dag_run.conf.accountstatus }}",
                "type": "State administrative"
            }
        )

        def get_federal_legislative_375(type_name, users_list_task):
            available_user_profiles = rail.get_dag_run_var(
                rail.result(users_list_task)['name'])
            user_info = list(filter(
                lambda item: item['type'] == type_name, available_user_profiles))
            return user_info[0]['useruri'] if user_info else None

        log_f_l_user_profile_uri_375 = rail.PythonOperator(
            task_id='log_f_l_user_profile_uri_375',
            # pylint: disable=line-too-long
            python_callable=lambda: get_federal_legislative_375(
                'Federal Legislative', 'declare_list_320')
        )

        declare_list_update_dag_runs_376 = rail.SetVariableOperator(
            task_id='declare_list_update_dag_runs_376',
            name='user_process_update_dag_runs_376',
            value=[]
        )

        get_320_list_376 = rail.PythonOperator(
            task_id='get_320_list_376',
            python_callable=lambda: rail.get_dag_run_var(
                rail.result('declare_list_320')['name'])
        )

        foreach_declare_list_320_376 = rail.ForEachOperator(
            task_id='foreach_declare_list_320_376',
            items="{{ result('get_320_list_376') | to_json }}",
            start_task='get_all_substitute_user_assignments_for_user_377',
            end_task='foreach_declare_list_320_376_end'
        )

        get_all_substitute_user_assignments_for_user_377 = rail.RepliconServiceOperator(
            task_id='get_all_substitute_user_assignments_for_user_377',
            endpoint="/services/SubstituteUserAssignmentService1.svc/GetAllSubstituteUserAssignmentsForUser",
            data={
                "userUri": "{{ result('foreach_declare_list_320_376').useruri }}"
            }
        )

        log_substituteuserassigned_378 = rail.PythonOperator(
            task_id='log_substituteuserassigned_378',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_substitueUserUris(
                'log_f_l_user_profile_uri_375', 'get_all_substitute_user_assignments_for_user_377')
        )

        if_log_substituteuserassigned_378_blank_379 = rail.IfOperator(
            task_id='if_log_substituteuserassigned_378_blank_379',
            test='''{{ result('log_substituteuserassigned_378') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_assign_substitute_usersv2382",
            no_task="foreach_declare_list_320_376_end",
        )

        trigger_dag_run_live_nrdc_assign_substitute_usersv2382 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_assign_substitute_usersv2382',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_assignsubstituteusersv2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "suburi": rail.result('log_f_l_user_profile_uri_375'),
                "actualuri": rail.result('foreach_declare_list_320_376')['useruri'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        insert_to_user_dag_run_list_382 = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list_382',
            append=True,
            name='{{ result("declare_list_update_dag_runs_376").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_nrdc_assign_substitute_usersv2382"))[0]}}'
        )

        foreach_declare_list_320_376_end = rail.EmptyOperator(
            task_id='foreach_declare_list_320_376_end',
        )

        wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2382 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2382',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_user_dag_run_list_382").value | to_json }}'
        )

        if_request_currentprofilecount_equals_to_2_c4delegateto_c4_c3and_delegate2to7profiles_383 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_2_c4delegateto_c4_c3and_delegate2to7profiles_383',
            # pylint: disable=line-too-long
            test='''{{ dag_run.conf.currentprofilecount == 2  and dag_run.conf.memberof | matches('Delegate')  and dag_run.conf.memberof | matches('C4')  and dag_run.conf.memberof | matches('C3') }}''',
            yes_task="update_variable_384",
            no_task="if_request_currentprofilecount_equals_to_2_delegate_c4to_delegateprofile1profiles_418",
        )

        update_variable_384 = rail.SetVariableOperator(
            task_id='update_variable_384',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=7
        )

        if_declare_variable_41_value_equals_to_7_385 = rail.IfOperator(
            task_id='if_declare_variable_41_value_equals_to_7_385',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_41')['name']) == 7,
            yes_task="query_list_whereexistingprimaryprofilevalueis_delegate_386",
            no_task="if_request_currentprofilecount_equals_to_2_delegate_c4to_delegateprofile1profiles_418",
        )

        query_list_whereexistingprimaryprofilevalueis_delegate_386 = rail.QueryCollectionOperator(
            task_id='query_list_whereexistingprimaryprofilevalueis_delegate_386',
            query="""SELECT * FROM  existinguserdata WHERE  existinguserdata.type='Delegate'""",
        )

        def get_first_user_query(query_task):
            first_user_query = get_data_from_document(rail.result(
                query_task))
            return first_user_query[0] if first_user_query else {}

        get_first_records_from_query = rail.PythonOperator(
            task_id='get_first_records_from_query',
            python_callable=lambda:  get_first_user_query(
                'query_list_whereexistingprimaryprofilevalueis_delegate_386')
        )

        declare_variable_387 = rail.SetVariableOperator(
            task_id='declare_variable_387',
            append=False,
            name='useruri',
            value="{{ result('get_first_records_from_query').uri }}"
        )

        if_first_type_present_c4_creating_c3profiles_388 = rail.IfOperator(
            task_id='if_first_type_present_c4_creating_c3profiles_388',
            test="{{ result('get_first_records_from_query').type | is_truthy }}",
            yes_task="log_loginnameprimaryprofile_389",
            no_task="if_request_currentprofilecount_equals_to_2_delegate_c4to_delegateprofile1profiles_418",
        )

        log_loginnameprimaryprofile_389 = rail.PythonOperator(
            task_id='log_loginnameprimaryprofile_389',
            python_callable=lambda:  rail.result(
                'get_first_records_from_query')['userloginname']
        )

        log_useruri_primaryprofile_390 = rail.PythonOperator(
            task_id='log_useruri_primaryprofile_390',
            python_callable=lambda:  rail.get_dag_run_var(
                rail.result('declare_variable_387')['name'])
        )

        getuserdata_391 = rail.RepliconServicePageOperator(
            task_id="getuserdata_391",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('log_loginnameprimaryprofile_389')
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, rail.result('log_loginnameprimaryprofile_389'))
        )

        create_list_size5_395 = rail.EmptyOperator(
            task_id='create_list_size5_395',
        )

        declare_list_396 = rail.SetVariableOperator(
            task_id='declare_list_396',
            append=False,
            name='c3c4list',
            value=[]
        )

        get_c3_c4_list_397 = rail.PythonOperator(
            task_id='get_c3_c4_list_397',
            python_callable=lambda:  create_c3_c4_list(
                'log_loginnameprimaryprofile_389')
        )

        declare_list_update_dag_runs_402 = rail.SetVariableOperator(
            task_id='declare_list_update_dag_runs_402',
            name='user_process_update_dag_runs_402',
            value=[]
        )

        foreach_declare_list_396_402 = rail.ForEachOperator(
            task_id='foreach_declare_list_396_402',
            items="{{ result('get_c3_c4_list_397') | to_json }}",
            start_task='log_uriuser_403',
            end_task='foreach_declare_list_396_402_end'
        )

        log_uriuser_403 = rail.PythonOperator(
            task_id='log_uriuser_403',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_user_uri_215(
                'foreach_declare_list_396_402', 'getuserdata_391')
        )

        if_log_uriuser_403_present_404 = rail.IfOperator(
            task_id='if_log_uriuser_403_present_404',
            test='''{{ result('log_uriuser_403') | is_truthy }}''',
            yes_task="log_status_405",
            no_task="if_log_uriuser_403_blank_416",
        )

        log_status_405 = rail.PythonOperator(
            task_id='log_status_405',
            # pylint: disable=line-too-long
            python_callable=lambda: get_user_status_215(
                'foreach_declare_list_396_402', 'getuserdata_391')
        )

        if_log_status_405_not_equals_to_true_406 = rail.IfOperator(
            task_id='if_log_status_405_not_equals_to_true_406',
            test='''{{ result('log_status_405') != 'True' }}''',
            yes_task="re_enable_userprofile_407",
            no_task="if_log_uriuser_403_blank_416",
        )

        re_enable_userprofile_407 = rail.RepliconServiceOperator(
            task_id='re_enable_userprofile_407',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('log_uriuser_403') }}"
            }
        )

        remove_user_end_date_408 = rail.RepliconServiceOperator(
            task_id='remove_user_end_date_408',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('log_uriuser_403') }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": null,
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile409 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile409',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": rail.result('foreach_declare_list_396_402')['type'],
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": dag_run.conf['logonname'],
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('log_uriuser_403'),
                "locationuri": dag_run.conf['locationuri'],
                "type": rail.result('foreach_declare_list_396_402')['type'],
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        insert_to_user_dag_run_list_409 = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list_409',
            append=True,
            name='{{ result("declare_list_update_dag_runs_402").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile409"))[0]}}'
        )

        get_all_substitute_user_assignments_for_user_410 = rail.RepliconServiceOperator(
            task_id='get_all_substitute_user_assignments_for_user_410',
            endpoint="/services/SubstituteUserAssignmentService1.svc/GetAllSubstituteUserAssignmentsForUser",
            data={
                "userUri": "{{ result('log_uriuser_403') }}"
            }
        )

        log_substituteuserassigned_411 = rail.PythonOperator(
            task_id='log_substituteuserassigned_411',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_substitueUserUrisbyname(
                'log_loginnameprimaryprofile_389', 'get_all_substitute_user_assignments_for_user_410')
        )

        if_log_substituteuserassigned_411_blank_412 = rail.IfOperator(
            task_id='if_log_substituteuserassigned_411_blank_412',
            test='''{{ result('log_substituteuserassigned_411') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_assign_substitute_usersv2415",
            no_task="if_log_uriuser_403_blank_416",
        )

        trigger_dag_run_live_nrdc_assign_substitute_usersv2415 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_assign_substitute_usersv2415',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_assignsubstituteusersv2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "suburi": rail.result('log_useruri_primaryprofile_390'),
                "actualuri": rail.result('log_uriuser_403'),
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        insert_to_user_dag_run_list_415 = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list_415',
            append=True,
            name='{{ result("declare_list_update_dag_runs_402").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_nrdc_assign_substitute_usersv2415"))[0]}}'
        )

        if_log_uriuser_403_blank_416 = rail.IfOperator(
            task_id='if_log_uriuser_403_blank_416',
            test='''{{ result('log_uriuser_403') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update417",
            no_task="foreach_declare_list_396_402_end",
        )

        trigger_dag_run_live_nrdc_basic_add_update417 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update417',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": rail.result('foreach_declare_list_396_402')['type'],
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('foreach_declare_list_396_402')['loginname'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": rail.result('foreach_declare_list_396_402')['type'],
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": rail.result('log_useruri_primaryprofile_285'),
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": rail.result('foreach_declare_list_396_402')['timesheetpolicy'],
                "authtype": "replicon",
                "status": dag_run.conf['accountstatus']
            }
        )

        insert_to_user_dag_run_list_417 = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list_417',
            append=True,
            name='{{ result("declare_list_update_dag_runs_402").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_nrdc_basic_add_update417"))[0]}}'
        )

        foreach_declare_list_396_402_end = rail.EmptyOperator(
            task_id='foreach_declare_list_396_402_end',
        )

        get_dag_run_ids_418 = rail.PythonOperator(
            task_id='get_dag_run_ids_418',
            python_callable=lambda: rail.get_dag_run_var(
                rail.result('declare_list_update_dag_runs_402')['name'])
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update417 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update417',
            execution_timeout=timedelta(days=14),
            dag_runs="{{ result('get_dag_run_ids_418') | to_json}}"
        )

        if_request_currentprofilecount_equals_to_2_delegate_c4to_delegateprofile1profiles_418 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_2_delegate_c4to_delegateprofile1profiles_418',
            # pylint: disable=line-too-long
            test='''{{ dag_run.conf.currentprofilecount == 2  and dag_run.conf.memberof | matches('Delegate')  and dag_run.conf.memberof | matches('C4') | is_falsy }}''',
            yes_task="update_variable_419",
            no_task="if_request_currentprofilecount_equals_to_2_delegate_c4to_c4profile1profiles_424",
        )

        update_variable_419 = rail.SetVariableOperator(
            task_id='update_variable_419',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=1
        )

        if_declare_variable_41_value_equals_to_1_420 = rail.IfOperator(
            task_id='if_declare_variable_41_value_equals_to_1_420',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_41')['name']) == 1,
            yes_task="foreach_accumulate_list_items_16_421",
            no_task="if_request_currentprofilecount_equals_to_2_delegate_c4to_c4profile1profiles_424",
        )

        foreach_accumulate_list_items_16_421 = rail.ForEachOperator(
            task_id='foreach_accumulate_list_items_16_421',
            items="{{ result('create_list_14') | to_json}}",
            start_task='if_foreach_3157e122_421_type_equals_to_c4_422',
            end_task='foreach_accumulate_list_items_16_421_end'
        )

        if_foreach_3157e122_421_type_equals_to_c4_422 = rail.IfOperator(
            task_id='if_foreach_3157e122_421_type_equals_to_c4_422',
            test='''{{ result('foreach_accumulate_list_items_16_421').type == 'C4' }}''',
            yes_task="disable_loginoldprimaryprofile_423",
            no_task="foreach_accumulate_list_items_16_421_end",
        )

        disable_loginoldprimaryprofile_423 = rail.RepliconServiceOperator(
            task_id='disable_loginoldprimaryprofile_423',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_421').uri }}"
            }
        )

        foreach_accumulate_list_items_16_421_end = rail.EmptyOperator(
            task_id='foreach_accumulate_list_items_16_421_end',
        )

        if_request_currentprofilecount_equals_to_2_delegate_c4to_c4profile1profiles_424 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_2_delegate_c4to_c4profile1profiles_424',
            # pylint: disable=line-too-long
            test='''{{ dag_run.conf.currentprofilecount == 2  and dag_run.conf.memberof | matches('Delegate') | is_falsy  and dag_run.conf.memberof | matches('C4') }}''',
            yes_task="update_variable_425",
            no_task="if_request_currentprofilecount_equals_to_5_only_c4_c3to_c4_c36profiles_438",
        )

        update_variable_425 = rail.SetVariableOperator(
            task_id='update_variable_425',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=1
        )

        if_declare_variable_41_value_equals_to_1_426 = rail.IfOperator(
            task_id='if_declare_variable_41_value_equals_to_1_426',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_41')['name']) == 1,
            yes_task="log_primaryprofileloginname_427",
            no_task="if_request_currentprofilecount_equals_to_5_only_c4_c3to_c4_c36profiles_438",
        )

        log_primaryprofileloginname_427 = rail.PythonOperator(
            task_id='log_primaryprofileloginname_427',
            # pylint: disable=line-too-long
            python_callable=lambda: get_user_from_list_by_type(
                'Delegate', 'create_list_14')
        )

        query_list_wherevalueis_delegate_428 = rail.QueryCollectionOperator(
            task_id='query_list_wherevalueis_delegate_428',
            query="""SELECT * FROM  existinguserdata WHERE  existinguserdata.type = 'Delegate'""",
        )

        get_first_records_from_query_429 = rail.PythonOperator(
            task_id='get_first_records_from_query_429',
            python_callable=lambda:  get_first_user_query(
                'query_list_wherevalueis_delegate_428')
        )

        if_first_type_present_delegate_429 = rail.IfOperator(
            task_id='if_first_type_present_delegate_429',
            test="{{ result('get_first_records_from_query_429').type | is_truthy }}",
            yes_task="updateuserloginname_set_replicon_authentication_for_user_430",
            no_task="query_list_wherevalueisc4_434",
        )

        updateuserloginname_set_replicon_authentication_for_user_430 = rail.RepliconServiceOperator(
            task_id='updateuserloginname_set_replicon_authentication_for_user_430',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ result('get_first_records_from_query_429').uri }}",
                "loginName": "{{ result('get_first_records_from_query_429').userloginname }}d",
                "password": "Replicon@12",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        update_email_removingemail_431 = rail.RepliconServiceOperator(
            task_id='update_email_removingemail_431',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('get_first_records_from_query_429').uri }}",
                "email": null
            }
        )

        disable_loginoldprimaryprofile_432 = rail.RepliconServiceOperator(
            task_id='disable_loginoldprimaryprofile_432',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ result('get_first_records_from_query_429').uri }}"
            }
        )

        update_user_end_date_433 = rail.RepliconServiceOperator(
            task_id='update_user_end_date_433',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('get_first_records_from_query_429').uri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": {
                                "year": "{{ result('log_todays_year_19') }}",
                                "month": "{{ result('log_todays_month_20') }}",
                                "day": "{{ result('log_todays_day_21') }}"
                            }
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        query_list_wherevalueisc4_434 = rail.QueryCollectionOperator(
            task_id='query_list_wherevalueisc4_434',
            query="""SELECT * FROM  existinguserdata WHERE  existinguserdata.type = 'C4'""",
        )

        get_first_records_from_query_434 = rail.PythonOperator(
            task_id='get_first_records_from_query_434',
            python_callable=lambda:  get_first_user_query(
                'query_list_wherevalueisc4_434')
        )

        if_first_type_present_c4_435 = rail.IfOperator(
            task_id='if_first_type_present_c4_435',
            test="{{ result('get_first_records_from_query_434').type | is_truthy}}",
            yes_task="updatetoprimaryprofile_set_s_s_o_authentication_for_user_436",
            no_task="if_request_currentprofilecount_equals_to_5_only_c4_c3to_c4_c36profiles_438",
        )

        updatetoprimaryprofile_set_s_s_o_authentication_for_user_436 = rail.RepliconServiceOperator(
            task_id='updatetoprimaryprofile_set_s_s_o_authentication_for_user_436',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('get_first_records_from_query_434').uri }}",
                "loginName": "{{ result('log_primaryprofileloginname_427') }}"
            }
        )

        update_email_addingemail_437 = rail.RepliconServiceOperator(
            task_id='update_email_addingemail_437',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('get_first_records_from_query_434').uri }}",
                "email": "{{ dag_run.conf.emailaddress }}"
            }
        )

        if_request_currentprofilecount_equals_to_5_only_c4_c3to_c4_c36profiles_438 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_5_only_c4_c3to_c4_c36profiles_438',
            # pylint: disable=line-too-long
            test='''{{ dag_run.conf.currentprofilecount == 5  and dag_run.conf.memberof | matches('C4')  and dag_run.conf.memberof | matches('C3')  and dag_run.conf.memberof | matches('Delegate') | is_falsy }}''',
            yes_task="update_variable_439",
            no_task="if_request_currentprofilecount_equals_to_5_c3_c4to_c41profiles2_profiles_478",
        )

        update_variable_439 = rail.SetVariableOperator(
            task_id='update_variable_439',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=6
        )

        if_declare_variable_41_value_equals_to_6_440 = rail.IfOperator(
            task_id='if_declare_variable_41_value_equals_to_6_440',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_41')['name']) == 6,
            yes_task="log_loginnamefromemailprimaryprofile_441",
            no_task="if_request_currentprofilecount_equals_to_5_c3_c4to_c41profiles2_profiles_478",
        )

        log_loginnamefromemailprimaryprofile_441 = rail.PythonOperator(
            task_id='log_loginnamefromemailprimaryprofile_441',
            # pylint: disable=line-too-long
            python_callable=lambda: get_user_from_list_by_type(
                'Federal Legislative', 'create_list_14')
        )

        log_userurimainprofile_442 = rail.PythonOperator(
            task_id='log_userurimainprofile_442',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_user_uri_from_list_by_type(
                'Federal Legislative', 'create_list_14')
        )

        if_log_userurimainprofile_442_present_443 = rail.IfOperator(
            task_id='if_log_userurimainprofile_442_present_443',
            test='''{{ result('log_userurimainprofile_442') | is_truthy }}''',
            yes_task="updateuserloginname_set_replicon_authentication_for_user_444",
            no_task="log_loginnameprimaryold_446",
        )

        updateuserloginname_set_replicon_authentication_for_user_444 = rail.RepliconServiceOperator(
            task_id='updateuserloginname_set_replicon_authentication_for_user_444',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ result('log_userurimainprofile_442') }}",
                "loginName": "{{ result('log_loginnamefromemailprimaryprofile_441') }}fl",
                "password": "Replicon@12",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        update_email_removingemail_445 = rail.RepliconServiceOperator(
            task_id='update_email_removingemail_445',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('log_userurimainprofile_442') }}",
                "email": null
            }
        )

        log_loginnameprimaryold_446 = rail.PythonOperator(
            task_id='log_loginnameprimaryold_446',
            python_callable=lambda:  rail.result(
                'log_loginnamefromemailprimaryprofile_441') + "af"
        )

        search_users_447 = rail.RepliconServicePageOperator(
            task_id="search_users_447",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('log_loginnameprimaryold_446')
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, rail.result('log_loginnameprimaryold_446'))
        )

        if_search_users_447_users_less_than_1_448 = rail.IfOperator(
            task_id='if_search_users_447_users_less_than_1_448',
            test="{{result('search_users_447') | length == 0}}",
            yes_task="trigger_dag_run_live_nrdc_basic_add_update449",
            no_task="if_search_users_447_users_greater_than_0_450",
        )

        trigger_dag_run_live_nrdc_basic_add_update449 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update449',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_loginnamefromemailprimaryprofile_441'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "Action Fund",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "NA",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "C4 Timesheet",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update449 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update449',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update449") }}'
        )

        gather_user_uri_449 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_uri_449',
            dag_runs="{{ result('trigger_dag_run_live_nrdc_basic_add_update449')}}",
            dagrun_task_id='log_useruri_71',
            flatten=True
        )

        if_search_users_447_users_greater_than_0_450 = rail.IfOperator(
            task_id='if_search_users_447_users_greater_than_0_450',
            test="{{result('search_users_447') | length > 0}}",
            yes_task="log_useruribasedonthesuffix_451",
            no_task="log_newprimaryuseruri_470",
        )

        log_useruribasedonthesuffix_451 = rail.PythonOperator(
            task_id='log_useruribasedonthesuffix_451',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_user_uri_125(
                'log_loginnameprimaryold_446', 'search_users_447')
        )

        if_log_useruribasedonthesuffix_451_present_452 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_451_present_452',
            test='''{{ result('log_useruribasedonthesuffix_451') | is_truthy }}''',
            yes_task="log_userstatusbasedonthesuffix_453",
            no_task="if_log_useruribasedonthesuffix_451_blank_460",
        )

        log_userstatusbasedonthesuffix_453 = rail.PythonOperator(
            task_id='log_userstatusbasedonthesuffix_453',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_user_status(
                'log_loginnameprimaryold_446', 'search_users_447')
        )

        if_log_userstatusbasedonthesuffix_453_equals_to_false_454 = rail.IfOperator(
            task_id='if_log_userstatusbasedonthesuffix_453_equals_to_false_454',
            test='''{{ result('log_userstatusbasedonthesuffix_453') == 'False' }}''',
            yes_task="updatetoprimaryprofile_set_s_s_o_authentication_for_user_455",
            no_task="if_log_useruribasedonthesuffix_451_blank_460",
        )

        updatetoprimaryprofile_set_s_s_o_authentication_for_user_455 = rail.RepliconServiceOperator(
            task_id='updatetoprimaryprofile_set_s_s_o_authentication_for_user_455',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_451') }}",
                "loginName": "{{ result('log_loginnamefromemailprimaryprofile_441') }}"
            }
        )

        re_enable_userprofile_456 = rail.RepliconServiceOperator(
            task_id='re_enable_userprofile_456',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_451') }}"
            }
        )

        update_emailaddingemail_457 = rail.RepliconServiceOperator(
            task_id='update_emailaddingemail_457',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_451') }}",
                "email": "{{ dag_run.conf.emailaddress }}"
            }
        )

        update_user_end_dateremoveenddate_458 = rail.RepliconServiceOperator(
            task_id='update_user_end_dateremoveenddate_458',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('log_useruribasedonthesuffix_451') }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": null
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile459 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile459',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": dag_run.conf['logonname'],
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('log_useruribasedonthesuffix_451'),
                "locationuri": dag_run.conf['locationuri'],
                "type": "C4",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile459 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile459',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile459") }}'
        )

        if_log_useruribasedonthesuffix_451_blank_460 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_451_blank_460',
            test='''{{ result('log_useruribasedonthesuffix_451') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update461",
            no_task="gather_user_uri_461",
        )

        trigger_dag_run_live_nrdc_basic_add_update461 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update461',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_loginnamefromemailprimaryprofile_441'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "Action Fund",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": rail.result('log_useruri_primaryprofile_285'),
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "C4 Timesheet",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update461 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update461',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update461") }}'
        )

        gather_user_uri_461 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_uri_461',
            dag_runs="{{ result('trigger_dag_run_live_nrdc_basic_add_update461')}}",
            dagrun_task_id='log_useruri_71',
            flatten=True
        )

        log_primaryprofileuri_462 = rail.PythonOperator(
            task_id='log_primaryprofileuri_462',
            python_callable=lambda:  get_newly_created_from_basic_add_task(
                'gather_user_uri_449', 'log_useruribasedonthesuffix_451', 'gather_user_uri_461')
        )

        declare_list_update_dag_runs_463 = rail.SetVariableOperator(
            task_id='declare_list_update_dag_runs_463',
            name='user_process_update_dag_runs_463',
            value=[]
        )

        foreach_accumulate_list_items_16_463 = rail.ForEachOperator(
            task_id='foreach_accumulate_list_items_16_463',
            items="{{ result('create_list_14') | to_json}}",
            start_task='get_all_substitute_user_assignments_for_user_464',
            end_task='foreach_accumulate_list_items_16_463_end'
        )

        get_all_substitute_user_assignments_for_user_464 = rail.RepliconServiceOperator(
            task_id='get_all_substitute_user_assignments_for_user_464',
            endpoint="/services/SubstituteUserAssignmentService1.svc/GetAllSubstituteUserAssignmentsForUser",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_463').uri }}"
            }
        )

        log_substituteuserassigned_465 = rail.PythonOperator(
            task_id='log_substituteuserassigned_465',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_substitueUserUris(
                'log_primaryprofileuri_462', 'get_all_substitute_user_assignments_for_user_464')
        )

        if_log_substituteuserassigned_465_blank_466 = rail.IfOperator(
            task_id='if_log_substituteuserassigned_465_blank_466',
            test='''{{ result('log_substituteuserassigned_465') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_assign_substitute_usersv2469",
            no_task="foreach_accumulate_list_items_16_463_end",
        )

        trigger_dag_run_live_nrdc_assign_substitute_usersv2469 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_assign_substitute_usersv2469',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_assignsubstituteusersv2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "suburi": rail.result('log_primaryprofileuri_462'),
                "actualuri": rail.result('foreach_accumulate_list_items_16_463')['uri'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        insert_to_user_dag_run_list_463 = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list_463',
            append=True,
            name='{{ result("declare_list_update_dag_runs_463").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_nrdc_assign_substitute_usersv2469"))[0]}}'
        )

        foreach_accumulate_list_items_16_463_end = rail.EmptyOperator(
            task_id='foreach_accumulate_list_items_16_463_end',
        )

        wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2469 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2469',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_user_dag_run_list_463").value | to_json }}'
        )

        gather_user_uri_470 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_uri_470',
            dag_runs="{{ result('trigger_dag_run_live_nrdc_assign_substitute_usersv2469')}}",
            dagrun_task_id='log_useruri_71',
            flatten=True
        )

        log_newprimaryuseruri_470 = rail.PythonOperator(
            task_id='log_newprimaryuseruri_470',
            python_callable=lambda: get_newly_created_from_basic_add_task(
                'gather_user_uri_449', 'log_useruribasedonthesuffix_451', 'gather_user_uri_461')
        )

        declare_list_update_dag_runs_471 = rail.SetVariableOperator(
            task_id='declare_list_update_dag_runs_471',
            name='user_process_update_dag_runs_471',
            value=[]
        )

        foreach_accumulate_list_items_16_471 = rail.ForEachOperator(
            task_id='foreach_accumulate_list_items_16_471',
            items="{{ result('create_list_14') | to_json}}",
            start_task='get_all_substitute_user_assignments_for_user_472',
            end_task='foreach_accumulate_list_items_16_471_end'
        )

        get_all_substitute_user_assignments_for_user_472 = rail.RepliconServiceOperator(
            task_id='get_all_substitute_user_assignments_for_user_472',
            endpoint="/services/SubstituteUserAssignmentService1.svc/GetAllSubstituteUserAssignmentsForUser",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_471').uri }}"
            }
        )

        log_substituteuserassigned_473 = rail.PythonOperator(
            task_id='log_substituteuserassigned_473',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_substitueUserUrisbyname(
                'log_loginnamefromemailprimaryprofile_441', 'get_all_substitute_user_assignments_for_user_472')
        )

        if_log_substituteuserassigned_473_blank_474 = rail.IfOperator(
            task_id='if_log_substituteuserassigned_473_blank_474',
            test='''{{ result('log_substituteuserassigned_473') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_assign_substitute_usersv2477",
            no_task="foreach_accumulate_list_items_16_471_end",
        )

        trigger_dag_run_live_nrdc_assign_substitute_usersv2477 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_assign_substitute_usersv2477',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_assignsubstituteusersv2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "suburi": rail.result('log_newprimaryuseruri_470'),
                "actualuri": rail.result('foreach_accumulate_list_items_16_471')['uri'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        insert_to_user_dag_run_list_471 = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list_471',
            append=True,
            name='{{ result("declare_list_update_dag_runs_471").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile334"))[0]}}'
        )

        foreach_accumulate_list_items_16_471_end = rail.EmptyOperator(
            task_id='foreach_accumulate_list_items_16_471_end',
        )

        wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2477 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2477',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_user_dag_run_list_471").value | to_json }}'
        )

        if_request_currentprofilecount_equals_to_5_c3_c4to_c41profiles2_profiles_478 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_5_c3_c4to_c41profiles2_profiles_478',
            test='''{{ dag_run.conf.currentprofilecount == 5  and dag_run.conf.memberof | matches('C3') | is_falsy }}''',
            yes_task="log_loginnamefromemailprimaryprofile_479",
            no_task="if_request_currentprofilecount_equals_to_5_c3_c4to_c4_c3and_delegate7profiles_548",
        )

        log_loginnamefromemailprimaryprofile_479 = rail.PythonOperator(
            task_id='log_loginnamefromemailprimaryprofile_479',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_user_from_list_by_type(
                'Federal Legislative', 'create_list_14')
        )

        log_userurimainprofile_480 = rail.PythonOperator(
            task_id='log_userurimainprofile_480',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_user_uri_from_list_by_type(
                'Federal Legislative', 'create_list_14')
        )

        declare_variable_481 = rail.SetVariableOperator(
            task_id='declare_variable_481',
            append=False,
            name='useruri',
            value="{{ result('log_userurimainprofile_480') }}"
        )

        if_log_userurimainprofile_480_present_482 = rail.IfOperator(
            task_id='if_log_userurimainprofile_480_present_482',
            test='''{{ result('log_userurimainprofile_480') | is_truthy }}''',
            yes_task="updateuserloginname_set_replicon_authentication_for_user_483",
            no_task="if_request_memberof_contains_c4_487",
        )

        updateuserloginname_set_replicon_authentication_for_user_483 = rail.RepliconServiceOperator(
            task_id='updateuserloginname_set_replicon_authentication_for_user_483',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ result('log_userurimainprofile_480') }}",
                "loginName": "{{ result('log_loginnamefromemailprimaryprofile_479') }}fl",
                "password": "Replicon@12",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        update_email_removingemail_484 = rail.RepliconServiceOperator(
            task_id='update_email_removingemail_484',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('log_userurimainprofile_480') }}",
                "email": null
            }
        )

        disable_userprofile_485 = rail.RepliconServiceOperator(
            task_id='disable_userprofile_485',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ result('log_userurimainprofile_480') }}"
            }
        )

        update_user_end_date_486 = rail.RepliconServiceOperator(
            task_id='update_user_end_date_486',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('log_userurimainprofile_480') }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": {
                                "year": "{{result('log_todays_year_19')}}",
                                "month": "{{result('log_todays_month_20')}}",
                                "day": "{{result('log_todays_day_21')}}"
                            }
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        if_request_memberof_contains_c4_487 = rail.IfOperator(
            task_id='if_request_memberof_contains_c4_487',
            test='''{{ dag_run.conf.memberof | matches('C4')  and dag_run.conf.memberof | matches('Delegate') | is_falsy }}''',
            yes_task="log_loginnameprimaryold_488",
            no_task="if_request_memberof_contains_delegate_only_delegate_505",
        )

        log_loginnameprimaryold_488 = rail.PythonOperator(
            task_id='log_loginnameprimaryold_488',
            python_callable=lambda:  rail.result(
                'log_loginnamefromemailprimaryprofile_479') + "af"
        )

        search_users_489 = rail.RepliconServicePageOperator(
            task_id="search_users_489",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('log_loginnameprimaryold_488')
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, rail.result('log_loginnameprimaryold_488'))
        )

        if_search_users_489_users_less_than_1_490 = rail.IfOperator(
            task_id='if_search_users_489_users_less_than_1_490',
            test="{{result('search_users_489') | length == 0}}",
            yes_task="trigger_dag_run_live_nrdc_basic_add_update491",
            no_task="if_search_users_489_users_greater_than_0_492",
        )

        trigger_dag_run_live_nrdc_basic_add_update491 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update491',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_loginnamefromemailprimaryprofile_479'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "C4",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "NA",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "C4 Timesheet",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update491 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update491',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update491") }}'
        )

        gather_user_uri_491 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_uri_491',
            dag_runs="{{ result('trigger_dag_run_live_nrdc_basic_add_update491')}}",
            dagrun_task_id='log_useruri_71',
            flatten=True
        )

        if_search_users_489_users_greater_than_0_492 = rail.IfOperator(
            task_id='if_search_users_489_users_greater_than_0_492',
            test="{{result('search_users_489') | length > 0}}",
            yes_task="log_useruribasedonthesuffix_493",
            no_task="update_variable_primaryprofileuri_504",
        )

        log_useruribasedonthesuffix_493 = rail.PythonOperator(
            task_id='log_useruribasedonthesuffix_493',
            python_callable=lambda:  get_user_uri_125(
                'log_loginnameprimaryold_488', 'search_users_489')
        )

        if_log_useruribasedonthesuffix_493_present_494 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_493_present_494',
            test='''{{ result('log_useruribasedonthesuffix_493') | is_truthy }}''',
            yes_task="log_userstatusbasedonthesuffix_495",
            no_task="if_log_useruribasedonthesuffix_493_blank_502",
        )

        log_userstatusbasedonthesuffix_495 = rail.PythonOperator(
            task_id='log_userstatusbasedonthesuffix_495',
            python_callable=lambda:  get_user_status(
                'log_loginnameprimaryold_488', 'search_users_489')
        )

        if_log_userstatusbasedonthesuffix_495_equals_to_false_496 = rail.IfOperator(
            task_id='if_log_userstatusbasedonthesuffix_495_equals_to_false_496',
            test='''{{ result('log_userstatusbasedonthesuffix_495') == 'False' }}''',
            yes_task="updatetoprimaryprofile_set_s_s_o_authentication_for_user_497",
            no_task="if_log_useruribasedonthesuffix_493_blank_502",
        )

        updatetoprimaryprofile_set_s_s_o_authentication_for_user_497 = rail.RepliconServiceOperator(
            task_id='updatetoprimaryprofile_set_s_s_o_authentication_for_user_497',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_493') }}",
                "loginName": "{{ result('log_loginnamefromemailprimaryprofile_479') }}"
            }
        )

        re_enable_userprofile_498 = rail.RepliconServiceOperator(
            task_id='re_enable_userprofile_498',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_493') }}"
            }
        )

        update_emailaddingemail_499 = rail.RepliconServiceOperator(
            task_id='update_emailaddingemail_499',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_493') }}",
                "email": "{{ dag_run.conf.emailaddress }}"
            }
        )

        update_user_end_dateremoveenddate_500 = rail.RepliconServiceOperator(
            task_id='update_user_end_dateremoveenddate_500',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('log_useruribasedonthesuffix_493') }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": null
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile501 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile501',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": dag_run.conf['logonname'],
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('log_useruribasedonthesuffix_493'),
                "locationuri": dag_run.conf['locationuri'],
                "type": "C4",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile501 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile501',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile501") }}'
        )

        if_log_useruribasedonthesuffix_493_blank_502 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_493_blank_502',
            test='''{{ result('log_useruribasedonthesuffix_493') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update503",
            no_task="update_variable_primaryprofileuri_504",
        )

        trigger_dag_run_live_nrdc_basic_add_update503 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update503',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_loginnamefromemailprimaryprofile_479'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "C4",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "NA",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "C4 Timesheet",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update503 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update503',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update503") }}'
        )

        gather_user_uri_503 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_uri_503',
            dag_runs="{{ result('trigger_dag_run_live_nrdc_basic_add_update503')}}",
            dagrun_task_id='log_useruri_71',
            flatten=True
        )

        def get_user_uri_basic_add_task_504():
            user_uri_491 = rail.result('gather_user_uri_491')[
                0] if rail.result('gather_user_uri_491') else None
            user_uri_493 = rail.result('log_useruribasedonthesuffix_493')
            user_uri_503 = rail.result('gather_user_uri_503')[
                0] if rail.result('gather_user_uri_503') else None
            return user_uri_491 or user_uri_493 or user_uri_503

        update_variable_primaryprofileuri_504 = rail.SetVariableOperator(
            task_id='update_variable_primaryprofileuri_504',
            append=False,
            name='{{ result("declare_variable_481").name }}',
            value=get_user_uri_basic_add_task_504
        )

        if_request_memberof_contains_delegate_only_delegate_505 = rail.IfOperator(
            task_id='if_request_memberof_contains_delegate_only_delegate_505',
            test='''{{ dag_run.conf.memberof | matches('Delegate') }}''',
            yes_task="log_loginnameprimaryold_506",
            no_task="foreach_accumulate_list_items_16_545",
        )

        log_loginnameprimaryold_506 = rail.PythonOperator(
            task_id='log_loginnameprimaryold_506',
            python_callable=lambda:  rail.result(
                'log_loginnamefromemailprimaryprofile_479') + "d"
        )

        search_users_507 = rail.RepliconServicePageOperator(
            task_id="search_users_507",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('log_loginnameprimaryold_506')
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, rail.result('log_loginnameprimaryold_506'))
        )

        if_search_users_507_users_less_than_1_508 = rail.IfOperator(
            task_id='if_search_users_507_users_less_than_1_508',
            test="{{result('search_users_507') | length == 0}}",
            yes_task="trigger_dag_run_live_nrdc_basic_add_update509",
            no_task="if_search_users_507_users_greater_than_0_510",
        )

        trigger_dag_run_live_nrdc_basic_add_update509 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update509',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Delegate",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_loginnamefromemailprimaryprofile_479'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "Delegate",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "NA",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "NA",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update509 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update509',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update509") }}'
        )

        gather_user_uri_509 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_uri_509',
            dag_runs="{{ result('trigger_dag_run_live_nrdc_basic_add_update509')}}",
            dagrun_task_id='log_useruri_71',
            flatten=True
        )

        if_search_users_507_users_greater_than_0_510 = rail.IfOperator(
            task_id='if_search_users_507_users_greater_than_0_510',
            test="{{result('search_users_507') | length > 0}}",
            yes_task="log_useruribasedonthesuffix_511",
            no_task="update_variable_primaryprofileuri_522",
        )

        log_useruribasedonthesuffix_511 = rail.PythonOperator(
            task_id='log_useruribasedonthesuffix_511',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_user_uri_125(
                'log_loginnameprimaryold_506', 'search_users_507')
        )

        if_log_useruribasedonthesuffix_511_present_512 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_511_present_512',
            test='''{{ result('log_useruribasedonthesuffix_511') | is_truthy }}''',
            yes_task="log_userstatusbasedonthesuffix_513",
            no_task="if_log_useruribasedonthesuffix_511_blank_520",
        )

        log_userstatusbasedonthesuffix_513 = rail.PythonOperator(
            task_id='log_userstatusbasedonthesuffix_513',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_user_status(
                'log_loginnameprimaryold_506', 'search_users_507')
        )

        if_log_userstatusbasedonthesuffix_513_not_equals_to_false_514 = rail.IfOperator(
            task_id='if_log_userstatusbasedonthesuffix_513_not_equals_to_false_514',
            test='''{{ result('log_userstatusbasedonthesuffix_513') != 'False' }}''',
            yes_task="updatetoprimaryprofile_set_s_s_o_authentication_for_user_515",
            no_task="if_log_useruribasedonthesuffix_511_blank_520",
        )

        updatetoprimaryprofile_set_s_s_o_authentication_for_user_515 = rail.RepliconServiceOperator(
            task_id='updatetoprimaryprofile_set_s_s_o_authentication_for_user_515',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_511') }}",
                "loginName": "{{ result('log_loginnamefromemailprimaryprofile_479') }}"
            }
        )

        re_enable_userprofile_516 = rail.RepliconServiceOperator(
            task_id='re_enable_userprofile_516',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_511') }}"
            }
        )

        update_emailaddingemail_517 = rail.RepliconServiceOperator(
            task_id='update_emailaddingemail_517',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_511') }}",
                "email": "{{ dag_run.conf.emailaddress }}"
            }
        )

        update_user_end_dateremoveenddate_518 = rail.RepliconServiceOperator(
            task_id='update_user_end_dateremoveenddate_518',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('log_useruribasedonthesuffix_511') }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": null
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile519 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile519',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Delegate",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": dag_run.conf['logonname'],
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('log_useruribasedonthesuffix_511'),
                "locationuri": dag_run.conf['locationuri'],
                "type": "Delegate",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile519 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile519',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile519") }}'
        )

        if_log_useruribasedonthesuffix_511_blank_520 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_511_blank_520',
            test='''{{ result('log_useruribasedonthesuffix_511') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update521",
            no_task="update_variable_primaryprofileuri_522",
        )

        trigger_dag_run_live_nrdc_basic_add_update521 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update521',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Delegate",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_loginnamefromemailprimaryprofile_479'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "Delegate",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "NA",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "na",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update521 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update521',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update521") }}'
        )

        gather_user_uri_521 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_uri_521',
            dag_runs="{{ result('trigger_dag_run_live_nrdc_basic_add_update521')}}",
            dagrun_task_id='log_useruri_71',
            flatten=True
        )

        def get_user_uri_basic_add_task_522():
            user_uri_509 = rail.result('gather_user_uri_509')[
                0] if rail.result('gather_user_uri_509') else None
            user_uri_511 = rail.result('log_useruribasedonthesuffix_511')
            user_uri_521 = rail.result('gather_user_uri_521')[
                0] if rail.result('gather_user_uri_521') else None
            return user_uri_509 or user_uri_511 or user_uri_521

        update_variable_primaryprofileuri_522 = rail.SetVariableOperator(
            task_id='update_variable_primaryprofileuri_522',
            append=False,
            name='{{ result("declare_variable_481").name }}',
            value=get_user_uri_basic_add_task_522
        )

        if_request_memberof_contains_c4_delegateand_c42profiles_523 = rail.IfOperator(
            task_id='if_request_memberof_contains_c4_delegateand_c42profiles_523',
            test='''{{ dag_run.conf.memberof | matches('C4') }}''',
            yes_task="log_loginnameprimaryold_524",
            no_task="foreach_accumulate_list_items_16_545",
        )

        log_loginnameprimaryold_524 = rail.PythonOperator(
            task_id='log_loginnameprimaryold_524',
            python_callable=lambda:  rail.result(
                'log_loginnamefromemailprimaryprofile_479') + "af"
        )

        search_users_525 = rail.RepliconServicePageOperator(
            task_id="search_users_525",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('log_loginnameprimaryold_524')
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, rail.result('log_loginnameprimaryold_524'))
        )

        if_search_users_525_users_less_than_1_526 = rail.IfOperator(
            task_id='if_search_users_525_users_less_than_1_526',
            test="{{result('search_users_525') | length == 0}}",
            yes_task="trigger_dag_run_live_nrdc_basic_add_update527",
            no_task="if_search_users_525_users_greater_than_0_528",
        )

        trigger_dag_run_live_nrdc_basic_add_update527 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update527',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_loginnameprimaryold_524'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "C4",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "NA",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "C4 Timesheet",
                "authtype": "replicon",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update527 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update527',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update527") }}'
        )

        gather_user_uri_527 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_uri_527',
            dag_runs="{{ result('trigger_dag_run_live_nrdc_basic_add_update527')}}",
            dagrun_task_id='log_useruri_71',
            flatten=True
        )

        if_search_users_525_users_greater_than_0_528 = rail.IfOperator(
            task_id='if_search_users_525_users_greater_than_0_528',
            test="{{result('search_users_525') | length > 0}}",
            yes_task="log_useruribasedonthesuffix_529",
            no_task="log_childuseruri_538",
        )

        log_useruribasedonthesuffix_529 = rail.PythonOperator(
            task_id='log_useruribasedonthesuffix_529',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_user_uri_125(
                'log_loginnameprimaryold_524', 'search_users_525')
        )

        if_log_useruribasedonthesuffix_529_present_530 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_529_present_530',
            test='''{{ result('log_useruribasedonthesuffix_529') | is_truthy }}''',
            yes_task="log_userstatusbasedonthesuffix_531",
            no_task="if_log_useruribasedonthesuffix_529_blank_536",
        )

        log_userstatusbasedonthesuffix_531 = rail.PythonOperator(
            task_id='log_userstatusbasedonthesuffix_531',
            python_callable=lambda:  get_user_status(
                'log_loginnameprimaryold_524', 'search_users_525')
        )

        if_log_userstatusbasedonthesuffix_531_is_not_true_532 = rail.IfOperator(
            task_id='if_log_userstatusbasedonthesuffix_531_is_not_true_532',
            test='''{{ result('log_userstatusbasedonthesuffix_531') | is_falsy }}''',
            yes_task="re_enable_userprofile_533",
            no_task="if_log_useruribasedonthesuffix_529_blank_536",
        )

        re_enable_userprofile_533 = rail.RepliconServiceOperator(
            task_id='re_enable_userprofile_533',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_529') }}"
            }
        )

        update_user_end_dateremoveenddate_534 = rail.RepliconServiceOperator(
            task_id='update_user_end_dateremoveenddate_534',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('log_useruribasedonthesuffix_529') }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": {
                            "emailAddress": "{{ dag_run.conf.logonname }}"
                        },
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": null
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile535 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile535',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": dag_run.conf['logonname'],
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('log_useruribasedonthesuffix_529'),
                "locationuri": dag_run.conf['locationuri'],
                "type": "C4",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile535 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile535',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile535") }}'
        )

        if_log_useruribasedonthesuffix_529_blank_536 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_529_blank_536',
            test='''{{ result('log_useruribasedonthesuffix_529') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update537",
            no_task="log_childuseruri_538",
        )

        trigger_dag_run_live_nrdc_basic_add_update537 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update537',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_loginnamefromemailprimaryprofile_479'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "C4",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "NA",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "C4 Timesheet",
                "authtype": "replicon",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update537 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update537',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update537") }}'
        )

        gather_user_uri_537 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_uri_537',
            dag_runs="{{ result('trigger_dag_run_live_nrdc_basic_add_update537')}}",
            dagrun_task_id='log_useruri_71',
            flatten=True
        )

        log_childuseruri_538 = rail.PythonOperator(
            task_id='log_childuseruri_538',
            python_callable=lambda:  get_newly_created_from_basic_add_task(
                'gather_user_uri_527', 'log_useruribasedonthesuffix_529', 'gather_user_uri_537')
        )

        get_all_substitute_user_assignments_for_user_539 = rail.RepliconServiceOperator(
            task_id='get_all_substitute_user_assignments_for_user_539',
            endpoint="/services/SubstituteUserAssignmentService1.svc/GetAllSubstituteUserAssignmentsForUser",
            data={
                "userUri": "{{ result('log_childuseruri_538') }}"
            }
        )

        log_substituteuserassigned_540 = rail.PythonOperator(
            task_id='log_substituteuserassigned_540',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_substitueUserUrisbyname(
                'log_loginnamefromemailprimaryprofile_479', 'get_all_substitute_user_assignments_for_user_539')
        )

        if_log_substituteuserassigned_540_blank_541 = rail.IfOperator(
            task_id='if_log_substituteuserassigned_540_blank_541',
            test='''{{ result('log_substituteuserassigned_540') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_assign_substitute_usersv2544",
            no_task="foreach_accumulate_list_items_16_545",
        )

        trigger_dag_run_live_nrdc_assign_substitute_usersv2544 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_assign_substitute_usersv2544',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_assignsubstituteusersv2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "suburi": rail.get_dag_run_var(rail.result('declare_variable_481')['name']),
                "actualuri": rail.result('log_childuseruri_538'),
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2544 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2544',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_assign_substitute_usersv2544") }}'
        )

        foreach_accumulate_list_items_16_545 = rail.ForEachOperator(
            task_id='foreach_accumulate_list_items_16_545',
            items="{{ result('create_list_14') | to_json}}",
            start_task='if_foreach_3157e122_545_type_not_equals_to_federallegislative_546',
            end_task='foreach_accumulate_list_items_16_545_end'
        )

        if_foreach_3157e122_545_type_not_equals_to_federallegislative_546 = rail.IfOperator(
            task_id='if_foreach_3157e122_545_type_not_equals_to_federallegislative_546',
            test='''{{ result('foreach_accumulate_list_items_16_545').type != 'Federal Legislative' }}''',
            yes_task="disable_loginoldprofiles_547",
            no_task="foreach_accumulate_list_items_16_545_end",
        )

        disable_loginoldprofiles_547 = rail.RepliconServiceOperator(
            task_id='disable_loginoldprofiles_547',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_545').uri }}"
            }
        )

        foreach_accumulate_list_items_16_545_end = rail.EmptyOperator(
            task_id='foreach_accumulate_list_items_16_545_end',
        )

        if_request_currentprofilecount_equals_to_5_c3_c4to_c4_c3and_delegate7profiles_548 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_5_c3_c4to_c4_c3and_delegate7profiles_548',
            # pylint: disable=line-too-long
            test='''{{ dag_run.conf.currentprofilecount == 5  and dag_run.conf.memberof | matches('C3')  and dag_run.conf.memberof | matches('C4')  and dag_run.conf.memberof | matches('Delegate') }}''',
            yes_task="update_variable_549",
            no_task="if_request_currentprofilecount_equals_to_5_delegate_c3to_delegate_c36profiles_601",
        )

        update_variable_549 = rail.SetVariableOperator(
            task_id='update_variable_549',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=7
        )

        if_declare_variable_41_value_equals_to_7_550 = rail.IfOperator(
            task_id='if_declare_variable_41_value_equals_to_7_550',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_41')['name']) == 7,
            yes_task="log_loginnamefromemailprimaryprofile_551",
            no_task="if_request_currentprofilecount_equals_to_5_delegate_c3to_delegate_c36profiles_601",
        )

        log_loginnamefromemailprimaryprofile_551 = rail.PythonOperator(
            task_id='log_loginnamefromemailprimaryprofile_551',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_user_from_list_by_type(
                'Federal Legislative', 'create_list_14')
        )

        log_userurimainprofile_552 = rail.PythonOperator(
            task_id='log_userurimainprofile_552',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_user_uri_from_list_by_type(
                'Federal Legislative', 'create_list_14')
        )

        declare_variable_553 = rail.SetVariableOperator(
            task_id='declare_variable_553',
            append=False,
            name='useruri',
            value="{{ result('log_userurimainprofile_480') }}"
        )

        if_log_loginnamefromemailprimaryprofile_551_present_554 = rail.IfOperator(
            task_id='if_log_loginnamefromemailprimaryprofile_551_present_554',
            test='''{{ result('log_loginnamefromemailprimaryprofile_551') | is_truthy }}''',
            yes_task="updateuserloginname_set_replicon_authentication_for_user_555",
            no_task="if_request_memberof_contains_delegate_only_delegate_557",
        )

        updateuserloginname_set_replicon_authentication_for_user_555 = rail.RepliconServiceOperator(
            task_id='updateuserloginname_set_replicon_authentication_for_user_555',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ result('log_userurimainprofile_552') }}",
                "loginName": "{{ result('log_loginnamefromemailprimaryprofile_551') }}fl",
                "password": "Replicon@12",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        update_email_removingemail_556 = rail.RepliconServiceOperator(
            task_id='update_email_removingemail_556',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('log_userurimainprofile_552') }}",
                "email": null
            }
        )

        if_request_memberof_contains_delegate_only_delegate_557 = rail.IfOperator(
            task_id='if_request_memberof_contains_delegate_only_delegate_557',
            test='''{{ dag_run.conf.memberof | matches('Delegate') }}''',
            yes_task="log_loginnameprimarynewd_558",
            no_task="if_request_currentprofilecount_equals_to_5_delegate_c3to_delegate_c36profiles_601",
        )

        log_loginnameprimarynewd_558 = rail.PythonOperator(
            task_id='log_loginnameprimarynewd_558',
            python_callable=lambda:  rail.result(
                'log_loginnamefromemailprimaryprofile_551') + "d"
        )

        search_users_559 = rail.RepliconServicePageOperator(
            task_id="search_users_559",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('log_loginnameprimarynewd_558')
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, rail.result('log_loginnameprimarynewd_558'))
        )

        if_search_users_559_users_less_than_1_560 = rail.IfOperator(
            task_id='if_search_users_559_users_less_than_1_560',
            test="{{result('search_users_559') | length == 0}}",
            yes_task="trigger_dag_run_live_nrdc_basic_add_update561",
            no_task="if_search_users_559_users_greater_than_0_562",
        )

        trigger_dag_run_live_nrdc_basic_add_update561 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update561',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Delegate",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_loginnamefromemailprimaryprofile_551'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "Delegate",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "NA",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "na",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update561 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update561',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update561") }}'
        )

        if_search_users_559_users_greater_than_0_562 = rail.IfOperator(
            task_id='if_search_users_559_users_greater_than_0_562',
            test="{{result('search_users_559') | length > 0}}",
            yes_task="log_useruribasedonthesuffix_563",
            no_task="update_variable_primaryprofileuri_574",
        )

        log_useruribasedonthesuffix_563 = rail.PythonOperator(
            task_id='log_useruribasedonthesuffix_563',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_user_uri_125(
                'log_loginnameprimarynewd_558', 'search_users_559')
        )

        if_log_useruribasedonthesuffix_563_present_564 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_563_present_564',
            test='''{{ result('log_useruribasedonthesuffix_563') | is_truthy }}''',
            yes_task="log_userstatusbasedonthesuffix_565",
            no_task="if_log_useruribasedonthesuffix_563_blank_572",
        )

        log_userstatusbasedonthesuffix_565 = rail.PythonOperator(
            task_id='log_userstatusbasedonthesuffix_565',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_user_status(
                'log_loginnameprimarynewd_558', 'search_users_559')
        )

        if_log_userstatusbasedonthesuffix_565_equals_to_false_566 = rail.IfOperator(
            task_id='if_log_userstatusbasedonthesuffix_565_equals_to_false_566',
            test='''{{ result('log_userstatusbasedonthesuffix_565') == 'False' }}''',
            yes_task="updatetoprimaryprofile_set_s_s_o_authentication_for_user_567",
            no_task="if_log_useruribasedonthesuffix_563_blank_572",
        )

        updatetoprimaryprofile_set_s_s_o_authentication_for_user_567 = rail.RepliconServiceOperator(
            task_id='updatetoprimaryprofile_set_s_s_o_authentication_for_user_567',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_563') }}",
                "loginName": "{{ result('log_loginnamefromemailprimaryprofile_551') }}"
            }
        )

        re_enable_userprofile_568 = rail.RepliconServiceOperator(
            task_id='re_enable_userprofile_568',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_563') }}"
            }
        )

        update_emailaddingemail_569 = rail.RepliconServiceOperator(
            task_id='update_emailaddingemail_569',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_563') }}",
                "email": "{{ dag_run.conf.emailaddress }}"
            }
        )

        update_user_end_dateremoveenddate_570 = rail.RepliconServiceOperator(
            task_id='update_user_end_dateremoveenddate_570',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('log_useruribasedonthesuffix_563') }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": null
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile571 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile571',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Delegate",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": dag_run.conf['logonname'],
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('log_useruribasedonthesuffix_563'),
                "locationuri": dag_run.conf['locationuri'],
                "type": "Delegate",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile571 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile571',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile571") }}'
        )

        if_log_useruribasedonthesuffix_563_blank_572 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_563_blank_572',
            test='''{{ result('log_useruribasedonthesuffix_563') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update573",
            no_task="update_variable_primaryprofileuri_574",
        )

        trigger_dag_run_live_nrdc_basic_add_update573 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update573',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Delegate",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_loginnamefromemailprimaryprofile_479'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "Delegate",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "NA",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "na",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update573 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update573',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update573") }}'
        )

        update_variable_primaryprofileuri_574 = rail.SetVariableOperator(
            task_id='update_variable_primaryprofileuri_574',
            append=False,
            name='{{ result("declare_variable_553").name }}',
            # pylint: disable=line-too-long
            value="('data.workato_service.trigger_dag_run_live_nrdc_basic_add_update561.reply.useruri.present? ? _('data.workato_service.trigger_dag_run_live_nrdc_basic_add_update561.reply.useruri : (result('log_useruribasedonthesuffix_563')').present? ? result('log_useruribasedonthesuffix_563')') : _('data.workato_service.trigger_dag_run_live_nrdc_basic_add_update573.reply.useruri)"
        )

        if_request_memberof_contains_c4_delegateand_c42profiles_575 = rail.IfOperator(
            task_id='if_request_memberof_contains_c4_delegateand_c42profiles_575',
            test='''{{ dag_run.conf.memberof | matches('C4') }}''',
            yes_task="log_loginnameprimaryold_576",
            no_task="declare_list_update_dag_runs_597",
        )

        log_loginnameprimaryold_576 = rail.PythonOperator(
            task_id='log_loginnameprimaryold_576',
            python_callable=lambda:  rail.result(
                'log_loginnamefromemailprimaryprofile_551') + "af"
        )

        search_users_577 = rail.RepliconServicePageOperator(
            task_id="search_users_577",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('log_loginnameprimaryold_576')
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, rail.result('log_loginnameprimaryold_576'))
        )

        if_search_users_577_users_less_than_1_578 = rail.IfOperator(
            task_id='if_search_users_577_users_less_than_1_578',
            test="{{result('search_users_577') | length == 0}}",
            yes_task="trigger_dag_run_live_nrdc_basic_add_update579",
            no_task="if_search_users_577_users_greater_than_0_580",
        )

        trigger_dag_run_live_nrdc_basic_add_update579 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update579',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_loginnameprimaryold_576'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "C4",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "NA",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "C4 Timesheet",
                "authtype": "replicon",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update579 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update579',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update579") }}'
        )

        gather_user_uri_579 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_uri_579',
            dag_runs="{{ result('trigger_dag_run_live_nrdc_basic_add_update579')}}",
            dagrun_task_id='log_useruri_71',
            flatten=True
        )

        if_search_users_577_users_greater_than_0_580 = rail.IfOperator(
            task_id='if_search_users_577_users_greater_than_0_580',
            test="{{result('search_users_577') | length > 0}}",
            yes_task="log_useruribasedonthesuffix_581",
            no_task="log_childuseruri_590",
        )

        log_useruribasedonthesuffix_581 = rail.PythonOperator(
            task_id='log_useruribasedonthesuffix_581',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_user_uri_125(
                'log_loginnameprimaryold_576', 'search_users_577')
        )

        if_log_useruribasedonthesuffix_581_present_582 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_581_present_582',
            test='''{{ result('log_useruribasedonthesuffix_581') | is_truthy }}''',
            yes_task="log_userstatusbasedonthesuffix_583",
            no_task="if_log_useruribasedonthesuffix_581_blank_588",
        )

        log_userstatusbasedonthesuffix_583 = rail.PythonOperator(
            task_id='log_userstatusbasedonthesuffix_583',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_user_status(
                'log_loginnameprimaryold_576', 'search_users_577')
        )

        if_log_userstatusbasedonthesuffix_583_equals_to_false_584 = rail.IfOperator(
            task_id='if_log_userstatusbasedonthesuffix_583_equals_to_false_584',
            test='''{{ result('log_userstatusbasedonthesuffix_583') == 'False' }}''',
            yes_task="re_enable_userprofile_585",
            no_task="if_log_useruribasedonthesuffix_581_blank_588",
        )

        re_enable_userprofile_585 = rail.RepliconServiceOperator(
            task_id='re_enable_userprofile_585',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_581') }}"
            }
        )

        update_user_end_dateremoveenddate_586 = rail.RepliconServiceOperator(
            task_id='update_user_end_dateremoveenddate_586',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('log_useruribasedonthesuffix_581') }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": {
                            "emailAddress": null
                        },
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": null
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile587 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile587',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": dag_run.conf['logonname'],
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('log_useruribasedonthesuffix_581'),
                "locationuri": dag_run.conf['locationuri'],
                "type": "C4",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile587 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile587',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile587") }}'
        )

        if_log_useruribasedonthesuffix_581_blank_588 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_581_blank_588',
            test='''{{ result('log_useruribasedonthesuffix_581') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update589",
            no_task="log_childuseruri_590",
        )

        trigger_dag_run_live_nrdc_basic_add_update589 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update589',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_loginnameprimaryold_576'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "C4",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "NA",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "C4 Timesheet",
                "authtype": "replicon",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update589 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update589',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update589") }}'
        )

        gather_user_uri_589 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_uri_589',
            dag_runs="{{ result('trigger_dag_run_live_nrdc_basic_add_update589')}}",
            dagrun_task_id='log_useruri_71',
            flatten=True
        )

        log_childuseruri_590 = rail.PythonOperator(
            task_id='log_childuseruri_590',
            python_callable=lambda:  get_newly_created_from_basic_add_task(
                'gather_user_uri_579', 'log_useruribasedonthesuffix_581', 'gather_user_uri_589')
        )

        get_all_substitute_user_assignments_for_user_591 = rail.RepliconServiceOperator(
            task_id='get_all_substitute_user_assignments_for_user_591',
            endpoint="/services/SubstituteUserAssignmentService1.svc/GetAllSubstituteUserAssignmentsForUser",
            data={
                "userUri": "{{ result('log_childuseruri_590') }}"
            }
        )

        log_substituteuserassigned_592 = rail.PythonOperator(
            task_id='log_substituteuserassigned_592',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_substitueUserUrisbyname(
                'log_loginnamefromemailprimaryprofile_551', 'get_all_substitute_user_assignments_for_user_591')
        )

        if_log_substituteuserassigned_592_blank_593 = rail.IfOperator(
            task_id='if_log_substituteuserassigned_592_blank_593',
            test='''{{ result('log_substituteuserassigned_592') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_assign_substitute_usersv2596",
            no_task="declare_list_update_dag_runs_597",
        )

        trigger_dag_run_live_nrdc_assign_substitute_usersv2596 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_assign_substitute_usersv2596',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_assignsubstituteusersv2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "suburi": rail.get_dag_run_var(rail.result('declare_variable_553')['name']),
                "actualuri": rail.result('log_childuseruri_590'),
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2596 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2596',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_assign_substitute_usersv2596") }}'
        )

        declare_list_update_dag_runs_597 = rail.SetVariableOperator(
            task_id='declare_list_update_dag_runs_597',
            name='user_process_update_dag_runs_597',
            value=[]
        )

        foreach_accumulate_list_items_16_597 = rail.ForEachOperator(
            task_id='foreach_accumulate_list_items_16_597',
            items="{{ result('create_list_14') | to_json}}",
            start_task='trigger_dag_run_live_nrdc_assign_substitute_usersv2600',
            end_task='foreach_accumulate_list_items_16_597_end'
        )

        trigger_dag_run_live_nrdc_assign_substitute_usersv2600 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_assign_substitute_usersv2600',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_assignsubstituteusersv2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "suburi": rail.get_dag_run_var(rail.result('declare_variable_553')['name']),
                "actualuri": rail.result('foreach_accumulate_list_items_16_597')['uri'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        insert_to_user_dag_run_list_597 = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list_597',
            append=True,
            name='{{ result("declare_list_update_dag_runs_597").name }}',
            value='{{(result("trigger_dag_run_live_nrdc_assign_substitute_usersv2600"))[0]}}'
        )

        foreach_accumulate_list_items_16_597_end = rail.EmptyOperator(
            task_id='foreach_accumulate_list_items_16_597_end',
        )

        wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2600 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2600',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_user_dag_run_list_597").value | to_json }}'
        )

        if_request_currentprofilecount_equals_to_5_delegate_c3to_delegate_c36profiles_601 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_5_delegate_c3to_delegate_c36profiles_601',
            # pylint: disable=line-too-long
            test='''{{ dag_run.conf.currentprofilecount == 5  and dag_run.conf.memberof | matches('Delegate')  and dag_run.conf.memberof | matches('C3')  and dag_run.conf.memberof | matches('C4') | is_falsy }}''',
            yes_task="update_variable_602",
            no_task="if_request_currentprofilecount_equals_to_6_delegate_c3or_c4_c3to_delegate_c3_c47profiles_633",
        )

        update_variable_602 = rail.SetVariableOperator(
            task_id='update_variable_602',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=6
        )

        if_declare_variable_41_value_equals_to_6_603 = rail.IfOperator(
            task_id='if_declare_variable_41_value_equals_to_6_603',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_41')['name']) == 6,
            yes_task="log_loginnameforprimaryprofile_604",
            no_task="if_request_currentprofilecount_equals_to_6_delegate_c3or_c4_c3to_delegate_c3_c47profiles_633",
        )

        log_loginnameforprimaryprofile_604 = rail.PythonOperator(
            task_id='log_loginnameforprimaryprofile_604',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_user_from_list_by_type(
                'Federal Legislative', 'create_list_14')
        )

        log_useruriforprimaryprofile_605 = rail.PythonOperator(
            task_id='log_useruriforprimaryprofile_605',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_user_uri_from_list_by_type(
                'Federal Legislative', 'create_list_14')
        )

        if_log_useruriforprimaryprofile_605_present_606 = rail.IfOperator(
            task_id='if_log_useruriforprimaryprofile_605_present_606',
            test='''{{ result('log_useruriforprimaryprofile_605') | is_truthy }}''',
            yes_task="updateuserloginname_set_replicon_authentication_for_user_607",
            no_task="log_loginnameprimaryold_609",
        )

        updateuserloginname_set_replicon_authentication_for_user_607 = rail.RepliconServiceOperator(
            task_id='updateuserloginname_set_replicon_authentication_for_user_607',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ result('log_useruriforprimaryprofile_605') }}",
                "loginName": "{{ result('log_loginnameforprimaryprofile_604') }}fl",
                "password": "Replicon@12",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        update_email_removingemail_608 = rail.RepliconServiceOperator(
            task_id='update_email_removingemail_608',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('log_useruriforprimaryprofile_605') }}",
                "email": null
            }
        )

        log_loginnameprimaryold_609 = rail.PythonOperator(
            task_id='log_loginnameprimaryold_609',
            python_callable=lambda:  rail.result(
                'log_loginnameforprimaryprofile_604') + "d"
        )

        search_users_610 = rail.RepliconServicePageOperator(
            task_id="search_users_610",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                'page': 1,
                'pagesize': 10,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('log_loginnameprimaryold_609')
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, rail.result('log_loginnameprimaryold_609'))
        )

        if_search_users_610_users_less_than_1_611 = rail.IfOperator(
            task_id='if_search_users_610_users_less_than_1_611',
            test="{{result('search_users_610') | length == 0}}",
            yes_task="trigger_dag_run_live_nrdc_basic_add_update612",
            no_task="if_search_users_610_users_greater_than_0_613",
        )

        trigger_dag_run_live_nrdc_basic_add_update612 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update612',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Delegate",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_loginnameforprimaryprofile_604'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "Delegate",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": rail.result('log_useruri_primaryprofile_285'),
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "No Timesheet",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update612 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update612',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update612") }}'
        )

        gather_user_uri_612 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_uri_612',
            dag_runs="{{ result('trigger_dag_run_live_nrdc_basic_add_update612')}}",
            dagrun_task_id='log_useruri_71',
            flatten=True
        )

        if_search_users_610_users_greater_than_0_613 = rail.IfOperator(
            task_id='if_search_users_610_users_greater_than_0_613',
            test="{{result('search_users_610') | length > 0}}",
            yes_task="log_useruribasedonthesuffix_614",
            no_task="if_request_currentprofilecount_equals_to_6_delegate_c3or_c4_c3to_delegate_c3_c47profiles_633",
        )

        log_useruribasedonthesuffix_614 = rail.PythonOperator(
            task_id='log_useruribasedonthesuffix_614',
            # pylint: disable=line-too-long
            python_callable=lambda: get_user_uri_125(
                "log_loginnameprimaryold_609", "search_users_610")
        )

        if_log_useruribasedonthesuffix_614_present_615 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_614_present_615',
            test='''{{ result('log_useruribasedonthesuffix_614') | is_truthy }}''',
            yes_task="log_userstatusbasedonthesuffix_616",
            no_task="if_log_useruribasedonthesuffix_614_blank_623",
        )

        log_userstatusbasedonthesuffix_616 = rail.PythonOperator(
            task_id='log_userstatusbasedonthesuffix_616',
            # pylint: disable=line-too-long
            python_callable=lambda: get_user_status(
                "log_loginnameprimaryold_609", "search_users_610")
        )

        if_log_userstatusbasedonthesuffix_616_equals_to_false_617 = rail.IfOperator(
            task_id='if_log_userstatusbasedonthesuffix_616_equals_to_false_617',
            test='''{{ result('log_userstatusbasedonthesuffix_616') == 'False' }}''',
            yes_task="re_enable_userprofile_618",
            no_task="if_log_useruribasedonthesuffix_614_blank_623",
        )

        re_enable_userprofile_618 = rail.RepliconServiceOperator(
            task_id='re_enable_userprofile_618',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_614') }}"
            }
        )

        updatetoprimaryprofile_set_s_s_o_authentication_for_user_619 = rail.RepliconServiceOperator(
            task_id='updatetoprimaryprofile_set_s_s_o_authentication_for_user_619',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_614') }}",
                "loginName": "{{ result('log_loginnameforprimaryprofile_604') }}"
            }
        )

        update_emailaddingemail_620 = rail.RepliconServiceOperator(
            task_id='update_emailaddingemail_620',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_614') }}",
                "email": "{{ dag_run.conf.emailaddress }}"
            }
        )

        update_user_end_dateremoveenddate_621 = rail.RepliconServiceOperator(
            task_id='update_user_end_dateremoveenddate_621',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('log_useruribasedonthesuffix_614') }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": null
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile622 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile622',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Delegate",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": dag_run.conf['logonname'],
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('log_useruribasedonthesuffix_614'),
                "locationuri": dag_run.conf['locationuri'],
                "type": "Delegate",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile622 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile622',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile622") }}'
        )

        if_log_useruribasedonthesuffix_614_blank_623 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_614_blank_623',
            test='''{{ result('log_useruribasedonthesuffix_614') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update624",
            no_task="log_primaryprofileuri_625",
        )

        trigger_dag_run_live_nrdc_basic_add_update624 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update624',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Delegate",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_loginnameforprimaryprofile_604'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "Delegate",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "NA",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "No Timesheet",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update624 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update624',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update624") }}'
        )

        gather_user_uri_625 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_uri_625',
            dag_runs="{{ result('trigger_dag_run_live_nrdc_basic_add_update624')}}",
            dagrun_task_id='log_useruri_71',
            flatten=True
        )

        log_primaryprofileuri_625 = rail.PythonOperator(
            task_id='log_primaryprofileuri_625',
            # pylint: disable=line-too-long
            python_callable=lambda: get_newly_created_from_basic_add_task(
                'gather_user_uri_612', 'log_useruribasedonthesuffix_614', 'gather_user_uri_625')
        )

        declare_list_update_dag_runs_626 = rail.SetVariableOperator(
            task_id='declare_list_update_dag_runs_626',
            name='user_process_update_dag_runs_626',
            value=[]
        )

        foreach_accumulate_list_items_16_626 = rail.ForEachOperator(
            task_id='foreach_accumulate_list_items_16_626',
            items="{{ result('create_list_14') | to_json}}",
            start_task='get_all_substitute_user_assignments_for_user_627',
            end_task='foreach_accumulate_list_items_16_626_end'
        )

        get_all_substitute_user_assignments_for_user_627 = rail.RepliconServiceOperator(
            task_id='get_all_substitute_user_assignments_for_user_627',
            endpoint="/services/SubstituteUserAssignmentService1.svc/GetAllSubstituteUserAssignmentsForUser",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_626').uri }}"
            }
        )

        log_substituteuserassigned_628 = rail.PythonOperator(
            task_id='log_substituteuserassigned_628',
            # pylint: disable=line-too-long
            python_callable=lambda:  get_substitueUserUris(
                'log_primaryprofileuri_625', 'get_all_substitute_user_assignments_for_user_627')
        )

        if_log_substituteuserassigned_628_blank_629 = rail.IfOperator(
            task_id='if_log_substituteuserassigned_628_blank_629',
            test='''{{ result('log_substituteuserassigned_628') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_assign_substitute_usersv2632",
            no_task="foreach_accumulate_list_items_16_626_end",
        )

        trigger_dag_run_live_nrdc_assign_substitute_usersv2632 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_assign_substitute_usersv2632',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_assignsubstituteusersv2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "suburi": rail.result('log_primaryprofileuri_625'),
                "actualuri": rail.result('foreach_accumulate_list_items_16_626')['uri'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        insert_to_user_dag_run_list_632 = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list_632',
            append=True,
            name='{{ result("declare_list_update_dag_runs_322").name }}',
            # pylint: disable=line-too-long
            value='{{(result("declare_list_update_dag_runs_626"))[0]}}'
        )

        wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2632 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2632',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_user_dag_run_list_632").value | to_json }}'
        )

        foreach_accumulate_list_items_16_626_end = rail.EmptyOperator(
            task_id='foreach_accumulate_list_items_16_626_end',
        )

        if_request_currentprofilecount_equals_to_6_delegate_c3or_c4_c3to_delegate_c3_c47profiles_633 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_6_delegate_c3or_c4_c3to_delegate_c3_c47profiles_633',
            # pylint: disable=line-too-long
            test='''{{ dag_run.conf.currentprofilecount == 6  and dag_run.conf.memberof | matches('Delegate')  and dag_run.conf.memberof | matches('C4')  and dag_run.conf.memberof | matches('C3') }}''',
            yes_task="update_variable_634",
            no_task="if_request_currentprofilecount_equals_to_6_only_c4_c3_c4toonly_c41profiles_701",
        )

        update_variable_634 = rail.SetVariableOperator(
            task_id='update_variable_634',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=7
        )

        if_declare_variable_41_value_equals_to_7_635 = rail.IfOperator(
            task_id='if_declare_variable_41_value_equals_to_7_635',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_41')['name']) == 7,
            yes_task="log_primaryprofileloginnameif_c4isprimaryprofile_636",
            no_task="if_request_currentprofilecount_equals_to_6_only_c4_c3_c4toonly_c41profiles_701",
        )

        log_primaryprofileloginnameif_c4isprimaryprofile_636 = rail.PythonOperator(
            task_id='log_primaryprofileloginnameif_c4isprimaryprofile_636',
            python_callable=lambda:  get_user_from_list_by_type(
                'C4', 'create_list_14')
        )

        log_primaryprofileloginnameif_delegateisprimaryprofile_637 = rail.PythonOperator(
            task_id='log_primaryprofileloginnameif_delegateisprimaryprofile_637',
            python_callable=lambda:  get_user_from_list_by_type(
                'Delegate', 'create_list_14')
        )

        log_loginnamefromemailprimaryprofile_638 = rail.PythonOperator(
            task_id='log_loginnamefromemailprimaryprofile_638',
            python_callable=lambda:  rail.result('log_primaryprofileloginnameif_c4isprimaryprofile_636') or rail.result(
                'log_primaryprofileloginnameif_delegateisprimaryprofile_637')
        )

        log_userurimainprofile_639 = rail.PythonOperator(
            task_id='log_userurimainprofile_639',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'create_list_14'), 'userloginname', rail.result('log_loginnamefromemailprimaryprofile_638'), 'uri')
        )

        if_log_primaryprofileloginnameif_c4isprimaryprofile_636_present_whenc4isprimary_640 = rail.IfOperator(
            task_id='if_log_primaryprofileloginnameif_c4isprimaryprofile_636_present_whenc4isprimary_640',
            test='''{{ result('log_primaryprofileloginnameif_c4isprimaryprofile_636') | is_truthy }}''',
            yes_task="updateuserloginname_set_replicon_authentication_for_user_641",
            no_task="if_log_primaryprofileloginnameif_delegateisprimaryprofile_637_present_whendelegateisprimaryc4addedupdated_673",
        )

        updateuserloginname_set_replicon_authentication_for_user_641 = rail.RepliconServiceOperator(
            task_id='updateuserloginname_set_replicon_authentication_for_user_641',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ result('log_userurimainprofile_639') }}",
                "loginName": "{{ result('log_loginnamefromemailprimaryprofile_638') }}af",
                "password": "Replicon@12",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        update_email_removingemail_642 = rail.RepliconServiceOperator(
            task_id='update_email_removingemail_642',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('log_userurimainprofile_639') }}",
                "email": null
            }
        )

        log_loginnameprimaryoldd_643 = rail.PythonOperator(
            task_id='log_loginnameprimaryoldd_643',
            python_callable=lambda:  rail.result(
                'log_primaryprofileloginnameif_c4isprimaryprofile_636') + "d"
        )

        search_users_644 = rail.RepliconServicePageOperator(
            task_id="search_users_644",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('log_loginnameprimaryoldd_643')
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, rail.result('log_loginnameprimaryoldd_643'))
        )

        if_search_users_644_users_less_than_1_645 = rail.IfOperator(
            task_id='if_search_users_644_users_less_than_1_645',
            test="{{result('search_users_644') | length == 0}}",
            yes_task="trigger_dag_run_live_nrdc_basic_add_update646",
            no_task="if_search_users_644_users_greater_than_0_647",
        )

        trigger_dag_run_live_nrdc_basic_add_update646 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update646',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Delegate",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_loginnamefromemailprimaryprofile_638'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "Delegate",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "NA",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "NA",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update646 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update646',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update646") }}'
        )

        gather_user_uri_646 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_uri_646',
            dag_runs="{{ result('trigger_dag_run_live_nrdc_basic_add_update646')}}",
            dagrun_task_id='log_useruri_71',
            flatten=True
        )

        if_search_users_644_users_greater_than_0_647 = rail.IfOperator(
            task_id='if_search_users_644_users_greater_than_0_647',
            test="{{result('search_users_644') | length > 0}}",
            yes_task="log_useruribasedonthesuffix_648",
            no_task="log_newprimaryuseruri_658",
        )

        log_useruribasedonthesuffix_648 = rail.PythonOperator(
            task_id='log_useruribasedonthesuffix_648',
            python_callable=lambda:  get_user_uri_125(
                'log_primaryprofileloginnameif_c4isprimaryprofile_636', 'search_users_644')
        )

        if_log_useruribasedonthesuffix_648_present_649 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_648_present_649',
            test='''{{ result('log_useruribasedonthesuffix_648') | is_truthy }}''',
            yes_task="log_userstatusbasedonthesuffix_650",
            no_task="if_log_useruribasedonthesuffix_648_blank_656",
        )

        log_userstatusbasedonthesuffix_650 = rail.PythonOperator(
            task_id='log_userstatusbasedonthesuffix_650',
            python_callable=lambda:  get_user_status(
                'log_primaryprofileloginnameif_c4isprimaryprofile_636', 'search_users_644')
        )

        if_log_userstatusbasedonthesuffix_650_equals_to_false_651 = rail.IfOperator(
            task_id='if_log_userstatusbasedonthesuffix_650_equals_to_false_651',
            test='''{{ result('log_userstatusbasedonthesuffix_650') == 'False' }}''',
            yes_task="re_enable_userprofile_652",
            no_task="if_log_useruribasedonthesuffix_648_blank_656",
        )

        re_enable_userprofile_652 = rail.RepliconServiceOperator(
            task_id='re_enable_userprofile_652',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_648') }}"
            }
        )

        updatetoprimaryprofile_set_s_s_o_authentication_for_user_653 = rail.RepliconServiceOperator(
            task_id='updatetoprimaryprofile_set_s_s_o_authentication_for_user_653',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_648') }}",
                "loginName": "{{ result('log_primaryprofileloginnameif_c4isprimaryprofile_636') }}"
            }
        )

        update_emailaddingemail_654 = rail.RepliconServiceOperator(
            task_id='update_emailaddingemail_654',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_648') }}",
                "email": "{{ dag_run.conf.emailaddress }}"
            }
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile655 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile655',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Delegate",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": dag_run.conf['logonname'],
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('log_useruribasedonthesuffix_648'),
                "locationuri": dag_run.conf['locationuri'],
                "type": "Delegate",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile655 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile655',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile655") }}'
        )

        if_log_useruribasedonthesuffix_648_blank_656 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_648_blank_656',
            test='''{{ result('log_useruribasedonthesuffix_648') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update657",
            no_task="log_newprimaryuseruri_658",
        )

        trigger_dag_run_live_nrdc_basic_add_update657 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update657',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Delegate",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_loginnamefromemailprimaryprofile_638'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "Delegate",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": rail.result('log_useruri_primaryprofile_285'),
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "NA",
                "authtype": "sso",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update657 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update657',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update657") }}'
        )

        gather_user_uri_658 = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_uri_658',
            dag_runs="{{ result('trigger_dag_run_live_nrdc_basic_add_update657')}}",
            dagrun_task_id='log_useruri_71',
            flatten=True
        )

        log_newprimaryuseruri_658 = rail.PythonOperator(
            task_id='log_newprimaryuseruri_658',
            python_callable=lambda:  get_newly_created_from_basic_add_task(
                'gather_user_uri_658', 'log_useruribasedonthesuffix_648', 'gather_user_uri_646')
        )

        declare_list_update_dag_runs_666 = rail.SetVariableOperator(
            task_id='declare_list_update_dag_runs_666',
            name='user_process_update_dag_runs_666',
            value=[]
        )

        foreach_accumulate_list_items_16_666 = rail.ForEachOperator(
            task_id='foreach_accumulate_list_items_16_666',
            items="{{ result('create_list_14') | to_json}}",
            start_task='get_all_substitute_user_assignments_for_user_667',
            end_task='foreach_accumulate_list_items_16_666_end'
        )

        get_all_substitute_user_assignments_for_user_667 = rail.RepliconServiceOperator(
            task_id='get_all_substitute_user_assignments_for_user_667',
            endpoint="/services/SubstituteUserAssignmentService1.svc/GetAllSubstituteUserAssignmentsForUser",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_666').uri }}"
            }
        )

        log_substituteuserassigned_668 = rail.PythonOperator(
            task_id='log_substituteuserassigned_668',
            python_callable=lambda:  get_substitueUserUrisbyname(
                'log_loginnamefromemailprimaryprofile_638', 'get_all_substitute_user_assignments_for_user_667')
        )

        if_log_substituteuserassigned_668_blank_669 = rail.IfOperator(
            task_id='if_log_substituteuserassigned_668_blank_669',
            test='''{{ result('log_substituteuserassigned_668') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_assign_substitute_usersv2672",
            no_task="foreach_accumulate_list_items_16_666_end",
        )

        trigger_dag_run_live_nrdc_assign_substitute_usersv2672 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_assign_substitute_usersv2672',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_assignsubstituteusersv2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "suburi": rail.result('log_newprimaryuseruri_658'),
                "actualuri": rail.result('foreach_accumulate_list_items_16_666')['uri'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        insert_to_user_dag_run_list_666 = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list_666',
            append=True,
            name='{{ result("declare_list_update_dag_runs_666").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_nrdc_assign_substitute_usersv2672"))[0]}}'
        )

        wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2672 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2672',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_user_dag_run_list_666").value | to_json }}'
        )

        foreach_accumulate_list_items_16_666_end = rail.EmptyOperator(
            task_id='foreach_accumulate_list_items_16_666_end',
        )

        if_log_primaryprofileloginnameif_delegateisprimaryprofile_637_present_whendelegateisprimaryc4addedupdated_673 = rail.IfOperator(
            task_id='if_log_primaryprofileloginnameif_delegateisprimaryprofile_637_present_whendelegateisprimaryc4addedupdated_673',
            test='''{{ result('log_primaryprofileloginnameif_delegateisprimaryprofile_637') | is_truthy }}''',
            yes_task="log_loginnamewithaf_674",
            no_task="if_request_currentprofilecount_equals_to_6_only_c4_c3_c4toonly_c41profiles_701",
        )

        log_loginnamewithaf_674 = rail.PythonOperator(
            task_id='log_loginnamewithaf_674',
            python_callable=lambda:  rail.result(
                'log_primaryprofileloginnameif_delegateisprimaryprofile_637')+"af"
        )

        search_users_675 = rail.RepliconServicePageOperator(
            task_id="search_users_675",
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: {
                'page': 1,
                'pagesize': 100,
                'columnUris': [
                    'urn:replicon:user-list-column:user',
                    'urn:replicon:user-list-column:login-name',
                    'urn:replicon:user-list-column:employee-id',
                    'urn:replicon:user-list-column:enabled'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:user-list-filter:text'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': rail.result('log_loginnamewithaf_674')
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=lambda result: all_result_data_handler(
                result, rail.result('log_loginnamewithaf_674'))
        )

        if_search_users_675_users_less_than_1_676 = rail.IfOperator(
            task_id='if_search_users_675_users_less_than_1_676',
            test="{{result('search_users_675') | length == 0}}",
            yes_task="trigger_dag_run_live_nrdc_basic_add_update677",
            no_task="if_search_users_644_users_greater_than_0_678",
        )

        trigger_dag_run_live_nrdc_basic_add_update677 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update677',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_loginnamewithaf_674'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "C4",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": rail.result('log_userurimainprofile_639'),
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "C4 Timesheet",
                "authtype": "replicon",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update677 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update677',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update677") }}'
        )

        if_search_users_644_users_greater_than_0_678 = rail.IfOperator(
            task_id='if_search_users_644_users_greater_than_0_678',
            test="{{result('search_users_675') | length > 0}}",
            yes_task="log_useruribasedonthesuffix_679",
            no_task="if_request_currentprofilecount_equals_to_6_only_c4_c3_c4toonly_c41profiles_701",
        )

        log_useruribasedonthesuffix_679 = rail.PythonOperator(
            task_id='log_useruribasedonthesuffix_679',
            python_callable=lambda:  get_user_uri_125(
                'log_loginnamewithaf_674', 'search_users_675')
        )

        if_log_useruribasedonthesuffix_679_present_680 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_679_present_680',
            test='''{{ result('log_useruribasedonthesuffix_679') | is_truthy }}''',
            yes_task="log_userstatusbasedonthesuffix_681",
            no_task="if_log_useruribasedonthesuffix_679_blank_691",
        )

        log_userstatusbasedonthesuffix_681 = rail.PythonOperator(
            task_id='log_userstatusbasedonthesuffix_681',
            python_callable=lambda:  get_user_status(
                'log_loginnamewithaf_674', 'search_users_675')
        )

        if_log_userstatusbasedonthesuffix_681_equals_to_false_682 = rail.IfOperator(
            task_id='if_log_userstatusbasedonthesuffix_681_equals_to_false_682',
            test='''{{ result('log_userstatusbasedonthesuffix_681') == 'False' }}''',
            yes_task="re_enable_userprofile_683",
            no_task="if_log_useruribasedonthesuffix_679_blank_691",
        )

        re_enable_userprofile_683 = rail.RepliconServiceOperator(
            task_id='re_enable_userprofile_683',
            endpoint="/services/securityService1.svc/EnableLogin",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_679') }}"
            }
        )

        get_all_substitute_user_assignments_for_user_684 = rail.RepliconServiceOperator(
            task_id='get_all_substitute_user_assignments_for_user_684',
            endpoint="/services/SubstituteUserAssignmentService1.svc/GetAllSubstituteUserAssignmentsForUser",
            data={
                "userUri": "{{ result('log_useruribasedonthesuffix_679') }}"
            }
        )

        log_substituteuserassigned_685 = rail.PythonOperator(
            task_id='log_substituteuserassigned_685',
            python_callable=lambda: get_substitueUserUris(
                'log_userurimainprofile_639', 'get_all_substitute_user_assignments_for_user_684')
        )

        if_log_substituteuserassigned_685_blank_686 = rail.IfOperator(
            task_id='if_log_substituteuserassigned_685_blank_686',
            test='''{{ result('log_substituteuserassigned_685') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_assign_substitute_usersv2689",
            no_task="trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile690",
        )

        trigger_dag_run_live_nrdc_assign_substitute_usersv2689 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_assign_substitute_usersv2689',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_assignsubstituteusersv2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "suburi": rail.result('log_userurimainprofile_639'),
                "actualuri": rail.result('log_useruribasedonthesuffix_679'),
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2689 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2689',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_assign_substitute_usersv2689") }}'
        )

        trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile690 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile690',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_updaterehiredisableuserbasicprofile_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                "employeeid": dag_run.conf['employeeid'],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "logonname": dag_run.conf['logonname'],
                "accountstatus": dag_run.conf['accountstatus'],
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "useruri": rail.result('log_useruribasedonthesuffix_679'),
                "locationuri": dag_run.conf['locationuri'],
                "type": "C4",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile690 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile690',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile690") }}'
        )

        if_log_useruribasedonthesuffix_679_blank_691 = rail.IfOperator(
            task_id='if_log_useruribasedonthesuffix_679_blank_691',
            test='''{{ result('log_useruribasedonthesuffix_679') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_basic_add_update692",
            no_task="if_request_currentprofilecount_equals_to_6_only_c4_c3_c4toonly_c41profiles_701",
        )

        trigger_dag_run_live_nrdc_basic_add_update692 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_basic_add_update692',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_basicaddupdate_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "firstname": "Action Fund",
                "lastname": dag_run.conf['displayname'],
                "emailaddress": dag_run.conf['emailaddress'],
                # pylint: disable=line-too-long
                "empid": dag_run.conf['employeeid'] if dag_run.conf['employeeid'] and not '-' in dag_run.conf['employeeid'] else dag_run.conf['logonname'].split('@')[0],
                "empnumber": dag_run.conf['empnumber'],
                "whencreated": dag_run.conf['whencreated'],
                "office": dag_run.conf['office'],
                "loginname": rail.result('log_loginnamewithaf_674'),
                "department": dag_run.conf['department'],
                "memberof": dag_run.conf['memberof'],
                "manager": dag_run.conf['manager'],
                "title": dag_run.conf['title'],
                "type": "C4",
                "locationuri": dag_run.conf['locationuri'],
                "primaryuseruri": "NA",
                "parentjobid": get_dagrun_ecid(dag_run),
                "userfullname": dag_run.conf['firstname'] + " " + dag_run.conf['lastname'],
                "timesheettype": "NA",
                "authtype": "replicon",
                "status": dag_run.conf['accountstatus']
            }
        )

        wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update692 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update692',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_nrdc_basic_add_update692") }}'
        )

        if_request_currentprofilecount_equals_to_6_only_c4_c3_c4toonly_c41profiles_701 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_6_only_c4_c3_c4toonly_c41profiles_701',
            # pylint: disable=line-too-long
            test='''{{ dag_run.conf.currentprofilecount == 6  and dag_run.conf.memberof | matches('C4')  and dag_run.conf.memberof | matches('C3') | is_falsy }}''',
            yes_task="update_variable_702",
            no_task="if_request_currentprofilecount_equals_to_6_c3_delegatetoonly_delegate1profiles_707",
        )

        update_variable_702 = rail.SetVariableOperator(
            task_id='update_variable_702',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=1
        )

        if_declare_variable_41_value_equals_to_1_703 = rail.IfOperator(
            task_id='if_declare_variable_41_value_equals_to_1_703',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_41')['name']) == 1,
            yes_task="foreach_accumulate_list_items_16_704",
            no_task="if_request_currentprofilecount_equals_to_6_c3_delegatetoonly_delegate1profiles_707",
        )

        foreach_accumulate_list_items_16_704 = rail.ForEachOperator(
            task_id='foreach_accumulate_list_items_16_704',
            items="{{ result('create_list_14') | to_json}}",
            start_task='if_foreach_3157e122_704_type_not_equals_to_c4_705',
            end_task='foreach_accumulate_list_items_16_704_end'
        )

        if_foreach_3157e122_704_type_not_equals_to_c4_705 = rail.IfOperator(
            task_id='if_foreach_3157e122_704_type_not_equals_to_c4_705',
            test='''{{ result('foreach_accumulate_list_items_16_704').type != 'C4' }}''',
            yes_task="disable_loginoldprimaryprofile_706",
            no_task="foreach_accumulate_list_items_16_704_end",
        )

        disable_loginoldprimaryprofile_706 = rail.RepliconServiceOperator(
            task_id='disable_loginoldprimaryprofile_706',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_704').uri }}"
            }
        )

        foreach_accumulate_list_items_16_704_end = rail.EmptyOperator(
            task_id='foreach_accumulate_list_items_16_704_end',
        )

        if_request_currentprofilecount_equals_to_6_c3_delegatetoonly_delegate1profiles_707 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_6_c3_delegatetoonly_delegate1profiles_707',
            # pylint: disable=line-too-long
            test='''{{ dag_run.conf.currentprofilecount == 6  and dag_run.conf.memberof | matches('Delegate')  and dag_run.conf.memberof | matches('C3') | is_falsy }}''',
            yes_task="update_variable_708",
            no_task="if_request_currentprofilecount_equals_to_6_o_n_l_y_c3_c3_c4to_c35profiles_delegate_c3to_c3_713",
        )

        update_variable_708 = rail.SetVariableOperator(
            task_id='update_variable_708',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=1
        )

        if_declare_variable_41_value_equals_to_1_709 = rail.IfOperator(
            task_id='if_declare_variable_41_value_equals_to_1_709',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_41')['name']) == 1,
            yes_task="foreach_accumulate_list_items_16_710",
            no_task="if_request_currentprofilecount_equals_to_6_o_n_l_y_c3_c3_c4to_c35profiles_delegate_c3to_c3_713",
        )

        foreach_accumulate_list_items_16_710 = rail.ForEachOperator(
            task_id='foreach_accumulate_list_items_16_710',
            items="{{ result('create_list_14') | to_json}}",
            start_task='if_foreach_3157e122_710_type_not_equals_to_delegate_711',
            end_task='foreach_accumulate_list_items_16_710_end'
        )

        if_foreach_3157e122_710_type_not_equals_to_delegate_711 = rail.IfOperator(
            task_id='if_foreach_3157e122_710_type_not_equals_to_delegate_711',
            test='''{{ result('foreach_accumulate_list_items_16_710').type != 'Delegate' }}''',
            yes_task="disable_loginoldprimaryprofile_712",
            no_task="foreach_accumulate_list_items_16_710_end",
        )

        disable_loginoldprimaryprofile_712 = rail.RepliconServiceOperator(
            task_id='disable_loginoldprimaryprofile_712',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_710').uri }}"
            }
        )

        foreach_accumulate_list_items_16_710_end = rail.EmptyOperator(
            task_id='foreach_accumulate_list_items_16_710_end',
        )

        if_request_currentprofilecount_equals_to_6_o_n_l_y_c3_c3_c4to_c35profiles_delegate_c3to_c3_713 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_6_o_n_l_y_c3_c3_c4to_c35profiles_delegate_c3to_c3_713',
            # pylint: disable=line-too-long
            test='''{{ dag_run.conf.currentprofilecount == 6  and dag_run.conf.memberof | matches('C3')  and dag_run.conf.memberof | matches('C4') | is_falsy  and dag_run.conf.memberof | matches('Delegate') | is_falsy }}''',
            yes_task="update_variable_714",
            no_task="if_request_currentprofilecount_equals_to_7_c3_c4anddelegatetodelegate1profiles_742",
        )

        update_variable_714 = rail.SetVariableOperator(
            task_id='update_variable_714',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=5
        )

        if_declare_variable_41_value_equals_to_5_715 = rail.IfOperator(
            task_id='if_declare_variable_41_value_equals_to_5_715',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_41')['name']) == 5,
            yes_task="log_primaryprofileloginnameif_c4isprimaryprofile_716",
            no_task="if_request_currentprofilecount_equals_to_7_c3_c4anddelegatetodelegate1profiles_742",
        )

        log_primaryprofileloginnameif_c4isprimaryprofile_716 = rail.PythonOperator(
            task_id='log_primaryprofileloginnameif_c4isprimaryprofile_716',
            python_callable=lambda:  get_user_from_list_by_type(
                'C4', 'create_list_14')
        )

        log_primaryprofileloginnameif_delegateisprimaryprofile_717 = rail.PythonOperator(
            task_id='log_primaryprofileloginnameif_delegateisprimaryprofile_717',
            python_callable=lambda:  get_user_from_list_by_type(
                'Delegate', 'create_list_14')
        )

        log_requiredprimaryprofileloginname_718 = rail.PythonOperator(
            task_id='log_requiredprimaryprofileloginname_718',
            python_callable=lambda:  rail.result('log_primaryprofileloginnameif_c4isprimaryprofile_716') or rail.result(
                'log_primaryprofileloginnameif_delegateisprimaryprofile_717')
        )

        log_requiredprimaryprofile_uri_719 = rail.PythonOperator(
            task_id='log_requiredprimaryprofile_uri_719',
            python_callable=lambda:  get_user_uri_from_list(
                'log_requiredprimaryprofileloginname_718', 'create_list_14')
        )

        declare_list_update_dag_runs_720 = rail.SetVariableOperator(
            task_id='declare_list_update_dag_runs_720',
            name='user_process_update_dag_runs_720',
            value=[]
        )

        foreach_accumulate_list_items_16_720 = rail.ForEachOperator(
            task_id='foreach_accumulate_list_items_16_720',
            items="{{ result('create_list_14') | to_json}}",
            start_task='if_foreach_3157e122_720_type_equals_to_c4_721',
            end_task='foreach_accumulate_list_items_16_720_end'
        )

        if_foreach_3157e122_720_type_equals_to_c4_721 = rail.IfOperator(
            task_id='if_foreach_3157e122_720_type_equals_to_c4_721',
            test='''{{ result('foreach_accumulate_list_items_16_720').type == 'C4' }}''',
            yes_task="updateuserloginname_set_replicon_authentication_for_user_722",
            no_task="if_foreach_3157e122_720_type_equals_to_delegate_726",
        )

        updateuserloginname_set_replicon_authentication_for_user_722 = rail.RepliconServiceOperator(
            task_id='updateuserloginname_set_replicon_authentication_for_user_722',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_720').uri }}",
                "loginName": "{{ result('foreach_accumulate_list_items_16_720').userloginname }}af",
                "password": "Replicon@12",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        update_email_removingemail_723 = rail.RepliconServiceOperator(
            task_id='update_email_removingemail_723',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_720').uri }}",
                "email": null
            }
        )

        disable_loginoldprimaryprofile_724 = rail.RepliconServiceOperator(
            task_id='disable_loginoldprimaryprofile_724',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_720').uri }}"
            }
        )

        update_user_end_date_725 = rail.RepliconServiceOperator(
            task_id='update_user_end_date_725',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('foreach_accumulate_list_items_16_720').uri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": {
                                "year": "{{ result('log_todays_year_19') }}",
                                "month": "{{ result('log_todays_month_20') }}",
                                "day": "{{ result('log_todays_day_21') }}"
                            }
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        if_foreach_3157e122_720_type_equals_to_delegate_726 = rail.IfOperator(
            task_id='if_foreach_3157e122_720_type_equals_to_delegate_726',
            test='''{{ result('foreach_accumulate_list_items_16_720').type == 'Delegate' }}''',
            yes_task="updateuserloginname_set_replicon_authentication_for_user_727",
            no_task="if_foreach_3157e122_720_type_equals_to_federallegislative_731",
        )

        updateuserloginname_set_replicon_authentication_for_user_727 = rail.RepliconServiceOperator(
            task_id='updateuserloginname_set_replicon_authentication_for_user_727',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_720').uri }}",
                "loginName": "{{ result('foreach_accumulate_list_items_16_720').userloginname }}d",
                "password": "Replicon@12",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        update_email_removingemail_728 = rail.RepliconServiceOperator(
            task_id='update_email_removingemail_728',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_720').uri }}",
                "email": null
            }
        )

        disable_loginoldprimaryprofile_729 = rail.RepliconServiceOperator(
            task_id='disable_loginoldprimaryprofile_729',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_720').uri }}"
            }
        )

        update_user_end_date_730 = rail.RepliconServiceOperator(
            task_id='update_user_end_date_730',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('foreach_accumulate_list_items_16_720').uri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": {
                                "year": "{{ result('log_todays_year_19') }}",
                                "month": "{{ result('log_todays_month_20') }}",
                                "day": "{{ result('log_todays_day_21') }}"
                            }
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        if_foreach_3157e122_720_type_equals_to_federallegislative_731 = rail.IfOperator(
            task_id='if_foreach_3157e122_720_type_equals_to_federallegislative_731',
            test='''{{ result('foreach_accumulate_list_items_16_720').type == 'Federal Legislative' }}''',
            yes_task="log_primaryprofileuri_732",
            no_task="if_foreach_3157e122_720_type_not_equals_to_federallegislative_735",
        )

        log_primaryprofileuri_732 = rail.PythonOperator(
            task_id='log_primaryprofileuri_732',
            python_callable=lambda:  rail.result(
                'foreach_accumulate_list_items_16_720')['uri']
        )

        updatetoprimaryprofile_set_s_s_o_authentication_for_user_733 = rail.RepliconServiceOperator(
            task_id='updatetoprimaryprofile_set_s_s_o_authentication_for_user_733',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_720').uri }}",
                "loginName": "{{ result('log_requiredprimaryprofileloginname_718') }}"
            }
        )

        update_emailaddingemail_734 = rail.RepliconServiceOperator(
            task_id='update_emailaddingemail_734',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_720').uri }}",
                "email": "{{ dag_run.conf.emailaddress }}"
            }
        )

        if_foreach_3157e122_720_type_not_equals_to_federallegislative_735 = rail.IfOperator(
            task_id='if_foreach_3157e122_720_type_not_equals_to_federallegislative_735',
            # pylint: disable=line-too-long
            test='''{{ result('foreach_accumulate_list_items_16_720').type != 'Federal Legislative'  and result('foreach_accumulate_list_items_16_720').type != 'C4'  and result('foreach_accumulate_list_items_16_720').type != 'Delegate' }}''',
            yes_task="get_all_substitute_user_assignments_for_user_736",
            no_task="foreach_accumulate_list_items_16_720_end",
        )

        get_all_substitute_user_assignments_for_user_736 = rail.RepliconServiceOperator(
            task_id='get_all_substitute_user_assignments_for_user_736',
            endpoint="/services/SubstituteUserAssignmentService1.svc/GetAllSubstituteUserAssignmentsForUser",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_720').uri }}"
            }
        )

        log_substituteuserassigned_737 = rail.PythonOperator(
            task_id='log_substituteuserassigned_737',
            python_callable=lambda:  get_substitueUserUrisbyname(
                'log_requiredprimaryprofileloginname_718', 'get_all_substitute_user_assignments_for_user_736')
        )

        if_log_substituteuserassigned_737_blank_738 = rail.IfOperator(
            task_id='if_log_substituteuserassigned_737_blank_738',
            test='''{{ result('log_substituteuserassigned_737') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_assign_substitute_usersv2741",
            no_task="foreach_accumulate_list_items_16_720_end",
        )

        trigger_dag_run_live_nrdc_assign_substitute_usersv2741 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_assign_substitute_usersv2741',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_assignsubstituteusersv2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "suburi": rail.result('log_primaryprofileuri_732'),
                "actualuri": rail.result('foreach_accumulate_list_items_16_720')['uri'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        insert_to_user_dag_run_list_720 = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list_720',
            append=True,
            name='{{ result("declare_list_update_dag_runs_720").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_nrdc_assign_substitute_usersv2741"))[0]}}'
        )

        foreach_accumulate_list_items_16_720_end = rail.EmptyOperator(
            task_id='foreach_accumulate_list_items_16_720_end',
        )

        wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2741 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2741',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_user_dag_run_list_720").value | to_json }}'
        )

        if_request_currentprofilecount_equals_to_7_c3_c4anddelegatetodelegate1profiles_742 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_7_c3_c4anddelegatetodelegate1profiles_742',
            # pylint: disable=line-too-long
            test='''{{ dag_run.conf.currentprofilecount == 7  and dag_run.conf.memberof | matches('Delegate')  and dag_run.conf.memberof | matches('C3') | is_falsy  and dag_run.conf.memberof | matches('C4') | is_falsy }}''',
            yes_task="update_variable_743",
            no_task="if_request_currentprofilecount_equals_to_7_c3_c4and_delegateto_c35profiles_748",
        )

        update_variable_743 = rail.SetVariableOperator(
            task_id='update_variable_743',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=1
        )

        if_declare_variable_41_value_equals_to_1_744 = rail.IfOperator(
            task_id='if_declare_variable_41_value_equals_to_1_744',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_41')['name']) == 1,
            yes_task="foreach_accumulate_list_items_16_745",
            no_task="if_request_currentprofilecount_equals_to_7_c3_c4and_delegateto_c35profiles_748",
        )

        foreach_accumulate_list_items_16_745 = rail.ForEachOperator(
            task_id='foreach_accumulate_list_items_16_745',
            items="{{ result('create_list_14') | to_json}}",
            start_task='if_foreach_3157e122_745_type_not_equals_to_delegate_746',
            end_task='foreach_accumulate_list_items_16_745_end'
        )

        if_foreach_3157e122_745_type_not_equals_to_delegate_746 = rail.IfOperator(
            task_id='if_foreach_3157e122_745_type_not_equals_to_delegate_746',
            test='''{{ result('foreach_accumulate_list_items_16_745').type != 'Delegate' }}''',
            yes_task="disable_loginoldprimaryprofile_747",
            no_task="foreach_accumulate_list_items_16_745_end",
        )

        disable_loginoldprimaryprofile_747 = rail.RepliconServiceOperator(
            task_id='disable_loginoldprimaryprofile_747',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_745').uri }}"
            }
        )

        foreach_accumulate_list_items_16_745_end = rail.EmptyOperator(
            task_id='foreach_accumulate_list_items_16_745_end',
        )

        if_request_currentprofilecount_equals_to_7_c3_c4and_delegateto_c35profiles_748 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_7_c3_c4and_delegateto_c35profiles_748',
            # pylint: disable=line-too-long
            test='''{{ dag_run.conf.currentprofilecount == 7  and dag_run.conf.memberof | matches('C3')  and dag_run.conf.memberof | matches('C4') | is_falsy  and dag_run.conf.memberof | matches('Delegate') | is_falsy }}''',
            yes_task="update_variable_749",
            no_task="if_request_currentprofilecount_equals_to_7_c4c3and_delegateto_c3_c4profile6profiles_772",
        )

        update_variable_749 = rail.SetVariableOperator(
            task_id='update_variable_749',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=5
        )

        if_declare_variable_41_value_equals_to_5_750 = rail.IfOperator(
            task_id='if_declare_variable_41_value_equals_to_5_750',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_41')['name']) == 5,
            yes_task="log_primaryprofileloginnameif_delegateisprimaryprofile_751",
            no_task="if_request_currentprofilecount_equals_to_7_c4c3and_delegateto_c3_c4profile6profiles_772",
        )

        log_primaryprofileloginnameif_delegateisprimaryprofile_751 = rail.PythonOperator(
            task_id='log_primaryprofileloginnameif_delegateisprimaryprofile_751',
            python_callable=lambda:  get_user_from_list_by_type(
                'Delegate', 'create_list_14')
        )

        log_requiredprimaryprofile_uri_752 = rail.PythonOperator(
            task_id='log_requiredprimaryprofile_uri_752',
            python_callable=lambda:  get_user_uri_from_list(
                'log_primaryprofileloginnameif_delegateisprimaryprofile_751', 'create_list_14')
        )

        declare_list_update_dag_runs_753 = rail.SetVariableOperator(
            task_id='declare_list_update_dag_runs_753',
            name='user_process_update_dag_runs_753',
            value=[]
        )

        foreach_accumulate_list_items_16_753 = rail.ForEachOperator(
            task_id='foreach_accumulate_list_items_16_753',
            items="{{ result('create_list_14') | to_json}}",
            start_task='if_foreach_3157e122_753_type_equals_to_c4_754',
            end_task='foreach_accumulate_list_items_16_753_end'
        )

        if_foreach_3157e122_753_type_equals_to_c4_754 = rail.IfOperator(
            task_id='if_foreach_3157e122_753_type_equals_to_c4_754',
            test='''{{ result('foreach_accumulate_list_items_16_753').type == 'C4' }}''',
            yes_task="disable_loginoldprimaryprofile_755",
            no_task="if_foreach_3157e122_753_type_equals_to_delegate_756",
        )

        disable_loginoldprimaryprofile_755 = rail.RepliconServiceOperator(
            task_id='disable_loginoldprimaryprofile_755',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_753').uri }}"
            }
        )

        if_foreach_3157e122_753_type_equals_to_delegate_756 = rail.IfOperator(
            task_id='if_foreach_3157e122_753_type_equals_to_delegate_756',
            test='''{{ result('foreach_accumulate_list_items_16_753').type == 'Delegate' }}''',
            yes_task="updateuserloginname_set_replicon_authentication_for_user_757",
            no_task="if_foreach_3157e122_753_type_equals_to_federallegislative_761",
        )

        updateuserloginname_set_replicon_authentication_for_user_757 = rail.RepliconServiceOperator(
            task_id='updateuserloginname_set_replicon_authentication_for_user_757',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_753').uri }}",
                "loginName": "{{ result('foreach_accumulate_list_items_16_753').userloginname }}d",
                "password": "Replicon@12",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        update_email_removingemail_758 = rail.RepliconServiceOperator(
            task_id='update_email_removingemail_758',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_753').uri }}",
                "email": null
            }
        )

        disable_loginoldprimaryprofile_759 = rail.RepliconServiceOperator(
            task_id='disable_loginoldprimaryprofile_759',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_753').uri }}"
            }
        )

        update_user_end_date_760 = rail.RepliconServiceOperator(
            task_id='update_user_end_date_760',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('foreach_accumulate_list_items_16_753').uri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": {
                                "year": "{{ result('log_todays_year_19') }}",
                                "month": "{{ result('log_todays_month_20') }}",
                                "day": "{{ result('log_todays_day_21') }}"
                            }
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        if_foreach_3157e122_753_type_equals_to_federallegislative_761 = rail.IfOperator(
            task_id='if_foreach_3157e122_753_type_equals_to_federallegislative_761',
            test='''{{ result('foreach_accumulate_list_items_16_753').type == 'Federal Legislative' }}''',
            yes_task="log_primaryprofileuri_762",
            no_task="if_foreach_3157e122_753_type_not_equals_to_federallegislative_765",
        )

        log_primaryprofileuri_762 = rail.PythonOperator(
            task_id='log_primaryprofileuri_762',
            python_callable=lambda:  rail.result(
                'foreach_accumulate_list_items_16_753')['uri']
        )

        updatetoprimaryprofile_set_s_s_o_authentication_for_user_763 = rail.RepliconServiceOperator(
            task_id='updatetoprimaryprofile_set_s_s_o_authentication_for_user_763',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_753').uri }}",
                "loginName": "{{ result('log_primaryprofileloginnameif_delegateisprimaryprofile_751') }}"
            }
        )

        update_emailaddingemail_764 = rail.RepliconServiceOperator(
            task_id='update_emailaddingemail_764',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_753').uri }}",
                "email": "{{ dag_run.conf.logonname }}"
            }
        )

        if_foreach_3157e122_753_type_not_equals_to_federallegislative_765 = rail.IfOperator(
            task_id='if_foreach_3157e122_753_type_not_equals_to_federallegislative_765',
            # pylint: disable=line-too-long
            test='''{{ result('foreach_accumulate_list_items_16_753').type != 'Federal Legislative'  and result('foreach_accumulate_list_items_16_753').type != 'Delegate'  and result('foreach_accumulate_list_items_16_753').type != 'C4' }}''',
            yes_task="get_all_substitute_user_assignments_for_user_766",
            no_task="foreach_accumulate_list_items_16_753_end",
        )

        get_all_substitute_user_assignments_for_user_766 = rail.RepliconServiceOperator(
            task_id='get_all_substitute_user_assignments_for_user_766',
            endpoint="/services/SubstituteUserAssignmentService1.svc/GetAllSubstituteUserAssignmentsForUser",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_753').uri }}"
            }
        )

        log_substituteuserassigned_767 = rail.PythonOperator(
            task_id='log_substituteuserassigned_767',
            python_callable=lambda:  get_substitueUserUris(
                'log_primaryprofileuri_762', 'get_all_substitute_user_assignments_for_user_766')
        )

        if_log_substituteuserassigned_767_blank_768 = rail.IfOperator(
            task_id='if_log_substituteuserassigned_767_blank_768',
            test='''{{ result('log_substituteuserassigned_767') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_assign_substitute_usersv2771",
            no_task="foreach_accumulate_list_items_16_753_end",
        )

        trigger_dag_run_live_nrdc_assign_substitute_usersv2771 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_assign_substitute_usersv2771',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_assignsubstituteusersv2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "suburi": rail.result('log_primaryprofileuri_762'),
                "actualuri": rail.result('foreach_accumulate_list_items_16_753')['uri'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        insert_to_user_dag_run_list_753 = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list_753',
            append=True,
            name='{{ result("declare_list_update_dag_runs_753").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_nrdc_assign_substitute_usersv2771"))[0]}}'
        )

        wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2771 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2771',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_user_dag_run_list_753").value | to_json }}'
        )

        foreach_accumulate_list_items_16_753_end = rail.EmptyOperator(
            task_id='foreach_accumulate_list_items_16_753_end',
        )

        if_request_currentprofilecount_equals_to_7_c4c3and_delegateto_c3_c4profile6profiles_772 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_7_c4c3and_delegateto_c3_c4profile6profiles_772',
            # pylint: disable=line-too-long
            test='''{{ dag_run.conf.currentprofilecount == 7  and dag_run.conf.memberof | matches('C4')  and dag_run.conf.memberof | matches('Delegate') | is_falsy  and dag_run.conf.memberof | matches('C3') }}''',
            yes_task="update_variable_773",
            no_task="if_request_currentprofilecount_equals_to_7_c4c3and_delegateto_c4profile1profiles_796",
        )

        update_variable_773 = rail.SetVariableOperator(
            task_id='update_variable_773',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=6
        )

        if_declare_variable_41_value_equals_to_6_774 = rail.IfOperator(
            task_id='if_declare_variable_41_value_equals_to_6_774',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_41')['name']) == 6,
            yes_task="query_list_wherevalueis_delegate_775",
            no_task="if_request_currentprofilecount_equals_to_7_c4c3and_delegateto_c4profile1profiles_796",
        )

        query_list_wherevalueis_delegate_775 = rail.QueryCollectionOperator(
            task_id='query_list_wherevalueis_delegate_775',
            query="""SELECT * FROM  existinguserdata WHERE  existinguserdata.type = 'Delegate'""",
        )

        get_first_records_from_query_775 = rail.PythonOperator(
            task_id='get_first_records_from_query_775',
            python_callable=lambda:  get_first_user_query(
                'query_list_wherevalueis_delegate_775')
        )

        if_first_type_present_delegate_776 = rail.IfOperator(
            task_id='if_first_type_present_delegate_776',
            test='''{{ result('get_first_records_from_query_775').type == 'Delegate' | is_truthy }}''',
            yes_task="updateuserloginname_set_replicon_authentication_for_user_777",
            no_task="log_primaryprofileloginnameifdelegateisprimaryprofile_781",
        )

        updateuserloginname_set_replicon_authentication_for_user_777 = rail.RepliconServiceOperator(
            task_id='updateuserloginname_set_replicon_authentication_for_user_777',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ result('get_first_records_from_query_775').uri }}",
                "loginName": "{{ result('get_first_records_from_query_775').userloginname }}d",
                "password": "Replicon@12",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        update_email_removingemail_778 = rail.RepliconServiceOperator(
            task_id='update_email_removingemail_778',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('get_first_records_from_query_775').uri }}",
                "email": null
            }
        )

        disable_loginoldprimaryprofile_779 = rail.RepliconServiceOperator(
            task_id='disable_loginoldprimaryprofile_779',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ result('get_first_records_from_query_775').uri }}"
            }
        )

        update_user_end_date_780 = rail.RepliconServiceOperator(
            task_id='update_user_end_date_780',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('get_first_records_from_query_775').uri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": {
                                "year": "{{ result('log_todays_year_19') }}",
                                "month": "{{ result('log_todays_month_20') }}",
                                "day": "{{ result('log_todays_day_21') }}"
                            }
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        log_primaryprofileloginnameifdelegateisprimaryprofile_781 = rail.PythonOperator(
            task_id='log_primaryprofileloginnameifdelegateisprimaryprofile_781',
            python_callable=lambda:  get_user_from_list_by_type(
                'Delegate', 'create_list_14')
        )

        log_requiredprimaryprofile_uri_782 = rail.PythonOperator(
            task_id='log_requiredprimaryprofile_uri_782',
            python_callable=lambda:  get_user_uri_from_list_by_type(
                'Delegate', 'create_list_14')
        )

        query_list_wherevalueisc4_783 = rail.QueryCollectionOperator(
            task_id='query_list_wherevalueisc4_783',
            query="""SELECT * FROM  existinguserdata WHERE  existinguserdata.type = 'C4'""",
        )

        get_first_records_from_query_783 = rail.PythonOperator(
            task_id='get_first_records_from_query_783',
            python_callable=lambda:  get_first_user_query(
                'query_list_wherevalueisc4_783')
        )

        if_first_type_present_c4_784 = rail.IfOperator(
            task_id='if_first_type_present_c4_784',
            test="{{ result('get_first_records_from_query_783').type | is_truthy }}",
            yes_task="log_newprimaryprofileuri_785",
            no_task="declare_list_update_dag_runs_788",
        )

        log_newprimaryprofileuri_785 = rail.PythonOperator(
            task_id='log_newprimaryprofileuri_785',
            python_callable=lambda:  rail.result(
                'get_first_records_from_query_783')['uri']
        )

        updatetoprimaryprofile_set_s_s_o_authentication_for_user_786 = rail.RepliconServiceOperator(
            task_id='updatetoprimaryprofile_set_s_s_o_authentication_for_user_786',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('get_first_records_from_query_783').uri }}",
                "loginName": "{{ result('log_primaryprofileloginnameifdelegateisprimaryprofile_781') }}"
            }
        )

        update_emailaddemail_787 = rail.RepliconServiceOperator(
            task_id='update_emailaddemail_787',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('get_first_records_from_query_783').uri }}",
                "email": "{{ dag_run.conf.emailaddress }}"
            }
        )

        declare_list_update_dag_runs_788 = rail.SetVariableOperator(
            task_id='declare_list_update_dag_runs_788',
            name='user_process_update_dag_runs_788',
            value=[]
        )

        foreach_accumulate_list_items_16_788 = rail.ForEachOperator(
            task_id='foreach_accumulate_list_items_16_788',
            items="{{ result('create_list_14') | to_json}}",
            start_task='if_foreach_3157e122_788_type_not_equals_to_c4_789',
            end_task='foreach_accumulate_list_items_16_788_end'
        )

        if_foreach_3157e122_788_type_not_equals_to_c4_789 = rail.IfOperator(
            task_id='if_foreach_3157e122_788_type_not_equals_to_c4_789',
            test='''{{ result('foreach_accumulate_list_items_16_788').type != 'C4'  and result('foreach_accumulate_list_items_16_788').type != 'Delegate' }}''',
            yes_task="get_all_substitute_user_assignments_for_user_790",
            no_task="foreach_accumulate_list_items_16_788_end",
        )

        get_all_substitute_user_assignments_for_user_790 = rail.RepliconServiceOperator(
            task_id='get_all_substitute_user_assignments_for_user_790',
            endpoint="/services/SubstituteUserAssignmentService1.svc/GetAllSubstituteUserAssignmentsForUser",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_788').uri }}"
            }
        )

        log_substituteuserassigned_791 = rail.PythonOperator(
            task_id='log_substituteuserassigned_791',
            python_callable=lambda:  get_substitueUserUris(
                'log_newprimaryprofileuri_785', 'get_all_substitute_user_assignments_for_user_790')
        )

        if_log_substituteuserassigned_791_blank_792 = rail.IfOperator(
            task_id='if_log_substituteuserassigned_791_blank_792',
            test='''{{ result('log_substituteuserassigned_791') | is_falsy }}''',
            yes_task="trigger_dag_run_live_nrdc_assign_substitute_usersv2795",
            no_task="foreach_accumulate_list_items_16_788_end",
        )

        trigger_dag_run_live_nrdc_assign_substitute_usersv2795 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_assign_substitute_usersv2795',
            retries=0,
            items=[1],
            trigger_dag_id=f'nrdc_assignsubstituteusersv2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run: {
                "suburi": rail.result('log_newprimaryprofileuri_785'),
                "actualuri": rail.result('foreach_accumulate_list_items_16_788')['uri'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        insert_to_user_dag_run_list_788 = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list_788',
            append=True,
            name='{{ result("declare_list_update_dag_runs_788").name }}',
            value='{{(result("trigger_dag_run_live_nrdc_assign_substitute_usersv2795"))[0]}}'
        )

        foreach_accumulate_list_items_16_788_end = rail.EmptyOperator(
            task_id='foreach_accumulate_list_items_16_788_end',
        )

        wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2795 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2795',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_user_dag_run_list_788").value | to_json }}'
        )

        if_request_currentprofilecount_equals_to_7_c4c3and_delegateto_c4profile1profiles_796 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_7_c4c3and_delegateto_c4profile1profiles_796',
            # pylint: disable=line-too-long
            test='''{{ dag_run.conf.currentprofilecount == 7  and dag_run.conf.memberof | matches('C4')  and dag_run.conf.memberof | matches('Delegate') | is_falsy  and dag_run.conf.memberof | matches('C3') | is_falsy }}''',
            yes_task="update_variable_797",
            no_task="if_request_currentprofilecount_equals_to_7_delegate_c4_c3to_delegate_c3profiles6profiles_814",
        )

        update_variable_797 = rail.SetVariableOperator(
            task_id='update_variable_797',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=1
        )

        if_declare_variable_41_value_equals_to_1_798 = rail.IfOperator(
            task_id='if_declare_variable_41_value_equals_to_1_798',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_41')['name']) == 1,
            yes_task="query_list_wherevalueis_delegate_799",
            no_task="if_request_currentprofilecount_equals_to_7_delegate_c4_c3to_delegate_c3profiles6profiles_814",
        )

        query_list_wherevalueis_delegate_799 = rail.QueryCollectionOperator(
            task_id='query_list_wherevalueis_delegate_799',
            query="""SELECT * FROM  existinguserdata WHERE  existinguserdata.type = 'Delegate'""",
        )

        get_first_records_from_query_799 = rail.PythonOperator(
            task_id='get_first_records_from_query_799',
            python_callable=lambda:  get_first_user_query(
                'query_list_wherevalueis_delegate_799')
        )

        if_first_type_present_delegate_800 = rail.IfOperator(
            task_id='if_first_type_present_delegate_800',
            test="{{ result('get_first_records_from_query_799').type | is_truthy }}",
            yes_task="updateuserloginname_set_replicon_authentication_for_user_801",
            no_task="log_primaryprofileloginnameifdelegateisprimaryprofile_805",
        )

        updateuserloginname_set_replicon_authentication_for_user_801 = rail.RepliconServiceOperator(
            task_id='updateuserloginname_set_replicon_authentication_for_user_801',
            endpoint="/services/securityservice1.svc/SetRepliconAuthenticationForUser",
            data={
                "userUri": "{{ result('get_first_records_from_query_799').uri }}",
                "loginName": "{{ result('get_first_records_from_query_799').userloginname }}d",
                "password": "Replicon@12",
                "forcePasswordChangeOnLoginOption": "urn:replicon:force-password-change-on-login:enable"
            }
        )

        update_email_removingemail_802 = rail.RepliconServiceOperator(
            task_id='update_email_removingemail_802',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('get_first_records_from_query_799').uri }}",
                "email": null
            }
        )

        disable_loginoldprimaryprofile_803 = rail.RepliconServiceOperator(
            task_id='disable_loginoldprimaryprofile_803',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ result('get_first_records_from_query_799').uri }}"
            }
        )

        update_user_end_date_804 = rail.RepliconServiceOperator(
            task_id='update_user_end_date_804',
            endpoint="/services/ImportService1.svc/ApplyUserModifications",
            data={
                "user": {
                    "uri": "{{ result('get_first_records_from_query_799').uri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "timezoneToApply": null,
                    "workWeekStartToApply": null,
                    "holidayCalendarToApply": null,
                    "schedulePolicyToApply": null,
                    "locationScheduleToApply": null,
                    "divisionScheduleToApply": null,
                    "costCenterScheduleToApply": null,
                    "serviceCenterScheduleToApply": null,
                    "permissionSetsToApply": null,
                    "policySetsToApply": null,
                    "policyDataAccessScopesToApply": null,
                    "policyDataAccessScopesToApply2": null,
                    "notificationPreferencesToApply": null,
                    "timesheetPeriodTypeToApply": null,
                    "timesheetApprovalPathToApply": null,
                    "validationRuleToApply": null,
                    "activitiesToApply": [],
                    "activitiesToApply2": null,
                    "defaultActivityToApply": null,
                    "defaultActivityToApply2": null,
                    "expenseApprovalPathToApply": null,
                    "timeOffApprovalPathToApply": null,
                    "productAssignmentsToApply": null,
                    "timeBankPolicyToApply": null,
                    "securitySettingsToApply": null,
                    "supervisorsToApply": null,
                    "supervisorsModifications": null,
                    "payrollRatesToApply": null,
                    "payrollRatesModifications": null,
                    "overtimeRulesToApply": null,
                    "overtimeRulesModifications": null,
                    "customFieldValuesToApply": [],
                    "departmentToApply": null,
                    "employeeTypeToApply": null,
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": {
                                "year": "{{ result('log_todays_year_19') }}",
                                "month": "{{ result('log_todays_month_20') }}",
                                "day": "{{ result('log_todays_day_21') }}"
                            }
                        },
                        "employeeId": null
                    },
                    "payRulesToApply": null,
                    "payRatesModifications": null,
                    "placeAssignmentsModifications": null
                }
            }
        )

        log_primaryprofileloginnameifdelegateisprimaryprofile_805 = rail.PythonOperator(
            task_id='log_primaryprofileloginnameifdelegateisprimaryprofile_805',
            python_callable=lambda:  get_user_from_list_by_type(
                'Delegate', 'create_list_14')
        )

        log_requiredprimaryprofile_uri_806 = rail.PythonOperator(
            task_id='log_requiredprimaryprofile_uri_806',
            python_callable=lambda:  get_user_uri_from_list_by_type(
                'Delegate', 'create_list_14')
        )

        query_list_wherevalueisc4_807 = rail.QueryCollectionOperator(
            task_id='query_list_wherevalueisc4_807',
            query="""SELECT * FROM  existinguserdata WHERE  existinguserdata.type = 'C4'""",
        )

        get_first_records_from_query_807 = rail.PythonOperator(
            task_id='get_first_records_from_query_807',
            python_callable=lambda:  get_first_user_query(
                'query_list_wherevalueisc4_807')
        )

        def is_c4_present():
            c4_query_collection = rail.result('query_list_wherevalueisc4_783')
            c4_query = get_data_from_document(
                c4_query_collection) if c4_query_collection else []
            return len(c4_query) > 0

        if_first_type_present_c4_808 = rail.IfOperator(
            task_id='if_first_type_present_c4_808',
            test=is_c4_present,
            yes_task="updatetoprimaryprofile_set_s_s_o_authentication_for_user_809",
            no_task="foreach_accumulate_list_items_16_811",
        )

        updatetoprimaryprofile_set_s_s_o_authentication_for_user_809 = rail.RepliconServiceOperator(
            task_id='updatetoprimaryprofile_set_s_s_o_authentication_for_user_809',
            endpoint="/services/SecurityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ result('get_first_records_from_query_807').uri }}",
                "loginName": "{{ result('log_primaryprofileloginnameifdelegateisprimaryprofile_805') }}"
            }
        )

        update_emailaddemail_810 = rail.RepliconServiceOperator(
            task_id='update_emailaddemail_810',
            endpoint="/services/UserService1.svc/UpdateEmail",
            data={
                "userUri": "{{ result('get_first_records_from_query_807').uri }}",
                "email": "{{ dag_run.conf.logonname }}"
            }
        )

        foreach_accumulate_list_items_16_811 = rail.ForEachOperator(
            task_id='foreach_accumulate_list_items_16_811',
            items="{{ result('create_list_14') | to_json}}",
            start_task='if_foreach_3157e122_811_type_not_equals_to_c4_812',
            end_task='foreach_accumulate_list_items_16_811_end'
        )

        if_foreach_3157e122_811_type_not_equals_to_c4_812 = rail.IfOperator(
            task_id='if_foreach_3157e122_811_type_not_equals_to_c4_812',
            test='''{{ result('foreach_accumulate_list_items_16_811').type != 'C4'  and result('foreach_accumulate_list_items_16_811').type != 'Delegate' }}''',
            yes_task="disable_loginoldprimaryprofile_813",
            no_task="foreach_accumulate_list_items_16_811_end",
        )

        disable_loginoldprimaryprofile_813 = rail.RepliconServiceOperator(
            task_id='disable_loginoldprimaryprofile_813',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_811').uri }}"
            }
        )

        foreach_accumulate_list_items_16_811_end = rail.EmptyOperator(
            task_id='foreach_accumulate_list_items_16_811_end',
        )

        if_request_currentprofilecount_equals_to_7_delegate_c4_c3to_delegate_c3profiles6profiles_814 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_7_delegate_c4_c3to_delegate_c3profiles6profiles_814',
            # pylint: disable=line-too-long
            test='''{{ dag_run.conf.currentprofilecount == 7  and dag_run.conf.memberof | matches('Delegate')  and dag_run.conf.memberof | matches('C4') | is_falsy  and dag_run.conf.memberof | matches('C3') }}''',
            yes_task="update_variable_815",
            no_task="if_request_currentprofilecount_equals_to_7_c3_c4anddelegatetodelegateandc42profiles_820",
        )

        update_variable_815 = rail.SetVariableOperator(
            task_id='update_variable_815',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=6
        )

        if_declare_variable_41_value_equals_to_6_816 = rail.IfOperator(
            task_id='if_declare_variable_41_value_equals_to_6_816',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_41')['name']) == 6,
            yes_task="foreach_accumulate_list_items_16_817",
            no_task="if_request_currentprofilecount_equals_to_7_c3_c4anddelegatetodelegateandc42profiles_820",
        )

        foreach_accumulate_list_items_16_817 = rail.ForEachOperator(
            task_id='foreach_accumulate_list_items_16_817',
            items="{{ result('create_list_14') | to_json}}",
            start_task='if_foreach_3157e122_817_type_equals_to_c4_818',
            end_task='foreach_accumulate_list_items_16_817_end'
        )

        if_foreach_3157e122_817_type_equals_to_c4_818 = rail.IfOperator(
            task_id='if_foreach_3157e122_817_type_equals_to_c4_818',
            test='''{{ result('foreach_accumulate_list_items_16_817').type == 'C4' }}''',
            yes_task="disable_loginoldprimaryprofile_819",
            no_task="foreach_accumulate_list_items_16_817_end",
        )

        disable_loginoldprimaryprofile_819 = rail.RepliconServiceOperator(
            task_id='disable_loginoldprimaryprofile_819',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_817').uri }}"
            }
        )

        foreach_accumulate_list_items_16_817_end = rail.EmptyOperator(
            task_id='foreach_accumulate_list_items_16_817_end',
        )

        if_request_currentprofilecount_equals_to_7_c3_c4anddelegatetodelegateandc42profiles_820 = rail.IfOperator(
            task_id='if_request_currentprofilecount_equals_to_7_c3_c4anddelegatetodelegateandc42profiles_820',
            # pylint: disable=line-too-long
            test='''{{ dag_run.conf.currentprofilecount == 7  and dag_run.conf.memberof | matches('Delegate')  and dag_run.conf.memberof | matches('C3') | is_falsy  and dag_run.conf.memberof | matches('C4') }}''',
            yes_task="update_variable_821",
            no_task="log_to_sumo",
        )

        update_variable_821 = rail.SetVariableOperator(
            task_id='update_variable_821',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=2
        )

        if_declare_variable_41_value_equals_to_2_822 = rail.IfOperator(
            task_id='if_declare_variable_41_value_equals_to_2_822',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_41')['name']) == 2,
            yes_task="foreach_accumulate_list_items_16_823",
            no_task="log_to_sumo",
        )

        foreach_accumulate_list_items_16_823 = rail.ForEachOperator(
            task_id='foreach_accumulate_list_items_16_823',
            items="{{ result('create_list_14') | to_json}}",
            start_task='if_foreach_3157e122_823_type_not_equals_to_delegate_824',
            end_task='foreach_accumulate_list_items_16_823_end'
        )

        if_foreach_3157e122_823_type_not_equals_to_delegate_824 = rail.IfOperator(
            task_id='if_foreach_3157e122_823_type_not_equals_to_delegate_824',
            test='''{{ result('foreach_accumulate_list_items_16_823').type != 'Delegate'  and result('foreach_accumulate_list_items_16_823').type != 'C4' }}''',
            yes_task="disable_loginoldprimaryprofile_825",
            no_task="foreach_accumulate_list_items_16_823_end",
        )

        disable_loginoldprimaryprofile_825 = rail.RepliconServiceOperator(
            task_id='disable_loginoldprimaryprofile_825',
            endpoint="/services/securityservice1.svc/DisableLogin",
            data={
                "userUri": "{{ result('foreach_accumulate_list_items_16_823').uri }}"
            }
        )

        foreach_accumulate_list_items_16_823_end = rail.EmptyOperator(
            task_id='foreach_accumulate_list_items_16_823_end',
        )

        # nrdc_user_import_logs_add_entry_827 = rail.WriteLogOperator(
        #     task_id='nrdc_user_import_logs_add_entry_827',
        #     # log="{{ fixme result('create_log') }}",
        #     message="fixme get message from prop ",
        #     severity="fixme get severity from prop ",
        #     properties={
        #         "user": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname | {{ dag_run.conf.emailaddress }}",
        #         "action": "Update User",
        #         "status": "Error",
        #         "details": "User Not Updated #{_('data.catch.catch_826') }}|{{ dag_run_ecid() }}"
        #     }
        # )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> declare_variable_3
        declare_variable_3 >> if_request_memberof_not_contains_c4_4
        if_request_memberof_not_contains_c4_4 >> rail.Label(
            'Yes') >> update_variable_5 >> log_accountstatus_6
        if_request_memberof_not_contains_c4_4 >> rail.Label(
            'No') >> log_accountstatus_6 >> if_request_department_blank_7
        if_request_department_blank_7 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_8 >> stop_9 >> log_to_sumo
        if_request_department_blank_7 >> rail.Label(
            'No') >> log_toidentify_c4or_delegateprimaryprofileexistingusers_10 >> \
            create_list_14 >> create_list_17 >> log_todayin_m_m_d_d_y_y_y_y_18 >> \
            log_todays_year_19 >> log_todays_month_20 >> log_todays_day_21 >> get_all_policy_sets_22 >> \
            get_all_custom_fields_23 >> \
            log_type_u_ri_24 >> if_request_currentprofilecount_equals_to_1_25
        if_request_currentprofilecount_equals_to_1_25 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile26 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile26 >> \
            declare_rehire_list_dag_runs >> if_request_currentprofilecount_equals_to_2_27
        if_request_currentprofilecount_equals_to_1_25 >> rail.Label(
            'No') >> declare_rehire_list_dag_runs
        if_request_currentprofilecount_equals_to_2_27 >> rail.Label(
            'Yes') >> foreach_accumulate_list_items_16_28 >> \
            trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile29 >> \
            insert_to_rehire_user_dag_run_list_2 >> foreach_accumulate_list_items_16_28_end
        foreach_accumulate_list_items_16_28 >> foreach_accumulate_list_items_16_28_end >> \
            wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile29 >> \
            if_request_currentprofilecount_equals_to_5_30
        if_request_currentprofilecount_equals_to_2_27 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_5_30
        if_request_currentprofilecount_equals_to_5_30 >> rail.Label(
            'Yes') >> foreach_accumulate_list_items_16_31 >> trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile32 >> \
            insert_to_rehire_user_dag_run_list_5 >> foreach_accumulate_list_items_16_31_end
        foreach_accumulate_list_items_16_31 >> foreach_accumulate_list_items_16_31_end >> \
            wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile32 >> if_request_currentprofilecount_equals_to_6_33
        if_request_currentprofilecount_equals_to_5_30 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_6_33
        if_request_currentprofilecount_equals_to_6_33 >> rail.Label(
            'Yes') >> foreach_accumulate_list_items_16_34 >> trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile35 >>\
            insert_to_rehire_user_dag_run_list_6 >> foreach_accumulate_list_items_16_34_end
        foreach_accumulate_list_items_16_34 >> foreach_accumulate_list_items_16_34_end >> \
            wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile35 >> if_request_currentprofilecount_equals_to_7_36
        if_request_currentprofilecount_equals_to_6_33 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_7_36
        if_request_currentprofilecount_equals_to_7_36 >> rail.Label(
            'Yes') >> foreach_accumulate_list_items_16_37 >> trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile38 >> \
            insert_to_rehire_user_dag_run_list_7 >> foreach_accumulate_list_items_16_37_end
        foreach_accumulate_list_items_16_37 >> foreach_accumulate_list_items_16_37_end >> \
            wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile38 >> if_request_accountstatus_equals_to_disabled_39
        if_request_currentprofilecount_equals_to_7_36 >> rail.Label(
            'No') >> if_request_accountstatus_equals_to_disabled_39
        if_request_accountstatus_equals_to_disabled_39 >> rail.Label(
            'Yes') >> stop_40
        if_request_accountstatus_equals_to_disabled_39 >> rail.Label(
            'No') >> declare_variable_41 >> if_request_currentprofilecount_equals_to_1_c4to_delegateor_delegateto_c41profiles_42
        if_request_currentprofilecount_equals_to_1_c4to_delegateor_delegateto_c41profiles_42 >> rail.Label(
            'Yes') >> update_variable_43 >> if_request_memberof_contains_c4_44
        if_request_memberof_contains_c4_44 >> rail.Label(
            'Yes') >> update_variable_45 >> if_declare_variable_41_value_equals_to_1_46
        if_request_memberof_contains_c4_44 >> rail.Label(
            'No') >> if_declare_variable_41_value_equals_to_1_46
        if_declare_variable_41_value_equals_to_1_46 >> rail.Label(
            'Yes') >> query_list_whereexistingprimaryprofilevalueis_c4or_delegate_47 >> declare_variable_48 >> profilevalueis_c4or_delegate_47 >> \
            if_request_memberof_contains_c4_delegateto_c4_49
        if_request_memberof_contains_c4_delegateto_c4_49 >> rail.Label(
            'Yes') >> updateuserloginname_set_replicon_authentication_for_user_50 >> update_email_removingemail_51 >> \
            disable_loginoldprimaryprofile_52 >> update_user_end_date_53 >> log_loginnamewithaf_54 >> search_users_55 >> \
            if_search_users_55_users_less_than_1_56
        if_search_users_55_users_less_than_1_56 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update57 >> wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update57 >> \
            if_request_memberof_contains_delegate_c4to_delegate_69
        if_search_users_55_users_less_than_1_56 >> rail.Label(
            'No') >> if_search_users_55_users_greater_than_0_58
        if_search_users_55_users_greater_than_0_58 >> rail.Label(
            'Yes') >> log_useruribasedonthesuffix_59 >> if_log_useruribasedonthesuffix_59_present_60
        if_log_useruribasedonthesuffix_59_present_60 >> rail.Label(
            'Yes') >> log_userstatusbasedonthesuffix_61 >> if_log_userstatusbasedonthesuffix_61_equals_to_false_62
        if_log_userstatusbasedonthesuffix_61_equals_to_false_62 >> rail.Label(
            'Yes') >> re_enable_userprofile_63 >> removeenddate_64 >> updatetoprimaryprofile_set_s_s_o_authentication_for_user_65 >> \
            trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile66 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile66 >> \
            if_search_users_55_users_blank_67
        if_log_userstatusbasedonthesuffix_61_equals_to_false_62 >> rail.Label(
            'No') >> if_search_users_55_users_blank_67
        if_log_useruribasedonthesuffix_59_present_60 >> rail.Label(
            'No') >> if_search_users_55_users_blank_67
        if_search_users_55_users_blank_67 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update68 >> wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update68 >> \
            if_request_memberof_contains_delegate_c4to_delegate_69
        if_search_users_55_users_blank_67 >> rail.Label(
            'No') >> if_request_memberof_contains_delegate_c4to_delegate_69
        if_search_users_55_users_greater_than_0_58 >> rail.Label(
            'No') >> if_request_memberof_contains_delegate_c4to_delegate_69
        if_request_memberof_contains_c4_delegateto_c4_49 >> rail.Label(
            'No') >> if_request_memberof_contains_delegate_c4to_delegate_69
        if_request_memberof_contains_delegate_c4to_delegate_69 >> rail.Label(
            'Yes') >> updateuserloginname_set_replicon_authentication_for_user_70 >> update_email_removingemail_71 >> \
            disable_loginoldprimaryprofile_72 >> update_user_end_date_73 >> log_loginnamewithd_74 >> search_users_75 >> \
            if_search_users_75_users_less_than_1_76
        if_search_users_75_users_less_than_1_76 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update77 >> wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update77 >> \
            if_search_users_75_users_greater_than_0_78
        if_search_users_75_users_less_than_1_76 >> rail.Label(
            'No') >> if_search_users_75_users_greater_than_0_78
        if_search_users_75_users_greater_than_0_78 >> rail.Label(
            'Yes') >> log_useruribasedonthesuffix_79 >> if_log_useruribasedonthesuffix_79_present_80
        if_log_useruribasedonthesuffix_79_present_80 >> rail.Label(
            'Yes') >> log_userstatusbasedonthesuffix_81 >> if_log_userstatusbasedonthesuffix_81_equals_to_false_82
        if_log_userstatusbasedonthesuffix_81_equals_to_false_82 >> rail.Label(
            'Yes') >> re_enable_userprofile_83 >> removeenddate_84 >> updatetoprimaryprofile_set_s_s_o_authentication_for_user_85 >> \
            trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile86 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile86 >> \
            if_log_useruribasedonthesuffix_79_blank_87
        if_log_userstatusbasedonthesuffix_81_equals_to_false_82 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_79_blank_87
        if_log_useruribasedonthesuffix_79_present_80 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_79_blank_87
        if_log_useruribasedonthesuffix_79_blank_87 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update88 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update88 >> \
            if_request_currentprofilecount_equals_to_1_delegateto_delegateand_c4or_c4to_delegateand_c42profiles_89
        if_log_useruribasedonthesuffix_79_blank_87 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_1_delegateto_delegateand_c4or_c4to_delegateand_c42profiles_89
        if_search_users_75_users_greater_than_0_78 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_1_delegateto_delegateand_c4or_c4to_delegateand_c42profiles_89
        if_request_memberof_contains_delegate_c4to_delegate_69 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_1_delegateto_delegateand_c4or_c4to_delegateand_c42profiles_89
        if_declare_variable_41_value_equals_to_1_46 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_1_delegateto_delegateand_c4or_c4to_delegateand_c42profiles_89
        if_request_currentprofilecount_equals_to_1_c4to_delegateor_delegateto_c41profiles_42 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_1_delegateto_delegateand_c4or_c4to_delegateand_c42profiles_89
        if_request_currentprofilecount_equals_to_1_delegateto_delegateand_c4or_c4to_delegateand_c42profiles_89 >> rail.Label(
            'Yes') >> update_variable_90 >> if_declare_variable_41_value_equals_to_2_91
        if_declare_variable_41_value_equals_to_2_91 >> rail.Label(
            'Yes') >> if_request_currenttype_contains_c4_92
        if_request_currenttype_contains_c4_92 >> rail.Label(
            'Yes') >> log_loginnameprimaryprofile_93 >> getuserdata_94 >> \
            log_useruri_primaryprofile_98 >> if_log_useruri_primaryprofile_98_present_99
        if_log_useruri_primaryprofile_98_present_99 >> rail.Label(
            'Yes') >> updateuserloginname_set_replicon_authentication_for_user_100 >> update_email_removingemail_101 >> \
            log_loginnamewithsuffix_102
        if_log_useruri_primaryprofile_98_present_99 >> rail.Label(
            'No') >> log_loginnamewithsuffix_102 >> log_t_y_p_e_103 >> log_timesheet_t_y_p_e_104 >> log_uri_105 >> if_log_uri_105_present_106
        if_log_uri_105_present_106 >> rail.Label(
            'Yes') >> log_status_107 >> if_log_status_107_not_equals_to_true_108
        if_log_status_107_not_equals_to_true_108 >> rail.Label(
            'Yes') >> re_enable_userprofile_109 >> remove_user_end_date_110 >> updatetoprimaryprofile_set_s_s_o_authentication_for_user_111 >> \
            trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile112 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile112 >> if_log_uri_105_blank_113
        if_log_status_107_not_equals_to_true_108 >> rail.Label(
            'No') >> if_log_uri_105_blank_113
        if_log_uri_105_present_106 >> rail.Label(
            'No') >> if_log_uri_105_blank_113
        if_log_uri_105_blank_113 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update114 >> wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update114 >> \
            gather_user_uri >> if_request_currenttype_contains_delegate_123
        if_log_uri_105_blank_113 >> rail.Label(
            'No') >> log_primaryuseruri_115 >> declare_substitute_user_dag_runs >> \
            foreach_accumulate_list_items_16_116 >> get_all_substitute_user_assignments_for_user_117 >> \
            log_substituteuserassigned_118 >> if_log_substituteuserassigned_118_blank_119
        if_log_substituteuserassigned_118_blank_119 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_assign_substitute_usersv2122 >> insert_substitute_user_dag_run_list >> \
            foreach_accumulate_list_items_16_116_end
        if_log_substituteuserassigned_118_blank_119 >> rail.Label(
            'No') >> foreach_accumulate_list_items_16_116_end
        foreach_accumulate_list_items_16_116 >> foreach_accumulate_list_items_16_116_end >> \
            wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2122 >> if_request_currenttype_contains_delegate_123
        if_request_currenttype_contains_c4_92 >> rail.Label(
            'No') >> if_request_currenttype_contains_delegate_123
        if_request_currenttype_contains_delegate_123 >> rail.Label(
            'Yes') >> log_loginnameprimaryprofile_124 >> log_useruri_primaryprofile_125 >> log_loginnamewithsuffix_126 >> \
            log_t_y_p_e_127 >> log_timesheet_t_y_p_e_128 >> search_users_129 >> if_search_users_129_users_less_than_1_130
        if_search_users_129_users_less_than_1_130 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update131 >> wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update131 >> \
            if_search_users_129_users_greater_than_0_132
        if_search_users_129_users_less_than_1_130 >> rail.Label(
            'No') >> if_search_users_129_users_greater_than_0_132
        if_search_users_129_users_greater_than_0_132 >> rail.Label(
            'Yes') >> log_useruribasedonthesuffix_133 >> if_log_useruribasedonthesuffix_133_present_134
        if_log_useruribasedonthesuffix_133_present_134 >> rail.Label(
            'Yes') >> log_userstatusbasedonthesuffix_135 >> if_log_userstatusbasedonthesuffix_135_equals_to_false_136
        if_log_userstatusbasedonthesuffix_135_equals_to_false_136 >> rail.Label(
            'Yes') >> re_enable_userprofile_137 >> get_all_substitute_user_assignments_for_user_138 >> \
            log_substituteuserassigned_139 >> if_log_substituteuserassigned_139_blank_140
        if_log_substituteuserassigned_139_blank_140 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_assign_substitute_usersv2143 >> wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2143 >> \
            trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile144
        if_log_substituteuserassigned_139_blank_140 >> rail.Label(
            'No') >> trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile144 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile144 >> \
            if_log_useruribasedonthesuffix_133_blank_145
        if_log_userstatusbasedonthesuffix_135_equals_to_false_136 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_133_blank_145
        if_log_useruribasedonthesuffix_133_present_134 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_133_blank_145
        if_log_useruribasedonthesuffix_133_blank_145 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update146 >> wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update146 >> \
            if_request_currentprofilecount_equals_to_1_c4to_c4_c3or_delegateto_delegate_c36profiles_147
        if_log_useruribasedonthesuffix_133_blank_145 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_1_c4to_c4_c3or_delegateto_delegate_c36profiles_147
        if_search_users_129_users_greater_than_0_132 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_1_c4to_c4_c3or_delegateto_delegate_c36profiles_147
        if_request_currenttype_contains_delegate_123 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_1_c4to_c4_c3or_delegateto_delegate_c36profiles_147
        if_declare_variable_41_value_equals_to_2_91 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_1_c4to_c4_c3or_delegateto_delegate_c36profiles_147
        if_request_currentprofilecount_equals_to_1_delegateto_delegateand_c4or_c4to_delegateand_c42profiles_89 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_1_c4to_c4_c3or_delegateto_delegate_c36profiles_147
        if_request_currentprofilecount_equals_to_1_c4to_c4_c3or_delegateto_delegate_c36profiles_147 >> rail.Label(
            'Yes') >> update_variable_148 >> query_list_whereexistingprimaryprofilevalueis_delegate_149 >> \
            get_first_primary_records_149 >> declare_variable_150 >> \
            if_request_memberof_contains_c4_delegateto_c4_c3addingnewprimaryprofileanddisablingold_151
        if_request_memberof_contains_c4_delegateto_c4_c3addingnewprimaryprofileanddisablingold_151 >> rail.Label(
            'Yes') >> update_variable_152 >> if_declare_variable_41_value_equals_to_6_153
        if_declare_variable_41_value_equals_to_6_153 >> rail.Label(
            'Yes') >> updateuserloginname_set_replicon_authentication_for_user_154 >> update_email_removingemail_155 >> \
            disable_loginoldprimaryprofile_156 >> update_user_end_date_157 >> log_loginnamewithaf_158 >> search_users_159 >> \
            if_search_users_159_users_less_than_1_160
        if_search_users_159_users_less_than_1_160 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update161 >> wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update161 >> \
            if_search_users_159_users_greater_than_0_162
        if_search_users_159_users_less_than_1_160 >> rail.Label(
            'No') >> if_search_users_159_users_greater_than_0_162
        if_search_users_159_users_greater_than_0_162 >> rail.Label(
            'Yes') >> log_useruribasedonthesuffix_163 >> if_log_useruribasedonthesuffix_163_present_164
        if_log_useruribasedonthesuffix_163_present_164 >> rail.Label(
            'Yes') >> log_userstatusbasedonthesuffix_165 >> if_log_userstatusbasedonthesuffix_165_equals_to_false_166
        if_log_userstatusbasedonthesuffix_165_equals_to_false_166 >> rail.Label(
            'Yes') >> re_enable_userprofile_167 >> removeenddate_168 >> \
            updatetoprimaryprofile_set_s_s_o_authentication_for_user_169 >> \
            trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile170 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile170 >> \
            if_log_useruribasedonthesuffix_163_blank_171
        if_log_userstatusbasedonthesuffix_165_equals_to_false_166 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_163_blank_171
        if_log_useruribasedonthesuffix_163_present_164 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_163_blank_171
        if_log_useruribasedonthesuffix_163_blank_171 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update172 >> wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update172 >> update_variable_173
        if_log_useruribasedonthesuffix_163_blank_171 >> rail.Label(
            'No') >> update_variable_173
        if_search_users_159_users_greater_than_0_162 >> rail.Label(
            'No') >> update_variable_173 >> query_list_whereexistingprimaryprofilevalueis_c4_174 >> get_first_primary_records_174 >> \
            if_request_memberof_contains_delegate_c4to_delegate_c3addingnewprimaryprofileanddisablingold_174
        if_declare_variable_41_value_equals_to_6_153 >> rail.Label(
            'No') >> if_request_memberof_contains_delegate_c4to_delegate_c3addingnewprimaryprofileanddisablingold_174
        if_request_memberof_contains_c4_delegateto_c4_c3addingnewprimaryprofileanddisablingold_151 >> rail.Label(
            'No') >> if_request_memberof_contains_delegate_c4to_delegate_c3addingnewprimaryprofileanddisablingold_174
        if_request_memberof_contains_delegate_c4to_delegate_c3addingnewprimaryprofileanddisablingold_174 >> rail.Label(
            'Yes') >> update_variable_175 >> if_declare_variable_41_value_equals_to_6_176
        if_declare_variable_41_value_equals_to_6_176 >> rail.Label(
            'Yes') >> updateuserloginname_set_replicon_authentication_for_user_177 >> \
            update_email_removingemail_178 >> disable_loginoldprimaryprofile_179 >> update_user_end_date_180 >> \
            log_loginnamewithd_181 >> search_users_182 >> if_search_users_182_users_less_than_1_183
        if_search_users_182_users_less_than_1_183 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update184 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update184 >> if_search_users_182_users_greater_than_0_185
        if_search_users_182_users_less_than_1_183 >> rail.Label(
            'No') >> if_search_users_182_users_greater_than_0_185
        if_search_users_182_users_greater_than_0_185 >> rail.Label(
            'Yes') >> log_useruribasedonthesuffix_186 >> if_log_useruribasedonthesuffix_186_present_187
        if_log_useruribasedonthesuffix_186_present_187 >> rail.Label(
            'Yes') >> log_userstatusbasedonthesuffix_188 >> if_log_userstatusbasedonthesuffix_188_equals_to_false_189
        if_log_userstatusbasedonthesuffix_188_equals_to_false_189 >> rail.Label(
            'Yes') >> re_enable_userprofile_190 >> removeenddate_191 >> \
            updatetoprimaryprofile_set_s_s_o_authentication_for_user_192 >> \
            trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile193 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile193 >> \
            if_log_useruribasedonthesuffix_186_blank_194
        if_log_userstatusbasedonthesuffix_188_equals_to_false_189 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_186_blank_194
        if_log_useruribasedonthesuffix_186_present_187 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_186_blank_194
        if_log_useruribasedonthesuffix_186_blank_194 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update195 >> wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update195 >> update_variable_196
        if_log_useruribasedonthesuffix_186_blank_194 >> rail.Label(
            'No') >> update_variable_196
        if_search_users_182_users_greater_than_0_185 >> rail.Label(
            'No') >> update_variable_196 >> if_request_memberof_contains_c4_197
        if_declare_variable_41_value_equals_to_6_176 >> rail.Label(
            'No') >> if_request_memberof_contains_c4_197
        if_request_memberof_contains_delegate_c4to_delegate_c3addingnewprimaryprofileanddisablingold_174 >> rail.Label(
            'No') >> if_request_memberof_contains_c4_197
        if_request_memberof_contains_c4_197 >> rail.Label(
            'Yes') >> update_variable_198 >> if_declare_variable_41_value_equals_to_6_199
        if_request_memberof_contains_c4_197 >> rail.Label(
            'No') >> if_declare_variable_41_value_equals_to_6_199
        if_declare_variable_41_value_equals_to_6_199 >> rail.Label(
            'Yes') >> query_list_whereexistingprimaryprofilevalueis_delegate_c4_200 >> get_first_primary_records_200 >> if_first_type_present_c4_c4and_c3_200
        if_first_type_present_c4_c4and_c3_200 >> rail.Label(
            'Yes') >> log_loginnameprimaryprofile_201 >> log_useruri_primaryprofile_202 >> getuserdata_203 >> \
            create_list_size5_207 >> declare_list_208 >> get_c3_c4_list >> declare_list_update_dag_runs_214 >> \
            foreach_declare_list_208_214 >> log_uriuser_215 >> if_log_uriuser_215_present_216
        if_log_uriuser_215_present_216 >> rail.Label(
            'Yes') >> log_status_217 >> if_log_status_217_not_equals_to_true_218
        if_log_status_217_not_equals_to_true_218 >> rail.Label(
            'Yes') >> re_enable_userprofile_219 >> remove_user_end_date_220 >> \
            trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile221 >> insert_to_user_rehire_disable_user_list >>\
            get_all_substitute_user_assignments_for_user_222 >> log_substituteuserassigned_223 >> \
            if_log_substituteuserassigned_223_blank_224
        if_log_substituteuserassigned_223_blank_224 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_assign_substitute_usersv2227 >> insert_to_assign_substitute_user_list >>\
            if_log_uriuser_215_blank_228
        if_log_substituteuserassigned_223_blank_224 >> rail.Label(
            'No') >> if_log_uriuser_215_blank_228
        if_log_status_217_not_equals_to_true_218 >> rail.Label(
            'No') >> if_log_uriuser_215_blank_228
        if_log_uriuser_215_present_216 >> rail.Label(
            'No') >> if_log_uriuser_215_blank_228
        if_log_uriuser_215_blank_228 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update229 >> \
            insert_to_user_basic_add_list >> \
            foreach_declare_list_208_214_end
        if_log_uriuser_215_blank_228 >> rail.Label(
            'No') >> foreach_declare_list_208_214_end
        foreach_declare_list_208_214 >> foreach_declare_list_208_214_end >> \
            get_dag_run_ids_229 >> wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update229 >> \
            if_request_currentprofilecount_equals_to_1_c4to_c4_c3anddelegateor_delegateto_c4_c3and_delegate1to7profiles_230
        if_first_type_present_c4_c4and_c3_200 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_1_c4to_c4_c3anddelegateor_delegateto_c4_c3and_delegate1to7profiles_230
        if_declare_variable_41_value_equals_to_6_199 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_1_c4to_c4_c3anddelegateor_delegateto_c4_c3and_delegate1to7profiles_230
        if_request_currentprofilecount_equals_to_1_c4to_c4_c3or_delegateto_delegate_c36profiles_147 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_1_c4to_c4_c3anddelegateor_delegateto_c4_c3and_delegate1to7profiles_230
        if_request_currentprofilecount_equals_to_1_c4to_c4_c3anddelegateor_delegateto_c4_c3and_delegate1to7profiles_230 >> rail.Label(
            'Yes') >> update_variable_231 >> if_declare_variable_41_value_equals_to_7_232
        if_declare_variable_41_value_equals_to_7_232 >> rail.Label(
            'Yes') >> query_list_whereexistingprimaryprofilevalueis_delegate_233 >> get_first_primary_records_233 >> declare_variable_234 >> \
            if_first_type_equals_to_delegate_keeping_delegateas_primary_235
        if_first_type_equals_to_delegate_keeping_delegateas_primary_235 >> rail.Label(
            'Yes') >> log_loginnamewithaf_236 >> search_users_237 >> if_search_users_237_users_less_than_1_238
        if_search_users_237_users_less_than_1_238 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update239 >> wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update239 >> \
            gather_user_uri_239 >> if_search_users_237_users_greater_than_0_240
        if_search_users_237_users_less_than_1_238 >> rail.Label(
            'No') >> if_search_users_237_users_greater_than_0_240
        if_search_users_237_users_greater_than_0_240 >> rail.Label(
            'Yes') >> log_useruribasedonthesuffix_241 >> if_log_useruribasedonthesuffix_241_present_242
        if_log_useruribasedonthesuffix_241_present_242 >> rail.Label(
            'Yes') >> log_userstatusbasedonthesuffix_243 >> if_log_userstatusbasedonthesuffix_243_equals_to_false_244
        if_log_userstatusbasedonthesuffix_243_equals_to_false_244 >> rail.Label(
            'Yes') >> re_enable_userprofile_245 >> removeenddate_246 >> \
            trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile247 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile247 >> \
            if_log_useruribasedonthesuffix_241_blank_248
        if_log_userstatusbasedonthesuffix_243_equals_to_false_244 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_241_blank_248
        if_log_useruribasedonthesuffix_241_present_242 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_241_blank_248
        if_log_useruribasedonthesuffix_241_blank_248 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update249 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update249 >> gather_user_uri_249 >> \
            update_variable_250
        if_log_useruribasedonthesuffix_241_blank_248 >> rail.Label(
            'No') >> update_variable_250
        if_search_users_237_users_greater_than_0_240 >> rail.Label(
            'No') >> update_variable_250 >> log_a_fchilduseruri_251 >> \
            get_all_substitute_user_assignments_for_user_252 >> log_substituteuserassigned_253 >> \
            if_log_substituteuserassigned_253_blank_254
        if_log_substituteuserassigned_253_blank_254 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_assign_substitute_usersv2257 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2257 >> if_first_type_equals_to_c4_c4to_delegate_258
        if_log_substituteuserassigned_253_blank_254 >> rail.Label(
            'No') >> if_first_type_equals_to_c4_c4to_delegate_258
        if_first_type_equals_to_delegate_keeping_delegateas_primary_235 >> rail.Label(
            'No') >> if_first_type_equals_to_c4_c4to_delegate_258
        if_first_type_equals_to_c4_c4to_delegate_258 >> rail.Label(
            'Yes') >> updateuserloginname_set_replicon_authentication_for_user_259 >> \
            update_email_removingemail_260 >> log_loginnamewithd_261 >> search_users_262 >> if_search_users_262_users_less_than_1_263
        if_search_users_262_users_less_than_1_263 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update264 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update264 >> if_search_users_262_users_greater_than_0_265
        if_search_users_262_users_less_than_1_263 >> rail.Label(
            'No') >> if_search_users_262_users_greater_than_0_265
        if_search_users_262_users_greater_than_0_265 >> rail.Label(
            'Yes') >> log_useruribasedonthesuffix_266 >> if_log_useruribasedonthesuffix_266_present_267
        if_log_useruribasedonthesuffix_266_present_267 >> rail.Label(
            'Yes') >> log_userstatusbasedonthesuffix_268 >> if_log_userstatusbasedonthesuffix_268_equals_to_false_269
        if_log_userstatusbasedonthesuffix_268_equals_to_false_269 >> rail.Label(
            'Yes') >> re_enable_userprofile_270 >> removeenddate_271 >> updatetoprimaryprofile_set_s_s_o_authentication_for_user_272 >> \
            trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile273 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile273 >> if_log_useruribasedonthesuffix_266_blank_274
        if_log_userstatusbasedonthesuffix_268_equals_to_false_269 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_266_blank_274
        if_log_useruribasedonthesuffix_266_present_267 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_266_blank_274
        if_log_useruribasedonthesuffix_266_blank_274 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update275 >> wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update275 >> update_variable_276
        if_log_useruribasedonthesuffix_266_blank_274 >> rail.Label(
            'No') >> update_variable_276
        if_search_users_262_users_greater_than_0_265 >> rail.Label(
            'No') >> update_variable_276 >> get_all_substitute_user_assignments_for_user_277 >> \
            log_substituteuserassigned_278 >> if_log_substituteuserassigned_278_blank_279
        if_log_substituteuserassigned_278_blank_279 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_assign_substitute_usersv2282 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2282 >> if_first_type_present_c4_c4and_c3_283
        if_log_substituteuserassigned_278_blank_279 >> rail.Label(
            'No') >> if_first_type_present_c4_c4and_c3_283
        if_first_type_equals_to_c4_c4to_delegate_258 >> rail.Label(
            'No') >> query_list_whereexistingprimaryprofilevalueis_c4_delegate_283 >> get_first_primary_records_283 >> if_first_type_present_c4_c4and_c3_283
        if_first_type_present_c4_c4and_c3_283 >> rail.Label(
            'Yes') >> log_loginnameprimaryprofile_284 >> log_useruri_primaryprofile_285 >> getuserdata_286 >>\
            create_list_size5_290 >> declare_list_291 >> get_c3_c4_list_297 >> declare_list_update_dag_runs_297 >>\
            foreach_declare_list_291_297 >> log_uriuser_298 >> if_log_uriuser_298_present_299
        if_log_uriuser_298_present_299 >> rail.Label(
            'Yes') >> log_status_300 >> if_log_status_300_not_equals_to_true_301
        if_log_status_300_not_equals_to_true_301 >> rail.Label(
            'Yes') >> re_enable_userprofile_302 >> remove_user_end_date_303 >> \
            trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile304 >> \
            insert_to_user_rehire_disable_user_list_304 >> \
            get_all_substitute_user_assignments_for_user_305 >> log_substituteuserassigned_306 >> if_log_substituteuserassigned_306_blank_307
        if_log_substituteuserassigned_306_blank_307 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_assign_substitute_usersv2310 >> \
            insert_to_user_rehire_disable_user_list_310 >> if_log_uriuser_298_blank_311
        if_log_substituteuserassigned_306_blank_307 >> rail.Label(
            'No') >> if_log_uriuser_298_blank_311
        if_log_status_300_not_equals_to_true_301 >> rail.Label(
            'No') >> if_log_uriuser_298_blank_311
        if_log_uriuser_298_present_299 >> rail.Label(
            'No') >> if_log_uriuser_298_blank_311
        if_log_uriuser_298_blank_311 >> rail.Label(
            'No') >> trigger_dag_run_live_nrdc_basic_add_update312 >> insert_to_user_rehire_disable_user_list_312 >> \
            foreach_declare_list_291_297_end
        if_log_uriuser_298_blank_311 >> rail.Label(
            'Yes') >> foreach_declare_list_291_297_end
        foreach_declare_list_291_297 >> foreach_declare_list_291_297_end >> get_dag_run_ids_310 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2310 >>\
            if_request_currentprofilecount_equals_to_1_c4to_c3or_delegateto_c35profiles_313
        if_first_type_present_c4_c4and_c3_283 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_1_c4to_c3or_delegateto_c35profiles_313
        if_declare_variable_41_value_equals_to_7_232 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_1_c4to_c3or_delegateto_c35profiles_313
        if_request_currentprofilecount_equals_to_1_c4to_c4_c3anddelegateor_delegateto_c4_c3and_delegate1to7profiles_230 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_1_c4to_c3or_delegateto_c35profiles_313
        if_request_currentprofilecount_equals_to_1_c4to_c3or_delegateto_c35profiles_313 >> rail.Label(
            'Yes') >> update_variable_314 >> if_declare_variable_41_value_equals_to_5_315
        if_declare_variable_41_value_equals_to_5_315 >> rail.Label(
            'Yes') >> log_primaryloginname_316 >> log_primary_uri_317 >> log_primary_user_type_318 >> \
            search_users_319 >> declare_list_320 >> declare_variable_321 >> declare_list_update_dag_runs_322 >>\
            foreach_search_users_319_322 >> if_login_name_textvalue_equals_to_datalogger38faa588message_323
        if_login_name_textvalue_equals_to_datalogger38faa588message_323 >> rail.Label(
            'Yes') >> if_log_primary_user_type_318_equals_to_delegate_324
        if_log_primary_user_type_318_equals_to_delegate_324 >> rail.Label(
            'Yes') >> update_variable_325 >> update_loginname_326
        if_log_primary_user_type_318_equals_to_delegate_324 >> rail.Label(
            'No') >> update_loginname_326 >> add_end_dateandemail_327 >> \
            disable_userprofile_328 >> if_login_name_textvalue_equals_to_datalogger38faa588messagefl_329
        if_login_name_textvalue_equals_to_datalogger38faa588message_323 >> rail.Label(
            'No') >> if_login_name_textvalue_equals_to_datalogger38faa588messagefl_329
        if_login_name_textvalue_equals_to_datalogger38faa588messagefl_329 >> rail.Label(
            'Yes') >> insert_to_list_330 >> update_loginnameandmakeprimaryprofile_331 >> if_enabled_boolvalue_is_not_true_332
        if_enabled_boolvalue_is_not_true_332 >> rail.Label(
            'Yes') >> remove_end_date_333 >> if_login_name_textvalue_equals_to_datalogger38faa588messagell_335
        if_enabled_boolvalue_is_not_true_332 >> rail.Label(
            'No') >> trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile334 >> \
            insert_to_user_dag_run_list_334 >> if_login_name_textvalue_equals_to_datalogger38faa588messagell_335
        if_login_name_textvalue_equals_to_datalogger38faa588messagefl_329 >> rail.Label(
            'No') >> if_login_name_textvalue_equals_to_datalogger38faa588messagell_335
        if_login_name_textvalue_equals_to_datalogger38faa588messagell_335 >> rail.Label(
            'Yes') >> insert_to_list_336 >> if_enabled_boolvalue_is_not_true_337
        if_enabled_boolvalue_is_not_true_337 >> rail.Label(
            'Yes') >> remove_end_date_338 >> if_login_name_textvalue_equals_to_datalogger38faa588messagela_340
        if_enabled_boolvalue_is_not_true_337 >> rail.Label(
            'No') >> trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile339 >> \
            insert_to_user_dag_run_list_339 >> if_login_name_textvalue_equals_to_datalogger38faa588messagela_340
        if_login_name_textvalue_equals_to_datalogger38faa588messagell_335 >> rail.Label(
            'No') >> if_login_name_textvalue_equals_to_datalogger38faa588messagela_340
        if_login_name_textvalue_equals_to_datalogger38faa588messagela_340 >> rail.Label(
            'Yes') >> insert_to_list_341 >> if_enabled_boolvalue_is_not_true_342
        if_enabled_boolvalue_is_not_true_342 >> rail.Label(
            'Yes') >> remove_end_date_343 >> if_login_name_textvalue_equals_to_datalogger38faa588messagesl_345
        if_enabled_boolvalue_is_not_true_342 >> rail.Label(
            'No') >> trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile344 >> \
            insert_to_user_dag_run_list_345 >> if_login_name_textvalue_equals_to_datalogger38faa588messagesl_345
        if_login_name_textvalue_equals_to_datalogger38faa588messagela_340 >> rail.Label(
            'No') >> if_login_name_textvalue_equals_to_datalogger38faa588messagesl_345
        if_login_name_textvalue_equals_to_datalogger38faa588messagesl_345 >> rail.Label(
            'Yes') >> insert_to_list_346 >> if_enabled_boolvalue_is_not_true_347
        if_enabled_boolvalue_is_not_true_347 >> rail.Label(
            'Yes') >> remove_end_date_348 >> if_login_name_textvalue_equals_to_datalogger38faa588messagesa_350
        if_enabled_boolvalue_is_not_true_347 >> rail.Label(
            'No') >> trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile349 >> \
            insert_to_user_dag_run_list_349 >> if_login_name_textvalue_equals_to_datalogger38faa588messagesa_350
        if_login_name_textvalue_equals_to_datalogger38faa588messagesl_345 >> rail.Label(
            'No') >> if_login_name_textvalue_equals_to_datalogger38faa588messagesa_350
        if_login_name_textvalue_equals_to_datalogger38faa588messagesa_350 >> rail.Label(
            'Yes') >> insert_to_list_351 >> if_enabled_boolvalue_is_not_true_352
        if_enabled_boolvalue_is_not_true_352 >> rail.Label(
            'Yes') >> remove_end_date_353 >> trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile354
        if_enabled_boolvalue_is_not_true_352 >> rail.Label(
            'No') >> trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile354 >> \
            insert_to_user_dag_run_list_354 >> foreach_search_users_319_322_end
        if_login_name_textvalue_equals_to_datalogger38faa588messagesa_350 >> rail.Label(
            'No') >> foreach_search_users_319_322_end
        foreach_search_users_319_322 >> foreach_search_users_319_322_end >> \
            get_dag_run_ids_354 >> wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile354 >>\
            log_checkif_federal_legislativeisavailable_355 >> \
            if_log_checkif_federal_legislativeisavailable_355_blank_356
        if_log_checkif_federal_legislativeisavailable_355_blank_356 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update357 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update357 >> gather_user_uri_358 >>\
            insert_to_list_358 >> log_checkif_statelegislativeisavailable_359
        if_log_checkif_federal_legislativeisavailable_355_blank_356 >> rail.Label(
            'No') >> log_checkif_statelegislativeisavailable_359 >> if_log_checkif_statelegislativeisavailable_359_blank_360
        if_log_checkif_statelegislativeisavailable_359_blank_360 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update361 >> wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update361 >> \
            gather_user_uri_362 >> insert_to_list_362 >> log_checkif_locallegislativeisavailable_363
        if_log_checkif_statelegislativeisavailable_359_blank_360 >> rail.Label(
            'No') >> log_checkif_locallegislativeisavailable_363 >> if_log_checkif_locallegislativeisavailable_363_blank_364
        if_log_checkif_locallegislativeisavailable_363_blank_364 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update365 >> wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update365 >> \
            gather_user_uri_366 >> insert_to_list_366 >> log_checkif_localadministrativeisavailable_367
        if_log_checkif_locallegislativeisavailable_363_blank_364 >> rail.Label(
            'No') >> log_checkif_localadministrativeisavailable_367 >> if_log_checkif_localadministrativeisavailable_367_blank_368
        if_log_checkif_localadministrativeisavailable_367_blank_368 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update369 >> wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update369 >> \
            gather_user_uri_370 >> insert_to_list_370 >> log_checkif_stateadministrativeisavailable_371
        if_log_checkif_localadministrativeisavailable_367_blank_368 >> rail.Label(
            'No') >> log_checkif_stateadministrativeisavailable_371 >> if_log_checkif_stateadministrativeisavailable_371_blank_372
        if_log_checkif_stateadministrativeisavailable_371_blank_372 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update373 >> wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update373 >> \
            gather_user_uri_374 >> insert_to_list_374 >> log_f_l_user_profile_uri_375
        if_log_checkif_stateadministrativeisavailable_371_blank_372 >> rail.Label(
            'No') >> log_f_l_user_profile_uri_375 >> declare_list_update_dag_runs_376 >> get_320_list_376 >>\
            foreach_declare_list_320_376 >> get_all_substitute_user_assignments_for_user_377 >> \
            log_substituteuserassigned_378 >> if_log_substituteuserassigned_378_blank_379
        if_log_substituteuserassigned_378_blank_379 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_assign_substitute_usersv2382 >> insert_to_user_dag_run_list_382 >> \
            foreach_declare_list_320_376_end
        if_log_substituteuserassigned_378_blank_379 >> rail.Label(
            'No') >> foreach_declare_list_320_376_end
        foreach_declare_list_320_376 >> foreach_declare_list_320_376_end >> wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2382 >> \
            if_request_currentprofilecount_equals_to_2_c4delegateto_c4_c3and_delegate2to7profiles_383
        if_declare_variable_41_value_equals_to_5_315 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_2_c4delegateto_c4_c3and_delegate2to7profiles_383
        if_request_currentprofilecount_equals_to_1_c4to_c3or_delegateto_c35profiles_313 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_2_c4delegateto_c4_c3and_delegate2to7profiles_383
        if_request_currentprofilecount_equals_to_2_c4delegateto_c4_c3and_delegate2to7profiles_383 >> rail.Label(
            'Yes') >> update_variable_384 >> if_declare_variable_41_value_equals_to_7_385
        if_declare_variable_41_value_equals_to_7_385 >> rail.Label(
            'Yes') >> query_list_whereexistingprimaryprofilevalueis_delegate_386 >> get_first_records_from_query >> declare_variable_387 >>\
            if_first_type_present_c4_creating_c3profiles_388
        if_first_type_present_c4_creating_c3profiles_388 >> rail.Label(
            'Yes') >> log_loginnameprimaryprofile_389 >> log_useruri_primaryprofile_390 >> getuserdata_391 >> \
            create_list_size5_395 >> declare_list_396 >> get_c3_c4_list_397 >> \
            declare_list_update_dag_runs_402 >> foreach_declare_list_396_402 >> \
            log_uriuser_403 >> if_log_uriuser_403_present_404
        if_log_uriuser_403_present_404 >> rail.Label(
            'Yes') >> log_status_405 >> if_log_status_405_not_equals_to_true_406
        if_log_status_405_not_equals_to_true_406 >> rail.Label(
            'Yes') >> re_enable_userprofile_407 >> remove_user_end_date_408 >> trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile409 >> \
            insert_to_user_dag_run_list_409 >> get_all_substitute_user_assignments_for_user_410 >> \
            log_substituteuserassigned_411 >> if_log_substituteuserassigned_411_blank_412
        if_log_substituteuserassigned_411_blank_412 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_assign_substitute_usersv2415 >> \
            insert_to_user_dag_run_list_415 >> if_log_uriuser_403_blank_416
        if_log_substituteuserassigned_411_blank_412 >> rail.Label(
            'No') >> if_log_uriuser_403_blank_416
        if_log_status_405_not_equals_to_true_406 >> rail.Label(
            'No') >> if_log_uriuser_403_blank_416
        if_log_uriuser_403_present_404 >> rail.Label(
            'No') >> if_log_uriuser_403_blank_416
        if_log_uriuser_403_blank_416 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update417 >> insert_to_user_dag_run_list_417 >> \
            foreach_declare_list_396_402_end
        if_log_uriuser_403_blank_416 >> rail.Label(
            'No') >> foreach_declare_list_396_402_end
        foreach_declare_list_396_402 >> foreach_declare_list_396_402_end >> get_dag_run_ids_418 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update417 >>\
            if_request_currentprofilecount_equals_to_2_delegate_c4to_delegateprofile1profiles_418
        if_first_type_present_c4_creating_c3profiles_388 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_2_delegate_c4to_delegateprofile1profiles_418
        if_declare_variable_41_value_equals_to_7_385 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_2_delegate_c4to_delegateprofile1profiles_418
        if_request_currentprofilecount_equals_to_2_c4delegateto_c4_c3and_delegate2to7profiles_383 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_2_delegate_c4to_delegateprofile1profiles_418
        if_request_currentprofilecount_equals_to_2_delegate_c4to_delegateprofile1profiles_418 >> rail.Label(
            'Yes') >> update_variable_419 >> if_declare_variable_41_value_equals_to_1_420
        if_declare_variable_41_value_equals_to_1_420 >> rail.Label(
            'Yes') >> foreach_accumulate_list_items_16_421 >> if_foreach_3157e122_421_type_equals_to_c4_422
        if_foreach_3157e122_421_type_equals_to_c4_422 >> rail.Label(
            'Yes') >> disable_loginoldprimaryprofile_423 >> foreach_accumulate_list_items_16_421_end
        if_foreach_3157e122_421_type_equals_to_c4_422 >> rail.Label(
            'No') >> foreach_accumulate_list_items_16_421_end
        foreach_accumulate_list_items_16_421 >> foreach_accumulate_list_items_16_421_end >> \
            if_request_currentprofilecount_equals_to_2_delegate_c4to_c4profile1profiles_424
        if_declare_variable_41_value_equals_to_1_420 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_2_delegate_c4to_c4profile1profiles_424
        if_request_currentprofilecount_equals_to_2_delegate_c4to_delegateprofile1profiles_418 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_2_delegate_c4to_c4profile1profiles_424
        if_request_currentprofilecount_equals_to_2_delegate_c4to_c4profile1profiles_424 >> rail.Label(
            'Yes') >> update_variable_425 >> if_declare_variable_41_value_equals_to_1_426
        if_declare_variable_41_value_equals_to_1_426 >> rail.Label(
            'Yes') >> log_primaryprofileloginname_427 >> query_list_wherevalueis_delegate_428 >> get_first_records_from_query_429 >> \
            if_first_type_present_delegate_429
        if_first_type_present_delegate_429 >> rail.Label(
            'Yes') >> updateuserloginname_set_replicon_authentication_for_user_430 >> update_email_removingemail_431 >> \
            disable_loginoldprimaryprofile_432 >> update_user_end_date_433 >> query_list_wherevalueisc4_434
        if_first_type_present_delegate_429 >> rail.Label(
            'No') >> query_list_wherevalueisc4_434 >> get_first_records_from_query_434 >> if_first_type_present_c4_435
        if_first_type_present_c4_435 >> rail.Label(
            'Yes') >> updatetoprimaryprofile_set_s_s_o_authentication_for_user_436 >> \
            update_email_addingemail_437 >> if_request_currentprofilecount_equals_to_5_only_c4_c3to_c4_c36profiles_438
        if_first_type_present_c4_435 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_5_only_c4_c3to_c4_c36profiles_438
        if_declare_variable_41_value_equals_to_1_426 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_5_only_c4_c3to_c4_c36profiles_438
        if_request_currentprofilecount_equals_to_2_delegate_c4to_c4profile1profiles_424 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_5_only_c4_c3to_c4_c36profiles_438
        if_request_currentprofilecount_equals_to_5_only_c4_c3to_c4_c36profiles_438 >> rail.Label(
            'Yes') >> update_variable_439 >> if_declare_variable_41_value_equals_to_6_440
        if_declare_variable_41_value_equals_to_6_440 >> rail.Label(
            'Yes') >> log_loginnamefromemailprimaryprofile_441 >> log_userurimainprofile_442 >> if_log_userurimainprofile_442_present_443
        if_log_userurimainprofile_442_present_443 >> rail.Label(
            'Yes') >> updateuserloginname_set_replicon_authentication_for_user_444 >> update_email_removingemail_445 >> log_loginnameprimaryold_446
        if_log_userurimainprofile_442_present_443 >> rail.Label(
            'No') >> log_loginnameprimaryold_446 >> search_users_447 >> if_search_users_447_users_less_than_1_448
        if_search_users_447_users_less_than_1_448 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update449 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update449 >> gather_user_uri_449 >> \
            if_search_users_447_users_greater_than_0_450
        if_search_users_447_users_less_than_1_448 >> rail.Label(
            'No') >> if_search_users_447_users_greater_than_0_450
        if_search_users_447_users_greater_than_0_450 >> rail.Label(
            'Yes') >> log_useruribasedonthesuffix_451 >> if_log_useruribasedonthesuffix_451_present_452
        if_log_useruribasedonthesuffix_451_present_452 >> rail.Label(
            'Yes') >> log_userstatusbasedonthesuffix_453 >> if_log_userstatusbasedonthesuffix_453_equals_to_false_454
        if_log_userstatusbasedonthesuffix_453_equals_to_false_454 >> rail.Label(
            'Yes') >> updatetoprimaryprofile_set_s_s_o_authentication_for_user_455 >> re_enable_userprofile_456 >> \
            update_emailaddingemail_457 >> update_user_end_dateremoveenddate_458 >> trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile459 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile459 >> if_log_useruribasedonthesuffix_451_blank_460
        if_log_userstatusbasedonthesuffix_453_equals_to_false_454 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_451_blank_460
        if_log_useruribasedonthesuffix_451_present_452 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_451_blank_460
        if_log_useruribasedonthesuffix_451_blank_460 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update461 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update461 >> log_newprimaryuseruri_470
        if_log_useruribasedonthesuffix_451_blank_460 >> rail.Label(
            'No') >> gather_user_uri_461 >> log_primaryprofileuri_462 >> declare_list_update_dag_runs_463 >>\
            foreach_accumulate_list_items_16_463 >> get_all_substitute_user_assignments_for_user_464 >> \
            log_substituteuserassigned_465 >> if_log_substituteuserassigned_465_blank_466
        if_log_substituteuserassigned_465_blank_466 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_assign_substitute_usersv2469 >>\
            insert_to_user_dag_run_list_463 >> foreach_accumulate_list_items_16_463_end
        if_log_substituteuserassigned_465_blank_466 >> rail.Label(
            'No') >> foreach_accumulate_list_items_16_463_end
        foreach_accumulate_list_items_16_463 >> foreach_accumulate_list_items_16_463_end >> \
            wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2469 >> gather_user_uri_470 >> \
            log_newprimaryuseruri_470
        if_search_users_447_users_greater_than_0_450 >> rail.Label(
            'No') >> log_newprimaryuseruri_470 >> declare_list_update_dag_runs_471 >> \
            foreach_accumulate_list_items_16_471 >> \
            get_all_substitute_user_assignments_for_user_472 >> log_substituteuserassigned_473 >> if_log_substituteuserassigned_473_blank_474
        if_log_substituteuserassigned_473_blank_474 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_assign_substitute_usersv2477 >> insert_to_user_dag_run_list_471 >> \
            foreach_accumulate_list_items_16_471_end
        if_log_substituteuserassigned_473_blank_474 >> rail.Label(
            'No') >> foreach_accumulate_list_items_16_471_end
        foreach_accumulate_list_items_16_471 >> foreach_accumulate_list_items_16_471_end >> \
            wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2477 >> \
            if_request_currentprofilecount_equals_to_5_c3_c4to_c41profiles2_profiles_478
        if_declare_variable_41_value_equals_to_6_440 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_5_c3_c4to_c41profiles2_profiles_478
        if_request_currentprofilecount_equals_to_5_only_c4_c3to_c4_c36profiles_438 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_5_c3_c4to_c41profiles2_profiles_478
        if_request_currentprofilecount_equals_to_5_c3_c4to_c41profiles2_profiles_478 >> rail.Label(
            'Yes') >> log_loginnamefromemailprimaryprofile_479 >> log_userurimainprofile_480 >> \
            declare_variable_481 >> if_log_userurimainprofile_480_present_482
        if_log_userurimainprofile_480_present_482 >> rail.Label(
            'Yes') >> updateuserloginname_set_replicon_authentication_for_user_483 >> update_email_removingemail_484 >> \
            disable_userprofile_485 >> update_user_end_date_486 >> if_request_memberof_contains_c4_487
        if_log_userurimainprofile_480_present_482 >> rail.Label(
            'No') >> if_request_memberof_contains_c4_487
        if_request_memberof_contains_c4_487 >> rail.Label(
            'Yes') >> log_loginnameprimaryold_488 >> search_users_489 >> if_search_users_489_users_less_than_1_490
        if_search_users_489_users_less_than_1_490 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update491 >> wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update491 >> \
            gather_user_uri_491 >> if_search_users_489_users_greater_than_0_492
        if_search_users_489_users_less_than_1_490 >> rail.Label(
            'No') >> if_search_users_489_users_greater_than_0_492
        if_search_users_489_users_greater_than_0_492 >> rail.Label(
            'Yes') >> log_useruribasedonthesuffix_493 >> if_log_useruribasedonthesuffix_493_present_494
        if_log_useruribasedonthesuffix_493_present_494 >> rail.Label(
            'Yes') >> log_userstatusbasedonthesuffix_495 >> if_log_userstatusbasedonthesuffix_495_equals_to_false_496
        if_log_userstatusbasedonthesuffix_495_equals_to_false_496 >> rail.Label(
            'Yes') >> updatetoprimaryprofile_set_s_s_o_authentication_for_user_497 >> re_enable_userprofile_498 >> \
            update_emailaddingemail_499 >> update_user_end_dateremoveenddate_500 >> trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile501 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile501 >> if_log_useruribasedonthesuffix_493_blank_502
        if_log_userstatusbasedonthesuffix_495_equals_to_false_496 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_493_blank_502
        if_log_useruribasedonthesuffix_493_present_494 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_493_blank_502
        if_log_useruribasedonthesuffix_493_blank_502 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update503 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update503 >> gather_user_uri_503 >> update_variable_primaryprofileuri_504
        if_log_useruribasedonthesuffix_493_blank_502 >> rail.Label(
            'No') >> update_variable_primaryprofileuri_504
        if_search_users_489_users_greater_than_0_492 >> rail.Label(
            'No') >> update_variable_primaryprofileuri_504 >> if_request_memberof_contains_delegate_only_delegate_505
        if_request_memberof_contains_c4_487 >> rail.Label(
            'No') >> if_request_memberof_contains_delegate_only_delegate_505
        if_request_memberof_contains_delegate_only_delegate_505 >> rail.Label(
            'Yes') >> log_loginnameprimaryold_506 >> search_users_507 >> if_search_users_507_users_less_than_1_508
        if_search_users_507_users_less_than_1_508 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update509 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update509 >> gather_user_uri_509 >> if_search_users_507_users_greater_than_0_510
        if_search_users_507_users_less_than_1_508 >> rail.Label(
            'No') >> if_search_users_507_users_greater_than_0_510
        if_search_users_507_users_greater_than_0_510 >> rail.Label(
            'Yes') >> log_useruribasedonthesuffix_511 >> if_log_useruribasedonthesuffix_511_present_512
        if_log_useruribasedonthesuffix_511_present_512 >> rail.Label(
            'Yes') >> log_userstatusbasedonthesuffix_513 >> if_log_userstatusbasedonthesuffix_513_not_equals_to_false_514
        if_log_userstatusbasedonthesuffix_513_not_equals_to_false_514 >> rail.Label(
            'Yes') >> updatetoprimaryprofile_set_s_s_o_authentication_for_user_515 >> re_enable_userprofile_516 >> update_emailaddingemail_517 >> \
            update_user_end_dateremoveenddate_518 >> trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile519 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile519 >> if_log_useruribasedonthesuffix_511_blank_520
        if_log_userstatusbasedonthesuffix_513_not_equals_to_false_514 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_511_blank_520
        if_log_useruribasedonthesuffix_511_present_512 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_511_blank_520
        if_log_useruribasedonthesuffix_511_blank_520 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update521 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update521 >> gather_user_uri_521 >> update_variable_primaryprofileuri_522
        if_log_useruribasedonthesuffix_511_blank_520 >> rail.Label(
            'No') >> update_variable_primaryprofileuri_522
        if_search_users_507_users_greater_than_0_510 >> rail.Label(
            'No') >> update_variable_primaryprofileuri_522 >> if_request_memberof_contains_c4_delegateand_c42profiles_523
        if_request_memberof_contains_c4_delegateand_c42profiles_523 >> rail.Label(
            'Yes') >> log_loginnameprimaryold_524 >> search_users_525 >> if_search_users_525_users_less_than_1_526
        if_search_users_525_users_less_than_1_526 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update527 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update527 >> gather_user_uri_527 >> if_search_users_525_users_greater_than_0_528
        if_search_users_525_users_less_than_1_526 >> rail.Label(
            'No') >> if_search_users_525_users_greater_than_0_528
        if_search_users_525_users_greater_than_0_528 >> rail.Label(
            'Yes') >> log_useruribasedonthesuffix_529 >> if_log_useruribasedonthesuffix_529_present_530
        if_log_useruribasedonthesuffix_529_present_530 >> rail.Label(
            'Yes') >> log_userstatusbasedonthesuffix_531 >> if_log_userstatusbasedonthesuffix_531_is_not_true_532
        if_log_userstatusbasedonthesuffix_531_is_not_true_532 >> rail.Label(
            'Yes') >> re_enable_userprofile_533 >> update_user_end_dateremoveenddate_534 >> \
            trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile535 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile535 >> \
            if_log_useruribasedonthesuffix_529_blank_536
        if_log_userstatusbasedonthesuffix_531_is_not_true_532 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_529_blank_536
        if_log_useruribasedonthesuffix_529_present_530 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_529_blank_536
        if_log_useruribasedonthesuffix_529_blank_536 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update537 >> wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update537 >> \
            gather_user_uri_537 >> log_childuseruri_538
        if_log_useruribasedonthesuffix_529_blank_536 >> rail.Label(
            'No') >> log_childuseruri_538
        if_search_users_525_users_greater_than_0_528 >> rail.Label(
            'No') >> log_childuseruri_538 >> get_all_substitute_user_assignments_for_user_539 >> \
            log_substituteuserassigned_540 >> if_log_substituteuserassigned_540_blank_541
        if_log_substituteuserassigned_540_blank_541 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_assign_substitute_usersv2544 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2544 >> \
            foreach_accumulate_list_items_16_545
        if_log_substituteuserassigned_540_blank_541 >> rail.Label(
            'No') >> foreach_accumulate_list_items_16_545
        if_request_memberof_contains_c4_delegateand_c42profiles_523 >> rail.Label(
            'No') >> foreach_accumulate_list_items_16_545
        if_request_memberof_contains_delegate_only_delegate_505 >> rail.Label(
            'No') >> foreach_accumulate_list_items_16_545 >> if_foreach_3157e122_545_type_not_equals_to_federallegislative_546
        if_foreach_3157e122_545_type_not_equals_to_federallegislative_546 >> rail.Label(
            'Yes') >> disable_loginoldprofiles_547 >> foreach_accumulate_list_items_16_545_end
        if_foreach_3157e122_545_type_not_equals_to_federallegislative_546 >> rail.Label(
            'No') >> foreach_accumulate_list_items_16_545_end
        foreach_accumulate_list_items_16_545 >> foreach_accumulate_list_items_16_545_end >> \
            if_request_currentprofilecount_equals_to_5_c3_c4to_c4_c3and_delegate7profiles_548
        if_request_currentprofilecount_equals_to_5_c3_c4to_c41profiles2_profiles_478 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_5_c3_c4to_c4_c3and_delegate7profiles_548
        if_request_currentprofilecount_equals_to_5_c3_c4to_c4_c3and_delegate7profiles_548 >> rail.Label(
            'Yes') >> update_variable_549 >> if_declare_variable_41_value_equals_to_7_550
        if_declare_variable_41_value_equals_to_7_550 >> rail.Label(
            'Yes') >> log_loginnamefromemailprimaryprofile_551 >> log_userurimainprofile_552 >> \
            declare_variable_553 >> if_log_loginnamefromemailprimaryprofile_551_present_554
        if_log_loginnamefromemailprimaryprofile_551_present_554 >> rail.Label(
            'Yes') >> updateuserloginname_set_replicon_authentication_for_user_555 >> \
            update_email_removingemail_556 >> if_request_memberof_contains_delegate_only_delegate_557
        if_log_loginnamefromemailprimaryprofile_551_present_554 >> rail.Label(
            'No') >> if_request_memberof_contains_delegate_only_delegate_557
        if_request_memberof_contains_delegate_only_delegate_557 >> rail.Label(
            'Yes') >> log_loginnameprimarynewd_558 >> search_users_559 >> if_search_users_559_users_less_than_1_560
        if_search_users_559_users_less_than_1_560 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update561 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update561 >> if_search_users_559_users_greater_than_0_562
        if_search_users_559_users_less_than_1_560 >> rail.Label(
            'No') >> if_search_users_559_users_greater_than_0_562
        if_search_users_559_users_greater_than_0_562 >> rail.Label(
            'Yes') >> log_useruribasedonthesuffix_563 >> if_log_useruribasedonthesuffix_563_present_564
        if_log_useruribasedonthesuffix_563_present_564 >> rail.Label(
            'Yes') >> log_userstatusbasedonthesuffix_565 >> if_log_userstatusbasedonthesuffix_565_equals_to_false_566
        if_log_userstatusbasedonthesuffix_565_equals_to_false_566 >> rail.Label(
            'Yes') >> updatetoprimaryprofile_set_s_s_o_authentication_for_user_567 >> re_enable_userprofile_568 >> update_emailaddingemail_569 >> \
            update_user_end_dateremoveenddate_570 >> trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile571 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile571 >> if_log_useruribasedonthesuffix_563_blank_572
        if_log_userstatusbasedonthesuffix_565_equals_to_false_566 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_563_blank_572
        if_log_useruribasedonthesuffix_563_present_564 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_563_blank_572
        if_log_useruribasedonthesuffix_563_blank_572 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update573 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update573 >> update_variable_primaryprofileuri_574
        if_log_useruribasedonthesuffix_563_blank_572 >> rail.Label(
            'No') >> update_variable_primaryprofileuri_574
        if_search_users_559_users_greater_than_0_562 >> rail.Label(
            'No') >> update_variable_primaryprofileuri_574 >> if_request_memberof_contains_c4_delegateand_c42profiles_575
        if_request_memberof_contains_c4_delegateand_c42profiles_575 >> rail.Label(
            'Yes') >> log_loginnameprimaryold_576 >> search_users_577 >> if_search_users_577_users_less_than_1_578
        if_search_users_577_users_less_than_1_578 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update579 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update579 >> gather_user_uri_579 >> \
            if_search_users_577_users_greater_than_0_580
        if_search_users_577_users_less_than_1_578 >> rail.Label(
            'No') >> if_search_users_577_users_greater_than_0_580
        if_search_users_577_users_greater_than_0_580 >> rail.Label(
            'Yes') >> log_useruribasedonthesuffix_581 >> if_log_useruribasedonthesuffix_581_present_582
        if_log_useruribasedonthesuffix_581_present_582 >> rail.Label(
            'Yes') >> log_userstatusbasedonthesuffix_583 >> if_log_userstatusbasedonthesuffix_583_equals_to_false_584
        if_log_userstatusbasedonthesuffix_583_equals_to_false_584 >> rail.Label(
            'Yes') >> re_enable_userprofile_585 >> update_user_end_dateremoveenddate_586 >> \
            trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile587 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile587 >> if_log_useruribasedonthesuffix_581_blank_588
        if_log_userstatusbasedonthesuffix_583_equals_to_false_584 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_581_blank_588
        if_log_useruribasedonthesuffix_581_present_582 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_581_blank_588
        if_log_useruribasedonthesuffix_581_blank_588 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update589 >> wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update589 >> \
            gather_user_uri_589 >> log_childuseruri_590
        if_log_useruribasedonthesuffix_581_blank_588 >> rail.Label(
            'No') >> log_childuseruri_590
        if_search_users_577_users_greater_than_0_580 >> rail.Label(
            'No') >> log_childuseruri_590 >> get_all_substitute_user_assignments_for_user_591 >> \
            log_substituteuserassigned_592 >> if_log_substituteuserassigned_592_blank_593
        if_log_substituteuserassigned_592_blank_593 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_assign_substitute_usersv2596 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2596 >> declare_list_update_dag_runs_597
        if_log_substituteuserassigned_592_blank_593 >> rail.Label(
            'No') >> declare_list_update_dag_runs_597
        if_request_memberof_contains_c4_delegateand_c42profiles_575 >> rail.Label(
            'No') >> declare_list_update_dag_runs_597 >> foreach_accumulate_list_items_16_597 >> \
            trigger_dag_run_live_nrdc_assign_substitute_usersv2600 >> insert_to_user_dag_run_list_597 >> \
            foreach_accumulate_list_items_16_597_end
        foreach_accumulate_list_items_16_597 >> foreach_accumulate_list_items_16_597_end >> \
            wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2600 >> \
            if_request_currentprofilecount_equals_to_5_delegate_c3to_delegate_c36profiles_601
        if_request_memberof_contains_delegate_only_delegate_557 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_5_delegate_c3to_delegate_c36profiles_601
        if_declare_variable_41_value_equals_to_7_550 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_5_delegate_c3to_delegate_c36profiles_601
        if_request_currentprofilecount_equals_to_5_c3_c4to_c4_c3and_delegate7profiles_548 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_5_delegate_c3to_delegate_c36profiles_601
        if_request_currentprofilecount_equals_to_5_delegate_c3to_delegate_c36profiles_601 >> rail.Label(
            'Yes') >> update_variable_602 >> if_declare_variable_41_value_equals_to_6_603
        if_declare_variable_41_value_equals_to_6_603 >> rail.Label(
            'Yes') >> log_loginnameforprimaryprofile_604 >> log_useruriforprimaryprofile_605 >> if_log_useruriforprimaryprofile_605_present_606
        if_log_useruriforprimaryprofile_605_present_606 >> rail.Label(
            'Yes') >> updateuserloginname_set_replicon_authentication_for_user_607 >> update_email_removingemail_608 >> log_loginnameprimaryold_609
        if_log_useruriforprimaryprofile_605_present_606 >> rail.Label(
            'No') >> log_loginnameprimaryold_609 >> search_users_610 >> if_search_users_610_users_less_than_1_611
        if_search_users_610_users_less_than_1_611 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update612 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update612 >> gather_user_uri_612 >> \
            if_search_users_610_users_greater_than_0_613
        if_search_users_610_users_less_than_1_611 >> rail.Label(
            'No') >> if_search_users_610_users_greater_than_0_613
        if_search_users_610_users_greater_than_0_613 >> rail.Label(
            'Yes') >> log_useruribasedonthesuffix_614 >> if_log_useruribasedonthesuffix_614_present_615
        if_log_useruribasedonthesuffix_614_present_615 >> rail.Label(
            'Yes') >> log_userstatusbasedonthesuffix_616 >> if_log_userstatusbasedonthesuffix_616_equals_to_false_617
        if_log_userstatusbasedonthesuffix_616_equals_to_false_617 >> rail.Label(
            'Yes') >> re_enable_userprofile_618 >> updatetoprimaryprofile_set_s_s_o_authentication_for_user_619 >> update_emailaddingemail_620 >> \
            update_user_end_dateremoveenddate_621 >> trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile622 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile622 >> if_log_useruribasedonthesuffix_614_blank_623
        if_log_userstatusbasedonthesuffix_616_equals_to_false_617 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_614_blank_623
        if_log_useruribasedonthesuffix_614_present_615 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_614_blank_623
        if_log_useruribasedonthesuffix_614_blank_623 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update624 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update624 >> gather_user_uri_625 >> log_primaryprofileuri_625
        if_log_useruribasedonthesuffix_614_blank_623 >> rail.Label(
            'No') >> log_primaryprofileuri_625 >> declare_list_update_dag_runs_626 >> foreach_accumulate_list_items_16_626 >> \
            get_all_substitute_user_assignments_for_user_627 >> \
            log_substituteuserassigned_628 >> if_log_substituteuserassigned_628_blank_629
        if_log_substituteuserassigned_628_blank_629 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_assign_substitute_usersv2632 >> \
            insert_to_user_dag_run_list_632 >> \
            foreach_accumulate_list_items_16_626_end
        if_log_substituteuserassigned_628_blank_629 >> rail.Label(
            'No') >> foreach_accumulate_list_items_16_626_end
        foreach_accumulate_list_items_16_626 >> foreach_accumulate_list_items_16_626_end >> \
            wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2632 >> \
            if_request_currentprofilecount_equals_to_6_delegate_c3or_c4_c3to_delegate_c3_c47profiles_633
        if_search_users_610_users_greater_than_0_613 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_6_delegate_c3or_c4_c3to_delegate_c3_c47profiles_633
        if_declare_variable_41_value_equals_to_6_603 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_6_delegate_c3or_c4_c3to_delegate_c3_c47profiles_633
        if_request_currentprofilecount_equals_to_5_delegate_c3to_delegate_c36profiles_601 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_6_delegate_c3or_c4_c3to_delegate_c3_c47profiles_633
        if_request_currentprofilecount_equals_to_6_delegate_c3or_c4_c3to_delegate_c3_c47profiles_633 >> rail.Label(
            'Yes') >> update_variable_634 >> if_declare_variable_41_value_equals_to_7_635
        if_declare_variable_41_value_equals_to_7_635 >> rail.Label(
            'Yes') >> log_primaryprofileloginnameif_c4isprimaryprofile_636 >> log_primaryprofileloginnameif_delegateisprimaryprofile_637 >> \
            log_loginnamefromemailprimaryprofile_638 >> log_userurimainprofile_639 >> \
            if_log_primaryprofileloginnameif_c4isprimaryprofile_636_present_whenc4isprimary_640
        if_log_primaryprofileloginnameif_c4isprimaryprofile_636_present_whenc4isprimary_640 >> rail.Label(
            'Yes') >> updateuserloginname_set_replicon_authentication_for_user_641 >> update_email_removingemail_642 >> log_loginnameprimaryoldd_643 >> \
            search_users_644 >> if_search_users_644_users_less_than_1_645
        if_search_users_644_users_less_than_1_645 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update646 >> wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update646 >> \
            gather_user_uri_646 >> \
            if_log_primaryprofileloginnameif_delegateisprimaryprofile_637_present_whendelegateisprimaryc4addedupdated_673
        if_search_users_644_users_less_than_1_645 >> rail.Label(
            'No') >> if_search_users_644_users_greater_than_0_647
        if_search_users_644_users_greater_than_0_647 >> rail.Label(
            'Yes') >> log_useruribasedonthesuffix_648 >> if_log_useruribasedonthesuffix_648_present_649
        if_log_useruribasedonthesuffix_648_present_649 >> rail.Label(
            'Yes') >> log_userstatusbasedonthesuffix_650 >> if_log_userstatusbasedonthesuffix_650_equals_to_false_651
        if_log_userstatusbasedonthesuffix_650_equals_to_false_651 >> rail.Label(
            'Yes') >> re_enable_userprofile_652 >> updatetoprimaryprofile_set_s_s_o_authentication_for_user_653 >> update_emailaddingemail_654 >> \
            trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile655 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile655 >> if_log_useruribasedonthesuffix_648_blank_656
        if_log_userstatusbasedonthesuffix_650_equals_to_false_651 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_648_blank_656
        if_log_useruribasedonthesuffix_648_present_649 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_648_blank_656
        if_log_useruribasedonthesuffix_648_blank_656 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update657 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update657 >> gather_user_uri_658 >> log_newprimaryuseruri_658
        if_log_useruribasedonthesuffix_648_blank_656 >> rail.Label(
            'No') >> log_newprimaryuseruri_658
        if_search_users_644_users_greater_than_0_647 >> rail.Label(
            'No') >> log_newprimaryuseruri_658 >> declare_list_update_dag_runs_666 >> \
            foreach_accumulate_list_items_16_666 >> get_all_substitute_user_assignments_for_user_667 >> \
            log_substituteuserassigned_668 >> if_log_substituteuserassigned_668_blank_669
        if_log_substituteuserassigned_668_blank_669 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_assign_substitute_usersv2672 >> \
            insert_to_user_dag_run_list_666 >> \
            foreach_accumulate_list_items_16_666_end
        if_log_substituteuserassigned_668_blank_669 >> rail.Label(
            'No') >> foreach_accumulate_list_items_16_666_end
        foreach_accumulate_list_items_16_666 >> foreach_accumulate_list_items_16_666_end >> \
            wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2672 >> \
            if_log_primaryprofileloginnameif_delegateisprimaryprofile_637_present_whendelegateisprimaryc4addedupdated_673
        if_log_primaryprofileloginnameif_c4isprimaryprofile_636_present_whenc4isprimary_640 >> rail.Label(
            'No') >> if_log_primaryprofileloginnameif_delegateisprimaryprofile_637_present_whendelegateisprimaryc4addedupdated_673
        if_log_primaryprofileloginnameif_delegateisprimaryprofile_637_present_whendelegateisprimaryc4addedupdated_673 >> rail.Label(
            'Yes') >> log_loginnamewithaf_674 >> search_users_675 >> if_search_users_675_users_less_than_1_676
        if_search_users_675_users_less_than_1_676 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update677 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update677 >> if_search_users_644_users_greater_than_0_678
        if_search_users_675_users_less_than_1_676 >> rail.Label(
            'No') >> if_search_users_644_users_greater_than_0_678
        if_search_users_644_users_greater_than_0_678 >> rail.Label(
            'Yes') >> log_useruribasedonthesuffix_679 >> if_log_useruribasedonthesuffix_679_present_680
        if_log_useruribasedonthesuffix_679_present_680 >> rail.Label(
            'Yes') >> log_userstatusbasedonthesuffix_681 >> if_log_userstatusbasedonthesuffix_681_equals_to_false_682
        if_log_useruribasedonthesuffix_679_present_680 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_679_blank_691
        if_log_userstatusbasedonthesuffix_681_equals_to_false_682 >> rail.Label(
            'Yes') >> re_enable_userprofile_683 >> get_all_substitute_user_assignments_for_user_684 >> \
            log_substituteuserassigned_685 >> if_log_substituteuserassigned_685_blank_686
        if_log_substituteuserassigned_685_blank_686 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_assign_substitute_usersv2689 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2689 >> trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile690
        if_log_substituteuserassigned_685_blank_686 >> rail.Label(
            'No') >> trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile690 >> \
            wait_for_completion_trigger_dag_run_live_nrdc_update_rehire_disable_user_basic_profile690 >> if_log_useruribasedonthesuffix_679_blank_691
        if_log_userstatusbasedonthesuffix_681_equals_to_false_682 >> rail.Label(
            'No') >> if_log_useruribasedonthesuffix_679_blank_691
        if_log_useruribasedonthesuffix_679_blank_691 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_6_only_c4_c3_c4toonly_c41profiles_701
        if_log_useruribasedonthesuffix_679_blank_691 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_basic_add_update692 >> wait_for_completion_trigger_dag_run_live_nrdc_basic_add_update692 >> \
            if_request_currentprofilecount_equals_to_6_only_c4_c3_c4toonly_c41profiles_701
        if_request_currentprofilecount_equals_to_6_only_c4_c3_c4toonly_c41profiles_701 >> rail.Label('No') >> \
            if_request_currentprofilecount_equals_to_6_c3_delegatetoonly_delegate1profiles_707

        if_search_users_644_users_greater_than_0_678 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_6_only_c4_c3_c4toonly_c41profiles_701
        if_log_primaryprofileloginnameif_delegateisprimaryprofile_637_present_whendelegateisprimaryc4addedupdated_673 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_6_only_c4_c3_c4toonly_c41profiles_701
        if_declare_variable_41_value_equals_to_7_635 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_6_only_c4_c3_c4toonly_c41profiles_701
        if_request_currentprofilecount_equals_to_6_delegate_c3or_c4_c3to_delegate_c3_c47profiles_633 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_6_only_c4_c3_c4toonly_c41profiles_701
        if_request_currentprofilecount_equals_to_6_only_c4_c3_c4toonly_c41profiles_701 >> rail.Label(
            'Yes') >> update_variable_702 >> if_declare_variable_41_value_equals_to_1_703
        if_declare_variable_41_value_equals_to_1_703 >> rail.Label(
            'Yes') >> foreach_accumulate_list_items_16_704 >> if_foreach_3157e122_704_type_not_equals_to_c4_705
        if_foreach_3157e122_704_type_not_equals_to_c4_705 >> rail.Label(
            'Yes') >> disable_loginoldprimaryprofile_706 >> foreach_accumulate_list_items_16_704_end
        if_foreach_3157e122_704_type_not_equals_to_c4_705 >> rail.Label(
            'No') >> foreach_accumulate_list_items_16_704_end
        foreach_accumulate_list_items_16_704 >> foreach_accumulate_list_items_16_704_end >> \
            if_request_currentprofilecount_equals_to_6_c3_delegatetoonly_delegate1profiles_707
        if_declare_variable_41_value_equals_to_1_703 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_6_c3_delegatetoonly_delegate1profiles_707
        if_request_currentprofilecount_equals_to_6_only_c4_c3_c4toonly_c41profiles_701 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_6_c3_delegatetoonly_delegate1profiles_707
        if_request_currentprofilecount_equals_to_6_c3_delegatetoonly_delegate1profiles_707 >> rail.Label(
            'Yes') >> update_variable_708 >> if_declare_variable_41_value_equals_to_1_709
        if_declare_variable_41_value_equals_to_1_709 >> rail.Label(
            'Yes') >> foreach_accumulate_list_items_16_710 >> if_foreach_3157e122_710_type_not_equals_to_delegate_711
        if_foreach_3157e122_710_type_not_equals_to_delegate_711 >> rail.Label(
            'Yes') >> disable_loginoldprimaryprofile_712 >> foreach_accumulate_list_items_16_710_end
        if_foreach_3157e122_710_type_not_equals_to_delegate_711 >> rail.Label(
            'No') >> foreach_accumulate_list_items_16_710_end
        foreach_accumulate_list_items_16_710 >> foreach_accumulate_list_items_16_710_end >> \
            if_request_currentprofilecount_equals_to_6_o_n_l_y_c3_c3_c4to_c35profiles_delegate_c3to_c3_713
        if_declare_variable_41_value_equals_to_1_709 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_6_o_n_l_y_c3_c3_c4to_c35profiles_delegate_c3to_c3_713
        if_request_currentprofilecount_equals_to_6_c3_delegatetoonly_delegate1profiles_707 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_6_o_n_l_y_c3_c3_c4to_c35profiles_delegate_c3to_c3_713
        if_request_currentprofilecount_equals_to_6_o_n_l_y_c3_c3_c4to_c35profiles_delegate_c3to_c3_713 >> rail.Label(
            'Yes') >> update_variable_714 >> if_declare_variable_41_value_equals_to_5_715
        if_declare_variable_41_value_equals_to_5_715 >> rail.Label(
            'Yes') >> log_primaryprofileloginnameif_c4isprimaryprofile_716 >> log_primaryprofileloginnameif_delegateisprimaryprofile_717 >> \
            log_requiredprimaryprofileloginname_718 >> log_requiredprimaryprofile_uri_719 >> \
            declare_list_update_dag_runs_720 >> foreach_accumulate_list_items_16_720 >> \
            if_foreach_3157e122_720_type_equals_to_c4_721
        if_foreach_3157e122_720_type_equals_to_c4_721 >> rail.Label(
            'Yes') >> updateuserloginname_set_replicon_authentication_for_user_722 >> update_email_removingemail_723 >> disable_loginoldprimaryprofile_724 >> \
            update_user_end_date_725 >> if_foreach_3157e122_720_type_equals_to_delegate_726
        if_foreach_3157e122_720_type_equals_to_c4_721 >> rail.Label(
            'No') >> if_foreach_3157e122_720_type_equals_to_delegate_726
        if_foreach_3157e122_720_type_equals_to_delegate_726 >> rail.Label(
            'Yes') >> updateuserloginname_set_replicon_authentication_for_user_727 >> update_email_removingemail_728 >> \
            disable_loginoldprimaryprofile_729 >> update_user_end_date_730 >> if_foreach_3157e122_720_type_equals_to_federallegislative_731
        if_foreach_3157e122_720_type_equals_to_delegate_726 >> rail.Label(
            'No') >> if_foreach_3157e122_720_type_equals_to_federallegislative_731
        if_foreach_3157e122_720_type_equals_to_federallegislative_731 >> rail.Label(
            'Yes') >> log_primaryprofileuri_732 >> updatetoprimaryprofile_set_s_s_o_authentication_for_user_733 >> update_emailaddingemail_734 >> \
            if_foreach_3157e122_720_type_not_equals_to_federallegislative_735
        if_foreach_3157e122_720_type_equals_to_federallegislative_731 >> rail.Label(
            'No') >> if_foreach_3157e122_720_type_not_equals_to_federallegislative_735
        if_foreach_3157e122_720_type_not_equals_to_federallegislative_735 >> rail.Label(
            'Yes') >> get_all_substitute_user_assignments_for_user_736 >> log_substituteuserassigned_737 >> if_log_substituteuserassigned_737_blank_738
        if_log_substituteuserassigned_737_blank_738 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_assign_substitute_usersv2741 >> \
            insert_to_user_dag_run_list_720 >> \
            foreach_accumulate_list_items_16_720_end
        if_log_substituteuserassigned_737_blank_738 >> rail.Label(
            'No') >> foreach_accumulate_list_items_16_720_end
        if_foreach_3157e122_720_type_not_equals_to_federallegislative_735 >> rail.Label(
            'No') >> foreach_accumulate_list_items_16_720_end
        foreach_accumulate_list_items_16_720 >> foreach_accumulate_list_items_16_720_end >> \
            wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2741 >> \
            if_request_currentprofilecount_equals_to_7_c3_c4anddelegatetodelegate1profiles_742
        if_declare_variable_41_value_equals_to_5_715 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_7_c3_c4anddelegatetodelegate1profiles_742
        if_request_currentprofilecount_equals_to_6_o_n_l_y_c3_c3_c4to_c35profiles_delegate_c3to_c3_713 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_7_c3_c4anddelegatetodelegate1profiles_742
        if_request_currentprofilecount_equals_to_7_c3_c4anddelegatetodelegate1profiles_742 >> rail.Label(
            'Yes') >> update_variable_743 >> if_declare_variable_41_value_equals_to_1_744
        if_declare_variable_41_value_equals_to_1_744 >> rail.Label(
            'Yes') >> foreach_accumulate_list_items_16_745 >> if_foreach_3157e122_745_type_not_equals_to_delegate_746
        if_foreach_3157e122_745_type_not_equals_to_delegate_746 >> rail.Label(
            'Yes') >> disable_loginoldprimaryprofile_747 >> foreach_accumulate_list_items_16_745_end
        if_foreach_3157e122_745_type_not_equals_to_delegate_746 >> rail.Label(
            'No') >> foreach_accumulate_list_items_16_745_end
        foreach_accumulate_list_items_16_745 >> foreach_accumulate_list_items_16_745_end >> \
            if_request_currentprofilecount_equals_to_7_c3_c4and_delegateto_c35profiles_748
        if_declare_variable_41_value_equals_to_1_744 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_7_c3_c4and_delegateto_c35profiles_748
        if_request_currentprofilecount_equals_to_7_c3_c4anddelegatetodelegate1profiles_742 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_7_c3_c4and_delegateto_c35profiles_748
        if_request_currentprofilecount_equals_to_7_c3_c4and_delegateto_c35profiles_748 >> rail.Label(
            'Yes') >> update_variable_749 >> if_declare_variable_41_value_equals_to_5_750
        if_declare_variable_41_value_equals_to_5_750 >> rail.Label(
            'Yes') >> log_primaryprofileloginnameif_delegateisprimaryprofile_751 >> log_requiredprimaryprofile_uri_752 >> \
            declare_list_update_dag_runs_753 >> \
            foreach_accumulate_list_items_16_753 >> if_foreach_3157e122_753_type_equals_to_c4_754
        if_foreach_3157e122_753_type_equals_to_c4_754 >> rail.Label(
            'Yes') >> disable_loginoldprimaryprofile_755 >> if_foreach_3157e122_753_type_equals_to_delegate_756
        if_foreach_3157e122_753_type_equals_to_c4_754 >> rail.Label(
            'No') >> if_foreach_3157e122_753_type_equals_to_delegate_756
        if_foreach_3157e122_753_type_equals_to_delegate_756 >> rail.Label(
            'Yes') >> updateuserloginname_set_replicon_authentication_for_user_757 >> update_email_removingemail_758 >> disable_loginoldprimaryprofile_759 >> \
            update_user_end_date_760 >> if_foreach_3157e122_753_type_equals_to_federallegislative_761
        if_foreach_3157e122_753_type_equals_to_delegate_756 >> rail.Label(
            'No') >> if_foreach_3157e122_753_type_equals_to_federallegislative_761
        if_foreach_3157e122_753_type_equals_to_federallegislative_761 >> rail.Label(
            'Yes') >> log_primaryprofileuri_762 >> updatetoprimaryprofile_set_s_s_o_authentication_for_user_763 >> update_emailaddingemail_764 >> \
            if_foreach_3157e122_753_type_not_equals_to_federallegislative_765
        if_foreach_3157e122_753_type_equals_to_federallegislative_761 >> rail.Label(
            'No') >> if_foreach_3157e122_753_type_not_equals_to_federallegislative_765
        if_foreach_3157e122_753_type_not_equals_to_federallegislative_765 >> rail.Label(
            'Yes') >> get_all_substitute_user_assignments_for_user_766 >> log_substituteuserassigned_767 >> if_log_substituteuserassigned_767_blank_768
        if_log_substituteuserassigned_767_blank_768 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_assign_substitute_usersv2771 >> \
            insert_to_user_dag_run_list_753 >> \
            foreach_accumulate_list_items_16_753_end
        if_log_substituteuserassigned_767_blank_768 >> rail.Label(
            'No') >> foreach_accumulate_list_items_16_753_end
        if_foreach_3157e122_753_type_not_equals_to_federallegislative_765 >> rail.Label(
            'No') >> foreach_accumulate_list_items_16_753_end
        foreach_accumulate_list_items_16_753 >> foreach_accumulate_list_items_16_753_end >> \
            wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2771 >> \
            if_request_currentprofilecount_equals_to_7_c4c3and_delegateto_c3_c4profile6profiles_772
        if_declare_variable_41_value_equals_to_5_750 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_7_c4c3and_delegateto_c3_c4profile6profiles_772
        if_request_currentprofilecount_equals_to_7_c3_c4and_delegateto_c35profiles_748 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_7_c4c3and_delegateto_c3_c4profile6profiles_772
        if_request_currentprofilecount_equals_to_7_c4c3and_delegateto_c3_c4profile6profiles_772 >> rail.Label(
            'Yes') >> update_variable_773 >> if_declare_variable_41_value_equals_to_6_774
        if_declare_variable_41_value_equals_to_6_774 >> rail.Label(
            'Yes') >> query_list_wherevalueis_delegate_775 >> get_first_records_from_query_775 >> if_first_type_present_delegate_776
        if_first_type_present_delegate_776 >> rail.Label(
            'Yes') >> updateuserloginname_set_replicon_authentication_for_user_777 >> update_email_removingemail_778 >> \
            disable_loginoldprimaryprofile_779 >> update_user_end_date_780 >> log_primaryprofileloginnameifdelegateisprimaryprofile_781
        if_first_type_present_delegate_776 >> rail.Label(
            'No') >> log_primaryprofileloginnameifdelegateisprimaryprofile_781 >> \
            log_requiredprimaryprofile_uri_782 >> query_list_wherevalueisc4_783 >> get_first_records_from_query_783 >> if_first_type_present_c4_784
        if_first_type_present_c4_784 >> rail.Label(
            'Yes') >> log_newprimaryprofileuri_785 >> updatetoprimaryprofile_set_s_s_o_authentication_for_user_786 >> \
            update_emailaddemail_787 >> declare_list_update_dag_runs_788
        if_first_type_present_c4_784 >> rail.Label(
            'No') >> declare_list_update_dag_runs_788 >> foreach_accumulate_list_items_16_788 >> if_foreach_3157e122_788_type_not_equals_to_c4_789
        if_foreach_3157e122_788_type_not_equals_to_c4_789 >> rail.Label(
            'Yes') >> get_all_substitute_user_assignments_for_user_790 >> log_substituteuserassigned_791 >> if_log_substituteuserassigned_791_blank_792
        if_log_substituteuserassigned_791_blank_792 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_assign_substitute_usersv2795 >> insert_to_user_dag_run_list_788 >> \
            foreach_accumulate_list_items_16_788_end
        if_log_substituteuserassigned_791_blank_792 >> rail.Label(
            'No') >> foreach_accumulate_list_items_16_788_end
        if_foreach_3157e122_788_type_not_equals_to_c4_789 >> rail.Label(
            'No') >> foreach_accumulate_list_items_16_788_end
        foreach_accumulate_list_items_16_788 >> foreach_accumulate_list_items_16_788_end >> \
            wait_for_completion_trigger_dag_run_live_nrdc_assign_substitute_usersv2795 >> \
            if_request_currentprofilecount_equals_to_7_c4c3and_delegateto_c4profile1profiles_796
        if_declare_variable_41_value_equals_to_6_774 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_7_c4c3and_delegateto_c4profile1profiles_796
        if_request_currentprofilecount_equals_to_7_c4c3and_delegateto_c3_c4profile6profiles_772 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_7_c4c3and_delegateto_c4profile1profiles_796
        if_request_currentprofilecount_equals_to_7_c4c3and_delegateto_c4profile1profiles_796 >> rail.Label(
            'Yes') >> update_variable_797 >> if_declare_variable_41_value_equals_to_1_798
        if_declare_variable_41_value_equals_to_1_798 >> rail.Label(
            'Yes') >> query_list_wherevalueis_delegate_799 >> get_first_records_from_query_799 >> if_first_type_present_delegate_800
        if_first_type_present_delegate_800 >> rail.Label(
            'Yes') >> updateuserloginname_set_replicon_authentication_for_user_801 >> update_email_removingemail_802 >> \
            disable_loginoldprimaryprofile_803 >> update_user_end_date_804 >> log_primaryprofileloginnameifdelegateisprimaryprofile_805
        if_first_type_present_delegate_800 >> rail.Label(
            'No') >> log_primaryprofileloginnameifdelegateisprimaryprofile_805 >> log_requiredprimaryprofile_uri_806 >> \
            query_list_wherevalueisc4_807 >> get_first_records_from_query_807 >> if_first_type_present_c4_808
        if_first_type_present_c4_808 >> rail.Label(
            'Yes') >> updatetoprimaryprofile_set_s_s_o_authentication_for_user_809 >> update_emailaddemail_810 >> foreach_accumulate_list_items_16_811
        if_first_type_present_c4_808 >> rail.Label(
            'No') >> foreach_accumulate_list_items_16_811 >> if_foreach_3157e122_811_type_not_equals_to_c4_812
        if_foreach_3157e122_811_type_not_equals_to_c4_812 >> rail.Label(
            'Yes') >> disable_loginoldprimaryprofile_813 >> foreach_accumulate_list_items_16_811_end
        if_foreach_3157e122_811_type_not_equals_to_c4_812 >> rail.Label(
            'No') >> foreach_accumulate_list_items_16_811_end
        foreach_accumulate_list_items_16_811 >> foreach_accumulate_list_items_16_811_end >> \
            if_request_currentprofilecount_equals_to_7_delegate_c4_c3to_delegate_c3profiles6profiles_814
        if_declare_variable_41_value_equals_to_1_798 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_7_delegate_c4_c3to_delegate_c3profiles6profiles_814
        if_request_currentprofilecount_equals_to_7_c4c3and_delegateto_c4profile1profiles_796 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_7_delegate_c4_c3to_delegate_c3profiles6profiles_814
        if_request_currentprofilecount_equals_to_7_delegate_c4_c3to_delegate_c3profiles6profiles_814 >> rail.Label(
            'Yes') >> update_variable_815 >> if_declare_variable_41_value_equals_to_6_816
        if_declare_variable_41_value_equals_to_6_816 >> rail.Label(
            'Yes') >> foreach_accumulate_list_items_16_817 >> if_foreach_3157e122_817_type_equals_to_c4_818
        if_foreach_3157e122_817_type_equals_to_c4_818 >> rail.Label(
            'Yes') >> disable_loginoldprimaryprofile_819 >> foreach_accumulate_list_items_16_817_end
        if_foreach_3157e122_817_type_equals_to_c4_818 >> rail.Label(
            'No') >> foreach_accumulate_list_items_16_817_end
        foreach_accumulate_list_items_16_817 >> foreach_accumulate_list_items_16_817_end >> \
            if_request_currentprofilecount_equals_to_7_c3_c4anddelegatetodelegateandc42profiles_820
        if_declare_variable_41_value_equals_to_6_816 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_7_c3_c4anddelegatetodelegateandc42profiles_820
        if_request_currentprofilecount_equals_to_7_delegate_c4_c3to_delegate_c3profiles6profiles_814 >> rail.Label(
            'No') >> if_request_currentprofilecount_equals_to_7_c3_c4anddelegatetodelegateandc42profiles_820
        if_request_currentprofilecount_equals_to_7_c3_c4anddelegatetodelegateandc42profiles_820 >> rail.Label(
            'Yes') >> update_variable_821 >> if_declare_variable_41_value_equals_to_2_822
        if_declare_variable_41_value_equals_to_2_822 >> rail.Label(
            'Yes') >> foreach_accumulate_list_items_16_823 >> if_foreach_3157e122_823_type_not_equals_to_delegate_824
        if_foreach_3157e122_823_type_not_equals_to_delegate_824 >> rail.Label(
            'Yes') >> disable_loginoldprimaryprofile_825 >> foreach_accumulate_list_items_16_823_end
        if_foreach_3157e122_823_type_not_equals_to_delegate_824 >> rail.Label(
            'No') >> foreach_accumulate_list_items_16_823_end
        foreach_accumulate_list_items_16_823 >> foreach_accumulate_list_items_16_823_end >> log_to_sumo
        if_declare_variable_41_value_equals_to_2_822 >> rail.Label(
            'No') >> log_to_sumo
        if_request_currentprofilecount_equals_to_7_c3_c4anddelegatetodelegateandc42profiles_820 >> rail.Label(
            'No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
