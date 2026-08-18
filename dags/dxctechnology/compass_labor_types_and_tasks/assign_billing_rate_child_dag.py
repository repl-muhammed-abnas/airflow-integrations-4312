
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_labor_types_and_tasks_assign_billingrate_child_{config.sub_erp_name}_{config.instance}',
        description=f'DXC_COMPASS_Labour Types and Task Assign Billing Rate Child {config.sub_erp_name}_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_dag_run_child_process,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config
        )

        def conf():
            return rail.get_current_context()['dag_run'].conf

        get_project_team_member_details = rail.RepliconServiceOperator(
            task_id='get_project_team_member_details',
            endpoint="/services/ProjectService1.svc/GetProjectTeamMemberDetails",
            data={
                "projectUri": "{{dag_run.conf.projecturi}}",
                "resourceUri": "{{dag_run.conf.useruri}}",
                "asOfDate": {
                    "year":  "{{ dag_run.conf.year }}",
                    "month":  "{{ dag_run.conf.month }}",
                    "day":  "{{ dag_run.conf.day }}"
                }
            }
        )

        get_existing_billing_rates = rail.PythonOperator(
            task_id='get_existing_billing_rates',
            python_callable=lambda: list(map(lambda x: x['billingRate']['uri'],
                                             rail.result('get_project_team_member_details')[
                'billingRatesAllowedForBillingTime'])) if conf()['billingratename'] else null
        )

        has_billing_rate_name = rail.IfOperator(
            task_id='has_billing_rate_name',
            test="{{ dag_run.conf.billingratename != '|Billable' and dag_run.conf.billingratename != '|Non-Billable' }}",
            yes_task="update_billing_rate_is_available_for_assignment_to_team_members",
            no_task="has_default_billingratename",
        )

        update_billing_rate_is_available_for_assignment_to_team_members = rail.RepliconServiceOperator(
            task_id='update_billing_rate_is_available_for_assignment_to_team_members',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/UpdateBillingRateIsAvailableForAssignmentToTeamMembers",
            data={
                "projectUri": "{{ dag_run.conf.projecturi }}",
                "billingRateUri": "{{ dag_run.conf.billingrateuri }}",
                "billingRateAvailableForAssignmentOptionUri": "urn:replicon:billing-rate-available-for-assignment-option:available"
            }
        )

        has_non_billable_rate = rail.IfOperator(
            task_id='has_non_billable_rate',
            test=lambda: conf()['billingratename'].endswith('|Non-Billable') and
            rail.find_first_by_attr_and_get_attr(
                rail.result('get_project_team_member_details')[
                    'billingRatesAllowedForBillingTime'],
                'billingRate.name',
                rail.get_current_context(
                )['dag_run'].conf['billingratename']+'|Billable',
                'billingRate.uri'
            ),
            yes_task="get_defaultbillingrateuri1",
            no_task="has_labortypepresent",
        )

        get_defaultbillingrateuri1 = rail.RenderTemplateOperator(
            task_id='get_defaultbillingrateuri1',
            template="{{ result('get_project_team_member_details').billingRatesAllowedForBillingTime | find_first_by_attr_and_get_attr('billingRate.name',dag_run.conf.billingratename + '|Billable','billingRate.uri') }}",
            target="result",
        )

        has_labortypepresent = rail.IfOperator(
            task_id='has_labortypepresent',
            test=lambda: conf()['labortypepresent'] == 'Yes' and
            conf()['default'] and
            rail.get_current_context(
            )['dag_run'].conf['billingratename'].endswith('|Billable'),
            yes_task="get_defaultbillingrateuri2",
            no_task="has_default_type",
        )

        get_defaultbillingrateuri2 = rail.RenderTemplateOperator(
            task_id='get_defaultbillingrateuri2',
            template="{{ dag_run.conf.billingrateuri}}",
            target="result",
        )

        has_default_type = rail.IfOperator(
            task_id='has_default_type',
            test="{{ dag_run.conf.default | is_truthy }}",
            yes_task="get_defaultbillingrateuri3",
            no_task="has_no_billingrate_uri",
        )

        get_defaultbillingrateuri3 = rail.RenderTemplateOperator(
            task_id='get_defaultbillingrateuri3',
            template="{{ dag_run.conf.billingrateuri}}",
            target="result",
        )

        has_no_billingrate_uri = rail.IfOperator(
            task_id='has_no_billingrate_uri',
            test="{{ result('get_existing_billing_rates') | is_falsy}}",
            yes_task="put_project_team_member_billing_rates_allowed_for_billing_time3",
            no_task="has_nobillingrateassigned",
        )

        def get_billingrateassignment_payload():
            billingRateUris = []
            billingRateUris.append(conf()['billingrateuri'])
            if rail.result('get_existing_billing_rates'):
                billingRateUris.extend(rail.result(
                    'get_existing_billing_rates'))

            return {
                "projectTeamMemberBillingRate": {
                    "projectUri": conf()['projecturi'],
                    "resourceUri": conf()['useruri'],
                    "billingRateUris": billingRateUris,
                    "billingRateCopyOptionUri": "urn:replicon:billing-rate-copy-option:do-not-copy-billing-rates-from-client",
                    "defaultBillingRateUri": rail.result('get_defaultbillingrateuri5') or
                    rail.result('get_defaultbillingrateuri4') or
                    rail.result('get_defaultbillingrateuri3') or
                    rail.result('get_defaultbillingrateuri2') or
                    rail.result('get_defaultbillingrateuri1'),
                }
            }

        put_project_team_member_billing_rates_allowed_for_billing_time3 = rail.RepliconServiceOperator(
            task_id='put_project_team_member_billing_rates_allowed_for_billing_time3',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/PutProjectTeamMemberBillingRatesAllowedForBillingTime3",
            data=get_billingrateassignment_payload)

        has_nobillingrateassigned = rail.IfOperator(
            task_id='has_nobillingrateassigned',
            test=lambda: not bool(rail.find_first_by_attr_and_get_attr(
                rail.result('get_project_team_member_details')[
                    'billingRatesAllowedForBillingTime'],
                'billingRate.uri',
                conf()['billingrateuri']
            )),
            yes_task="put_project_team_member_billing_rates_allowed_for_billing_time3_default",
            no_task="put_project_team_member_billing_rates_allowed_for_billing_time3_update",
        )

        put_project_team_member_billing_rates_allowed_for_billing_time3_update = rail.RepliconServiceOperator(
            task_id='put_project_team_member_billing_rates_allowed_for_billing_time3_update',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/PutProjectTeamMemberBillingRatesAllowedForBillingTime3",
            data=get_billingrateassignment_payload
        )

        put_project_team_member_billing_rates_allowed_for_billing_time3_default = rail.RepliconServiceOperator(
            task_id='put_project_team_member_billing_rates_allowed_for_billing_time3_default',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/PutProjectTeamMemberBillingRatesAllowedForBillingTime3",
            data=get_billingrateassignment_payload
        )

        has_default_billingratename = rail.IfOperator(
            task_id='has_default_billingratename',
            test="{{ dag_run.conf.billingratename == '|Billable' or dag_run.conf.billingratename == '|Non-Billable' }}",
            yes_task="has_user_uri",
            no_task="log_success"
        )

        has_user_uri = rail.IfOperator(
            task_id='has_user_uri',
            test="{{ dag_run.conf.useruri | is_truthy}}",
            yes_task="has_labortypepresent2",
            no_task="has_no_assignment",
        )

        has_labortypepresent2 = rail.IfOperator(
            task_id='has_labortypepresent2',
            test="{{ dag_run.conf.labortypepresent == 'No' and dag_run.conf.default and dag_run.conf.billingratename == '|Billable' }}",
            yes_task="get_defaultbillingrateuri4",
            no_task="has_labortypepresent3",
        )

        get_defaultbillingrateuri4 = rail.PythonOperator(
            task_id='get_defaultbillingrateuri4',
            python_callable=lambda: rail.get_current_context()[
                'dag_run'].conf['billingrateuri']
        )

        has_labortypepresent3 = rail.IfOperator(
            task_id='has_labortypepresent3',
            test="{{ dag_run.conf.labortypepresent == 'No' and not dag_run.conf.default and dag_run.conf.billingratename == '|Non-Billable' }}",
            yes_task="get_defaultbillingrateuri5",
            no_task="has_no_assignment",
        )

        get_defaultbillingrateuri5 = rail.PythonOperator(
            task_id='get_defaultbillingrateuri5',
            python_callable=lambda: rail.get_current_context()[
                'dag_run'].conf['billingrateuri']
        )

        has_no_assignment = rail.IfOperator(
            task_id='has_no_assignment',
            test="{{ result('get_existing_billing_rates') | is_falsy }}",
            yes_task="put_project_team_member_billing_rates_allowed_for_billing_time_nonbill",
            no_task="get_taks_assigned_billing_rates",
        )

        put_project_team_member_billing_rates_allowed_for_billing_time_nonbill = rail.RepliconServiceOperator(
            task_id='put_project_team_member_billing_rates_allowed_for_billing_time_nonbill',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/PutProjectTeamMemberBillingRatesAllowedForBillingTime3",
            data=get_billingrateassignment_payload
        )

        get_taks_assigned_billing_rates = rail.PythonOperator(
            task_id='get_taks_assigned_billing_rates',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('get_project_team_member_details')[
                    'billingRatesAllowedForBillingTime'],
                'billingRate.uri',
                conf()['billingrateuri'])
        )

        has_no_assignment_4 = rail.IfOperator(
            task_id='has_no_assignment_4',
            test="{{ result('get_taks_assigned_billing_rates') | is_falsy }}",
            yes_task="put_project_team_member_billing_rates_allowed_for_billing_time3_2",
            no_task="put_project_team_member_billing_rates_allowed_for_billing_time3_3",
        )

        put_project_team_member_billing_rates_allowed_for_billing_time3_2 = rail.RepliconServiceOperator(
            task_id='put_project_team_member_billing_rates_allowed_for_billing_time3_2',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/PutProjectTeamMemberBillingRatesAllowedForBillingTime3",
            data=get_billingrateassignment_payload
        )

        put_project_team_member_billing_rates_allowed_for_billing_time3_3 = rail.RepliconServiceOperator(
            task_id='put_project_team_member_billing_rates_allowed_for_billing_time3_3',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/PutProjectTeamMemberBillingRatesAllowedForBillingTime3",
            data=get_billingrateassignment_payload
        )

        log_success = rail.WriteLogOperator(
            task_id='log_success',
            log="{{dag_run.conf.log}}",
            message='Added successfully',
            severity='Success',
            properties={
                'wbs': '{{dag_run.conf.projectname}}',
                'task': '',
                'billingrate': '{{dag_run.conf.billingratename}}',
                'message': 'Added successfully',
                'status': 'Success',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message()}}',
            properties={
                'wbs': '{{dag_run.conf.projectname}}',
                'task': '',
                'billingrate': '{{dag_run.conf.billingratename}}',
                'message': '{{ get_error_message()}}',
                'status': 'Error',
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        get_project_team_member_details >> get_existing_billing_rates >> has_billing_rate_name

        has_billing_rate_name >> rail.Label(
            'Yes') >> update_billing_rate_is_available_for_assignment_to_team_members
        has_billing_rate_name >> rail.Label(
            'No') >> has_default_billingratename
        update_billing_rate_is_available_for_assignment_to_team_members >> has_non_billable_rate

        has_non_billable_rate >> rail.Label(
            'Yes') >> get_defaultbillingrateuri1 >> has_labortypepresent
        has_non_billable_rate >> rail.Label('No') >> has_labortypepresent

        has_labortypepresent >> rail.Label(
            'Yes') >> get_defaultbillingrateuri2 >> has_default_type
        has_labortypepresent >> rail.Label('No') >> has_default_type

        has_default_type >> rail.Label(
            'Yes') >> get_defaultbillingrateuri3 >> has_no_billingrate_uri
        has_default_type >> rail.Label('No') >> has_no_billingrate_uri

        has_no_billingrate_uri >> rail.Label(
            'yes') >> put_project_team_member_billing_rates_allowed_for_billing_time3 >> has_nobillingrateassigned
        has_no_billingrate_uri >> rail.Label(
            'No') >> has_nobillingrateassigned

        has_nobillingrateassigned >> rail.Label(
            'Yes') >> put_project_team_member_billing_rates_allowed_for_billing_time3_default >> has_default_billingratename
        has_nobillingrateassigned >> rail.Label(
            'No') >> put_project_team_member_billing_rates_allowed_for_billing_time3_update >> has_default_billingratename

        has_default_billingratename >> rail.Label('Yes') >> has_user_uri
        has_default_billingratename >> rail.Label('No') >> log_success
        has_user_uri >> rail.Label('Yes') >> has_labortypepresent2
        has_user_uri >> rail.Label('No') >> has_no_assignment
        has_labortypepresent2 >> rail.Label(
            'yes') >> get_defaultbillingrateuri4 >> has_labortypepresent3
        has_labortypepresent2 >> rail.Label('no') >> has_labortypepresent3
        has_labortypepresent3 >> rail.Label(
            'Yes') >> get_defaultbillingrateuri5 >> has_no_assignment
        has_labortypepresent3 >> rail.Label('no') >> has_no_assignment

        has_no_assignment >> rail.Label(
            'yes') >> put_project_team_member_billing_rates_allowed_for_billing_time_nonbill >> get_taks_assigned_billing_rates
        has_no_assignment >> rail.Label(
            'no') >> get_taks_assigned_billing_rates

        get_taks_assigned_billing_rates >> has_no_assignment_4

        has_no_assignment_4 >> rail.Label(
            'Yes') >> put_project_team_member_billing_rates_allowed_for_billing_time3_2 >> log_success
        has_no_assignment_4 >> rail.Label(
            'No') >> put_project_team_member_billing_rates_allowed_for_billing_time3_3 >> log_success

        log_success >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
