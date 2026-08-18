from datetime import datetime
import rail

# config : https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/c1_leanstaffing_assignment_v1/config.py


def create_dag(config):
    dag_id_postfix = f'_{config.instance}'
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_c1_leanstaffassignment_webhooks{dag_id_postfix}_v1',
        description=f'DXC C1 Leanstaffing Assignment Webhook receiver {config.instance} v1',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 1, 1),
        webhook_conf=[
            rail.WebhookConf(
                hmac_secret_var=config.team_rate_modified_token_variable_name,
                trigger_condition='{{ data.authority.actingUser.loginName | lower | starts_with("repliconintpsa") or not data.authority.actingUser.loginName | lower | starts_with("repliconint") }}'
            ),
            rail.WebhookConf(
                hmac_secret_var=config.team_dates_modified_token_variable_name,
                trigger_condition='{{ data.authority.actingUser.loginName | lower | starts_with("repliconintpsa") or not data.authority.actingUser.loginName | lower | starts_with("repliconint") }}'
            ),
            rail.WebhookConf(
                hmac_secret_var=config.team_modified_token_variable_name,
                trigger_condition='{{ data.authority.actingUser.loginName | lower | starts_with("repliconintpsa") or not data.authority.actingUser.loginName | lower | starts_with("repliconint") }}'
            ),
        ],
        max_active_runs=config.max_webhook_master_active_dag_runs
    ) as dag:

        # This is webhook processor for 3 events
        # Going forward this dag will remain static and
        # processing of the webhook events ( from `load_project`) should be done on a separate child dag
        # to avoid the changing the webhook event target URL

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        was_triggered_by_repliconpsa = rail.IfOperator(
                task_id="was_triggered_by_repliconpsa",
                test='{{ dag_run.conf.webhook.data.authority.actingUser.loginName | lower | starts_with("repliconintpsa") }}',
                yes_task="is_valid_webhookevent",
                no_task='was_triggered_by_replicon'
            )

        was_triggered_by_replicon = rail.IfOperator(
                task_id="was_triggered_by_replicon",
                test='{{ dag_run.conf.webhook.data.authority.actingUser.loginName | lower | starts_with("repliconint") }}',
                yes_task="delete_this_dagrun",
                no_task='is_valid_webhookevent'
            )
        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        is_valid_webhookevent = rail.IfOperator(
            task_id = "is_valid_webhookevent",
            test = "{{ dag_run.conf.webhook.headers['X-Replicon-Webhook-Event-Type'] in ['ProjectTeamModified', 'ProjectTeamMemberAssignmentDatesModified',\
                'ProjectTeamMemberBillingRateAssociationsModified']}}",
            yes_task="load_project",
            no_task= "fail_invalid_webhookevent"
        )

        fail_invalid_webhookevent = rail.FailOperator(
            task_id = "fail_invalid_webhookevent",
            message= "Received invalid webhook trigger event: '{{dag_run.conf.webhook.headers['X-Replicon-Webhook-Event-Type']}}'"
        )

        load_project = rail.RepliconServiceOperator(
            task_id='load_project',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={"projects": [
                {"uri": "{{ dag_run.conf.webhook.data.project.uri }}"}]},
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                {"projectDetails": None}])[0]['projectDetails'],
        )

        does_project_exist = rail.IfOperator(
            task_id="does_project_exist",
            test="{{ result('load_project') is not none  }}",
            yes_task="check_psa_condition",
        )

        def bool_check_psa_condition(dag_run):
            if (dag_run.conf['webhook']['data']['authority']['actingUser']['loginName']).lower().startswith('repliconintpsa'):
                extension_field_values = rail.result('load_project')['extensionFieldValues']
                psa_flag_extension_field = rail.find_first_by_attr_and_get_attr(
                    rail.result('load_project')['extensionFieldValues'], 'definition.displayText', 'PSA Flag') if extension_field_values else None
                return psa_flag_extension_field['tag']['displayText'] == 'X' if psa_flag_extension_field else False

            return True

        check_psa_condition = rail.IfOperator(
            task_id = "check_psa_condition",
            test=bool_check_psa_condition,
            yes_task= "is_not_opporutunity_wbs_type"
        )

        def is_opportunity_wbs_type_not_present():
            extension_field_values = rail.result('load_project')['extensionFieldValues']
            wbs_type_extension_field = rail.find_first_by_attr_and_get_attr(
                rail.result('load_project')['extensionFieldValues'], 'definition.displayText', 'WBS Type') if extension_field_values else None
            return wbs_type_extension_field['tag']['displayText'] != 'Opportunity' if wbs_type_extension_field else True

        is_not_opporutunity_wbs_type = rail.IfOperator(
            task_id="is_not_opporutunity_wbs_type",
            test=is_opportunity_wbs_type_not_present,
            yes_task="does_project_have_division",
        )

        does_project_have_division = rail.IfOperator(
            task_id="does_project_have_division",
            test="{{ result('load_project') | attr_or_default('division.uri') is not none  }}",
            yes_task="load_division",
            no_task='get_project_type_oef',
        )

        load_division = rail.RepliconServiceOperator(
            task_id='load_division',
            endpoint='/services/DivisionService1.svc/GetDivisionDetails',
            data={"divisionUri": "{{ result('load_project').division.uri }}"},
        )

        is_division_C1 = rail.IfOperator(
            task_id="is_division_C1",
            test="{{ result('load_division') | attr_or_default('code') == 'C1' }}",
            yes_task="get_project_type_oef",
        )

        get_project_type_oef = rail.RenderTemplateOperator(
            task_id='get_project_type_oef',
            target='result',
            # pylint: disable=line-too-long
            template="{{ result('load_project').extensionFieldValues | filter_by_attr('tag.definition.displayText', 'equals', 'Project Type') | first_or_default() | attr_or_default('tag.displayText') }}",
        )

        has_project_type_oef = rail.IfOperator(
            task_id="has_project_type_oef",
            test="{{ result('get_project_type_oef') is not none }}",
            yes_task="is_project_type_ES",
        )

        is_project_type_ES = rail.IfOperator(
            task_id="is_project_type_ES",
            test="{{ result('get_project_type_oef') == 'ES' }}",
            yes_task="does_project_start_with_Edash",
            no_task="is_project_type_IC",
        )

        does_project_start_with_Edash = rail.IfOperator(
            task_id="does_project_start_with_Edash",
            test="{{ result('load_project').name | starts_with('E-') }}",
            no_task="is_project_type_IC"
        )

        is_project_type_IC = rail.IfOperator(
            task_id="is_project_type_IC",
            test="{{ result('get_project_type_oef') == 'IC' }}",
            yes_task="does_project_start_with_Xdash",
            no_task="get_taskassignment_billingratechangedate_udf",
        )

        does_project_start_with_Xdash = rail.IfOperator(
            task_id="does_project_start_with_Xdash",
            test="{{ result('load_project').name | starts_with('X-') }}",
            no_task="get_taskassignment_billingratechangedate_udf"
        )

        get_taskassignment_billingratechangedate_udf = rail.RepliconServiceOperator(
            task_id = "get_taskassignment_billingratechangedate_udf",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data = {
                    "objectUri": "urn:replicon:object-type:project"
                },
            response_filter= lambda response: rail.find_first_by_attr_and_get_attr(response.json()['d'], 'displayText', 'Taskassignment_billingratechangedate')
        )

        update_taskassignment_billingratechangedate = rail.RepliconServiceOperator(
            task_id="update_taskassignment_billingratechangedate",
            endpoint='/services/CustomFieldService1.svc/UpdateDateValue',
            data={
                'objectUri': '{{ result("load_project").uri }}',
                'customFieldUri':'{{result("get_taskassignment_billingratechangedate_udf").uri}}',
                'value': {
                    'year': '{{ macros.datetime.fromisoformat(dag_run.conf.webhook.received_at).year }}',
                    'month': '{{ macros.datetime.fromisoformat(dag_run.conf.webhook.received_at).month }}',
                    'day': '{{ macros.datetime.fromisoformat(dag_run.conf.webhook.received_at).day }}',
                }
            }
        )

        get_webhook_log = rail.CreateLogOperator(
            task_id="get_webhook_log",
            tenant_wide_name="c1_leanstaffassignment_webhooks_v1" if config.company_key != 'DXCSandbox2' else 'c1_leanstaffassignment_webhooks_sb2_v1',
            existing_log_mode="append",
        )

        write_webhook_to_log = rail.WriteLogOperator(
            task_id="write_webhook_to_log",
            log="{{ result('get_webhook_log') }}",
            message="{{ dag_run.conf.webhook.headers['X-Replicon-Webhook-Event-Type'] }}",
            properties={
                'project_uri': "{{ result('load_project').uri }}",
                'project_name': "{{ result('load_project').name }}",
                'event_time': "{{ dag_run.conf.webhook.received_at }}",
                'user_uri': "{{ dag_run.conf.webhook.data | attr_or_default('teamMember.resource.uri', '') }}",
            }
        )

        was_triggered_by_repliconpsa >> rail.Label("Yes") >> is_valid_webhookevent
        was_triggered_by_repliconpsa >> rail.Label("No") >> was_triggered_by_replicon
        was_triggered_by_replicon >> rail.Label("Yes") >> delete_this_dagrun
        was_triggered_by_replicon >> rail.Label("No") >> is_valid_webhookevent >> rail.Label("Yes") >> load_project >> \
            does_project_exist >> rail.Label("Yes") >> check_psa_condition >> rail.Label("Yes") >> is_not_opporutunity_wbs_type >> \
            rail.Label("Yes") >> does_project_have_division >> rail.Label("Yes") >> load_division >> \
            is_division_C1 >> rail.Label("Yes") >> get_project_type_oef >> has_project_type_oef >> \
            rail.Label("Yes") >> is_project_type_ES >> rail.Label("Yes") >> \
            does_project_start_with_Edash >> rail.Label("No") >> is_project_type_IC >> rail.Label("Yes") >> \
            does_project_start_with_Xdash >> rail.Label("No") >> get_taskassignment_billingratechangedate_udf>> \
            update_taskassignment_billingratechangedate >> get_webhook_log >> write_webhook_to_log
        does_project_have_division >> rail.Label("No") >> get_project_type_oef
        is_project_type_ES >> rail.Label("No") >> is_project_type_IC >> rail.Label(
            "No") >> get_taskassignment_billingratechangedate_udf >> update_taskassignment_billingratechangedate

        is_valid_webhookevent >> rail.Label("No") >> fail_invalid_webhookevent

    return dag


rail.for_each_instance(create_dag)
