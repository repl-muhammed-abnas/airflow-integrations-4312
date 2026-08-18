import rail
null = None

def run_report_with_custom_filters(group_name,report_details,date_filter,end_task):
    with rail.TaskGroup(group_id=f'run_report_with_custom_filters_{group_name}',prefix_group_id=False):

        create_report_details=rail.PythonOperator(
            task_id=f'create_report_details_{group_name}',
            python_callable= lambda: {
                "daterange": rail.find_first_by_attr_and_get_attr(rail.result(
                                report_details)['filterConfiguration']['enabledFilters'],'displayText',date_filter,'uri',''),
                "usergroupcurrent": rail.find_first_by_attr_and_get_attr(rail.result(
                                report_details)['filterConfiguration']['enabledFilters'],'displayText','CurrentCostCenterFilter','uri',''),
                "jobtitle": rail.find_first_by_attr_and_get_attr(rail.result(
                                report_details)['filterConfiguration']['enabledFilters'],'displayText','CurrentServiceCenterFilter','uri','')
            }
        )

        create_reporteefilterforpayrolldata_list=rail.SetVariableOperator(
            task_id=f'create_reporteefilterforpayrolldata_list_{group_name}',
            append=False,
            name='reporteefilterforpayrolldata',
            value=[]
        )

        add_to_reporteefilterforpayrolldata_list=rail.SetVariableOperator(
            task_id=f'add_to_reporteefilterforpayrolldata_list_{group_name}',
            append=True,
            name='{{ result("create_reporteefilterforpayrolldata_list_' + group_name + '").name }}',
            value={
                "reportFilterUri": "{{ result('create_report_details_" + group_name + "').daterange }}",
                "value": null
            }
        )

        add_to_reporteefilterforpayroll_data_list=rail.SetVariableOperator(
            task_id=f'add_to_reporteefilterforpayroll_data_list_{group_name}',
            append=True,
            name='{{ result("create_reporteefilterforpayrolldata_list_' + group_name + '").name }}',
            value={
                "reportFilterUri": "{{ result('create_report_details_" + group_name + "').daterange }}",
                "value": "{{ result('create_run_details').startdate }}"
            }
        )

        add_to_reporteefilterfor_payroll_data_list=rail.SetVariableOperator(
            task_id=f'add_to_reporteefilterfor_payroll_data_list_{group_name}',
            append=True,
            name='{{ result("create_reporteefilterforpayrolldata_list_' + group_name + '").name }}',
            value={
                "reportFilterUri": "{{ result('create_report_details_" + group_name + "').daterange }}",
                "value": "{{ result('create_run_details').enddate }}"
            }
        )

        if_payload_has_costcenter_present=rail.IfOperator(
            task_id=f'if_payload_has_costcenter_present_{group_name}',
            test='''{{ dag_run.conf.webhook.data.CostCenter | is_truthy }}''',
            yes_task=f"get_usergroup_list_{group_name}",
            no_task=f"if_payload_has_servicecenter_present_{group_name}",
        )

        get_usergroup_list=rail.PythonOperator(
            task_id=f'get_usergroup_list_{group_name}',
            python_callable= lambda dag_run: [ {
                'usergroup': item
            } for item in dag_run.conf['webhook']['data']['CostCenter'].split(',')]
        )

        foreach_usergroup=rail.ForEachOperator(
            task_id=f'foreach_usergroup_{group_name}',
            items=lambda: rail.result(f'get_usergroup_list_{group_name}'),
            start_task = f'add_to_reporteefilter_for_payroll_data_list_{group_name}',
            end_task = f'foreach_usergroup_end_{group_name}'
        )

        add_to_reporteefilter_for_payroll_data_list=rail.SetVariableOperator(
            task_id=f'add_to_reporteefilter_for_payroll_data_list_{group_name}',
            append=True,
            name='{{ result("create_reporteefilterforpayrolldata_list_' + group_name + '").name }}',
            value={
                "reportFilterUri": "{{ result('create_report_details_" + group_name + "').usergroupcurrent }}",
                "value": "{{ result('foreach_usergroup_" + group_name + "').usergroup }}"
            }
        )

        foreach_usergroup_end=rail.EmptyOperator(
            task_id=f'foreach_usergroup_end_{group_name}',
        )

        if_payload_has_servicecenter_present=rail.IfOperator(
            task_id=f'if_payload_has_servicecenter_present_{group_name}',
            test='''{{ dag_run.conf.webhook.data.ServiceCenter | is_truthy }}''',
            yes_task=f"get_jobtitle_list_{group_name}",
            no_task=f"log_report_filter_for_{group_name}",
        )

        get_jobtitle_list=rail.PythonOperator(
            task_id=f'get_jobtitle_list_{group_name}',
            python_callable= lambda dag_run: [{
                'jobtitle': item
            } for item in dag_run.conf['webhook']['data']['ServiceCenter'].split(',')]
        )

        foreach_jobtitle=rail.ForEachOperator(
            task_id=f'foreach_jobtitle_{group_name}',
            items=lambda: rail.result(f'get_jobtitle_list_{group_name}'),
            start_task = f'add_to_reportee_filter_for_payroll_data_list_{group_name}',
            end_task = f'foreach_jobtitle_end_{group_name}'
        )

        add_to_reportee_filter_for_payroll_data_list=rail.SetVariableOperator(
            task_id=f'add_to_reportee_filter_for_payroll_data_list_{group_name}',
            append=True,
            name='{{ result("create_reporteefilterforpayrolldata_list_' + group_name + '").name }}',
            value={
                "reportFilterUri": "{{ result('create_report_details_" + group_name + "').jobtitle }}",
                "value": "{{ result('foreach_jobtitle_" + group_name + "').jobtitle }}"
            }
        )

        foreach_jobtitle_end=rail.EmptyOperator(
            task_id=f'foreach_jobtitle_end_{group_name}',
        )

        log_report_filter_for=rail.PythonOperator(
            task_id=f'log_report_filter_for_{group_name}',
            python_callable= lambda:  rail.get_dag_run_var('reporteefilterforpayrolldata')
        )

        run_custom_report = rail.run_report2(
            group_id=f'run_custom_report_{group_name}',
            report_params=lambda:{
                "reportParameters": [
                    {
                        "reportUri": rail.result(report_details)['uri'],
                        "filterValues": rail.result(f'log_report_filter_for_{group_name}'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact'
        )

        # fail_dag_with_error=rail.FailOperator(
        #     task_id=f'fail_dag_with_error_{group_name}',
        #     message='''{{(result('run_custom_report_''' + group_name + '''.get_report_result')| load_json_artifact).reportGenerationResults[0].error }}'''
        # )


        create_report_details >> create_reporteefilterforpayrolldata_list
        create_reporteefilterforpayrolldata_list >> add_to_reporteefilterforpayrolldata_list >> add_to_reporteefilterforpayroll_data_list
        add_to_reporteefilterforpayroll_data_list >> add_to_reporteefilterfor_payroll_data_list >> if_payload_has_costcenter_present
        if_payload_has_costcenter_present >> rail.Label('Yes') >> get_usergroup_list >> foreach_usergroup >> add_to_reporteefilter_for_payroll_data_list
        add_to_reporteefilter_for_payroll_data_list >> foreach_usergroup_end
        foreach_usergroup >> foreach_usergroup_end >> if_payload_has_servicecenter_present
        if_payload_has_costcenter_present >> rail.Label('No') >> if_payload_has_servicecenter_present
        if_payload_has_servicecenter_present >> rail.Label('Yes') >> get_jobtitle_list >> foreach_jobtitle >> add_to_reportee_filter_for_payroll_data_list
        add_to_reportee_filter_for_payroll_data_list >> foreach_jobtitle_end
        foreach_jobtitle >> foreach_jobtitle_end >> log_report_filter_for
        if_payload_has_servicecenter_present >> rail.Label('No') >> log_report_filter_for >> run_custom_report

    return create_report_details,run_custom_report
