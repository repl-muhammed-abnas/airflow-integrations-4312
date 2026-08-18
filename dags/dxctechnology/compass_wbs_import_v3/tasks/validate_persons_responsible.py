import rail


def validate_persons_responsible():

    with rail.TaskGroup(group_id='validate_persons_responsible', prefix_group_id=False):

        are_persons_responsible_unique = rail.IfOperator(
            task_id="are_persons_responsible_unique",
            test="{{ dag_run.conf.personresponsible1 != dag_run.conf.personresponsible2 or dag_run.conf.personresponsible1 | length == 0 }}",
            yes_task="were_both_persons_provided",
            no_task="record_same_person_responsible",
        )

        record_same_person_responsible = rail.WriteLogOperator(
            task_id='record_same_person_responsible',
            log='{{ result("create_exception_log") }}',
            message='Person responsible 1 and Person responsible 2 have same user',
        )

        were_both_persons_provided = rail.IfOperator(
            task_id="were_both_persons_provided",
            test="{{ dag_run.conf.personresponsible1 | length > 0 and dag_run.conf.personresponsible2 | length > 0 }}",
            yes_task="load_persons_responsible_userinfo",
            no_task="record_responsible_persons_missing",
        )

        record_responsible_persons_missing = rail.WriteLogOperator(
            task_id='record_responsible_persons_missing',
            log='{{ result("create_exception_log") }}',
            message='{{ [\
                "Person responsible 1 is not present" if dag_run.conf.personresponsible1 | length == 0 else none, \
                "Person responsible 2 is not present" if dag_run.conf.personresponsible2 | length == 0 else none, \
                ] | remove_empty | join(", ") }}',
        )

        def load_persons_responsible_data():
            context = rail.get_current_context()
            persons = [
                context['dag_run'].conf['personresponsible1'],
                context['dag_run'].conf['personresponsible2']]

            def create_filter(person):
                return {
                    "leftExpression": {"filterDefinitionUri": "urn:replicon:user-list-filter:text"},
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {"value": {"text": person}},
                }

            person_filters = list(map(create_filter, filter(None, persons)))
            if len(person_filters) > 1:
                filter_expression = {
                    "leftExpression": person_filters[0],
                    "operatorUri": "urn:replicon:filter-operator:or",
                    "rightExpression": person_filters[1],
                }
            else:
                filter_expression = person_filters[0] if len(
                    person_filters) > 0 else None

            return {
                "page": "1",
                "pagesize": 10000 if len(person_filters) > 0 else 0,
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:employee-type-group",
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:enabled",
                    "urn:replicon:user-list-column:end-date",
                    "urn:replicon:user-list-column:location"
                ],
                "filterExpression": filter_expression
            }

        def load_persons_responsible_handle_response(response):
            rows = response.json()['d']['rows']

            def get_user_info(employee_id):
                user = next(
                    filter(
                        lambda r: employee_id and r['cells'][2].get(
                            'textValue') == employee_id,
                        rows),
                    None)

                def format_date(
                    d): return f"{d['year']:04}-{d['month']:02}-{d['day']:02}" if d else None
                if user:
                    return {
                        "useruri": user['cells'][0].get('uri'),
                        "name": user['cells'][0].get('textValue'),
                        "status": user['cells'][3].get('textValue'),
                        "employeegroup": '|'.join([eg.get('textValue') for eg in user['cells'][1].get('cellCollection', [])]) or 'No Employee Group assigned',
                        "enddate": format_date(user['cells'][4].get('dateValue')),
                        "country": user['cells'][5].get('textValue'),
                    }
                return None
            context = rail.get_current_context()
            return {
                'projectleader': get_user_info(context['dag_run'].conf['personresponsible1']),
                'comanager': get_user_info(context['dag_run'].conf['personresponsible2'])
            }
        load_persons_responsible_userinfo = rail.RepliconServiceOperator(
            task_id='load_persons_responsible_userinfo',
            endpoint='/services/UserListService1.svc/GetData',
            data=load_persons_responsible_data,
            response_filter=load_persons_responsible_handle_response,
        )

        did_both_persons_load_successfully = rail.IfOperator(
            task_id='did_both_persons_load_successfully',
            # pylint: disable=line-too-long
            test="{{ result('load_persons_responsible_userinfo').projectleader is not none and result('load_persons_responsible_userinfo').comanager is not none }}",
            yes_task='userinfo_loaded',
            no_task='record_user_unavailable'
        )

        record_user_unavailable = rail.WriteLogOperator(
            task_id='record_user_unavailable',
            log='{{ result("create_exception_log") }}',
            message='{{ [\
                "Person responsible 1 \\"" + dag_run.conf.personresponsible1 + "\\" is not available in Replicon" if result("load_persons_responsible_userinfo").projectleader is none else none, \
                "Person responsible 2 \\"" + dag_run.conf.personresponsible2 + "\\" is not available in Replicon" if result("load_persons_responsible_userinfo").comanager is none else none, \
                ] | remove_empty | join(", ") }}',
        )

        userinfo_loaded = rail.EmptyOperator(task_id='userinfo_loaded')

    are_persons_responsible_unique >> rail.Label("Yes") >> were_both_persons_provided >> rail.Label("Yes") >> load_persons_responsible_userinfo >> \
        did_both_persons_load_successfully >> rail.Label(
            "Yes") >> userinfo_loaded
    are_persons_responsible_unique >> rail.Label("No") >> record_same_person_responsible >> were_both_persons_provided >> rail.Label("No") >> \
        record_responsible_persons_missing >> load_persons_responsible_userinfo
    did_both_persons_load_successfully >> rail.Label(
        "No") >> record_user_unavailable >> userinfo_loaded
    return are_persons_responsible_unique, userinfo_loaded
