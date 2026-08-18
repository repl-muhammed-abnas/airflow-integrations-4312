from datetime import datetime, timedelta, timezone
from functools import lru_cache
import json
import os
import airflow
import rail
from airflow.models import DagModel, Variable, DagRun
from airflow.models.serialized_dag import SerializedDagModel
from airflow.utils.session import NEW_SESSION, provide_session
from sqlalchemy.sql import func
from sqlalchemy import cast, String as sqlalchemy_string, or_
from system.disable_integrations.config import (replicon_conn_id, DISABLE_INACTIVE_DAGS_PREVIOUSLY_CHANGED_FILEPATHS_DETAILS_VAR_NAME, BASE_BRANCH, PR_TITLE,
    GITHUB_USER_TOKEN_VAR_NAME, INACTIVE_DAGS_CHECK_DAYS_VAR_NAME, TO_EMAIL_ADDR, CC_EMAIL_ADDR, FROM_EMAIL_ADDR,
    DISABLE_INACTIVE_DAGS_IGNORE_INTEGRATIONS_VAR_NAME, NOT_FOUND_MSG, REPO_NAME, REPO_OWNER, COMMIT_DATE_FORMAT, NEW_CONTENT_TO_ADD)
from system.disable_integrations import custom_methods

with airflow.DAG(
    dag_id="disable_inactive_integrations",
    schedule="0 14 * * MON",  # Runs every Monday at 2:00 PM UTC
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['system_airflow_disable_dags_cleanup'],
    user_defined_macros=rail.dag.get_macros(),
    user_defined_filters=rail.dag.get_filters(),
    default_args={
        'owner': 'system',
        'replicon_conn_id': replicon_conn_id
    },
    default_view="graph",
    max_active_runs=1
) as airflow_dag:

    @provide_session
    def get_disabled_dag_details_by_count(session=NEW_SESSION):

        inactive_dags_check_days = Variable.get(
            INACTIVE_DAGS_CHECK_DAYS_VAR_NAME, default_var=60)

        threshold = int(inactive_dags_check_days) if isinstance(
            inactive_dags_check_days, str) else inactive_dags_check_days

        threshold_date = datetime.now(timezone.utc) - timedelta(days=threshold)

        query = (
            session.query(
                DagModel.dag_id,
                DagModel.owners,
                DagModel.fileloc,
                # pylint: disable = protected-access
                cast(SerializedDagModel._data, sqlalchemy_string),
                func.max(DagRun.execution_date).label("max_exe_date")
            )
            .join(
                DagRun, DagModel.dag_id == DagRun.dag_id,
                isouter=True
            )
            .join(
                SerializedDagModel,
                SerializedDagModel.dag_id == DagModel.dag_id,
                isouter=True
            )
            .filter(
                # Keeping the DagModel.is_active to get only those dags which are available in the UI
                #! DagModel.is_active may be removed in the future
                DagModel.is_active, DagModel.is_paused
            )
            .group_by(
                DagModel.dag_id,
                DagModel.owners,
                DagModel.fileloc,
                # pylint: disable = protected-access
                cast(SerializedDagModel._data, sqlalchemy_string)
            )
            .having(
                or_(
                    func.max(DagRun.execution_date) < threshold_date,
                    func.max(DagRun.execution_date).is_(None)
                )
            )
        )

        print(f"Executing SQL Query... {query}")
        queried_dags = query.all()
        unique_instance_file_path = set()
        queried_dags_list = []
        if queried_dags:
            for record in queried_dags:
                _default_args: dict = json.loads(
                    record[3] if record[3] else "{}")
                instance_file_path = _default_args.get('dag', {}).get('default_args', {}).get(
                    '__var', {}).get('instance_config_file', NOT_FOUND_MSG)
                if instance_file_path != NOT_FOUND_MSG:
                    unique_instance_file_path.add(
                        (record[1], instance_file_path))
                queried_dags_list.append(
                    {
                        'dag_id': record[0],
                        'owner': record[1],
                        'fileloc': record[2],
                        "instance_file_path": instance_file_path,
                        "last_exe_date": custom_methods.convert_date_to_str(record[4])

                    })

        return {
            'dag_count': len(queried_dags_list),
            'threshold': threshold,
            'threshold_date': threshold_date.strftime("%Y-%m-%dT%H:%M:%S"),
            'dags': queried_dags_list,
            'region': os.environ.get('REGION', 'unknown'),
            'environment': os.environ.get('AIRFLOW_ENVIRONMENT', 'unknown'),
            'reg_env': f"{os.environ.get('REGION', 'unknown')}-{os.environ.get('AIRFLOW_ENVIRONMENT', 'unknown')}",
            "unique_instance_file_path": list(unique_instance_file_path),
            'count': len(unique_instance_file_path),
            'repo_name': REPO_NAME
        }

    get_disabled_dag_details = rail.PythonOperator(
        task_id="get_disabled_dag_details",
        priority_weight=10,
        python_callable=get_disabled_dag_details_by_count
    )

    has_any_inactive_dags = rail.IfOperator(
        task_id="has_any_inactive_dags",
        test="{{ result('get_disabled_dag_details').count > 0 }}",
        yes_task="get_instance_paths_to_change"
    )

    def _get_instance_paths_to_change():
        # Variable is saved as {"filepath_list": [], "last_branch_name": "xxx", "last_pull_request": ""}
        previously_changed_filepath:dict = Variable.get(
            DISABLE_INACTIVE_DAGS_PREVIOUSLY_CHANGED_FILEPATHS_DETAILS_VAR_NAME, deserialize_json=True)
        # Variable is saved as { "ignore_list": [company_key/integration_folder_name, mammoet/user_import] }
        integrations_to_ignore: list = custom_methods.get_ignored_list()

        filepath_list: list = previously_changed_filepath['filepath_list'].copy(
        )
        files_to_change = []
        files_to_change_with_owner = []
        ignored_list = []
        unique_instance_file_path = rail.result("get_disabled_dag_details")[
            'unique_instance_file_path']

        for owner, path in unique_instance_file_path:
            if path in filepath_list:
                continue

            _, integration_path = custom_methods.get_integration_path(path)
            if integration_path in integrations_to_ignore:
                ignored_list.append(path)
                continue

            files_to_change.append(path)
            files_to_change_with_owner.append([owner, path])

        filepath_list.extend(files_to_change)
        for index, path in enumerate(filepath_list):
            if path not in unique_instance_file_path:
                filepath_list.pop(index)

        return {
            "files_to_change": files_to_change,
            "files_to_change_with_owner": files_to_change_with_owner,
            "count": len(files_to_change),
            "last_pr_number": previously_changed_filepath['last_pull_request'].split('/')[-1],
            'last_pull_request': previously_changed_filepath['last_pull_request'],
            "last_branch_name": previously_changed_filepath['last_branch_name'],
            "current_variable_details": previously_changed_filepath,
            "updated_details": {'filepath_list': filepath_list},
            "ignored_list": ignored_list,
            "ignored_list_count": len(ignored_list),
            "ignore_list_var_name": DISABLE_INACTIVE_DAGS_IGNORE_INTEGRATIONS_VAR_NAME
        }

    get_instance_paths_to_change = rail.PythonOperator(
        task_id="get_instance_paths_to_change",
        python_callable=_get_instance_paths_to_change
    )

    has_any_files_to_change = rail.IfOperator(
        task_id="has_any_files_to_change",
        test="{{result('filter_file_paths', 'files_to_update') | length > 0}}",
        yes_task="create_new_branch",
        no_task="has_ignored_records"
    )

    has_ignored_records = rail.IfOperator(
        task_id="has_ignored_records",
        test="{{result('get_instance_paths_to_change').ignored_list_count > 0}}",
        yes_task="render_email_template_ignored",
        no_task="render_email_template_exception"
    )

    render_email_template_ignored = rail.RenderTemplateOperator(
        task_id="render_email_template_ignored",
        template_file="emails/all_ignored_dags.html",
        target='result'
    )

    # Task may get removed
    # send_ignored_email = rail.PythonOperator(
    #     task_id = "send_ignored_email",
    #     python_callable=custom_methods.send_standard_response_callable,
    #     op_args=[
    #         FROM_EMAIL_ADDR,
    #         TO_EMAIL_ADDR,
    #         CC_EMAIL_ADDR,
    #         "ignored"
    #     ]
    # )

    render_email_template_exception = rail.RenderTemplateOperator(
        task_id="render_email_template_exception",
        template_file="emails/email_body_previous_pr_not_actioned.html",
        target='result'
    )

    send_previous_pr_not_actioned = rail.PythonOperator(
        task_id="send_previous_pr_not_actioned",
        python_callable=custom_methods.send_standard_response_callable,
        op_args=[
            FROM_EMAIL_ADDR,
            TO_EMAIL_ADDR,
            CC_EMAIL_ADDR,
            "exception"
        ]
    )

    @provide_session
    def get_all_dag_details_for_integrations(integration_owner, integration_path, session=NEW_SESSION):
        print(integration_path)
        query_dags = session.query(
            DagModel.dag_id,
            DagModel.owners,
            DagModel.is_active,
            DagModel.is_paused,
            DagModel.fileloc
        ).filter(
            DagModel.fileloc.like(f"{integration_path}%")
        ).filter(
            DagModel.owners == integration_owner
        ).all()

        integrations_dag_ids = []
        dag_details = {}
        if not query_dags:
            print("No dag details found")
            return {}
        for _dag in query_dags:
            integrations_dag_ids.append(_dag[0])
            dag_details[_dag[0]] = {
                "owners": _dag[1],
                "is_active": _dag[2],
                "is_paused": _dag[3],
                "fileloc": _dag[4],
                "last_exe_date": ""
            }

        query_available_dags_in_ui = session.query(
            SerializedDagModel.dag_id
        ).filter(SerializedDagModel.dag_id.in_(integrations_dag_ids)).all()

        valid_integrations_dag_ids_available_dags_in_ui = [
            _dag_id[0] for _dag_id in query_available_dags_in_ui]

        query_last_runs_per_dag = session.query(
            DagRun.dag_id,
            func.max(DagRun.execution_date).label("max_exe_date")
        ).filter(
            DagRun.dag_id.in_(valid_integrations_dag_ids_available_dags_in_ui)
        ).group_by(
            DagRun.dag_id
        )

        dag_run_details = dict()
        for item in query_last_runs_per_dag.all():
            dag_run_details[item[0]] = custom_methods.convert_date_to_str(item[1])

        final_data = dict()
        for dag_id, _dag_details in dag_details.items():

            if dag_id not in valid_integrations_dag_ids_available_dags_in_ui:
                continue

            path = "/".join((_dag_details['fileloc'].split(".py")
                            [0]).split("/")[:-1])
            path = path.replace("/opt/airflow/dags/", "")
            key = f"{path}|{_dag_details['owners']}"
            _dag_details['last_exe_date'] = dag_run_details.get(dag_id, "")

            if key in final_data:
                final_data[key].append({**_dag_details, **{"dag_id": dag_id}})
                final_data[key] = final_data[key]
            else:
                final_data[key] = [{**_dag_details, **{"dag_id": dag_id}}]

        return final_data

    def integration_data():
        threshold = rail.result("get_disabled_dag_details")['threshold']
        today_minus_sixty_days = datetime.now() - timedelta(days=threshold)

        def is_allowed_for_update(integration_details:dict):
            for _, v in integration_details.items():
                execution_dates = list(filter(None, map(
                    lambda item: custom_methods.convert_date_str_to_date_time(item['last_exe_date']), v)))
                if not execution_dates:
                    # allowed to be disabled as no execution date is found for any dag_run
                    return True, "No execution dates"
                if max(execution_dates) >= today_minus_sixty_days:
                    return False, "Last execution date is more than today minus 60.Days"
                # is_paused == False means the dag is enabled
                if False in list(filter(None, map(lambda item: item['is_paused'], v))):
                    return False, "There are 1 or more enabled dags for this integration"
                return True, "There are no enabled dags for this integration"

        data = rail.result("get_instance_paths_to_change")[
            "files_to_change_with_owner"]
        files_to_update = []
        all_integrations_data = []
        for owner, path in data:
            print(f"processing for owner : {owner} & path : {path}")
            integration_details = get_all_dag_details_for_integrations(
                owner, custom_methods.get_integration_path(path)[0])
            all_integrations_data.append(integration_details)
            allowed, comment = is_allowed_for_update(integration_details)
            files_to_update.append((allowed, comment, path))
        rail.set_result(key="files_to_update", val=files_to_update)
        return all_integrations_data

    filter_file_paths = rail.PythonOperator(
        task_id="filter_file_paths",
        python_callable=integration_data
    )

    create_new_branch = rail.PythonOperator(
        task_id="create_new_branch",
        python_callable=custom_methods.create_new_branch_callable
    )

    def update_instance_files_to_disable_callable():
        github_user_token = Variable.get(GITHUB_USER_TOKEN_VAR_NAME)
        headers = {"Authorization": f"Bearer {github_user_token}"}
        new_branch_name = rail.result('create_new_branch')['branch']

        last_commit_and_details_on_files = dict()
        fourteenth_day_from_today_in_past = (
            datetime.now() - timedelta(days=14)).date()

        to_disable_dag_details = rail.result(
            "filter_file_paths", "files_to_update")

        dice_team_members = custom_methods.get_all_team_members_details_for_integration(
            headers
        )

        for tuple_item in to_disable_dag_details:
            allowed = tuple_item[0]
            allowed_comment_message = tuple_item[1]
            original_instance_file_path = tuple_item[2]
            instance_file_path = original_instance_file_path.replace("/opt/airflow/dags/repo/", "")

            if not allowed:
                print(f"Skipping the file as it is not allowed to be disabled. Reason: {allowed_comment_message}")
                last_commit_and_details_on_files[instance_file_path.replace(
                    '/opt/airflow/', '')] = {
                    "name": NOT_FOUND_MSG,
                    "date": NOT_FOUND_MSG,
                    'status': 'Ignored',
                    'message': f"Skipping the file as it is not allowed to be disabled. Reason: {allowed_comment_message}"
                }
                continue

            if instance_file_path == NOT_FOUND_MSG:
                print("Skipping the file as instance file path is not found")
                last_commit_and_details_on_files[instance_file_path.replace(
                    '/opt/airflow/', '')] = {
                    "name": NOT_FOUND_MSG,
                    "date": NOT_FOUND_MSG,
                    'status': 'Ignored',
                    'message': f"Skipping the file as instance file path is not found"
                }
                continue

            # Getting Commit details for the integration folder itself
            _author_name, _author_email, _commit_date, _commit_msg, author_url = custom_methods.get_latest_commit_info(REPO_OWNER, REPO_NAME,
                                                                                        f"dags/{custom_methods.get_integration_path(original_instance_file_path)[1]}", 'main', headers)

            if not custom_methods.validate_if_author_from_integration_team(_author_name, _author_email, author_url, dice_team_members):
                print(f"For {instance_file_path} update skipped as last commit was not done by integration team member")
                last_commit_and_details_on_files[instance_file_path.replace(
                    '/opt/airflow/', '')] = {
                    "name": f"{_author_name} | {author_url.replace('https://api.github.com/users/', '')}",
                    "date": _commit_date,
                    'status': 'Ignored',
                    'message': f"For {instance_file_path} update skipped as last commit on integration folder was not done by integration team member"
                }
                continue

            if _commit_date and custom_methods.convert_date_str_to_date_time(_commit_date, COMMIT_DATE_FORMAT, "date").date() > fourteenth_day_from_today_in_past:
                print(f"For {instance_file_path} update skipped as last commit is not older than 14 days")
                last_commit_and_details_on_files[instance_file_path.replace(
                    '/opt/airflow/', '')] = {
                    "name": _author_name,
                    "date": _commit_date,
                    'status': 'Ignored',
                    'message': f"{_commit_msg}; For {instance_file_path} update skipped as last commit is not older than 14 days"
                }
                continue

            instance_file_path = instance_file_path.replace(
                '/opt/airflow/', '')

            # Getting Commit details for the instance file
            # not added integration team check as it is already done above on the integration folder itself
            author_name, _, commit_date, commit_msg, author_url = custom_methods.get_latest_commit_info(
                REPO_OWNER, REPO_NAME, instance_file_path, 'main', headers)

            can_update_file, content, current_sha = custom_methods.update_file_content_and_encode(
                REPO_OWNER, REPO_NAME, instance_file_path, new_branch_name, headers, NEW_CONTENT_TO_ADD)

            if not can_update_file:
                print(f"For {instance_file_path} update skipped as {content}")
                last_commit_and_details_on_files[instance_file_path] = {
                    "name": author_name,
                    "date": commit_date,
                    'status': 'Skipped',
                    'message': f"For {instance_file_path} update skipped as {content}"
                }
                continue

            last_commit_and_details_on_files[instance_file_path] = {
                "name": author_name,
                "date": commit_date,
                'status': 'Updated'
            }

            custom_methods.update_file_via_api(REPO_OWNER,
                                                REPO_NAME, instance_file_path, new_branch_name, headers, content, current_sha, custom_methods.get_commit_message(
                                                instance_file_path))
            last_commit_and_details_on_files[instance_file_path]['message'] = commit_msg


        return {
            "branch": new_branch_name,
            "pull_request": custom_methods.create_pull_request(REPO_OWNER, REPO_NAME, BASE_BRANCH, new_branch_name, PR_TITLE, headers),
            "last_commit_and_details_on_files": last_commit_and_details_on_files
        }

    update_instance_files_to_disable = rail.PythonOperator(
        task_id="update_instance_files_to_disable",
        python_callable=update_instance_files_to_disable_callable
    )

    def _update_the_variable_details():
        custom_methods.release_cached_memory()
        Variable.set(key=DISABLE_INACTIVE_DAGS_PREVIOUSLY_CHANGED_FILEPATHS_DETAILS_VAR_NAME,
                    value={
                        "filepath_list": rail.result("get_instance_paths_to_change")['files_to_change'],
                        "last_branch_name": rail.result("create_new_branch")['branch'],
                        "last_pull_request": rail.result("update_instance_files_to_disable")['pull_request']
                    },
                    serialize_json=True
                    )
        return f"Variable {DISABLE_INACTIVE_DAGS_PREVIOUSLY_CHANGED_FILEPATHS_DETAILS_VAR_NAME} updated successfully"

    update_the_variable_details = rail.PythonOperator(
        task_id="update_the_variable_details",
        python_callable=_update_the_variable_details
    )


    def get_create_csv_row(item):
        last_commit_and_details_on_file: dict = custom_methods.get_last_commit_and_details_on_files(
        ).get(item['instance_file_path'].replace("/opt/airflow/", ""), {})
        return [
            item['dag_id'],
            item['owner'],
            item['fileloc'].replace("/opt/airflow/", ""),
            item['instance_file_path'].replace("/opt/airflow/", ""),
            custom_methods.get_integration_path(item['fileloc']) in custom_methods.get_ignored_list(),
            last_commit_and_details_on_file.get('name', NOT_FOUND_MSG),
            last_commit_and_details_on_file.get('date', NOT_FOUND_MSG),
            last_commit_and_details_on_file.get('status', NOT_FOUND_MSG),
            last_commit_and_details_on_file.get('message', NOT_FOUND_MSG)
        ]

    create_csv = rail.WriteCSVFileOperator(
        task_id="create_csv",
        source=lambda: rail.result("get_disabled_dag_details")['dags'],
        header=['DagId', 'Owner', 'FileLocation', 'InstanceFileLocation',
                'InIgnoredList', 'LastCommitUSer', 'LastCommitDate', 'Status', 'Details'],
        row=get_create_csv_row
    )

    render_email_template_success = rail.RenderTemplateOperator(
        task_id="render_email_template_success",
        template_file="emails/email_body.html",
        target='result'
    )

    send_email = rail.PythonOperator(
        task_id="send_email",
        python_callable=custom_methods.send_standard_response_callable,
        op_args=[
            FROM_EMAIL_ADDR,
            TO_EMAIL_ADDR,
            CC_EMAIL_ADDR,
            "success"
        ]
    )

    get_disabled_dag_details >> has_any_inactive_dags >> rail.Label("Yes") >> get_instance_paths_to_change >> filter_file_paths >> has_any_files_to_change\
        >> rail.Label("Yes") >> create_new_branch >>\
        update_instance_files_to_disable >> create_csv >> render_email_template_success >> send_email >> update_the_variable_details

    has_any_files_to_change >> rail.Label("No") >> has_ignored_records
    has_ignored_records >> rail.Label("Yes") >> render_email_template_ignored
    has_ignored_records >> rail.Label(
        "No") >> render_email_template_exception >> send_previous_pr_not_actioned
