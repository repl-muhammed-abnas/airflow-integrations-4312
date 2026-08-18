import rail
from pwcglobal.time_export_v3.python_callable_method import get_api_log_message
from pwcglobal.time_export_v3.python_callable_method import get_task_state


def get_post_to_endpoint(config, endpoint_index):
    open_bracket = '{{'
    close_bracket = '}}'
    with rail.TaskGroup(group_id=f"post_data_to_endpoint_{endpoint_index}", prefix_group_id=False):

        def get_endpoint_detail(dag_run):
            if int(endpoint_index) <= 0:
                raise Exception(
                    f"the endpoint index is less than 1, received endpoint_index: {endpoint_index}")

            if int(endpoint_index) > len(rail.result('get_api_endpoint_details_for_location')):
                return None

            api_detail = rail.result('get_api_endpoint_details_for_location')[
                int(endpoint_index)-1]
            return {
                ** api_detail,
                ** {"header": {
                    "Accept": "application/json",
                    "apikey": api_detail['apikey'],
                    "apikeysecret": api_detail['apikeysecret'],
                    "filename": dag_run.conf['export_file_name'] + ".json",
                    "territory": dag_run.conf['code'],
                    "proxyauth": api_detail['proxyauthorization']
                }}
            }

        get_endpoint_details = rail.PythonOperator(
            task_id=f"get_endpoint_details_{endpoint_index}",
            python_callable=get_endpoint_detail
        )

        has_found_endpoint = rail.IfOperator(
            task_id=f"has_found_endpoint_{endpoint_index}",
            test=lambda: bool(rail.result(get_endpoint_details.task_id)),
            yes_task=f"can_post_to_endpoint_{endpoint_index}",
            no_task=f"finish_{endpoint_index}",
        )

        can_post_to_endpoint = rail.IfOperator(
            task_id=f"can_post_to_endpoint_{endpoint_index}",
            test=lambda: rail.result(get_endpoint_details.task_id)[
                'can_post_to_api'].lower() == "yes",
            yes_task=f"upload_json_payload_sftp_{endpoint_index}",
            no_task=f"finish_{endpoint_index}"
        )

        finish = rail.EmptyOperator(
            task_id=f"finish_{endpoint_index}"
        )

        upload_json_payload_sftp = rail.SFTPUploadFileOperator(
            task_id=f"upload_json_payload_sftp_{endpoint_index}",
            content="{{ result('prepare_api_payload') }}",
            #pylint: disable=line-too-long
            remote_filepath=f'{open_bracket} result("{get_endpoint_details.task_id}").json_upload_filepath {close_bracket}/{open_bracket} dag_run.conf.export_file_name {close_bracket}.json'
        )

        # New header structure for Germany dev testing.
        # can_post_to_api is "No" for all other instances, so directly updating headers here for dev testing.
        post_to_endpoint = rail.HTTPUploadFileOperator(
            task_id=f"post_to_endpoint_{endpoint_index}",
            method="POST",
            http_conn_id=f'{open_bracket} result("{get_endpoint_details.task_id}").api_http_conn_id {close_bracket}',
            content_type="application/json",
            endpoint=f'{open_bracket} result("{get_endpoint_details.task_id}").endpoint_url {close_bracket}',
            content="{{ result('prepare_api_payload') }}",
            headers={
                "Content-Type": "application/json",
                "ClientId": f'{open_bracket} result("{get_endpoint_details.task_id}").header.apikey {close_bracket}',
                "ClientSecret": f'{open_bracket} result("{get_endpoint_details.task_id}").header.apikeysecret {close_bracket}',
                "Ocp-Apim-Subscription-Key": f'{open_bracket} result("{get_endpoint_details.task_id}").header.proxyauth {close_bracket}',
                "filename": f'{open_bracket} result("{get_endpoint_details.task_id}").header.filename {close_bracket}',
                "territory": f'{open_bracket} result("{get_endpoint_details.task_id}").header.territory {close_bracket}'
            },
            retries=0
        )

        catch_error = rail.EmptyOperator(
            task_id=f'catch_error_{endpoint_index}',
            trigger_rule='one_failed'
        )

        is_api_upload_failed = rail.IfOperator(
            task_id=f"is_api_upload_failed_{endpoint_index}",
            test=f'{open_bracket}get_task_state("post_to_endpoint_{endpoint_index}") == "failed"{close_bracket}',
            yes_task=f"upload_json_payload_backup_sftp_{endpoint_index}"
        )

        upload_json_payload_backup_sftp = rail.SFTPUploadFileOperator(
            task_id=f"upload_json_payload_backup_sftp_{endpoint_index}",
            content="{{ result('prepare_api_payload') }}",
            #pylint: disable=line-too-long
            remote_filepath=f'{open_bracket} result("{get_endpoint_details.task_id}").api_failed_upload_filepath {close_bracket}/{open_bracket} dag_run.conf.export_file_name {close_bracket}.json'
        )

        send_api_fail_email = rail.EmailOperator(
            task_id=f"send_api_fail_email_{endpoint_index}",
            subject="{{ get_company_key() }} | API Posting Failed for {{ dag_run.conf.export_file_name }} - {{ dag_run.conf.process_start_time }}",
            html_content="email_api_posting_failed.html",
            to=f'{open_bracket} result("{get_endpoint_details.task_id}").failure_email_address {close_bracket}',
            bcc=config.api_failed_alert_email,
            params={
                "get_endpoint_details": get_endpoint_details.task_id
            }
        )

        is_post_to_api_success_fail = rail.IfOperator(
            task_id=f"is_post_to_api_success_fail_{endpoint_index}",
            trigger_rule='all_done',
            test=lambda: bool(get_task_state(post_to_endpoint.task_id) == "success" or get_task_state(post_to_endpoint.task_id) == "failed"),
            yes_task=f"get_api_log_{endpoint_index}"
        )

        get_api_log = rail.PythonOperator(
            task_id=f"get_api_log_{endpoint_index}",
            python_callable=lambda dag_run: get_api_log_message(
                dag_run, post_to_endpoint.task_id, get_endpoint_details.task_id)
        )

        get_endpoint_details >> has_found_endpoint >> rail.Label(
            "Yes") >> can_post_to_endpoint >> rail.Label("No") >> finish
        has_found_endpoint >> rail.Label("No") >> finish

        can_post_to_endpoint >> rail.Label("Yes") >> upload_json_payload_sftp >> post_to_endpoint
        post_to_endpoint >> rail.Label("On Error") >> catch_error >> is_api_upload_failed >> rail.Label(
            "Yes") >> upload_json_payload_backup_sftp >> send_api_fail_email
        post_to_endpoint >> is_post_to_api_success_fail
        is_post_to_api_success_fail >> rail.Label("Yes") >> get_api_log

        return get_endpoint_details, finish, get_api_log
