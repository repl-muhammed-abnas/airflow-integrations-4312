import rail
from system.integration_testing.config import tenant_slug



def assert_searchuser_method(error_message):
    assert f"urn:replicon-tenant:{tenant_slug}:user:2" == rail.result(
        "search_adminuser_list_operator"), error_message


def assert_createuser_data(error_message):
    expected_entries = {
        "displayText": "b1, a1",
        "loginName": "a1b1",
        "slug": "a1b1"
    }

    assert expected_entries == {
        k: v for k, v in rail.result(
            "create_user").items() if k not in ('uri')
    }, error_message


def assert_repliconpage_operator(error_message):
    dag_run_conf = rail.get_current_context()['dag_run'].conf
    assert dag_run_conf['employeeid'] == rail.result(
        "search_createduser"), error_message


def assert_target_value(error_message):
    dag_run_conf = rail.get_current_context()['dag_run'].conf
    actual_value = rail.load_json_artifact(rail.result("get_user_details"))[
        "employeeId"
    ]
    assert dag_run_conf['employeeid'] == actual_value, error_message


def assert_employeetypegrouplist_value(error_message):
    response = rail.result("get_all_employeetypegrouplist_data")
    expected_value = [
        {
            'name': 'Consultant',
            'uri': f'urn:replicon-tenant:{tenant_slug}:employee-type-group:3f87ea90-9512-4ef4-b737-0af2cf4477d8',
            'code': 'Consultant'
        }
    ]
    assert expected_value == [response[0]], error_message


def assert_response_check(error_message):
    response = rail.result("bulk_get_users")
    dag_run_conf = rail.get_current_context()['dag_run'].conf
    actual_value = response[0].get("userDetails", {}).get("employeeId")
    assert dag_run_conf['employeeid'] == actual_value, error_message


def assert_input_data(error_message):
    assert f"urn:replicon-tenant:{tenant_slug}:user:2" in rail.result(
        "get_user_uris"), error_message  # check if admin user is present in Replicon


def assert_responsecheck(error_message):
    actual_entries = list(filter(
        lambda x: x["loginName"] == "admin", rail.result("check_response_data")))
    assert any("admin" == actual["loginName"]
               for actual in actual_entries
               ), error_message


def assert_batch_response(error_message):
    expected_value = {
        'batchUri': True,
        'endTimestamp': True,
        'executionState': True
    }
    actual_response = rail.result('wait_for_delete_batch')
    actual_value = {
        key: key in actual_response for key in ['batchUri', 'endTimestamp', 'executionState']
    }
    assert expected_value == actual_value, error_message
