import json
import rail


def assert_get_method(error_message):
    actual_value = rail.result('get_simple_http_operator')
    assert len(actual_value) > 0, error_message


def assert_post_method(error_message):
    expected_value = {
        "title": "foo",
        "body": "bar",
        "userId": 1,
        "id": 101
    }
    actual_value = json.loads(rail.result('post_simple_http_operator'))
    assert expected_value == actual_value, error_message


def assert_s3_method(error_message):
    expected_value = [{'value': '1', 'id': '1'}, {'value': '2', 'id': '2'}]
    actual_value = rail.load_all_records(rail.result('load_csv_file'))

    assert expected_value == actual_value, error_message


def assert_s3listkey_method(error_message):
    expected_value = rail.render_template(
        '{{dag_run.conf.network_filepath}}{{dag_run_ecid()}}_{{dag_run.conf.file_name}}')
    actual_value = rail.result('list_s3_reference_files')

    assert expected_value in actual_value, error_message


def assert_s3movefile_method(error_message):
    expected_value = [{'value': '1', 'id': '1'}, {'value': '2', 'id': '2'}]
    actual_value = rail.load_all_records(rail.result('load_new_csv_file'))

    assert expected_value == actual_value, error_message
