import rail


def assert_response(error_message):
    expected_value = {'author': 'Gambardella, Matthew', 'genre': 'Computer',
                      'price': '44.95', 'title': "XML Developer's Guide"}
    actual_value = rail.load_all_records(rail.result('load_artifact_xml_data'))
    assert expected_value in actual_value, error_message


def assert_target_response(error_message):
    expected_value = [{'author': 'Gambardella, Matthew', 'genre': 'Computer', 'price': '44.95', 'title': "XML Developer's Guide"}, {
        'author': 'Galos, Mike', 'genre': 'Computer', 'price': '49.95', 'title': 'Visual Studio 7: A Comprehensive Guide'}]
    actual_value = rail.result('load_result_xml_data')
    assert expected_value == actual_value, error_message
