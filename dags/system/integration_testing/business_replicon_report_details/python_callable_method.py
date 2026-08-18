import rail


def assert_response(error_message):
    expected_value = 'urn:replicon-tenant:a7427f9c8a1747fc81f7ef31746e293e:report:3657914e-ce9d-41c7-ba7f-8f6e94bc233b'
    actual_value = rail.result('get_user_report_details')['uri']
    assert expected_value == actual_value, error_message
