import rail


def assert_department_response(error_message):
    expected_value = [
        'urn:replicon-tenant:a7427f9c8a1747fc81f7ef31746e293e:department-group:bf22b06c-8a20-4603-9eaf-83ed518b120c']
    actual_value = rail.result('get_company_department')
    assert expected_value == actual_value, error_message


def assert_division_response(error_message):
    expected_value = [
        'urn:replicon-tenant:a7427f9c8a1747fc81f7ef31746e293e:division:2f07137d-8a99-48b2-a559-1a7a6e3d4b48']
    actual_value = rail.result('get_ftp_divisions')
    assert expected_value == actual_value, error_message


def assert_employeetype_response(error_message):
    expected_value = [
        'urn:replicon-tenant:a7427f9c8a1747fc81f7ef31746e293e:employee-type-group:76b7cb64-800f-4ea2-99e6-22e688d11e05']
    actual_value = rail.result('get_salaried_employeetype')
    assert expected_value == actual_value, error_message
