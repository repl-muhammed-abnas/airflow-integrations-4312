# pylint: disable=too-many-statements
from rail import find_first_by_attr_and_get_attr


def get_permission_set(response):
    permission_set = find_first_by_attr_and_get_attr(
        response, 'policyUri', 'urn:replicon:policy:client-representation', 'permissionSet')
    return permission_set.get('name') if permission_set else False


def get_auth_token(response):
    auth_token = find_first_by_attr_and_get_attr(
        response['sessionCookies'], 'name', 'AUTHTOKEN', 'value')
    return auth_token.strip()
