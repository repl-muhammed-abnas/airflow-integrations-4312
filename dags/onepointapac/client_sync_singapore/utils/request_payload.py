import uuid
import rail


def get_create_client_param(dag_run):
    return {
        'target': None,
        'modifications': {
            'nameToApply': {'value': dag_run.conf.get('client_name')} if dag_run.conf.get('client_name') else None,
            'descriptionToApply': None,
            'statusToApply': True,
            'clientContactToApply': {'value': dag_run.conf.get('billing_contact')} if dag_run.conf.get('billing_contact') else None,
            'clientAddressToApply': {
                'address': {'value': dag_run.conf.get('client_address')} if dag_run.conf.get('client_address') else None,
                'city': {'value': dag_run.conf.get('client_city')} if dag_run.conf.get('client_city') else None,
                'stateProvince': {'value': dag_run.conf.get('client_state_province')} if dag_run.conf.get('client_state_province') else None,
                'country': {'value': {'uri': dag_run.conf.get('client_country')}} if dag_run.conf.get('client_country') else None,
                'zipPostalCode': {'value': dag_run.conf.get('client_zip_postal_code')} if dag_run.conf.get('client_zip_postal_code') else None,
                'phoneNumber': {'value': dag_run.conf.get('client_phone_number')} if dag_run.conf.get('client_phone_number') else None,
                'email': {'value': dag_run.conf.get('client_email')} if dag_run.conf.get('client_email') else None,
                'faxNumber': {'value': dag_run.conf.get('client_fax_number')} if dag_run.conf.get('client_fax_number') else None,
                'website': None
            },
            'billingAddressToApply': {
                'address': {'value': dag_run.conf.get('billing_address')} if dag_run.conf.get('billing_address') else None,
                'city': {'value': dag_run.conf.get('billing_city')} if dag_run.conf.get('billing_city') else None,
                'stateProvince': {'value': dag_run.conf.get('billing_state_province')} if dag_run.conf.get('billing_state_province') else None,
                'country': {'value': {'uri': dag_run.conf.get('billing_country')}} if dag_run.conf.get('billing_country') else None,
                'zipPostalCode': {'value': dag_run.conf.get('billing_zip_postal_code')} if dag_run.conf.get('billing_zip_postal_code') else None,
                'phoneNumber': None,
                'email': None,
                'faxNumber': None,
                'website': None
            },
            'billingRatesToApply': None,
            'clientManagerToApply': None,
            'clientSharingToApply': None,
            'expenseCodesToApply': None,
            'customFieldsToApply': [],
            'taxProfileToApply': None
        },
        'clientModificationOptionUri': 'urn:replicon:client-modification-option:save',
        'unitOfWorkId': str(uuid.uuid4()),
    }


def get_update_currency_payload(dag_run):
    return {
        'clientUri': rail.result('create_client').get('uri'),
        'currency': {'symbol': dag_run.conf.get('currency')}
    }
