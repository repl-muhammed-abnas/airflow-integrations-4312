import rail


def parse_name(full_name):
    """
    Parse full name into first and last name.

    Args:
        full_name (str): Full name to parse

    Returns:
        tuple: (first_name, last_name) or (None, None) if invalid
    """
    if not full_name or not full_name.strip():
        return None, None

    parts = full_name.strip().split()
    if len(parts) == 0:
        return None, None
    elif len(parts) == 1:
        return parts[0], parts[0]  # Single name goes to both
    else:
        # First word is first name, rest is last name
        return parts[0], ' '.join(parts[1:])


def create_customer_qbo_request(dag_run):
    """
    Create QuickBooks customer request payload from Replicon client details.

    Args:
        dag_run: Airflow dag_run object containing configuration

    Returns:
        dict: QuickBooks customer API request payload
    """
    client_details = rail.result('get_client_details')
    billing_address = client_details.get('billingAddress', {})
    client_address = client_details.get('clientAddress', {})

    # Build base payload with required fields
    payload = {
        'DisplayName': dag_run.conf['client_name'],
        'FullyQualifiedName': dag_run.conf['client_name'],
        'Active': True,
        'CompanyName': client_details.get('name', ''),
        'Job': False,
        'BillWithParent': False,
    }

    # Add optional email address
    primary_email_address = billing_address.get('email')
    if primary_email_address:
        payload['PrimaryEmailAddr'] = {'Address': primary_email_address}

    # Add optional phone number
    primary_phone = billing_address.get('phoneNumber')
    if primary_phone:
        payload['PrimaryPhone'] = {'FreeFormNumber': primary_phone}

    # Add billing address if available
    if billing_address:
        bill_addr = {}
        if billing_address.get('city'):
            bill_addr['City'] = billing_address['city']
        if billing_address.get('address'):
            bill_addr['Line1'] = billing_address['address']
        if billing_address.get('zipPostalCode'):
            bill_addr['PostalCode'] = billing_address['zipPostalCode']
        if billing_address.get('country') and billing_address['country'].get('name'):
            bill_addr['Country'] = billing_address['country']['name']
        if billing_address.get('stateProvince'):
            bill_addr['CountrySubDivisionCode'] = billing_address['stateProvince']

        if bill_addr:
            payload['BillAddr'] = bill_addr

    # Add shipping address if available
    if client_address:
        ship_addr = {}
        if client_address.get('city'):
            ship_addr['City'] = client_address['city']
        if client_address.get('address'):
            ship_addr['Line1'] = client_address['address']
        if client_address.get('zipPostalCode'):
            ship_addr['PostalCode'] = client_address['zipPostalCode']
        if client_address.get('country') and client_address['country'].get('name'):
            ship_addr['Country'] = client_address['country']['name']
        if client_address.get('stateProvince'):
            ship_addr['CountrySubDivisionCode'] = client_address['stateProvince']

        if ship_addr:
            payload['ShipAddr'] = ship_addr

    # Parse and add contact name
    billing_contact = client_details.get('billingContact', '')
    first_name, last_name = parse_name(billing_contact)
    if first_name:
        payload['FirstName'] = first_name
    if last_name:
        payload['FamilyName'] = last_name

    # Add optional notes
    if client_details.get('comment'):
        payload['Notes'] = client_details['comment']

    # Add optional fax number
    fax_number = billing_address.get('faxNumber')
    if fax_number:
        payload['Fax'] = {'FreeFormNumber': fax_number}

    # Add optional website
    website = client_address.get('webSite')
    if website:
        payload['WebAddr'] = {'URI': website}

    return payload



