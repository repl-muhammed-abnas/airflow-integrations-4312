import pycountry
import rail


def parse_xero_contacts():
    def get_country_name_from_iso(iso_code):
        if iso_code is None or (isinstance(iso_code, str) and len(iso_code) > 3):
            return iso_code
        country = pycountry.countries.get(
            alpha_2=iso_code) or pycountry.countries.get(alpha_3=iso_code)
        return country.name if country else ''

    country_mapper = {
        'US': 'United States',
        'USA': 'United States',
        'America': 'United States',
        'United States of America': 'United States',
        'HK': 'Hong Kong',
        'SG': 'Singapore',
        'UK': 'United Kingdom',
        'UAE': 'United Arab Emirates'
    }
    incoming_contact = []
    contact_raw_data = rail.result('get_new_or_updated_contacts_in_xero').get('Contacts', [])
    existing_client_in_replicon = rail.result('get_all_clients')
    countries_from_replicon = rail.result('get_all_countries')

    for contact in contact_raw_data:
        if not (contact.get('Name') or '').strip():
            continue

        existing_client_info = list(filter(
            lambda data: contact.get('Name') and
            data['name'].lower() == contact['Name'].lower(), existing_client_in_replicon))

        street_address = rail.find_first_by_attr_and_get_attr(
            contact.get('Addresses', []), 'AddressType', 'STREET')
        postal_address = rail.find_first_by_attr_and_get_attr(
            contact.get('Addresses', []), 'AddressType', 'POBOX')

        client_country_to_assign = country_mapper.get(
            street_address.get('Country') if street_address else None,
            get_country_name_from_iso(street_address.get('Country') if street_address else None))
        billing_country_to_assign = country_mapper.get(
            postal_address.get('Country') if postal_address else None,
            get_country_name_from_iso(postal_address.get('Country') if postal_address else None))

        client_country_info = list(filter(
            lambda data: client_country_to_assign and
            data.get('name', '').lower() == client_country_to_assign.lower(), countries_from_replicon))
        billing_country_info = list(filter(
            lambda data: billing_country_to_assign and
            data.get('name', '').lower() == billing_country_to_assign.lower(), countries_from_replicon))

        default_phone = rail.find_first_by_attr_and_get_attr(
            contact.get('Phones', []), 'PhoneType', 'DEFAULT')
        fax_phone = rail.find_first_by_attr_and_get_attr(
            contact.get('Phones', []), 'PhoneType', 'FAX')

        client_address = ''
        if street_address and street_address.get('AddressLine1'):
            if street_address.get('AddressLine2') and street_address.get('AddressLine2').strip() not in street_address.get('AddressLine1'):
                client_address = ', '.join([street_address.get('AddressLine1'), street_address.get('AddressLine2')])
            else:
                client_address = street_address.get('AddressLine1')

        billing_address = ''
        if postal_address and postal_address.get('AddressLine1'):
            if postal_address.get('AddressLine2') and postal_address.get('AddressLine2').strip() not in postal_address.get('AddressLine1'):
                billing_address = ', '.join([postal_address.get('AddressLine1'), postal_address.get('AddressLine2')])
            else:
                billing_address = postal_address.get('AddressLine1')

        incoming_contact.append({
            'client_name': contact.get('Name', ''),
            'is_new_client': not existing_client_info,
            'billing_contact': f"{contact.get('FirstName', '')} {contact.get('LastName', '')}".strip(),
            'client_address': client_address,
            'client_city': street_address.get('City', '') if street_address else '',
            'client_state_province': street_address.get('Region', '') if street_address else '',
            'client_country': client_country_info[0]['uri'] if client_country_info else None,
            'client_zip_postal_code': street_address.get('PostalCode', '') if street_address else '',
            'billing_address': billing_address,
            'billing_city': postal_address.get('City', '') if postal_address else '',
            'billing_state_province': postal_address.get('Region', '') if postal_address else '',
            'billing_country': billing_country_info[0]['uri'] if billing_country_info else None,
            'billing_zip_postal_code': postal_address.get('PostalCode', '') if postal_address else '',
            'client_phone_number': default_phone.get('PhoneNumber', '') if default_phone else '',
            'client_fax_number': fax_phone.get('PhoneNumber', '') if fax_phone else '',
            'client_email': contact.get('EmailAddress', '')
        })

    return incoming_contact


def get_downstreamtasks_error(client_name, error_message):
    return {
        'error': f'Error with {client_name} - {error_message}'
    }
