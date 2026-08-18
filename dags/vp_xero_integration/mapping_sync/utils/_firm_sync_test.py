"""
Unit tests for the pure helper functions in `_firm_sync` (Xero → VP firm
mapping). These cover the deterministic field/body logic without Airflow or VP
round-trips; the full engine (`sync_xero_firms_to_vp`) is exercised end-to-end
against dev-airflow.
"""
from unittest import TestCase

from vp_xero_integration.mapping_sync.utils._firm_sync import (
    _parse_account_number,
    _xero_status_to_vp_status,
    _xero_contact_phone,
    _resolve_code,
    build_vp_firm_create_body,
    build_vp_firm_address_bodies,
)


class TestFirmSyncHelpers(TestCase):

    # --- _parse_account_number (SL<client>/PL<vendor>) ---

    def test_parse_account_number_full(self):
        self.assertEqual(_parse_account_number('SL100/PL200'), ('100', '200'))

    def test_parse_account_number_client_only(self):
        self.assertEqual(_parse_account_number('SL100'), ('100', ''))

    def test_parse_account_number_no_prefixes(self):
        self.assertEqual(_parse_account_number('100/200'), ('100', '200'))

    def test_parse_account_number_blank(self):
        self.assertEqual(_parse_account_number(''), ('', ''))
        self.assertEqual(_parse_account_number(None), ('', ''))

    # --- status mapping ---

    def test_status_active_archived(self):
        self.assertEqual(_xero_status_to_vp_status('ACTIVE'), 'A')
        self.assertEqual(_xero_status_to_vp_status('archived'), 'I')
        self.assertEqual(_xero_status_to_vp_status(''), 'A')
        self.assertEqual(_xero_status_to_vp_status(None), 'A')

    # --- phone preference (DDI over DEFAULT) ---

    def test_phone_prefers_ddi(self):
        contact = {'Phones': [
            {'PhoneType': 'DEFAULT', 'PhoneNumber': '111'},
            {'PhoneType': 'DDI', 'PhoneNumber': '222'},
        ]}
        self.assertEqual(_xero_contact_phone(contact), '222')

    def test_phone_falls_back_to_default(self):
        contact = {'Phones': [{'PhoneType': 'DEFAULT', 'PhoneNumber': '111'}]}
        self.assertEqual(_xero_contact_phone(contact), '111')

    def test_phone_none_when_absent(self):
        self.assertIsNone(_xero_contact_phone({'Phones': []}))

    # --- _resolve_code ---

    def test_resolve_code_by_description(self):
        index = {'united states': 'US', 'new york': 'NY'}
        self.assertEqual(_resolve_code(index, 'United States'), 'US')

    def test_resolve_code_passthrough_unknown(self):
        self.assertEqual(_resolve_code({}, 'Atlantis'), 'Atlantis')

    def test_resolve_code_none_for_blank(self):
        self.assertIsNone(_resolve_code({'x': 'y'}, ''))
        self.assertIsNone(_resolve_code({'x': 'y'}, None))

    # --- build_vp_firm_create_body ---

    def test_create_body_customer_supplier_inds_and_codes(self):
        contact = {
            'Name': 'Acme', 'ContactStatus': 'ACTIVE',
            'IsCustomer': True, 'IsSupplier': True,
        }
        body = build_vp_firm_create_body(contact, 'ORG1', '100', '200')
        self.assertEqual(body['Name'], 'Acme')
        self.assertEqual(body['SortName'], 'Acme')
        self.assertEqual(body['ClientInd'], 'Y')
        self.assertEqual(body['VendorInd'], 'Y')
        self.assertEqual(body['Client'], '100')
        self.assertEqual(body['Vendor'], '200')
        self.assertEqual(body['Org'], 'ORG1')
        self.assertEqual(body['Status'], 'A')
        self.assertEqual(body['ReadyForApproval'], True)

    def test_create_body_drops_none_org_and_codes(self):
        contact = {'Name': 'NoCodes', 'ContactStatus': 'ARCHIVED',
                   'IsCustomer': False, 'IsSupplier': False}
        body = build_vp_firm_create_body(contact, None, '', '')
        self.assertNotIn('Org', body)
        self.assertNotIn('Client', body)
        self.assertNotIn('Vendor', body)
        self.assertEqual(body['ClientInd'], 'N')
        self.assertEqual(body['VendorInd'], 'N')
        self.assertEqual(body['Status'], 'I')

    # --- build_vp_firm_address_bodies ---

    def test_address_bodies_street_and_pobox(self):
        contact = {
            'EmailAddress': 'a@b.com', 'TaxNumber': 'TX1',
            'Phones': [{'PhoneType': 'DDI', 'PhoneNumber': '222'}],
            'Addresses': [
                {'AddressType': 'STREET', 'AddressLine1': '1 Main',
                 'City': 'NYC', 'Region': 'New York', 'Country': 'United States',
                 'PostalCode': '10001'},
                {'AddressType': 'POBOX', 'AddressLine1': 'PO 9', 'City': 'NYC'},
                {'AddressType': 'DELIVERY', 'AddressLine1': 'ignore me'},
            ],
        }
        bodies = build_vp_firm_address_bodies(
            contact,
            {'united states': 'US'},
            {'new york': 'NY'},
        )
        # DELIVERY dropped; STREET + POBOX kept.
        self.assertEqual(len(bodies), 2)
        street = next(b for b in bodies if b['AddressType'] == 'STREET')
        pobox = next(b for b in bodies if b['AddressType'] == 'POBOX')
        self.assertEqual(street['PrimaryInd'], 'true')
        self.assertEqual(street['Billing'], 'false')
        self.assertEqual(street['State'], 'NY')
        self.assertEqual(street['Country'], 'US')
        self.assertEqual(street['Phone'], '222')
        self.assertEqual(street['Email'], 'a@b.com')
        self.assertEqual(street['TaxRegistrationNumber'], 'TX1')
        self.assertIn('CLAddressID', street)
        self.assertEqual(pobox['Billing'], 'true')
        self.assertEqual(pobox['PrimaryInd'], 'false')

    def test_address_bodies_empty_when_no_addresses(self):
        self.assertEqual(build_vp_firm_address_bodies({}, {}, {}), [])
