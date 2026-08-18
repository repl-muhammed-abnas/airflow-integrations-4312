"""Unit tests for pure helpers in firm_sync_upsert."""
from unittest import TestCase

from vp_xero_integration_v2.firm_sync_upsert.utils.python_callable_method import (
    _build_addresses,
    _build_phones,
)


class TestFirmSyncUpsertHelpers(TestCase):

    def test_build_phones_empty_when_no_number(self):
        self.assertEqual(_build_phones({}), [])
        self.assertEqual(_build_phones({'phone_number': None}), [])
        self.assertEqual(_build_phones({'phone_number': ''}), [])

    def test_build_phones_returns_list_with_number(self):
        result = _build_phones({'phone_number': '555-1234'})
        self.assertEqual(result, [{'PhoneType': 'DEFAULT', 'PhoneNumber': '555-1234'}])

    def test_build_phones_coerces_to_string(self):
        result = _build_phones({'phone_number': 5551234})
        self.assertEqual(result[0]['PhoneNumber'], '5551234')

    def test_build_addresses_empty_when_no_addrs(self):
        self.assertEqual(_build_addresses({}), [])

    def test_build_addresses_omits_addr_with_only_type(self):
        # Only AddressType, no other fields → omitted (len(clean) <= 1)
        result = _build_addresses({'street_addr': {'AddressType': 'STREET'}})
        self.assertEqual(result, [])

    def test_build_addresses_includes_addr_with_city(self):
        result = _build_addresses({
            'street_addr': {'AddressType': 'STREET', 'City': 'New York'}
        })
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]['City'], 'New York')

    def test_build_addresses_both_types(self):
        result = _build_addresses({
            'street_addr': {'AddressType': 'STREET', 'City': 'NYC'},
            'pobox_addr': {'AddressType': 'POBOX', 'City': 'NYC'},
        })
        self.assertEqual(len(result), 2)
