"""
PGP utility functions for monitoring PGP key expiry from Airflow connections.

Uses gnupg library (same as RAIL) to parse PGP public keys and extract expiration dates.
"""
import json
import os
import tempfile
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any

import gnupg

from airflow.models import Connection, Variable
from airflow.utils.session import NEW_SESSION, provide_session


def _extract_uid(uids: list) -> Optional[str]:
    """
    Extract the first UID from PGP UIDs list.

    UIDs are typically in format "Name <email@example.com>" or just "Name".
    """
    if not uids:
        return None

    uid = uids[0]
    return uid.strip() if uid.strip() else None


def extract_pgp_key_info(public_key_string: str) -> Dict[str, Any]:
    """
    Extract metadata from a PGP public key string.

    Uses gnupg library (same approach as RAIL's PGP operators).
    Checks all keys and subkeys in the bundle and returns the earliest expiry date.
    """
    if not public_key_string:
        return {
            'created_at': None,
            'expires_at': None,
            'uid': None
        }

    try:
        with tempfile.TemporaryDirectory() as gnupghome:
            gpg_client = gnupg.GPG(gnupghome=gnupghome)
            import_result = gpg_client.import_keys(public_key_string)

            if not import_result.results:
                raise ValueError("Failed to import PGP key")

            keys = gpg_client.list_keys()

            if not keys:
                raise ValueError("No keys found after import")

            primary_key = keys[0]
            uid = _extract_uid(primary_key.get('uids', []))

            created_at = None
            if primary_key.get('date'):
                created_at = datetime.fromtimestamp(int(primary_key['date']), tz=timezone.utc)

            earliest_expiry = None

            for key in keys:
                if key.get('expires') and key['expires']:
                    key_expiry = datetime.fromtimestamp(int(key['expires']), tz=timezone.utc)
                    if earliest_expiry is None or key_expiry < earliest_expiry:
                        earliest_expiry = key_expiry

                subkeys = key.get('subkeys', [])
                for subkey in subkeys:
                    if len(subkey) >= 3 and subkey[2]:
                        try:
                            subkey_expiry = datetime.fromtimestamp(int(subkey[2]), tz=timezone.utc)
                            if earliest_expiry is None or subkey_expiry < earliest_expiry:
                                earliest_expiry = subkey_expiry
                        except (ValueError, TypeError):
                            continue

            return {
                'created_at': created_at,
                'expires_at': earliest_expiry,
                'uid': uid
            }

    except ValueError:
        raise
    except Exception as e:
        raise ValueError(f"Could not parse PGP key: {e}") from e


def _extract_key_from_extra(extra_field: Optional[str]) -> Optional[str]:
    """Extract PGP key from the extra field which is stored as JSON."""
    if not extra_field:
        return None

    try:
        extra_data = json.loads(extra_field)
        key_fields = [
            'extra__pgp__public_key',
            'public_key',
            'pgp_public_key',
            'key',
            'pgp_key'
        ]
        for field in key_fields:
            if field in extra_data and extra_data[field]:
                return extra_data[field]
        return None
    except (json.JSONDecodeError, TypeError):
        if '-----BEGIN PGP PUBLIC KEY BLOCK-----' in extra_field:
            return extra_field
        return None


def _extract_key_from_connection(extra: Optional[str], password: Optional[str] = None) -> Optional[str]:
    """Extract PGP public key from connection extra or password fields."""
    extra_key = _extract_key_from_extra(extra)
    if extra_key:
        return extra_key

    if password and '-----BEGIN PGP PUBLIC KEY BLOCK-----' in password:
        return password

    return None


@provide_session
def get_pgp_connections(session=NEW_SESSION) -> List[Dict[str, Any]]:
    """Query all Airflow connections where conn_type is 'pgp'."""
    conn_ids = session.query(Connection.conn_id).filter(
        Connection.conn_type == 'pgp'
    ).all()

    pgp_connections = []
    for (conn_id,) in conn_ids:
        conn = Connection.get_connection_from_secrets(conn_id)
        extra = conn.extra
        password = conn.password
        public_key = _extract_key_from_connection(extra, password)
        pgp_connections.append({
            'conn_id': conn_id,
            'conn_type': conn.conn_type or '',
            'description': conn.description or '',
            'host': conn.host or '',
            'schema': conn.schema or '',
            'login': conn.login or '',
            'port': conn.port,
            'public_key': public_key
        })

    return pgp_connections


def get_pgp_key_expiry_status(
    warning_days_min: int,
    warning_days_max: int,
    excluded_conn_ids_var_name: Optional[str] = None
) -> Dict[str, Any]:
    """
    Fetch all PGP connections, parse keys, and check for expired/expiring keys.

    Alerts for:
    - Expired keys (days_left < 0)
    - Keys expiring within the warning window (warning_days_min <= days_left <= warning_days_max)
    """
    excluded_conn_ids = []
    if excluded_conn_ids_var_name:
        try:
            excluded_json = Variable.get(excluded_conn_ids_var_name, default_var='[]')
            excluded_conn_ids = json.loads(excluded_json)
        except json.JSONDecodeError:
            pass

    pgp_connections = get_pgp_connections()
    print(f"[DEBUG] Total PGP connections found: {len(pgp_connections)}")

    if not pgp_connections:
        return {
            'should_send_alert': False,
            'expired': [],
            'expiring_soon': [],
            'expired_count': 0,
            'expiring_count': 0,
            'warning_days_min': warning_days_min,
            'warning_days_max': warning_days_max,
            'region': os.environ.get('REGION', 'unknown'),
            'environment': os.environ.get('AIRFLOW_ENVIRONMENT', 'unknown')
        }

    now = datetime.now(timezone.utc)
    expired = []
    expiring_soon = []

    for conn_data in pgp_connections:
        conn_id = conn_data['conn_id']
        print(f"[DEBUG] Processing connection: {conn_id}")

        if conn_id in excluded_conn_ids:
            print(f"[DEBUG] {conn_id}: Skipped - excluded")
            continue

        public_key = conn_data.get('public_key')
        if not public_key:
            print(f"[DEBUG] {conn_id}: Skipped - no public key found")
            continue

        try:
            key_info = extract_pgp_key_info(public_key)
            print(f"[DEBUG] {conn_id}: Key parsed successfully")
        except ValueError as e:
            print(f"[DEBUG] {conn_id}: Skipped - parse error: {e}")
            continue

        expires_at = key_info.get('expires_at')

        if expires_at is None:
            print(f"[DEBUG] {conn_id}: Skipped - no expiry date")
            continue

        print(f"[DEBUG] {conn_id}: expires_at={expires_at}, days_left={(expires_at.date() - now.date()).days}")

        days_left = (expires_at.date() - now.date()).days

        created_at = key_info.get('created_at')
        validity = ''
        if created_at:
            validity_days = (expires_at.date() - created_at.date()).days
            validity_years = validity_days // 365
            if validity_years > 0:
                validity = f"{validity_years} Year{'s' if validity_years > 1 else ''} ({validity_days} Days)"
            else:
                validity = f"{validity_days} Days"

        result_entry = {
            'conn_id': conn_id,
            'uid': key_info.get('uid', ''),
            'description': conn_data.get('description', ''),
            'created_at': created_at.isoformat() if created_at else '',
            'validity': validity,
            'expires_at': expires_at.isoformat(),
            'days_left': days_left
        }

        if days_left < 0:
            expired.append(result_entry)
        elif warning_days_min <= days_left <= warning_days_max:
            expiring_soon.append(result_entry)

    expired.sort(key=lambda x: x['days_left'])
    expiring_soon.sort(key=lambda x: x['days_left'])

    print(f"[DEBUG] Final counts - Expired: {len(expired)}, Expiring soon: {len(expiring_soon)}")

    return {
        'should_send_alert': len(expired) > 0 or len(expiring_soon) > 0,
        'expired': expired,
        'expiring_soon': expiring_soon,
        'expired_count': len(expired),
        'expiring_count': len(expiring_soon),
        'warning_days_min': warning_days_min,
        'warning_days_max': warning_days_max,
        'region': os.environ.get('REGION', 'unknown'),
        'environment': os.environ.get('AIRFLOW_ENVIRONMENT', 'unknown')
    }
