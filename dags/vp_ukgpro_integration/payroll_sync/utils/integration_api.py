"""
Integration Platform API utilities for VP UKG Pro Payroll Sync.
Handles OAuth2 authentication and integration config retrieval.
"""
import base64
import json
import logging
from airflow.models import Variable
import rail


def get_oauth2_credentials():
    """
    Retrieve OAuth2 credentials from Airflow Variables.
    
    Returns:
        dict: Contains client_id, client_secret, and basic_auth
    """
    client_id = Variable.get('vantagepoint_client_id')
    client_secret = Variable.get('vantagepoint_client_secret')

    # Create basic auth header (for reference, though not used in OAuth2 flow)
    credentials = f"{client_id}:{client_secret}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()

    return {
        'client_id': client_id,
        'client_secret': client_secret,
        'basic_auth': f'Basic {encoded_credentials}'
    }


def parse_integration_config(instance_name='dev'):
    """
    Parse integration configuration from Integration Platform API response.
    
    Args:
        instance_name: Instance name for fallback client_id (default: 'dev')
    
    Returns:
        dict: Parsed integration config with connection IDs and settings
        
    Raises:
        ValueError: If no integration found or response is invalid
    """
    # Get bearer token from OAuth2 response
    oauth_response = json.loads(rail.result('get_oauth_token'))
    bearer_token = oauth_response.get('access_token')

    # Get integration details using bearer token
    integration_response = json.loads(
        rail.result('get_integration_config')
    )

    # Extract first integration (should be filtered by integration type)
    integrations = integration_response.get('integrations', [])
    if not integrations:
        raise ValueError(
            "No integration found for type: payroll_sync, status: enabled"
        )

    integration = integrations[0]

    # Get client_id from database for this integration
    # Use instance_name as fallback if not provided
    client_id = integration.get('client_id', instance_name)

    # Extract connection IDs and config
    parsed_config = {
        'integration_id': integration.get('id'),
        'customer_id': integration.get('customer_id'),
        'clientId': client_id,  # For middleware reporting
        'vp_conn_id': integration.get('connections')['vantagepoint'],
        'ukgpro_conn_id': integration.get('connections')['ukgpro'],
        'integration_type': integration.get('integration_type', 'payroll_sync'),
        'config': integration.get('config', {}),
        'bearer_token': bearer_token,  # Store for future use if needed
    }

    logging.info(
        "Parsed integration config: integration_id=%s, customer_id=%s, "
        "vp_conn_id=%s, ukgpro_conn_id=%s",
        parsed_config['integration_id'],
        parsed_config['customer_id'],
        parsed_config['vp_conn_id'],
        parsed_config['ukgpro_conn_id']
    )

    return parsed_config
