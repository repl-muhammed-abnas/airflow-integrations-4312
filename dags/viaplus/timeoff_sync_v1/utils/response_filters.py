"""
Response filter utilities for processing Keka and Replicon API responses.
"""
import logging
from datetime import datetime
from typing import Dict, Optional
import rail

logger = logging.getLogger(__name__)


def get_existing_booking_details(response, dag_run):
    """Extract existing Replicon booking details by Keka Booking ID."""
    keka_booking_id = str(dag_run.conf["booking_data"]["id"])
    return list(filter(lambda x: x['keka_booking_id'] == keka_booking_id, map(lambda row: {
        "timeoff_uri": row['cells'][0]['uri'],
        "timeoff_type": row['cells'][1]['textValue'],
        "keka_booking_id": row['cells'][2]['textValue'],
        "start_date": str(row['cells'][3]['dateValue']['year']) + '-' + str(row['cells'][3]['dateValue']['month']).zfill(2) + '-' + str(row['cells'][3]['dateValue']['day']).zfill(2),
        "end_date": str(row['cells'][4]['dateValue']['year']) + '-' + str(row['cells'][4]['dateValue']['month']).zfill(2) + '-' + str(row['cells'][4]['dateValue']['day']).zfill(2),
    }, response['rows'])))


def filter_keka_timeoff_data(config):
    """
    Filter and categorize Keka time-off data.
    Handles numeric status codes from Keka API.
    
    Keka Status Codes:
    - 0 = Pending (IGNORE)
    - 1 = Approved (SYNC TO REPLICON - CREATE/UPDATE)
    - 2 = Rejected (DELETE FROM REPLICON)
    - 3 = Cancelled (DELETE FROM REPLICON)
    
    Args:
        config: Instance configuration
    
    Returns:
        Dictionary with 'created_updated' and 'cancelled' lists
    """
    keka_response = rail.result("get_timeoff_bookings_from_keka")
    logging_details = rail.result("logging_details")
    
    all_leaves = keka_response.get('all_leaves', [])
    
    if not all_leaves:
        logger.info("No leave requests found in Keka response")
        return {
            "created_updated": [],
            "cancelled": []
        }
    
    cutoff_time = logging_details.get('cutoff_time_obj')
    
    created_updated = []
    cancelled = []
    
    for leave in all_leaves:
        status_code = leave.get('status')
        last_modified = leave.get('lastActionTakenOn', '')
        
        from viaplus.timeoff_sync_v1.utils.custom_methods import parse_keka_datetime
        modified_dt = parse_keka_datetime(last_modified)
        
        # Filter by cutoff time (only process recent changes)
        if cutoff_time and modified_dt and modified_dt < cutoff_time:
            logger.debug(f"Skipping leave {leave.get('id')} - modified before cutoff")
            continue
        
        # Extract leave type name from selection array
        selection = leave.get('selection', [])
        if selection and len(selection) > 0:
            leave['leaveTypeName'] = selection[0].get('leaveTypeName', '')
            leave['leaveTypeIdentifier'] = selection[0].get('leaveTypeIdentifier', '')
        
        # Categorize by status code
        if status_code == 1:  # Approved
            created_updated.append(leave)
            logger.debug(f"Added to created_updated: {leave.get('id')}")
        elif status_code in [2, 3]:  # Rejected or Cancelled
            cancelled.append(leave)
            logger.debug(f"Added to cancelled: {leave.get('id')}")
        else:  # Pending
            logger.debug(f"Ignoring pending leave: {leave.get('id')}")
    
    logger.info(f"Filtered {len(all_leaves)} total leaves:")
    logger.info(f"  - {len(created_updated)} approved leaves to create/update")
    logger.info(f"  - {len(cancelled)} rejected/cancelled leaves to delete")
    
    return {
        "created_updated": created_updated,
        "cancelled": cancelled
    }


def get_booking_id_oef_value(response):
    """Extract Keka Booking ID OEF slug from response."""
    return list(filter(lambda x: x['booking_id_oef_name'] == 'Keka Booking ID', map(lambda row: {
        "booking_id_oef_name": row['cells'][0]['textValue'],
        "booking_id_oef_value": (row['cells'][1]['uri']).split(':')[-1],
    }, response['rows'])))[0]