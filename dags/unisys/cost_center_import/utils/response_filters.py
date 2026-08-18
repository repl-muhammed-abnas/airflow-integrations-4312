"""
Response Filters Module for Unisys Cost Center Import Integration

This module contains functions to process and transform responses from Replicon API calls,
specifically for the GetHierarchyData service that retrieves division information.

Functions:
    extract_hierarchy_data: Extract cost center data from GetHierarchyData response
    parse_division_row: Parse individual row from hierarchy response
    has_more_pages: Check if more pages are available in paginated response

Design Reference:
    Based on cost_center_design.txt API response structure for GetHierarchyData
"""
import json
def extract_hierarchy_data(response):
    """
    Extract cost center data from GetHierarchyData service response.

    Design Reference (from cost_center_design.txt - Step 10):
        Data handler logic for GetHierarchyData response:
        - First array element (cells[0]) contains hierarchy codes
        - Second array element (cells[1]) contains hierarchy names
        - Third element (cells[2]) contains enabled status
        - hierarchyLevel: 0 = Company, 1 = Cost Center

    Response Structure:
        {
          "d": {
            "header": [...],
            "rows": [
              {
                "cells": [
                  {"cellCollection": [{"textValue": "101"}, {"textValue": "1005"}]},  # codes
                  {"cellCollection": [{"textValue": "UNISYS"}, {"textValue": "Sales"}]},  # names
                  {"boolValue": true}  # status
                ],
                "hierarchyLevel": 1
              }
            ]
          }
        }

    Args:
        response (dict): Response from GetHierarchyData service

    Returns:
        list: List of cost center records with structure:
            {
                'company': str,
                'company_name': str,
                'cost_center': str,
                'cost_center_name': str,
                'status': str
            }

    Example:
        >>> data = extract_hierarchy_data(api_response)
        >>> # Returns list of cost center dictionaries
    """
    extracted_data = []

    rows = response

    for row in rows:
        # Only process hierarchy level 1 (cost centers under companies)
        # Level 0 is company, Level 1 is cost center
        hierarchy_level = row.get("hierarchyLevel", 0)
        if hierarchy_level == 0:
            # Extract company-level data (parent divisions)
            cells = row.get("cells", [])
            if len(cells) >= 3:
                company_code = cells[0].get("cellCollection", [])[0].get("textValue", "").strip()
                company_name = cells[1].get("cellCollection", [])[0].get("textValue", "").strip()
                is_enabled = cells[2].get("boolValue", False)
                status = "enabled" if is_enabled else "disabled"

                if company_code:
                    extracted_data.append({
                        "company": company_code,
                        "company_name": company_name,
                        "cost_center": "",
                        "cost_center_name": "",
                        "status": status,
                    })
        elif hierarchy_level == 1:
            # Extract cost center-level data (child divisions)
            parsed_row = parse_division_row(row)
            if parsed_row:
                extracted_data.append(parsed_row)
    return json.dumps(extracted_data)


def parse_division_row(row):
    """
    Parse a single row from GetHierarchyData response into cost center record.

    Design Reference (from cost_center_design.txt):
        cells[0].cellCollection:
            - First element: Company code
            - Second element: Cost center code
        cells[1].cellCollection:
            - First element: Company name
            - Second element: Cost center name
        cells[2].boolValue: Effectively enabled status

    Args:
        row (dict): Single row from GetHierarchyData response

    Returns:
        dict: Parsed cost center record or None if invalid

    Example:
        >>> row_data = {...}  # Row from API response
        >>> parsed = parse_division_row(row_data)
        >>> # Returns: {'company': '101', 'cost_center': '1005', ...}
    """
    try:
        cells = row.get("cells", [])

        if len(cells) < 3:
            return None

        # Extract codes from first cell
        code_collection = cells[0].get("cellCollection", [])
        if len(code_collection) < 2:
            return None

        company_code = code_collection[0].get("textValue", "").strip()
        cost_center_code = code_collection[1].get("textValue", "").strip()

        # Extract names from second cell
        name_collection = cells[1].get("cellCollection", [])
        if len(name_collection) < 2:
            return None

        company_name = name_collection[0].get("textValue", "").strip()
        cost_center_name = name_collection[1].get("textValue", "").strip()

        # Extract status from third cell
        is_enabled = cells[2].get("boolValue", False)
        status = "enabled" if is_enabled else "disabled"

        # Skip if any required field is empty (including cost center name)
        if not company_code or not company_name:
            return None

        return {
            "company": company_code,
            "company_name": company_name,
            "cost_center": cost_center_code,
            "cost_center_name": cost_center_name,
            "status": status,
        }

    except (KeyError, IndexError, AttributeError):
        # Log error but don't fail - skip invalid rows
        return None
    