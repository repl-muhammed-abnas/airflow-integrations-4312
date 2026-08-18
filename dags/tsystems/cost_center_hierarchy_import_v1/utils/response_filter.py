"""
Response filters for API responses in the T-Systems Cost Center Hierarchy Import integration.
"""

from typing import Dict, List, Any, Optional

# Constants for header URIs
HEADER_MAPPINGS = {
    'URI': 'urn:replicon:department-group-list-column:department-group',
    'FullPath': 'urn:replicon:department-group-list-column:full-path',
    'FullPathCode': 'urn:replicon:department-group-list-column:full-path-code',
    'Enabled': 'urn:replicon:department-group-list-column:effectively-enabled',
    'Description': 'urn:replicon:department-group-list-column:description',
    'Name': 'urn:replicon:department-group-list-column:name',
    'Code': 'urn:replicon:department-group-list-column:code'
}


def map_departments(response: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Maps the response from GetData API for departments to a standardized format.
    
    Args:
        response: The raw API response containing department data
        
    Returns:
        List of department dictionaries with standardized fields:
        - URI: Department unique identifier
        - FullPath: Hierarchical path of department names
        - FullPathCode: Hierarchical path of department codes
        - Enabled: Boolean indicating if department is active
        - Description: Department description
        - Name: Department name
        - Code: Department code
    """
    if not response or 'rows' not in response:
        return []
    
    # Build header index mapping
    header_index_map = _build_header_map(response.get('header', []))
    
    # Process each row
    departments = []
    for row in response.get('rows', []):
        department = _process_row(row, header_index_map)
        if department:  # Only add non-empty departments
            departments.append(department)
    
    return departments


def _build_header_map(headers: List[Dict[str, str]]) -> Dict[str, int]:
    """
    Creates a mapping of header URIs to their column indices.
    
    Args:
        headers: List of header definitions from API response
        
    Returns:
        Dictionary mapping header URIs to column indices
    """
    return {header.get('uri', ''): idx for idx, header in enumerate(headers)}


def _process_row(row: Dict[str, Any], header_map: Dict[str, int]) -> Optional[Dict[str, Any]]:
    """
    Processes a single row of department data.
    
    Args:
        row: Single row from the API response
        header_map: Mapping of header URIs to column indices
        
    Returns:
        Department dictionary or None if row is invalid
    """
    cells = row.get('cells', [])
    
    # Validate row has sufficient cells
    if not cells:
        return None
    
    department = {}
    
    # Define field types for extraction
    field_types = {
        'URI': 'uri',
        'FullPath': 'collection',
        'FullPathCode': 'collection',
        'Enabled': 'boolean',
        'Description': 'text',
        'Name': 'text',
        'Code': 'text'
    }
    
    for field_name, header_uri in HEADER_MAPPINGS.items():
        col_idx = header_map.get(header_uri)
        if col_idx is not None and col_idx < len(cells):
            cell = cells[col_idx]
            field_type = field_types.get(field_name, 'text')
            
            # Handle collection fields separately
            if field_type == 'collection':
                value = _extract_collection_path(cell)
            else:
                value = _extract_cell_value(cell, field_type)
            
            if value is not None:  # Only add non-None values
                department[field_name] = value
    
    return department if department else None


def _extract_cell_value(cell: Dict[str, Any], field_type: str) -> Optional[Any]:
    """
    Extracts value from a cell based on the field type.
    
    Args:
        cell: Cell data from API response
        field_type: Type of field to extract ('uri', 'text', 'boolean')
        
    Returns:
        Extracted value or None if not found
    """
    if field_type == 'uri':
        return cell.get('uri')
    elif field_type == 'boolean':
        return cell.get('boolValue') if 'boolValue' in cell else None
    else:  # Default to text extraction
        return cell.get('textValue', '').strip() if 'textValue' in cell else None


def _extract_collection_path(cell: Dict[str, Any]) -> Optional[str]:
    """
    Extracts and joins collection values into a pipe-delimited path.
    
    Args:
        cell: Cell containing collection data
        
    Returns:
        Pipe-delimited string of collection values or None
    """
    if 'cellCollection' not in cell:
        return None
    
    collection = cell.get('cellCollection', [])
    if not collection:
        return None
    
    # Extract text values from collection items
    path_items = [
        item.get('textValue', '').strip() 
        for item in collection 
        if item.get('textValue', '').strip()
    ]
    
    return '|'.join(path_items) if path_items else None


def combine_and_map_departments(results):
    """
    Processes and combines results from all pages of department data.
    
    Args:
        results: List of API responses from each page
        
    Returns:
        Combined list of mapped department dictionaries
    """
    all_departments = []
    
    # Process each page of results
    for page_response in results:
        departments = map_departments(page_response)
        all_departments.extend(departments)
    
    return all_departments

def map_permission_sets(response):
    """
    Maps the permission sets response to a more usable format.
    
    Args:
        response: The raw API response
        
    Returns:
        Dictionary of permission sets by name
    """
    permissions = {}
    
    for permission_set in response:
        name = permission_set.get('name', '')
        if name:
            permissions[name] = {
                'uri': permission_set.get('uri', ''),
                'name': name,
                'displayText': permission_set.get('displayText', '')
            }
    
    return permissions

def format_logs(logs):
    """
    Format logs for output report.
    
    Args:
        logs: List of log entries
        
    Returns:
        Dictionary containing formatted logs and summary information
    """
    total_count = len(logs) if logs else 0
    success_count = 0
    error_count = 0
    exception_count = 0
    skipped_count = 0
    
    formatted_logs = []
    
    for log in logs:
        props = log.get('properties', {})
        status = props.get('status', '').lower()
        
        if status == 'success':
            success_count += 1
        elif status == 'error':
            error_count += 1
        elif status == 'exception':
            exception_count += 1
        elif status == 'skipped' or status == 'unchanged':
            skipped_count += 1
            
        formatted_logs.append({
            'code': props.get('code', ''),
            'name': props.get('name', ''),
            'description': props.get('description', ''),
            'status': props.get('status', ''),
            'action': props.get('action', ''),
            'details': props.get('details', ''),
            'manager_id': props.get('manager_id', '')
        })
    
    return {
        'formatted_logs': formatted_logs,
        'total_count': total_count,
        'success_count': success_count,
        'error_count': error_count,
        'exception_count': exception_count,
        'skipped_count': skipped_count
    }