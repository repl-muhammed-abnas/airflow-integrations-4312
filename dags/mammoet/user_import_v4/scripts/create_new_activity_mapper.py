#!/usr/bin/env python3
"""
Excel/CSV to JSON Converter
============================
Convert Excel or CSV files to formatted JSON with preserved data types.

Requirements:
    pip install pandas openpyxl

Usage Examples:
    # As a module
    from excel_csv_to_json import convert_to_json
    convert_to_json('data.csv', 'output.json')
    
    # Command line
    python excel_csv_to_json.py data.csv
    python excel_csv_to_json.py data.xlsx output.json
    
    # Quick inline usage
    python excel_csv_to_json.py
"""

import pandas as pd
import json
import sys
import os
from pathlib import Path


def convert_to_json(input_file, output_file=None, preserve_leading_zeros=True, verbose=True):
    """
    Convert Excel or CSV file to formatted JSON.
    
    Args:
        input_file (str): Path to input Excel (.xlsx, .xls) or CSV file
        output_file (str, optional): Path to output JSON file. If None, uses input filename with .json extension
        preserve_leading_zeros (bool): If True, preserves leading zeros in code fields (default: True)
        verbose (bool): If True, prints conversion details (default: True)
    
    Returns:
        list: List of dictionaries representing the data
        
    Example:
        >>> data = convert_to_json('data.csv', 'output.json')
        >>> print(f"Converted {len(data)} records")
    """
    
    # Validate input file exists
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    # Get file extension
    file_ext = Path(input_file).suffix.lower()
    
    # Configure dtype to preserve leading zeros in code columns
    dtype_config = None
    if preserve_leading_zeros:
        dtype_config = {
            'activity_type_code': str,
            'cost_center_code': str
        }
    
    # Read file based on extension
    try:
        if file_ext in ['.xlsx', '.xls']:
            df = pd.read_excel(input_file, dtype=dtype_config)
            if verbose:
                print(f"✓ Successfully read Excel file: {input_file}")
        elif file_ext == '.csv':
            df = pd.read_csv(input_file, dtype=dtype_config)
            if verbose:
                print(f"✓ Successfully read CSV file: {input_file}")
        else:
            raise ValueError(f"Unsupported file format: {file_ext}. Please use .xlsx, .xls, or .csv")
    except Exception as e:
        raise Exception(f"Error reading file: {str(e)}")
    
    # Display DataFrame info
    if verbose:
        print(f"  Rows: {df.shape[0]}, Columns: {df.shape[1]}")
        print(f"  Column names: {', '.join(df.columns)}")
    
    # Convert DataFrame to list of dictionaries
    data_list = df.to_dict(orient='records')
    
    # Determine output filename
    if output_file is None:
        output_file = Path(input_file).stem + '.json'
    
    # Write to JSON file with formatting
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data_list, f, indent=4, ensure_ascii=False)
    
    if verbose:
        print(f"✓ JSON output saved to: {output_file}")
        print(f"✓ Total records converted: {len(data_list)}")
    
    return data_list


def quick_convert(input_file):
    """
    Quick converter function - minimal parameters.
    
    Args:
        input_file (str): Path to CSV or Excel file
        
    Returns:
        str: Path to the created JSON file
        
    Example:
        >>> output = quick_convert('my_data.csv')
        >>> print(f"Created: {output}")
    """
    output_file = Path(input_file).stem + '.json'
    convert_to_json(input_file, output_file)
    return output_file


def batch_convert(input_files, output_dir=None):
    """
    Convert multiple files at once.
    
    Args:
        input_files (list): List of input file paths
        output_dir (str, optional): Directory for output files. If None, uses same directory as input
        
    Returns:
        list: List of output file paths
        
    Example:
        >>> outputs = batch_convert(['data1.csv', 'data2.xlsx'])
    """
    results = []
    for input_file in input_files:
        if output_dir:
            output_file = os.path.join(output_dir, Path(input_file).stem + '.json')
        else:
            output_file = Path(input_file).stem + '.json'
        
        try:
            convert_to_json(input_file, output_file)
            results.append(output_file)
        except Exception as e:
            print(f"✗ Error converting {input_file}: {str(e)}")
    
    return results


def dataframe_to_json_string(df, preserve_leading_zeros=True):
    """
    Convert a pandas DataFrame to a formatted JSON string.
    
    Args:
        df (pd.DataFrame): Input DataFrame
        preserve_leading_zeros (bool): If True, converts code columns to strings
        
    Returns:
        str: Formatted JSON string
        
    Example:
        >>> import pandas as pd
        >>> df = pd.read_csv('data.csv')
        >>> json_str = dataframe_to_json_string(df)
        >>> print(json_str)
    """
    if preserve_leading_zeros:
        # Convert code columns to strings if they exist
        for col in df.columns:
            if 'code' in col.lower():
                df[col] = df[col].astype(str)
    
    data_list = df.to_dict(orient='records')
    return json.dumps(data_list, indent=4, ensure_ascii=False)


def main():
    """Main function for command-line usage."""
    
    if len(sys.argv) < 2:
        print("=" * 60)
        print("Excel/CSV to JSON Converter")
        print("=" * 60)
        print("\nUsage:")
        print("  python excel_csv_to_json.py <input_file> [output_file]")
        print("\nExamples:")
        print("  python excel_csv_to_json.py data.csv")
        print("  python excel_csv_to_json.py data.xlsx output.json")
        print("  python excel_csv_to_json.py data.csv my_output.json")
        print("\nSupported formats:")
        print("  - CSV (.csv)")
        print("  - Excel (.xlsx, .xls)")
        print("\nFeatures:")
        print("  ✓ Preserves leading zeros in code fields")
        print("  ✓ UTF-8 encoding support")
        print("  ✓ Formatted JSON output (4-space indent)")
        print("=" * 60)
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    try:
        print("\n" + "=" * 60)
        convert_to_json(input_file, output_file)
        print("=" * 60)
        print("✓ Conversion completed successfully!\n")
    except Exception as e:
        print("\n" + "=" * 60)
        print(f"✗ Error: {str(e)}")
        print("=" * 60 + "\n")
        sys.exit(1)


# ============================================================================
# QUICK USAGE EXAMPLES (for copy-paste)
# ============================================================================

def example_usage():
    """
    Example usage patterns - uncomment and modify as needed.
    """
    
    # Example 1: Simple conversion
    # convert_to_json('data.csv', 'output.json')
    
    # Example 2: Quick convert (auto-generates output filename)
    # quick_convert('data.csv')
    
    # Example 3: Convert Excel file
    # convert_to_json('data.xlsx', 'output.json')
    
    # Example 4: Batch conversion
    # batch_convert(['file1.csv', 'file2.xlsx', 'file3.csv'])
    
    # Example 5: Direct DataFrame usage
    # import pandas as pd
    # df = pd.read_csv('data.csv', dtype={'activity_type_code': str})
    # json_str = dataframe_to_json_string(df)
    # print(json_str)
    
    # Example 6: Inline conversion (no function call)
    # import pandas as pd
    # import json
    # df = pd.read_csv('data.csv', dtype={'activity_type_code': str, 'cost_center_code': str})
    # with open('output.json', 'w', encoding='utf-8') as f:
    #     json.dump(df.to_dict(orient='records'), f, indent=4, ensure_ascii=False)
    
    pass


if __name__ == "__main__":
    main()