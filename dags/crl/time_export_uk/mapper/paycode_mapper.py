"""
UK Pay Code Mapper for PTA Export
Maps Replicon Time Type (UK) dropdown values to SAP ECC pay codes

Note: Only OT (Overtime) codes are mapped here.
All other entries are time-offs and will derive pay_type from timeoff_type_description.
If no time type is selected, default is '0'.
"""

UK_PAY_CODE_MAPPER = [
    {"time_type_code": "[UK] OT 1.0", "sap_time_code": "0802"},
    {"time_type_code": "[UK] OT 1.5", "sap_time_code": "0802"},
    {"time_type_code": "[UK] OT 2.0", "sap_time_code": "0802"},
    {"time_type_code": "[UK] OT 3.0", "sap_time_code": "0802"},
    {"time_type_code": "[UK] Regular Time", "sap_time_code": "0800"}
]