"""
Germany Pay Code Mapper for Time Export
Maps Replicon Time Type (Germany) OEF dropdown values to SAP ECC attendance codes.

Source: CRL - Germany - Time Data Extract v1.0 26062026.xlsx

Note: Per spec (Palwasha Ahmed, Apr 2026), the Time Type (Germany) OEF field remains
BLANK for all Germany employees — this mapper is not exercised at runtime. All worked-time
entries fall through to the default '0800'. Absence/time-off entries derive pay_type
from the Time Off Type Description field in Replicon.

Unmapped attendance types (SAP code TBD by CRL — will default to 800 until updated):
  [DEU] Ordered Overtime
  [DEU] Unordered Overtime
  [DEU] OT Allowance 25%
  [DEU] Night Allowance
  [DEU] Saturday Allowance
  [DEU] Sunday Allowance
  [DEU] Regular Public Holiday Allowance
  [DEU] Higher Public Holiday Allowance
"""

GERMANY_PAY_CODE_MAPPER = [
    {"time_type_code": "[DEU] Regular Hours",                              "sap_time_code": "800"},
    {"time_type_code": "[DEU] TOIL",                                       "sap_time_code": "803"},
    {"time_type_code": "[DEU] Auszahlung von Zeitguthaben/TOIL (Paid Time)", "sap_time_code": "802"},
]
