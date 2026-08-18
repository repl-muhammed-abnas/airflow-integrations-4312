"""
Region-specific UDFs (User Defined Fields) and OEFs (Object Extension Fields) configuration.

This module provides O(1) lookup for:
- get_region_fields(region): Get UDFs/OEFs for a region and fields NOT applicable to that region
- get_excluded_fields(region): Get fields that should be excluded for a region (not applicable)

Usage:
    from user_import.common_utils.region_fields_config import get_region_fields, get_excluded_fields

    region_fields, excluded_fields = get_region_fields("global")
    # or directly:
    excluded = get_excluded_fields("global")
"""

from typing import Tuple, List, Dict, FrozenSet

# =============================================================================
# Region Constants
# =============================================================================

DXC_GLOBAL = "global"
DXC_AUSTRALIA = "australia"
DXC_CANADA = "canada"
DXC_COSTA_RICA = "costa_rica"
DXC_HUNGARY = "hungary"
DXC_INDIA = "india"
DXC_PHILIPPINES = "philippines"
DXC_PORTUGAL = "portugal"
DXC_UKI = "uki"
DXC_UKI_C1 = "uki_c1"
DXC_USA_CSC = "usa_csc"
DXC_USA_LES = "usa_les"

SUPPORTED_REGIONS: FrozenSet[str] = frozenset([
    DXC_GLOBAL, DXC_AUSTRALIA, DXC_CANADA, DXC_COSTA_RICA, DXC_HUNGARY,
    DXC_INDIA, DXC_PHILIPPINES, DXC_PORTUGAL, DXC_UKI, DXC_UKI_C1, DXC_USA_CSC, DXC_USA_LES,
])

# =============================================================================
# Field Configuration (Single Source of Truth)
# =============================================================================

_ALL = SUPPORTED_REGIONS
_GLOBAL_LIKE = frozenset({DXC_GLOBAL, DXC_CANADA, DXC_COSTA_RICA, DXC_INDIA, DXC_PORTUGAL, DXC_USA_CSC, DXC_USA_LES})
_HUNGARY_LIKE = frozenset({DXC_HUNGARY, DXC_PHILIPPINES, DXC_UKI, DXC_UKI_C1})

# UDF definitions: field_name -> frozenset of regions that use it
_UDF_CONFIG: Dict[str, FrozenSet[str]] = {
    # Common UDFs (all regions)
    "Gender": _ALL,
    "Continuous Service Date": _ALL,
    "On Leave": _ALL,
    "Job Activity Type": _ALL,
    "FTE": _ALL,
    "FTE %": _ALL,
    "International Assignee": _ALL,
    "International assignee start date": _ALL,
    "International assignee end date": _ALL,
    "PSA User": _ALL,
    "assignment_type": _ALL,

    # PERNER - now used by ALL regions (Hungary, Philippines, UKI added)
    "PERNER": _ALL,
    # Personnel Area fields - only Global-like regions + Australia
    "Personnel Area Code": _GLOBAL_LIKE | {DXC_AUSTRALIA} | {DXC_UKI_C1},
    "Personnel Area Description": _GLOBAL_LIKE | {DXC_AUSTRALIA} | {DXC_UKI_C1},
    "IA PERNER ID": _ALL,
    "Management Level": _ALL - {DXC_AUSTRALIA},

    # Hungary-like regions (Work Shift, Date of Birth, Time Type, Middle Name)
    "Work Shift": _HUNGARY_LIKE,
    "Date of Birth": _HUNGARY_LIKE | {DXC_AUSTRALIA},
    "Time Type": _HUNGARY_LIKE | {DXC_AUSTRALIA},
    "Middle Name": _HUNGARY_LIKE | {DXC_AUSTRALIA},

    # Australia-only UDFs
    "Annual Leave Anni. Date": frozenset({DXC_AUSTRALIA}),
    "LSL Anniversary Date": frozenset({DXC_AUSTRALIA}),
    "Personal Leave Anni. Date": frozenset({DXC_AUSTRALIA}),
    "Weekly Scheduled Hours": frozenset({DXC_AUSTRALIA}),
    "Employee Group": frozenset({DXC_AUSTRALIA}),
    "Employee Sub Group": frozenset({DXC_AUSTRALIA}),
    "Terms and Conditions": frozenset({DXC_AUSTRALIA}),
    "Termination Reason": frozenset({DXC_AUSTRALIA}),
    "Termination Reason Code": frozenset({DXC_AUSTRALIA}),
    "RUT": frozenset({DXC_AUSTRALIA}),

    # USA CSC-only UDFs
    "EE Group": frozenset({DXC_USA_CSC}),
}

# OEF definitions: field_name -> frozenset of regions that use it
_OEF_CONFIG: Dict[str, FrozenSet[str]] = {
    "Additional Job Classifications": frozenset({DXC_UKI}),
    "Employee Representative Status": frozenset({DXC_UKI}),
    "Employee Representative Effective Date": frozenset({DXC_UKI}),
}

# =============================================================================
# Pre-computed Lookups (O(1) access)
# =============================================================================

# All fields combined
_ALL_FIELDS: FrozenSet[str] = frozenset(_UDF_CONFIG.keys()) | frozenset(_OEF_CONFIG.keys())

# Pre-compute per-region data for O(1) lookup
_REGION_FIELDS: Dict[str, Tuple[str, ...]] = {}
_REGION_EXCLUDED: Dict[str, Tuple[str, ...]] = {}
_REGION_UDFS: Dict[str, Tuple[str, ...]] = {}
_REGION_OEFS: Dict[str, Tuple[str, ...]] = {}

for _region in SUPPORTED_REGIONS:
    # Fields applicable to this region
    _udfs = tuple(sorted(f for f, regions in _UDF_CONFIG.items() if _region in regions))
    _oefs = tuple(sorted(f for f, regions in _OEF_CONFIG.items() if _region in regions))
    _fields = tuple(sorted(set(_udfs) | set(_oefs)))

    # Fields NOT applicable to this region (excluded)
    _excluded = tuple(sorted(_ALL_FIELDS - set(_fields)))

    _REGION_UDFS[_region] = _udfs
    _REGION_OEFS[_region] = _oefs
    _REGION_FIELDS[_region] = _fields
    _REGION_EXCLUDED[_region] = _excluded

# Clean up loop variables
del _region, _udfs, _oefs, _fields, _excluded

# Public immutable versions
REGION_UDFS: Dict[str, Tuple[str, ...]] = _REGION_UDFS
REGION_OEFS: Dict[str, Tuple[str, ...]] = _REGION_OEFS
ALL_UDFS: Tuple[str, ...] = tuple(sorted(_UDF_CONFIG.keys()))
ALL_OEFS: Tuple[str, ...] = tuple(sorted(_OEF_CONFIG.keys()))

# =============================================================================
# Public API Functions (O(1) lookup)
# =============================================================================

def get_region_fields(region: str) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
    region_key = region.lower()

    if region_key not in SUPPORTED_REGIONS:
        raise ValueError(
            f"Unsupported region: '{region}'. "
            f"Supported regions are: {', '.join(sorted(SUPPORTED_REGIONS))}"
        )

    return _REGION_FIELDS[region_key], _REGION_EXCLUDED[region_key]


def get_excluded_fields(region: str) -> Tuple[str, ...]:
    region_key = region.lower()

    if region_key not in SUPPORTED_REGIONS:
        raise ValueError(
            f"Unsupported region: '{region}'. "
            f"Supported regions are: {', '.join(sorted(SUPPORTED_REGIONS))}"
        )

    return _REGION_EXCLUDED[region_key]


def get_region_udfs(region: str) -> Tuple[str, ...]:
    region_key = region.lower()
    if region_key not in SUPPORTED_REGIONS:
        raise ValueError(f"Unsupported region: '{region}'")
    return _REGION_UDFS[region_key]


def get_region_oefs(region: str) -> Tuple[str, ...]:
    region_key = region.lower()
    if region_key not in SUPPORTED_REGIONS:
        raise ValueError(f"Unsupported region: '{region}'")
    return _REGION_OEFS[region_key]


def is_valid_region(region: str) -> bool:
    return region.lower() in SUPPORTED_REGIONS


def list_all_regions() -> List[str]:
    return sorted(SUPPORTED_REGIONS)


def get_regions_with_oefs() -> List[str]:
    return [r for r in SUPPORTED_REGIONS if _REGION_OEFS[r]]
