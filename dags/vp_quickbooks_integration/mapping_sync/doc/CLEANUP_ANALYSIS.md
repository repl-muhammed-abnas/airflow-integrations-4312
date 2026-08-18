# File Cleanup Analysis for vp_quickbooks_integration

## Current Files in Directory

### Files to **KEEP** (Required for restructured implementation)
1. **`__init__.py`** ✅ - Package initialization file
2. **`utils/`** ✅ - Utility modules (mapping_utils.py)
3. **`lookup_tables/`** ✅ - Lookup table schema definitions
4. **`mapping/`** ✅ - Restructured mapping modules with main DAG
5. **`config.py`** ✅ - Configuration file (if used by other components)

### Files to **REMOVE** (Obsolete/Duplicate)
1. **`mapping_population_dag.py`** ❌ - OLD DAG file (replaced by mapping/mapping_population_dag.py)
2. **`mapping_population_dag.py_1`** ❌ - Backup file (no longer needed)
3. **`connection_trigger_dag.py`** ❌ - Old connection DAG (functionality integrated into main DAG)
4. **`validate_dependencies.py`** ❌ - Old validation script (replaced by mapping subfolder validation)

### Documentation Files to **EVALUATE**
1. **`README.md`** 📝 - Keep but may need updating for new structure
2. **`RESTRUCTURING_SUMMARY.md`** 📝 - Keep as historical reference

## Cleanup Actions Required

### Phase 1: Remove Obsolete DAG Files
- Remove old main DAG file (replaced by mapping subfolder version)
- Remove backup DAG file
- Remove old connection trigger DAG (functionality integrated)

### Phase 2: Remove Obsolete Utility Files
- Remove old validation script (replaced by mapping-specific validation)

### Phase 3: Update Documentation
- Update README.md to reflect new structure
- Keep restructuring summary for reference

## Impact Analysis

### Safe to Remove
- **Old DAG files**: Functionality moved to mapping subfolder
- **Backup files**: No longer needed after successful restructuring
- **Old validation scripts**: Replaced by location-specific validation

### Files Requiring Evaluation
- **config.py**: Check if used by other components before removal
- **connection_trigger_dag.py**: Verify functionality integrated into main DAG

## Post-Cleanup Expected Structure
```
vp_quickbooks_integration/
├── __init__.py                    # Package init
├── config.py                     # Configuration (if needed)
├── README.md                     # Updated documentation
├── RESTRUCTURING_SUMMARY.md      # Historical reference
├── utils/                        # Utility modules
│   ├── __init__.py
│   └── mapping_utils.py
├── lookup_tables/                # Table schemas
│   └── *.lookup_table.json
└── mapping/                      # Main implementation
    ├── __init__.py
    ├── mapping_population_dag.py  # Main DAG
    ├── core/
    ├── transaction_tracking/
    └── state_management/
```