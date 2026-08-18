# Reverse Engineering — Run Metadata

- **Date:** 2026-06-24
- **Performed by:** Deltek AIDLC (automated reverse-engineering pass)
- **Scope:** Vantagepoint ↔ Xero migration slice only (source Workato `014-501 PSA`, target `airflow_mapping_framework`, and root-level migration/validation tooling). Other monorepo sub-projects not deeply analyzed.
- **Method:** Static analysis of source files and three parallel exploration passes (Xero integration, mapping framework, migration scripts). No code executed; no files modified.
- **Project type:** Brownfield.

## Artifacts produced
- business-overview.md
- architecture.md
- code-structure.md
- component-inventory.md
- technology-stack.md
- dependencies.md
- api-documentation.md
- code-quality-assessment.md
- reverse-engineering-timestamp.md (this file)

## Confidence / caveats
- Recipe internals (71 recipes) inventoried at folder/format level, not line-by-line.
- Data-presence findings for lookup tables based on sampled files; a full export check is recommended before migration.
- The `customer_id` schema mismatch in root scripts is inferred from the current `schema.sql` vs the script's INSERT columns; confirm which schema version the target DB actually runs.
