# Business Overview — Vantagepoint ↔ Xero Migration Slice

> **Scope note:** This reverse-engineering pass is scoped to the components relevant to *migration/data work for the Vantagepoint–Xero integration* (per AIDLC session goal). It covers three areas: the **source** (Workato Xero integration `014-501`), the **target** (Airflow Mapping Framework), and the **existing migration tooling**. Other monorepo sub-projects (QuickBooks 014-503, Talent, IntegrationPlatform, replicon-airflow-library) are referenced only where they intersect.

## Purpose

Deltek **Vantagepoint** (professional services automation / front office) integrates with **Xero** (accounting) so firms can run PSA in Vantagepoint while keeping their general ledger in Xero. The integration syncs firms (clients/vendors), chart of accounts, tax codes, employees, AP vouchers, invoices, cash receipts, employee expenses, journal entries, and units between the two systems.

This integration currently runs on **Workato** (recipe-based iPaaS). The broader initiative is migrating these integrations off Workato onto an **Airflow-based platform** with a shared, multi-tenant **Mapping Framework** that replaces Workato lookup tables.

## The Migration Problem

- The Xero integration `014-501 PSA` stores its mapping data in **Workato lookup tables** (firm map, employee map, chart of accounts, tax codes, currency codes, etc.).
- The new platform stores mappings in a **PostgreSQL multi-tenant schema** (`mapping_configs` + `mapping_tables`) consumed by Airflow operators.
- A migration path already exists and has been exercised for **QuickBooks (014-503)** via root-level CSV migration scripts. **Xero (014-501) has not yet been migrated** by those root scripts, though the framework's `workato_importer.py` declares `vantagepoint_xero` support.

## Business Value of the Migration

- Consolidates lookup/mapping data into one governed, multi-tenant store (per-customer + per-region isolation).
- Enables Airflow DAGs to resolve mappings at runtime with caching and validation.
- Removes dependence on Workato for mapping storage and management.

## Key Stakeholder Concepts

| Term | Meaning |
| --- | --- |
| `014-501 PSA` | The Workato package ID for the Vantagepoint–Xero integration |
| `014-503 PSA` | The Workato package ID for the Vantagepoint–QuickBooks integration (already migrated) |
| Lookup table | Workato's key/value mapping store (the data being migrated) |
| Mapping Framework | The Airflow/PostgreSQL target replacing lookup tables |
| Tenant / customer | A single Deltek customer instance; data is isolated per customer |
| Region | US / UK / CA regional variant of mapping data |
