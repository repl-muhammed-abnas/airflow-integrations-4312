from airflow.models import Variable


def get_tenant_email(config):
    """Resolve tenant notification email from instance config, Airflow Variable, or internal_email.

    Resolution order:
    1. config.tenant_email — hand-authored instance files; existing deployments unchanged.
    2. Airflow Variable ``ce_procore_{customer_id}_{deployment_slug}_customer_email`` —
       set by the self-service UI when the user provides an email address.
    3. config.internal_email — last-resort fallback so the internal team is always notified.
    4. Empty list
    """
    email = getattr(config, 'tenant_email', None)
    if email:
        return [email] if isinstance(email, str) else email
    customer = getattr(config, 'customer_id', None)
    deployment = getattr(config, 'deployment_slug', None)
    if customer and deployment:
        val = Variable.get(f"ce_procore_{customer}_{deployment}_customer_email", default_var=None)
        if val:
            return [addr.strip() for addr in val.split(',') if addr.strip()]
    internal = getattr(config, 'internal_email', None)
    if internal:
        return [internal] if isinstance(internal, str) else internal
    return []


def normalize_ce_identifier(value):
    """Uppercase an identifier for case-insensitive matching against ComputerEase.

    ComputerEase canonicalizes unique identifiers (phase/category/cost codes,
    job/vendor codes, cost-type references, etc.) to UPPERCASE and matches them
    case-sensitively. Procore preserves the original casing, so any Procore-derived
    value used to look up, filter, or compare against a CE record must be normalized
    through this helper first - otherwise a casing difference yields "not found when
    it exists" and duplicate creation. Returns the value unchanged when falsy.
    """
    if not value:
        return value
    return str(value).upper()
