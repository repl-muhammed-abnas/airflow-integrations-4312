def build_flat_code(phase_code, category_code, cost_type):
    phase_code = (phase_code or '').strip()
    category_code = (category_code or '').strip()
    cost_type = (cost_type or '').strip()
    parts = []
    if phase_code:
        parts.append(phase_code)
    if category_code:
        parts.append(category_code)
    cost_code = '-'.join(parts) if parts else ''
    return f"{cost_code}.{cost_type}" if cost_code and cost_type else ''
