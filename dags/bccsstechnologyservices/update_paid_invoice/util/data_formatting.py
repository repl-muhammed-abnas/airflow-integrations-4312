def check_put_success(response, invoice_uri):
    res = response.json()["d"]
    if "invoiceReference" in res and "uri" in res['invoiceReference']:
        if res['invoiceReference']["uri"] == invoice_uri:
            return True
    return False

def check_payment_date(payment_date):
    if not payment_date or payment_date == "" or "/" not in payment_date:
        return False
    return True
