"""Account cash helpers.

Cash is kept as a dated balance per ``(account, currency)``.  New manual and
DCA transactions adjust the latest balance for the transaction currency, while
imported history is treated as already reflected in imported balances.
"""
from .db import iso, now_iso, today_str


def transaction_cash_delta(tx_type, amount, fee=0.0):
    """Signed cash change in the transaction currency."""
    amount = float(amount or 0)
    fee = float(fee or 0)
    if tx_type == 'buy':
        return -(amount + fee)
    if tx_type in ('sell', 'dividend', 'interest'):
        return amount - fee
    if tx_type == 'fee':
        return -amount
    if tx_type == 'reinvest':
        return -fee
    return 0.0


def adjust_account_cash(conn, account_id, currency, delta, on_date, source='transaction'):
    """Apply a signed change to the latest cash row for account/currency."""
    if not account_id or abs(delta) < 1e-12:
        return
    currency = (currency or 'CNY').upper()
    tx_date = iso(on_date or today_str())[:10]
    latest = conn.execute(
        """SELECT id, balance_date, amount FROM wm_account_balance
           WHERE account_id = ? AND UPPER(COALESCE(currency, 'CNY')) = ?
           ORDER BY balance_date DESC, id DESC LIMIT 1""",
        (account_id, currency),
    ).fetchone()
    if latest:
        balance_date = max(str(latest['balance_date'])[:10], tx_date)
        conn.execute(
            """UPDATE wm_account_balance
               SET amount = ?, balance_date = ?, source = ?, created_at = ?
               WHERE id = ?""",
            (float(latest['amount'] or 0) + delta, balance_date, source,
             now_iso(), latest['id']),
        )
    else:
        conn.execute(
            """INSERT INTO wm_account_balance
               (account_id, balance_date, amount, currency, source, created_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (account_id, tx_date, delta, currency, source, now_iso()),
        )


def apply_transaction_cash(conn, tx):
    """Apply a newly recorded transaction's cash impact."""
    tx = dict(tx)
    if not tx.get('account_id'):
        return
    delta = transaction_cash_delta(tx.get('type'), tx.get('amount'), tx.get('fee'))
    if abs(delta) < 1e-12:
        return
    adjust_account_cash(conn, tx['account_id'], tx.get('currency'), delta,
                        tx.get('trade_date'))


def reverse_transaction_cash(conn, tx):
    """Undo a transaction's previously applied cash impact."""
    tx = dict(tx)
    if not tx.get('account_id'):
        return
    delta = transaction_cash_delta(tx.get('type'), tx.get('amount'), tx.get('fee'))
    if abs(delta) < 1e-12:
        return
    adjust_account_cash(conn, tx['account_id'], tx.get('currency'), -delta,
                        tx.get('trade_date'))
