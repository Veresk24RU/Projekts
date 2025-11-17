from pathlib import Path
text = Path('ReadOT4ET.py').read_text(encoding='utf-8')
start = text.index('def validate_opening_balances')
end = text.index('\ndef move_processed')
new_block = '''def validate_opening_balances(conn: sqlite3.Connection, ws, portfolio: str) -> None:
    if not OPENING_BALANCE_CHECK_ENABLED:
        return
    reported = parse_opening_balances_v2(ws)
    has_rows = conn.execute(
        f"SELECT 1 FROM {TABLE_NAME} WHERE Portfolio=? LIMIT 1", (portfolio,)
    ).fetchone() is not None
    if not has_rows:
        for code in ("RUB", "CNY", "USD"):
            val = reported.get(code, Decimal("0"))
            if val != Decimal("0"):
                raise RuntimeError("Входящий остатот денежных средств не совпадает.")
        return
    expected = {"RUB": Decimal("0"), "CNY": Decimal("0"), "USD": Decimal("0")}
    for code in ("RUB", "CNY", "USD"):
        rep = reported.get(code)
        exp = expected.get(code, Decimal("0"))
        if rep is None:
            if exp != Decimal("0"):
                raise RuntimeError("Входящий остатот денежных средств не совпадает.")
        elif rep != exp:
            raise RuntimeError("Входящий остатот денежных средств не совпадает.")

'''
Path('ReadOT4ET.py').write_text(text[:start] + new_block + text[end:], encoding='utf-8')
