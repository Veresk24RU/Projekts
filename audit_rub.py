import sqlite3, os
from ReadOT4ET import OUT_DIR, DB_PATH, export_rub_audit, print_rub_summary, connect_db

if not os.path.exists(DB_PATH):
    print('DB not found:', DB_PATH)
else:
    conn = connect_db()
    print_rub_summary(conn)
    audit_path = os.path.join(OUT_DIR, 'Audit_RUB.xlsx')
    export_rub_audit(conn, audit_path)
    print('Audit written:', audit_path)