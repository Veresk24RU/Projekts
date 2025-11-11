import argparse
import os
import sys
from collections import Counter
import sqlite3


def main():
    parser = argparse.ArgumentParser(description="Показать дубликаты внутри Excel-файла по ключу Event,Date,Symbol,Quantity,Price")
    parser.add_argument("--file", dest="file", required=False, help="Путь к .xlsx файлу. Если не задан, берётся последний в input_xlsx")
    parser.add_argument("--limit", dest="limit", type=int, default=50, help="Сколько групп дубликатов показать (по убыванию частоты)")
    parser.add_argument("--sheet", dest="sheet", default=None, help="Имя листа или индекс")
    args = parser.parse_args()

    base_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, base_dir)
    import Skript_ANALIZ as m  # noqa

    db_path = os.path.join(base_dir, "operations.db")
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(operations)")
    info = cur.fetchall()
    if not info:
        conn.close()
        print("Таблица operations не найдена. Сначала загрузите данные.")
        sys.exit(1)
    columns = [str(r[1]) for r in info]
    conn.close()

    if args.file:
        excel_path = os.path.abspath(args.file)
    else:
        in_dir = os.path.join(base_dir, "input_xlsx")
        files = [os.path.join(in_dir, n) for n in os.listdir(in_dir) if n.lower().endswith(".xlsx")]
        if not files:
            print("В папке input_xlsx нет .xlsx файлов")
            sys.exit(1)
        excel_path = sorted(files, key=os.path.getmtime)[-1]

    rows, skipped = m._load_excel_file_core(excel_path, args.sheet, columns)
    norm_rows = [m._normalize_row_values(r, columns) for r in rows]
    index = {c: i for i, c in enumerate(columns)}
    key_cols = ["Event", "Date", "Symbol", "Quantity", "Price"]
    missing = [c for c in key_cols if c not in index]
    if missing:
        print("В таблице отсутствуют столбцы ключа:", ", ".join(missing))
        sys.exit(1)
    keys = [tuple(row[index[c]] for c in key_cols) for row in norm_rows]
    cnt = Counter(keys)
    dups = [(k, cnt[k]) for k in cnt if cnt[k] > 1]
    print(f"Файл: {excel_path}")
    print(f"Всего строк (после фильтра мусора): {len(norm_rows)}; отфильтровано мусора: {skipped}")
    print(f"Групп дубликатов: {len(dups)}")
    for i, (k, c) in enumerate(sorted(dups, key=lambda x: -x[1])[: max(1, args.limit)]):
        print(f"{i+1:3d}. count={c} | key={k}")


if __name__ == "__main__":
    main()
