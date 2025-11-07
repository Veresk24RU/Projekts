import pandas as pd
import requests
import time
import os

# Получить путь текущей папки
current_dir = os.path.dirname(os.path.abspath(__file__))
file_path = os.path.join(current_dir, '11.txt')

# Прочитать имена облигаций из файла
with open(file_path, 'r', encoding='utf-8') as f:
    bond_names = [line.strip() for line in f if line.strip()]

search_url = 'https://iss.moex.com/iss/securities.json?q={query}&limit=10'

def get_isin_by_shortname(shortname):
    url = search_url.format(query=shortname)
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        columns = data['securities']['columns']
        isin_idx = columns.index('isin') if 'isin' in columns else None
        shortname_idx = columns.index('shortname') if 'shortname' in columns else None
        if isin_idx is not None and shortname_idx is not None:
            for row in data['securities']['data']:
                if row[shortname_idx] and shortname.lower() in row[shortname_idx].lower():
                    return row[isin_idx]
        return ''
    except Exception:
        return ''

results = []
for name in bond_names:
    isin = get_isin_by_shortname(name)
    results.append({'Короткое имя': name, 'ISIN': isin})
    print(f'Обработан: {name} -> ISIN: {isin}')
    time.sleep(0.25)

df = pd.DataFrame(results)
df.to_excel('isin_result.xlsx', index=False)

found_count = df['ISIN'].astype(bool).sum()
not_found_count = (~df['ISIN'].astype(bool)).sum()
print(f'Всего найдено ISIN: {found_count}, не найдено: {not_found_count}')
print('Результаты сохранены в isin_result.xlsx')
