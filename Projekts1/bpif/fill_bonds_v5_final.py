"""
Скрипт для заполнения данных об облигациях из MOEX ISS API
ВЕРСИЯ 5 - ФИНАЛЬНАЯ - исправлено получение emitent_title
emitent_title находится в securities секции, а не в отдельной description
"""

import pandas as pd
import requests
from datetime import datetime
from pathlib import Path
import time
import warnings

warnings.filterwarnings('ignore', category=pd.errors.SettingWithCopyWarning)

# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================

INPUT_FILE = Path(r"C:\Users\veres\OneDrive\Рабочий стол\Projekts\bpif\BPIF.xlsx")
OUTPUT_FILE = Path(r"C:\Users\veres\OneDrive\Рабочий стол\Projekts\bpif\BPIF_filled.xlsx")

MOEX_ISS_BASE = "https://iss.moex.com/iss"
REQUEST_TIMEOUT = 10

# ============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# ============================================================================

def get_emitent_title(isin: str) -> str:
    """
    Получает имя эмитента через /iss/securities.json endpoint
    emitent_title находится в securities колонках, а не в отдельной description секции
    
    Args:
        isin: ISIN код облигации
    
    Returns:
        Имя эмитента или пустая строка
    """
    try:
        url = f"{MOEX_ISS_BASE}/securities.json"
        params = {
            "q": isin,
            "iss.meta": "off"
        }
        
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        # emitent_title находится в securities колонках
        if "securities" in data and data["securities"]["data"]:
            sec_columns = data["securities"]["columns"]
            sec_row = data["securities"]["data"][0]
            
            # Ищем индекс колонки emitent_title
            if "emitent_title" in sec_columns:
                idx = sec_columns.index("emitent_title")
                emitent = sec_row[idx]
                return str(emitent).strip() if emitent else ""
        
        return ""
    except:
        return ""


def get_security_info_by_isin(isin: str) -> dict:
    """
    Получает информацию об облигации по ISIN из MOEX ISS API
    
    Args:
        isin: ISIN код облигации
    
    Returns:
        Словарь с информацией об облигации
    """
    try:
        url = f"{MOEX_ISS_BASE}/engines/stock/markets/bonds/securities/{isin}.json"
        params = {
            "iss.meta": "off",
            "iss.only": "securities,marketdata"
        }
        
        response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()
        
        result = {}
        
        # Извлекаем данные из securities
        if data.get("securities", {}).get("data"):
            sec_columns = data["securities"]["columns"]
            sec_row = data["securities"]["data"][0]
            
            for i, col in enumerate(sec_columns):
                if i < len(sec_row):
                    result[col] = sec_row[i]
        
        # Извлекаем данные из marketdata (особенно YIELD для YTM)
        if data.get("marketdata", {}).get("data"):
            market_columns = data["marketdata"]["columns"]
            market_row = data["marketdata"]["data"][0]
            
            for i, col in enumerate(market_columns):
                if i < len(market_row):
                    # Добавляем YIELD и другие важные поля
                    if col in ["YIELD", "YIELDTOOFFER", "YIELDATWAPRICE"]:
                        result[col] = market_row[i]
        
        return result
    except:
        return {}


def parse_date(date_str) -> str:
    """Преобразует дату из YYYY-MM-DD в DD.MM.YYYY"""
    if not date_str:
        return ""
    
    try:
        if isinstance(date_str, datetime):
            return date_str.strftime("%d.%m.%Y")
        
        date_str = str(date_str).strip()
        if not date_str or date_str.lower() in ['nan', 'none', '', '0000-00-00']:
            return ""
        
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return date_obj.strftime("%d.%m.%Y")
    except:
        return ""


def get_coupon_frequency(next_coupon_date: str) -> int:
    """Определяет частоту купона на основе даты следующего купона"""
    if not next_coupon_date:
        return 0
    
    try:
        next_coupon = datetime.strptime(str(next_coupon_date).strip(), "%Y-%m-%d")
        today = datetime.now()
        
        days_until_coupon = (next_coupon - today).days
        
        if days_until_coupon > 365 or days_until_coupon < 0:
            return 0
        
        if 5 <= days_until_coupon <= 35:
            return 12
        elif 80 <= days_until_coupon <= 100:
            return 4
        elif 150 <= days_until_coupon <= 190:
            return 2
        elif 25 <= days_until_coupon <= 60:
            return 4
        elif 100 <= days_until_coupon <= 150:
            return 2
        else:
            return 0
    except:
        return 0


def extract_next_offer_date(offerdate_str: str) -> str:
    """Извлекает ближайшую дату оферты из строки"""
    if not offerdate_str:
        return ""
    
    try:
        offerdate_str = str(offerdate_str).strip()
        
        dates_list = [offerdate_str]
        for sep in [',', ';', ' ']:
            if sep in offerdate_str:
                dates_list = offerdate_str.split(sep)
                break
        
        valid_dates = []
        for date_str in dates_list:
            date_str = date_str.strip()
            if not date_str:
                continue
            
            try:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                valid_dates.append(date_obj)
            except:
                continue
        
        if not valid_dates:
            return ""
        
        today = datetime.now()
        future_dates = [d for d in valid_dates if d >= today]
        
        if future_dates:
            nearest = min(future_dates)
        else:
            nearest = max(valid_dates)
        
        return nearest.strftime("%d.%m.%Y")
    except:
        return ""


def fill_bond_data(df: pd.DataFrame) -> pd.DataFrame:
    """Заполняет данные об облигациях для всего DataFrame"""
    
    for idx in range(len(df)):
        isin = df.at[idx, "ISIN"]
        shortname = df.at[idx, "Shortname"] if "Shortname" in df.columns else ""
        
        if pd.isna(isin) or not str(isin).strip():
            df.at[idx, "Логирование"] = "ISIN отсутствует"
            print(f"[{idx+2:3d}] ISIN: {'':20} | Shortname: {str(shortname)[:30]:30} ... ✗ (ISIN отсутствует)")
            continue
        
        isin_str = str(isin).strip()
        print(f"[{idx+2:3d}] ISIN: {isin_str:20} | Shortname: {str(shortname)[:30]:30} ... ", end="", flush=True)
        
        try:
            # Запрос 1: Основная информация (YIELD, купоны, даты и т.д.)
            bond_info = get_security_info_by_isin(isin_str)
            
            if not bond_info:
                df.at[idx, "Логирование"] = "no_data"
                print("✗ (no_data)")
                continue
            
            # Запрос 2: Имя эмитента
            emitter_name = get_emitent_title(isin_str)
            if emitter_name:
                bond_info["EMITTER_NAME"] = emitter_name
            
            # ========== ЗАПОЛНЯЕМ ПОЛЯ ==========
            
            # Эмитент
            if pd.isna(df.at[idx, "Эмитент"]) or str(df.at[idx, "Эмитент"]).strip() == "":
                emitter = bond_info.get("EMITTER_NAME") or bond_info.get("NAME", "")
                if emitter:
                    df.at[idx, "Эмитент"] = str(emitter).strip()
            
            # YTM, % - берем YIELD из marketdata
            ytm_col = "YTM, %"
            if pd.isna(df.at[idx, ytm_col]) or str(df.at[idx, ytm_col]).strip() == "":
                # Приоритет: YIELD > YIELDTOOFFER > YIELDATWAPRICE
                ytm = bond_info.get("YIELD") or bond_info.get("YIELDTOOFFER") or bond_info.get("YIELDATWAPRICE")
                if ytm is not None and str(ytm).strip() not in ['', 'nan', 'None']:
                    try:
                        df.at[idx, ytm_col] = float(ytm)
                    except:
                        pass
            
            # Купонная доходность
            coupon_yield_col = None
            for col in df.columns:
                if "купонная" in col.lower() and "доходность" in col.lower():
                    coupon_yield_col = col
                    break
            
            if coupon_yield_col:
                if pd.isna(df.at[idx, coupon_yield_col]) or str(df.at[idx, coupon_yield_col]).strip() == "":
                    coupon_percent = bond_info.get("COUPONPERCENT")
                    if coupon_percent is not None and str(coupon_percent).strip() not in ['', 'nan']:
                        try:
                            df.at[idx, coupon_yield_col] = float(coupon_percent)
                        except:
                            coupon_value = bond_info.get("COUPONVALUE")
                            face_value = bond_info.get("FACEVALUE")
                            
                            if coupon_value and face_value:
                                try:
                                    cv = float(coupon_value)
                                    fv = float(face_value)
                                    if fv > 0:
                                        coupon_yield = (cv / fv) * 100
                                        df.at[idx, coupon_yield_col] = coupon_yield
                                except:
                                    pass
            
            # Купон
            coupon_col = None
            for col in df.columns:
                if col.strip() == "Купон":
                    coupon_col = col
                    break
            
            if coupon_col:
                if pd.isna(df.at[idx, coupon_col]) or str(df.at[idx, coupon_col]).strip() == "":
                    coupon_value = bond_info.get("COUPONVALUE")
                    if coupon_value is not None and str(coupon_value).strip() not in ['', 'nan']:
                        try:
                            df.at[idx, coupon_col] = float(coupon_value)
                        except:
                            pass
            
            # Валюта
            currency_col = None
            for col in df.columns:
                if col.strip() in ["Волюта", "Валюта", "Currency"]:
                    currency_col = col
                    break
            
            if currency_col:
                if pd.isna(df.at[idx, currency_col]) or str(df.at[idx, currency_col]).strip() == "":
                    currency = bond_info.get("FACEUNIT")
                    if currency:
                        df.at[idx, currency_col] = str(currency).strip()
            
            # Частота купона
            freq_col = None
            for col in df.columns:
                if "частота" in col.lower() and "купона" in col.lower():
                    freq_col = col
                    break
            
            if freq_col:
                if pd.isna(df.at[idx, freq_col]) or str(df.at[idx, freq_col]).strip() == "":
                    next_coupon = bond_info.get("NEXTCOUPON")
                    frequency = get_coupon_frequency(next_coupon)
                    
                    if frequency > 0:
                        df.at[idx, freq_col] = frequency
                    else:
                        df.at[idx, freq_col] = 2
            
            # Дата погашения
            maturity_col = None
            for col in df.columns:
                if "дата погашения" in col.lower():
                    maturity_col = col
                    break
            
            if maturity_col:
                if pd.isna(df.at[idx, maturity_col]) or str(df.at[idx, maturity_col]).strip() == "":
                    maturity_date = bond_info.get("MATDATE")
                    if maturity_date:
                        parsed_date = parse_date(maturity_date)
                        if parsed_date:
                            df.at[idx, maturity_col] = parsed_date
            
            # Дата оферты
            offer_col = None
            for col in df.columns:
                if "дата оферты" in col.lower():
                    offer_col = col
                    break
            
            if offer_col:
                if pd.isna(df.at[idx, offer_col]) or str(df.at[idx, offer_col]).strip() == "":
                    offer_dates = bond_info.get("OFFERDATE")
                    if offer_dates:
                        next_offer = extract_next_offer_date(offer_dates)
                        if next_offer:
                            df.at[idx, offer_col] = next_offer
            
            df.at[idx, "Логирование"] = "OK"
            print("✓")
            
        except Exception as e:
            df.at[idx, "Логирование"] = f"Error: {str(e)[:30]}"
            print(f"✗ ({str(e)[:30]})")
        
        # Задержка между запросами
        time.sleep(0.5)
    
    return df


# ============================================================================
# ОСНОВНАЯ ЛОГИКА
# ============================================================================

def main():
    """Основная функция"""
    
    print("=" * 100)
    print("╔" + "═" * 98 + "╗")
    print("║" + " " * 10 + "ЗАПОЛНЕНИЕ ДАННЫХ ОБ ОБЛИГАЦИЯХ ИЗ MOEX ISS API (ВЕРСИЯ 5 - ФИНАЛЬНАЯ)" + " " * 11 + "║")
    print("╚" + "═" * 98 + "╝")
    print()
    
    if not INPUT_FILE.exists():
        print(f"❌ ОШИБКА: Файл {INPUT_FILE} не найден!")
        return
    
    print(f"📂 Входной файл:  {INPUT_FILE}")
    print(f"💾 Выходной файл: {OUTPUT_FILE}")
    print()
    
    print("📖 Чтение входного файла...")
    try:
        df = pd.read_excel(INPUT_FILE, sheet_name=0)
    except Exception as e:
        print(f"❌ Ошибка при чтении файла: {e}")
        return
    
    print(f"✓ Прочитано {len(df)} строк")
    print()
    
    if "ISIN" not in df.columns:
        print("❌ Ошибка: в файле отсутствует колонка 'ISIN'")
        return
    
    if "Логирование" not in df.columns:
        df["Логирование"] = ""
    
    print("🔄 Обработка облигаций (это может занять несколько минут):")
    print("-" * 100)
    
    df = fill_bond_data(df)
    
    print("-" * 100)
    print()
    
    print("💾 Сохранение результатов...")
    try:
        OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
        
        with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Структура фонда', index=False)
        
        print(f"✓ Файл успешно сохранен: {OUTPUT_FILE}")
    except Exception as e:
        print(f"❌ Ошибка при сохранении файла: {e}")
        return
    
    print()
    print("=" * 100)
    
    # Статистика
    print()
    print("📊 СТАТИСТИКА:")
    print("-" * 100)
    ok_count = len(df[df["Логирование"] == "OK"])
    error_count = len(df) - ok_count
    
    print(f"  ✓ Успешно заполнено: {ok_count} облигаций из {len(df)}")
    print(f"  ✗ Ошибок/пропусков: {error_count} облигаций")
    
    if error_count > 0 and error_count <= 20:
        print()
        print("⚠️  Детали всех ошибок:")
        errors_df = df[df["Логирование"] != "OK"][["ISIN", "Логирование"]]
        for idx, (i, row) in enumerate(errors_df.iterrows(), 1):
            print(f"  {idx}. {str(row['ISIN']):20} → {row['Логирование']}")
    elif error_count > 20:
        print()
        print("⚠️  Первые 20 ошибок:")
        errors_df = df[df["Логирование"] != "OK"][["ISIN", "Логирование"]].head(20)
        for idx, (i, row) in enumerate(errors_df.iterrows(), 1):
            print(f"  {idx}. {str(row['ISIN']):20} → {row['Логирование']}")
        print(f"  ... и еще {error_count - 20} ошибок")
    
    print()
    print("✓ ГОТОВО!")
    print("=" * 100)


if __name__ == "__main__":
    main()
