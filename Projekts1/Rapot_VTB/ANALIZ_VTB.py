from __future__ import annotations

import json
import logging
import os
import sqlite3
import textwrap
from collections import defaultdict
from datetime import date
from pathlib import Path
from statistics import mean
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
import requests
from reportlab.lib.pagesizes import A4
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.pdfgen import canvas

ROOT_DIR = Path(__file__).resolve().parent
DB_PATH = ROOT_DIR / "out" / "Operation_VTB.sqlite"
TELEGRAM_CONFIG_PATH = ROOT_DIR / "telegram_config.json"
TELEGRAM_API_URL = "https://api.telegram.org"
TELEGRAM_MESSAGE_LIMIT = 3400
CBR_RATES_URL = "https://www.cbr-xml-daily.ru/latest.js"
CBR_HEADERS = {
    "Accept": "application/json, text/javascript",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
}

LOGGER = logging.getLogger("analiz_vtb")

# Base currency rates used to convert all amounts to RUB.
# Defaults are used until fresh rates are fetched from CBR.
CURRENCY_RATES = {
    "RUR": 1.0,
    "RUB": 1.0,
    "SUR": 1.0,
    "CNY": 11.32,
    "KGS": 1.0,
    "USD": 80.6,
    "EUR": 93.7,
}


def fetch_fx_rates(currency_codes: Iterable[str]) -> Dict[str, float]:
    normalized_codes = {str(code or "").strip().upper() for code in currency_codes if code}
    normalized_codes = {code for code in normalized_codes if code}
    if not normalized_codes:
        return {}

    try:
        response = requests.get(
            CBR_RATES_URL,
            headers=CBR_HEADERS,
            timeout=15,
        )
        response.raise_for_status()
        payload = response.json()
    except requests.RequestException as exc:
        LOGGER.warning("�� 㤠���� �������� ����� �����: %s", exc)
        return {}

    base_currency = str(payload.get("base", "RUB")).strip().upper()
    rates_data = payload.get("rates")
    if not isinstance(rates_data, dict):
        LOGGER.warning("�⢥� �� �� ᮤ�ন� ����� rates.")
        return {}

    rub_per_base = 1.0
    if base_currency != "RUB":
        rub_rate = rates_data.get("RUB")
        if rub_rate is None:
            LOGGER.warning("�� 㤠���� ������� ���� RUB � �⢥� ��.")
        else:
            try:
                rub_per_base = float(rub_rate)
            except (TypeError, ValueError):
                LOGGER.warning("�� 㤠���� ������� ���� RUB � �⢥� ��.")
                rub_per_base = 1.0

    result: Dict[str, float] = {}
    for code in normalized_codes:
        if code in {"RUB", "RUR", "SUR"}:
            result[code] = 1.0
            continue

        raw_rate = rates_data.get(code)
        try:
            rate_value = float(raw_rate)
        except (TypeError, ValueError):
            LOGGER.warning("��� ���४⭮�� ���� ��� %s", code)
            continue
        if rate_value == 0:
            LOGGER.warning("� �⢥� �� �㫥��� ���� ��� %s", code)
            continue

        if base_currency == "RUB":
            result[code] = 1.0 / rate_value
        else:
            result[code] = rub_per_base / rate_value

    return result
def refresh_currency_rates() -> None:
    fetched = fetch_fx_rates(CURRENCY_RATES.keys())
    if not fetched:
        return
    CURRENCY_RATES.update(fetched)


refresh_currency_rates()

DEFAULT_EXIT_EVENTS = {"SELL", "DIVIDEND", "REPAYMENT", "AMORTISATION"}
ETF_CODE = "ETF"
STOCK_CODE = "АКЦИЯ"

TRACKING_COLUMNS = {
    "NetCashIn": "REAL NOT NULL DEFAULT 0",
    "CurrentValue": "REAL NOT NULL DEFAULT 0",
    "CurrentInstrumentCount": "REAL NOT NULL DEFAULT 0",
    "Expenses": "REAL NOT NULL DEFAULT 0",
    "WorkingCapital": "REAL NOT NULL DEFAULT 0",
    "AbsoluteProfitValue": "REAL NOT NULL DEFAULT 0",
    "AbsoluteProfitPercent": "REAL NOT NULL DEFAULT 0",
    "AverageYield": "REAL NOT NULL DEFAULT 0",
    "AverageCouponYield": "REAL NOT NULL DEFAULT 0",
}

TARGET_SHARES = {
    "ETF": 15.0,
    "Еврооблигации": 15.0,
    "ОФЗ": 20.0,
    "Корпоративная облигация": 35.0,
    "Муниципальная облигация": 15.0,
}

SUR_ONLY_YIELD_PORTFOLIOS = {
    "124JAU STANDART",
    "124JAV IIS-3",
}

PDF_PAGE_WIDTH, PDF_PAGE_HEIGHT = A4
PDF_MARGIN = 36
PDF_FONT_SIZE = 12
PDF_LINE_HEIGHT = 16
PDF_WRAP_WIDTH = 92
PDF_FONT_CANDIDATES = [
    Path(os.environ.get("WINDIR", "C:\\Windows")) / "Fonts" / "arial.ttf",
    Path(os.environ.get("WINDIR", "C:\\Windows")) / "Fonts" / "Arial.ttf",
    Path(os.environ.get("WINDIR", "C:\\Windows")) / "Fonts" / "calibri.ttf",
    Path(os.environ.get("WINDIR", "C:\\Windows")) / "Fonts" / "segoeui.ttf",
]

def round_money(value: Optional[float]) -> float:
    return round(float(value or 0.0) + 1e-9, 2)


def average(values: Iterable[float]) -> Optional[float]:
    data = [v for v in values if v is not None]
    return mean(data) if data else None


def sanitize_currency(code: Optional[str]) -> str:
    if not code:
        return "RUR"
    return code.strip().upper()


def to_rub(value: Optional[float], currency: Optional[str]) -> float:
    amount = float(value or 0.0)
    if not amount:
        return 0.0
    code = sanitize_currency(currency)
    rate = CURRENCY_RATES.get(code)
    if rate is None:
        LOGGER.warning("Нет корректного курса для %s. Использую 1.0.", code)
        rate = 1.0
        CURRENCY_RATES[code] = rate
    return amount * rate



def ensure_database_available(db_path: Path) -> None:
    if not db_path.exists():
        raise FileNotFoundError(f"Database file not found: {db_path}")


def ensure_tracking_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS Analiz_Portfolio (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Portfolio TEXT NOT NULL,
            ReportDate TEXT NOT NULL,
            CreateDate TEXT NOT NULL
        )
        """
    )
    conn.commit()
    for column, definition in TRACKING_COLUMNS.items():
        ensure_column_exists(conn, "Analiz_Portfolio", column, definition)


def ensure_column_exists(
    conn: sqlite3.Connection, table: str, column: str, definition: str
) -> None:
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
    if column not in existing:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")
        conn.commit()


def collect_latest_portfolio_rows(conn: sqlite3.Connection) -> Dict[str, List[sqlite3.Row]]:
    rows_by_portfolio: Dict[str, List[sqlite3.Row]] = defaultdict(list)
    query = """
        SELECT fp.*
        FROM Final_Portfolio fp
        INNER JOIN (
            SELECT Portfolio, MAX(ReportDate) AS ReportDate
            FROM Final_Portfolio
            GROUP BY Portfolio
        ) latest
            ON fp.Portfolio = latest.Portfolio
           AND fp.ReportDate = latest.ReportDate
    """
    for row in conn.execute(query):
        rows_by_portfolio[row["Portfolio"]].append(row)
    return rows_by_portfolio


def clamp_yield(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    if value < 0:
        return None
    return min(value, 30.0)


def collect_final_portfolio_stats(
    conn: sqlite3.Connection,
) -> Tuple[Dict[str, dict], Dict[str, List[sqlite3.Row]]]:
    rows_map = collect_latest_portfolio_rows(conn)
    stats: Dict[str, dict] = {}

    for portfolio, rows in rows_map.items():
        report_date = rows[0]["ReportDate"]
        total_planned_qty = sum(filter(None, (row["PlannedQty"] for row in rows)))
        planned_value_rub = sum(filter(None, (row["PlannedValRub"] for row in rows)))
        market_value_rub = sum(filter(None, (row["Stoim_activRUB"] for row in rows)))

        non_default_rows = [
            row for row in rows if (row["is_traded"] or "").strip().upper() != "ДЕФОЛТ"
        ]
        default_identifiers = set()
        for row in rows:
            if (row["is_traded"] or "").strip().upper() == "ДЕФОЛТ":
                if row["isin"]:
                    default_identifiers.add(row["isin"])
                if row["secid"]:
                    default_identifiers.add(row["secid"])

        sum_stoim = sum(filter(None, (row["Stoim_activRUB"] for row in non_default_rows)))
        sum_planned = sum(filter(None, (row["PlannedValRub"] for row in non_default_rows)))
        current_value = (sum_stoim + sum_planned) / 2 if non_default_rows else 0.0
        unique_secids = {
            row["secid"]
            for row in non_default_rows
            if row["secid"]
        }

        # Расчёт доходности
        yield_values: List[float] = []
        yield_wap_values: List[float] = []
        coupon_totals: Dict[str, float] = defaultdict(float)
        coupon_counts: Dict[str, int] = defaultdict(int)
        coupon_total_non_rub = 0.0
        coupon_count_non_rub = 0

        for row in rows:
            faceunit = sanitize_currency(row["FACEUNIT"])
            sec_type = (row["SECTYPE"] or "").strip().upper()
            is_default = (row["is_traded"] or "").strip().upper() == "ДЕФОЛТ"

            include_in_yield = faceunit == "SUR" or sec_type == ETF_CODE
            if include_in_yield:
                if sec_type == ETF_CODE:
                    adj_yield = 15.3
                    adj_yield_wap = 15.3
                else:
                    adj_yield = clamp_yield(row["YIELD"])
                    adj_yield_wap = clamp_yield(row["YIELDATWAPRICE"])
                if adj_yield is not None:
                    yield_values.append(adj_yield)
                if adj_yield_wap is not None:
                    yield_wap_values.append(adj_yield_wap)

            if (
                not is_default
                and sec_type not in {ETF_CODE, STOCK_CODE}
                and faceunit != "SUR"
            ):
                coupon_value = row["COUPONPERCENT"]
                if coupon_value is not None:
                    coupon_totals[faceunit] += coupon_value
                    coupon_counts[faceunit] += 1
                    coupon_total_non_rub += coupon_value
                    coupon_count_non_rub += 1

        avg_yield = average(yield_values)
        avg_yield_wap = average(yield_wap_values)
        yield_components = [value for value in [avg_yield, avg_yield_wap] if value is not None]
        average_yield = sum(yield_components) / len(yield_components) if yield_components else 0.0

        if portfolio in SUR_ONLY_YIELD_PORTFOLIOS:
            # These portfolios require SUR-only YIELD averages pulled directly from Final_Portfolio.
            sur_yields = [
                float(row["YIELD"])
                for row in rows
                if row["YIELD"] is not None and sanitize_currency(row["FACEUNIT"]) == "SUR"
            ]
            sur_average = average(sur_yields)
            average_yield = sur_average if sur_average is not None else 0.0

        coupon_yield_by_currency = {}
        for currency, total in coupon_totals.items():
            count = coupon_counts[currency]
            coupon_yield_by_currency[currency] = round_money(total / count)
        average_coupon_yield = (
            coupon_total_non_rub / coupon_count_non_rub if coupon_count_non_rub else 0.0
        )

        stats[portfolio] = {
            "portfolio": portfolio,
            "report_date": report_date,
            "instrument_count": len(rows),
            "total_planned_qty": total_planned_qty,
            "planned_value_rub": planned_value_rub,
            "market_value_rub": market_value_rub,
            "current_value": current_value,
            "current_instruments": float(len(unique_secids)),
            "average_yield": average_yield,
            "average_coupon_yield": average_coupon_yield,
            "coupon_yield_by_currency": coupon_yield_by_currency,
            "default_identifiers": default_identifiers,
        }

    return stats, rows_map


def collect_operation_stats(
    conn: sqlite3.Connection, default_identifiers_map: Dict[str, set]
) -> Dict[str, dict]:
    stats: Dict[str, dict] = defaultdict(
        lambda: {
            "total_operations": 0,
            "first_operation_date": None,
            "last_operation_date": None,
            "total_cash_flow": 0.0,
            "event_breakdown": {},
            "cash_in_total": 0.0,
            "cash_out_total": 0.0,
            "fee_tax_total": 0.0,
            "fee_event_total": 0.0,
            "default_buy_total": 0.0,
            "default_exit_total": 0.0,
        }
    )

    query = """
        SELECT Portfolio, Event, Date, Sumtransaction, FeeTax, Faceunit, Symbol, Currency
        FROM Operation_VTB
    """
    for row in conn.execute(query):
        portfolio = row["Portfolio"]
        record = stats[portfolio]

        event = (row["Event"] or "").strip().upper()
        event_display = row["Event"] or "UNKNOWN"
        record["event_breakdown"].setdefault(event_display, 0)
        record["event_breakdown"][event_display] += 1

        record["total_operations"] += 1
        record["total_cash_flow"] += row["Sumtransaction"] or 0.0

        operation_date = row["Date"]
        if operation_date:
            if not record["first_operation_date"] or operation_date < record["first_operation_date"]:
                record["first_operation_date"] = operation_date
            if not record["last_operation_date"] or operation_date > record["last_operation_date"]:
                record["last_operation_date"] = operation_date

        currency = row["Currency"] or row["Faceunit"]
        sum_rub = to_rub(row["Sumtransaction"], currency)
        fee_rub = to_rub(row["FeeTax"], currency)

        if event == "CASH_IN":
            record["cash_in_total"] += sum_rub
        elif event == "CASH_OUT":
            record["cash_out_total"] += sum_rub

        if fee_rub:
            record["fee_tax_total"] += fee_rub
        if event == "FEE":
            record["fee_event_total"] += sum_rub

        default_symbols = default_identifiers_map.get(portfolio, set())
        symbol = row["Symbol"]
        if symbol and symbol in default_symbols:
            if event == "BUY":
                record["default_buy_total"] += sum_rub
            elif event in DEFAULT_EXIT_EVENTS:
                record["default_exit_total"] += sum_rub

    finalized_stats: Dict[str, dict] = {}
    for portfolio, record in stats.items():
        net_cash_in = record["cash_in_total"] + record["cash_out_total"]
        default_component = max(record["default_buy_total"] - record["default_exit_total"], 0.0)
        expenses = record["fee_tax_total"] + abs(record["fee_event_total"]) + default_component
        working_capital = net_cash_in - expenses

        finalized_stats[portfolio] = {
            "total_operations": record["total_operations"],
            "first_operation_date": record["first_operation_date"],
            "last_operation_date": record["last_operation_date"],
            "total_cash_flow": record["total_cash_flow"],
            "event_breakdown": record["event_breakdown"],
            "net_cash_in": net_cash_in,
            "expenses": expenses,
            "working_capital": working_capital,
            "default_expenses": default_component,
        }

    return finalized_stats


def should_insert_tracking_row(
    conn: sqlite3.Connection, portfolio: str, report_date: str, creation_date: str
) -> bool:
    query = """
        SELECT 1
        FROM Analiz_Portfolio
        WHERE Portfolio = ?
          AND ReportDate = ?
          AND CreateDate = ?
        LIMIT 1
    """
    cursor = conn.execute(query, (portfolio, report_date, creation_date))
    return cursor.fetchone() is None


def insert_tracking_row(conn: sqlite3.Connection, item: dict, creation_date: str) -> bool:
    if not should_insert_tracking_row(conn, item["portfolio"], item["report_date"], creation_date):
        return False

    payload = (
        item["portfolio"],
        item["report_date"],
        creation_date,
        round_money(item["net_cash_in"]),
        round_money(item["current_value"]),
        round_money(item["current_instruments"]),
        round_money(item["expenses"]),
        round_money(item["working_capital"]),
        round_money(item["absolute_profit_value"]),
        round_money(item["absolute_profit_percent"]),
        round_money(item["average_yield"]),
        round_money(item["average_coupon_yield"]),
    )

    conn.execute(
        """
        INSERT INTO Analiz_Portfolio (
            Portfolio,
            ReportDate,
            CreateDate,
            NetCashIn,
            CurrentValue,
            CurrentInstrumentCount,
            Expenses,
            WorkingCapital,
            AbsoluteProfitValue,
            AbsoluteProfitPercent,
            AverageYield,
            AverageCouponYield
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    conn.commit()
    return True


def build_summary(
    final_stats: Dict[str, dict], operation_stats: Dict[str, dict]
) -> List[dict]:
    summary: List[dict] = []
    for portfolio, stats in sorted(final_stats.items()):
        merged = dict(stats)
        op_stats = operation_stats.get(
            portfolio,
            {
                "total_operations": 0,
                "first_operation_date": None,
                "last_operation_date": None,
                "total_cash_flow": 0.0,
                "event_breakdown": {},
                "net_cash_in": 0.0,
                "expenses": 0.0,
                "working_capital": 0.0,
            },
        )
        merged.update(op_stats)

        profit_raw = merged["current_value"] - merged["net_cash_in"]
        merged["absolute_profit_value"] = abs(profit_raw)
        if merged["net_cash_in"]:
            merged["absolute_profit_percent"] = abs(profit_raw) / abs(merged["net_cash_in"]) * 100
        else:
            merged["absolute_profit_percent"] = 0.0

        summary.append(merged)
    return summary


def compute_overall_metrics(
    summary: List[dict], operation_stats: Dict[str, dict]
) -> dict:
    if not summary:
        return {}

    count = len(summary)
    total_current_value = sum(item["current_value"] for item in summary)
    total_expenses = sum(item["expenses"] for item in summary)
    total_working_capital = sum(item["working_capital"] for item in summary)
    total_net_cash = sum(item["net_cash_in"] for item in summary)
    total_abs_profit = sum(item["absolute_profit_value"] for item in summary)
    avg_abs_profit_percent = (
        sum(item["absolute_profit_percent"] for item in summary) / count if count else 0.0
    )
    avg_yield = sum(item["average_yield"] for item in summary) / count if count else 0.0
    avg_coupon_yield = (
        sum(item["average_coupon_yield"] for item in summary) / count if count else 0.0
    )
    total_default_expenses = sum(
        operation_stats.get(item["portfolio"], {}).get("default_expenses", 0.0)
        for item in summary
    )

    return {
        "TotalCurrentValue": total_current_value,
        "TotalNetCashIn": total_net_cash,
        "TotalExpenses": total_expenses,
        "TotalDefaultExpenses": total_default_expenses,
        "TotalWorkingCapital": total_working_capital,
        "TotalAbsoluteProfitValue": total_abs_profit,
        "AverageAbsoluteProfitPercent": avg_abs_profit_percent,
        "AverageYield": avg_yield,
        "AverageCouponYield": avg_coupon_yield,
    }


def build_distributions(
    portfolio_rows: Dict[str, List[sqlite3.Row]], target_shares: Dict[str, float]
) -> List[dict]:
    distribution_data: List[dict] = []
    target_portfolios = {"124JAV IIS-3", "124JAU STANDART"}
    for portfolio, rows in portfolio_rows.items():
        apply_targets = portfolio in target_portfolios
        non_default_rows = [
            row for row in rows if (row["is_traded"] or "").strip().upper() != "ДЕФОЛТ"
        ]
        total_value = sum(float(row["Stoim_activRUB"] or 0.0) for row in non_default_rows)
        type_values: Dict[str, float] = defaultdict(float)
        type_counts: Dict[str, set] = defaultdict(set)

        for row in non_default_rows:
            sec_type = (row["SECTYPE"] or "UNKNOWN").strip()
            if sec_type == "Еврооблигация":
                sec_type = "Еврооблигации"
            value = float(row["Stoim_activRUB"] or 0.0)
            type_values[sec_type] += value
            if row["secid"]:
                type_counts[sec_type].add(row["secid"])

        all_types = set(type_values.keys())
        if apply_targets:
            all_types |= set(target_shares.keys())
        for sec_type in sorted(all_types):
            type_value = type_values.get(sec_type, 0.0)
            actual_share = (type_value / total_value * 100) if total_value else 0.0
            target_share = target_shares.get(sec_type, 0.0) if apply_targets else 0.0
            share_diff = actual_share - target_share if apply_targets else 0.0

            needed_amount = 0.0
            if apply_targets and target_share > 0 and actual_share < target_share and total_value > 0:
                target_fraction = target_share / 100
                numerator = target_fraction * total_value - type_value
                if numerator > 0:
                    needed_amount = numerator / (1 - target_fraction)

            distribution_data.append(
                {
                    "Portfolio": portfolio,
                    "SECType": sec_type,
                    "TypeValue": type_value,
                    "InstrumentCount": len(type_counts.get(sec_type, set())),
                    "ActualSharePercent": actual_share,
                    "TargetSharePercent": target_share,
                    "ShareDifferencePercent": share_diff,
                    "AdditionalInvestmentNeeded": needed_amount,
                }
            )
    return distribution_data


def collect_default_securities(conn: sqlite3.Connection) -> List[dict]:
    query = """
        SELECT Portfolio, isin, secid, shortname, HASTECHNICALDEFAULT, HASDEFAULT, is_traded
        FROM Final_Portfolio
        WHERE COALESCE(CAST(NULLIF(HASTECHNICALDEFAULT, '') AS INTEGER), 0) = 1
           OR COALESCE(CAST(NULLIF(HASDEFAULT, '') AS INTEGER), 0) = 1
           OR UPPER(COALESCE(is_traded, '')) = 'ДЕФОЛТ'
    """
    results: List[dict] = []
    for row in conn.execute(query):
        results.append(
            {
                "Portfolio": row["Portfolio"],
                "ISIN": row["isin"],
                "SECID": row["secid"],
                "ShortName": row["shortname"],
                "HASTECHNICALDEFAULT": row["HASTECHNICALDEFAULT"],
                "HASDEFAULT": row["HASDEFAULT"],
                "is_traded": row["is_traded"],
            }
        )
    return results


def aggregate_coupon_yields(portfolio_rows: Dict[str, List[sqlite3.Row]]) -> List[dict]:
    totals: Dict[str, float] = defaultdict(float)
    counts: Dict[str, int] = defaultdict(int)

    for rows in portfolio_rows.values():
        for row in rows:
            if (row["is_traded"] or "").strip().upper() == "ДЕФОЛТ":
                continue
            sec_type = (row["SECTYPE"] or "").strip().upper()
            faceunit = sanitize_currency(row["FACEUNIT"])
            if sec_type in {ETF_CODE, STOCK_CODE} or faceunit == "SUR":
                continue
            coupon = row["COUPONPERCENT"]
            if coupon is not None:
                totals[faceunit] += coupon
                counts[faceunit] += 1

    aggregated = []
    for currency, total in totals.items():
        aggregated.append(
            {
                "Currency": currency,
                "AverageCouponPercent": round_money(total / counts[currency]),
                "Count": counts[currency],
            }
        )
    return aggregated


def build_rankings(
    portfolio_rows: Dict[str, List[sqlite3.Row]], abnormal_set: set
) -> Dict[str, Dict[str, List[dict]]]:
    portfolio_rankings: Dict[str, Dict[str, List[dict]]] = {}
    target_portfolios = {"124JAU STANDART", "124JAV IIS-3"}

    for portfolio in target_portfolios:
        rows = portfolio_rows.get(portfolio, [])
        securities: Dict[str, dict] = {}
        for row in rows:
            secid = row["secid"]
            if not secid:
                continue
            entry = securities.setdefault(
                secid,
                {
                    "Portfolio": portfolio,
                    "SECID": secid,
                    "ISIN": row["isin"],
                    "ShortName": row["shortname"],
                    "SECTYPE": row["SECTYPE"],
                    "FACEUNIT": sanitize_currency(row["FACEUNIT"]),
                    "scores": [],
                },
            )
            components = [
                value
                for value in (row["YIELD"], row["YIELDATWAPRICE"], row["COUPONPERCENT"])
                if value is not None
            ]
            entry["scores"].extend(components)

        prepared: List[dict] = []
        for entry in securities.values():
            if entry["scores"]:
                identifiers = {entry["SECID"], entry["ISIN"]}
                if identifiers & abnormal_set:
                    continue
                entry["Score"] = sum(entry["scores"]) / len(entry["scores"])
                prepared.append(entry)

        sur = [
            item
            for item in prepared
            if item["FACEUNIT"] == "SUR" and (item["SECTYPE"] or "").strip().upper() != "ОФЗ"
        ]
        sur_sorted = sorted(sur, key=lambda x: x["Score"], reverse=True)
        sur_top = sur_sorted[:5]
        sur_bottom = sorted(sur, key=lambda x: x["Score"])[:5]

        non_sur = [item for item in prepared if item["FACEUNIT"] != "SUR"]
        non_sur_top = sorted(non_sur, key=lambda x: x["Score"], reverse=True)[:3]
        non_sur_bottom = sorted(non_sur, key=lambda x: x["Score"])[:1]

        portfolio_rankings[portfolio] = {
            "SUR_Top": sur_top,
            "SUR_Bottom": sur_bottom,
            "NonSUR_Top": non_sur_top,
            "NonSUR_Bottom": non_sur_bottom,
        }

    return portfolio_rankings


def detect_abnormal_yields(portfolio_rows: Dict[str, List[sqlite3.Row]]) -> List[dict]:
    abnormal: List[dict] = []
    for rows in portfolio_rows.values():
        for row in rows:
            yield_value = row["YIELD"]
            if yield_value is None:
                continue
            if yield_value < 0 or yield_value > 30:
                abnormal.append(
                    {
                        "Portfolio": row["Portfolio"],
                        "SECID": row["secid"],
                        "ISIN": row["isin"],
                        "ShortName": row["shortname"],
                        "YIELD": yield_value,
                        "FACEUNIT": row["FACEUNIT"],
                        "SECTYPE": row["SECTYPE"],
                    }
                )
    return abnormal


def export_to_excel(
    summary: List[dict],
    overall_metrics: dict,
    distribution_rows: List[dict],
    defaults_info: List[dict],
    coupon_currency_overall: List[dict],
    rankings: Dict[str, Dict[str, List[dict]]],
    abnormal_yields: List[dict],
) -> None:
    output_path = ROOT_DIR / "out" / "OUTPUT_Analiz_portfolio.xlsx"

    if output_path.exists():
        try:
            output_path.unlink()
        except PermissionError as exc:
            raise SystemExit(
                f"Cannot overwrite {output_path}. Close the file and retry."
            ) from exc

    summary_rows: List[dict] = []
    for item in summary:
        row = {
            key: value
            for key, value in item.items()
            if key not in {"event_breakdown", "coupon_yield_by_currency", "default_identifiers"}
        }
        row["EventBreakdown"] = json.dumps(item["event_breakdown"], ensure_ascii=False)
        row["CouponYieldByCurrency"] = json.dumps(
            item["coupon_yield_by_currency"], ensure_ascii=False
        )
        summary_rows.append(row)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        pd.DataFrame(summary_rows).to_excel(
            writer, sheet_name="PortfolioSummary", index=False
        )
        pd.DataFrame([overall_metrics]).to_excel(
            writer, sheet_name="Overall", index=False
        )
        pd.DataFrame(distribution_rows).to_excel(
            writer, sheet_name="Distributions", index=False
        )
        pd.DataFrame(defaults_info).to_excel(
            writer, sheet_name="DefaultSecurities", index=False
        )
        pd.DataFrame(coupon_currency_overall).to_excel(
            writer, sheet_name="CouponByCurrency", index=False
        )
        for portfolio, data in rankings.items():
            pd.DataFrame(data.get("SUR_Top", [])).to_excel(
                writer, sheet_name=f"{portfolio}_SUR_Top", index=False
            )
            pd.DataFrame(data.get("SUR_Bottom", [])).to_excel(
                writer, sheet_name=f"{portfolio}_SUR_Bottom", index=False
            )
            pd.DataFrame(data.get("NonSUR_Top", [])).to_excel(
                writer, sheet_name=f"{portfolio}_NonSUR_Top", index=False
            )
            pd.DataFrame(data.get("NonSUR_Bottom", [])).to_excel(
                writer, sheet_name=f"{portfolio}_NonSUR_Bottom", index=False
            )
        pd.DataFrame(abnormal_yields).to_excel(
            writer, sheet_name="AbnormalYields", index=False
        )


def _normalize_chat_ids(raw_value: Optional[object]) -> List[str]:
    if raw_value is None:
        return []
    if isinstance(raw_value, str):
        candidates = raw_value.replace(";", ",").split(",")
    elif isinstance(raw_value, Iterable):
        candidates = list(raw_value)
    else:
        candidates = [raw_value]
    normalized: List[str] = []
    for value in candidates:
        value_str = str(value).strip()
        if value_str:
            normalized.append(value_str)
    return normalized


def load_telegram_settings() -> Tuple[Optional[str], List[str]]:
    token = (
        os.environ.get("TELEGRAM_BOT_TOKEN")
        or os.environ.get("BOT_TOKEN")
        or None
    )
    chat_ids = _normalize_chat_ids(
        os.environ.get("TELEGRAM_CHAT_IDS") or os.environ.get("TELEGRAM_CHAT_ID")
    )

    if TELEGRAM_CONFIG_PATH.exists():
        try:
            config = json.loads(TELEGRAM_CONFIG_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise SystemExit(f"telegram_config.json поврежден: {exc}") from exc
        token = token or config.get("token") or config.get("bot_token")
        if not chat_ids:
            chat_ids = _normalize_chat_ids(
                config.get("chat_ids") or config.get("chat_id")
            )
    return token, chat_ids


def format_money(value: Optional[float]) -> str:
    amount = round_money(value)
    return f"{amount:,.2f}".replace(",", " ")


def format_percent(value: Optional[float]) -> str:
    if value is None:
        return "0.00%"
    return f"{value:.2f}%"


def portfolio_alias(portfolio_name: Optional[str]) -> str:
    if not portfolio_name:
        return "—"
    first, _, rest = portfolio_name.partition(" ")
    return rest.strip() or portfolio_name


def db_flag(value: Optional[object]) -> bool:
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return value != 0
    normalized = str(value).strip().lower()
    return normalized not in {"", "0", "false", "нет"}


def prepare_report_context(
    summary: List[dict],
    overall_metrics: dict,
    creation_date: str,
    defaults_info: List[dict],
    distribution_rows: List[dict],
    coupon_currency_overall: List[dict],
    rankings: Dict[str, Dict[str, List[dict]]],
) -> dict:
    overall_raw = overall_metrics or {}
    default_avg_yield = (
        overall_raw.get("AverageYield")
        if overall_raw.get("AverageYield") is not None
        else overall_raw.get("AveragePortfolioYield")
    )
    if default_avg_yield is None:
        default_avg_yield = 0.0

    overall_data = {
        "total_current_value": round_money(overall_raw.get("TotalCurrentValue", 0.0)),
        "total_expenses": round_money(overall_raw.get("TotalExpenses", 0.0)),
        "total_working_capital": round_money(overall_raw.get("TotalWorkingCapital", 0.0)),
        "total_net_cash_in": round_money(overall_raw.get("TotalNetCashIn", 0.0)),
        "total_absolute_profit_value": round_money(
            overall_raw.get("TotalAbsoluteProfitValue", 0.0)
        ),
        "average_portfolio_yield": default_avg_yield or 0.0,
        "average_coupon_yield": overall_raw.get("AverageCouponYield", 0.0) or 0.0,
        "average_absolute_profit_percent": overall_raw.get(
            "AverageAbsoluteProfitPercent", 0.0
        )
        or 0.0,
        "total_default_expenses": round_money(
            overall_raw.get("TotalDefaultExpenses", 0.0)
        ),
    }
    overall_data["total_current_value_fmt"] = format_money(
        overall_data["total_current_value"]
    )
    overall_data["total_expenses_fmt"] = format_money(overall_data["total_expenses"])
    overall_data["total_working_capital_fmt"] = format_money(
        overall_data["total_working_capital"]
    )
    overall_data["total_net_cash_in_fmt"] = format_money(
        overall_data["total_net_cash_in"]
    )
    overall_data["total_absolute_profit_value_fmt"] = format_money(
        overall_data["total_absolute_profit_value"]
    )
    overall_data["average_portfolio_yield_fmt"] = format_percent(
        overall_data["average_portfolio_yield"]
    )
    overall_data["average_yield_fmt"] = format_percent(
        default_avg_yield or 0.0
    )
    overall_data["average_coupon_yield_fmt"] = format_percent(
        overall_data["average_coupon_yield"]
    )
    overall_data["average_absolute_profit_percent_fmt"] = format_percent(
        overall_data["average_absolute_profit_percent"]
    )
    overall_data["total_default_expenses_fmt"] = format_money(
        overall_data["total_default_expenses"]
    )

    distribution_map: Dict[str, Dict[str, dict]] = defaultdict(dict)
    for row in distribution_rows:
        portfolio = row.get("Portfolio")
        sec_type = row.get("SECType")
        if not portfolio or not sec_type:
            continue
        entry = {
            "instrument_count": int(row.get("InstrumentCount") or 0),
            "actual_share": float(row.get("ActualSharePercent") or 0.0),
            "target_share": float(row.get("TargetSharePercent") or 0.0),
            "share_diff": float(row.get("ShareDifferencePercent") or 0.0),
            "type_value": round_money(row.get("TypeValue") or 0.0),
            "additional_needed": round_money(row.get("AdditionalInvestmentNeeded") or 0.0),
        }
        entry["actual_share_fmt"] = format_percent(entry["actual_share"])
        entry["target_share_fmt"] = format_percent(entry["target_share"])
        entry["share_diff_fmt"] = format_percent(entry["share_diff"])
        entry["type_value_fmt"] = format_money(entry["type_value"])
        entry["additional_needed_fmt"] = format_money(entry["additional_needed"])
        distribution_map[portfolio][sec_type] = entry

    default_sections = {"default": [], "technical": []}
    for item in defaults_info:
        portfolio = item.get("Portfolio")
        if not portfolio:
            continue
        security = {
            "Portfolio": portfolio,
            "portfolio_alias": portfolio_alias(portfolio),
            "ShortName": item.get("ShortName") or item.get("SECID") or item.get("ISIN") or "—",
            "ISIN": item.get("ISIN") or "",
            "SECID": item.get("SECID") or "",
        }
        is_traded_flag = (item.get("is_traded") or "").strip().upper() == "ДЕФОЛТ"
        has_default = db_flag(item.get("HASDEFAULT"))
        has_technical = db_flag(item.get("HASTECHNICALDEFAULT"))
        if (is_traded_flag or has_default) and not has_technical:
            default_sections["default"].append(security)
        if has_technical:
            default_sections["technical"].append(security)

    context = {
        "creation_date": creation_date,
        "overall": overall_data,
        "portfolios": [],
        "portfolios_index": {},
        "defaults": default_sections,
        "distributions": distribution_map,
        "rankings": rankings or {},
        "currency_quotes": {
            code: format_money(CURRENCY_RATES.get(code, 0.0)) for code in ("USD", "EUR", "CNY")
        },
        "coupon_currency_overall": coupon_currency_overall,
        "raw": {
            "summary": summary,
            "overall_metrics": overall_metrics,
            "defaults_info": defaults_info,
            "distribution_rows": distribution_rows,
        },
    }

    for item in summary:
        net_cash = round_money(item.get("net_cash_in"))
        profit_value = round_money(item.get("absolute_profit_value"))
        profit_percent = item.get("absolute_profit_percent") or 0.0
        portfolio_entry = {
            "code": item.get("portfolio", "-"),
            "alias": portfolio_alias(item.get("portfolio")),
            "report_date": item.get("report_date", "-"),
            "current_value": round_money(item.get("current_value")),
            "net_cash_in": net_cash,
            "profit_value": profit_value,
            "profit_percent": profit_percent,
            "average_yield": item.get("average_yield") or 0.0,
            "average_coupon_yield": item.get("average_coupon_yield") or 0.0,
            "working_capital": round_money(item.get("working_capital")),
            "expenses": round_money(item.get("expenses")),
            "current_instruments": int(item.get("current_instruments") or 0),
            "instrument_count": int(item.get("instrument_count") or 0),
            "total_operations": int(item.get("total_operations") or 0),
            "first_operation_date": item.get("first_operation_date") or "—",
            "last_operation_date": item.get("last_operation_date") or "—",
        }
        portfolio_entry["current_value_fmt"] = format_money(
            portfolio_entry["current_value"]
        )
        portfolio_entry["net_cash_in_fmt"] = format_money(portfolio_entry["net_cash_in"])
        portfolio_entry["profit_value_fmt"] = format_money(portfolio_entry["profit_value"])
        portfolio_entry["profit_percent_fmt"] = format_percent(profit_percent)
        portfolio_entry["average_yield_fmt"] = format_percent(
            portfolio_entry["average_yield"]
        )
        portfolio_entry["average_coupon_yield_fmt"] = format_percent(
            portfolio_entry["average_coupon_yield"]
        )
        portfolio_entry["working_capital_fmt"] = format_money(
            portfolio_entry["working_capital"]
        )
        portfolio_entry["expenses_fmt"] = format_money(portfolio_entry["expenses"])
        context["portfolios"].append(portfolio_entry)
        context["portfolios_index"][portfolio_entry["code"]] = portfolio_entry

    return context


def format_security_list(items: List[dict]) -> str:
    if not items:
        return "Нет активов."
    formatted: List[str] = []
    for entry in items:
        portfolio_name = entry.get("portfolio_alias") or portfolio_alias(entry.get("Portfolio"))
        short_name = entry.get("ShortName") or entry.get("SECID") or entry.get("ISIN") or "—"
        identifier = entry.get("ISIN") or entry.get("SECID") or "—"
        formatted.append(f"- {portfolio_name} {short_name} ({identifier})")
    return "\n".join(formatted)


def format_distribution_line(entry: Optional[dict], include_diff: bool = False) -> str:
    if not entry:
        return "Данных нет."
    parts = [
        f"♦️ {entry['instrument_count']} шт.",
        f"♦️ {entry['actual_share_fmt']}",
        f"♦️ {entry['type_value_fmt']} ₽",
    ]
    if include_diff:
        parts.append(f"♦️ {entry['share_diff_fmt']}")
    return " ".join(parts)


def format_additional_investment(entry: Optional[dict]) -> Optional[str]:
    if not entry:
        return None
    needed = entry.get("additional_needed")
    if not needed or needed <= 0:
        return None
    return f"Докупить на {entry.get('additional_needed_fmt', format_money(needed))} ₽"


def digit_emoji(index: int) -> str:
    mapping = {1: "1️⃣", 2: "2️⃣", 3: "3️⃣", 4: "4️⃣", 5: "5️⃣"}
    return mapping.get(index, f"{index}.")


def format_ranking_block(entries: List[dict], limit: int, fallback: str) -> List[str]:
    if not entries:
        return [fallback]
    lines: List[str] = []
    for idx, item in enumerate(entries[:limit], start=1):
        short_name = item.get("ShortName") or item.get("SECID") or "—"
        identifier = item.get("ISIN") or item.get("SECID") or "—"
        score = format_percent(item.get("Score"))
        lines.append(f"{digit_emoji(idx)} {short_name} ({identifier}) — {score}")
    return lines


def build_overall_message(report_context: dict) -> str:
    creation_date = report_context.get("creation_date", "-")
    overall = report_context.get("overall") or {}
    portfolios_index = report_context.get("portfolios_index") or {}
    kopilka = portfolios_index.get("1UJ8S KOPILKA")
    default_sections = report_context.get("defaults") or {}
    default_default_list = default_sections.get("default", [])
    default_technical_list = default_sections.get("technical", [])
    distributions = report_context.get("distributions") or {}
    kopilka_distribution = distributions.get("1UJ8S KOPILKA", {})
    currencies = report_context.get("currency_quotes") or {}

    lines = [
        f"📊 Анализ Инвестиционных Портфелей на {creation_date}",
        "",
        "💼 Общий объем инвестиций:",
        (
            f"{overall.get('total_current_value_fmt', '0.00')}₽ из которых "
            f"{kopilka.get('current_value_fmt', '0.00') if kopilka else '0.00'}₽ на счету \"KOPILKA\""
        ),
        "",
        f"- 💰 Работающие деньги: {overall.get('total_working_capital_fmt', '0.00')} ₽",
        f"- ✅ Абсолютная прибыль: {overall.get('total_absolute_profit_value_fmt', '0.00')} ₽",
        f"- 💸 Прирост капитала: {overall.get('average_absolute_profit_percent_fmt', '0.00%')}",
        f"- 🏆 Доходность: {overall.get('average_yield_fmt', overall.get('average_portfolio_yield_fmt', '0.00%'))}",
        "-----------------------------------------------------------------",
        "⛔️ ДЕФОЛТНЫЕ АКТИВЫ📛",
        "❌ Дефолт",
        format_security_list(default_default_list),
        "⁉️ Тех.Дефолт",
        format_security_list(default_technical_list),
        "-----------------------------------------------------------------",
        "🔹🔹🔹 Портфель \"KOPILKA\" 🔹🔹🔹",
    ]
    if kopilka:
        lines.extend(
            [
                f"  💰 Текущая стоимость: {kopilka['current_value_fmt']} ₽",
                f"  📊 Количество активов: {kopilka['instrument_count']}",
                f"  ✅ Абсолютная прибыль: {kopilka['profit_value_fmt']} ₽",
                f"  📉 Прирост: {kopilka['profit_percent_fmt']}",
                f"  💵 Средняя доходность: {kopilka['average_yield_fmt']}",
            ]
        )
    else:
        lines.append("  Данные по портфелю KOPILKA отсутствуют.")

    lines.extend(
        [
            "",
            "🧮🧮🧮 Распределение по типам активов:",
            "1️⃣ ETF",
            format_distribution_line(kopilka_distribution.get("ETF")),
        ]
    )

    lines.extend(
        [
            "-----------------------------------------------------------------",
            "  Котировки валют на дату анализа",
            f"🇺🇸 USD: {currencies.get('USD', '0.00')} ₽",
            f"🇪🇺 EUR: {currencies.get('EUR', '0.00')} ₽",
            f"🇨🇳 CNY: {currencies.get('CNY', '0.00')} ₽",
            "-----------------------------------------------------------------",
        ]
    )
    return "\n".join(lines).strip()


def build_portfolio_message(report_context: dict, portfolio_code: str) -> str:
    entry = (report_context.get("portfolios_index") or {}).get(portfolio_code)
    if not entry:
        return ""
    distributions = report_context.get("distributions") or {}
    distribution_rows = distributions.get(portfolio_code, {})
    rankings = report_context.get("rankings") or {}
    ranking_data = rankings.get(portfolio_code, {})

    lines = [
        f"🔹🔹🔹 Портфель \"{entry['alias']}\" 🔹🔹🔹",
        f"  💰 Текущая стоимость: {entry['current_value_fmt']} ₽",
        f"  📊 Количество активов: {entry['instrument_count']}",
        f"  ✅ Абсолютная прибыль: {entry['profit_value_fmt']} ₽",
        f"  📉 Прирост капитала: {entry['profit_percent_fmt']}",
        f"  🏆 Доходность: {entry['average_yield_fmt']}",
        f"  💵 Средняя доходность Еврооблиг.: {entry['average_coupon_yield_fmt']}",
        "",
        "🧮🧮🧮 Распределение по типам активов и отклонения от эталонного распределения:",
    ]

    sec_types = [
        ("Корпоративные облигации", "Корпоративная облигация"),
        ("Еврооблигации", "Еврооблигации"),
        ("Муниципальные облигации", "Муниципальная облигация"),
        ("ОФЗ", "ОФЗ"),
        ("ETF", "ETF"),
    ]
    for idx, (title, sec_key) in enumerate(sec_types, start=1):
        entry_data = distribution_rows.get(sec_key)
        lines.append(f"{digit_emoji(idx)} {title}")
        lines.append(format_distribution_line(entry_data, include_diff=True))
        investment_line = format_additional_investment(entry_data)
        if investment_line:
            lines.append(investment_line)

    lines.extend(
        [
            "",
            "🌟🌟🌟 Топ-5 наиболее прибыльных активов:",
            *format_ranking_block(
                ranking_data.get("SUR_Top", []), 5, "Нет подходящих активов."
            ),
            "",
            "💹💹💹 Топ-2 прибыльные активы в иностранной валюте:",
            *format_ranking_block(
                ranking_data.get("NonSUR_Top", []), 2, "Нет данных по иностранной валюте."
            ),
            "",
            "‼️‼️‼️ Топ-5 НАИМЕНЕЕ прибыльных активов:",
            *format_ranking_block(
                ranking_data.get("SUR_Bottom", []), 5, "Нет данных по отстающим активам."
            ),
            "",
            "🧮🧮🧮 Наименее прибыльный актив в иностранной валюте:",
            *format_ranking_block(
                ranking_data.get("NonSUR_Bottom", []), 1, "Нет данных по иностранной валюте."
            ),
            "-----------------------------------------------------------------",
        ]
    )
    return "\n".join(lines).strip()


def build_report_messages(report_context: dict) -> List[str]:
    messages = [
        build_overall_message(report_context),
        build_portfolio_message(report_context, "124JAV IIS-3"),
        build_portfolio_message(report_context, "124JAU STANDART"),
    ]
    return [message for message in messages if message]


def split_message(text: str, limit: int = TELEGRAM_MESSAGE_LIMIT) -> List[str]:
    if len(text) <= limit:
        return [text]
    chunks: List[str] = []
    current_lines: List[str] = []
    current_length = 0
    for line in text.splitlines():
        addition = line + "\n"
        if current_length + len(addition) > limit and current_lines:
            chunks.append("\n".join(current_lines).rstrip())
            current_lines = [line]
            current_length = len(line)
        else:
            current_lines.append(line)
            current_length += len(addition)
    if current_lines:
        chunks.append("\n".join(current_lines).rstrip())
    return chunks



def _wrap_pdf_lines(messages: List[str]) -> List[str]:
    wrapper = textwrap.TextWrapper(
        width=PDF_WRAP_WIDTH,
        replace_whitespace=False,
        drop_whitespace=False,
        expand_tabs=False,
        break_long_words=True,
    )
    lines: List[str] = []
    for idx, message in enumerate(messages):
        inner_lines = message.splitlines() or [""]
        for raw_line in inner_lines:
            normalized = raw_line.rstrip("\r")
            wrapped = wrapper.wrap(normalized)
            if not wrapped:
                lines.append("")
            else:
                lines.extend(wrapped)
        if idx < len(messages) - 1:
            lines.extend(["", "-" * 80, ""])
    return lines or [""]


def _locate_pdf_font() -> Path:
    for candidate in PDF_FONT_CANDIDATES:
        if candidate and candidate.is_file():
            return candidate
    raise FileNotFoundError(
        "Не найден доступный TrueType-шрифт для PDF. Обновите список PDF_FONT_CANDIDATES."
    )


PDF_FONT_NAME = "VTB_Report_Font"
_PDF_FONT_REGISTERED = False


def ensure_pdf_font() -> str:
    global _PDF_FONT_REGISTERED
    if _PDF_FONT_REGISTERED:
        return PDF_FONT_NAME
    font_path = _locate_pdf_font()
    pdfmetrics.registerFont(TTFont(PDF_FONT_NAME, str(font_path)))
    _PDF_FONT_REGISTERED = True
    return PDF_FONT_NAME


def export_report_pdf(messages: List[str], output_path: Path) -> Path:
    output_path = Path(output_path)
    if not messages:
        raise ValueError("Отчёт пуст — нет данных для PDF.")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    lines = _wrap_pdf_lines(messages)
    font_name = ensure_pdf_font()

    pdf_canvas = canvas.Canvas(str(output_path), pagesize=A4)
    pdf_canvas.setAuthor("VTB Portfolio Assistant")
    pdf_canvas.setTitle("VTB Portfolio Report")
    pdf_canvas.setSubject("Автоматический анализ портфелей ВТБ")
    pdf_canvas.setCreator("ANALIZ_VTB.py")

    pdf_canvas.setFont(font_name, PDF_FONT_SIZE)
    y_position = PDF_PAGE_HEIGHT - PDF_MARGIN
    for line in lines:
        if y_position < PDF_MARGIN:
            pdf_canvas.showPage()
            pdf_canvas.setFont(font_name, PDF_FONT_SIZE)
            y_position = PDF_PAGE_HEIGHT - PDF_MARGIN
        pdf_canvas.drawString(PDF_MARGIN, y_position, line)
        y_position -= PDF_LINE_HEIGHT

    pdf_canvas.save()
    LOGGER.info("PDF report saved to %s", output_path)
    return output_path


def send_telegram_messages(messages: List[str]) -> None:
    token, chat_ids = load_telegram_settings()
    if not token or not chat_ids:
        print("Telegram: настройки отсутствуют (нет токена или chat_id).")
        return

    for chat_id in chat_ids:
        for message in messages:
            for chunk in split_message(message):
                response = requests.post(
                    f"{TELEGRAM_API_URL}/bot{token}/sendMessage",
                    json={"chat_id": chat_id, "text": chunk},
                    timeout=30,
                )
                try:
                    response.raise_for_status()
                except requests.RequestException as exc:
                    raise SystemExit(
                        f"Не удалось отправить сообщение в Telegram (chat_id={chat_id}): {exc}"
                    ) from exc
                try:
                    payload = response.json()
                except ValueError as exc:
                    raise SystemExit(
                        f"Telegram вернул некорректный ответ (chat_id={chat_id}): {exc}"
                    ) from exc
                if not payload.get("ok", False):
                    description = payload.get("description", "unknown error")
                    raise SystemExit(
                        f"Telegram отклонил сообщение (chat_id={chat_id}): {description}"
                    )


def run_analysis_pipeline(
    send_telegram: bool = False, pdf_path: Optional[Path] = None
) -> List[str]:
    ensure_database_available(DB_PATH)
    creation_date = date.today().isoformat()

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    ensure_tracking_table(conn)

    final_stats, portfolio_rows = collect_final_portfolio_stats(conn)
    if not final_stats:
        print("No active portfolios found in Final_Portfolio.")
        return []

    default_identifiers_map = {
        portfolio: data.get("default_identifiers", set()) for portfolio, data in final_stats.items()
    }
    operation_stats = collect_operation_stats(conn, default_identifiers_map)
    summary = build_summary(final_stats, operation_stats)
    overall_metrics = compute_overall_metrics(summary, operation_stats)
    distribution_rows = build_distributions(portfolio_rows, TARGET_SHARES)
    defaults_info = collect_default_securities(conn)
    coupon_currency_overall = aggregate_coupon_yields(portfolio_rows)
    abnormal_yields = detect_abnormal_yields(portfolio_rows)
    abnormal_identifiers = {
        entry["SECID"]
        for entry in abnormal_yields
        if entry.get("SECID")
    } | {
        entry["ISIN"]
        for entry in abnormal_yields
        if entry.get("ISIN")
    }
    rankings = build_rankings(portfolio_rows, abnormal_identifiers)

    added_rows = 0
    for item in summary:
        added = insert_tracking_row(conn, item, creation_date)
        if added:
            added_rows += 1

    report_context = prepare_report_context(
        summary,
        overall_metrics,
        creation_date,
        defaults_info,
        distribution_rows,
        coupon_currency_overall,
        rankings,
    )
    export_to_excel(
        summary,
        overall_metrics,
        distribution_rows,
        defaults_info,
        coupon_currency_overall,
        rankings,
        abnormal_yields,
    )

    messages = build_report_messages(report_context)
    if pdf_path:
        export_report_pdf(messages, Path(pdf_path))
    if send_telegram:
        send_telegram_messages(messages)

    print(f"Inserted rows into Analiz_Portfolio: {added_rows}")
    print("Excel report saved to OUTPUT_Analiz_portfolio.xlsx")
    return messages


def main() -> None:
    run_analysis_pipeline()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        raise SystemExit(f"Analysis failed: {exc}")
