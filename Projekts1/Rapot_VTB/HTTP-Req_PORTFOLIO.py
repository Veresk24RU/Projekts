from __future__ import annotations

import logging
import sqlite3
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence, Set, Tuple

import pandas as pd
import requests

BASE_URL = "https://iss.moex.com/iss"
PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "out" / "Operation_VTB.sqlite"
FINAL_PORTFOLIO_XLSX = PROJECT_ROOT / "out" / "PORTFEL.xlsx"
CBR_RATES_URL = "https://www.cbr-xml-daily.ru/latest.js"
CBR_HEADERS = {
    "Accept": "application/json, text/javascript",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
}
LOCAL_CURRENCY_CODES = {"RUB", "RUR", "SUR"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
LOGGER = logging.getLogger("moex_portfolio")


def _normalize_isin(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def _normalize_boardid(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def _map_trade_status(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if text == "1":
        return "ТОРГ"
    if text == "0":
        return "ДЕФОЛТ"
    return None


def _map_sectype(code: object, faceunit: object | None) -> str | None:
    if code is None:
        return None
    faceunit_text = (str(faceunit).strip().upper()) if faceunit else ""
    normalized = str(code).strip().upper()

    if normalized in {"1", "2"}:
        return "Акция"
    if normalized in {"3", "5"}:
        return "ОФЗ"
    if normalized == "4":
        return "Муниципальная облигация"
    if normalized in {"6", "7", "8", "L"}:
        if faceunit_text and faceunit_text != "SUR":
            return "Еврооблигация"
        return "Корпоративная облигация"
    if normalized in {"9", "A", "B", "J", "K"}:
        return "ETF"
    if normalized == "C":
        return "Ипотечные сертификаты участия"
    if normalized == "D":
        return "Депозитарные расписки"
    if normalized == "E":
        return "Варранты"
    if normalized == "F":
        return "Еврооблигации"
    if normalized == "G":
        return "Векселя"
    if normalized == "H":
        return "Залоговые расписки"
    return "Другие ЦБ"


def _to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        if isinstance(value, str):
            value = value.replace(",", ".")
        return float(value)
    except (TypeError, ValueError):
        return None


def collect_faceunits(descriptions: List[Dict[str, object]]) -> Set[str]:
    units: Set[str] = set(LOCAL_CURRENCY_CODES)
    for row in descriptions:
        if str(row.get("name")).strip().upper() != "FACEUNIT":
            continue
        value = str(row.get("value") or "").strip().upper()
        if value:
            units.add(value)
    return units


def fetch_fx_rates(currency_codes: Iterable[str]) -> Dict[str, float]:
    normalized_codes = {
        str(code or "").strip().upper() for code in currency_codes if code
    }
    normalized_codes = {code for code in normalized_codes if code}
    if not normalized_codes:
        return {code: 1.0 for code in LOCAL_CURRENCY_CODES}

    try:
        response = requests.get(
            CBR_RATES_URL,
            headers=CBR_HEADERS,
            timeout=15,
        )
        response.raise_for_status()
        payload: Dict[str, Any] = response.json()
    except requests.RequestException as exc:
        LOGGER.warning("Не удалось получить курсы ЦБ: %s", exc)
        return {code: 1.0 for code in normalized_codes}

    base_currency = str(payload.get("base", "RUB")).strip().upper()
    rates_data = payload.get("rates")
    if not isinstance(rates_data, dict):
        LOGGER.warning("Некорректный ответ курсов ЦБ.")
        return {code: 1.0 for code in normalized_codes}

    rub_per_base = 1.0
    if base_currency != "RUB":
        rub_rate = rates_data.get("RUB")
        rub_per_base_candidate = _to_float(rub_rate)
        if not rub_per_base_candidate:
            LOGGER.warning("Не удалось вычислить курс рубля из ответа ЦБ.")
            rub_per_base = 1.0
        else:
            rub_per_base = rub_per_base_candidate

    result: Dict[str, float] = {}
    for code in normalized_codes:
        if code in LOCAL_CURRENCY_CODES:
            result[code] = 1.0
            continue

        raw_rate = rates_data.get(code)
        rate_value = _to_float(raw_rate)
        if rate_value is None:
            LOGGER.warning("Нет курса для валюты %s", code)
            continue
        if rate_value == 0:
            LOGGER.warning("Нулевой курс для валюты %s", code)
            continue

        if base_currency == "RUB":
            result[code] = 1.0 / rate_value
        else:
            result[code] = rub_per_base / rate_value

    return result


def _calculate_stoim_activ(
    price: float | None,
    last: float | None,
    facevalue: float | None,
    planned_qty: float | None,
    nkd: float | None,
    fx_rate: float | None,
) -> float | None:
    if facevalue is None or planned_qty is None:
        return None

    if price is None and last is None:
        return None
    if price is None:
        price = last
    if last is None:
        last = price
    if price is None or last is None:
        return None

    nkd_value = nkd if nkd is not None else 0.0
    fx = fx_rate if fx_rate is not None and fx_rate > 0 else 1.0

    avg_price = (price + last) / 2
    base_component = (((avg_price / 100.0) * facevalue) * planned_qty) * fx
    return base_component + nkd_value


def load_isins(db_path: Path) -> List[str]:
    query = """
        SELECT DISTINCT TRIM(Symbol)
        FROM Current_Portfolio
        WHERE Symbol IS NOT NULL
          AND TRIM(Symbol) <> ''
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        records = {_normalize_isin(row[0]) for row in cursor.fetchall() if row[0]}
    records.discard("")
    isins = sorted(records)
    if not isins:
        raise RuntimeError("Таблица Current_Portfolio не содержит ISIN.")
    return isins


class MoexClient:
    def __init__(
        self,
        base_url: str = BASE_URL,
        lang: str = "ru",
        timeout: int = 15,
        max_retries: int = 3,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._lang = lang
        self._timeout = timeout
        self._max_retries = max_retries
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": "VTB-Portfolio-Collector/1.0",
            }
        )

    def fetch_security_by_isin(self, isin: str) -> Dict[str, object] | None:
        payload = self._request(
            "/securities.json",
            params={
                "iss.meta": "off",
                "lang": self._lang,
                "q": isin,
                "securities.columns": ",".join(
                    [
                        "secid",
                        "isin",
                        "shortname",
                        "name",
                        "emitent_title",
                        "primary_boardid",
                        "marketprice_boardid",
                    ]
                ),
            },
        )
        table = payload.get("securities")
        if not table:
            return None
        columns = table.get("columns", [])
        data = table.get("data", [])
        try:
            isin_index = columns.index("isin")
        except ValueError:
            return None
        for row in data:
            if row[isin_index] == isin:
                return dict(zip(columns, row))
        return None

    def fetch_security_details(
        self, secid: str
    ) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
        payload = self._request(
            f"/securities/{secid}.json",
            params={
                "iss.meta": "off",
                "lang": self._lang,
            },
        )
        description = self._table_to_dicts(payload.get("description"))
        boards = self._table_to_dicts(payload.get("boards"))
        return description, boards

    def fetch_board_market_snapshot(
        self,
        engine: str,
        market: str,
        boardid: str,
        secid: str,
    ) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]]:
        payload = self._request(
            f"/engines/{engine}/markets/{market}/securities/{secid}.json",
            params={
                "iss.meta": "off",
                "lang": self._lang,
                "boardid": boardid,
                "securities.columns": (
                    "SECID,BOARDID,SHORTNAME,SECNAME,SECTYPE,LISTLEVEL,LATNAME,ISIN"
                ),
                "marketdata.columns": (
                    "SECID,BOARDID,LAST,YIELD,YIELDATWAPRICE,BID,OFFER,ACCRUEDINT"
                ),
            },
        )
        security_rows = self._table_to_dicts(payload.get("securities"))
        market_rows = self._table_to_dicts(payload.get("marketdata"))
        return security_rows, market_rows

    def _request(self, path: str, params: Dict[str, str]) -> Dict[str, Any]:
        url = f"{self._base_url}/{path.lstrip('/')}"
        last_exc: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                response = self._session.get(url, params=params, timeout=self._timeout)
                response.raise_for_status()
                return response.json()
            except requests.RequestException as exc:
                last_exc = exc
                sleep_for = min(5, 2**attempt)
                LOGGER.warning(
                    "Ошибка запроса %s (попытка %s/%s): %s",
                    url,
                    attempt,
                    self._max_retries,
                    exc,
                )
                time.sleep(sleep_for)
        raise RuntimeError(f"Не удалось получить данные MOEX: {last_exc}") from last_exc

    @staticmethod
    def _table_to_dicts(
        table: Mapping[str, Sequence[object]] | None,
    ) -> List[Dict[str, object]]:
        if not table:
            return []
        columns = table.get("columns", [])
        return [dict(zip(columns, row)) for row in table.get("data", [])]


def collect_moex_data(
    isins: Iterable[str],
    client: MoexClient,
) -> Tuple[
    List[Dict[str, object]],
    List[Dict[str, object]],
    List[Dict[str, object]],
    List[Dict[str, object]],
    List[Dict[str, object]],
]:
    securities: List[Dict[str, object]] = []
    descriptions: List[Dict[str, object]] = []
    boards: List[Dict[str, object]] = []
    security_meta: List[Dict[str, object]] = []
    marketdata: List[Dict[str, object]] = []

    for isin in isins:
        LOGGER.info("Обработка ISIN %s", isin)
        try:
            security_row = client.fetch_security_by_isin(isin)
        except Exception as exc:
            LOGGER.error("Не удалось получить данные по ISIN %s: %s", isin, exc)
            continue

        if not security_row:
            LOGGER.warning("MOEX не вернул данных по ISIN %s", isin)
            continue

        security_row["lookup_isin"] = isin
        securities.append(security_row)

        secid = security_row.get("secid")
        if not secid:
            LOGGER.warning("Для ISIN %s нет SECID, пропускаю подробную выгрузку", isin)
            continue

        primary_board = _normalize_boardid(security_row.get("primary_boardid"))
        marketprice_board = _normalize_boardid(security_row.get("marketprice_boardid"))
        target_boards = {board_id for board_id in (primary_board, marketprice_board) if board_id}
        if not target_boards:
            LOGGER.warning(
                "У ISIN %s (SECID %s) не указаны primary/marketprice boardid",
                isin,
                secid,
            )

        try:
            desc_rows, board_rows = client.fetch_security_details(str(secid))
        except Exception as exc:
            LOGGER.error(
                "Не удалось получить подробности для SECID %s (ISIN %s): %s",
                secid,
                isin,
                exc,
            )
            continue

        for row in desc_rows:
            row["lookup_isin"] = isin
        descriptions.extend(desc_rows)

        board_index: Dict[str, Dict[str, object]] = {}
        for board_row in board_rows:
            board_row["lookup_isin"] = isin
            boardid = _normalize_boardid(board_row.get("boardid"))
            if target_boards and boardid not in target_boards:
                continue

            boards.append(board_row)
            board_index[boardid] = board_row

        if target_boards and not board_index:
            LOGGER.warning(
                "Не удалось найти в списке досок нужные boardid %s для ISIN %s",
                ", ".join(sorted(target_boards)),
                isin,
            )

        boards_to_fetch = target_boards or {
            _normalize_boardid(row.get("boardid")) for row in board_rows
        }
        boards_to_fetch = {b for b in boards_to_fetch if b}

        for boardid in boards_to_fetch:
            board_row = board_index.get(boardid)
            if not board_row:
                board_row = next(
                    (
                        row
                        for row in board_rows
                        if _normalize_boardid(row.get("boardid")) == boardid
                    ),
                    None,
                )
                if not board_row:
                    LOGGER.warning(
                        "Нет метаданных для boardid %s (ISIN %s), пропускаю снапшот",
                        boardid,
                        isin,
                    )
                    continue
                board_row["lookup_isin"] = isin
                boards.append(board_row)
                board_index[boardid] = board_row

            engine = str(board_row.get("engine") or "").strip()
            market = str(board_row.get("market") or "").strip()
            if not (engine and market):
                LOGGER.debug(
                    "Пропускаю доску %s без engine/market (ISIN %s)",
                    boardid,
                    isin,
                )
                continue

            try:
                sec_rows, market_rows = client.fetch_board_market_snapshot(
                    engine, market, boardid, str(secid)
                )
            except Exception as exc:
                LOGGER.error(
                    "Не удалось получить marketdata для %s (%s/%s/%s): %s",
                    secid,
                    engine,
                    market,
                    boardid,
                    exc,
                )
                continue

            for row in sec_rows:
                row["lookup_isin"] = isin
                row.setdefault("BOARDID", boardid)
                row.setdefault("ENGINE", engine)
                row.setdefault("MARKET", market)
            for row in market_rows:
                row["lookup_isin"] = isin
                row.setdefault("BOARDID", boardid)
                row.setdefault("ENGINE", engine)
                row.setdefault("MARKET", market)

            security_meta.extend(sec_rows)
            marketdata.extend(market_rows)

    return securities, descriptions, boards, security_meta, marketdata


def _group_rows_by_isin(rows: List[Dict[str, object]]) -> Dict[str, List[Dict[str, object]]]:
    grouped: Dict[str, List[Dict[str, object]]] = defaultdict(list)
    for row in rows:
        isin = _normalize_isin(row.get("lookup_isin"))
        if not isin:
            continue
        grouped[isin].append(row)
    return grouped


def _select_preferred_row(
    rows: List[Dict[str, object]],
    preferred_boardids: List[str],
    require_engine_market: bool = False,
) -> Dict[str, object] | None:
    if not rows:
        return None

    def is_valid(row: Dict[str, object]) -> bool:
        if not require_engine_market:
            return True
        engine = str(row.get("ENGINE") or row.get("engine") or "").strip()
        market = str(row.get("MARKET") or row.get("market") or "").strip()
        return bool(engine and market)

    def matches(row: Dict[str, object], boardid: str) -> bool:
        return _normalize_boardid(row.get("BOARDID") or row.get("boardid")) == boardid

    for boardid in preferred_boardids:
        if not boardid:
            continue
        for row in rows:
            if matches(row, boardid) and is_valid(row):
                return row

    for row in rows:
        if is_valid(row):
            return row
    return None


def fetch_current_portfolio_rows(db_path: Path) -> List[Dict[str, object]]:
    query = """
        SELECT
            Portfolio,
            ReportDate,
            Symbol,
            PlannedQty,
            Price,
            NKD,
            PlannedValRub
        FROM Current_Portfolio
        WHERE Symbol IS NOT NULL
          AND TRIM(Symbol) <> ''
    """
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query).fetchall()
    return [dict(row) for row in rows]


def prepare_final_portfolio_records(
    current_rows: List[Dict[str, object]],
    securities: List[Dict[str, object]],
    descriptions: List[Dict[str, object]],
    boards: List[Dict[str, object]],
    security_meta: List[Dict[str, object]],
    marketdata: List[Dict[str, object]],
    fx_rates: Dict[str, float],
) -> List[Tuple[object, ...]]:
    securities_by_isin: Dict[str, Dict[str, object]] = {}
    for row in securities:
        isin = _normalize_isin(row.get("lookup_isin"))
        if isin:
            securities_by_isin[isin] = row

    description_by_isin = _group_rows_by_isin(descriptions)
    boards_by_isin = _group_rows_by_isin(boards)
    secmeta_by_isin = _group_rows_by_isin(security_meta)
    marketdata_by_isin = _group_rows_by_isin(marketdata)

    records: List[Tuple[object, ...]] = []

    for cur_row in current_rows:
        isin_norm = _normalize_isin(cur_row.get("Symbol"))
        if not isin_norm:
            continue

        security_row = securities_by_isin.get(isin_norm)
        preferred_boardids: List[str] = []
        if security_row:
            preferred_boardids = [
                _normalize_boardid(security_row.get("primary_boardid")),
                _normalize_boardid(security_row.get("marketprice_boardid")),
            ]

        desc_rows = description_by_isin.get(isin_norm, [])
        desc_map = {
            str(item.get("name")).upper(): item.get("value")
            for item in desc_rows
            if item.get("name")
        }

        board_row = _select_preferred_row(
            boards_by_isin.get(isin_norm, []),
            preferred_boardids,
            require_engine_market=False,
        )
        secmeta_row = _select_preferred_row(
            secmeta_by_isin.get(isin_norm, []),
            preferred_boardids,
            require_engine_market=True,
        )
        market_row = _select_preferred_row(
            marketdata_by_isin.get(isin_norm, []),
            preferred_boardids,
            require_engine_market=True,
        )

        faceunit = desc_map.get("FACEUNIT")
        sectype_value = (
            _map_sectype(secmeta_row.get("SECTYPE"), faceunit) if secmeta_row else None
        )
        trade_status = _map_trade_status(board_row.get("is_traded")) if board_row else None

        planned_qty = _to_float(cur_row.get("PlannedQty"))
        price_value = _to_float(cur_row.get("Price"))
        nkd_value = _to_float(cur_row.get("NKD"))
        planned_val = _to_float(cur_row.get("PlannedValRub"))
        facevalue_value = _to_float(desc_map.get("FACEVALUE"))
        last_value = _to_float(market_row.get("LAST")) if market_row else None
        yield_value = _to_float(market_row.get("YIELD")) if market_row else None
        yield_wap_value = (
            _to_float(market_row.get("YIELDATWAPRICE")) if market_row else None
        )

        faceunit_code = str(faceunit or "").strip().upper()
        fx_rate = fx_rates.get(faceunit_code)
        if fx_rate is None:
            if not faceunit_code or faceunit_code in LOCAL_CURRENCY_CODES:
                fx_rate = 1.0
            else:
                LOGGER.warning("Нет курса для валюты %s, использую 1", faceunit_code)
                fx_rate = 1.0

        if sectype_value in {"Акция", "ETF"}:
            stoim_value = planned_val
        else:
            stoim_value = _calculate_stoim_activ(
                price=price_value,
                last=last_value,
                facevalue=facevalue_value,
                planned_qty=planned_qty,
                nkd=nkd_value,
                fx_rate=fx_rate,
            )

        record = (
            cur_row.get("Portfolio"),
            cur_row.get("ReportDate"),
            sectype_value,
            (security_row.get("isin") if security_row else cur_row.get("Symbol")),
            (security_row.get("secid") if security_row else None),
            security_row.get("shortname") if security_row else None,
            planned_qty,
            security_row.get("name") if security_row else None,
            security_row.get("emitent_title") if security_row else None,
            trade_status,
            desc_map.get("HASDEFAULT"),
            desc_map.get("HASTECHNICALDEFAULT"),
            desc_map.get("ISSUEDATE"),
            desc_map.get("MATDATE"),
            faceunit,
            _to_float(desc_map.get("INITIALFACEVALUE")),
            facevalue_value,
            _to_float(desc_map.get("COUPONFREQUENCY")),
            _to_float(desc_map.get("DAYSTOREDEMPTION")),
            _to_float(desc_map.get("COUPONVALUE")),
            _to_float(desc_map.get("COUPONPERCENT")),
            last_value,
            price_value,
            yield_value,
            yield_wap_value,
            nkd_value,
            planned_val,
            stoim_value,
        )
        records.append(record)

    return records


def write_final_portfolio(records: List[Tuple[object, ...]], db_path: Path) -> None:
    table_definition = """
        CREATE TABLE Final_Portfolio (
            Portfolio TEXT,
            ReportDate TEXT,
            SECTYPE TEXT,
            isin TEXT,
            secid TEXT,
            shortname TEXT,
            PlannedQty REAL,
            name TEXT,
            emitent_title TEXT,
            is_traded TEXT,
            HASDEFAULT TEXT,
            HASTECHNICALDEFAULT TEXT,
            ISSUEDATE TEXT,
            MATDATE TEXT,
            FACEUNIT TEXT,
            INITIALFACEVALUE REAL,
            FACEVALUE REAL,
            COUPONFREQUENCY REAL,
            DAYSTOREDEMPTION REAL,
            COUPONVALUE REAL,
            COUPONPERCENT REAL,
            LAST REAL,
            Price REAL,
            YIELD REAL,
            YIELDATWAPRICE REAL,
            NKD REAL,
            PlannedValRub REAL,
            Stoim_activRUB REAL
        )
    """
    insert_sql = """
        INSERT INTO Final_Portfolio (
            Portfolio,
            ReportDate,
            SECTYPE,
            isin,
            secid,
            shortname,
            PlannedQty,
            name,
            emitent_title,
            is_traded,
            HASDEFAULT,
            HASTECHNICALDEFAULT,
            ISSUEDATE,
            MATDATE,
            FACEUNIT,
            INITIALFACEVALUE,
            FACEVALUE,
            COUPONFREQUENCY,
            DAYSTOREDEMPTION,
            COUPONVALUE,
            COUPONPERCENT,
            LAST,
            Price,
            YIELD,
            YIELDATWAPRICE,
            NKD,
            PlannedValRub,
            Stoim_activRUB
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS Final_Portfolio")
        cursor.execute(table_definition)
        cursor.executemany(insert_sql, records)
        conn.commit()


def export_final_portfolio_excel(db_path: Path, output_path: Path) -> None:
    with sqlite3.connect(db_path) as conn:
        df = pd.read_sql_query("SELECT * FROM Final_Portfolio", conn)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_excel(output_path, index=False)


def main() -> None:
    if not DB_PATH.exists():
        raise FileNotFoundError(f"База данных не найдена: {DB_PATH}")

    isins = load_isins(DB_PATH)
    LOGGER.info("Найдено %s уникальных ISIN", len(isins))

    client = MoexClient()
    securities, descriptions, boards, security_meta, marketdata = collect_moex_data(
        isins, client
    )

    if not securities:
        raise RuntimeError("Не удалось получить данные по ни одной бумаге.")

    faceunits = collect_faceunits(descriptions)
    fx_rates = fetch_fx_rates(faceunits)

    current_rows = fetch_current_portfolio_rows(DB_PATH)
    final_records = prepare_final_portfolio_records(
        current_rows,
        securities,
        descriptions,
        boards,
        security_meta,
        marketdata,
        fx_rates,
    )
    if not final_records:
        raise RuntimeError("Не удалось подготовить данные для Final_Portfolio.")

    write_final_portfolio(final_records, DB_PATH)
    export_final_portfolio_excel(DB_PATH, FINAL_PORTFOLIO_XLSX)
    LOGGER.info("Final_Portfolio обновлена и выгружена в %s", FINAL_PORTFOLIO_XLSX)


if __name__ == "__main__":
    main()
