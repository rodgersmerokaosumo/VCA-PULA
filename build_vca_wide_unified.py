#!/usr/bin/env python3
"""
Build VCA WIDE (one column per question_key) directly from DB or a raw CSV.

- Executes your raw SQL (or reads a CSV) to get the WIDE (raw) table you approved.
- Internally constructs the long rows, but pivots so each question_key becomes a SINGLE column.
- Category-specific answers are aggregated into that single column (join distinct values).
- Optional: prefix category values with codes like "HS:", "GF:", etc. for clarity.

Examples:
  python build_vca_wide_unified.py --sql vca_raw_extract.sql --out-wide vca_wide_unified.csv
  python build_vca_wide_unified.py --in-csv raw_vca_extract.csv --out-wide vca_wide_unified.csv \
    --out-long vca_long.csv --include-dqc --label-categories

Env: loads DB creds from .env in same folder (DB_URL or DB_HOST/DB_PORT/DB_USER/DB_PASSWORD/DB_NAME)
"""

import os
import ast
import json
import math
import argparse
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text


# -----------------------------
# Config
# -----------------------------
META_COLS = [
    "response_id", "project_id", "questionnaire_id", "questionnaire_id_text",
    "uai_id", "adm_2_id", "submitted_by_id", "user_id",
    "created", "modified", "date_modified", "start_time", "end_time", "is_test", "farm_id"
]

CAT_SUFFIXES = ["gf", "hs", "wh", "mill", "shop", "store", "trader", "roaster", "exporter", "extractor", "other"]

CAT_LABELS = {
    "gf": "Grading facilities",
    "hs": "Hulling station",
    "wh": "Warehouses",
    "mill": "Wet mills/Pulperly",
    "shop": "Coffee shops/ brewers",
    "store": "Stores",
    "trader": "Traders",
    "roaster": "Roasters/Roasteries",
    "exporter": "Exporters",
    "extractor": "Coffee extractors",
    "other": "Other",
}

CAT_CODES = {  # short codes for prefixes
    "gf": "GF", "hs": "HS", "wh": "WH", "mill": "MILL", "shop": "SHOP",
    "store": "STORE", "trader": "TRADER", "roaster": "ROASTER",
    "exporter": "EXPORTER", "extractor": "EXTRACTOR", "other": "OTHER",
}

CAT_FLAG_FIELDS = {
    "q_vca_grading_facility": "gf",
    "q_vca_hulling_station": "hs",
    "q_vca_warehouse": "wh",
    "q_vca_mill": "mill",
    "q_vca_shop": "shop",
    "q_vca_store": "store",
    "q_vca_trader": "trader",
    "q_vca_roaster": "roaster",
    "q_vca_exporter": "exporter",
    "q_vca_extractor": "extractor",
    "q_vca_other": "other",
}

# Allowed choices (used in DQC)
CHOICES_Q1_TYPE_OF_VCA = {"Individual", "Registered Company", "Cooperative"}
CHOICES_Q2_POSITION = {"Owner", "Manager"}
CHOICES_Q22_TYPE = {"Arabica", "Robusta", "Does not apply", "Both"}
CHOICES_Q23_FORM = {"Red Cherries", "Kiboko", "Parchment", "DRUGAR", "FAQ (clean)", "Graded", "Roasted", "Does not apply"}
CHOICES_Q26_FROM = {"Farmers", "Trader", "Cooperative", "Exporter", "Other", "Does not apply"}

TRUTHY = {"yes", "true", "1", "y", "on", "checked"}


# -----------------------------
# .env + DB helpers
# -----------------------------
def load_env_from_file(env_path: Path) -> None:
    if not env_path.exists():
        return
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        k, v = k.strip(), v.strip().strip('"').strip("'")
        os.environ.setdefault(k, v)

def build_db_url() -> str:
    url = os.getenv("DB_URL")
    if url:
        return url
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("DB_USER", "postgres")
    pwd  = os.getenv("DB_PASSWORD", "postgres")
    db   = os.getenv("DB_NAME", "postgres")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


# -----------------------------
# Value helpers
# -----------------------------
def as_str(x: Any) -> Optional[str]:
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    if isinstance(x, (dict, list)):
        try:
            return json.dumps(x, ensure_ascii=False)
        except Exception:
            return str(x)
    s = str(x).strip()
    return s if s != "" else None

def parse_json_or_list(s: Any) -> Optional[Any]:
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return None
    if isinstance(s, (list, dict)):
        return s
    text = str(s).strip()
    if text == "":
        return None
    if (text.startswith("{") and text.endswith("}")) or (text.startswith("[") and text.endswith("]")):
        try:
            return json.loads(text)
        except Exception:
            pass
    try:
        return ast.literal_eval(text)
    except Exception:
        return text

def to_list(x: Any) -> List[str]:
    parsed = parse_json_or_list(x)
    if parsed is None:
        return []
    if isinstance(parsed, list):
        return [as_str(v) for v in parsed if as_str(v) is not None]
    if isinstance(parsed, dict):
        return []
    s = as_str(parsed)
    return [s] if s is not None else []

def truthy(x: Any) -> bool:
    s = as_str(x)
    return s is not None and s.lower() in TRUTHY

def clean_number(s: Optional[str]) -> Optional[float]:
    if s is None:
        return None
    txt = s.replace(",", " ").split()[0] if isinstance(s, str) else str(s)
    try:
        return float(txt)
    except Exception:
        import re
        t = re.sub(r"[^0-9.\-]", "", str(s))
        try:
            return float(t)
        except Exception:
            return None


# -----------------------------
# Build in-memory long rows
# -----------------------------
def build_category_rows(w: pd.Series) -> List[Dict[str, Any]]:
    rows = []
    meta = {k: w.get(k) for k in META_COLS if k in w}
    for flag_field, suffix in CAT_FLAG_FIELDS.items():
        val = w.get(flag_field)
        if truthy(val):
            rows.append({
                **meta, "question_key": "q13_business_category", "category": suffix,
                "value": CAT_LABELS.get(suffix, suffix), "source": "responses",
                "original_field": flag_field
            })
    return rows

def build_per_category_rows(w: pd.Series, base_field: str, qkey: str, label_categories: bool) -> List[Dict[str, Any]]:
    rows = []
    meta = {k: w.get(k) for k in META_COLS if k in w}
    for suffix in CAT_SUFFIXES:
        col = base_field.format(suffix=suffix)
        if col in w.index:
            val = as_str(w.get(col))
            if label_categories and val:
                val = f"{CAT_CODES.get(suffix, suffix).upper()}: {val}"
            rows.append({
                **meta, "question_key": qkey, "category": suffix,
                "value": val, "source": "responses", "original_field": col
            })
    return rows

def build_array_rows(w: pd.Series, col: str, qkey: str, category: Optional[str], label_categories: bool) -> List[Dict[str, Any]]:
    rows = []
    meta = {k: w.get(k) for k in META_COLS if k in w}
    items = to_list(w.get(col))
    if not items:
        rows.append({**meta, "question_key": qkey, "category": category, "value": None,
                     "source": "responses", "original_field": col})
        return rows
    for it in items:
        v = it
        if label_categories and category and category not in ("", "all"):
            v = f"{CAT_CODES.get(category, category).upper()}: {it}"
        rows.append({**meta, "question_key": qkey, "category": category, "value": v,
                     "source": "responses", "original_field": col})
    return rows

def build_scalar_row(w: pd.Series, col: str, qkey: str, category: Optional[str] = None,
                     source: str = "responses") -> Dict[str, Any]:
    meta = {k: w.get(k) for k in META_COLS if k in w}
    return {**meta, "question_key": qkey, "category": category,
            "value": as_str(w.get(col)), "source": source, "original_field": col}

def build_gps_rows(w: pd.Series) -> List[Dict[str, Any]]:
    rows = []
    meta = {k: w.get(k) for k in META_COLS if k in w}
    gps = parse_json_or_list(w.get("q28_vca_gps_json"))
    if isinstance(gps, dict):
        rows.append({**meta, "question_key": "q28_vca_gps_latitude", "category": None,
                     "value": as_str(gps.get("latitude")), "source": "responses", "original_field": "q28_vca_gps_json"})
        rows.append({**meta, "question_key": "q28_vca_gps_longitude", "category": None,
                     "value": as_str(gps.get("longitude")), "source": "responses", "original_field": "q28_vca_gps_json"})
    else:
        rows.append({**meta, "question_key": "q28_vca_gps_raw", "category": None,
                     "value": as_str(w.get("q28_vca_gps_json")), "source": "responses",
                     "original_field": "q28_vca_gps_json"})
    return rows


# -----------------------------
# DQC checks (same as before)
# -----------------------------
def run_dqc_for_row(long_row: Dict[str, Any], wide_row: pd.Series, selected_cats: set) -> Dict[str, Any]:
    q = long_row["question_key"]; cat = long_row["category"]; val = long_row["value"]
    dq_present = val is not None and str(val).strip() != ""
    dq_valid_choice = True; dq_numeric_ok = True; dq_dependency_ok = True; dq_contact_ok = True; dq_gps_ok = True
    reasons: List[str] = []

    if q == "q1_type_of_vca" and dq_present and val not in CHOICES_Q1_TYPE_OF_VCA:
        dq_valid_choice = False; reasons.append("invalid_choice")
    if q == "q2_vca_position" and dq_present and val not in CHOICES_Q2_POSITION:
        dq_valid_choice = False; reasons.append("invalid_choice")
    if q in {"q22_type_of_coffee", "q22_type_of_coffee_all"} and dq_present and val not in CHOICES_Q22_TYPE:
        dq_valid_choice = False; reasons.append("invalid_choice")
    if q in {"q23_coffee_form", "q23_coffee_form_all"} and dq_present and val not in CHOICES_Q23_FORM:
        dq_valid_choice = False; reasons.append("invalid_choice")
    if q in {"q26_receive_coffee_from", "q26_receive_coffee_from_all"} and dq_present and val not in CHOICES_Q26_FROM:
        dq_valid_choice = False; reasons.append("invalid_choice")

    if q in {"q4_age"}:
        num = clean_number(val); ok = num is not None and 18 <= num <= 99
        if not ok: dq_numeric_ok = False; reasons.append("age_out_of_range")
    if q in {"q20_hullers_operated", "q18_max_operating_capacity", "q19_max_storage", "q25_annual_kgs_received"}:
        num = clean_number(val)
        if num is None or num < 0: dq_numeric_ok = False; reasons.append("invalid_number")

    if q == "q11_is_legally_registered":
        is_reg = str(val).strip().lower() == "yes"
        if is_reg:
            tin = as_str(wide_row.get("q_tin_number"))
            if not tin: dq_dependency_ok = False; reasons.append("missing_tin_when_registered")

    if q in {"q8_has_national_id", "q_vca_id_number_available"}:
        has_id = str(val).strip().lower() == "yes"
        idnum = as_str(wide_row.get("q_vca_id_number") or wide_row.get("fr_id_number"))
        if has_id and not idnum: dq_dependency_ok = False; reasons.append("missing_id_number_when_available")

    if q in {"q15_business_name", "q16_business_address", "q18_max_operating_capacity", "q19_max_storage"} and cat:
        if cat in selected_cats and not dq_present:
            dq_dependency_ok = False; reasons.append(f"missing_value_for_selected_category:{cat}")

    if q in {"q7_email", "q_vca_email_address"} and dq_present:
        if "@" not in val or val.startswith("@") or val.endswith("@"): dq_contact_ok = False; reasons.append("bad_email_format")
    if q in {"q6_phone_number", "q_candidate_phone", "fr_phone_number"} and dq_present:
        digits = "".join(ch for ch in str(val) if ch.isdigit())
        if not (9 <= len(digits) <= 15): dq_contact_ok = False; reasons.append("bad_phone_length")

    if q == "q28_vca_gps_latitude" and dq_present:
        lat = clean_number(val)
        if lat is None or not (-90 <= lat <= 90): dq_gps_ok = False; reasons.append("lat_out_of_range")
    if q == "q28_vca_gps_longitude" and dq_present:
        lon = clean_number(val)
        if lon is None or not (-180 <= lon <= 180): dq_gps_ok = False; reasons.append("lon_out_of_range")

    dq_pass = dq_present and dq_valid_choice and dq_numeric_ok and dq_dependency_ok and dq_contact_ok and dq_gps_ok
    failed_reason = "" if dq_pass else ";".join(reasons) if reasons else "failed"

    return {
        "dq_present": dq_present, "dq_valid_choice": dq_valid_choice, "dq_numeric_ok": dq_numeric_ok,
        "dq_dependency_ok": dq_dependency_ok, "dq_contact_ok": dq_contact_ok, "dq_gps_ok": dq_gps_ok,
        "dq_pass": dq_pass, "dq_failed_reason": failed_reason,
    }


# -----------------------------
# Reshape to long, then pivot so each question_key is ONE column
# -----------------------------
def reshape_to_long(df: pd.DataFrame, label_categories: bool) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rows: List[Dict[str, Any]] = []
    consumed_cols: set = set()

    for _, w in df.iterrows():
        selected_cats = {suf for fld, suf in CAT_FLAG_FIELDS.items() if truthy(w.get(fld))}

        # Q13 categories
        rows.extend(build_category_rows(w))
        consumed_cols.update(CAT_FLAG_FIELDS.keys())

        # Identity / contact
        for spec in [
            ("q_type_of_vca","q1_type_of_vca"),
            ("q_vca_position","q2_vca_position"),
            ("fr_name","q3_full_name"),
            ("fr_age","q4_age"),
            ("fr_gender","q5_gender"),
            ("fr_phone_number","q6_phone_number"),
            ("q_vca_email_address","q7_email"),
            ("q_vca_id_number_available","q8_has_national_id"),
            ("q_vca_id_number","q9_national_id_number"),
            ("q_legally_registered","q11_is_legally_registered"),
            ("q_tin_number","q12_tin_number"),
        ]:
            col, qk = spec
            src = "farmer_responses" if col.startswith("fr_") else "responses"
            rows.append(build_scalar_row(w, col, qk, source=src))
            consumed_cols.add(col)

        # Per-category text/numbers (prefix values with category code if requested)
        rows.extend(build_per_category_rows(w, "q_{suffix}_business_name", "q15_business_name", label_categories))
        rows.extend(build_per_category_rows(w, "q_{suffix}_bussines_address", "q16_business_address", label_categories))
        rows.extend(build_per_category_rows(w, "q_{suffix}_max_operating_capacity", "q18_max_operating_capacity", label_categories))
        rows.extend(build_per_category_rows(w, "q_{suffix}_max_storage", "q19_max_storage", label_categories))
        consumed_cols.update([f"q_{s}_business_name" for s in CAT_SUFFIXES])
        consumed_cols.update([f"q_{s}_bussines_address" for s in CAT_SUFFIXES])
        consumed_cols.update([f"q_{s}_max_operating_capacity" for s in CAT_SUFFIXES])
        consumed_cols.update([f"q_{s}_max_storage" for s in CAT_SUFFIXES])

        # Q21 (HS only)
        if "q_total_processing_per_year_hs" in w.index:
            val = as_str(w.get("q_total_processing_per_year_hs"))
            if label_categories and val:
                val = f"{CAT_CODES['hs']}: {val}"
            rows.append({
                **{k: w.get(k) for k in META_COLS if k in w},
                "question_key": "q21_total_processing_per_year",
                "category": "hs",
                "value": val,
                "source": "responses",
                "original_field": "q_total_processing_per_year_hs"
            })
            consumed_cols.add("q_total_processing_per_year_hs")

        # Q20 hullers
        rows.append(build_scalar_row(w, "q_huller_operated", "q20_hullers_operated"))
        consumed_cols.add("q_huller_operated")

        # Arrays (prefix with category code if requested)
        rows.extend(build_array_rows(w, "q_type_coffee_sourced_json", "q22_type_of_coffee_all", category="all", label_categories=False)); consumed_cols.add("q_type_coffee_sourced_json")
        for s in CAT_SUFFIXES:
            col = f"q_type_coffee_sourced_{s}_json"
            if col in w.index:
                rows.extend(build_array_rows(w, col, "q22_type_of_coffee", category=s, label_categories=label_categories))
                consumed_cols.add(col)

        rows.extend(build_array_rows(w, "q_coffee_form_json", "q23_coffee_form_all", category="all", label_categories=False)); consumed_cols.add("q_coffee_form_json")
        for s in CAT_SUFFIXES:
            col = f"q_coffee_form_{s}_json"
            if col in w.index:
                rows.extend(build_array_rows(w, col, "q23_coffee_form", category=s, label_categories=label_categories))
                consumed_cols.add(col)

        rows.extend(build_array_rows(w, "q_recieve_coffee_from_json", "q26_receive_coffee_from_all", category="all", label_categories=False)); consumed_cols.add("q_recieve_coffee_from_json")
        for s in CAT_SUFFIXES:
            col = f"q_recieve_coffee_from_{s}_json"
            if col in w.index:
                rows.extend(build_array_rows(w, col, "q26_receive_coffee_from", category=s, label_categories=label_categories))
                consumed_cols.add(col)

        # Districts (prefix if requested)
        v = as_str(w.get("q_district_coffee_received"))
        rows.append(build_scalar_row(w, "q_district_coffee_received", "q24_districts_received_from", category="all"))
        consumed_cols.add("q_district_coffee_received")
        for s in CAT_SUFFIXES:
            col = f"q_district_coffee_received_{s}"
            if col in w.index:
                vv = as_str(w.get(col))
                if label_categories and vv:
                    vv = f"{CAT_CODES.get(s, s).upper()}: {vv}"
                rows.append({
                    **{k: w.get(k) for k in META_COLS if k in w},
                    "question_key": "q24_districts_received_from",
                    "category": s, "value": vv, "source": "responses", "original_field": col
                })
                consumed_cols.add(col)

        # Annual kgs (prefix if requested)
        rows.append(build_scalar_row(w, "q_coffee_recieved_in_a_year_kgs", "q25_annual_kgs_received", category="all"))
        consumed_cols.add("q_coffee_recieved_in_a_year_kgs")
        for s in CAT_SUFFIXES:
            col = f"q_coffee_recieved_in_a_year_kgs_{s}"
            if col in w.index:
                vv = as_str(w.get(col))
                if label_categories and vv:
                    vv = f"{CAT_CODES.get(s, s).upper()}: {vv}"
                rows.append({
                    **{k: w.get(k) for k in META_COLS if k in w},
                    "question_key": "q25_annual_kgs_received",
                    "category": s, "value": vv, "source": "responses", "original_field": col
                })
                consumed_cols.add(col)

        # GPS
        if "q28_vca_gps_json" in w.index:
            rows.extend(build_gps_rows(w)); consumed_cols.add("q28_vca_gps_json")

        # Candidate info
        for c, qk in [("q_candidate_name","q_candidate_name"), ("q_candidate_phone","q_candidate_phone"), ("q_candidate_id_number","q_candidate_id_number")]:
            rows.append(build_scalar_row(w, c, qk)); consumed_cols.add(c)

        # Generic fallback
        for col in w.index:
            if col in META_COLS or col in consumed_cols or col == "metadata_json":
                continue
            rows.append({**{k: w.get(k) for k in META_COLS if k in w},
                         "question_key": col, "category": None, "value": as_str(w.get(col)),
                         "source": "responses", "original_field": col})

    long_df = pd.DataFrame(rows)

    # DQC
    resp_to_cats: Dict[Any, set] = {}
    for rid, grp in long_df[long_df["question_key"] == "q13_business_category"].groupby("response_id"):
        resp_to_cats[rid] = set(grp["category"].dropna().tolist())

    dqc_rows = []
    wide_by_id = df.set_index("response_id", drop=False)
    for _, r in long_df.iterrows():
        rid = r.get("response_id")
        wrow = wide_by_id.loc[rid] if rid in wide_by_id.index else pd.Series(dtype=object)
        selcats = resp_to_cats.get(rid, set())
        dqc_rows.append(run_dqc_for_row(r.to_dict(), wrow, selcats))
    dqc_df = pd.concat([long_df.reset_index(drop=True), pd.DataFrame(dqc_rows)], axis=1)

    return long_df, dqc_df


# -----------------------------
# Pivot to ONE column per question_key
# -----------------------------
def agg_join_distinct(vals: Iterable[Any], joiner: str = " | ") -> Optional[str]:
    seen, out = set(), []
    for v in vals:
        if v is None or (isinstance(v, float) and pd.isna(v)):
            continue
        s = str(v).strip()
        if not s or s in seen:
            continue
        seen.add(s); out.append(s)
    return joiner.join(out) if out else None

def to_bool(x: Any) -> Optional[bool]:
    if isinstance(x, bool): return x
    if x is None or (isinstance(x, float) and pd.isna(x)): return None
    s = str(x).strip().lower()
    if s == "true": return True
    if s == "false": return False
    return None

def agg_all_true(vals: Iterable[Any]) -> Optional[bool]:
    has_any = False
    for v in vals:
        b = to_bool(v)
        if b is None: continue
        has_any = True
        if not b: return False
    return True if has_any else None

def pivot_questions_only(df: pd.DataFrame, include_dqc: bool, dqc_cols: List[str], joiner: str) -> pd.DataFrame:
    if "response_id" not in df.columns:
        raise ValueError("Missing response_id")
    meta = [c for c in META_COLS if c in df.columns]

    # Group by (meta..., question_key), ignore category in the column key
    group_keys = meta + ["question_key"]

    values = (
        df.groupby(group_keys, dropna=False)["value"]
          .agg(lambda s: agg_join_distinct(s.tolist(), joiner=joiner))
          .reset_index()
    )
    wide = values.pivot(index=meta, columns="question_key", values="value").reset_index()
    wide.columns.name = None

    if include_dqc:
        for dqc_col in dqc_cols:
            if dqc_col not in df.columns:
                continue
            if dqc_col == "dq_failed_reason":
                g = df.groupby(group_keys, dropna=False)[dqc_col].agg(lambda s: agg_join_distinct(s.tolist(), joiner=";")).reset_index()
            else:
                g = df.groupby(group_keys, dropna=False)[dqc_col].agg(lambda s: agg_all_true(s.tolist())).reset_index()
            w = g.pivot(index=meta, columns="question_key", values=dqc_col).reset_index()
            # Suffix DQC col names to avoid collisions with value columns
            w.columns = [c if c in meta else f"{c}__{dqc_col}" for c in w.columns]
            w.columns.name = None
            wide = wide.merge(w, on=meta, how="left")

    return wide


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--sql", help="Path to the raw SQL file to execute")
    src.add_argument("--in-csv", help="Path to a CSV already produced by the raw SQL")
    ap.add_argument("--out-wide", required=True, help="Path to write unified WIDE CSV (one col per question)")
    ap.add_argument("--out-long", help="Optional path to also write LONG CSV for audit")
    ap.add_argument("--include-dqc", action="store_true", help="Append DQC flags as extra columns (per question)")
    ap.add_argument("--dqc-cols", nargs="*", default=[
        "dq_present","dq_valid_choice","dq_numeric_ok","dq_dependency_ok","dq_contact_ok","dq_gps_ok","dq_pass","dq_failed_reason"
    ], help="Which DQC columns to include when --include-dqc is set")
    ap.add_argument("--joiner", default=" | ", help="Joiner for multiple values per question (e.g., multi-category)")
    ap.add_argument("--label-categories", action="store_true",
                    help="Prefix category-specific values inside cells, e.g., 'HS: ... | GF: ...'")
    args = ap.parse_args()

    # Load .env beside script (if present)
    script_dir = Path(__file__).resolve().parent
    load_env_from_file(script_dir / ".env")

    # Load the raw data
    if args.in_csv:
        df = pd.read_csv(args.in_csv, dtype=str, keep_default_na=False, na_values=[""])
    else:
        sql_text = Path(args.sql).read_text(encoding="utf-8")
        engine = create_engine(build_db_url(), future=True)
        with engine.connect() as conn:
            df = pd.read_sql_query(text(sql_text), conn)

    # Build long (+ DQC), then pivot to ONE column per question
    long_df, dqc_df = reshape_to_long(df, label_categories=args.label_categories)
    wide = pivot_questions_only(dqc_df, include_dqc=args.include_dqc, dqc_cols=args.dqc_cols, joiner=args.joiner)

    # Save outputs
    Path(args.out_wide).parent.mkdir(parents=True, exist_ok=True)
    wide.to_csv(args.out_wide, index=False, encoding="utf-8")
    if args.out_long:
        Path(args.out_long).parent.mkdir(parents=True, exist_ok=True)
        long_df.to_csv(args.out_long, index=False, encoding="utf-8")

    print(f"WIDE rows: {len(wide):,}, columns: {len(wide.columns):,}")
    if args.out_long:
        print(f"LONG rows: {len(long_df):,}")

if __name__ == "__main__":
    main()
