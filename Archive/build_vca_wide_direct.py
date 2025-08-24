#!/usr/bin/env python3
"""
Build VCA WIDE dataset directly from the raw SQL output (no dependency on a pre-made long file).

- Loads DB creds from a .env in the SAME folder as this script (or use --in-csv instead of --sql).
- Executes your raw SQL file (the wide SELECT you approved) OR reads a CSV of that raw output.
- Internally constructs the "long" structure in memory (one row per question/category/value),
  then pivots it to WIDE so each (question_key[, category]) becomes a column.

Outputs:
  --out-wide : required, the WIDE CSV
  --out-long : optional, also writes the LONG CSV for auditing
  --include-dqc : include DQC flag columns in the WIDE output (suffixed)
  --col-sep : separator between question_key and category in wide column names (default "__")

Examples:
  python build_vca_wide_direct.py --sql vca_raw_extract.sql --out-wide vca_wide.csv
  python build_vca_wide_direct.py --in-csv raw_vca_extract.csv --out-wide vca_wide.csv --out-long vca_long.csv --include-dqc
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
    # Priority: DB_URL, else parts
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
# Builders (wide → in-memory long)
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

def build_per_category_rows(w: pd.Series, base_field: str, qkey: str) -> List[Dict[str, Any]]:
    rows = []
    meta = {k: w.get(k) for k in META_COLS if k in w}
    for suffix in CAT_SUFFIXES:
        col = base_field.format(suffix=suffix)
        if col in w.index:
            rows.append({
                **meta, "question_key": qkey, "category": suffix,
                "value": as_str(w.get(col)), "source": "responses", "original_field": col
            })
    return rows

def build_array_rows(w: pd.Series, col: str, qkey: str, category: Optional[str]) -> List[Dict[str, Any]]:
    rows = []
    meta = {k: w.get(k) for k in META_COLS if k in w}
    items = to_list(w.get(col))
    if not items:
        rows.append({**meta, "question_key": qkey, "category": category, "value": None,
                     "source": "responses", "original_field": col})
        return rows
    for it in items:
        rows.append({**meta, "question_key": qkey, "category": category, "value": it,
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
# Reshape (in-memory long) and pivot to wide
# -----------------------------
def reshape_to_long(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
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

        # Per-category text/numbers
        rows.extend(build_per_category_rows(w, "q_{suffix}_business_name", "q15_business_name"))
        rows.extend(build_per_category_rows(w, "q_{suffix}_bussines_address", "q16_business_address"))
        rows.extend(build_per_category_rows(w, "q_{suffix}_max_operating_capacity", "q18_max_operating_capacity"))
        rows.extend(build_per_category_rows(w, "q_{suffix}_max_storage", "q19_max_storage"))
        consumed_cols.update([f"q_{s}_business_name" for s in CAT_SUFFIXES])
        consumed_cols.update([f"q_{s}_bussines_address" for s in CAT_SUFFIXES])
        consumed_cols.update([f"q_{s}_max_operating_capacity" for s in CAT_SUFFIXES])
        consumed_cols.update([f"q_{s}_max_storage" for s in CAT_SUFFIXES])

        # Q21 (HS only)
        if "q_total_processing_per_year_hs" in w.index:
            rows.append(build_scalar_row(w, "q_total_processing_per_year_hs", "q21_total_processing_per_year", category="hs"))
            consumed_cols.add("q_total_processing_per_year_hs")

        # Q20 hullers
        rows.append(build_scalar_row(w, "q_huller_operated", "q20_hullers_operated"))
        consumed_cols.add("q_huller_operated")

        # Arrays
        rows.extend(build_array_rows(w, "q_type_coffee_sourced_json", "q22_type_of_coffee_all", category="all")); consumed_cols.add("q_type_coffee_sourced_json")
        for s in CAT_SUFFIXES:
            col = f"q_type_coffee_sourced_{s}_json"
            if col in w.index: rows.extend(build_array_rows(w, col, "q22_type_of_coffee", category=s)); consumed_cols.add(col)

        rows.extend(build_array_rows(w, "q_coffee_form_json", "q23_coffee_form_all", category="all")); consumed_cols.add("q_coffee_form_json")
        for s in CAT_SUFFIXES:
            col = f"q_coffee_form_{s}_json"
            if col in w.index: rows.extend(build_array_rows(w, col, "q23_coffee_form", category=s)); consumed_cols.add(col)

        rows.extend(build_array_rows(w, "q_recieve_coffee_from_json", "q26_receive_coffee_from_all", category="all")); consumed_cols.add("q_recieve_coffee_from_json")
        for s in CAT_SUFFIXES:
            col = f"q_recieve_coffee_from_{s}_json"
            if col in w.index: rows.extend(build_array_rows(w, col, "q26_receive_coffee_from", category=s)); consumed_cols.add(col)

        # Districts
        rows.append(build_scalar_row(w, "q_district_coffee_received", "q24_districts_received_from", category="all")); consumed_cols.add("q_district_coffee_received")
        for s in CAT_SUFFIXES:
            col = f"q_district_coffee_received_{s}"
            if col in w.index: rows.append(build_scalar_row(w, col, "q24_districts_received_from", category=s)); consumed_cols.add(col)

        # Annual kgs
        rows.append(build_scalar_row(w, "q_coffee_recieved_in_a_year_kgs", "q25_annual_kgs_received", category="all")); consumed_cols.add("q_coffee_recieved_in_a_year_kgs")
        for s in CAT_SUFFIXES:
            col = f"q_coffee_recieved_in_a_year_kgs_{s}"
            if col in w.index: rows.append(build_scalar_row(w, col, "q25_annual_kgs_received", category=s)); consumed_cols.add(col)

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

def pivot_long_to_wide(df: pd.DataFrame, include_dqc: bool, dqc_cols: List[str], col_sep: str, joiner: str) -> pd.DataFrame:
    if "response_id" not in df.columns: raise ValueError("Missing response_id")
    meta = [c for c in META_COLS if c in df.columns]

    if "category" in df.columns:
        df["__col_key__"] = df["question_key"] + np.where(
            (df["category"].isna()) | (df["category"] == "") | (df["category"] == "all"),
            "",
            col_sep + df["category"].astype(str),
        )
    else:
        df["__col_key__"] = df["question_key"]

    group_keys = meta + ["__col_key__"]
    g = (
        df.groupby(group_keys, dropna=False)["value"]
        .agg(lambda s: agg_join_distinct(s.tolist(), joiner=joiner))
        .reset_index()
    )
    wide = g.pivot(index=meta, columns="__col_key__", values="value").reset_index()
    wide.columns.name = None

    if include_dqc:
        for dqc_col in dqc_cols:
            if dqc_col not in df.columns: continue
            if dqc_col == "dq_failed_reason":
                g_dqc = df.groupby(group_keys, dropna=False)[dqc_col].agg(lambda s: agg_join_distinct(s.tolist(), joiner=";")).reset_index()
            else:
                g_dqc = df.groupby(group_keys, dropna=False)[dqc_col].agg(lambda s: agg_all_true(s.tolist())).reset_index()
            w_dqc = g_dqc.pivot(index=meta, columns="__col_key__", values=dqc_col).reset_index()
            w_dqc.columns = [(c if c in meta else f"{c}{col_sep}{dqc_col}") for c in w_dqc.columns]
            w_dqc.columns.name = None
            wide = wide.merge(w_dqc, on=meta, how="left")

    return wide


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    src = ap.add_mutually_exclusive_group(required=True)
    src.add_argument("--sql", help="Path to the raw SQL file to execute")
    src.add_argument("--in-csv", help="Path to a CSV already produced by the raw SQL")
    ap.add_argument("--out-wide", required=True, help="Path to write WIDE CSV")
    ap.add_argument("--out-long", help="Optional path to also write LONG CSV")
    ap.add_argument("--include-dqc", action="store_true", help="Append DQC flags to WIDE columns")
    ap.add_argument("--dqc-cols", nargs="*", default=[
        "dq_present","dq_valid_choice","dq_numeric_ok","dq_dependency_ok","dq_contact_ok","dq_gps_ok","dq_pass","dq_failed_reason"
    ], help="Which DQC columns to include when --include-dqc is set")
    ap.add_argument("--col-sep", default="__", help="Column name separator between question_key and category")
    ap.add_argument("--joiner", default=" | ", help="Joiner for duplicate values")
    args = ap.parse_args()

    # Load .env beside script (if present)
    script_dir = Path(__file__).resolve().parent
    load_env_from_file(script_dir / ".env")

    # Load the raw wide data
    if args.in_csv:
        df = pd.read_csv(args.in_csv, dtype=str, keep_default_na=False, na_values=[""])
    else:
        # run SQL
        sql_text = Path(args.sql).read_text(encoding="utf-8")
        engine = create_engine(build_db_url(), future=True)
        with engine.connect() as conn:
            df = pd.read_sql_query(text(sql_text), conn)

    # Build in-memory long (+ DQC), then pivot to WIDE
    long_df, dqc_df = reshape_to_long(df)
    wide = pivot_long_to_wide(dqc_df, include_dqc=args.include_dqc, dqc_cols=args.dqc_cols,
                              col_sep=args.col_sep, joiner=args.joiner)

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
