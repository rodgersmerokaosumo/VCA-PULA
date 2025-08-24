#!/usr/bin/env python3
"""
Reshape VCA wide extract to long form and run DQC checks per row.

- Input: a CSV produced by your raw SQL (wide columns as you approved earlier).
- Output:
    1) --out-long: long-format rows (one row per (response, question, value)),
    2) --out-dqc:  same rows + DQC flags and overall pass/fail.

Long format columns:
  response_id, project_id, start_time, end_time, question_key, category, value, source, original_field

DQC columns added (in the DQC file):
  dq_present, dq_valid_choice, dq_numeric_ok, dq_dependency_ok, dq_contact_ok, dq_gps_ok,
  dq_pass, dq_failed_reason

Notes
- We keep values as text; when needed for checks, we parse safely without mutating the stored value.
- Arrays are EXPLODED into multiple rows (default behaviour).
- We include generic fallback rows for any columns not explicitly mapped, so "all responses" are represented.
"""

import argparse
import ast
import json
import math
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import pandas as pd

# -----------------------------
# Configuration / Lookups
# -----------------------------

META_COLS = [
    "response_id", "project_id", "questionnaire_id", "questionnaire_id_text",
    "uai_id", "adm_2_id", "submitted_by_id", "user_id",
    "created", "modified", "date_modified", "start_time", "end_time", "is_test", "farm_id"
]

# Category suffixes seen in column names
CAT_SUFFIXES = ["gf", "hs", "wh", "mill", "shop", "store", "trader", "roaster", "exporter", "extractor", "other"]

# Labels for q13_business_category values, keyed by suffix
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

# Mapping: category flags -> suffix
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

# Allowed choices for DQC
CHOICES_Q1_TYPE_OF_VCA = {"Individual", "Registered Company", "Cooperative"}
CHOICES_Q2_POSITION = {"Owner", "Manager"}
CHOICES_Q22_TYPE = {"Arabica", "Robusta", "Does not apply", "Both"}
CHOICES_Q23_FORM = {"Red Cherries", "Kiboko", "Parchment", "DRUGAR", "FAQ (clean)", "Graded", "Roasted", "Does not apply"}
CHOICES_Q26_FROM = {"Farmers", "Trader", "Cooperative", "Exporter", "Other", "Does not apply"}

TRUTHY = {"yes", "true", "1", "y", "on", "checked"}

# -----------------------------
# Helpers
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
    """Return parsed list/dict if possible; else None if blank; else raw string."""
    if s is None or (isinstance(s, float) and math.isnan(s)):
        return None
    if isinstance(s, (list, dict)):
        return s
    text = str(s).strip()
    if text == "":
        return None
    # Try JSON first
    if (text.startswith("{") and text.endswith("}")) or (text.startswith("[") and text.endswith("]")):
        try:
            return json.loads(text)
        except Exception:
            pass
    # Try Python literal (handles single-quoted lists)
    try:
        val = ast.literal_eval(text)
        return val
    except Exception:
        return text  # leave as-is

def to_list(x: Any) -> List[str]:
    """Make a list of strings: explode list-like; wrap scalars."""
    parsed = parse_json_or_list(x)
    if parsed is None:
        return []
    if isinstance(parsed, list):
        return [as_str(v) for v in parsed if as_str(v) is not None]
    # if a dict (e.g., GPS) -> not list; return empty (we handle GPS separately)
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
        # last resort: strip non-numeric except dot
        import re
        t = re.sub(r"[^0-9.\-]", "", str(s))
        try:
            return float(t)
        except Exception:
            return None

# -----------------------------
# Row builders (wide -> long)
# -----------------------------

def build_category_rows(w: pd.Series) -> List[Dict[str, Any]]:
    """Q13 categories: from q_vca_* flags -> one row per selected category."""
    rows = []
    meta = {k: w.get(k) for k in META_COLS if k in w}
    for flag_field, suffix in CAT_FLAG_FIELDS.items():
        val = w.get(flag_field)
        if truthy(val):
            rows.append({
                **meta,
                "question_key": "q13_business_category",
                "category": suffix,
                "value": CAT_LABELS.get(suffix, suffix),
                "source": "responses",
                "original_field": flag_field,
            })
    return rows

def build_per_category_rows(w: pd.Series, base_field: str, qkey: str) -> List[Dict[str, Any]]:
    """
    Build rows for per-category columns like:
      base_field: 'q_{suffix}_business_name' or 'q_{suffix}_max_storage' ...
    qkey will be something like 'q15_business_name' or 'q18_max_operating_capacity'.
    """
    rows = []
    meta = {k: w.get(k) for k in META_COLS if k in w}
    for suffix in CAT_SUFFIXES:
        col = base_field.format(suffix=suffix)
        if col in w.index:
            value = w.get(col)
            if value is None or (isinstance(value, float) and math.isnan(value)) or str(value).strip()=="":
                # Still emit a row to allow presence check when category is selected
                rows.append({
                    **meta, "question_key": qkey, "category": suffix,
                    "value": None, "source": "responses", "original_field": col
                })
            else:
                rows.append({
                    **meta, "question_key": qkey, "category": suffix,
                    "value": as_str(value), "source": "responses", "original_field": col
                })
    return rows

def build_array_rows(w: pd.Series, col: str, qkey: str, category: Optional[str]) -> List[Dict[str, Any]]:
    """Generic builder for array-valued fields (explode)."""
    rows = []
    meta = {k: w.get(k) for k in META_COLS if k in w}
    items = to_list(w.get(col))
    if not items:
        # Emit a blank row so presence checks can run if needed
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
    val = w.get(col)
    return {
        **meta, "question_key": qkey, "category": category,
        "value": as_str(val), "source": source, "original_field": col
    }

def build_gps_rows(w: pd.Series) -> List[Dict[str, Any]]:
    rows = []
    meta = {k: w.get(k) for k in META_COLS if k in w}
    gps = parse_json_or_list(w.get("q28_vca_gps_json"))
    if isinstance(gps, dict):
        lat = as_str(gps.get("latitude"))
        lon = as_str(gps.get("longitude"))
        rows.append({**meta, "question_key": "q28_vca_gps_latitude", "category": None,
                     "value": lat, "source": "responses", "original_field": "q28_vca_gps_json"})
        rows.append({**meta, "question_key": "q28_vca_gps_longitude", "category": None,
                     "value": lon, "source": "responses", "original_field": "q28_vca_gps_json"})
    else:
        # keep raw json string (or None)
        rows.append({**meta, "question_key": "q28_vca_gps_raw", "category": None,
                     "value": as_str(w.get("q28_vca_gps_json")), "source": "responses",
                     "original_field": "q28_vca_gps_json"})
    return rows

# -----------------------------
# DQC checks
# -----------------------------

def run_dqc_for_row(long_row: Dict[str, Any], wide_row: pd.Series, selected_cats: set) -> Dict[str, Any]:
    q = long_row["question_key"]
    cat = long_row["category"]
    val = long_row["value"]

    dq_present = val is not None and str(val).strip() != ""
    dq_valid_choice = True
    dq_numeric_ok = True
    dq_dependency_ok = True
    dq_contact_ok = True
    dq_gps_ok = True
    reasons: List[str] = []

    # Choice validations
    if q == "q1_type_of_vca":
        if dq_present and val not in CHOICES_Q1_TYPE_OF_VCA:
            dq_valid_choice = False
            reasons.append("invalid_choice")
    elif q == "q2_vca_position":
        if dq_present and val not in CHOICES_Q2_POSITION:
            dq_valid_choice = False
            reasons.append("invalid_choice")
    elif q in {"q22_type_of_coffee", "q22_type_of_coffee_all"}:
        if dq_present and val not in CHOICES_Q22_TYPE:
            dq_valid_choice = False
            reasons.append("invalid_choice")
    elif q in {"q23_coffee_form", "q23_coffee_form_all"}:
        if dq_present and val not in CHOICES_Q23_FORM:
            dq_valid_choice = False
            reasons.append("invalid_choice")
    elif q in {"q26_receive_coffee_from", "q26_receive_coffee_from_all"}:
        if dq_present and val not in CHOICES_Q26_FROM:
            dq_valid_choice = False
            reasons.append("invalid_choice")

    # Numeric checks
    if q in {"q4_age"}:
        num = clean_number(val)
        ok = num is not None and 18 <= num <= 99
        if not ok:
            dq_numeric_ok = False
            reasons.append("age_out_of_range")
    if q in {"q20_hullers_operated", "q18_max_operating_capacity", "q19_max_storage", "q25_annual_kgs_received"}:
        num = clean_number(val)
        if num is None or num < 0:
            dq_numeric_ok = False
            reasons.append("invalid_number")

    # Dependencies
    #  - if registered == Yes -> tin present
    if q == "q11_is_legally_registered":
        is_reg = str(val).strip().lower() == "yes"
        if is_reg:
            tin = as_str(wide_row.get("q_tin_number"))
            if tin is None or tin == "":
                dq_dependency_ok = False
                reasons.append("missing_tin_when_registered")
    #  - if id available == Yes -> id number present
    if q == "q8_has_national_id" or q == "q_vca_id_number_available":
        has_id = str(val).strip().lower() == "yes"
        idnum = as_str(wide_row.get("q_vca_id_number") or wide_row.get("fr_id_number"))
        if has_id and not idnum:
            dq_dependency_ok = False
            reasons.append("missing_id_number_when_available")
    #  - if a category is selected -> business name/address/capacity should be provided
    if q in {"q15_business_name", "q16_business_address", "q18_max_operating_capacity", "q19_max_storage"} and cat:
        # only enforce if that category is selected
        if cat in selected_cats and not dq_present:
            dq_dependency_ok = False
            reasons.append(f"missing_value_for_selected_category:{cat}")

    # Contact sanity (basic)
    if q in {"q7_email", "q_vca_email_address"} and dq_present:
        # very minimal email check
        if "@" not in val or val.startswith("@") or val.endswith("@"):
            dq_contact_ok = False
            reasons.append("bad_email_format")
    if q in {"q6_phone_number", "q_candidate_phone", "fr_phone_number"} and dq_present:
        digits = "".join(ch for ch in str(val) if ch.isdigit())
        if not (9 <= len(digits) <= 15):
            dq_contact_ok = False
            reasons.append("bad_phone_length")

    # GPS range
    if q == "q28_vca_gps_latitude" and dq_present:
        lat = clean_number(val)
        if lat is None or not (-90 <= lat <= 90):
            dq_gps_ok = False
            reasons.append("lat_out_of_range")
    if q == "q28_vca_gps_longitude" and dq_present:
        lon = clean_number(val)
        if lon is None or not (-180 <= lon <= 180):
            dq_gps_ok = False
            reasons.append("lon_out_of_range")

    dq_pass = dq_present and dq_valid_choice and dq_numeric_ok and dq_dependency_ok and dq_contact_ok and dq_gps_ok
    failed_reason = "" if dq_pass else ";".join(reasons) if reasons else "failed"

    return {
        "dq_present": dq_present,
        "dq_valid_choice": dq_valid_choice,
        "dq_numeric_ok": dq_numeric_ok,
        "dq_dependency_ok": dq_dependency_ok,
        "dq_contact_ok": dq_contact_ok,
        "dq_gps_ok": dq_gps_ok,
        "dq_pass": dq_pass,
        "dq_failed_reason": failed_reason,
    }

# -----------------------------
# Main reshaper
# -----------------------------

def reshape_to_long(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    rows: List[Dict[str, Any]] = []

    # Build once: columns we will treat specially (to avoid duplicate fallback rows)
    consumed_cols: set = set()

    # Iterate wide rows
    for _, w in df.iterrows():
        # 1) Category selections (Q13) from flags
        selected_cats = set()
        for flag_field, suffix in CAT_FLAG_FIELDS.items():
            if truthy(w.get(flag_field)):
                selected_cats.add(suffix)
        for r in build_category_rows(w):
            rows.append(r)
        consumed_cols.update(CAT_FLAG_FIELDS.keys())

        # 2) Identity / contact (Q1..)
        id_rows = [
            build_scalar_row(w, "q_type_of_vca", "q1_type_of_vca"),
            build_scalar_row(w, "q_vca_position", "q2_vca_position"),
            build_scalar_row(w, "fr_name", "q3_full_name", source="farmer_responses"),
            build_scalar_row(w, "fr_age", "q4_age", source="farmer_responses"),
            build_scalar_row(w, "fr_gender", "q5_gender", source="farmer_responses"),
            build_scalar_row(w, "fr_phone_number", "q6_phone_number", source="farmer_responses"),
            build_scalar_row(w, "q_vca_email_address", "q7_email"),
            build_scalar_row(w, "q_vca_id_number_available", "q8_has_national_id"),
            build_scalar_row(w, "q_vca_id_number", "q9_national_id_number"),
            build_scalar_row(w, "q_legally_registered", "q11_is_legally_registered"),
            build_scalar_row(w, "q_tin_number", "q12_tin_number"),
        ]
        rows.extend(id_rows)
        consumed_cols.update(["q_type_of_vca","q_vca_position","fr_name","fr_age","fr_gender",
                              "fr_phone_number","q_vca_email_address","q_vca_id_number_available",
                              "q_vca_id_number","q_legally_registered","q_tin_number"])

        # 3) Business names / addresses / capacities / storage (per category)
        rows.extend(build_per_category_rows(w, "q_{suffix}_business_name", "q15_business_name"))
        rows.extend(build_per_category_rows(w, "q_{suffix}_bussines_address", "q16_business_address"))
        rows.extend(build_per_category_rows(w, "q_{suffix}_max_operating_capacity", "q18_max_operating_capacity"))
        rows.extend(build_per_category_rows(w, "q_{suffix}_max_storage", "q19_max_storage"))
        consumed_cols.update([f"q_{suf}_business_name" for suf in CAT_SUFFIXES])
        consumed_cols.update([f"q_{suf}_bussines_address" for suf in CAT_SUFFIXES])
        consumed_cols.update([f"q_{suf}_max_operating_capacity" for suf in CAT_SUFFIXES])
        consumed_cols.update([f"q_{suf}_max_storage" for suf in CAT_SUFFIXES])

        # 4) HS total processing per year (Q21)
        if "q_total_processing_per_year_hs" in w.index:
            rows.append(build_scalar_row(w, "q_total_processing_per_year_hs", "q21_total_processing_per_year", category="hs"))
            consumed_cols.add("q_total_processing_per_year_hs")

        # 5) General-level numeric / counts (e.g., hullers)
        rows.append(build_scalar_row(w, "q_huller_operated", "q20_hullers_operated"))
        consumed_cols.add("q_huller_operated")

        # 6) Arrays / multi-selects (explode)
        #    Q22: type coffee sourced
        rows.extend(build_array_rows(w, "q_type_coffee_sourced_json", "q22_type_of_coffee_all", category="all"))
        consumed_cols.add("q_type_coffee_sourced_json")
        for suf in CAT_SUFFIXES:
            col = f"q_type_coffee_sourced_{suf}_json"
            if col in w.index:
                rows.extend(build_array_rows(w, col, "q22_type_of_coffee", category=suf))
                consumed_cols.add(col)

        #    Q23: coffee form
        rows.extend(build_array_rows(w, "q_coffee_form_json", "q23_coffee_form_all", category="all"))
        consumed_cols.add("q_coffee_form_json")
        for suf in CAT_SUFFIXES:
            col = f"q_coffee_form_{suf}_json"
            if col in w.index:
                rows.extend(build_array_rows(w, col, "q23_coffee_form", category=suf))
                consumed_cols.add(col)

        #    Q26: receive coffee from
        rows.extend(build_array_rows(w, "q_recieve_coffee_from_json", "q26_receive_coffee_from_all", category="all"))
        consumed_cols.add("q_recieve_coffee_from_json")
        for suf in CAT_SUFFIXES:
            col = f"q_recieve_coffee_from_{suf}_json"
            if col in w.index:
                rows.extend(build_array_rows(w, col, "q26_receive_coffee_from", category=suf))
                consumed_cols.add(col)

        # 7) Districts (strings; include general + per-category)
        rows.append(build_scalar_row(w, "q_district_coffee_received", "q24_districts_received_from", category="all"))
        consumed_cols.add("q_district_coffee_received")
        for suf in CAT_SUFFIXES:
            col = f"q_district_coffee_received_{suf}"
            if col in w.index:
                rows.append(build_scalar_row(w, col, "q24_districts_received_from", category=suf))
                consumed_cols.add(col)

        # 8) Annual kgs (strings general + per-category)
        rows.append(build_scalar_row(w, "q_coffee_recieved_in_a_year_kgs", "q25_annual_kgs_received", category="all"))
        consumed_cols.add("q_coffee_recieved_in_a_year_kgs")
        for suf in CAT_SUFFIXES:
            col = f"q_coffee_recieved_in_a_year_kgs_{suf}"
            if col in w.index:
                rows.append(build_scalar_row(w, col, "q25_annual_kgs_received", category=suf))
                consumed_cols.add(col)

        # 9) GPS
        if "q28_vca_gps_json" in w.index:
            rows.extend(build_gps_rows(w))
            consumed_cols.add("q28_vca_gps_json")

        # 10) Candidate info (optional)
        rows.append(build_scalar_row(w, "q_candidate_name", "q_candidate_name"))
        rows.append(build_scalar_row(w, "q_candidate_phone", "q_candidate_phone"))
        rows.append(build_scalar_row(w, "q_candidate_id_number", "q_candidate_id_number"))
        consumed_cols.update(["q_candidate_name","q_candidate_phone","q_candidate_id_number"])

        # 11) Generic fallback for ANY other columns not consumed and not purely meta
        for col in w.index:
            if col in META_COLS or col in consumed_cols:
                continue
            # Skip obvious JSON object columns already handled (metadata)
            if col == "metadata_json":
                continue
            val = w.get(col)
            rows.append({
                **{k: w.get(k) for k in META_COLS if k in w},
                "question_key": col,
                "category": None,
                "value": as_str(val),
                "source": "responses",
                "original_field": col
            })

    long_df = pd.DataFrame(rows)

    # -----------------------------
    # DQC per row
    # -----------------------------
    # Map response_id -> selected category set for dependency checks
    resp_to_cats: Dict[Any, set] = {}
    for rid, grp in long_df[long_df["question_key"] == "q13_business_category"].groupby("response_id"):
        resp_to_cats[rid] = set(grp["category"].dropna().tolist())

    # Build DQC rows by joining with the wide frame row (for dependency lookups like TIN)
    dqc_rows = []
    wide_by_id = df.set_index("response_id", drop=False)
    for idx, r in long_df.iterrows():
        rid = r.get("response_id")
        wrow = wide_by_id.loc[rid] if rid in wide_by_id.index else pd.Series(dtype=object)
        selcats = resp_to_cats.get(rid, set())
        dqc = run_dqc_for_row(r.to_dict(), wrow, selcats)
        dqc_rows.append(dqc)
    dqc_df = pd.concat([long_df.reset_index(drop=True), pd.DataFrame(dqc_rows)], axis=1)

    return long_df, dqc_df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="input_csv", required=True, help="Path to wide CSV produced by the raw SQL")
    ap.add_argument("--out-long", required=True, help="Path to write long format CSV")
    ap.add_argument("--out-dqc", required=True, help="Path to write long format with DQC flags CSV")
    args = ap.parse_args()

    df = pd.read_csv(args.input_csv, dtype=str, keep_default_na=False, na_values=[""])
    # Convert known JSON list/object columns back to Python structures where helpful
    # (We’ll parse on-demand when building rows.)

    long_df, dqc_df = reshape_to_long(df)

    Path(args.out_long).parent.mkdir(parents=True, exist_ok=True)
    long_df.to_csv(args.out_long, index=False, encoding="utf-8")

    Path(args.out_dqc).parent.mkdir(parents=True, exist_ok=True)
    dqc_df.to_csv(args.out_dqc, index=False, encoding="utf-8")

    print(f"Long rows: {len(long_df):,}")
    print(f"DQC rows:  {len(dqc_df):,}")
    print(f"Saved: {args.out_long}")
    print(f"Saved: {args.out_dqc}")


if __name__ == "__main__":
    main()
