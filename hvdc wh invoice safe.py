# -*- coding: utf-8 -*-
"""
HVDC Warehouse Invoice – Minimal, Clean, Recover Build (v0.3)
- 문법/들여쓰기 오류 제거, try/except 정리
- 3-모드 과금(Rate / Passthrough / No-charge) 정상 작동
- 리포터(HVDCExcelReporterFinal)와 완전 호환: 일할 과금 · 창고명 정규화 · 모드/요율 테이블
- 산출: SQM_Invoice과금 / Monthly_Charges_Match / Exceptions_and_Evidence
"""
from __future__ import annotations
import pandas as pd
import numpy as np
from itertools import combinations
import re
from difflib import SequenceMatcher

# ===== 공통 상수 (리포터와 동일 기준) =====
BILLING_MODE_RATE = {"DSV Outdoor", "DSV MZP", "DSV Indoor", "DSV Al Markaz"}
BILLING_MODE_PASSTHROUGH = {"AAA Storage", "Hauler Indoor", "DHL Warehouse"}
BILLING_MODE_NO_CHARGE = {"MOSB"}

WAREHOUSE_RATES = {
    "DSV Outdoor": 18.0,
    "DSV MZP": 33.0,
    "DSV Indoor": 47.0,
    "DSV Al Markaz": 47.0,
    "AAA Storage": 0.0,
    "Hauler Indoor": 0.0,
    "DHL Warehouse": 0.0,
    "MOSB": 0.0,
}

WAREHOUSE_NAME_MAPPING = {
    "DSV Al Markaz": ["DSV Al Markaz", "DSV AlMarkaz", "Al Markaz", "AlMarkaz"],
    "DSV Indoor": ["DSV Indoor", "DSVIndoor", "Indoor"],
    "DSV Outdoor": ["DSV Outdoor", "DSVOutdoor", "Outdoor"],
    "DSV MZP": ["DSV MZP", "DSVMZP", "MZP"],
    "AAA Storage": ["AAA Storage", "AAAStorage", "AAA"],
    "Hauler Indoor": ["Hauler Indoor", "HaulerIndoor", "Hauler"],
    "DHL Warehouse": ["DHL Warehouse", "DHLWarehouse", "DHL"],
    "MOSB": ["MOSB", "MOSB Storage"],
}

def normalize_warehouse_name(name: str) -> str:
    if name is None or (isinstance(name, float) and np.isnan(name)):
        return "Unknown"
    s = str(name).strip()
    for std, variants in WAREHOUSE_NAME_MAPPING.items():
        if s in variants:
            return std
        s_low = s.lower()
        for v in variants:
            if s_low in v.lower() or v.lower() in s_low:
                return std
    return s

def get_billing_mode(wh: str) -> str:
    if wh in BILLING_MODE_RATE: return "rate"
    if wh in BILLING_MODE_PASSTHROUGH: return "passthrough"
    if wh in BILLING_MODE_NO_CHARGE: return "no-charge"
    return "unknown"

def get_rate(wh: str) -> float:
    return float(WAREHOUSE_RATES.get(wh, 0.0))

# ===== HVDC CODE 매칭을 위한 유틸리티 함수들 =====
def to_num(s): 
    return pd.to_numeric(s, errors="coerce")

def close2(a, b, tol=0.10): 
    return (a is not None) and (b is not None) and abs(a - b) <= tol

def normalize_hvdc_code(code: str) -> str:
    """HVDC 코드 정규화"""
    if not code or not isinstance(code, str):
        return ""
    normalized = str(code).strip().upper()
    normalized = re.sub(r'[^\w\-]', '', normalized)
    normalized = re.sub(r'-+', '-', normalized)
    return normalized

def split_hvdc_code(code: str):
    """HVDC CODE 분해 함수"""
    if not isinstance(code, str):
        return [None]*5
    normalized_code = normalize_hvdc_code(code)
    parts = [p.strip() for p in normalized_code.split("-") if p.strip()]
    while len(parts) < 5:
        parts.append(None)
    return parts[:5]

def extract_parts(df, col_full="HVDC CODE", p1="HVDC CODE 1", p2="HVDC CODE 2", p3="HVDC CODE 3", p4="HVDC CODE 4", p5="HVDC CODE 5"):
    """HVDC CODE 파트 추출 함수"""
    for c in [p1,p2,p3,p4,p5]:
        if c not in df.columns:
            df[c] = None
    
    def fill_row(row):
        parts = split_hvdc_code(row.get(col_full))
        for i,(cn,val) in enumerate(zip([p1,p2,p3,p4,p5], parts), start=1):
            if pd.isna(row.get(cn)) or row.get(cn) is None:
                row[cn] = val
        return row
    
    df = df.apply(fill_row, axis=1)
    
    for c in [p1,p2,p3,p4,p5]:
        df[c] = df[c].astype(str).str.strip().str.upper().replace({"NAN": None, "NONE": None})
    
    return df

def expand_combined_codes(code: str):
    """Expand combined shorthand in a single string"""
    if not isinstance(code, str) or "," not in code:
        return {code} if isinstance(code, str) else set()
    code = code.replace(" ", "")
    parts = code.split(",")
    base = parts[0]
    expanded = {base}
    
    m = re.match(r"^(.*-)(\d+)(-[A-Za-z0-9]+)?$", base)
    if not m:
        prefix = base.rsplit("-", 1)[0] + "-"
        num_suffix = base.rsplit("-", 1)[-1]
        num_m = re.match(r"^(\d+)(-[A-Za-z0-9]+)?$", num_suffix)
        if num_m:
            num = num_m.group(1)
            sub = num_m.group(2) or ""
        else:
            num = None
            sub = ""
    else:
        prefix, num, sub = m.group(1), m.group(2), (m.group(3) or "")
    
    def pad4(x):
        return f"{int(x):04d}"
    
    for token in parts[1:]:
        t = token
        if "-" in t:
            num_part, sub_part = t.split("-", 1)
            if num_part.isdigit():
                full = f"{prefix}{pad4(num_part)}-{sub_part}"
                expanded.add(full)
            else:
                expanded.add(t)
        else:
            if t.isdigit() and num is not None:
                full = f"{prefix}{pad4(t)}{sub or ''}"
                expanded.add(full)
            else:
                expanded.add(t)
    return expanded

def explode_by_pkg(df_subset):
    """패키지 단위로 데이터를 explode하여 각 패키지별 단위 데이터 생성"""
    if df_subset.empty:
        return pd.DataFrame(columns=["Pkg", "G.W(kgs)", "CBM"])
    
    exploded_rows = []
    
    for idx, row in df_subset.iterrows():
        pkg_count = int(row.get("Pkg", 0))
        if pkg_count <= 0:
            continue
            
        unit_gw = float(row.get("G.W(kgs)", 0)) / pkg_count
        unit_cbm = float(row.get("CBM", 0)) / pkg_count
        
        for pkg_idx in range(pkg_count):
            exploded_rows.append({
                "Original_Index": idx,
                "Pkg_Unit": pkg_idx + 1,
                "Pkg": 1,
                "G.W(kgs)": unit_gw,
                "CBM": unit_cbm
            })
    
    return pd.DataFrame(exploded_rows)

def exact_subset_match(pkgs_df, k, gw_tgt, cbm_tgt, tol=0.10):
    """정확한 서브셋 매칭"""
    idxs = list(pkgs_df.index)
    arr_gw  = pkgs_df["G.W(kgs)"].values
    arr_cbm = pkgs_df["CBM"].values
    for comb in combinations(range(len(idxs)), k):
        gw = float(np.sum(arr_gw[list(comb)]))
        cbm = float(np.sum(arr_cbm[list(comb)]))
        if close2(gw, gw_tgt, tol) and close2(cbm, cbm_tgt, tol):
            picked = [idxs[i] for i in comb]
            return True, picked, gw, cbm
    return False, [], None, None

def robust_greedy_local(values_gw, values_cbm, k, gw_tgt, cbm_tgt, tol=0.10, max_iter=300):
    """강화된 그리디 로컬 검색"""
    n = len(values_gw)
    if n < k or k <= 0:
        return False, [], None, None
        
    ratio_target = gw_tgt / max(cbm_tgt, 1e-6)
    ratio = values_gw / np.clip(values_cbm, 1e-6, None)
    score2 = np.abs(ratio - ratio_target)
    
    gw_norm = (values_gw / max(gw_tgt, 1e-6))
    cbm_norm = (values_cbm / max(cbm_tgt, 1e-6))
    score = np.abs(gw_norm - gw_norm.mean()) + np.abs(cbm_norm - cbm_norm.mean())
    
    combined_score = 0.6 * score2 + 0.4 * score
    
    picked_indices = list(np.argsort(combined_score)[:k])
    gw = float(values_gw[picked_indices].sum())
    cbm = float(values_cbm[picked_indices].sum())
    
    if close2(gw, gw_tgt, tol) and close2(cbm, cbm_tgt, tol):
        return True, picked_indices, gw, cbm

    def calculate_error(indices):
        if len(indices) == 0:
            return float('inf')
        gw_sum = values_gw[indices].sum()
        cbm_sum = values_cbm[indices].sum()
        return abs(gw_sum - gw_tgt) + abs(cbm_sum - cbm_tgt)

    best_indices = picked_indices.copy()
    best_error = calculate_error(np.array(best_indices))
    
    for iteration in range(max_iter):
        improved = False
        current_indices = best_indices.copy()
        
        for i in range(k):
            current_set = set(current_indices)
            out_idx = current_indices[i]
            
            best_local_error = best_error
            best_replacement = None
            
            for in_idx in range(n):
                if in_idx in current_set:
                    continue
                    
                trial_indices = current_indices.copy()
                trial_indices[i] = in_idx
                
                trial_error = calculate_error(np.array(trial_indices))
                
                if trial_error < best_local_error:
                    best_local_error = trial_error
                    best_replacement = in_idx
                    
                    if trial_error == 0:
                        break
            
            if best_replacement is not None and best_local_error < best_error:
                current_indices[i] = best_replacement
                best_error = best_local_error
                improved = True
                
                gw_sum = float(values_gw[current_indices].sum())
                cbm_sum = float(values_cbm[current_indices].sum())
                if close2(gw_sum, gw_tgt, tol) and close2(cbm_sum, cbm_tgt, tol):
                    return True, current_indices, gw_sum, cbm_sum
        
        if improved:
            best_indices = current_indices.copy()
        else:
            break
    
    final_gw = float(values_gw[best_indices].sum())
    final_cbm = float(values_cbm[best_indices].sum())
    success = close2(final_gw, gw_tgt, tol) and close2(final_cbm, cbm_tgt, tol)
    
    return success, best_indices, final_gw, final_cbm

def find_subset_match(pkgs_df, k, gw_tgt, cbm_tgt, tol=0.10):
    """서브셋 매칭"""
    N = len(pkgs_df)
    if N < k or k <= 0:
        return {"found": False, "picked": [], "sum_gw": None, "sum_cbm": None, "method": "invalid"}
    
    if N <= 18:  # MAX_EXACT_N
        ok, picked, gw, cbm = exact_subset_match(pkgs_df, k, gw_tgt, cbm_tgt, tol)
        return {"found": ok, "picked": picked, "sum_gw": gw, "sum_cbm": cbm, "method": "exact"}
    
    values_gw = pkgs_df["G.W(kgs)"].values.astype(float)
    values_cbm = pkgs_df["CBM"].values.astype(float)
    
    success, picked_indices, sum_gw, sum_cbm = robust_greedy_local(
        values_gw, values_cbm, k, gw_tgt, cbm_tgt, tol
    )
    
    picked_df_indices = [pkgs_df.index[i] for i in picked_indices] if picked_indices else []
    
    return {
        "found": success,
        "picked": picked_df_indices,
        "sum_gw": sum_gw,
        "sum_cbm": sum_cbm,
        "method": "robust-greedy-local"
    }

def find_subset_match_exploded(cand_df, k, gw_tgt, cbm_tgt, tol=0.10):
    """Exploded 패키지 단위 매칭"""
    if cand_df.empty or k <= 0:
        return {"found": False, "picked": [], "sum_gw": None, "sum_cbm": None, "method": "no-candidate-exploded"}
    
    units = explode_by_pkg(cand_df[["Pkg", "G.W(kgs)", "CBM"]])
    
    if len(units) == 0:
        return {"found": False, "picked": [], "sum_gw": None, "sum_cbm": None, "method": "no-units-exploded"}
    
    vals_gw = units["G.W(kgs)"].values.astype(float)
    vals_cbm = units["CBM"].values.astype(float)
    
    if len(units) <= 18:
        arr_gw = vals_gw
        arr_cbm = vals_cbm
        found_exact = False
        picked_indices = []
        sum_gw = sum_cbm = None
        
        for comb in combinations(range(len(arr_gw)), k):
            gw = float(np.sum(arr_gw[list(comb)]))
            cbm = float(np.sum(arr_cbm[list(comb)]))
            if close2(gw, gw_tgt, tol) and close2(cbm, cbm_tgt, tol):
                picked_indices = list(comb)
                sum_gw, sum_cbm = gw, cbm
                found_exact = True
                break
        
        return {
            "found": found_exact,
            "picked": picked_indices,
            "sum_gw": sum_gw,
            "sum_cbm": sum_cbm,
            "method": "exact-exploded"
        }
    else:
        success, picked_indices, sum_gw, sum_cbm = robust_greedy_local(
            vals_gw, vals_cbm, k, gw_tgt, cbm_tgt, tol
        )
        
        return {
            "found": success,
            "picked": picked_indices,
            "sum_gw": sum_gw,
            "sum_cbm": sum_cbm,
            "method": "robust-greedy-local-exploded"
        }

def enhanced_subset_matching(cand_df, k, gw_tgt, cbm_tgt, tol=0.10, use_exploded=True):
    """강화된 서브셋 매칭"""
    if cand_df.empty or k <= 0:
        return {"found": False, "picked": [], "sum_gw": None, "sum_cbm": None, "method": "no-candidate"}
    
    results = []
    
    result1 = find_subset_match(cand_df[["G.W(kgs)", "CBM"]], k, gw_tgt, cbm_tgt, tol)
    if result1["found"]:
        results.append(result1)
    
    if use_exploded and "Pkg" in cand_df.columns:
        result2 = find_subset_match_exploded(cand_df, k, gw_tgt, cbm_tgt, tol)
        if result2["found"]:
            results.append(result2)
    
    if not results:
        return result1
    
    exact_results = [r for r in results if "exact" in r["method"]]
    if exact_results:
        return exact_results[0]
    
    def calculate_total_error(result):
        if (not result["found"] or 
            result["sum_gw"] is None or 
            result["sum_cbm"] is None or
            len(result.get("picked", [])) == 0):
            return float('inf')
        gw_error = abs(result["sum_gw"] - gw_tgt)
        cbm_error = abs(result["sum_cbm"] - cbm_tgt)
        return gw_error + cbm_error
    
    best_result = min(results, key=calculate_total_error)
    return best_result

def load_invoice_passthrough_amounts(invoice_path: str) -> dict[tuple[str, str], float]:
    """
    인보이스 원본에서 (YYYY-MM, Warehouse) 합계 추출 (passthrough 검증용)
    - Month / Warehouse / Invoice_Amount(AED) 컬럼 스키마만 충족하면 동작
    """
    try:
        df = pd.read_excel(invoice_path, sheet_name=0)
    except Exception as e:
        print(f"⚠️ 인보이스 파일 로드 실패: {e}")
        return {}

    df = df.rename(columns={
        "Operation Date": "Month",
        "Date": "Month",
        "Invoice_Date": "Month",
        "Amount (AED)": "Invoice_Amount",
        "Amount_AED": "Invoice_Amount",
        "TOTAL": "Invoice_Amount",
    })
    if "Month" not in df.columns:
        df["Month"] = pd.NaT
    if "Invoice_Amount" not in df.columns:
        df["Invoice_Amount"] = 0.0

    wh_col = None
    for c in ["Warehouse", "Location", "Storage_Location", "WH"]:
        if c in df.columns:
            wh_col = c
            break
    if wh_col is None:
        df["Warehouse"] = "Unknown"
        wh_col = "Warehouse"

    df["Month"] = pd.to_datetime(df["Month"], errors="coerce").dt.to_period("M").astype(str)
    df["Warehouse"] = df[wh_col].map(normalize_warehouse_name)

    grp = df.groupby(["Month", "Warehouse"], dropna=False)["Invoice_Amount"].sum().reset_index()
    return { (r["Month"], r["Warehouse"]): float(r["Invoice_Amount"]) for _, r in grp.iterrows() }

# ===== Reporter 연동 (일할 과금 + 매칭/예외 시트) =====
def create_monthly_charges_match(reporter, stats: dict, invoice_df: pd.DataFrame, delta_thr=0.02) -> pd.DataFrame:
    """
    월×창고 단위 System vs Invoice 검증
    - rate: |Δ%|≤2% PASS
    - passthrough: 금액 절대 차이 < 0.5 AED PASS
    - no-charge: 청구 0원
    """
    charges = stats.get("sqm_invoice_charges", {})
    sys_rows = []
    for ym, payload in charges.items():
        for wh, v in payload.items():
            if wh == "total_monthly_charge_aed" or not isinstance(v, dict):
                continue
            sys_rows.append({
                "Month": ym,
                "Warehouse": wh,
                "Billing_Mode": v.get("billing_mode", ""),
                "System_Avg_SQM": v.get("avg_sqm", 0.0),
                "System_Rate": v.get("rate_aed", 0.0),
                "System_Amount": v.get("monthly_charge_aed", 0.0),
                "Amount_Source": v.get("amount_source", ""),
            })
    df_sys = pd.DataFrame(sys_rows)

    inv = invoice_df.rename(columns={
        "Operation Date":"Month",
        "Amount (AED)":"Invoice_Amount",
        "Amount_AED":"Invoice_Amount",
        "Invoice_Date":"Month",
        "Billed_SQM":"Invoice_SQM",
        "Rate_AED_per_SQM":"Invoice_Rate",
    }).copy()
    inv["Month"] = pd.to_datetime(inv["Month"], errors="coerce").dt.to_period("M").astype(str)
    wh_col = None
    for c in ["Warehouse","Location","Storage_Location","WH"]:
        if c in inv.columns:
            wh_col = c; break
    if wh_col is None:
        inv["Warehouse"] = "Unknown"; wh_col = "Warehouse"
    inv["Warehouse"] = inv[wh_col].map(normalize_warehouse_name)
    for col in ["Invoice_Amount","Invoice_Rate","Invoice_SQM"]:
        if col not in inv.columns: inv[col] = np.nan
    inv_grp = inv.groupby(["Month","Warehouse"], dropna=False)[["Invoice_Amount","Invoice_Rate","Invoice_SQM"]].sum().reset_index()

    m = df_sys.merge(inv_grp, on=["Month","Warehouse"], how="left", validate="one_to_one")

    m["Expected_Mode"] = m["Warehouse"].map(lambda x: get_billing_mode(x))
    m["Contract_Rate"] = m["Warehouse"].map(lambda x: get_rate(x))

    def _recalc(row):
        mode = row["Expected_Mode"]
        if mode == "rate":
            return round(float(row["System_Avg_SQM"]) * float(row["Contract_Rate"]), 2)
        if mode == "passthrough":
            return float(row["System_Amount"])
        return 0.0
    m["System_Amount_Recalc"] = m.apply(_recalc, axis=1)

    m["Invoice_Amount"] = m["Invoice_Amount"].fillna(0.0).astype(float)
    m["Δ_AED"] = m["Invoice_Amount"] - m["System_Amount_Recalc"]
    m["Δ_%"] = np.where(m["Invoice_Amount"] == 0, 0.0, m["Δ_AED"] / m["Invoice_Amount"])

    def _status(row):
        mode = row["Expected_Mode"]; inv_amt = float(row["Invoice_Amount"]); sys_amt = float(row["System_Amount_Recalc"])
        if mode == "rate":        return "PASS" if abs(row["Δ_%"]) <= delta_thr else "FAIL"
        if mode == "passthrough": return "PASS" if abs(inv_amt - sys_amt) < 0.5 else "FAIL"
        if mode == "no-charge":   return "PASS" if inv_amt == 0.0 else "FAIL"
        return "FAIL"
    m["Status"] = m.apply(_status, axis=1)

    def _reason(row):
        mode = row["Expected_Mode"]
        if mode == "unknown": return "MODE_MISSING"
        if mode == "rate":
            if not np.isnan(row.get("Invoice_Rate", np.nan)) and abs(row["Invoice_Rate"] - row["Contract_Rate"]) > 1e-6:
                return "RATE_DIFF"
            return "" if row["Status"] == "PASS" else "PRORATION_MISMATCH"
        if mode == "passthrough": return "" if row["Status"] == "PASS" else "PASSTHROUGH_MISMATCH"
        if mode == "no-charge":   return "" if row["Status"] == "PASS" else "NOCHARGE_VIOLATION"
        return ""
    m["Reason_Code"] = m.apply(_reason, axis=1)

    out_cols = [
        "Month","Warehouse","Expected_Mode","Billing_Mode",
        "System_Avg_SQM","Contract_Rate","System_Amount_Recalc",
        "Invoice_SQM","Invoice_Rate","Invoice_Amount",
        "Δ_AED","Δ_%","Status","Reason_Code","Amount_Source"
    ]
    return m[out_cols].sort_values(["Month","Warehouse"]).reset_index(drop=True)

# ===== HVDC CODE 단위 매칭 함수들 =====
def _load_invoice_code_targets(path):
    """인보이스→코드별 타깃 추출"""
    inv = pd.read_excel(path, sheet_name="Invoice_Original")
    print(f"🔍 인보이스 컬럼: {list(inv.columns)}")
    
    # 컬럼명 정규화
    inv = inv.rename(columns={
        "Operation Date": "Month",
        "HVDC CODE": "HVDC CODE",
        "No. of Pkgs": "No. of Pkgs", 
        "Weight (kg)": "Weight (kg)",
        "CBM": "CBM",
        "REV NO": "REV NO"
    })
    
    # Month 컬럼 처리
    if "Month" not in inv.columns:
        print("⚠️ Month 컬럼을 찾을 수 없습니다. 기본값 사용")
        inv["Month"] = "2024-01"
    else:
        inv["Month"] = pd.to_datetime(inv["Month"], errors="coerce").dt.to_period("M").astype(str)
    
    need = ["Month", "HVDC CODE", "No. of Pkgs", "Weight (kg)", "CBM", "REV NO"]
    for c in need:
        if c not in inv.columns: 
            inv[c] = np.nan
            print(f"⚠️ {c} 컬럼을 찾을 수 없습니다. 기본값 사용")
    
    # HVDC CODE가 있는 행만 필터링
    result = inv[need].dropna(subset=["HVDC CODE"])
    print(f"✅ HVDC CODE 타깃: {len(result)}건")
    return result

def _load_all_with_parts(all_xlsx):
    """원천 데이터 로드 + CODE 파트/유닛 준비"""
    df = pd.read_excel(all_xlsx, sheet_name=0)
    for col in ["Pkg", "G.W(kgs)", "CBM"]:
        if col not in df.columns: 
            df[col] = 0
    # CODE 파트 채우기(1..5) + 벤더 검증 플래그
    df = extract_parts(df, col_full="HVDC CODE")  # 기존 함수 사용
    return df

def _build_candidate_pool(df_all, inv_code_row):
    """코드 확장 + 후보풀 생성"""
    raw_code = str(inv_code_row["HVDC CODE"])
    p1, p2, p3, p4 = (inv_code_row.get("HVDC CODE 1"), inv_code_row.get("HVDC CODE 2"), 
                      inv_code_row.get("HVDC CODE 3"), inv_code_row.get("HVDC CODE 4"))
    expanded = expand_combined_codes(raw_code)  # "…,195,189…" → 전체 풀
    cand = df_all[
        (df_all["HVDC CODE"].isin(expanded)) |
        ((df_all["HVDC CODE 1"] == p1) & (df_all["HVDC CODE 2"] == p2) & 
         (df_all["HVDC CODE 3"] == p3) & (df_all["HVDC CODE 4"] == p4))
    ].copy()
    return cand, expanded

def _match_one_code(inv_row, cand_df, tol=0.10):
    """단위 패키지로 explode 후 서브셋 매칭"""
    # 안전한 데이터 변환
    k_raw = pd.to_numeric(inv_row["No. of Pkgs"], errors="coerce")
    k = int(k_raw) if not pd.isna(k_raw) and k_raw > 0 else 0
    
    gw_raw = pd.to_numeric(inv_row["Weight (kg)"], errors="coerce")
    gwT = float(gw_raw) if not pd.isna(gw_raw) else 0.0
    
    cb_raw = pd.to_numeric(inv_row["CBM"], errors="coerce")
    cbT = float(cb_raw) if not pd.isna(cb_raw) else 0.0
    
    # NaN 값 처리
    if k <= 0:
        return {"Match_Status": "FAIL", "Reason": "INVALID_PKG_COUNT", "Picked_List": ""}
    
    if cand_df.empty or k <= 0:
        return {"Match_Status": "FAIL", "Reason": "NO_CANDIDATE", "Picked_List": ""}

    units = explode_by_pkg(cand_df[["Pkg", "G.W(kgs)", "CBM"]])  # 패키지→유닛
    if len(units) == 0: 
        return {"Match_Status": "FAIL", "Reason": "NO_UNITS"}

    # 작은 N=정확/큰 N=강화 그리디
    res = enhanced_subset_matching(cand_df, k, gwT, cbT, tol, use_exploded=True)
    gw_ok = (res.get("sum_gw") is not None) and abs(res["sum_gw"] - gwT) <= tol
    cb_ok = (res.get("sum_cbm") is not None) and abs(res["sum_cbm"] - cbT) <= tol
    status = "PASS" if (len(res.get("picked", [])) == k and gw_ok and cb_ok) else "FAIL"

    return {
        "Match_Status": status,
        "GW_SumPicked": res.get("sum_gw"),
        "CBM_SumPicked": res.get("sum_cbm"),
        "Method": res.get("method", ""),
        "Picked_Count": len(res.get("picked", [])),
        "Reason": "" if status == "PASS" else ("PKG/GW/CBM MISMATCH")
    }

def build_hvdc_code_match(invoice_path="HVDC WH IVOICE_0921.xlsx", all_path="hvdc.xlsx", tol=0.10):
    """메인: CODE 매칭 리포트 생성"""
    print("🔍 HVDC CODE 단위 매칭 시작...")
    
    inv = _load_invoice_code_targets(invoice_path)
    print(f"✅ 인보이스 CODE 타깃 로드: {len(inv)}건")
    
    all_df = _load_all_with_parts(all_path)
    print(f"✅ 원천 데이터 로드: {len(all_df)}건")

    rows, details = [], []
    # 인보이스 HVDC CODE 라인별 수행
    for _, r in inv.iterrows():
        cand, expanded = _build_candidate_pool(all_df, r)
        res = _match_one_code(r, cand, tol=tol)

        # 안전한 데이터 변환
        pkg_count = pd.to_numeric(r["No. of Pkgs"], errors="coerce")
        pkg_count = int(pkg_count) if not pd.isna(pkg_count) else 0
        
        weight = pd.to_numeric(r["Weight (kg)"], errors="coerce")
        weight = float(weight) if not pd.isna(weight) else 0.0
        
        cbm = pd.to_numeric(r["CBM"], errors="coerce")
        cbm = float(cbm) if not pd.isna(cbm) else 0.0
        
        rows.append({
            "Month": r["Month"],
            "Invoice_RAW_CODE": r["HVDC CODE"],
            "Expanded_Set": ", ".join(sorted(expanded)) if isinstance(expanded, set) else str(expanded),
            "Invoice_Pkgs(k)": pkg_count,
            "GW_Invoice": weight,
            "CBM_Invoice": cbm,
            "Candidate_Rows(N)": len(cand),
            "Picked_Count": res.get("Picked_Count", 0),
            "GW_SumPicked": res.get("GW_SumPicked"),
            "CBM_SumPicked": res.get("CBM_SumPicked"),
            "Method": res.get("Method", ""),
            "Match_Status": res["Match_Status"],
            "Reason": res.get("Reason", ""),
            "REV_NO": r.get("REV NO", "")
        })

    df_match = pd.DataFrame(rows).sort_values(["Month", "Invoice_RAW_CODE"]).reset_index(drop=True)
    df_ex = df_match[df_match["Match_Status"] != "PASS"].copy()
    
    print(f"✅ HVDC CODE 매칭 완료: {len(df_match)}건 (PASS: {len(df_match[df_match['Match_Status']=='PASS'])}, FAIL: {len(df_ex)})")
    
    return df_match, df_ex

def create_exceptions_and_evidence(match_df: pd.DataFrame, delta_thr=0.02) -> pd.DataFrame:
    if match_df is None or match_df.empty:
        return pd.DataFrame(columns=[
            "Month","Warehouse","Expected_Mode","Contract_Rate",
            "System_Avg_SQM","System_Amount_Recalc","Invoice_Amount",
            "Δ_AED","Δ_%","Grade","Status","Reason_Code",
            "Evidence_Flow_Timeline","Evidence_Daily_Occupancy","Evidence_Source_Sheet"
        ])
    df = match_df.copy()

    def _grade(row):
        if row["Expected_Mode"] == "rate":
            if abs(row["Δ_%"]) <= delta_thr: return "PASS"
            if abs(row["Δ_%"]) <= 0.05:      return "WARN"
            return "FAIL"
        return row["Status"]
    df["Grade"] = df.apply(_grade, axis=1)

    ex = df[df["Grade"] != "PASS"].copy()
    ex["Evidence_Flow_Timeline"]   = "Flow_Timeline"
    ex["Evidence_Daily_Occupancy"] = "SQM_피벗테이블"
    ex["Evidence_Source_Sheet"]    = "원본_데이터_샘플"

    keep = [
        "Month","Warehouse","Expected_Mode","Contract_Rate",
        "System_Avg_SQM","System_Amount_Recalc","Invoice_Amount",
        "Δ_AED","Δ_%","Grade","Status","Reason_Code",
        "Evidence_Flow_Timeline","Evidence_Daily_Occupancy","Evidence_Source_Sheet"
    ]
    return ex[keep].sort_values(["Month","Warehouse"]).reset_index(drop=True)

def main():
    # 1) Reporter 불러와 시스템 통계 산출(일할 과금 포함)
    from hvdc_excel_reporter_final_sqm_rev import HVDCExcelReporterFinal  # 동일 규칙/함수 재사용 :contentReference[oaicite:4]{index=4}
    reporter = HVDCExcelReporterFinal()
    stats = reporter.calculate_warehouse_statistics()

    # 2) 인보이스 원본 로드(월/창고/금액 정규화) - Passthrough 금액 연결
    INV_PATH = "HVDC WH IVOICE_0921.xlsx"   # 현행 파일명
    try:
        invoice_df = pd.read_excel(INV_PATH, sheet_name=0)
        print(f"✅ 인보이스 파일 로드 완료: {len(invoice_df)}건")
    except Exception as e:
        raise SystemExit(f"❌ 인보이스 파일 로드 실패: {e}")

    # 3) 🔧 PATCH: 인보이스→Passthrough dict (월×창고 금액) 확정 로더
    print("💰 Passthrough 금액 딕셔너리 생성 중...")
    
    # 컬럼명 정규화 (실제 인보이스 구조에 맞게)
    invoice_df = invoice_df.rename(columns={
        "Operation Date": "Month",
        "Amount (AED)": "Invoice_Amount", 
        "Amount_AED": "Invoice_Amount",
        "TOTAL": "Invoice_Amount"
    })
    
    # 창고 열 자동 탐색 + 정규화
    wh_col = next((c for c in ["Warehouse", "Location", "Storage_Location", "WH"] if c in invoice_df.columns), None)
    if wh_col is None:
        print("⚠️ 창고 컬럼을 찾을 수 없습니다. 기본값 'Warehouse' 사용")
        invoice_df["Warehouse"] = "Unknown"
        wh_col = "Warehouse"
    else:
        print(f"✅ 창고 컬럼 발견: {wh_col}")
    
    # 창고명 정규화 적용
    invoice_df["Warehouse"] = invoice_df[wh_col].map(normalize_warehouse_name)
    
    # 월 컬럼 정규화
    invoice_df["Month"] = pd.to_datetime(invoice_df["Month"], errors="coerce").dt.to_period("M").astype(str)
    
    # (YYYY-MM, Warehouse) -> 금액 dict 생성
    passthrough = (invoice_df.groupby(["Month", "Warehouse"])["Invoice_Amount"]
                   .sum().reset_index()
                   .set_index(["Month", "Warehouse"])["Invoice_Amount"]
                   .to_dict())
    
    print(f"✅ Passthrough 금액 딕셔너리 생성 완료: {len(passthrough)}개 항목")
    
    # 4) 🔧 PATCH: 엔진에 Passthrough dict 주입
    print("🔄 일할 과금 시스템에 Passthrough 금액 주입 중...")
    stats["sqm_invoice_charges"] = reporter.calculator.calculate_monthly_invoice_charges_prorated(
        stats["processed_data"], passthrough_amounts=passthrough
    )
    print("✅ Passthrough 금액 주입 완료")

    # 5) 월×창고 매칭 시트 생성
    match_df = create_monthly_charges_match(reporter, stats, invoice_df, delta_thr=0.02)

    # 6) 예외+증빙 시트 생성
    exceptions_df = create_exceptions_and_evidence(match_df, delta_thr=0.02)

    # 7) SQM_Invoice과금 시트는 reporter 결과를 평탄화해 사용
    sqm_invoice_sheet = reporter.create_sqm_invoice_sheet(stats)

    # 8) 🔧 NEW: HVDC CODE 단위 매칭 (인보이스에 HVDC CODE가 있는 경우만)
    print("🔍 HVDC CODE 단위 매칭 시작...")
    try:
        code_match_df, code_ex_df = build_hvdc_code_match("HVDC WH IVOICE_0921.xlsx", "hvdc.xlsx", tol=0.10)
        print("✅ HVDC CODE 매칭 완료")
    except Exception as e:
        print(f"⚠️ HVDC CODE 매칭 건너뜀: {e}")
        # 빈 DataFrame 생성
        code_match_df = pd.DataFrame(columns=[
            "Month", "Invoice_RAW_CODE", "Expanded_Set", "Invoice_Pkgs(k)", 
            "GW_Invoice", "CBM_Invoice", "Candidate_Rows(N)", "Picked_Count",
            "GW_SumPicked", "CBM_SumPicked", "Method", "Match_Status", "Reason", "REV_NO"
        ])
        code_ex_df = code_match_df.copy()

    # 9) 저장
    OUT = "HVDC_Invoice_Validation_Dashboard_with_Billing_CODE_MATCH.xlsx"
    with pd.ExcelWriter(OUT, engine="xlsxwriter") as w:
        sqm_invoice_sheet.to_excel(w, sheet_name="SQM_Invoice과금", index=False)
        match_df.to_excel(w, sheet_name="Monthly_Charges_Match", index=False)
        exceptions_df.to_excel(w, sheet_name="Exceptions_and_Evidence", index=False)
        # 🔧 NEW: HVDC CODE 매칭 시트들
        code_match_df.to_excel(w, sheet_name="HVDC_Code_Match", index=False)
        code_ex_df.to_excel(w, sheet_name="Exceptions_By_Code", index=False)
    
    print(f"✅ 저장 완료: {OUT}")
    print(f"   - SQM_Invoice과금: {len(sqm_invoice_sheet)}건")
    print(f"   - Monthly_Charges_Match: {len(match_df)}건")
    print(f"   - Exceptions_and_Evidence: {len(exceptions_df)}건")
    print(f"   - HVDC_Code_Match: {len(code_match_df)}건")
    print(f"   - Exceptions_By_Code: {len(code_ex_df)}건")

if __name__ == "__main__":
    main()
