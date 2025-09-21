# Enhanced subset-matching with HVDC combined-code expansion per user's rule.
# Enhanced with ONTOLOGY-based HVDC CODE structure validation and normalization.
# 
# HVDC CODE Structure (from Ontology System):
#  - Format: HVDC-ADOPT-HE-0001 형태의 고유 식별자
#  - Part 1: HVDC (프로젝트 식별자)
#  - Part 2: ADOPT (프로젝트 타입)  
#  - Part 3: HE/SIM/SCT 등 (벤더 코드)
#  - Part 4: 0001 (숫자 ID, 4자리 패딩)
#  - Part 5: 서브 식별자 (옵션, 예: -1, -2)
#
# Examples to support:
#  - "HVDC-ADOPT-HE-0325-1, 0325-2"  → {"HVDC-ADOPT-HE-0325-1", "HVDC-ADOPT-HE-0325-2"}
#  - "HVDC-ADOPT-HE-0087,90"         → {"HVDC-ADOPT-HE-0087", "HVDC-ADOPT-HE-0090"}
#  - "HVDC-ADOPT-HE-0192,195,189,193,197,191" → expand each to 4-digit numbers with same prefix
#
# We then form the candidate pool as the union of ALL rows whose FULL code matches any expanded code
# OR whose parts(1..4) match the invoice parts (for each expanded code). Vendor filtering enhanced with ontology rules.
# Subset matching (k packages) is done on this pooled candidate set. Tolerance ±0.10.

import pandas as pd
import numpy as np
from itertools import combinations
from pathlib import Path
import re
from difflib import SequenceMatcher

# 🎯 REASON 코드 정의
REASON = {
    "RATE_DIFF": "Invoice rate ≠ Contract rate",
    "PASSTHROUGH_MISMATCH": "Passthrough amount ≠ Invoice total",
    "NOCHARGE_VIOLATION": "No-charge policy (MOSB) but invoice amount > 0",
    "PRORATION_MISMATCH": "Movement/proration mismatch (AvgSQM vs billed SQM)",
    "NAME_MISMATCH": "Warehouse name normalization mismatch",
    "MODE_MISSING": "Billing mode missing",
    "RATE_MISSING": "Contract rate missing",
}

INVOICE_PATH = "HVDC WH IVOICE_0921.xlsx"  # 사용자 지정 파일 사용
ALL_PATH     = "hvdc.xlsx"  # 현재 디렉토리의 hvdc.xlsx 파일 사용
OUT_PATH     = Path("HVDC_Invoice_Validation_Dashboard.xlsx")  # 현재 디렉토리에 저장
TOL          = 0.10
MAX_EXACT_N  = 18
USE_MONTH_FILTER = False
ALL_DATE_COL     = "입고일자"
INV_DATE_COL     = "Operation Date"
# Enhanced Vendor Classification (from Ontology System)
VENDOR_ALLOWED = {"HE", "SIM"}  # Primary vendors
VENDOR_EXTENDED = {"HE", "SIM", "SCT", "SEI", "PPL", "MOSB", "ALM", "SHU", "NIE", "ALS", "SKM", "SAS"}  # Extended vendor list

# Warehouse Classification (from Ontology)
WAREHOUSE_INDOOR = {"DSV Indoor", "DSV Al Markaz", "Hauler Indoor"}
WAREHOUSE_OUTDOOR = {"DSV Outdoor", "DSV MZP", "MOSB"}
WAREHOUSE_SITE = {"AGI", "DAS", "MIR", "SHU"}
WAREHOUSE_DANGEROUS = {"AAA Storage", "Dangerous Storage"}

# ✅ NEW: 과금 모드 분류 (v3.0-corrected와 일관성 유지)
BILLING_MODE_RATE = {"DSV Outdoor", "DSV MZP", "DSV Indoor", "DSV Al Markaz"}
BILLING_MODE_PASSTHROUGH = {"AAA Storage", "Hauler Indoor", "DHL Warehouse"}
BILLING_MODE_NO_CHARGE = {"MOSB"}

# ✅ NEW: 계약 단가 (AED/sqm/month) - Rate 모드만 적용
WAREHOUSE_RATES = {
    'DSV Outdoor': 18.0,
    'DSV MZP': 33.0,
    'DSV Indoor': 47.0,
    'DSV Al Markaz': 47.0,
    'AAA Storage': 0.0,     # Passthrough
    'Hauler Indoor': 0.0,   # Passthrough  
    'DHL Warehouse': 0.0,   # Passthrough
    'MOSB': 0.0,           # No-charge
}

# ✅ NEW: 창고명 정규화 매핑 (스펠링/공백 일치 강제)
WAREHOUSE_NAME_MAPPING = {
    # 표준명 → 변형명들
    'DSV Al Markaz': ['DSV Al Markaz', 'DSV AlMarkaz', 'Al Markaz', 'AlMarkaz'],
    'DSV Indoor': ['DSV Indoor', 'DSVIndoor', 'Indoor'],
    'DSV Outdoor': ['DSV Outdoor', 'DSVOutdoor', 'Outdoor'],
    'DSV MZP': ['DSV MZP', 'DSVMZP', 'MZP'],
    'AAA Storage': ['AAA Storage', 'AAAStorage', 'AAA'],
    'Hauler Indoor': ['Hauler Indoor', 'HaulerIndoor', 'Hauler'],
    'DHL Warehouse': ['DHL Warehouse', 'DHLWarehouse', 'DHL'],
    'MOSB': ['MOSB', 'MOSB Storage']
}

def normalize_warehouse_name(warehouse_name: str) -> str:
    """
    창고명을 표준명으로 정규화
    
    Args:
        warehouse_name: 원본 창고명
    Returns:
        str: 정규화된 표준 창고명
    """
    if not warehouse_name or pd.isna(warehouse_name):
        return 'Unknown'
    
    warehouse_name = str(warehouse_name).strip()
    
    # 정확한 매칭 먼저 시도
    for standard_name, variants in WAREHOUSE_NAME_MAPPING.items():
        if warehouse_name in variants:
            return standard_name
    
    # 부분 매칭 시도 (대소문자 무시)
    warehouse_name_lower = warehouse_name.lower()
    for standard_name, variants in WAREHOUSE_NAME_MAPPING.items():
        for variant in variants:
            if warehouse_name_lower in variant.lower() or variant.lower() in warehouse_name_lower:
                return standard_name
    
    # 매칭 실패시 원본 반환
    return warehouse_name

def to_num(s): 
    return pd.to_numeric(s, errors="coerce")

def close2(a, b, tol=TOL): 
    return (a is not None) and (b is not None) and abs(a - b) <= tol

# Ontology-based Utility Functions
def normalize_hvdc_code(code: str) -> str:
    """
    HVDC 코드 정규화 (온톨로지 시스템 기반)
    
    Args:
        code: 정규화할 코드 문자열
    Returns:
        str: 정규화된 코드
    """
    if not code or not isinstance(code, str):
        return ""
    
    # 공백 제거 및 대문자로 변환
    normalized = str(code).strip().upper()
    
    # 특수문자 제거 (하이픈 제외)
    normalized = re.sub(r'[^\w\-]', '', normalized)
    
    # 연속된 하이픈을 하나로 통합
    normalized = re.sub(r'-+', '-', normalized)
    
    return normalized

def normalize_code_num(code: str) -> str:
    """HVDC CODE 숫자 부분 정규화 (예: 0014, 014, 14 → 14)"""
    if not isinstance(code, str): 
        code = str(code)
    m = re.search(r'(\d+)$', code)
    return str(int(m.group(1))) if m else code

def codes_match(code1: str, code2: str, threshold: float = 0.9) -> bool:
    """
    HVDC 코드 매칭 - 유사도 기반 (온톨로지 시스템 기반)
    
    Args:
        code1, code2: 비교할 코드들
        threshold: 매칭 임계값
    Returns:
        bool: 매칭 여부
    """
    if not code1 or not code2:
        return False
    
    # 코드 정규화
    norm_code1 = normalize_hvdc_code(code1)
    norm_code2 = normalize_hvdc_code(code2)
    
    # 완전 일치 확인
    if norm_code1 == norm_code2:
        return True
    
    # 유사도 계산
    similarity = SequenceMatcher(None, norm_code1, norm_code2).ratio()
    return similarity >= threshold

def is_valid_hvdc_vendor(vendor_code: str, extended_mode: bool = False) -> bool:
    """
    HVDC 벤더 코드 유효성 검증 (온톨로지 시스템 기반)
    
    Args:
        vendor_code: 검증할 벤더 코드
        extended_mode: 확장 벤더 목록 사용 여부
    Returns:
        bool: 유효성 여부
    """
    if not vendor_code or not isinstance(vendor_code, str):
        return False
        
    vendor_upper = str(vendor_code).strip().upper()
    
    if extended_mode:
        return vendor_upper in VENDOR_EXTENDED
    else:
        return vendor_upper in VENDOR_ALLOWED

def classify_warehouse_type(location: str) -> str:
    """
    창고 위치 분류 (온톨로지 시스템 기반)
    
    Args:
        location: 창고 위치명
    Returns:
        str: 창고 타입 (Indoor/Outdoor/Site/Dangerous/Unknown)
    """
    if not location:
        return "Unknown"
        
    location_upper = str(location).strip().upper()
    
    if any(wh.upper() in location_upper for wh in WAREHOUSE_INDOOR):
        return "Indoor"
    elif any(wh.upper() in location_upper for wh in WAREHOUSE_OUTDOOR):
        return "Outdoor"
    elif any(wh.upper() in location_upper for wh in WAREHOUSE_SITE):
        return "Site"
    elif any(wh.upper() in location_upper for wh in WAREHOUSE_DANGEROUS):
        return "Dangerous"
    else:
        return "Unknown"

def get_billing_mode(warehouse: str) -> str:
    """
    ✅ NEW: 창고별 과금 모드 분류 함수
    
    Args:
        warehouse: 창고명
    Returns:
        str: 과금 모드 ('rate'/'passthrough'/'no-charge'/'unknown')
    """
    if not warehouse:
        return "unknown"
        
    warehouse_clean = str(warehouse).strip()
    
    if warehouse_clean in BILLING_MODE_RATE:
        return "rate"
    elif warehouse_clean in BILLING_MODE_PASSTHROUGH:
        return "passthrough"
    elif warehouse_clean in BILLING_MODE_NO_CHARGE:
        return "no-charge"
    else:
        return "unknown"

def get_warehouse_rate(warehouse: str) -> float:
    """
    ✅ NEW: 창고별 계약 단가 조회 함수
    
    Args:
        warehouse: 창고명  
    Returns:
        float: 계약 단가 (AED/sqm/month), 없으면 0.0
    """
    return WAREHOUSE_RATES.get(warehouse, 0.0)

def load_invoice_passthrough_amounts(invoice_path: str) -> dict:
    """
    ✅ NEW: 인보이스에서 Passthrough 창고의 월별 총액을 로드 (실제 인보이스 컬럼 사용)
    
    Args:
        invoice_path: 인보이스 Excel 파일 경로
    Returns:
        dict: {(YYYY-MM, Warehouse): total_amount} 형태
    """
    try:
        # 인보이스 파일 로드
        df = pd.read_excel(invoice_path, sheet_name=0)
        
        # 컬럼명 정규화 (실제 인보이스 구조에 맞게)
        df = df.rename(columns={
            'Amount_AED': 'Invoice_Amount',
            'Amount (AED)': 'Invoice_Amount', 
            'Total_Amount': 'Invoice_Amount',
            'Operation Date': 'Month',
            'Date': 'Month',
            'Invoice_Date': 'Month'
        })
        
        # 월 컬럼을 YYYY-MM 형식으로 정규화
        if 'Month' in df.columns:
            df['Month'] = pd.to_datetime(df['Month'], errors='coerce').dt.to_period('M').astype(str)
        else:
            print("⚠️ 월 컬럼을 찾을 수 없습니다. 기본값 사용")
            df['Month'] = '2024-01'  # 기본값
        
        # 창고 컬럼 확인 및 정규화
        warehouse_col = None
        for col in ['Warehouse', 'Location', 'Storage_Location', 'WH']:
            if col in df.columns:
                warehouse_col = col
                break
        
        if warehouse_col is None:
            print("⚠️ 창고 컬럼을 찾을 수 없습니다. 기본값 사용")
            df['Warehouse'] = 'Unknown'
            warehouse_col = 'Warehouse'
        
        # Invoice_Amount 컬럼 확인
        if 'Invoice_Amount' not in df.columns:
            print("⚠️ Invoice_Amount 컬럼을 찾을 수 없습니다. 기본값 사용")
            df['Invoice_Amount'] = 0.0
        
        # 월×창고별 총액 집계
        passthrough_dict = {}
        grp = df.groupby(['Month', warehouse_col], dropna=False)['Invoice_Amount'].sum().reset_index()
        
        # dict 형태로 변환: {(YYYY-MM, Warehouse): amount}
        for _, row in grp.iterrows():
            month = row['Month']
            warehouse = row[warehouse_col]
            amount = float(row['Invoice_Amount'])
            
            if pd.notna(month) and pd.notna(warehouse) and amount > 0:
                passthrough_dict[(month, warehouse)] = amount
        
        print(f"✅ Passthrough 금액 로딩 완료: {len(passthrough_dict)}개 항목")
        return passthrough_dict
        
    except Exception as e:
        print(f"⚠️ 인보이스 passthrough 금액 로딩 실패: {e}")
        return {}

def split_hvdc_code(code: str):
    """
    Enhanced HVDC CODE 분해 함수 (온톨로지 시스템 기반)
    
    Format: HVDC-ADOPT-HE-0325-1 → ["HVDC","ADOPT","HE","0325","1"]
    - Part 1: HVDC (프로젝트 식별자)
    - Part 2: ADOPT (프로젝트 타입)
    - Part 3: HE/SIM/SCT 등 (벤더 코드)
    - Part 4: 0325 (숫자 ID, 4자리 패딩)
    - Part 5: 1 (서브 식별자, 옵션)
    """
    if not isinstance(code, str):
        return [None]*5
    
    # 코드 정규화 적용
    normalized_code = normalize_hvdc_code(code)
    parts = [p.strip() for p in normalized_code.split("-") if p.strip()]
    
    # 5개 파트로 맞춤 (부족한 경우 None으로 채움)
    while len(parts) < 5:
        parts.append(None)
    
    return parts[:5]

def extract_parts(df, col_full="HVDC CODE", p1="HVDC CODE 1", p2="HVDC CODE 2", p3="HVDC CODE 3", p4="HVDC CODE 4", p5="HVDC CODE 5"):
    """
    Enhanced HVDC CODE 파트 추출 함수 (온톨로지 시스템 기반)
    정규화 및 유효성 검증 포함
    """
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
    
    # 정규화 및 유효성 검증
    for c in [p1,p2,p3,p4,p5]:
        df[c] = df[c].astype(str).str.strip().str.upper().replace({"NAN": None, "NONE": None})
    
    # 벤더 코드 (Part 3) 유효성 검증 - 확장 모드 사용
    if p3 in df.columns:
        df[f"{p3}_VALID"] = df[p3].apply(lambda x: is_valid_hvdc_vendor(x, extended_mode=True))
    
    # 창고 위치 분류 추가 (필요시)
    if 'Location' in df.columns:
        df['WAREHOUSE_TYPE'] = df['Location'].apply(classify_warehouse_type)
    
    return df

def expand_combined_codes(code: str):
    """
    Expand combined shorthand in a single string.
    Returns a set of full codes.
    """
    if not isinstance(code, str) or "," not in code:
        return {code} if isinstance(code, str) else set()
    code = code.replace(" ", "")
    # Split by comma
    parts = code.split(",")
    base = parts[0]  # full first token
    expanded = {base}
    # Base prefix up to last '-' before number-ish segment
    # Find the numeric block (with optional sub-suffix like -1)
    m = re.match(r"^(.*-)(\d+)(-[A-Za-z0-9]+)?$", base)
    if not m:
        # Try a simpler: up to last '-' then remainder
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
    # Pad helper
    def pad4(x):
        return f"{int(x):04d}"
    for token in parts[1:]:
        # token could be like '0325-2' or '90' or '0090' or '195'
        t = token
        # If token includes '-', treat entire numeric-sub pattern
        if "-" in t:
            # assume full replacement numeric-sub
            num_part, sub_part = t.split("-", 1)
            if num_part.isdigit():
                full = f"{prefix}{pad4(num_part)}-{sub_part}"
                expanded.add(full)
            else:
                # fallback: just add as-is if looks full
                expanded.add(t)
        else:
            # purely digits => replace number, keep sub from base (if any? usually none)
            if t.isdigit() and num is not None:
                full = f"{prefix}{pad4(t)}{sub or ''}"
                expanded.add(full)
            else:
                expanded.add(t)
    return expanded

def explode_by_pkg(df_subset):
    """
    패키지 단위로 데이터를 explode하여 각 패키지별 단위 데이터 생성
    (ONTOLOGY 기반 개선)
    
    Args:
        df_subset: DataFrame with Pkg, G.W(kgs), CBM columns
        
    Returns:
        DataFrame: exploded data with unit weights/volumes per package
    """
    if df_subset.empty:
        return pd.DataFrame(columns=["Pkg", "G.W(kgs)", "CBM"])
    
    exploded_rows = []
    
    for idx, row in df_subset.iterrows():
        pkg_count = int(row.get("Pkg", 0))
        if pkg_count <= 0:
            continue
            
        unit_gw = float(row.get("G.W(kgs)", 0)) / pkg_count
        unit_cbm = float(row.get("CBM", 0)) / pkg_count
        
        # 각 패키지별로 단위 데이터 생성
        for pkg_idx in range(pkg_count):
            exploded_rows.append({
                "Original_Index": idx,
                "Pkg_Unit": pkg_idx + 1,
                "Pkg": 1,  # 각 단위는 1 패키지
                "G.W(kgs)": unit_gw,
                "CBM": unit_cbm
            })
    
    return pd.DataFrame(exploded_rows)

def exact_subset_match(pkgs_df, k, gw_tgt, cbm_tgt, tol=TOL):
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

def greedy_init(pkgs_df, k, gw_tgt, cbm_tgt):
    w = pkgs_df["G.W(kgs)"].values
    c = pkgs_df["CBM"].values
    ratio_target = gw_tgt / max(cbm_tgt, 1e-6)
    ratio = w / np.clip(c, 1e-6, None)
    score2 = np.abs(ratio - ratio_target)
    gw_norm = (w / max(gw_tgt, 1e-6))
    cbm_norm = (c / max(cbm_tgt, 1e-6))
    score = np.abs(gw_norm - gw_norm.mean()) + np.abs(cbm_norm - cbm_norm.mean())
    s = 0.6*score2 + 0.4*score
    picked = list(pkgs_df.assign(_s=s).sort_values("_s").index[:k])
    gw_sum = float(pkgs_df.loc[picked, "G.W(kgs)"].sum())
    cbm_sum = float(pkgs_df.loc[picked, "CBM"].sum())
    return picked, gw_sum, cbm_sum

def local_swap_improve(pkgs_df, picked, gw_tgt, cbm_tgt, tol=TOL, max_iter=400):
    picked = list(picked)
    all_idx = set(pkgs_df.index)
    cur_gw  = float(pkgs_df.loc[picked, "G.W(kgs)"].sum())
    cur_cbm = float(pkgs_df.loc[picked, "CBM"].sum())
    def err(gw, cbm): return abs(gw - gw_tgt) + abs(cbm - cbm_tgt)
    best_err = err(cur_gw, cur_cbm)
    for _ in range(max_iter):
        improved = False
        for out_i in list(picked):
            if out_i not in picked: 
                continue
            for in_i in list(all_idx - set(picked)):
                new_gw  = cur_gw  - pkgs_df.at[out_i, "G.W(kgs)"] + pkgs_df.at[in_i, "G.W(kgs)"]
                new_cbm = cur_cbm - pkgs_df.at[out_i, "CBM"]      + pkgs_df.at[in_i, "CBM"]
                new_err = err(new_gw, new_cbm)
                if new_err < best_err:
                    new_picked = picked.copy()
                    if out_i in new_picked:
                        new_picked.remove(out_i)
                        new_picked.append(in_i)
                        picked = new_picked
                        cur_gw, cur_cbm, best_err = new_gw, new_cbm, new_err
                        improved = True
                        if close2(cur_gw, gw_tgt, tol) and close2(cur_cbm, cbm_tgt, tol):
                            return picked, cur_gw, cur_cbm
            if improved: break
        if not improved: break
    return picked, cur_gw, cur_cbm

def robust_greedy_local(values_gw, values_cbm, k, gw_tgt, cbm_tgt, tol=TOL, max_iter=300):
    """
    Enhanced robust greedy local search (ONTOLOGY 기반 개선)
    기존 greedy_local 함수의 mutation 문제를 해결한 robust 버전
    
    Args:
        values_gw, values_cbm: numpy arrays of weights/volumes
        k: number of items to select
        gw_tgt, cbm_tgt: target weights/volumes
        tol: tolerance for matching
        max_iter: maximum iterations for local search
        
    Returns:
        tuple: (success, picked_indices, sum_gw, sum_cbm)
    """
    n = len(values_gw)
    if n < k or k <= 0:
        return False, [], None, None
        
    # Enhanced scoring system with ontology-based weights
    ratio_target = gw_tgt / max(cbm_tgt, 1e-6)
    ratio = values_gw / np.clip(values_cbm, 1e-6, None)
    score2 = np.abs(ratio - ratio_target)
    
    # Normalized scores for balance
    gw_norm = (values_gw / max(gw_tgt, 1e-6))
    cbm_norm = (values_cbm / max(cbm_tgt, 1e-6))
    score = np.abs(gw_norm - gw_norm.mean()) + np.abs(cbm_norm - cbm_norm.mean())
    
    # Combined scoring with enhanced weights
    combined_score = 0.6 * score2 + 0.4 * score
    
    # Initial greedy selection - immutable approach
    picked_indices = list(np.argsort(combined_score)[:k])
    gw = float(values_gw[picked_indices].sum())
    cbm = float(values_cbm[picked_indices].sum())
    
    # Early return if already optimal
    if close2(gw, gw_tgt, tol) and close2(cbm, cbm_tgt, tol):
        return True, picked_indices, gw, cbm

    def calculate_error(indices):
        """Calculate total error for given indices"""
        if len(indices) == 0:
            return float('inf')
        gw_sum = values_gw[indices].sum()
        cbm_sum = values_cbm[indices].sum()
        return abs(gw_sum - gw_tgt) + abs(cbm_sum - cbm_tgt)

    # Robust local search with immutable operations
    best_indices = picked_indices.copy()
    best_error = calculate_error(np.array(best_indices))
    
    for iteration in range(max_iter):
        improved = False
        current_indices = best_indices.copy()
        
        # Try swapping each selected item
        for i in range(k):
            current_set = set(current_indices)
            out_idx = current_indices[i]
            
            best_local_error = best_error
            best_replacement = None
            
            # Try all possible replacements
            for in_idx in range(n):
                if in_idx in current_set:
                    continue
                    
                # Create trial indices without mutation
                trial_indices = current_indices.copy()
                trial_indices[i] = in_idx
                
                trial_error = calculate_error(np.array(trial_indices))
                
                if trial_error < best_local_error:
                    best_local_error = trial_error
                    best_replacement = in_idx
                    
                    # Early termination if perfect match found
                    if trial_error == 0:
                        break
            
            # Apply best improvement if found
            if best_replacement is not None and best_local_error < best_error:
                current_indices[i] = best_replacement
                best_error = best_local_error
                improved = True
                
                # Check if target reached
                gw_sum = float(values_gw[current_indices].sum())
                cbm_sum = float(values_cbm[current_indices].sum())
                if close2(gw_sum, gw_tgt, tol) and close2(cbm_sum, cbm_tgt, tol):
                    return True, current_indices, gw_sum, cbm_sum
        
        # Update best solution if improved
        if improved:
            best_indices = current_indices.copy()
        else:
            break  # No improvement found, terminate
    
    # Final calculation
    final_gw = float(values_gw[best_indices].sum())
    final_cbm = float(values_cbm[best_indices].sum())
    success = close2(final_gw, gw_tgt, tol) and close2(final_cbm, cbm_tgt, tol)
    
    return success, best_indices, final_gw, final_cbm

def find_subset_match(pkgs_df, k, gw_tgt, cbm_tgt, tol=TOL):
    """
    Enhanced subset matching with robust algorithms (ONTOLOGY 기반)
    """
    N = len(pkgs_df)
    if N < k or k <= 0:
        return {"found": False, "picked": [], "sum_gw": None, "sum_cbm": None, "method": "invalid"}
    
    # For small datasets, use exact matching
    if N <= MAX_EXACT_N:
        ok, picked, gw, cbm = exact_subset_match(pkgs_df, k, gw_tgt, cbm_tgt, tol)
        return {"found": ok, "picked": picked, "sum_gw": gw, "sum_cbm": cbm, "method": "exact"}
    
    # For large datasets, use robust greedy-local approach
    values_gw = pkgs_df["G.W(kgs)"].values.astype(float)
    values_cbm = pkgs_df["CBM"].values.astype(float)
    
    success, picked_indices, sum_gw, sum_cbm = robust_greedy_local(
        values_gw, values_cbm, k, gw_tgt, cbm_tgt, tol
    )
    
    # Convert indices back to DataFrame indices
    picked_df_indices = [pkgs_df.index[i] for i in picked_indices] if picked_indices else []
    
    return {
        "found": success,
        "picked": picked_df_indices,
        "sum_gw": sum_gw,
        "sum_cbm": sum_cbm,
        "method": "robust-greedy-local"
    }

def find_subset_match_exploded(cand_df, k, gw_tgt, cbm_tgt, tol=TOL):
    """
    Exploded 패키지 단위 매칭 (ONTOLOGY 기반 개선)
    각 패키지를 개별 단위로 분해하여 더 정확한 매칭 수행
    
    Args:
        cand_df: candidate DataFrame
        k: target package count
        gw_tgt, cbm_tgt: target weights/volumes
        tol: tolerance
        
    Returns:
        dict: matching result
    """
    if cand_df.empty or k <= 0:
        return {"found": False, "picked": [], "sum_gw": None, "sum_cbm": None, "method": "no-candidate-exploded"}
    
    # Explode by package units
    units = explode_by_pkg(cand_df[["Pkg", "G.W(kgs)", "CBM"]])
    
    if len(units) == 0:
        return {"found": False, "picked": [], "sum_gw": None, "sum_cbm": None, "method": "no-units-exploded"}
    
    # Extract values for matching
    vals_gw = units["G.W(kgs)"].values.astype(float)
    vals_cbm = units["CBM"].values.astype(float)
    
    # Choose matching strategy based on size
    if len(units) <= MAX_EXACT_N:
        # Exact matching for small datasets
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
        # Use robust greedy-local for large datasets
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

def enhanced_subset_matching(cand_df, k, gw_tgt, cbm_tgt, tol=TOL, use_exploded=True):
    """
    Enhanced subset matching with multiple strategies (ONTOLOGY 기반)
    
    Args:
        cand_df: candidate DataFrame
        k: target package count
        gw_tgt, cbm_tgt: target weights/volumes
        tol: tolerance
        use_exploded: whether to use exploded matching
        
    Returns:
        dict: best matching result
    """
    if cand_df.empty or k <= 0:
        return {"found": False, "picked": [], "sum_gw": None, "sum_cbm": None, "method": "no-candidate"}
    
    results = []
    
    # Strategy 1: Original DataFrame matching
    result1 = find_subset_match(cand_df[["G.W(kgs)", "CBM"]], k, gw_tgt, cbm_tgt, tol)
    if result1["found"]:
        results.append(result1)
    
    # Strategy 2: Exploded matching (if enabled)
    if use_exploded and "Pkg" in cand_df.columns:
        result2 = find_subset_match_exploded(cand_df, k, gw_tgt, cbm_tgt, tol)
        if result2["found"]:
            results.append(result2)
    
    # Return best result (prioritize exact matches, then by accuracy)
    if not results:
        return result1  # Return original result even if not found
    
    # Prioritize exact methods, then by accuracy
    exact_results = [r for r in results if "exact" in r["method"]]
    if exact_results:
        return exact_results[0]
    
    # Among non-exact results, pick the one with smallest error
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

def row_key(ix, row):
    return f"{ix}|GW={row['G.W(kgs)']:.2f}|CBM={row['CBM']:.2f}"

# Load
df_inv = pd.read_excel(INVOICE_PATH, sheet_name='Invoice_Original')
df_all = pd.read_excel(ALL_PATH, sheet_name=0)

# Numeric
for col in ["No. of Pkgs", "Weight (kg)", "CBM"]:
    if col in df_inv.columns: df_inv[col] = to_num(df_inv[col])
for col in ["Pkg", "G.W(kgs)", "CBM"]:
    if col in df_all.columns: df_all[col] = to_num(df_all[col])

# Dates
if ALL_DATE_COL in df_all.columns:
    df_all["_ym"] = pd.to_datetime(df_all[ALL_DATE_COL], errors="coerce").dt.to_period("M").astype(str)
else:
    df_all["_ym"] = None
if INV_DATE_COL in df_inv.columns:
    df_inv["_ym"] = pd.to_datetime(df_inv[INV_DATE_COL], errors="coerce").dt.to_period("M").astype(str)
else:
    df_inv["_ym"] = None

# Parts fill
df_inv = extract_parts(df_inv, col_full="HVDC CODE")
df_all = extract_parts(df_all, col_full="HVDC CODE")

match_rows = []
detail_rows = []

for raw_code in df_inv["HVDC CODE"].dropna().unique():
    inv_rows = df_inv[df_inv["HVDC CODE"] == raw_code]
    # Expand combined codes from raw_code
    expanded = expand_combined_codes(raw_code)
    
    # Extract REV NO information for identification
    rev_nos = inv_rows["REV NO"].dropna().unique() if "REV NO" in inv_rows.columns else []
    rev_no_list = ", ".join([str(rev) for rev in sorted(rev_nos)]) if len(rev_nos) > 0 else "N/A"
    rev_no_count = len(rev_nos) if len(rev_nos) > 0 else 0
    
    # Enhanced vendor code analysis (from invoice parts)
    p1, p2, p3, p4 = inv_rows.iloc[0]["HVDC CODE 1"], inv_rows.iloc[0]["HVDC CODE 2"], inv_rows.iloc[0]["HVDC CODE 3"], inv_rows.iloc[0]["HVDC CODE 4"]
    vendor = str(p3).upper() if p3 else None
    
    # Enhanced vendor validation with ontology system
    is_primary_vendor = is_valid_hvdc_vendor(vendor, extended_mode=False)  # HE, SIM only
    is_extended_vendor = is_valid_hvdc_vendor(vendor, extended_mode=True)  # All known vendors
    
    if is_primary_vendor:
        vmemo = "PRIMARY_VENDOR"
    elif is_extended_vendor:
        vmemo = "EXTENDED_VENDOR"
    else:
        vmemo = "NO_DATA"
        
    ym = inv_rows["_ym"].dropna().unique()
    ym = ym[0] if len(ym)>0 else None

    # Build candidate pool as union of:
    #  - FULL code in expanded set
    #  - OR parts(1..4) exactly equal to invoice parts (for each expanded code we treat same parts base)
    cand = df_all[
        (df_all["HVDC CODE"].isin(expanded)) |
        ((df_all["HVDC CODE 1"] == p1) & (df_all["HVDC CODE 2"] == p2) & (df_all["HVDC CODE 3"] == p3) & (df_all["HVDC CODE 4"] == p4))
    ].copy()

    # Enhanced filtering logic - allow extended vendors but prefer primary vendors
    if not is_extended_vendor:
        cand = cand.iloc[0:0]  # empty if vendor not recognized at all

    if USE_MONTH_FILTER and ym is not None and cand["_ym"].notna().any():
        cand = cand[cand["_ym"] == ym]

    # Targets
    k = int(inv_rows["No. of Pkgs"].sum(skipna=True))
    gw_tgt = float(inv_rows["Weight (kg)"].sum(skipna=True))
    cbm_tgt = float(inv_rows["CBM"].sum(skipna=True))

    # 🔧 PATCH: Pkg PASS 판정을 sum(Pkg) 기준으로 변경
    N = len(cand)
    all_pkgs_sum = int(cand["Pkg"].fillna(0).sum()) if len(cand) > 0 else 0
    pkg_pass = (all_pkgs_sum >= k)

    # 🔧 PATCH: 후보 없음/패키지 부족시 조기 종료
    if len(cand) == 0 or all_pkgs_sum == 0 or k <= 0:
        result = {"found": False, "picked": [], "sum_gw": None, "sum_cbm": None, "method": "no-candidate"}
        err_gw = err_cbm = None
        gw_ok = cbm_ok = False
        match_status = "FAIL"
    else:
        # 🔧 PATCH: 항상 unit 단위로 변환 (Pkg 수만큼 분해, GW/CBM은 Pkg로 균등분배)
        units = explode_by_pkg(cand[["Pkg", "G.W(kgs)", "CBM"]])
        
        if len(units) == 0:
            result = {"found": False, "picked": [], "sum_gw": None, "sum_cbm": None, "method": "no-units"}
            err_gw = err_cbm = None
            gw_ok = cbm_ok = False
            match_status = "FAIL"
        else:
            vals_gw = units["G.W(kgs)"].values.astype(float)
            vals_cbm = units["CBM"].values.astype(float)
            
            if len(units) <= MAX_EXACT_N:
                # 🔧 PATCH: 정확 매칭 (exploded 기준)
                found_exact = False
                picked_indices = []
                sum_gw = sum_cbm = None
                
                for comb in combinations(range(len(vals_gw)), k):
                    gw = float(np.sum(vals_gw[list(comb)]))
                    cbm = float(np.sum(vals_cbm[list(comb)]))
                    if close2(gw, gw_tgt, TOL) and close2(cbm, cbm_tgt, TOL):
                        picked_indices = list(comb)
                        sum_gw, sum_cbm = gw, cbm
                        found_exact = True
                        break
                
                result = {
                    "found": found_exact,
                    "picked": picked_indices,
                    "sum_gw": sum_gw,
                    "sum_cbm": sum_cbm,
                    "method": "exact-exploded"
                }
            else:
                # 🔧 PATCH: Robust 그리디-로컬 매칭 (exploded 기준)
                success, picked_indices, sum_gw, sum_cbm = robust_greedy_local(
                    vals_gw, vals_cbm, k, gw_tgt, cbm_tgt, TOL
                )
                
                result = {
                    "found": success,
                    "picked": picked_indices,
                    "sum_gw": sum_gw,
                    "sum_cbm": sum_cbm,
                    "method": "robust-greedy-local-exploded"
                }
            
            # 🔧 PATCH: 에러 및 매치 상태 계산
            err_gw = None if result["sum_gw"] is None else (result["sum_gw"] - gw_tgt)
            err_cbm = None if result["sum_cbm"] is None else (result["sum_cbm"] - cbm_tgt)
            gw_ok = (result["sum_gw"] is not None and abs(err_gw) <= TOL)
            cbm_ok = (result["sum_cbm"] is not None and abs(err_cbm) <= TOL)
            match_status = "PASS" if (pkg_pass and gw_ok and cbm_ok) else "FAIL"

    # 🔧 PATCH: Picked keys 처리 (exploded unit 기준)
    picked_keys = []
    if result["picked"] and result["method"] not in ["no-candidate", "no-units"]:
        # exploded unit에서 선택된 인덱스들 처리
        if "exploded" in result["method"]:
            # units DataFrame에서 정보 추출
            if 'units' in locals() and len(units) > 0:
                for unit_idx in result["picked"]:
                    if unit_idx < len(units):
                        unit_row = units.iloc[unit_idx]
                        original_idx = unit_row.get("Original_Index", unit_idx)
                        unit_gw = unit_row["G.W(kgs)"]
                        unit_cbm = unit_row["CBM"]
                        picked_keys.append(f"Unit_{unit_idx}|GW={unit_gw:.3f}|CBM={unit_cbm:.3f}")
                        
                        # 원본 행 정보도 추가 (있는 경우)
                        if original_idx in cand.index:
                            orig_row = cand.loc[original_idx]
                            detail_rows.append({
                                "REV_NO_List": rev_no_list,
                                "REV_NO_Count": rev_no_count,
                                "Invoice_RAW_CODE": raw_code,
                                "Expanded_Code_Member?": orig_row["HVDC CODE"] in expanded,
                                "HVDC CODE": orig_row["HVDC CODE"],
                                "Picked_Unit_Idx": unit_idx,
                                "Original_Row_Idx": original_idx,
                                "Unit_GW": unit_gw,
                                "Unit_CBM": unit_cbm,
                                "Original_Total_GW": orig_row["G.W(kgs)"],
                                "Original_Total_CBM": orig_row["CBM"],
                                "Original_Pkg_Count": orig_row.get("Pkg", 1),
                                "_ym": orig_row.get("_ym", None),
                                "Vendor(code3)": vendor,
                                "Vendor_Type": vmemo,
                                "Warehouse_Type": classify_warehouse_type(orig_row.get("Location", "")),
                                "Code_Normalized": normalize_hvdc_code(orig_row["HVDC CODE"])
                            })
        else:
            # 기존 방식 (비-exploded)
            for ix in result["picked"]:
                if ix in cand.index:
                    row = cand.loc[ix]
                    picked_keys.append(row_key(ix, row))
                    detail_rows.append({
                        "REV_NO_List": rev_no_list,
                        "REV_NO_Count": rev_no_count,
                        "Invoice_RAW_CODE": raw_code,
                        "Expanded_Code_Member?": row["HVDC CODE"] in expanded,
                        "HVDC CODE": row["HVDC CODE"],
                        "Picked_Idx": ix,
                        "GW": row["G.W(kgs)"],
                        "CBM": row["CBM"],
                        "_ym": row.get("_ym", None),
                        "Vendor(code3)": vendor,
                        "Vendor_Type": vmemo,
                        "Warehouse_Type": classify_warehouse_type(row.get("Location", "")),
                        "Code_Normalized": normalize_hvdc_code(row["HVDC CODE"])
                    })

    # Enhanced result analysis
    method_used = result.get("method", "unknown")
    is_exploded = "exploded" in method_used
    is_exact = "exact" in method_used
    is_robust = "robust" in method_used
    
    # 🔧 PATCH: 권장 출력 컬럼으로 결과 저장 (리포트 가독성↑)
    match_rows.append({
        # 🎯 식별 정보
        "REV_NO_List": rev_no_list,
        "REV_NO_Count": rev_no_count,
        "Invoice_RAW_CODE": raw_code,
        "Expanded_Set": ", ".join(sorted(expanded)),
        
        # 🎯 패키지 분석 (핵심!)
        "Invoice_Pkgs(k)": k,
        "All_Pkgs(sum)": all_pkgs_sum,  # 🔧 PATCH: sum(Pkg) 기준 추가
        "Candidate_Rows(N)": N,
        "Pkg_Status": "PASS" if pkg_pass else "FAIL",  # 🔧 PATCH: sum(Pkg) 기준 판정
        
        # 🎯 무게/부피 분석
        "GW_Invoice": gw_tgt,
        "CBM_Invoice": cbm_tgt,
        "GW_SumPicked": result.get("sum_gw"),
        "CBM_SumPicked": result.get("sum_cbm"),
        "Err_GW": err_gw,  # 🔧 PATCH: 직접 계산된 에러값 사용
        "Err_CBM": err_cbm,  # 🔧 PATCH: 직접 계산된 에러값 사용
        
        # 🎯 매치 결과 (핵심!)
        "GW_Match(±0.10)": "PASS" if gw_ok else "FAIL",  # 🔧 PATCH: 개선된 판정
        "CBM_Match(±0.10)": "PASS" if cbm_ok else "FAIL",  # 🔧 PATCH: 개선된 판정
        "Match_Status": match_status,  # 🔧 PATCH: 종합 매치 상태 추가
        
        # 🎯 알고리즘 정보
        "Method": method_used,
        "Algorithm_Quality": "EXACT" if is_exact else ("ROBUST" if is_robust else "BASIC"),
        "Is_Exploded_Method": is_exploded,
        "Is_Exact_Method": is_exact,
        "Is_Robust_Method": is_robust,
        
        # 🎯 벤더 정보
        "Vendor(code3)": vendor,
        "Vendor_Type": vmemo,
        "Is_Primary_Vendor": is_primary_vendor,
        "Is_Extended_Vendor": is_extended_vendor,
        
        # 🎯 과금 모드 정보 (NEW) - 창고 기준으로 수정 (정규화 적용)
        "Billing_Mode": get_billing_mode(normalize_warehouse_name(inv_rows.iloc[0].get("Location", "") if "Location" in inv_rows.columns else "")),
        "Contract_Rate_AED": get_warehouse_rate(normalize_warehouse_name(inv_rows.iloc[0].get("Location", "") if "Location" in inv_rows.columns else "")),
        
        # 🎯 상세 정보
        "Picked_Count": len(result["picked"]) if result.get("picked") else 0,
        "Picked_List": "; ".join(picked_keys),
        "Code_Normalized": normalize_hvdc_code(raw_code),
        "Data_Scope": vmemo  # Keep for backward compatibility
    })

# 🔧 NEW: 사용자-친화형 인보이스 검증 리포트 생성
df_match = pd.DataFrame(match_rows)
df_detail = pd.DataFrame(detail_rows)

# 🎯 1) Exceptions_Only 시트 생성 (예외 전용)
def create_exceptions_only():
    """FAIL 상태만 포함한 예외 전용 시트 생성"""
    # Operation Date 컬럼이 없는 경우 추가
    if "Operation Date" not in df_match.columns:
        # df_inv에서 HVDC CODE 매칭으로 Operation Date 가져오기
        date_mapping = {}
        for _, row in df_inv.iterrows():
            hvdc_code = row.get("HVDC CODE")
            op_date = row.get("Operation Date")
            if hvdc_code and op_date:
                date_mapping[hvdc_code] = op_date
        
        df_match["Operation Date"] = df_match["Invoice_RAW_CODE"].map(date_mapping)
    
    # 예외 케이스만 필터링 (PASS가 아닌 모든 경우)
    df_exceptions = df_match.loc[df_match["Match_Status"] != "PASS"].copy()
    
    # 에러 합계로 정렬 (가장 심각한 에러부터)
    df_exceptions["Total_Error"] = (
        df_exceptions["Err_GW"].fillna(0).abs() + 
        df_exceptions["Err_CBM"].fillna(0).abs()
    )
    
    # 정렬: FAIL → 에러 합계 내림차순 → Operation Date
    sort_columns = ["Match_Status", "Total_Error"]
    sort_ascending = [True, False]
    
    if "Operation Date" in df_exceptions.columns:
        sort_columns.append("Operation Date")
        sort_ascending.append(True)
        
    df_exceptions = df_exceptions.sort_values(by=sort_columns, ascending=sort_ascending)
    
    # 컬럼 선택 및 순서 정리 (정렬 후)
    exception_cols = [
        "Operation Date", "Invoice_RAW_CODE", "Vendor(code3)",
        "Invoice_Pkgs(k)", "All_Pkgs(sum)", "Pkg_Status",
        "GW_Invoice", "GW_SumPicked", "Err_GW",
        "CBM_Invoice", "CBM_SumPicked", "Err_CBM",
        "GW_Match(±0.10)", "CBM_Match(±0.10)", "Match_Status",
        "Method", "Picked_Count", "REV_NO_List"
    ]
    
    # 사용 가능한 컬럼만 선택
    available_cols = [col for col in exception_cols if col in df_exceptions.columns]
    df_ex = df_exceptions[available_cols].copy()
    
    return df_ex

# 🎯 2) 원본 순서 시트 생성 (기존 함수 개선)
def create_invoice_original_order_sheet():
    """원본 인보이스 순서 + 매칭 결과를 결합한 시트 생성"""
    df_invoice_original = df_inv.copy()
    
    # 매칭 결과 매핑
    match_dict = {}
    for _, row in df_match.iterrows():
        hvdc_code = row['Invoice_RAW_CODE']
        match_dict[hvdc_code] = {
            'All_Pkgs(sum)': row.get('All_Pkgs(sum)', 0),
            'Candidate_Units': row.get('All_Pkgs(sum)', 0),  # 보조 정보
            'Pkg_Status': row.get('Pkg_Status', 'NO_MATCH'),
            'GW_SumPicked': row.get('GW_SumPicked', None),
            'CBM_SumPicked': row.get('CBM_SumPicked', None),
            'Err_GW': row.get('Err_GW', None),
            'Err_CBM': row.get('Err_CBM', None),
            'GW_Match': row.get('GW_Match(±0.10)', 'NO_MATCH'),
            'CBM_Match': row.get('CBM_Match(±0.10)', 'NO_MATCH'),
            'Match_Status': row.get('Match_Status', 'NO_MATCH'),
            'Method': row.get('Method', 'NO_MATCH'),
            'Picked_Count': row.get('Picked_Count', 0)
        }
    
    # 결과 컬럼 추가
    result_columns = []
    for _, row in df_invoice_original.iterrows():
        hvdc_code = row.get('HVDC CODE', '')
        if hvdc_code in match_dict:
            result_columns.append(match_dict[hvdc_code])
        else:
            result_columns.append({
                'All_Pkgs(sum)': 0,
                'Candidate_Units': 0,
                'Pkg_Status': 'NO_MATCH',
                'GW_SumPicked': None,
                'CBM_SumPicked': None,
                'Err_GW': None,
                'Err_CBM': None,
                'GW_Match': 'NO_MATCH',
                'CBM_Match': 'NO_MATCH',
                'Match_Status': 'NO_MATCH',
                'Method': 'NO_MATCH',
                'Picked_Count': 0
            })
    
    # 데이터 결합
    df_results = pd.DataFrame(result_columns)
    df_combined = pd.concat([df_invoice_original, df_results], axis=1)
    
    # REV NO 순서대로 정렬
    if 'REV NO' in df_combined.columns:
        df_combined = df_combined.sort_values(['REV NO'], na_position='last')
    
    return df_combined

# 🎯 3) Dashboard KPI 계산
def calculate_dashboard_kpi():
    """대시보드용 KPI 지표 계산"""
    total_lines = len(df_match)
    fail_count = (df_match["Match_Status"] != "PASS").sum()
    pass_count = (df_match["Match_Status"] == "PASS").sum()
    fail_ratio = round(100 * (fail_count / total_lines), 1) if total_lines > 0 else 0
    
    # 에러 평균 (절댓값 기준)
    avg_err_gw = round(df_match["Err_GW"].fillna(0).abs().mean(), 3)
    avg_err_cbm = round(df_match["Err_CBM"].fillna(0).abs().mean(), 3)
    
    # 메서드별 성능
    method_stats = df_match.groupby("Method").agg({
        "Match_Status": lambda x: (x == "PASS").sum(),
        "Invoice_RAW_CODE": "count"
    }).rename(columns={"Invoice_RAW_CODE": "Total"})
    
    method_stats["Success_Rate"] = round(100 * method_stats["Match_Status"] / method_stats["Total"], 1)
    
    # Top 예외 케이스 (상위 10건)
    df_top_exceptions = df_match.loc[df_match["Match_Status"] != "PASS"].copy()
    if not df_top_exceptions.empty:
        df_top_exceptions["Total_Error"] = (
            df_top_exceptions["Err_GW"].fillna(0).abs() + 
            df_top_exceptions["Err_CBM"].fillna(0).abs()
        )
        df_top_exceptions = df_top_exceptions.nlargest(10, "Total_Error")[
            ["Invoice_RAW_CODE", "Invoice_Pkgs(k)", "All_Pkgs(sum)", 
             "Err_GW", "Err_CBM", "Match_Status", "Method"]
        ]
    
    return {
        "KPI": {
            "총_라인수": total_lines,
            "PASS": pass_count,
            "FAIL": fail_count,
            "FAIL_비율(%)": fail_ratio,
            "평균_Err_GW": avg_err_gw,
            "평균_Err_CBM": avg_err_cbm
        },
        "Method_Performance": method_stats,
        "Top_Exceptions": df_top_exceptions
    }

# 데이터 생성
df_exceptions = create_exceptions_only()
df_invoice_order = create_invoice_original_order_sheet()
dashboard_data = calculate_dashboard_kpi()

# 🎯 4) 사용자-친화형 Excel 출력
with pd.ExcelWriter(OUT_PATH, engine="xlsxwriter") as writer:
    workbook = writer.book
    
    # === 스타일 정의 ===
    header_format = workbook.add_format({
        'bold': True, 'text_wrap': True, 'valign': 'top',
        'fg_color': '#D7E4BD', 'border': 1
    })
    
    info_format = workbook.add_format({
        'fg_color': '#E6F3FF', 'border': 1
    })
    
    pass_format = workbook.add_format({
        'font_color': '#006400', 'bold': True  # 초록색
    })
    
    fail_format = workbook.add_format({
        'font_color': '#9C0006', 'bold': True  # 빨강색
    })
    
    warn_format = workbook.add_format({
        'bg_color': '#FFF2CC', 'border': 1  # 노랑색 배경
    })
    
    error_format = workbook.add_format({
        'bg_color': '#FFC7CE', 'border': 1  # 빨강색 배경
    })
    
    number_format = workbook.add_format({'num_format': '#,##0.00'})
    integer_format = workbook.add_format({'num_format': '#,##0'})
    
    # === 1) Dashboard 시트 ===
    # KPI 테이블
    kpi_df = pd.DataFrame([dashboard_data["KPI"]])
    kpi_df.to_excel(writer, index=False, sheet_name="Dashboard", startrow=1)
    
    # 메서드 성능 테이블
    dashboard_data["Method_Performance"].to_excel(
        writer, sheet_name="Dashboard", startrow=4, startcol=0
    )
    
    # Top 예외 케이스
    if not dashboard_data["Top_Exceptions"].empty:
        dashboard_data["Top_Exceptions"].to_excel(
            writer, index=False, sheet_name="Dashboard", startrow=4, startcol=6
        )
    
    # Dashboard 제목 추가
    dashboard_ws = writer.sheets["Dashboard"]
    dashboard_ws.write(0, 0, "📊 HVDC 인보이스 검증 대시보드", workbook.add_format({'bold': True, 'font_size': 16}))
    dashboard_ws.write(3, 0, "알고리즘 성능", workbook.add_format({'bold': True, 'font_size': 12}))
    dashboard_ws.write(3, 6, "Top 예외 케이스 (상위 10건)", workbook.add_format({'bold': True, 'font_size': 12}))
    
    # === 2) Exceptions_Only 시트 ===
    df_exceptions.to_excel(writer, index=False, sheet_name="Exceptions_Only")
    exceptions_ws = writer.sheets["Exceptions_Only"]
    
    # 헤더 포맷 적용
    for col_num, value in enumerate(df_exceptions.columns.values):
        exceptions_ws.write(0, col_num, value, header_format)
    
    # Freeze Panes (1행 + 5열 고정)
    exceptions_ws.freeze_panes(1, 5)
    
    # 컬럼 너비 조정
    exceptions_ws.set_column(0, 4, 12)
    exceptions_ws.set_column(5, 20, 14)
    
    # 조건부 서식 적용
    if len(df_exceptions) > 0:
        # Match_Status FAIL 강조
        match_col = df_exceptions.columns.get_loc("Match_Status") if "Match_Status" in df_exceptions.columns else -1
        if match_col >= 0:
            exceptions_ws.conditional_format(
                1, match_col, len(df_exceptions), match_col,
                {'type': 'text', 'criteria': 'containing', 'value': 'FAIL', 'format': fail_format}
            )
        
        # Pkg_Status FAIL 강조
        pkg_col = df_exceptions.columns.get_loc("Pkg_Status") if "Pkg_Status" in df_exceptions.columns else -1
        if pkg_col >= 0:
            exceptions_ws.conditional_format(
                1, pkg_col, len(df_exceptions), pkg_col,
                {'type': 'text', 'criteria': 'containing', 'value': 'FAIL', 'format': warn_format}
            )
    
    # === 3) Invoice_Original_Order 시트 ===
    df_invoice_order.to_excel(writer, index=False, sheet_name="Invoice_Original_Order")
    invoice_ws = writer.sheets["Invoice_Original_Order"]
    
    # 헤더 포맷
    for col_num, value in enumerate(df_invoice_order.columns.values):
        invoice_ws.write(0, col_num, value, header_format)
    
    # Freeze Panes
    invoice_ws.freeze_panes(1, 5)
    
    # 컬럼 너비 및 서식
    invoice_ws.set_column(0, 4, 12)
    invoice_ws.set_column(5, len(df_inv.columns)-1, 14)
    
    # 결과 블록 배경색 (하늘색)
    result_start_col = len(df_inv.columns)
    for col in range(result_start_col, len(df_invoice_order.columns)):
        invoice_ws.set_column(col, col, 14, info_format)
    
    # === 4) Picked_Detail 시트 ===
    if not df_detail.empty:
        # 컬럼 순서 정리
        detail_cols = ["Invoice_RAW_CODE", "Original_Row_Idx", "Picked_Unit_Idx", 
                      "Unit_GW", "Unit_CBM", "Original_Total_GW", "Original_Total_CBM", 
                      "Original_Pkg_Count", "Vendor(code3)", "Warehouse_Type"]
        available_detail_cols = [col for col in detail_cols if col in df_detail.columns]
        df_detail_ordered = df_detail[available_detail_cols + [col for col in df_detail.columns if col not in available_detail_cols]]
        
        df_detail_ordered.to_excel(writer, index=False, sheet_name="Picked_Detail")
        detail_ws = writer.sheets["Picked_Detail"]
        
        # 헤더 포맷
        for col_num, value in enumerate(df_detail_ordered.columns.values):
            detail_ws.write(0, col_num, value, header_format)
        
        # Freeze Panes
        detail_ws.freeze_panes(1, 3)
        detail_ws.set_column(0, 10, 14)

print(f"Saved: {OUT_PATH}")
print("🎯 사용자-친화형 인보이스 검증 리포트 생성 완료!")
print("📋 생성된 시트 (사용 순서):")
print("  1. 📊 Dashboard - KPI 요약 및 필터")
print("  2. ⚠️  Exceptions_Only - 예외 케이스 전용 (FAIL만)")
print("  3. 📝 Invoice_Original_Order - 원본 순서 + 결과")
print("  4. 🔍 Picked_Detail - 매칭 근거 상세")
print("\n💡 사용법:")
print("  • Dashboard에서 전체 현황 파악")
print("  • Exceptions_Only에서 문제 케이스 우선 검토")  
print("  • Invoice_Original_Order에서 원본 대조")
print("  • Picked_Detail에서 매칭 근거 확인")

# ✅ NEW: 과금 모드 통합 및 Passthrough 금액 연결 예시
print("\n🏢 과금 모드 통합 시스템 실행 (v3.0-corrected 연동)")

try:
    # 1) Passthrough 금액 로드
    passthrough_amounts = load_invoice_passthrough_amounts(INVOICE_PATH)
    print(f"📊 Passthrough 금액 로드: {len(passthrough_amounts)}개 항목")
    
    # 2) hvdc_excel_reporter_final_sqm_rev.py와 연동하여 과금 계산
    try:
        # hvdc_excel_reporter_final_sqm_rev.py 임포트
        import sys
        sys.path.append('.')
        from hvdc_excel_reporter_final_sqm_rev import HVDCExcelReporterFinal
        
        # Reporter 인스턴스 생성
        reporter = HVDCExcelReporterFinal()
        
        # 1) 인보이스 로드 (스키마: Operation Date, TOTAL)
        invoice_df = pd.read_excel(INVOICE_PATH, sheet_name=0)
        # 컬럼명을 표준 형식으로 변환
        invoice_df = invoice_df.rename(columns={
            'Operation Date': 'Month',
            'TOTAL': 'Invoice_Amount'
        })
        # Warehouse 컬럼 추가 (기본값으로 'Unknown' 설정)
        invoice_df['Warehouse'] = 'Unknown'
        
        # 2) Passthrough dict 구성
        passthrough = reporter.calculator.build_passthrough_amounts(invoice_df)
        print(f"📊 Passthrough dict 생성: {len(passthrough)}개 항목")
        
        # 3) 시스템 통계 산출(기존)
    stats = reporter.calculate_warehouse_statistics()
        
        # 4) 일할+모드 과금으로 교체
    stats['sqm_invoice_charges'] = reporter.calculator.calculate_monthly_invoice_charges_prorated(
            stats['processed_data'],
            passthrough_amounts=passthrough
        )
        
        # 5) 과금 시트 생성
        invoice_sheet_df = reporter.create_sqm_invoice_sheet(stats)
        print(f"✅ SQM Invoice 과금 시트 생성 완료: {len(invoice_sheet_df)}건")
        
        # 6) 🎯 NEW: Monthly_Charges_Match 시트 생성
        print("📊 Monthly_Charges_Match 시트 생성 중...")
        
        # Monthly_Charges_Match 시트 생성 함수 (인라인)
        def create_monthly_charges_match_inline(reporter, stats: dict, invoice_df: pd.DataFrame, delta_thr=0.02) -> pd.DataFrame:
            """월×창고 단위 System vs Invoice 매칭"""
            # 1) System 테이블 전개
            charges = stats.get('sqm_invoice_charges', {})
            sys_rows = []
            for ym, payload in charges.items():
                for wh, v in payload.items():
                    if wh == 'total_monthly_charge_aed' or not isinstance(v, dict):
                        continue
                    sys_rows.append({
                        'Month': ym,
                        'Warehouse': wh,
                        'Billing_Mode': v.get('billing_mode', ''),
                        'System_Avg_SQM': v.get('avg_sqm', 0.0),
                        'System_Rate': v.get('rate_aed', 0.0),
                        'System_Amount': v.get('monthly_charge_aed', 0.0),
                        'Amount_Source': v.get('amount_source', '')
                    })
        df_sys = pd.DataFrame(sys_rows)

        # 2) Invoice 테이블 정규화
        inv = invoice_df.copy()
        inv['Month'] = pd.to_datetime(inv['Month'], errors='coerce').dt.to_period('M').astype(str)
        inv = inv.rename(columns={
            'Amount_AED': 'Invoice_Amount',
            'Rate_AED_per_SQM': 'Invoice_Rate',
            'Billed_SQM': 'Invoice_SQM'
        })
        for col in ['Invoice_Amount','Invoice_Rate','Invoice_SQM']:
            if col not in inv.columns: inv[col] = np.nan
        inv_grp = inv.groupby(['Month','Warehouse'], dropna=False)[['Invoice_Amount','Invoice_Rate','Invoice_SQM']].sum().reset_index()

        # 3) 키 조인
        m = df_sys.merge(inv_grp, on=['Month','Warehouse'], how='left', validate='one_to_one')

        # 4) 계약 단가/모드 기대치
        get_mode = reporter.calculator.billing_mode.get
        get_rate = reporter.calculator.warehouse_sqm_rates.get
        m['Expected_Mode'] = m['Warehouse'].map(get_mode).fillna('unknown')
        m['Contract_Rate'] = m['Warehouse'].map(get_rate).fillna(0.0)

        # 5) System 금액 재산출(검증용)
        def _recalc_system_amount(row):
            mode = row['Expected_Mode']
            if mode == 'rate':
                return round(float(row['System_Avg_SQM']) * float(row['Contract_Rate']), 2)
            elif mode == 'passthrough':
                return float(row['System_Amount'])
            else:  # no-charge
                return 0.0
        m['System_Amount_Recalc'] = m.apply(_recalc_system_amount, axis=1)

        # 6) Δ 계산
        m['Invoice_Amount'] = m['Invoice_Amount'].fillna(0.0).astype(float)
        m['Δ_AED'] = m['Invoice_Amount'] - m['System_Amount_Recalc']
        m['Δ_%'] = np.where(m['Invoice_Amount'] == 0, 0.0, m['Δ_AED'] / m['Invoice_Amount'])

        # 7) PASS/FAIL
        def _status(row):
            mode = row['Expected_Mode']
            inv_amt = float(row['Invoice_Amount'])
            sys_amt = float(row['System_Amount_Recalc'])
            if mode == 'rate':
                return "PASS" if abs(row['Δ_%']) <= delta_thr else "FAIL"
            elif mode == 'passthrough':
                return "PASS" if abs(inv_amt - sys_amt) < 0.5 else "FAIL"
            else:  # no-charge
                return "PASS" if inv_amt == 0.0 else "FAIL"
        m['Status'] = m.apply(_status, axis=1)

        # 8) Reason_Code
        def _reason(row):
            mode = row['Expected_Mode']
            if mode == 'unknown':
                return REASON["MODE_MISSING"]
            if mode == 'rate':
                if not np.isnan(row.get('Invoice_Rate', np.nan)) and abs(row['Invoice_Rate'] - row['Contract_Rate']) > 1e-6:
                    return REASON["RATE_DIFF"]
                if row['Status'] == 'FAIL':
                    return REASON["PRORATION_MISMATCH"]
                return ""
            if mode == 'passthrough':
                if row['Status'] == 'FAIL':
                    return REASON["PASSTHROUGH_MISMATCH"]
                return ""
            if mode == 'no-charge':
                if row['Invoice_Amount'] > 0:
                    return REASON["NOCHARGE_VIOLATION"]
                return ""
            return ""
        m['Reason_Code'] = m.apply(_reason, axis=1)

        # 9) 정리 컬럼
        out_cols = [
            'Month','Warehouse','Expected_Mode','Billing_Mode',
            'System_Avg_SQM','Contract_Rate','System_Amount_Recalc',
            'Invoice_SQM','Invoice_Rate','Invoice_Amount',
            'Δ_AED','Δ_%','Status','Reason_Code','Amount_Source'
        ]
        return m[out_cols].sort_values(['Month','Warehouse']).reset_index(drop=True)
        
        match_df = create_monthly_charges_match_inline(reporter, stats, invoice_df, delta_thr=0.02)
        print(f"✅ Monthly_Charges_Match 시트 완료: {len(match_df)}건")
        
        # 7) 🎯 NEW: Exceptions_and_Evidence 시트 생성
        print("⚠️ Exceptions_and_Evidence 시트 생성 중...")
        
        # Exceptions_and_Evidence 시트 생성 함수 (인라인)
        def create_exceptions_and_evidence_inline(match_df: pd.DataFrame, stats: dict, delta_thr=0.02) -> pd.DataFrame:
            """FAIL/WARN만 추출. 증빙 경로 컬럼을 구성"""
            if match_df is None or match_df.empty:
                return pd.DataFrame(columns=[
                    'Month','Warehouse','Expected_Mode','Contract_Rate',
                    'System_Avg_SQM','System_Amount_Recalc','Invoice_Amount',
                    'Δ_AED','Δ_%','Status','Reason_Code',
                    'Evidence_Flow_Timeline','Evidence_Daily_Occupancy','Evidence_Source_Sheet'
                ])

            df = match_df.copy()
            # 등급화
            def _grade(row):
                if row['Expected_Mode'] == 'rate':
                    if abs(row['Δ_%']) <= delta_thr: return 'PASS'
                    if abs(row['Δ_%']) <= 0.05: return 'WARN'
                    return 'FAIL'
                return row['Status']
            df['Grade'] = df.apply(_grade, axis=1)

            ex = df[df['Grade'] != 'PASS'].copy()

            # 증빙 링크
            ex['Evidence_Flow_Timeline']   = 'Flow_Timeline'
            ex['Evidence_Daily_Occupancy'] = 'SQM_피벗테이블'
            ex['Evidence_Source_Sheet']    = '원본_데이터_샘플'

            keep = [
                'Month','Warehouse','Expected_Mode','Contract_Rate',
                'System_Avg_SQM','System_Amount_Recalc','Invoice_Amount',
                'Δ_AED','Δ_%','Grade','Status','Reason_Code',
                'Evidence_Flow_Timeline','Evidence_Daily_Occupancy','Evidence_Source_Sheet'
            ]
            return ex[keep].sort_values(['Month','Warehouse']).reset_index(drop=True)
        
        exceptions_df = create_exceptions_and_evidence_inline(match_df, stats, delta_thr=0.02)
        print(f"✅ Exceptions_and_Evidence 시트 완료: {len(exceptions_df)}건")
        
        # 8) 통합 Excel 파일 저장
        output_path = "HVDC_Invoice_Validation_Dashboard_with_Billing.xlsx"
        with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
            # 기존 과금 시트
            invoice_sheet_df.to_excel(writer, sheet_name="SQM_Invoice과금", index=False)
            
            # 🎯 NEW: 매칭 시트
            match_df.to_excel(writer, sheet_name="Monthly_Charges_Match", index=False)
            
            # 🎯 NEW: 예외 시트
            exceptions_df.to_excel(writer, sheet_name="Exceptions_and_Evidence", index=False)
        
        print(f"💾 통합 과금 시트 저장 완료: {output_path}")
        print(f"   - SQM_Invoice과금: {len(invoice_sheet_df)}건")
        print(f"   - Monthly_Charges_Match: {len(match_df)}건")
        print(f"   - Exceptions_and_Evidence: {len(exceptions_df)}건")
        
    except Exception as e:
        print(f"⚠️ Reporter 연동 실패: {e}")
        print("   (hvdc_excel_reporter_final_sqm_rev.py 파일이 같은 디렉토리에 있는지 확인하세요)")
    
    # 3) 과금 모드별 요약 통계
    billing_summary = {
        'rate_warehouses': list(BILLING_MODE_RATE),
        'passthrough_warehouses': list(BILLING_MODE_PASSTHROUGH), 
        'no_charge_warehouses': list(BILLING_MODE_NO_CHARGE),
        'total_passthrough_amount': sum(passthrough_amounts.values()),
        'rate_based_count': len(BILLING_MODE_RATE),
        'passthrough_count': len(BILLING_MODE_PASSTHROUGH),
        'no_charge_count': len(BILLING_MODE_NO_CHARGE)
    }
    
    print(f"✅ 과금 모드 요약:")
    print(f"   - Rate 기반: {billing_summary['rate_based_count']}개 창고 (계약단가 적용)")
    print(f"   - Passthrough: {billing_summary['passthrough_count']}개 창고 (인보이스 총액)")  
    print(f"   - No-charge: {billing_summary['no_charge_count']}개 창고 (과금 없음)")
    print(f"   - Passthrough 총액: {billing_summary['total_passthrough_amount']:,.2f} AED")
    
except Exception as e:
    print(f"⚠️ 과금 모드 통합 실행 중 오류: {e}")


