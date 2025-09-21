#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
간단한 HVDC 인보이스 테스트 스크립트
"""

import pandas as pd
import numpy as np
from pathlib import Path

print("🚀 HVDC 인보이스 테스트 시작")

try:
    # 1) 기본 파일 로드 테스트
    print("📂 파일 로드 테스트...")
    
    # 인보이스 파일 로드
    invoice_df = pd.read_excel("HVDC WH IVOICE_0921.xlsx", sheet_name='Invoice_Original')
    print(f"✅ 인보이스 파일 로드 완료: {len(invoice_df)}건")
    print(f"   컬럼: {list(invoice_df.columns)}")
    
    # HVDC 데이터 파일 로드
    hvdc_df = pd.read_excel("hvdc.xlsx", sheet_name=0)
    print(f"✅ HVDC 데이터 파일 로드 완료: {len(hvdc_df)}건")
    print(f"   컬럼: {list(hvdc_df.columns)}")
    
    # 2) 기본 매칭 테스트
    print("\n🔍 기본 매칭 테스트...")
    
    # HVDC CODE 매칭
    invoice_codes = set(invoice_df["HVDC CODE"].dropna().unique())
    hvdc_codes = set(hvdc_df["HVDC CODE"].dropna().unique())
    
    matched_codes = invoice_codes.intersection(hvdc_codes)
    print(f"✅ 매칭된 HVDC CODE: {len(matched_codes)}개")
    print(f"   인보이스 총 코드: {len(invoice_codes)}개")
    print(f"   HVDC 데이터 총 코드: {len(hvdc_codes)}개")
    
    # 3) 과금 모드 테스트
    print("\n💰 과금 모드 테스트...")
    
    # 과금 모드 정의
    BILLING_MODE_RATE = {"DSV Outdoor", "DSV MZP", "DSV Indoor", "DSV Al Markaz"}
    BILLING_MODE_PASSTHROUGH = {"AAA Storage", "Hauler Indoor", "DHL Warehouse"}
    BILLING_MODE_NO_CHARGE = {"MOSB"}
    
    WAREHOUSE_RATES = {
        'DSV Outdoor': 18.0,
        'DSV MZP': 33.0,
        'DSV Indoor': 47.0,
        'DSV Al Markaz': 47.0,
        'AAA Storage': 0.0,
        'Hauler Indoor': 0.0,
        'DHL Warehouse': 0.0,
        'MOSB': 0.0,
    }
    
    print(f"✅ Rate 모드 창고: {len(BILLING_MODE_RATE)}개")
    print(f"✅ Passthrough 모드 창고: {len(BILLING_MODE_PASSTHROUGH)}개")
    print(f"✅ No-charge 모드 창고: {len(BILLING_MODE_NO_CHARGE)}개")
    
    # 4) 기본 통계
    print("\n📊 기본 통계...")
    
    if "Location" in invoice_df.columns:
        locations = invoice_df["Location"].value_counts()
        print(f"✅ 인보이스 Location 분포:")
        for loc, count in locations.head(10).items():
            print(f"   {loc}: {count}건")
    
    if "TOTAL" in invoice_df.columns:
        total_amount = invoice_df["TOTAL"].sum()
        print(f"✅ 인보이스 총액: {total_amount:,.2f} AED")
    
    print("\n🎉 기본 테스트 완료!")
    
except Exception as e:
    print(f"❌ 오류 발생: {e}")
    import traceback
    traceback.print_exc()
