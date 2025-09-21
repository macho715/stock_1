#!/usr/bin/env python3
"""
HVDC SKU Master Hub - KPI 즉시 검증
DuckDB 쿼리를 통한 핵심 KPI 분석
"""

import duckdb
import pandas as pd
from pathlib import Path

def validate_sku_master_kpis():
    """SKU Master Hub KPI 검증 실행"""
    
    db_path = "out/sku_master.duckdb"
    if not Path(db_path).exists():
        print("❌ DuckDB 파일을 찾을 수 없습니다:", db_path)
        return
    
    print("🔍 HVDC SKU Master Hub - KPI 검증 시작")
    print("=" * 60)
    
    con = duckdb.connect(db_path)
    
    try:
        # 1) 테이블 기본 정보
        print("\n📊 1. 테이블 기본 정보")
        print("-" * 40)
        
        total_records = con.execute("SELECT COUNT(*) FROM sku_master").fetchone()[0]
        print(f"✅ 총 레코드 수: {total_records:,}개")
        
        columns = con.execute("PRAGMA table_info('sku_master')").fetchall()
        print(f"✅ 컬럼 수: {len(columns)}개")
        print("📋 주요 컬럼:", [col[1] for col in columns[:10]])
        
        # 2) Flow Coverage (0~4 모두 존재? → 100% 기대)
        print("\n🔄 2. Flow Coverage 검증 (0-4 완전성)")
        print("-" * 40)
        
        flow_dist = con.execute("""
            SELECT FLOW_CODE, COUNT(*) AS cnt, 
                   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
            FROM sku_master 
            WHERE FLOW_CODE IS NOT NULL
            GROUP BY FLOW_CODE 
            ORDER BY FLOW_CODE
        """).fetchall()
        
        for flow_code, cnt, pct in flow_dist:
            flow_desc = {
                0: "Pre Arrival", 
                1: "Port → Site", 
                2: "Port → WH → Site",
                3: "Port → WH → MOSB → Site", 
                4: "Multi-hop"
            }.get(flow_code, "Unknown")
            print(f"  ✅ Flow {flow_code} ({flow_desc}): {cnt:,}건 ({pct}%)")
        
        flow_coverage = len(flow_dist)
        print(f"🎯 Flow Coverage: {flow_coverage}/5 = {flow_coverage/5*100:.0f}% {'✅ PASS' if flow_coverage == 5 else '⚠️ 미완성'}")
        
        # 3) PKG Accuracy 
        print("\n📦 3. PKG Accuracy 검증")
        print("-" * 40)
        
        pkg_stats = con.execute("""
            SELECT 
                COUNT(*) AS total_records,
                COUNT(Pkg) AS pkg_not_null,
                ROUND(COUNT(Pkg) * 100.0 / COUNT(*), 2) AS pkg_accuracy_pct,
                SUM(CAST(Pkg AS INTEGER)) AS total_packages
            FROM sku_master
        """).fetchone()
        
        total, pkg_not_null, pkg_accuracy, total_pkg = pkg_stats
        print(f"  ✅ 총 레코드: {total:,}개")
        print(f"  ✅ PKG 데이터 있음: {pkg_not_null:,}개")
        print(f"  🎯 PKG Accuracy: {pkg_accuracy}% {'✅ PASS' if pkg_accuracy >= 99.0 else '⚠️ 기준 미달 (<99%)'}")
        print(f"  📦 총 패키지 수: {total_pkg:,}개")
        
        # 4) 최신 위치 분포 (재고 개념 추정)
        print("\n📍 4. Final Location 분포 (상위 10)")
        print("-" * 40)
        
        location_dist = con.execute("""
            SELECT Final_Location, COUNT(*) AS n_cases,
                   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
            FROM sku_master 
            WHERE Final_Location IS NOT NULL
            GROUP BY Final_Location 
            ORDER BY n_cases DESC 
            LIMIT 10
        """).fetchall()
        
        for location, cases, pct in location_dist:
            print(f"  📍 {location}: {cases:,}건 ({pct}%)")
        
        # 5) 인보이스 매칭 상태
        print("\n💰 5. Invoice 매칭 상태")
        print("-" * 40)
        
        # Check invoice column type first
        invoice_check = con.execute("""
            SELECT 
                COUNT(*) AS total,
                COUNT(inv_match_status) AS has_status,
                COUNT(CASE WHEN inv_match_status IS NOT NULL THEN 1 END) AS not_null_status
            FROM sku_master
        """).fetchone()
        
        total, has_status, not_null = invoice_check
        print(f"  📋 Invoice 데이터 현황:")
        print(f"     - 총 레코드: {total:,}개")
        print(f"     - 매칭 상태 있음: {has_status:,}개")
        
        if has_status > 0:
            # Try to get actual values
            sample_values = con.execute("""
                SELECT DISTINCT inv_match_status 
                FROM sku_master 
                WHERE inv_match_status IS NOT NULL 
                LIMIT 5
            """).fetchall()
            print(f"     - 상태 값 예시: {[v[0] for v in sample_values]}")
            
            invoice_stats = con.execute("""
                SELECT 
                    CASE WHEN inv_match_status IS NULL THEN 'NO_DATA' ELSE CAST(inv_match_status AS VARCHAR) END AS status, 
                    COUNT(*) AS n,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
                FROM sku_master 
                GROUP BY inv_match_status 
                ORDER BY n DESC
            """).fetchall()
        else:
            print("  ➖ Invoice 매칭 데이터가 없습니다 (향후 통합 예정)")
            invoice_stats = [('NO_DATA', total, 100.0)]
        
        for status, count, pct in invoice_stats:
            status_icon = "✅" if status == "PASS" else "⚠️" if status == "FAIL" else "➖"
            print(f"  {status_icon} {status}: {count:,}건 ({pct}%)")
        
        # 6) 벤더×Flow 요약
        print("\n🏢 6. Vendor × Flow Code 분포")
        print("-" * 40)
        
        vendor_flow = con.execute("""
            SELECT Vendor, FLOW_CODE, COUNT(*) AS n,
                   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
            FROM sku_master 
            WHERE Vendor IS NOT NULL AND FLOW_CODE IS NOT NULL
            GROUP BY Vendor, FLOW_CODE 
            ORDER BY Vendor, FLOW_CODE
        """).fetchall()
        
        current_vendor = None
        for vendor, flow_code, count, pct in vendor_flow:
            if vendor != current_vendor:
                if current_vendor is not None:
                    print()
                print(f"🏢 {vendor}:")
                current_vendor = vendor
            print(f"   Flow {flow_code}: {count:,}건 ({pct}%)")
        
        # 7) 데이터 품질 요약
        print(f"\n📈 7. 전체 KPI 요약")
        print("=" * 40)
        
        quality_check = con.execute("""
            SELECT 
                COUNT(*) AS total_records,
                COUNT(DISTINCT SKU) AS unique_skus,
                COUNT(Vendor) AS has_vendor,
                COUNT(Final_Location) AS has_location,
                COUNT(FLOW_CODE) AS has_flow_code,
                COUNT(Pkg) AS has_pkg,
                COUNT(GW) AS has_gw,
                COUNT(CBM) AS has_cbm
            FROM sku_master
        """).fetchone()
        
        total, unique_skus, has_vendor, has_location, has_flow, has_pkg, has_gw, has_cbm = quality_check
        
        print(f"📊 레코드 통계:")
        print(f"   - 총 레코드: {total:,}개")
        print(f"   - 고유 SKU: {unique_skus:,}개 ({'✅ 무중복' if total == unique_skus else '⚠️ 중복 있음'})")
        
        print(f"📋 데이터 완성도:")
        print(f"   - Vendor: {has_vendor/total*100:.1f}%")
        print(f"   - Final Location: {has_location/total*100:.1f}%") 
        print(f"   - Flow Code: {has_flow/total*100:.1f}%")
        print(f"   - Package Info: {has_pkg/total*100:.1f}%")
        print(f"   - Weight/Volume: {has_gw/total*100:.1f}%/{has_cbm/total*100:.1f}%")
        
        # 종합 평가
        print(f"\n🎯 종합 KPI 평가")
        print("=" * 40)
        
        pass_count = 0
        total_checks = 0
        
        # Flow Coverage
        total_checks += 1
        if flow_coverage == 5:
            print("✅ Flow Coverage: PASS (100%)")
            pass_count += 1
        else:
            print("⚠️ Flow Coverage: FAIL (미완성)")
        
        # PKG Accuracy
        total_checks += 1
        if pkg_accuracy >= 99.0:
            print("✅ PKG Accuracy: PASS (≥99%)")
            pass_count += 1
        else:
            print("⚠️ PKG Accuracy: FAIL (<99%)")
        
        # Data Integrity
        total_checks += 1
        if total == unique_skus:
            print("✅ SKU 무결성: PASS (중복 없음)")
            pass_count += 1
        else:
            print("⚠️ SKU 무결성: FAIL (중복 존재)")
        
        # Location Coverage
        total_checks += 1
        location_coverage = has_location/total*100
        if location_coverage >= 90:
            print("✅ Location Coverage: PASS (≥90%)")
            pass_count += 1
        else:
            print(f"⚠️ Location Coverage: FAIL ({location_coverage:.1f}%)")
        
        print(f"\n🏆 최종 결과: {pass_count}/{total_checks} 검증 통과 ({pass_count/total_checks*100:.0f}%)")
        
        if pass_count == total_checks:
            print("🎉 모든 KPI 검증 통과! 시스템이 정상 작동 중입니다.")
        elif pass_count >= total_checks * 0.75:
            print("⚡ 대부분 검증 통과! 일부 최적화 필요합니다.")
        else:
            print("⚠️ 일부 검증 실패. 데이터 품질 점검이 필요합니다.")
            
    except Exception as e:
        print(f"❌ 검증 중 오류 발생: {str(e)}")
    finally:
        con.close()

if __name__ == "__main__":
    validate_sku_master_kpis()
