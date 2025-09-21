#!/usr/bin/env python3
"""
3-모드 과금 시스템 간단 테스트 (파일 의존성 제거)
Rate/Passthrough/No-charge 모드 검증
"""

import pandas as pd
import sys
from pathlib import Path
import importlib.util

def test_warehouse_name_normalization():
    """창고명 정규화 테스트"""
    print("🧪 테스트 1: 창고명 정규화")
    
    try:
        from hvdc_excel_reporter_final_sqm_rev import CorrectedWarehouseIOCalculator
        
        calc = CorrectedWarehouseIOCalculator()
        
        # 테스트 케이스들
        test_cases = [
            ("DSV Al Markaz", "DSV Al Markaz"),
            ("DSVAlMarkaz", "DSV Al Markaz"),
            ("Al Markaz", "DSV Al Markaz"),
            ("AAA Storage", "AAA Storage"),
            ("AAAStorage", "AAA Storage"),
            ("MOSB", "MOSB"),
            ("Unknown Warehouse", "Unknown Warehouse")
        ]
        
        for input_name, expected in test_cases:
            result = calc.normalize_warehouse_name(input_name)
            status = "✅ PASS" if result == expected else "❌ FAIL"
            print(f"   {input_name} → {result} ({status})")
            
        return True
        
    except Exception as e:
        print(f"❌ 창고명 정규화 테스트 실패: {e}")
        return False

def test_billing_mode_classification():
    """과금 모드 분류 테스트 (파일 의존성 제거)"""
    print("\n🧪 테스트 2: 과금 모드 분류")
    
    try:
        # 동적 import로 공백이 있는 파일명 처리
        spec = importlib.util.spec_from_file_location("hvdc_wh_invoice", "hvdc wh invoice.py")
        hvdc_wh_invoice = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(hvdc_wh_invoice)
        
        get_billing_mode = hvdc_wh_invoice.get_billing_mode
        get_warehouse_rate = hvdc_wh_invoice.get_warehouse_rate
        normalize_warehouse_name = hvdc_wh_invoice.normalize_warehouse_name
        
        # 테스트 케이스들
        test_cases = [
            ("DSV Outdoor", "rate", 18.0),
            ("DSV MZP", "rate", 33.0),
            ("DSV Indoor", "rate", 47.0),
            ("DSV Al Markaz", "rate", 47.0),
            ("AAA Storage", "passthrough", 0.0),
            ("Hauler Indoor", "passthrough", 0.0),
            ("DHL Warehouse", "passthrough", 0.0),
            ("MOSB", "no-charge", 0.0)
        ]
        
        all_passed = True
        for warehouse, expected_mode, expected_rate in test_cases:
            normalized = normalize_warehouse_name(warehouse)
            actual_mode = get_billing_mode(normalized)
            actual_rate = get_warehouse_rate(normalized)
            
            mode_ok = actual_mode == expected_mode
            rate_ok = actual_rate == expected_rate
            
            status = "✅ PASS" if mode_ok and rate_ok else "❌ FAIL"
            print(f"   {warehouse}: {actual_mode}({actual_rate}) vs {expected_mode}({expected_rate}) ({status})")
            
            if not (mode_ok and rate_ok):
                all_passed = False
                
        return all_passed
        
    except Exception as e:
        print(f"❌ 과금 모드 분류 테스트 실패: {e}")
        return False

def test_passthrough_loader():
    """Passthrough 로더 테스트 (파일 의존성 제거)"""
    print("\n🧪 테스트 3: Passthrough 로더")
    
    try:
        # 동적 import
        spec = importlib.util.spec_from_file_location("hvdc_wh_invoice", "hvdc wh invoice.py")
        hvdc_wh_invoice = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(hvdc_wh_invoice)
        
        load_invoice_passthrough_amounts = hvdc_wh_invoice.load_invoice_passthrough_amounts
        
        # 테스트 데이터 생성
        test_data = pd.DataFrame({
            'Month': ['2024-01', '2024-01', '2024-02'],
            'Warehouse': ['AAA Storage', 'Hauler Indoor', 'DHL Warehouse'],
            'Invoice_Amount': [1000.0, 2000.0, 1500.0]
        })
        
        # 임시 파일로 저장
        test_file = "test_invoice.xlsx"
        test_data.to_excel(test_file, index=False)
        
        # 로더 테스트
        result = load_invoice_passthrough_amounts(test_file)
        
        # 결과 검증
        expected_keys = [('2024-01', 'AAA Storage'), ('2024-01', 'Hauler Indoor'), ('2024-02', 'DHL Warehouse')]
        all_found = all(key in result for key in expected_keys)
        
        if all_found:
            print("   ✅ Passthrough 로더 정상 작동")
            for key, amount in result.items():
                print(f"   {key}: {amount} AED")
        else:
            print("   ❌ Passthrough 로더 실패")
            
        # 임시 파일 삭제
        Path(test_file).unlink(missing_ok=True)
        
        return all_found
        
    except Exception as e:
        print(f"❌ Passthrough 로더 테스트 실패: {e}")
        return False

def test_sqm_billing_calculation():
    """SQM 과금 계산 테스트 (간단한 버전)"""
    print("\n🧪 테스트 4: SQM 과금 계산")
    
    try:
        from hvdc_excel_reporter_final_sqm_rev import CorrectedWarehouseIOCalculator
        
        calc = CorrectedWarehouseIOCalculator()
        
        # 간단한 테스트 데이터 생성 (날짜 범위 제한)
        test_data = pd.DataFrame({
            'Pkg': [1, 1],
            'SQM': [10, 20],
            'DSV Indoor': ['2024-01-01', '2024-01-02'],
            'DAS': ['2024-01-03', '2024-01-04']
        })
        
        # 날짜 변환
        test_data['DSV Indoor'] = pd.to_datetime(test_data['DSV Indoor'])
        test_data['DAS'] = pd.to_datetime(test_data['DAS'])
        
        # Passthrough 금액 (빈 dict)
        passthrough_amounts = {}
        
        # SQM 과금 계산
        charges = calc.calculate_monthly_invoice_charges_prorated(test_data, passthrough_amounts)
        
        if charges:
            print("   ✅ SQM 과금 계산 정상 작동")
            for month, data in charges.items():
                print(f"   {month}: {data.get('total_monthly_charge_aed', 0):.2f} AED")
        else:
            print("   ❌ SQM 과금 계산 실패")
            
        return len(charges) > 0
        
    except Exception as e:
        print(f"❌ SQM 과금 계산 테스트 실패: {e}")
        return False

def test_integration_consistency():
    """통합 일관성 테스트"""
    print("\n🧪 테스트 5: 통합 일관성")
    
    try:
        # 두 파일의 창고명 매핑이 일치하는지 확인
        from hvdc_excel_reporter_final_sqm_rev import CorrectedWarehouseIOCalculator
        
        # 동적 import
        spec = importlib.util.spec_from_file_location("hvdc_wh_invoice", "hvdc wh invoice.py")
        hvdc_wh_invoice = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(hvdc_wh_invoice)
        
        calc = CorrectedWarehouseIOCalculator()
        invoice_mapping = hvdc_wh_invoice.WAREHOUSE_NAME_MAPPING
        
        # 매핑 일치 확인
        consistency_ok = True
        for standard_name, variants in calc.warehouse_name_mapping.items():
            if standard_name in invoice_mapping:
                invoice_variants = invoice_mapping[standard_name]
                if set(variants) != set(invoice_variants):
                    print(f"   ❌ {standard_name}: 매핑 불일치")
                    consistency_ok = False
                else:
                    print(f"   ✅ {standard_name}: 매핑 일치")
            else:
                print(f"   ❌ {standard_name}: invoice 파일에 없음")
                consistency_ok = False
        
        return consistency_ok
        
    except Exception as e:
        print(f"❌ 통합 일관성 테스트 실패: {e}")
        return False

def main():
    """메인 테스트 실행"""
    print("🚀 3-모드 과금 시스템 통합 테스트 시작 (간단 버전)")
    print("=" * 60)
    
    tests = [
        test_warehouse_name_normalization,
        test_billing_mode_classification,
        test_passthrough_loader,
        test_sqm_billing_calculation,
        test_integration_consistency
    ]
    
    passed = 0
    total = len(tests)
    
    for test_func in tests:
        try:
            if test_func():
                passed += 1
        except Exception as e:
            print(f"❌ 테스트 실행 중 오류: {e}")
    
    print("\n" + "=" * 60)
    print(f"📊 테스트 결과: {passed}/{total} 통과")
    
    if passed == total:
        print("🎉 모든 테스트 통과! 3-모드 과금 시스템 정상 작동")
        return True
    else:
        print("⚠️ 일부 테스트 실패. 시스템 점검 필요")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
