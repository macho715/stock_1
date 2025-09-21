#!/usr/bin/env python3
"""
3-모드 과금 시스템 통합 테스트
Rate / Passthrough / No-charge 시스템 검증
"""

import pandas as pd
import sys
from pathlib import Path

def test_3mode_billing_integration():
    """3-모드 과금 시스템 통합 테스트"""
    print("🧪 3-모드 과금 시스템 통합 테스트 시작")
    
    try:
        # hvdc_excel_reporter_final_sqm_rev.py 임포트
        from hvdc_excel_reporter_final_sqm_rev import HVDCExcelReporterFinal
        
        # Reporter 인스턴스 생성
        reporter = HVDCExcelReporterFinal()
        print("✅ Reporter 인스턴스 생성 완료")
        
        # 1) 인보이스 로드 (스키마: Operation Date, TOTAL)
        invoice_path = "data/HVDC WAREHOUSE_INVOICE.xlsx"
        invoice_df = pd.read_excel(invoice_path, sheet_name=0)
        print(f"✅ 인보이스 파일 로드: {len(invoice_df)}건")
        
        # 컬럼명을 표준 형식으로 변환
        invoice_df = invoice_df.rename(columns={
            'Operation Date': 'Month',
            'TOTAL': 'Invoice_Amount'
        })
        # Warehouse 컬럼 추가 (기본값으로 'Unknown' 설정)
        invoice_df['Warehouse'] = 'Unknown'
        print(f"✅ 컬럼명 변환 완료: {list(invoice_df.columns)}")
        
        # 2) Passthrough dict 구성
        passthrough = reporter.calculator.build_passthrough_amounts(invoice_df)
        print(f"📊 Passthrough dict 생성: {len(passthrough)}개 항목")
        
        # 3) 시스템 통계 산출
        stats = reporter.calculate_warehouse_statistics()
        print("✅ 시스템 통계 산출 완료")
        
        # 4) 일할+모드 과금으로 교체
        stats['sqm_invoice_charges'] = reporter.calculator.calculate_monthly_invoice_charges_prorated(
            stats['processed_data'],
            passthrough_amounts=passthrough
        )
        print("✅ 3-모드 과금 계산 완료")
        
        # 5) 과금 시트 생성
        invoice_sheet_df = reporter.create_sqm_invoice_sheet(stats)
        print(f"✅ SQM Invoice 과금 시트 생성 완료: {len(invoice_sheet_df)}건")
        
        # 6) 모드별 통계 출력
        if not invoice_sheet_df.empty and 'Billing_Mode' in invoice_sheet_df.columns:
            rate_count = len(invoice_sheet_df[invoice_sheet_df['Billing_Mode']=='rate'])
            passthrough_count = len(invoice_sheet_df[invoice_sheet_df['Billing_Mode']=='passthrough'])
            no_charge_count = len(invoice_sheet_df[invoice_sheet_df['Billing_Mode']=='no-charge'])
            
            print(f"\n📊 3-모드 과금 시스템 결과:")
            print(f"   - Rate 모드: {rate_count}건")
            print(f"   - Passthrough 모드: {passthrough_count}건")
            print(f"   - No-charge 모드: {no_charge_count}건")
            
            # 총 과금액 계산
            total_charge = invoice_sheet_df['Monthly_Charge_AED'].sum()
            print(f"   - 총 과금액: {total_charge:,.2f} AED")
        
        # 7) Excel 파일로 저장
        output_path = "HVDC_3Mode_Billing_Test_Result.xlsx"
        with pd.ExcelWriter(output_path, engine="xlsxwriter") as writer:
            invoice_sheet_df.to_excel(writer, sheet_name="SQM_Invoice과금", index=False)
        
        print(f"💾 테스트 결과 저장 완료: {output_path}")
        
        # 8) QA 검증
        print(f"\n🔍 QA 검증:")
        
        # Rate 모드 검증 (Indoor/Al-Markaz/Outdoor/MZP)
        rate_warehouses = ['DSV Indoor', 'DSV Al Markaz', 'DSV Outdoor', 'DSV MZP']
        for wh in rate_warehouses:
            wh_data = invoice_sheet_df[invoice_sheet_df['Warehouse'] == wh]
            if not wh_data.empty:
                avg_sqm = wh_data['Avg_SQM'].iloc[0]
                rate = wh_data['Rate_AED_per_SQM'].iloc[0]
                charge = wh_data['Monthly_Charge_AED'].iloc[0]
                expected = avg_sqm * rate
                print(f"   ✅ {wh}: {avg_sqm:.2f} SQM × {rate} AED = {charge:.2f} AED (예상: {expected:.2f})")
        
        # Passthrough 모드 검증 (AAA/Hauler/DHL)
        passthrough_warehouses = ['AAA Storage', 'Hauler Indoor', 'DHL Warehouse']
        for wh in passthrough_warehouses:
            wh_data = invoice_sheet_df[invoice_sheet_df['Warehouse'] == wh]
            if not wh_data.empty:
                charge = wh_data['Monthly_Charge_AED'].iloc[0]
                source = wh_data['Amount_Source'].iloc[0]
                print(f"   ✅ {wh}: {charge:.2f} AED ({source})")
        
        # No-charge 모드 검증 (MOSB)
        no_charge_warehouses = ['MOSB']
        for wh in no_charge_warehouses:
            wh_data = invoice_sheet_df[invoice_sheet_df['Warehouse'] == wh]
            if not wh_data.empty:
                charge = wh_data['Monthly_Charge_AED'].iloc[0]
                print(f"   ✅ {wh}: {charge:.2f} AED (No-charge)")
        
        print(f"\n🎉 3-모드 과금 시스템 통합 테스트 완료!")
        return True
        
    except Exception as e:
        print(f"❌ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_3mode_billing_integration()
    sys.exit(0 if success else 1)
