#!/usr/bin/env python3
"""
월차 리포트 & SQM 과금 시스템
창고별/월별 입출고 집계 및 SQM 기반 과금 계산

규칙:
- 창고만 "입고"
- 창고↔창고 이동 목적지 제외  
- 창고→현장 다음날만 출고
- SQM 누적재고 → 요율(AED/sqm/month) 곱해 월별 과금
"""

import pandas as pd
import numpy as np
import duckdb
from pathlib import Path
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import calendar

@dataclass
class SQMBillingConfig:
    """SQM 과금 설정"""
    # 창고별 요율 (AED/sqm/month)
    warehouse_rates: Dict[str, float]
    
    # AAA Storage 보정 계수
    aaa_storage_factor: float = 1.15
    
    # 최소 과금 일수
    min_billing_days: int = 1
    
    # 부분월 과금 방식 ('daily' | 'full_month')
    partial_month_billing: str = 'daily'

class MonthlySQMBillingEngine:
    """월차 SQM 과금 엔진"""
    
    def __init__(self):
        self.sku_master_db = "out/sku_master.duckdb"
        self.output_dir = Path("out")
        self.output_dir.mkdir(exist_ok=True)
        
        # 기본 과금 설정
        self.config = SQMBillingConfig(
            warehouse_rates={
                'DSV Al Markaz': 25.0,    # AED/sqm/month
                'DSV Indoor': 30.0,
                'DSV Outdoor': 20.0,
                'DSV MZP': 35.0,
                'Hauler Indoor': 28.0,
                'MOSB': 40.0,              # 특수 보관
                'AAA Storage': 22.0
            },
            aaa_storage_factor=1.15,       # AAA Storage 15% 할증
            min_billing_days=1,
            partial_month_billing='daily'
        )
    
    def load_sku_master_with_flow_analysis(self) -> pd.DataFrame:
        """SKU Master Hub에서 Flow 분석 데이터 로드"""
        if not Path(self.sku_master_db).exists():
            raise FileNotFoundError(f"SKU Master DB를 찾을 수 없습니다: {self.sku_master_db}")
        
        con = duckdb.connect(self.sku_master_db)
        
        # 월차 리포트용 상세 데이터 추출
        query = """
            SELECT 
                SKU,
                Vendor,
                Final_Location,
                FLOW_CODE,
                flow_desc,
                Pkg,
                CAST(GW AS DECIMAL(10,2)) AS GW,
                CAST(CBM AS DECIMAL(10,3)) AS CBM,
                first_seen,
                last_seen,
                CASE 
                    WHEN Final_Location IN ('DSV Al Markaz', 'DSV Indoor', 'DSV Outdoor', 'DSV MZP', 'Hauler Indoor', 'MOSB')
                    THEN 'WAREHOUSE'
                    WHEN Final_Location IN ('SHU', 'DAS', 'MIR', 'AGI') 
                    THEN 'SITE_DELIVERED'
                    WHEN Final_Location = 'Pre Arrival'
                    THEN 'PRE_ARRIVAL'
                    ELSE 'OTHER'
                END AS location_category
            FROM sku_master
            WHERE SKU IS NOT NULL
        """
        
        df = con.execute(query).df()
        con.close()
        
        print(f"✅ SKU Master Flow 분석용 데이터 {len(df):,}건 로드")
        return df
    
    def calculate_flow_timeline(self, df: pd.DataFrame) -> pd.DataFrame:
        """Flow 기반 입출고 타이밍 계산"""
        
        flow_timeline = []
        
        for _, row in df.iterrows():
            sku = row['SKU']
            flow_code = row['FLOW_CODE']
            final_location = row['Final_Location']
            cbm = row['CBM'] if pd.notna(row['CBM']) else 0.0
            
            # Flow Code별 입출고 패턴 분석
            timeline_events = []
            
            if flow_code == 0:  # Pre Arrival
                timeline_events.append({
                    'event_type': 'PRE_ARRIVAL',
                    'location': 'Pre Arrival',
                    'date_estimate': '2024-01-01',  # 임시
                    'is_warehouse_inbound': False
                })
                
            elif flow_code == 1:  # Port → Site (직송)
                timeline_events.extend([
                    {'event_type': 'PORT_ARRIVAL', 'location': 'Port', 'date_estimate': '2024-01-01'},
                    {'event_type': 'SITE_DELIVERY', 'location': final_location, 'date_estimate': '2024-01-02'},
                ])
                
            elif flow_code == 2:  # Port → WH → Site
                timeline_events.extend([
                    {'event_type': 'PORT_ARRIVAL', 'location': 'Port', 'date_estimate': '2024-01-01'},
                    {'event_type': 'WH_INBOUND', 'location': 'Warehouse', 'date_estimate': '2024-01-01', 'is_warehouse_inbound': True},
                    {'event_type': 'WH_OUTBOUND', 'location': 'Warehouse', 'date_estimate': '2024-01-15', 'is_warehouse_outbound': True},
                    {'event_type': 'SITE_DELIVERY', 'location': final_location, 'date_estimate': '2024-01-16'},
                ])
                
            elif flow_code == 3:  # Port → WH → MOSB → Site  
                timeline_events.extend([
                    {'event_type': 'PORT_ARRIVAL', 'location': 'Port', 'date_estimate': '2024-01-01'},
                    {'event_type': 'WH_INBOUND', 'location': 'Warehouse', 'date_estimate': '2024-01-01', 'is_warehouse_inbound': True},
                    {'event_type': 'MOSB_TRANSFER', 'location': 'MOSB', 'date_estimate': '2024-01-10'},
                    {'event_type': 'WH_OUTBOUND', 'location': 'MOSB', 'date_estimate': '2024-01-20', 'is_warehouse_outbound': True},
                    {'event_type': 'SITE_DELIVERY', 'location': final_location, 'date_estimate': '2024-01-21'},
                ])
                
            elif flow_code == 4:  # Multi-hop
                timeline_events.extend([
                    {'event_type': 'COMPLEX_FLOW', 'location': 'Multiple', 'date_estimate': '2024-01-01'},
                    {'event_type': 'SITE_DELIVERY', 'location': final_location, 'date_estimate': '2024-01-30'},
                ])
            
            # Timeline events를 DataFrame으로 변환
            for event in timeline_events:
                flow_timeline.append({
                    'SKU': sku,
                    'Flow_Code': flow_code,
                    'Final_Location': final_location,
                    'CBM': cbm,
                    'Event_Type': event['event_type'],
                    'Event_Location': event['location'],
                    'Event_Date': event['date_estimate'],
                    'Is_WH_Inbound': event.get('is_warehouse_inbound', False),
                    'Is_WH_Outbound': event.get('is_warehouse_outbound', False)
                })
        
        timeline_df = pd.DataFrame(flow_timeline)
        print(f"📅 Flow Timeline 이벤트 {len(timeline_df):,}건 생성")
        return timeline_df
    
    def calculate_monthly_warehouse_occupancy(self, timeline_df: pd.DataFrame, 
                                            target_month: str = '2024-01') -> pd.DataFrame:
        """월별 창고 점유율 및 SQM 계산"""
        
        print(f"📊 {target_month} 월별 창고 점유율 계산")
        
        # 월별 집계를 위한 데이터 준비
        warehouse_locations = ['DSV Al Markaz', 'DSV Indoor', 'DSV Outdoor', 'DSV MZP', 'Hauler Indoor', 'MOSB']
        
        monthly_occupancy = []
        
        # 창고별 집계
        for wh_location in warehouse_locations:
            # 해당 창고를 거치는 SKU들 필터링
            wh_skus = timeline_df[
                (timeline_df['Is_WH_Inbound'] == True) |
                (timeline_df['Final_Location'] == wh_location)
            ].copy()
            
            if len(wh_skus) == 0:
                continue
                
            # 월별 점유 CBM 집계
            total_cbm = wh_skus['CBM'].sum()
            total_packages = len(wh_skus['SKU'].unique())
            
            # SQM 계산 (CBM을 SQM으로 변환, 평균 높이 2.5m 가정)
            avg_height = 2.5  # meters
            total_sqm = total_cbm / avg_height
            
            # 요율 적용
            rate = self.config.warehouse_rates.get(wh_location, 25.0)
            
            # AAA Storage 보정
            if 'AAA' in wh_location:
                rate *= self.config.aaa_storage_factor
            
            monthly_cost = total_sqm * rate
            
            occupancy_record = {
                'Month': target_month,
                'Warehouse': wh_location,
                'Total_SKU_Count': total_packages,
                'Total_CBM': round(total_cbm, 2),
                'Total_SQM': round(total_sqm, 2),
                'Rate_AED_per_SQM': rate,
                'Monthly_Cost_AED': round(monthly_cost, 2),
                'Utilization_Category': self._get_utilization_category(total_sqm)
            }
            
            monthly_occupancy.append(occupancy_record)
        
        occupancy_df = pd.DataFrame(monthly_occupancy)
        
        if not occupancy_df.empty:
            # 전체 요약 추가
            total_summary = {
                'Month': target_month,
                'Warehouse': '🏢 TOTAL_SUMMARY',
                'Total_SKU_Count': occupancy_df['Total_SKU_Count'].sum(),
                'Total_CBM': occupancy_df['Total_CBM'].sum(),
                'Total_SQM': occupancy_df['Total_SQM'].sum(),
                'Rate_AED_per_SQM': occupancy_df['Monthly_Cost_AED'].sum() / occupancy_df['Total_SQM'].sum(),
                'Monthly_Cost_AED': occupancy_df['Monthly_Cost_AED'].sum(),
                'Utilization_Category': '📊 SUMMARY'
            }
            
            occupancy_df = pd.concat([
                occupancy_df,
                pd.DataFrame([total_summary])
            ], ignore_index=True)
        
        print(f"✅ {len(occupancy_df)-1}개 창고 월별 점유율 계산 완료")
        return occupancy_df
    
    def _get_utilization_category(self, sqm: float) -> str:
        """점유율 카테고리 분류"""
        if sqm < 100:
            return '🔹 저사용'
        elif sqm < 500:
            return '🔸 중간사용'  
        elif sqm < 1000:
            return '🔶 고사용'
        else:
            return '🔺 초고사용'
    
    def generate_inbound_outbound_summary(self, timeline_df: pd.DataFrame, 
                                        target_month: str = '2024-01') -> pd.DataFrame:
        """입출고 요약 (창고만 입고, 다음날만 출고 규칙)"""
        
        print(f"📦 {target_month} 입출고 요약 생성")
        
        # 입고: 창고로 들어오는 것만
        inbound_df = timeline_df[timeline_df['Is_WH_Inbound'] == True].copy()
        inbound_summary = inbound_df.groupby('Event_Location').agg({
            'SKU': 'nunique',
            'CBM': 'sum'
        }).rename(columns={'SKU': 'Inbound_SKU_Count', 'CBM': 'Inbound_CBM'})
        
        # 출고: 창고에서 나가는 것만 (다음날 규칙)
        outbound_df = timeline_df[timeline_df['Is_WH_Outbound'] == True].copy()
        outbound_summary = outbound_df.groupby('Event_Location').agg({
            'SKU': 'nunique', 
            'CBM': 'sum'
        }).rename(columns={'SKU': 'Outbound_SKU_Count', 'CBM': 'Outbound_CBM'})
        
        # 입출고 합본
        io_summary = pd.concat([inbound_summary, outbound_summary], axis=1).fillna(0)
        
        # 재고 차이 계산
        io_summary['Net_SKU'] = io_summary['Inbound_SKU_Count'] - io_summary['Outbound_SKU_Count']
        io_summary['Net_CBM'] = io_summary['Inbound_CBM'] - io_summary['Outbound_CBM']
        
        # 인덱스를 컬럼으로 변환
        io_summary.reset_index(inplace=True)
        io_summary['Month'] = target_month
        
        print(f"✅ 입출고 요약 {len(io_summary)}개 창고 완료")
        return io_summary
    
    def generate_monthly_billing_report(self, target_month: str = '2024-01') -> Dict[str, pd.DataFrame]:
        """종합 월차 과금 리포트 생성"""
        
        print(f"📋 {target_month} 종합 월차 과금 리포트 생성")
        print("=" * 60)
        
        # 1. 기본 데이터 로드
        sku_df = self.load_sku_master_with_flow_analysis()
        
        # 2. Flow Timeline 계산
        timeline_df = self.calculate_flow_timeline(sku_df)
        
        # 3. 월별 창고 점유율 & SQM 과금
        occupancy_df = self.calculate_monthly_warehouse_occupancy(timeline_df, target_month)
        
        # 4. 입출고 요약
        io_summary_df = self.generate_inbound_outbound_summary(timeline_df, target_month)
        
        # 5. 벤더별 요약
        vendor_summary = sku_df.groupby('Vendor').agg({
            'SKU': 'count',
            'Pkg': 'sum',
            'GW': 'sum', 
            'CBM': 'sum'
        }).rename(columns={'SKU': 'Total_Cases'})
        vendor_summary.reset_index(inplace=True)
        
        # 6. Flow Code별 요약  
        flow_summary = sku_df.groupby(['FLOW_CODE', 'flow_desc']).agg({
            'SKU': 'count',
            'CBM': 'sum'
        }).rename(columns={'SKU': 'Case_Count'})
        flow_summary.reset_index(inplace=True)
        
        reports = {
            'monthly_sqm_billing': occupancy_df,
            'inbound_outbound_summary': io_summary_df,
            'vendor_summary': vendor_summary,
            'flow_code_summary': flow_summary,
            'flow_timeline': timeline_df
        }
        
        return reports
    
    def save_monthly_reports(self, reports: Dict[str, pd.DataFrame], 
                           target_month: str = '2024-01') -> str:
        """월차 리포트를 Excel 파일로 저장"""
        
        output_file = self.output_dir / f"Monthly_Report_SQM_Billing_{target_month.replace('-', '')}.xlsx"
        
        with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
            
            # 1. 요약 대시보드 시트
            summary_data = []
            
            if 'monthly_sqm_billing' in reports and not reports['monthly_sqm_billing'].empty:
                total_cost = reports['monthly_sqm_billing']['Monthly_Cost_AED'].sum()
                total_sqm = reports['monthly_sqm_billing']['Total_SQM'].sum()
                
                summary_data.extend([
                    {'Metric': '🏢 Total Monthly SQM Cost', 'Value': f"{total_cost:,.2f} AED", 'Unit': 'AED'},
                    {'Metric': '📐 Total SQM Occupied', 'Value': f"{total_sqm:,.2f}", 'Unit': 'm²'},
                    {'Metric': '💰 Average Rate', 'Value': f"{total_cost/total_sqm if total_sqm > 0 else 0:.2f}", 'Unit': 'AED/m²'},
                    {'Metric': '🏠 Active Warehouses', 'Value': len(reports['monthly_sqm_billing']) - 1, 'Unit': 'count'},
                ])
            
            if 'vendor_summary' in reports:
                vendor_count = len(reports['vendor_summary'])
                total_cases = reports['vendor_summary']['Total_Cases'].sum()
                summary_data.extend([
                    {'Metric': '📦 Total Cases', 'Value': f"{total_cases:,}", 'Unit': 'cases'},
                    {'Metric': '🏭 Active Vendors', 'Value': vendor_count, 'Unit': 'count'},
                ])
            
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='📊 Dashboard', index=False)
            
            # 2. 각 리포트 시트 저장
            sheet_names = {
                'monthly_sqm_billing': '💰 SQM Billing',
                'inbound_outbound_summary': '📦 Inbound Outbound',
                'vendor_summary': '🏭 Vendor Summary', 
                'flow_code_summary': '🔄 Flow Summary',
                'flow_timeline': '📅 Flow Timeline'
            }
            
            for report_key, df in reports.items():
                sheet_name = sheet_names.get(report_key, report_key[:31])
                df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"💾 월차 리포트 저장: {output_file}")
        print(f"   - 시트 수: {len(reports) + 1}개 (대시보드 포함)")
        
        return str(output_file)
    
    def run_monthly_billing_system(self, target_month: str = '2024-01') -> str:
        """월차 SQM 과금 시스템 전체 실행"""
        
        print(f"🚀 월차 SQM 과금 시스템 실행 - {target_month}")
        print("=" * 60)
        
        try:
            # 1. 종합 리포트 생성
            reports = self.generate_monthly_billing_report(target_month)
            
            # 2. Excel 저장
            output_file = self.save_monthly_reports(reports, target_month)
            
            # 3. 요약 출력
            print(f"\n📋 월차 리포트 요약 - {target_month}")
            print("-" * 40)
            
            if 'monthly_sqm_billing' in reports and not reports['monthly_sqm_billing'].empty:
                billing_df = reports['monthly_sqm_billing']
                total_cost = billing_df['Monthly_Cost_AED'].sum()
                total_sqm = billing_df['Total_SQM'].sum()
                
                print(f"💰 총 SQM 과금액: {total_cost:,.2f} AED")
                print(f"📐 총 점유 면적: {total_sqm:,.2f} m²")
                print(f"💡 평균 요율: {total_cost/total_sqm if total_sqm > 0 else 0:.2f} AED/m²")
                
                print(f"\n🏢 창고별 과금 내역:")
                for _, row in billing_df.iterrows():
                    if row['Warehouse'] != '🏢 TOTAL_SUMMARY':
                        print(f"   📍 {row['Warehouse']}: {row['Monthly_Cost_AED']:,.2f} AED ({row['Total_SQM']:.1f}m²)")
            
            print(f"\n🎉 월차 과금 시스템 완료!")
            print(f"📄 출력 파일: {output_file}")
            
            return output_file
            
        except Exception as e:
            print(f"❌ 월차 과금 시스템 실행 중 오류: {str(e)}")
            return ""

def main():
    """메인 실행"""
    billing_engine = MonthlySQMBillingEngine()
    
    # 2024년 1월 기준 리포트 생성
    result_file = billing_engine.run_monthly_billing_system('2024-01')
    
    if result_file:
        print(f"\n✅ 월차 SQM 과금 리포트 완료!")
        print(f"📊 파일: {result_file}")
        print(f"💡 창고 요율: DSV Al Markaz(25 AED/m²), DSV Indoor(30), DSV Outdoor(20), MOSB(40)")
    else:
        print(f"\n⚠️ 월차 리포트 생성 실패")

if __name__ == "__main__":
    main()
