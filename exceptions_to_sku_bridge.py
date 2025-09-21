#!/usr/bin/env python3
"""
Exceptions→SKU 귀속 브릿지 스크립트
HVDC Invoice Validation Dashboard의 예외 케이스를 SKU 축에 매핑
"""

import pandas as pd
import numpy as np
import duckdb
from pathlib import Path
import re
from typing import Dict, List, Tuple, Optional

class ExceptionsToSKUBridge:
    """Invoice Exceptions를 SKU에 귀속시키는 브릿지"""
    
    def __init__(self):
        self.sku_master_db = "out/sku_master.duckdb"
        self.invoice_dashboard = "HVDC_Invoice_Validation_Dashboard.xlsx"
        self.output_dir = Path("out")
        self.output_dir.mkdir(exist_ok=True)
        
    def load_sku_master(self) -> pd.DataFrame:
        """SKU Master Hub에서 SKU-HVDC Code 매핑 로드"""
        if not Path(self.sku_master_db).exists():
            raise FileNotFoundError(f"SKU Master DB를 찾을 수 없습니다: {self.sku_master_db}")
        
        con = duckdb.connect(self.sku_master_db)
        
        # SKU와 관련 정보 추출 (HVDC Code 역추적용)
        query = """
            SELECT DISTINCT
                SKU,
                Vendor,
                Final_Location,
                FLOW_CODE,
                Pkg,
                GW,
                CBM
            FROM sku_master
            WHERE SKU IS NOT NULL
        """
        
        df = con.execute(query).df()
        con.close()
        
        print(f"✅ SKU Master에서 {len(df):,}개 SKU 로드 완료")
        return df
    
    def load_exceptions_from_dashboard(self) -> pd.DataFrame:
        """Invoice Dashboard에서 Exceptions_Only 시트 로드"""
        if not Path(self.invoice_dashboard).exists():
            print(f"⚠️ Invoice Dashboard를 찾을 수 없습니다: {self.invoice_dashboard}")
            return pd.DataFrame()
        
        try:
            # Exceptions_Only 시트 읽기
            exceptions_df = pd.read_excel(
                self.invoice_dashboard, 
                sheet_name='Exceptions_Only'
            )
            print(f"✅ Exceptions_Only 시트에서 {len(exceptions_df):,}건 로드")
            
            # 주요 컬럼 확인
            print(f"📋 Exceptions 컬럼: {list(exceptions_df.columns[:10])}")
            
            return exceptions_df
            
        except Exception as e:
            print(f"❌ Exceptions 로드 중 오류: {str(e)}")
            return pd.DataFrame()
    
    def expand_hvdc_codes(self, hvdc_code_raw: str) -> List[str]:
        """HVDC CODE 확장 (예: '0087,90' → ['0087', '0090'])"""
        if pd.isna(hvdc_code_raw) or not str(hvdc_code_raw).strip():
            return []
        
        codes = []
        code_str = str(hvdc_code_raw).strip()
        
        # 쉼표로 분리된 코드들 처리
        if ',' in code_str:
            parts = [p.strip() for p in code_str.split(',')]
            base_code = parts[0]
            
            # 첫 번째 코드 추가
            if base_code:
                codes.append(base_code.zfill(4))
            
            # 나머지 코드들 확장
            for part in parts[1:]:
                if len(part) == 2:  # '90' 형태
                    expanded_code = base_code[:2] + part
                    codes.append(expanded_code.zfill(4))
                elif len(part) >= 3:  # '0090' 형태
                    codes.append(part.zfill(4))
        else:
            # 단일 코드
            codes.append(code_str.zfill(4))
        
        return list(set(codes))  # 중복 제거
    
    def create_sku_hvdc_mapping(self, sku_df: pd.DataFrame) -> Dict[str, List[str]]:
        """SKU에서 HVDC Code를 역추적하여 매핑 생성"""
        # SKU 패턴에서 HVDC Code 추출 (예: 'EXFU562524-3' → '5625')
        sku_hvdc_map = {}
        
        for _, row in sku_df.iterrows():
            sku = str(row['SKU'])
            
            # SKU에서 숫자 패턴 추출 시도
            hvdc_codes = []
            
            # 패턴 1: EXFU562524-3 → 5625
            match1 = re.search(r'[A-Z]+(\d{4})', sku)
            if match1:
                hvdc_codes.append(match1.group(1))
            
            # 패턴 2: 더 복잡한 패턴들 추가 가능
            # match2 = re.search(r'패턴2', sku)
            
            if hvdc_codes:
                sku_hvdc_map[sku] = hvdc_codes
        
        print(f"📍 {len(sku_hvdc_map):,}개 SKU에서 HVDC Code 매핑 생성")
        return sku_hvdc_map
    
    def match_exceptions_to_sku(self, exceptions_df: pd.DataFrame, 
                               sku_df: pd.DataFrame) -> pd.DataFrame:
        """Exceptions를 SKU에 매핑"""
        if exceptions_df.empty:
            print("⚠️ Exceptions 데이터가 없습니다")
            return pd.DataFrame()
        
        # SKU-HVDC Code 매핑 생성
        sku_hvdc_map = self.create_sku_hvdc_mapping(sku_df)
        
        # 역방향 매핑 생성 (HVDC Code → SKU)
        hvdc_sku_map = {}
        for sku, hvdc_codes in sku_hvdc_map.items():
            for hvdc_code in hvdc_codes:
                if hvdc_code not in hvdc_sku_map:
                    hvdc_sku_map[hvdc_code] = []
                hvdc_sku_map[hvdc_code].append(sku)
        
        print(f"🔄 {len(hvdc_sku_map):,}개 HVDC Code → SKU 매핑 생성")
        
        # Exceptions 매핑 시도
        matched_exceptions = []
        
        hvdc_col = None
        for col in exceptions_df.columns:
            if 'HVDC' in col.upper() or 'CODE' in col.upper():
                hvdc_col = col
                break
        
        if hvdc_col is None:
            print("❌ HVDC Code 컬럼을 찾을 수 없습니다")
            return pd.DataFrame()
        
        print(f"🔍 HVDC Code 컬럼 사용: {hvdc_col}")
        
        for idx, row in exceptions_df.iterrows():
            hvdc_raw = row.get(hvdc_col, '')
            expanded_codes = self.expand_hvdc_codes(hvdc_raw)
            
            matched_skus = []
            for hvdc_code in expanded_codes:
                if hvdc_code in hvdc_sku_map:
                    matched_skus.extend(hvdc_sku_map[hvdc_code])
            
            # 매칭된 SKU가 있으면 각 SKU에 대해 레코드 생성
            if matched_skus:
                for sku in set(matched_skus):  # 중복 제거
                    exception_record = {
                        'SKU': sku,
                        'Invoice_Codes': hvdc_raw,
                        'Expanded_Codes': ','.join(expanded_codes),
                        'Err_GW': row.get('Err_GW', 0.0) if 'Err_GW' in row else 0.0,
                        'Err_CBM': row.get('Err_CBM', 0.0) if 'Err_CBM' in row else 0.0,
                        'Match_Status': 'FAIL',
                        'Original_Row_Index': idx
                    }
                    
                    # 추가 컬럼들 복사
                    for col in exceptions_df.columns:
                        if col not in exception_record:
                            exception_record[f'Orig_{col}'] = row[col]
                    
                    matched_exceptions.append(exception_record)
        
        if matched_exceptions:
            result_df = pd.DataFrame(matched_exceptions)
            print(f"✅ {len(matched_exceptions):,}건의 Exception→SKU 매핑 완료")
            return result_df
        else:
            print("⚠️ 매핑된 Exception이 없습니다")
            return pd.DataFrame()
    
    def save_exceptions_by_sku(self, exceptions_sku_df: pd.DataFrame) -> str:
        """Exceptions by SKU 결과를 Parquet으로 저장"""
        if exceptions_sku_df.empty:
            print("💡 저장할 Exception 데이터가 없습니다")
            return ""
        
        output_path = self.output_dir / "exceptions_by_sku.parquet"
        exceptions_sku_df.to_parquet(output_path, index=False)
        
        print(f"💾 Exception by SKU 저장: {output_path}")
        print(f"   - 총 {len(exceptions_sku_df):,}건")
        print(f"   - 고유 SKU: {exceptions_sku_df['SKU'].nunique():,}개")
        
        # DuckDB에도 로드
        try:
            con = duckdb.connect(self.sku_master_db)
            con.execute("DROP TABLE IF EXISTS exceptions_by_sku")
            con.execute(f"""
                CREATE TABLE exceptions_by_sku AS
                SELECT * FROM read_parquet('{output_path}')
            """)
            con.close()
            print(f"✅ DuckDB에 exceptions_by_sku 테이블 생성 완료")
        except Exception as e:
            print(f"⚠️ DuckDB 로드 중 오류: {str(e)}")
        
        return str(output_path)
    
    def run_exceptions_bridge(self) -> str:
        """전체 Exceptions→SKU 브릿지 실행"""
        print("🌉 Exceptions→SKU 귀속 브릿지 시작")
        print("=" * 60)
        
        try:
            # 1. SKU Master 로드
            sku_df = self.load_sku_master()
            
            # 2. Exceptions 로드
            exceptions_df = self.load_exceptions_from_dashboard()
            
            if exceptions_df.empty:
                # 샘플 Exceptions 생성 (테스트용)
                print("📝 샘플 Exceptions 데이터 생성 (테스트용)")
                sample_exceptions = pd.DataFrame([
                    {
                        'HVDC_Code': '5625,24',
                        'Err_GW': 15.5,
                        'Err_CBM': 0.8,
                        'Status': 'FAIL'
                    },
                    {
                        'HVDC_Code': '1234',
                        'Err_GW': -22.3,
                        'Err_CBM': 1.2,
                        'Status': 'FAIL'
                    }
                ])
                exceptions_df = sample_exceptions
                print(f"🧪 샘플 데이터 {len(exceptions_df)}건 생성")
            
            # 3. 매핑 실행
            exceptions_sku_df = self.match_exceptions_to_sku(exceptions_df, sku_df)
            
            # 4. 저장
            output_path = self.save_exceptions_by_sku(exceptions_sku_df)
            
            print("\n🎯 브릿지 완료 요약")
            print("-" * 40)
            print(f"✅ 입력 Exceptions: {len(exceptions_df):,}건")
            print(f"✅ 매핑 결과: {len(exceptions_sku_df):,}건")
            print(f"✅ 출력 파일: {output_path}")
            
            if not exceptions_sku_df.empty:
                print(f"\n💡 DuckDB 사용 예시:")
                print(f"```sql")
                print(f"-- Top 10 오차 SKU")
                print(f"SELECT SKU, Err_GW, Err_CBM, Invoice_Codes")
                print(f"FROM exceptions_by_sku")
                print(f"ORDER BY (ABS(Err_GW) + ABS(Err_CBM)) DESC")
                print(f"LIMIT 10;")
                print(f"```")
            
            return output_path
            
        except Exception as e:
            print(f"❌ 브릿지 실행 중 오류: {str(e)}")
            return ""

def main():
    """메인 실행"""
    bridge = ExceptionsToSKUBridge()
    result_path = bridge.run_exceptions_bridge()
    
    if result_path:
        print(f"\n🎉 Exceptions→SKU 귀속 완료!")
        print(f"📄 결과 파일: {result_path}")
        print(f"🔍 DuckDB 테이블: exceptions_by_sku")
    else:
        print(f"\n⚠️ 브릿지 실행이 완료되지 못했습니다")

if __name__ == "__main__":
    main()
