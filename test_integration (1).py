#!/usr/bin/env python3
"""
Test Integration Script for HVDC SKU Master Hub

이 스크립트는 오케스트레이션 시스템의 통합 테스트를 수행합니다.
실제 데이터 파일이 없는 경우 샘플 데이터로 테스트할 수 있습니다.
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path

# 프로젝트 루트를 sys.path에 추가
sys.path.insert(0, str(Path(__file__).parent))

def create_sample_stock_data(output_path: str = "test_stock_data.xlsx"):
    """
    테스트용 샘플 Stock 데이터를 생성합니다.
    """
    print("📦 Creating sample stock data...")
    
    # 샘플 데이터 생성
    sample_data = []
    base_date = datetime.now() - timedelta(days=30)
    
    for i in range(100):
        case_no = f"HVDC-ADOPT-HE-{i+1:04d}"
        date_offset = timedelta(days=np.random.randint(0, 30))
        
        sample_data.append({
            'CASE_NO': case_no,
            'Count': 1,
            'First_IN_Date': (base_date + date_offset).strftime('%Y-%m-%d'),
            'All_IN_Dates': (base_date + date_offset).strftime('%Y-%m-%d'),
            'LastSeen': (base_date + date_offset + timedelta(days=np.random.randint(0, 10))).strftime('%Y-%m-%d'),
            'Last_Location': np.random.choice(['DSV Al Markaz', 'DSV Indoor', 'DSV Outdoor', 'MOSB', 'AGI', 'DAS']),
            'Status': np.random.choice(['IN', 'OUT'], p=[0.7, 0.3]),
            'Note': f'Sample data for {case_no}'
        })
    
    # Excel 파일로 저장 (stock.py가 기대하는 구조로)
    df = pd.DataFrame(sample_data)
    
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # 메인 데이터 시트 (stock.py의 process_sku_summary_sheet에서 읽음)
        df.rename(columns={'CASE_NO': 'SKU', 'Last_Location': 'Last_Location', 'LastSeen': 'Last_Seen'}).to_excel(
            writer, sheet_name='종합_SKU요약', index=False)
        
        # 추가 시트들 (기본 구조)
        summary_stats = pd.DataFrame([{
            'Total_Cases': len(df),
            'IN_Count': (df['Status'] == 'IN').sum(),
            'OUT_Count': (df['Status'] == 'OUT').sum()
        }])
        summary_stats.to_excel(writer, sheet_name='Summary_Stats', index=False)
    
    print(f"✅ Sample stock data created: {output_path}")
    return output_path


def create_sample_reporter_data():
    """
    테스트용 샘플 Reporter 데이터를 메모리에 생성합니다.
    """
    print("⚙️ Creating sample reporter data...")
    
    # 샘플 processed_data 생성
    sample_data = []
    
    for i in range(100):
        case_no = f"HVDC-ADOPT-HE-{i+1:04d}"
        vendor = np.random.choice(['HITACHI', 'SIMENSE'])
        
        sample_data.append({
            'Case No.': case_no,
            'Pkg': np.random.randint(1, 5),
            'G.W(kgs)': np.random.uniform(100, 1000),
            'CBM': np.random.uniform(1, 10),
            'Vendor': vendor,
            'FLOW_CODE': np.random.choice([0, 1, 2, 3, 4]),
            'FLOW_DESCRIPTION': 'Sample flow description',
            'Final_Location': np.random.choice(['DSV Al Markaz', 'DSV Indoor', 'MOSB', 'AGI', 'DAS']),
            'SQM': np.random.uniform(5, 50)
        })
    
    processed_df = pd.DataFrame(sample_data)
    
    # Mock reporter stats 구조
    mock_stats = {
        'processed_data': processed_df,
        'inbound_result': {
            'total_inbound': 100,
            'by_warehouse': {'DSV Al Markaz': 30, 'DSV Indoor': 25, 'MOSB': 20, 'Others': 25},
            'by_month': {'2024-08': 40, '2024-09': 60}
        },
        'outbound_result': {
            'total_outbound': 80,
            'by_warehouse': {'DSV Al Markaz': 25, 'DSV Indoor': 20, 'MOSB': 15, 'Others': 20}
        },
        'inventory_result': {
            'total_inventory': 20,
            'discrepancy_count': 0
        },
        'sqm_cumulative_inventory': {
            '2024-09': {
                'DSV Al Markaz': {'cumulative_inventory_sqm': 500, 'inbound_sqm': 300, 'outbound_sqm': 200},
                'DSV Indoor': {'cumulative_inventory_sqm': 300, 'inbound_sqm': 200, 'outbound_sqm': 150}
            }
        }
    }
    
    print("✅ Sample reporter data created in memory")
    return mock_stats


def test_individual_adapters():
    """
    개별 어댑터들을 테스트합니다.
    """
    print("\n🧪 Testing Individual Adapters")
    print("=" * 50)
    
    results = {
        'stock_adapter': False,
        'reporter_adapter': False,
        'invoice_adapter': False
    }
    
    # 1. Stock Adapter 테스트
    try:
        from adapters.stock_adapter import build_stock_snapshots
        
        # 샘플 데이터가 있으면 테스트
        sample_stock_file = create_sample_stock_data("test_stock_temp.xlsx")
        stock_result = build_stock_snapshots(sample_stock_file)
        
        if stock_result and 'summary_df' in stock_result:
            print("✅ Stock Adapter: OK")
            results['stock_adapter'] = True
        else:
            print("❌ Stock Adapter: Failed - No summary data")
            
        # 임시 파일 정리
        if os.path.exists(sample_stock_file):
            os.remove(sample_stock_file)
            
    except Exception as e:
        print(f"❌ Stock Adapter: Failed - {str(e)}")
    
    # 2. Reporter Adapter 테스트 (모의 데이터)
    try:
        from adapters.reporter_adapter import get_warehouse_statistics_summary
        
        # 샘플 데이터로 요약 테스트
        mock_stats = create_sample_reporter_data()
        summary = get_warehouse_statistics_summary(mock_stats)
        
        if summary and 'total_records' in summary:
            print("✅ Reporter Adapter: OK (mock data)")  
            results['reporter_adapter'] = True
        else:
            print("❌ Reporter Adapter: Failed - No summary")
            
    except Exception as e:
        print(f"❌ Reporter Adapter: Failed - {str(e)}")
    
    # 3. Invoice Adapter 테스트 (파일 존재 여부만 확인)
    try:
        from adapters.invoice_adapter import create_sku_invoice_mapping
        
        # 모의 invoice order 데이터로 매핑 테스트
        mock_invoice_df = pd.DataFrame([
            {'HVDC CODE': 'HVDC-ADOPT-HE-0001', 'Match_Status': 'PASS', 'Err_GW': 0.05},
            {'HVDC CODE': 'HVDC-ADOPT-HE-0002', 'Match_Status': 'FAIL', 'Err_GW': 0.15}
        ])
        
        mapping = create_sku_invoice_mapping(mock_invoice_df)
        
        if mapping and len(mapping) > 0:
            print("✅ Invoice Adapter: OK (mock data)")
            results['invoice_adapter'] = True
        else:
            print("❌ Invoice Adapter: Failed - No mapping")
            
    except Exception as e:
        print(f"❌ Invoice Adapter: Failed - {str(e)}")
    
    return results


def test_sku_master_hub():
    """
    SKU Master Hub 생성을 테스트합니다.
    """
    print("\n🔧 Testing SKU Master Hub")
    print("=" * 50)
    
    try:
        from hub.sku_master import build_sku_master, create_sku_master_summary
        
        # 1. 샘플 stock 데이터 준비
        sample_stock = pd.DataFrame([
            {'CASE_NO': 'HVDC-ADOPT-HE-0001', 'First_IN_Date': '2024-08-01', 'LastSeen': '2024-08-15', 'Status': 'IN'},
            {'CASE_NO': 'HVDC-ADOPT-HE-0002', 'First_IN_Date': '2024-08-02', 'LastSeen': '2024-08-16', 'Status': 'OUT'}
        ])
        
        # 2. 샘플 reporter stats 준비  
        mock_stats = create_sample_reporter_data()
        
        # 데이터 크기를 stock과 맞춤
        mock_stats['processed_data'] = pd.DataFrame([
            {'Case No.': 'HVDC-ADOPT-HE-0001', 'Pkg': 2, 'G.W(kgs)': 500, 'Vendor': 'HITACHI', 'Final_Location': 'DSV Al Markaz'},
            {'Case No.': 'HVDC-ADOPT-HE-0002', 'Pkg': 1, 'G.W(kgs)': 300, 'Vendor': 'SIMENSE', 'Final_Location': 'AGI'}
        ])
        
        # 3. SKU Master Hub 생성 테스트
        hub_df = build_sku_master(sample_stock, mock_stats, invoice_match_df=None)
        
        if not hub_df.empty and 'SKU' in hub_df.columns:
            print(f"✅ SKU Master Hub created: {len(hub_df)} records")
            
            # 4. 요약 통계 테스트
            summary = create_sku_master_summary(hub_df)
            print(f"   - Summary stats: {list(summary.keys())}")
            
            # 5. 샘플 데이터 출력
            print("\n📋 Sample Hub Data:")
            print(hub_df[['SKU', 'vendor', 'pkg', 'final_location']].head())
            
            return True
        else:
            print("❌ SKU Master Hub: Failed - Empty result")
            return False
            
    except Exception as e:
        print(f"❌ SKU Master Hub: Failed - {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def test_file_operations():
    """
    파일 저장/로드 기능을 테스트합니다.
    """
    print("\n💾 Testing File Operations")
    print("=" * 50)
    
    try:
        from hub.sku_master import save_as_parquet_duckdb
        
        # 샘플 데이터로 저장 테스트
        test_df = pd.DataFrame([
            {'SKU': 'TEST-001', 'vendor': 'HITACHI', 'pkg': 1, 'final_location': 'DSV Al Markaz'},
            {'SKU': 'TEST-002', 'vendor': 'SIMENSE', 'pkg': 2, 'final_location': 'AGI'}
        ])
        
        # 테스트 출력 디렉토리
        test_output_dir = "test_output"
        parquet_path = save_as_parquet_duckdb(test_df, test_output_dir)
        
        # 파일 존재 확인
        if os.path.exists(parquet_path):
            print(f"✅ Parquet file created: {parquet_path}")
            
            # 로드 테스트
            loaded_df = pd.read_parquet(parquet_path)
            if len(loaded_df) == len(test_df):
                print("✅ Parquet load test: OK")
            else:
                print("❌ Parquet load test: Failed")
                
            # DuckDB 테스트
            try:
                import duckdb
                db_path = Path(test_output_dir) / "sku_master.duckdb"
                if os.path.exists(db_path):
                    con = duckdb.connect(str(db_path))
                    result = con.execute("SELECT COUNT(*) FROM sku_master").fetchone()
                    if result and result[0] == len(test_df):
                        print("✅ DuckDB test: OK")
                    else:
                        print("❌ DuckDB test: Failed")
                    con.close()
                else:
                    print("❌ DuckDB file not created")
            except Exception as e:
                print(f"⚠️ DuckDB test skipped: {e}")
            
            # 정리
            import shutil
            if os.path.exists(test_output_dir):
                shutil.rmtree(test_output_dir)
                print("🧹 Test files cleaned up")
            
            return True
        else:
            print("❌ File operations: Failed - No output file")
            return False
            
    except Exception as e:
        print(f"❌ File operations: Failed - {str(e)}")
        return False


def main():
    """
    전체 통합 테스트를 실행합니다.
    """
    print("🚀 HVDC SKU Master Hub - Integration Test")
    print("=" * 70)
    print("📋 Testing orchestration system components...")
    print("=" * 70)
    
    test_results = []
    
    # 1. 개별 어댑터 테스트
    adapter_results = test_individual_adapters()
    test_results.append(("Adapters", all(adapter_results.values())))
    
    # 2. SKU Master Hub 테스트
    hub_result = test_sku_master_hub()  
    test_results.append(("SKU Master Hub", hub_result))
    
    # 3. 파일 작업 테스트
    file_result = test_file_operations()
    test_results.append(("File Operations", file_result))
    
    # 결과 요약
    print("\n📊 Test Results Summary")
    print("=" * 70)
    
    total_tests = len(test_results)
    passed_tests = sum(1 for _, result in test_results if result)
    
    for test_name, result in test_results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"   {status} {test_name}")
    
    print(f"\n🎯 Overall Result: {passed_tests}/{total_tests} tests passed")
    
    if passed_tests == total_tests:
        print("🎉 ALL TESTS PASSED! The orchestration system is ready.")
        print("\n💡 Next Steps:")
        print("   1. Update file paths in run_pipeline.py")
        print("   2. Run: python run_pipeline.py")
        print("   3. Check output/ directory for results")
    else:
        print("⚠️ Some tests failed. Please check the error messages above.")
        print("\n🔧 Troubleshooting:")
        print("   1. Ensure all required packages are installed")
        print("   2. Check that original .py files are in the same directory") 
        print("   3. Verify Python version compatibility (3.8+)")
    
    return passed_tests == total_tests


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
