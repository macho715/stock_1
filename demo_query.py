import duckdb

print("🚀 HVDC SKU Master Hub - 즉시 사용 가능한 쿼리 예시")
print("=" * 60)

con = duckdb.connect('out/sku_master.duckdb')

# 창고별 현황
print("\n📦 DSV 창고별 현재 보관 현황:")
result = con.execute("""
    SELECT Final_Location, COUNT(*) as cases, 
           ROUND(SUM(CBM), 2) as total_cbm,
           ROUND(AVG(GW), 0) as avg_weight
    FROM sku_master 
    WHERE Final_Location LIKE 'DSV%' 
    GROUP BY Final_Location 
    ORDER BY cases DESC
""").fetchall()

for location, cases, cbm, weight in result:
    print(f"   📍 {location}: {cases:,}건, {cbm:,}m³, 평균 {weight:,}kg")

# 현장 배송 현황  
print(f"\n🏗️ 현장 배송 완료 현황:")
result = con.execute("""
    SELECT Final_Location, COUNT(*) as cases
    FROM sku_master 
    WHERE Final_Location IN ('SHU', 'DAS', 'MIR', 'AGI')
    GROUP BY Final_Location 
    ORDER BY cases DESC
""").fetchall()

for location, cases in result:
    print(f"   🎯 {location}: {cases:,}건 배송 완료")

con.close()

print(f"\n💡 더 많은 쿼리는 다음 파일에서 확인:")
print(f"   📄 quick_queries.sql (DuckDB에서 직접 실행)")
print(f"   🔍 kpi_validation.py (전체 KPI 재검증)")
