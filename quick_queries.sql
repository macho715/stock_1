-- HVDC SKU Master Hub - 즉시 실행 가능한 DuckDB 쿼리들
-- 사용법: duckdb out/sku_master.duckdb < quick_queries.sql

-- ===========================================
-- 1. 일일 운영 현황 조회
-- ===========================================

.print '🏢 창고별 현재 재고 현황'
SELECT 
    Final_Location,
    COUNT(*) AS case_count,
    SUM(Pkg) AS total_packages,
    ROUND(AVG(GW), 2) AS avg_weight,
    ROUND(SUM(CBM), 2) AS total_cbm
FROM sku_master 
WHERE Final_Location LIKE 'DSV%' OR Final_Location = 'MOSB'
GROUP BY Final_Location
ORDER BY case_count DESC;

-- ===========================================
-- 2. Flow Code별 물류 현황
-- ===========================================

.print '🔄 물류 경로별 처리 현황'
SELECT 
    FLOW_CODE,
    CASE 
        WHEN FLOW_CODE = 0 THEN 'Pre Arrival'
        WHEN FLOW_CODE = 1 THEN 'Port → Site (직송)'
        WHEN FLOW_CODE = 2 THEN 'Port → WH → Site'
        WHEN FLOW_CODE = 3 THEN 'Port → WH → MOSB → Site'
        WHEN FLOW_CODE = 4 THEN 'Multi-hop (복잡)'
        ELSE 'Unknown'
    END AS flow_description,
    COUNT(*) AS case_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage,
    ROUND(AVG(GW), 2) AS avg_weight,
    ROUND(SUM(CBM), 2) AS total_volume
FROM sku_master 
GROUP BY FLOW_CODE, flow_description
ORDER BY FLOW_CODE;

-- ===========================================
-- 3. 현장별 배송 완료 현황
-- ===========================================

.print '🏗️ 현장별 배송 완료 현황'
SELECT 
    Final_Location,
    COUNT(*) AS delivered_cases,
    SUM(Pkg) AS total_packages,
    ROUND(SUM(GW)/1000, 2) AS total_weight_tons,
    ROUND(SUM(CBM), 2) AS total_volume_cbm
FROM sku_master 
WHERE Final_Location IN ('SHU', 'DAS', 'MIR', 'AGI')
GROUP BY Final_Location
ORDER BY delivered_cases DESC;

-- ===========================================
-- 4. 창고 효율성 분석
-- ===========================================

.print '📦 창고별 처리 효율성'
WITH warehouse_summary AS (
    SELECT 
        Final_Location,
        COUNT(*) AS cases_stored,
        ROUND(AVG(GW), 2) AS avg_case_weight,
        ROUND(SUM(CBM), 2) AS total_space_used,
        ROUND(SUM(CBM) / COUNT(*), 3) AS avg_space_per_case
    FROM sku_master 
    WHERE Final_Location IN ('DSV Al Markaz', 'DSV Indoor', 'DSV Outdoor', 'Hauler Indoor', 'MOSB')
    GROUP BY Final_Location
)
SELECT 
    Final_Location AS warehouse,
    cases_stored,
    avg_case_weight,
    total_space_used,
    avg_space_per_case,
    CASE 
        WHEN avg_space_per_case < 1.0 THEN '🔥 고효율'
        WHEN avg_space_per_case < 2.0 THEN '✅ 양호'
        ELSE '⚠️ 개선필요'
    END AS efficiency_grade
FROM warehouse_summary
ORDER BY avg_space_per_case;

-- ===========================================
-- 5. 중량별 케이스 분포
-- ===========================================

.print '⚖️ 중량별 케이스 분포 분석'
SELECT 
    CASE 
        WHEN GW < 500 THEN '경량 (<500kg)'
        WHEN GW < 1000 THEN '중간 (500-1000kg)'
        WHEN GW < 2000 THEN '중량 (1-2톤)'
        WHEN GW < 5000 THEN '대형 (2-5톤)'
        ELSE '초대형 (5톤+)'
    END AS weight_category,
    COUNT(*) AS case_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage,
    ROUND(MIN(GW), 0) AS min_weight,
    ROUND(MAX(GW), 0) AS max_weight,
    ROUND(AVG(GW), 0) AS avg_weight
FROM sku_master 
WHERE GW IS NOT NULL
GROUP BY weight_category
ORDER BY min_weight;

-- ===========================================
-- 6. 패키지 크기별 분포
-- ===========================================

.print '📏 패키지 크기별 분포'
SELECT 
    CASE 
        WHEN CBM < 5 THEN '소형 (<5m³)'
        WHEN CBM < 15 THEN '중형 (5-15m³)'
        WHEN CBM < 30 THEN '대형 (15-30m³)'
        ELSE '초대형 (30m³+)'
    END AS size_category,
    COUNT(*) AS case_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage,
    ROUND(SUM(CBM), 2) AS total_volume,
    ROUND(AVG(CBM), 2) AS avg_volume
FROM sku_master 
WHERE CBM IS NOT NULL
GROUP BY size_category
ORDER BY avg_volume;

-- ===========================================
-- 7. SKU 검색 (예시)
-- ===========================================

.print '🔍 특정 SKU 상세 정보 (예시: 상위 5개)'
SELECT 
    SKU,
    Vendor,
    Final_Location,
    CASE 
        WHEN FLOW_CODE = 1 THEN 'Direct Delivery'
        WHEN FLOW_CODE = 2 THEN 'Via Warehouse' 
        WHEN FLOW_CODE = 3 THEN 'Via MOSB'
        ELSE 'Other'
    END AS delivery_type,
    Pkg AS packages,
    ROUND(GW, 0) AS weight_kg,
    ROUND(CBM, 2) AS volume_m3
FROM sku_master 
ORDER BY GW DESC
LIMIT 5;
