-- industry_edges → company_edges 연결 경로 쿼리
-- P0-α: industry_edges와 company_edges 간 간접 연결 확인

-- 1. 특정 기업의 산업 간접 노출 드라이버 조회
-- 예: SK하이닉스(000660)가 어떤 산업 드라이버에 노출되어 있는지
SELECT 
    ie.source_driver_code,
    ie.target_sector_code,
    ie.logic_summary,
    ie.key_sentence,
    isec.major_sector,
    isec.sub_sector
FROM industry_edges ie
JOIN investor_sector isec 
    ON ie.target_sector_code = isec.major_sector
WHERE isec.ticker = :ticker
    AND ie.valid_from <= CURRENT_DATE
    AND ie.valid_to >= CURRENT_DATE
ORDER BY ie.created_at DESC;

-- 2. 특정 드라이버에 노출된 섹터 및 해당 섹터의 기업 목록
-- 예: OIL_PRICE_WTI에 노출된 섹터와 기업
SELECT 
    ie.source_driver_code,
    ie.target_sector_code,
    ie.logic_summary,
    isec.ticker,
    s.company_name,
    isec.major_sector,
    isec.sub_sector
FROM industry_edges ie
JOIN investor_sector isec 
    ON ie.target_sector_code = isec.major_sector
JOIN stocks s 
    ON isec.ticker = s.ticker
WHERE ie.source_driver_code = :driver_code
    AND ie.valid_from <= CURRENT_DATE
    AND ie.valid_to >= CURRENT_DATE
ORDER BY ie.created_at DESC, isec.ticker;

-- 3. 특정 기업의 직접 노출(company edges)과 간접 노출(industry edges) 통합 조회
-- 예: 삼성전자(005930)의 모든 드라이버 노출 (직접 + 간접)
SELECT 
    'DIRECT' as exposure_type,
    e.target_id as driver_code,
    e.properties->>'kg_mechanism' as mechanism,
    e.properties->>'kg_polarity' as polarity,
    NULL as sector_code,
    NULL as industry_logic
FROM edges e
WHERE e.source_id = :ticker
    AND e.relation_type = 'DRIVEN_BY'
    AND e.source_type = 'COMPANY'
    AND e.target_type = 'DRIVER'

UNION ALL

SELECT 
    'INDIRECT' as exposure_type,
    ie.source_driver_code as driver_code,
    NULL as mechanism,
    NULL as polarity,
    ie.target_sector_code as sector_code,
    ie.logic_summary as industry_logic
FROM industry_edges ie
JOIN investor_sector isec 
    ON ie.target_sector_code = isec.major_sector
WHERE isec.ticker = :ticker
    AND ie.valid_from <= CURRENT_DATE
    AND ie.valid_to >= CURRENT_DATE

ORDER BY exposure_type, driver_code;

-- 4. 섹터별 드라이버 노출 통계 (검증용)
SELECT 
    ie.target_sector_code,
    COUNT(DISTINCT ie.source_driver_code) as driver_count,
    COUNT(DISTINCT isec.ticker) as company_count,
    ARRAY_AGG(DISTINCT ie.source_driver_code) as drivers
FROM industry_edges ie
LEFT JOIN investor_sector isec 
    ON ie.target_sector_code = isec.major_sector
WHERE ie.valid_from <= CURRENT_DATE
    AND ie.valid_to >= CURRENT_DATE
GROUP BY ie.target_sector_code
ORDER BY driver_count DESC, company_count DESC;

