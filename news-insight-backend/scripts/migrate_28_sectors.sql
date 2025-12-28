-- =============================================================================
-- 28개 섹터 온톨로지 마이그레이션 스크립트
-- =============================================================================
-- 목적: 투자 인과 추론 및 영향 분석을 위한 28개 섹터 체계 구축
-- 실행일: 마이그레이션 전 backup 권장
-- =============================================================================

BEGIN;

-- =============================================================================
-- Step 1: 기존 섹터 비활성화 (하위 호환성 유지)
-- =============================================================================
UPDATE sector_ontology 
SET is_active = 'N', updated_at = CURRENT_TIMESTAMP 
WHERE sector_code IN (
    'SEC_INDUSTRIAL', 
    'SEC_DISCRETIONARY', 
    'SEC_STAPLE', 
    'SEC_MEDICAL', 
    'SEC_CHEMICAL', 
    'SEC_UTILITIES',
    'SEC_CONSTRUCTION'
);

-- =============================================================================
-- Step 2: 신규 섹터 INSERT (ON CONFLICT로 업데이트)
-- =============================================================================

-- [Tech & Growth] - 5개
INSERT INTO sector_ontology (sector_code, sector_name_ko, sector_name_en, level, is_active, description) VALUES
    ('SEC_SEMI', '반도체', 'Semiconductor', 1, 'Y', '반도체 (메모리/비메모리/장비)'),
    ('SEC_BATTERY', '2차전지', 'Battery', 1, 'Y', '2차전지 (셀/소재/장비)'),
    ('SEC_IT', 'IT/소프트웨어', 'IT/Software', 1, 'Y', '인터넷/SW/클라우드/보안'),
    ('SEC_GAME', '게임', 'Game', 1, 'Y', '게임 (모바일/PC/콘솔)'),
    ('SEC_ELECTRONICS', '가전/디스플레이', 'Electronics/Display', 1, 'Y', '가전/디스플레이/IT부품')

ON CONFLICT (sector_code) DO UPDATE SET
    sector_name_ko = EXCLUDED.sector_name_ko,
    sector_name_en = EXCLUDED.sector_name_en,
    description = EXCLUDED.description,
    is_active = 'Y',
    updated_at = CURRENT_TIMESTAMP;

-- [Mobility] - 2개
INSERT INTO sector_ontology (sector_code, sector_name_ko, sector_name_en, level, is_active, description) VALUES
    ('SEC_AUTO', '자동차', 'Automotive', 1, 'Y', '자동차 (완성차/부품)'),
    ('SEC_TIRE', '타이어/고무', 'Tire/Rubber', 1, 'Y', '타이어/합성고무')

ON CONFLICT (sector_code) DO UPDATE SET
    sector_name_ko = EXCLUDED.sector_name_ko,
    sector_name_en = EXCLUDED.sector_name_en,
    description = EXCLUDED.description,
    is_active = 'Y',
    updated_at = CURRENT_TIMESTAMP;

-- [Industry & Cyclical] - 6개
INSERT INTO sector_ontology (sector_code, sector_name_ko, sector_name_en, level, is_active, description) VALUES
    ('SEC_SHIP', '조선/해운', 'Shipbuilding/Shipping', 1, 'Y', '조선/해운/기자재'),
    ('SEC_DEFENSE', '방산/우주항공', 'Defense/Aerospace', 1, 'Y', '방산/우주항공'),
    ('SEC_MACH', '기계/전력기기', 'Machinery/Power Equipment', 1, 'Y', '기계/전력기기/건설장비'),
    ('SEC_CONST', '건설/토목', 'Construction/Civil Engineering', 1, 'Y', '건설/토목/플랜트'),
    ('SEC_STEEL', '철강/비철금속', 'Steel/Non-Ferrous Metals', 1, 'Y', '철강/비철금속'),
    ('SEC_CHEM', '화학/정유', 'Chemical/Refining', 1, 'Y', '화학/정유')

ON CONFLICT (sector_code) DO UPDATE SET
    sector_name_ko = EXCLUDED.sector_name_ko,
    sector_name_en = EXCLUDED.sector_name_en,
    description = EXCLUDED.description,
    is_active = 'Y',
    updated_at = CURRENT_TIMESTAMP;

-- [Consumer & K-Culture] - 6개
INSERT INTO sector_ontology (sector_code, sector_name_ko, sector_name_en, level, is_active, description) VALUES
    ('SEC_ENT', '엔터테인먼트', 'Entertainment', 1, 'Y', '엔터테인먼트/미디어/콘텐츠'),
    ('SEC_COSMETIC', '화장품/패션', 'Cosmetic/Fashion', 1, 'Y', '화장품/패션/OEM'),
    ('SEC_TRAVEL', '여행/항공', 'Travel/Aviation', 1, 'Y', '여행/항공/카지노/면세'),
    ('SEC_FOOD', '음식료', 'Food/Beverage', 1, 'Y', '음식료/담배'),
    ('SEC_RETAIL', '유통', 'Retail', 1, 'Y', '유통/백화점/편의점/이커머스'),
    ('SEC_CONSUMER', '기타 소비재', 'Other Consumer', 1, 'Y', '기타 경기소비재 (가구/렌탈)')

ON CONFLICT (sector_code) DO UPDATE SET
    sector_name_ko = EXCLUDED.sector_name_ko,
    sector_name_en = EXCLUDED.sector_name_en,
    description = EXCLUDED.description,
    is_active = 'Y',
    updated_at = CURRENT_TIMESTAMP;

-- [Healthcare] - 2개
INSERT INTO sector_ontology (sector_code, sector_name_ko, sector_name_en, level, is_active, description) VALUES
    ('SEC_BIO', '바이오/제약', 'Biotech/Pharma', 1, 'Y', '바이오/제약/신약/CMO'),
    ('SEC_MEDDEV', '의료기기', 'Medical Device', 1, 'Y', '의료기기/임플란트/미용기기')

ON CONFLICT (sector_code) DO UPDATE SET
    sector_name_ko = EXCLUDED.sector_name_ko,
    sector_name_en = EXCLUDED.sector_name_en,
    description = EXCLUDED.description,
    is_active = 'Y',
    updated_at = CURRENT_TIMESTAMP;

-- [Finance] - 5개
INSERT INTO sector_ontology (sector_code, sector_name_ko, sector_name_en, level, is_active, description) VALUES
    ('SEC_BANK', '은행', 'Bank', 1, 'Y', '은행'),
    ('SEC_SEC', '증권', 'Securities', 1, 'Y', '증권'),
    ('SEC_INS', '보험', 'Insurance', 1, 'Y', '보험'),
    ('SEC_CARD', '카드/캐피탈', 'Card/Capital', 1, 'Y', '카드/캐피탈/리츠'),
    ('SEC_HOLDING', '지주사', 'Holding Company', 1, 'Y', '지주사 (복합기업)')

ON CONFLICT (sector_code) DO UPDATE SET
    sector_name_ko = EXCLUDED.sector_name_ko,
    sector_name_en = EXCLUDED.sector_name_en,
    description = EXCLUDED.description,
    is_active = 'Y',
    updated_at = CURRENT_TIMESTAMP;

-- [Utility] - 2개
INSERT INTO sector_ontology (sector_code, sector_name_ko, sector_name_en, level, is_active, description) VALUES
    ('SEC_UTIL', '유틸리티', 'Utilities', 1, 'Y', '전력/가스/환경'),
    ('SEC_TELECOM', '통신', 'Telecom', 1, 'Y', '통신 3사')

ON CONFLICT (sector_code) DO UPDATE SET
    sector_name_ko = EXCLUDED.sector_name_ko,
    sector_name_en = EXCLUDED.sector_name_en,
    description = EXCLUDED.description,
    is_active = 'Y',
    updated_at = CURRENT_TIMESTAMP;

-- =============================================================================
-- Step 3: 이름 변경된 섹터 업데이트
-- =============================================================================

-- SEC_CHEMICAL → SEC_CHEM
UPDATE sector_ontology 
SET 
    sector_code = 'SEC_CHEM',
    sector_name_ko = '화학/정유',
    description = '화학/정유',
    updated_at = CURRENT_TIMESTAMP
WHERE sector_code = 'SEC_CHEMICAL';

-- SEC_UTILITIES → SEC_UTIL
UPDATE sector_ontology 
SET 
    sector_code = 'SEC_UTIL',
    sector_name_ko = '유틸리티',
    description = '전력/가스/환경',
    updated_at = CURRENT_TIMESTAMP
WHERE sector_code = 'SEC_UTILITIES';

-- SEC_STAPLE → SEC_FOOD
UPDATE sector_ontology 
SET 
    sector_code = 'SEC_FOOD',
    sector_name_ko = '음식료',
    description = '음식료/담배',
    updated_at = CURRENT_TIMESTAMP
WHERE sector_code = 'SEC_STAPLE';

-- SEC_CONSTRUCTION → SEC_CONST
UPDATE sector_ontology 
SET 
    sector_code = 'SEC_CONST',
    sector_name_ko = '건설/토목',
    description = '건설/토목/플랜트',
    updated_at = CURRENT_TIMESTAMP
WHERE sector_code = 'SEC_CONSTRUCTION';

-- =============================================================================
-- Step 4: 검증 쿼리
-- =============================================================================

-- 활성화된 섹터 개수 확인 (28개여야 함)
SELECT 
    COUNT(*) as total_active_sectors,
    COUNT(CASE WHEN is_active = 'Y' THEN 1 END) as active_count
FROM sector_ontology
WHERE is_active = 'Y';

-- 28개 섹터 목록 확인
SELECT 
    sector_code,
    sector_name_ko,
    description,
    is_active,
    updated_at
FROM sector_ontology
WHERE is_active = 'Y'
ORDER BY 
    CASE 
        WHEN sector_code LIKE 'SEC_SEMI%' THEN 1
        WHEN sector_code LIKE 'SEC_BATTERY%' THEN 2
        WHEN sector_code LIKE 'SEC_IT%' THEN 3
        WHEN sector_code LIKE 'SEC_GAME%' THEN 4
        WHEN sector_code LIKE 'SEC_ELECTRONICS%' THEN 5
        WHEN sector_code LIKE 'SEC_AUTO%' THEN 6
        WHEN sector_code LIKE 'SEC_TIRE%' THEN 7
        WHEN sector_code LIKE 'SEC_SHIP%' THEN 8
        WHEN sector_code LIKE 'SEC_DEFENSE%' THEN 9
        WHEN sector_code LIKE 'SEC_MACH%' THEN 10
        WHEN sector_code LIKE 'SEC_CONST%' THEN 11
        WHEN sector_code LIKE 'SEC_STEEL%' THEN 12
        WHEN sector_code LIKE 'SEC_CHEM%' THEN 13
        WHEN sector_code LIKE 'SEC_ENT%' THEN 14
        WHEN sector_code LIKE 'SEC_COSMETIC%' THEN 15
        WHEN sector_code LIKE 'SEC_TRAVEL%' THEN 16
        WHEN sector_code LIKE 'SEC_FOOD%' THEN 17
        WHEN sector_code LIKE 'SEC_RETAIL%' THEN 18
        WHEN sector_code LIKE 'SEC_CONSUMER%' THEN 19
        WHEN sector_code LIKE 'SEC_BIO%' THEN 20
        WHEN sector_code LIKE 'SEC_MEDDEV%' THEN 21
        WHEN sector_code LIKE 'SEC_BANK%' THEN 22
        WHEN sector_code LIKE 'SEC_SEC%' THEN 23
        WHEN sector_code LIKE 'SEC_INS%' THEN 24
        WHEN sector_code LIKE 'SEC_CARD%' THEN 25
        WHEN sector_code LIKE 'SEC_HOLDING%' THEN 26
        WHEN sector_code LIKE 'SEC_UTIL%' THEN 27
        WHEN sector_code LIKE 'SEC_TELECOM%' THEN 28
        ELSE 99
    END,
    sector_code;

COMMIT;

-- =============================================================================
-- 마이그레이션 완료
-- =============================================================================
-- 다음 단계:
-- 1. sector_classifier.py의 SECTOR_KEYWORDS 딕셔너리 업데이트
-- 2. 섹터 재분류 실행 (scripts/45_auto_classify_sectors.py)
-- =============================================================================

