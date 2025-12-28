-- =============================================================================
-- Multi-label 섹터 분류를 위한 DB 스키마 확장
-- =============================================================================
-- 목적: 멀티 모델 앙상블 파이프라인에서 생성되는 multi-label 섹터 분류 저장
-- 실행일: 기존 데이터 보존
-- =============================================================================

BEGIN;

-- =============================================================================
-- Step 1: 기존 UNIQUE 제약조건 제거 (Multi-label 지원)
-- =============================================================================

-- ticker에 대한 UNIQUE 제약조건 확인 및 제거
DO $$
BEGIN
    -- ticker 컬럼의 UNIQUE 제약조건이 있는지 확인
    IF EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE constraint_type = 'UNIQUE'
        AND table_name = 'investor_sector'
        AND constraint_name LIKE '%ticker%'
    ) THEN
        -- UNIQUE 제약조건 제거 (제약조건 이름 찾기)
        EXECUTE (
            SELECT 'ALTER TABLE investor_sector DROP CONSTRAINT ' || constraint_name || ';'
            FROM information_schema.table_constraints
            WHERE constraint_type = 'UNIQUE'
            AND table_name = 'investor_sector'
            AND constraint_name LIKE '%ticker%'
            LIMIT 1
        );
        RAISE NOTICE 'UNIQUE 제약조건 제거 완료';
    ELSE
        RAISE NOTICE 'UNIQUE 제약조건 없음 (이미 제거됨)';
    END IF;
END $$;

-- =============================================================================
-- Step 2: Multi-label 지원 필드 추가
-- =============================================================================

-- sector_weight: 섹터별 가중치 (0.0 ~ 1.0)
ALTER TABLE investor_sector 
ADD COLUMN IF NOT EXISTS sector_weight FLOAT DEFAULT 0.5;

-- is_primary: Primary 섹터 여부
ALTER TABLE investor_sector 
ADD COLUMN IF NOT EXISTS is_primary BOOLEAN DEFAULT FALSE;

-- =============================================================================
-- Step 3: 앙상블 메타데이터 필드 추가
-- =============================================================================

-- 각 단계별 점수
ALTER TABLE investor_sector 
ADD COLUMN IF NOT EXISTS rule_score FLOAT;

ALTER TABLE investor_sector 
ADD COLUMN IF NOT EXISTS embedding_score FLOAT;

ALTER TABLE investor_sector 
ADD COLUMN IF NOT EXISTS bge_score FLOAT;

ALTER TABLE investor_sector 
ADD COLUMN IF NOT EXISTS gpt_score FLOAT;

ALTER TABLE investor_sector 
ADD COLUMN IF NOT EXISTS ensemble_score FLOAT;

-- 분류 근거 (GPT가 제공)
ALTER TABLE investor_sector 
ADD COLUMN IF NOT EXISTS classification_reasoning TEXT;

-- =============================================================================
-- Step 4: 인덱스 추가
-- =============================================================================

-- Primary 섹터 조회용 인덱스
CREATE INDEX IF NOT EXISTS idx_investor_sector_primary 
ON investor_sector(ticker, is_primary) 
WHERE is_primary = TRUE;

-- 섹터 가중치 기반 정렬용 인덱스
CREATE INDEX IF NOT EXISTS idx_investor_sector_weight 
ON investor_sector(ticker, sector_weight DESC);

-- =============================================================================
-- Step 5: 기존 데이터 처리
-- =============================================================================

-- 기존 데이터 중 첫 번째 레코드를 Primary로 설정
UPDATE investor_sector
SET is_primary = TRUE
WHERE id IN (
    SELECT DISTINCT ON (ticker) id
    FROM investor_sector
    ORDER BY ticker, created_at ASC
);

-- 기존 데이터에 기본 가중치 부여
UPDATE investor_sector
SET sector_weight = 1.0
WHERE sector_weight IS NULL OR sector_weight = 0.5;

-- =============================================================================
-- Step 6: 검증 쿼리
-- =============================================================================

-- Multi-label 지원 확인
SELECT 
    ticker,
    COUNT(*) as sector_count,
    COUNT(CASE WHEN is_primary = TRUE THEN 1 END) as primary_count,
    SUM(sector_weight) as total_weight
FROM investor_sector
GROUP BY ticker
HAVING COUNT(*) > 1
ORDER BY sector_count DESC
LIMIT 10;

COMMIT;

-- =============================================================================
-- 마이그레이션 완료
-- =============================================================================
-- 다음 단계:
-- 1. 멀티 모델 앙상블 파이프라인 구현
-- 2. 섹터 재분류 실행 (multi-label 지원)
-- =============================================================================

