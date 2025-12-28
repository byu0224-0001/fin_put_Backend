-- industry_edges 테이블에 soft delete 컬럼 추가
-- P0+: 과거 오매핑 데이터 정리/회수 정책

-- 1. is_active 컬럼 추가 (기본값 true)
ALTER TABLE industry_edges
ADD COLUMN IF NOT EXISTS is_active BOOLEAN DEFAULT TRUE;

-- 2. disabled_reason 컬럼 추가
ALTER TABLE industry_edges
ADD COLUMN IF NOT EXISTS disabled_reason TEXT;

-- 3. disabled_at 컬럼 추가
ALTER TABLE industry_edges
ADD COLUMN IF NOT EXISTS disabled_at TIMESTAMP;

-- 4. 인덱스 추가 (활성 레코드 조회 최적화)
CREATE INDEX IF NOT EXISTS idx_industry_edges_is_active 
ON industry_edges(is_active) 
WHERE is_active = TRUE;

-- 5. 코멘트 추가
COMMENT ON COLUMN industry_edges.is_active IS 
    '레코드 활성화 여부 (false면 soft delete, 과거 오매핑 데이터 회수용)';
    
COMMENT ON COLUMN industry_edges.disabled_reason IS 
    '비활성화 사유 (예: SANITY_CHECK_FAILED, SECTOR_MAPPING_ERROR)';
    
COMMENT ON COLUMN industry_edges.disabled_at IS 
    '비활성화 시각';

