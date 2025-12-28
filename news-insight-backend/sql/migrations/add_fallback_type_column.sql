-- fallback_type 컬럼 추가 마이그레이션
-- 실행일: 2025-12-16

-- fallback_type 컬럼 추가
ALTER TABLE investor_sector 
ADD COLUMN IF NOT EXISTS fallback_type VARCHAR(20);

-- 인덱스 추가 (선택적)
CREATE INDEX IF NOT EXISTS idx_investor_sector_fallback_type 
ON investor_sector(fallback_type);

-- 기존 데이터 마이그레이션 (fallback_used가 NULL이 아닌 경우)
UPDATE investor_sector
SET fallback_type = fallback_used
WHERE fallback_used IS NOT NULL 
  AND fallback_type IS NULL;

-- fallback_used를 NULL로 초기화 (이제는 BOOLEAN 용도로 사용)
-- 주의: 이 부분은 실제로는 fallback_used를 BOOLEAN으로 변경해야 하지만,
-- 현재는 VARCHAR이므로 일단 유지하고, 코드에서만 True/False 사용

COMMENT ON COLUMN investor_sector.fallback_used IS 'Fallback 사용 여부 (BOOLEAN 용도, 현재 VARCHAR)';
COMMENT ON COLUMN investor_sector.fallback_type IS 'Fallback 타입: RULE, TOP1, KRX, UNKNOWN';

