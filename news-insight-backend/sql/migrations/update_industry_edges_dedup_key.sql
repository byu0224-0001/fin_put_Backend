-- industry_edges 테이블 중복 키 정책 변경 마이그레이션
-- P0: "내용이 왕이다" - driver_code를 제외하고 (target_sector_code, logic_fingerprint)만 체크

-- 1. logic_fingerprint 컬럼 추가 (중복 체크용)
ALTER TABLE industry_edges
ADD COLUMN IF NOT EXISTS logic_fingerprint VARCHAR(32);

-- 2. 기존 데이터의 logic_fingerprint 계산 (SHA256 해시의 앞 16자리)
-- Python에서 계산하므로 여기서는 스킵 (이미 저장된 데이터가 없으면 불필요)
-- UPDATE industry_edges
-- SET logic_fingerprint = SUBSTRING(
--     ENCODE(DIGEST(logic_summary, 'sha256'), 'hex'),
--     1, 16
-- )
-- WHERE logic_summary IS NOT NULL AND logic_fingerprint IS NULL;

-- 3. 기존 UNIQUE 제약 제거
ALTER TABLE industry_edges
DROP CONSTRAINT IF EXISTS unique_industry_edge_source_target;

-- 4. 새로운 UNIQUE 제약 추가: (target_sector_code, logic_fingerprint)
-- ⚠️ 주의: PostgreSQL은 UNIQUE 제약에 WHERE 절을 직접 지원하지 않으므로
-- 부분 인덱스(partial unique index)를 사용
CREATE UNIQUE INDEX IF NOT EXISTS unique_industry_edge_sector_logic
ON industry_edges(target_sector_code, logic_fingerprint)
WHERE logic_fingerprint IS NOT NULL;

-- 5. 인덱스 추가 (조회 성능 최적화)
CREATE INDEX IF NOT EXISTS idx_industry_edges_logic_fingerprint
ON industry_edges(logic_fingerprint);

CREATE INDEX IF NOT EXISTS idx_industry_edges_sector_logic
ON industry_edges(target_sector_code, logic_fingerprint);

-- 6. 코멘트 업데이트
COMMENT ON COLUMN industry_edges.logic_fingerprint IS 
    '논리 요약(logic_summary)의 SHA256 해시 앞 16자리 - 중복 체크용 (driver_code 무관)';

