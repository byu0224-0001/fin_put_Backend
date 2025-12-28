-- edges 테이블 개선 마이그레이션
-- source_type, target_type, properties 컬럼 추가
-- Driver Tags를 properties에 저장할 수 있도록 확장

-- 1. source_type, target_type 컬럼 추가
ALTER TABLE edges
ADD COLUMN IF NOT EXISTS source_type VARCHAR(20),
ADD COLUMN IF NOT EXISTS target_type VARCHAR(20);

-- 2. properties 컬럼 추가 (JSONB)
ALTER TABLE edges
ADD COLUMN IF NOT EXISTS properties JSONB DEFAULT '{}'::jsonb;

-- 3. 인덱스 추가
CREATE INDEX IF NOT EXISTS idx_edges_source_type ON edges(source_type);
CREATE INDEX IF NOT EXISTS idx_edges_target_type ON edges(target_type);
CREATE INDEX IF NOT EXISTS idx_edges_properties ON edges USING GIN (properties);

-- 4. CHECK 제약 (선택적 - 데이터 무결성 강화)
ALTER TABLE edges
DROP CONSTRAINT IF EXISTS chk_edges_source_type;

ALTER TABLE edges
ADD CONSTRAINT chk_edges_source_type 
CHECK (source_type IS NULL OR source_type IN ('COMPANY', 'DRIVER', 'SECTOR', 'TAG', 'ECONVAR'));

ALTER TABLE edges
DROP CONSTRAINT IF EXISTS chk_edges_target_type;

ALTER TABLE edges
ADD CONSTRAINT chk_edges_target_type 
CHECK (target_type IS NULL OR target_type IN ('COMPANY', 'DRIVER', 'SECTOR', 'TAG', 'ECONVAR'));

-- 5. 기존 데이터 업데이트 (기본값 설정)
-- source_id가 ticker 형식이면 COMPANY로 설정
UPDATE edges
SET source_type = 'COMPANY'
WHERE source_type IS NULL 
  AND source_id ~ '^[0-9]{6}$';  -- 6자리 숫자 (ticker 형식)

-- target_id가 ticker 형식이면 COMPANY로 설정
UPDATE edges
SET target_type = 'COMPANY'
WHERE target_type IS NULL 
  AND target_id ~ '^[0-9]{6}$';

-- relation_type이 DRIVEN_BY인 경우 target_type을 DRIVER로 설정
UPDATE edges
SET target_type = 'DRIVER'
WHERE relation_type = 'DRIVEN_BY' 
  AND target_type IS NULL;

-- relation_type이 HAS_TAG인 경우 target_type을 TAG로 설정
UPDATE edges
SET target_type = 'TAG'
WHERE relation_type = 'HAS_TAG' 
  AND target_type IS NULL;

-- relation_type이 BELONGS_TO인 경우 target_type을 SECTOR로 설정
UPDATE edges
SET target_type = 'SECTOR'
WHERE relation_type = 'BELONGS_TO' 
  AND target_type IS NULL;

-- 6. 코멘트 추가
COMMENT ON COLUMN edges.source_type IS '소스 엔티티 타입: COMPANY, DRIVER, SECTOR, TAG, ECONVAR';
COMMENT ON COLUMN edges.target_type IS '타겟 엔티티 타입: COMPANY, DRIVER, SECTOR, TAG, ECONVAR';
COMMENT ON COLUMN edges.properties IS '엣지 속성 (JSONB): weight, direction, driver_tags 등 포함 가능';

