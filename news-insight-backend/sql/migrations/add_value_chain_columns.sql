-- ⭐ Phase 2: 5단계 밸류체인 세분화를 위한 컬럼 추가 (단순화 버전)
-- value_chain: 주요 밸류체인 (예: "MID_SOFT", top1)
-- value_chain_detail: 보조 라벨 (예: "MID_HARD", top2, gap < 0.1일 때만)
-- value_chain_confidence: top1_score - top2_score (0.0~1.0)

ALTER TABLE investor_sector 
ADD COLUMN IF NOT EXISTS value_chain VARCHAR(50),
ADD COLUMN IF NOT EXISTS value_chain_detail VARCHAR(50),
ADD COLUMN IF NOT EXISTS value_chain_confidence FLOAT;

-- 인덱스 추가 (성능 개선)
CREATE INDEX IF NOT EXISTS idx_value_chain ON investor_sector(value_chain);
CREATE INDEX IF NOT EXISTS idx_investor_sector_sector_l1_value_chain 
ON investor_sector(sector_l1, value_chain) 
WHERE value_chain IS NOT NULL;

-- 주석 추가
COMMENT ON COLUMN investor_sector.value_chain IS '주요 밸류체인 (UPSTREAM, MID_HARD, MID_SOFT, DOWN_BIZ, DOWN_SERVICE, top1)';
COMMENT ON COLUMN investor_sector.value_chain_detail IS '보조 라벨 (top2, gap < 0.1일 때만)';
COMMENT ON COLUMN investor_sector.value_chain_confidence IS '밸류체인 분류 신뢰도 (top1_score - top2_score, 0.0~1.0)';

