-- InvestorSector 테이블에 인과 구조 분석 필드 추가
-- 레벨 2: 인과 구조 분석 메타데이터 (GPT 인과 Reasoning 생성)

-- causal_structure 필드 추가 (JSONB)
ALTER TABLE investor_sector 
ADD COLUMN IF NOT EXISTS causal_structure JSONB;

-- investment_insights 필드 추가 (TEXT)
ALTER TABLE investor_sector 
ADD COLUMN IF NOT EXISTS investment_insights TEXT;

-- 인덱스 추가 (JSONB 필드 검색 최적화)
CREATE INDEX IF NOT EXISTS idx_investor_sector_causal_structure 
ON investor_sector USING GIN (causal_structure);

