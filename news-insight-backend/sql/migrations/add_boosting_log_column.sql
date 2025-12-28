-- boosting_log 컬럼 추가 (JSONB)
ALTER TABLE investor_sector 
ADD COLUMN IF NOT EXISTS boosting_log JSONB;

COMMENT ON COLUMN investor_sector.boosting_log IS 'Boosting 적용 로그 (anchor_applied, kg_applied, reason, multiplier, final_boost)';


