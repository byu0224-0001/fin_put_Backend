-- confidence 필드 길이 확장 (20 -> 50)
-- HOLD 사유 포함 시 최대 30자까지 필요

ALTER TABLE investor_sector 
ALTER COLUMN confidence TYPE VARCHAR(50);

-- 주석 업데이트
COMMENT ON COLUMN investor_sector.confidence IS 'HIGH, MEDIUM, LOW, VERY_LOW, HOLD, HOLD:HOLD_* (HOLD 사유 포함)';

