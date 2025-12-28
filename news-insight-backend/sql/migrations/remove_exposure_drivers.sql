-- exposure_drivers 및 supporting_drivers 컬럼 제거 마이그레이션
-- Step 4A 중간 결과는 로그/메모리만 사용, DB에는 causal_structure.key_drivers만 저장

-- 1. exposure_drivers 컬럼 제거
ALTER TABLE investor_sector 
DROP COLUMN IF EXISTS exposure_drivers;

-- 2. supporting_drivers 컬럼 제거
ALTER TABLE investor_sector 
DROP COLUMN IF EXISTS supporting_drivers;

-- 3. 코멘트 추가 (참고용)
COMMENT ON COLUMN investor_sector.causal_structure IS 
'인과 구조 분석 결과 (JSONB). key_drivers에 driver_tags 포함. exposure_drivers는 Step 4A 중간 결과로 로그에만 기록됨.';

-- 4. 마이그레이션 완료 로그
DO $$
BEGIN
    RAISE NOTICE 'exposure_drivers 및 supporting_drivers 컬럼 제거 완료';
    RAISE NOTICE '이제 causal_structure.key_drivers만 사용합니다.';
END $$;

