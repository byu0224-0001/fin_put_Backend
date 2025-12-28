-- =============================================================================
-- Granular Sector 및 EconVar 연결 테이블 추가
-- GPT 제안 구조에 맞춘 확장
-- =============================================================================

-- 1. economic_variables 테이블 확장 (P/Q/C type 필드 추가)
ALTER TABLE economic_variables
ADD COLUMN IF NOT EXISTS type CHAR(1),  -- 'P' (Price), 'Q' (Quantity), 'C' (Cost)
ADD COLUMN IF NOT EXISTS source_hint VARCHAR(100),  -- 'FRED', 'ECOS', 'BLOOMBERG', 'CUSTOM' 등
ADD COLUMN IF NOT EXISTS unit VARCHAR(50);  -- 'USD/GB', 'USD/ton', '%' 등

-- type 필드에 인덱스 추가 (P/Q/C 필터링용)
CREATE INDEX IF NOT EXISTS idx_economic_variables_type 
ON economic_variables(type) 
WHERE type IS NOT NULL;

COMMENT ON COLUMN economic_variables.type IS '경제 변수 유형: P(Price), Q(Quantity), C(Cost)';
COMMENT ON COLUMN economic_variables.source_hint IS '데이터 소스 힌트: FRED, ECOS, BLOOMBERG, CUSTOM 등';
COMMENT ON COLUMN economic_variables.unit IS '단위: USD/GB, USD/ton, % 등';

-- =============================================================================
-- 2. sector_granular 테이블 생성 (Granular 산업 분류 정의)
-- =============================================================================

CREATE TABLE IF NOT EXISTS sector_granular (
    code VARCHAR(100) PRIMARY KEY,  -- 예: 'SEMI_MEMORY_HBM', 'BATTERY_CATHODE_NCM'
    major_sector VARCHAR(50) NOT NULL,  -- 예: 'SEC_SEMI'
    sub_sector VARCHAR(50) NOT NULL,  -- 예: 'MEMORY'
    display_name_ko VARCHAR(200) NOT NULL,  -- 예: 'HBM 메모리'
    display_name_en VARCHAR(200),  -- 예: 'HBM Memory'
    value_chain VARCHAR(20),  -- 'UPSTREAM', 'MIDSTREAM', 'DOWNSTREAM'
    description TEXT,
    keywords JSONB DEFAULT '[]'::jsonb,  -- 키워드 배열
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sector_granular_major_sub 
ON sector_granular(major_sector, sub_sector);

CREATE INDEX IF NOT EXISTS idx_sector_granular_sub_sector 
ON sector_granular(sub_sector);

CREATE INDEX IF NOT EXISTS idx_sector_granular_value_chain 
ON sector_granular(value_chain);

CREATE INDEX IF NOT EXISTS idx_sector_granular_is_active 
ON sector_granular(is_active);

COMMENT ON TABLE sector_granular IS 'Granular 산업 분류 정의 (Sub-sector 하위 세분화)';
COMMENT ON COLUMN sector_granular.code IS 'Granular 섹터 코드 (예: SEMI_MEMORY_HBM)';
COMMENT ON COLUMN sector_granular.major_sector IS '상위 Major Sector 코드 (예: SEC_SEMI)';
COMMENT ON COLUMN sector_granular.sub_sector IS '상위 Sub-sector 코드 (예: MEMORY)';
COMMENT ON COLUMN sector_granular.value_chain IS '밸류체인 위치: UPSTREAM, MIDSTREAM, DOWNSTREAM';

-- =============================================================================
-- 3. econvar_granular_link 테이블 생성 (경제변수 ↔ Granular 연결 매핑)
-- =============================================================================

CREATE TABLE IF NOT EXISTS econvar_granular_link (
    econvar_code VARCHAR(50) NOT NULL REFERENCES economic_variables(code) ON DELETE CASCADE,
    granular_code VARCHAR(100) NOT NULL REFERENCES sector_granular(code) ON DELETE CASCADE,
    polarity CHAR(1),  -- '+' (변수↑ 시 긍정적 영향) or '-' (변수↑ 시 부정적 영향)
    sensitivity NUMERIC(5,2),  -- 0.0 ~ 1.0 (민감도 가중치)
    note TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (econvar_code, granular_code)
);

CREATE INDEX IF NOT EXISTS idx_econvar_granular_econvar 
ON econvar_granular_link(econvar_code);

CREATE INDEX IF NOT EXISTS idx_econvar_granular_granular 
ON econvar_granular_link(granular_code);

CREATE INDEX IF NOT EXISTS idx_econvar_granular_polarity 
ON econvar_granular_link(polarity);

COMMENT ON TABLE econvar_granular_link IS '경제 변수 ↔ Granular Sector 연결 매핑';
COMMENT ON COLUMN econvar_granular_link.polarity IS '영향 방향: + (변수↑ 시 긍정), - (변수↑ 시 부정)';
COMMENT ON COLUMN econvar_granular_link.sensitivity IS '민감도 가중치: 0.0 ~ 1.0 (높을수록 영향 큼)';

-- =============================================================================
-- 4. updated_at 트리거 추가 (함수 존재 시에만)
-- =============================================================================

-- update_updated_at_column 함수가 존재하는지 확인 후 트리거 생성
DO $$
BEGIN
    -- 함수 존재 확인
    IF EXISTS (
        SELECT 1 FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = 'public' AND p.proname = 'update_updated_at_column'
    ) THEN
        -- sector_granular 트리거
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger WHERE tgname = 'update_sector_granular_updated_at'
        ) THEN
            CREATE TRIGGER update_sector_granular_updated_at 
            BEFORE UPDATE ON sector_granular
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
        END IF;
        
        -- econvar_granular_link 트리거
        IF NOT EXISTS (
            SELECT 1 FROM pg_trigger WHERE tgname = 'update_econvar_granular_link_updated_at'
        ) THEN
            CREATE TRIGGER update_econvar_granular_link_updated_at 
            BEFORE UPDATE ON econvar_granular_link
            FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
        END IF;
    ELSE
        RAISE NOTICE 'update_updated_at_column 함수가 없어 트리거를 생성하지 않습니다.';
    END IF;
END $$;

-- =============================================================================
-- 완료 메시지
-- =============================================================================

DO $$
BEGIN
    RAISE NOTICE '✅ Granular Sector 테이블 생성 완료!';
    RAISE NOTICE '- economic_variables 테이블 확장 (type, source_hint, unit)';
    RAISE NOTICE '- sector_granular 테이블 생성';
    RAISE NOTICE '- econvar_granular_link 테이블 생성';
    RAISE NOTICE '- 인덱스 및 트리거 설정 완료';
END $$;

