-- industry_edges 테이블 생성
-- Phase 2.0 P0+ 보강: 산업 리포트 인사이트 저장소

CREATE TABLE IF NOT EXISTS industry_edges (
    id SERIAL PRIMARY KEY,
    report_id VARCHAR(100) NOT NULL,  -- broker_reports.report_id 참조
    -- ⭐ 키 구조 명확화: source → target 관계
    source_driver_code VARCHAR(50),  -- 소스: 경제 변수 (driver_code) - 예: OIL_PRICE_WTI
    target_sector_code VARCHAR(50),  -- 타겟: 섹터 코드 (예: SEC_SEMI, SEC_CHEM)
    target_type VARCHAR(50),  -- 대상 타입 (SECTOR, VALUE_CHAIN, DRIVER)
    relation_type VARCHAR(50) DEFAULT 'INDUSTRY_DRIVEN_BY',  -- 관계 타입: INDUSTRY_DRIVEN_BY, AFFECTED_BY
    logic_summary TEXT,  -- 인사이트 요약 (analyst_logic)
    conditions JSONB,  -- 조건부 설명 (positive/negative)
    key_sentence TEXT,  -- 핵심 문장
    extraction_confidence VARCHAR(20),  -- HIGH, MED, LOW
    valid_from DATE,  -- 유효 시작일
    valid_to DATE,  -- 유효 종료일
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    -- ⚠️ 외래키는 broker_reports 테이블이 생성된 후에 추가
    -- ALTER TABLE industry_edges ADD CONSTRAINT fk_industry_edge_report 
    --     FOREIGN KEY (report_id) REFERENCES broker_reports(report_id) ON DELETE SET NULL;
    
    -- ⭐ 중복 방지: 같은 리포트의 같은 source-target 조합은 중복 저장 금지
    CONSTRAINT unique_industry_edge_source_target 
        UNIQUE (report_id, source_driver_code, target_sector_code)
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_industry_edges_report_id 
ON industry_edges(report_id);

CREATE INDEX IF NOT EXISTS idx_industry_edges_source_driver 
ON industry_edges(source_driver_code);

CREATE INDEX IF NOT EXISTS idx_industry_edges_target_sector 
ON industry_edges(target_sector_code);

CREATE INDEX IF NOT EXISTS idx_industry_edges_relation 
ON industry_edges(relation_type);

CREATE INDEX IF NOT EXISTS idx_industry_edges_valid_from 
ON industry_edges(valid_from DESC);

-- ⭐ 조인 성능 최적화: source-target 조합 인덱스
CREATE INDEX IF NOT EXISTS idx_industry_edges_source_target 
ON industry_edges(source_driver_code, target_sector_code);

-- 코멘트 추가
COMMENT ON TABLE industry_edges IS '산업 리포트 인사이트 저장소 (Ticker 없는 리포트용)';
COMMENT ON COLUMN industry_edges.source_driver_code IS '소스: 경제 변수 코드 (예: OIL_PRICE_WTI) - "무엇이"';
COMMENT ON COLUMN industry_edges.target_sector_code IS '타겟: 섹터 코드 (예: SEC_SEMI) - "누구에게"';
COMMENT ON COLUMN industry_edges.relation_type IS '관계 타입: INDUSTRY_DRIVEN_BY (드라이버→섹터), AFFECTED_BY (섹터→드라이버)';
COMMENT ON COLUMN industry_edges.target_type IS '대상 타입: SECTOR, VALUE_CHAIN, DRIVER';

