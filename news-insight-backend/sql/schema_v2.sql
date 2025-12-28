-- Financial Knowledge Graph System Database Schema (v2)
-- 5개 Axis + 보완 테이블

-- =============================================================================
-- Axis 1: Ontology (The Brain)
-- =============================================================================

-- 경제 변수 테이블
CREATE TABLE IF NOT EXISTS economic_variables (
    code VARCHAR(50) PRIMARY KEY,
    name_ko VARCHAR(200) NOT NULL,
    category VARCHAR(50),
    layer VARCHAR(20),
    synonyms JSONB DEFAULT '[]'::jsonb,
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_economic_variables_category ON economic_variables(category);
CREATE INDEX IF NOT EXISTS idx_economic_variables_layer ON economic_variables(layer);

-- =============================================================================
-- Axis 2: Company Master (The Body) - 확장
-- =============================================================================

-- 투자 섹터 분류 테이블
CREATE TABLE IF NOT EXISTS investor_sector (
    id VARCHAR(50) PRIMARY KEY,
    ticker VARCHAR(20) UNIQUE NOT NULL REFERENCES stocks(ticker) ON DELETE CASCADE,
    major_sector VARCHAR(50),
    sub_sector VARCHAR(50),
    value_chain VARCHAR(50),
    classification_method VARCHAR(20),
    confidence VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_investor_sector_ticker ON investor_sector(ticker);
CREATE INDEX IF NOT EXISTS idx_investor_sector_major_sector ON investor_sector(major_sector);

-- 섹터 온톨로지 테이블
CREATE TABLE IF NOT EXISTS sector_ontology (
    sector_code VARCHAR(50) PRIMARY KEY,
    parent_sector VARCHAR(50) REFERENCES sector_ontology(sector_code) ON DELETE SET NULL,
    sector_name_ko VARCHAR(200) NOT NULL,
    sector_name_en VARCHAR(200),
    value_chain_position VARCHAR(50),
    description TEXT,
    gics_code VARCHAR(20),
    level INTEGER DEFAULT 1,
    is_active VARCHAR(1) DEFAULT 'Y',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sector_ontology_parent ON sector_ontology(parent_sector);
CREATE INDEX IF NOT EXISTS idx_sector_ontology_level ON sector_ontology(level);
CREATE INDEX IF NOT EXISTS idx_sector_ontology_is_active ON sector_ontology(is_active);

-- 기업명 별칭 매핑 테이블 (Entity Resolution 보조)
CREATE TABLE IF NOT EXISTS company_aliases (
    id VARCHAR(100) PRIMARY KEY,
    alias_name VARCHAR(200) NOT NULL,
    official_name VARCHAR(200) NOT NULL,
    ticker VARCHAR(20) REFERENCES stocks(ticker) ON DELETE SET NULL,
    company_type VARCHAR(20) DEFAULT 'LISTED',
    confidence VARCHAR(20) DEFAULT 'HIGH',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_company_aliases_alias_name ON company_aliases(alias_name);
CREATE INDEX IF NOT EXISTS idx_company_aliases_ticker ON company_aliases(ticker);

-- =============================================================================
-- Axis 3: Qualitative Data (The Context)
-- =============================================================================

-- 기업 정성 정보 테이블
CREATE TABLE IF NOT EXISTS company_details (
    id VARCHAR(50) PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL REFERENCES stocks(ticker) ON DELETE CASCADE,
    source VARCHAR(50) NOT NULL,
    biz_summary TEXT,
    products JSONB DEFAULT '[]'::jsonb,
    clients JSONB DEFAULT '[]'::jsonb,
    supply_chain JSONB DEFAULT '[]'::jsonb,
    raw_materials JSONB DEFAULT '[]'::jsonb,
    risk_factors TEXT,
    cost_structure TEXT,
    keywords JSONB DEFAULT '[]'::jsonb,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_company_details_ticker ON company_details(ticker);
CREATE INDEX IF NOT EXISTS idx_company_details_source ON company_details(source);

-- Raw 데이터 저장 테이블 (보완)
CREATE TABLE IF NOT EXISTS company_details_raw (
    id VARCHAR(100) PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL REFERENCES stocks(ticker) ON DELETE CASCADE,
    source VARCHAR(50) NOT NULL,
    year VARCHAR(4) NOT NULL,
    raw_html TEXT,  -- TOAST 자동 압축
    raw_markdown TEXT,
    raw_json JSONB,
    processing_status VARCHAR(20) DEFAULT 'PENDING',
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    processed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_raw_ticker_source_year ON company_details_raw(ticker, source, year);
CREATE INDEX IF NOT EXISTS idx_raw_processing_status ON company_details_raw(processing_status);
CREATE INDEX IF NOT EXISTS idx_raw_ticker ON company_details_raw(ticker);

-- 버전 관리 테이블 (보완)
CREATE TABLE IF NOT EXISTS company_details_version (
    id VARCHAR(100) PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL REFERENCES stocks(ticker) ON DELETE CASCADE,
    year VARCHAR(4) NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    model_version VARCHAR(50),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    is_current VARCHAR(1) DEFAULT 'Y',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_version_ticker_year ON company_details_version(ticker, year);
CREATE INDEX IF NOT EXISTS idx_version_current ON company_details_version(ticker, year, is_current);
CREATE INDEX IF NOT EXISTS idx_version_ticker ON company_details_version(ticker);

-- =============================================================================
-- Axis 4: Quantitative Data (The Fuel)
-- =============================================================================

-- 경제 지표 시계열 테이블
CREATE TABLE IF NOT EXISTS economic_history (
    id VARCHAR(100) PRIMARY KEY,
    code VARCHAR(50) NOT NULL REFERENCES economic_variables(code) ON DELETE CASCADE,
    date DATE NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    unit VARCHAR(20),
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_economic_history_code ON economic_history(code);
CREATE INDEX IF NOT EXISTS idx_economic_history_date ON economic_history(date);
CREATE INDEX IF NOT EXISTS idx_economic_history_code_date ON economic_history(code, date);

-- 주가 시계열 테이블
CREATE TABLE IF NOT EXISTS stock_prices (
    id VARCHAR(100) PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL REFERENCES stocks(ticker) ON DELETE CASCADE,
    date DATE NOT NULL,
    open DOUBLE PRECISION,
    high DOUBLE PRECISION,
    low DOUBLE PRECISION,
    close DOUBLE PRECISION NOT NULL,
    volume DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stock_prices_ticker ON stock_prices(ticker);
CREATE INDEX IF NOT EXISTS idx_stock_prices_date ON stock_prices(date);
CREATE INDEX IF NOT EXISTS idx_stock_prices_ticker_date ON stock_prices(ticker, date);

-- =============================================================================
-- Axis 5: Logic & Links (The KG Layer)
-- =============================================================================

-- Knowledge Graph 관계 테이블
CREATE TABLE IF NOT EXISTS edges (
    id VARCHAR(150) PRIMARY KEY,
    source_id VARCHAR(100) NOT NULL,
    target_id VARCHAR(100) NOT NULL,
    relation_type VARCHAR(50) NOT NULL,
    weight DOUBLE PRECISION DEFAULT 1.0,
    evidence TEXT,
    source VARCHAR(50),
    direction VARCHAR(10) DEFAULT 'DIRECTED',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_edges_source_id ON edges(source_id);
CREATE INDEX IF NOT EXISTS idx_edges_target_id ON edges(target_id);
CREATE INDEX IF NOT EXISTS idx_edges_relation_type ON edges(relation_type);
CREATE INDEX IF NOT EXISTS idx_edges_source_target ON edges(source_id, target_id);

-- =============================================================================
-- 보완 테이블: Traceability
-- =============================================================================

-- 처리 로그 테이블 (Traceability)
CREATE TABLE IF NOT EXISTS processing_log (
    id VARCHAR(150) PRIMARY KEY,
    ticker VARCHAR(20) NOT NULL REFERENCES stocks(ticker) ON DELETE CASCADE,
    step VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    duration_ms INTEGER,
    extra_metadata TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_log_ticker ON processing_log(ticker);
CREATE INDEX IF NOT EXISTS idx_log_step ON processing_log(step);
CREATE INDEX IF NOT EXISTS idx_log_status ON processing_log(status);
CREATE INDEX IF NOT EXISTS idx_log_created_at ON processing_log(created_at);  -- 중요: 조회 성능 최적화
CREATE INDEX IF NOT EXISTS idx_log_ticker_step ON processing_log(ticker, step);
CREATE INDEX IF NOT EXISTS idx_log_status_created ON processing_log(status, created_at);
CREATE INDEX IF NOT EXISTS idx_log_ticker_created ON processing_log(ticker, created_at);

-- =============================================================================
-- 트리거 생성 (updated_at 자동 업데이트)
-- =============================================================================

-- updated_at 자동 업데이트 함수 (기존 함수 재사용)
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- 각 테이블에 트리거 생성
CREATE TRIGGER update_economic_variables_updated_at BEFORE UPDATE ON economic_variables
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_investor_sector_updated_at BEFORE UPDATE ON investor_sector
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_sector_ontology_updated_at BEFORE UPDATE ON sector_ontology
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_company_details_updated_at BEFORE UPDATE ON company_details
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_company_details_raw_updated_at BEFORE UPDATE ON company_details_raw
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_company_details_version_updated_at BEFORE UPDATE ON company_details_version
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_economic_history_updated_at BEFORE UPDATE ON economic_history
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_stock_prices_updated_at BEFORE UPDATE ON stock_prices
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_edges_updated_at BEFORE UPDATE ON edges
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- =============================================================================
-- 완료 메시지
-- =============================================================================

-- 스키마 생성 완료
DO $$
BEGIN
    RAISE NOTICE 'Financial Knowledge Graph System Schema (v2) 생성 완료!';
    RAISE NOTICE '- 5개 Axis 테이블 생성';
    RAISE NOTICE '- 4개 보완 테이블 생성';
    RAISE NOTICE '- 인덱스 및 트리거 설정 완료';
END $$;

