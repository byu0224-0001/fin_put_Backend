-- 증권사 리포트 데이터 수집을 위한 테이블 생성
-- Phase 2.1: 파일럿 수집용 스키마

-- ============================================================================
-- 1. broker_reports 테이블 (증권사 리포트 메타데이터 및 본문)
-- ============================================================================

CREATE TABLE IF NOT EXISTS broker_reports (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10),  -- ⭐ NULL 허용 (산업 리포트의 경우)
    report_id VARCHAR(100) UNIQUE NOT NULL,  -- 증권사 리포트 고유 ID (예: naver_20251219_001)
    broker_name VARCHAR(100),  -- 증권사명 (삼성증권, 미래에셋증권 등)
    analyst_name VARCHAR(100),  -- 애널리스트명
    report_title TEXT,  -- 리포트 제목
    report_date DATE,  -- 리포트 발행일
    report_type VARCHAR(50),  -- 리포트 유형 (INITIATION, UPDATE, REVIEW, EARNINGS 등)
    target_price INTEGER,  -- 목표주가 (원 단위)
    rating VARCHAR(20),  -- 투자의견 (BUY, HOLD, SELL, STRONG_BUY 등)
    report_url TEXT,  -- 리포트 원본 URL
    report_content TEXT,  -- 리포트 본문 (전체 텍스트)
    summary TEXT,  -- 리포트 요약 (LLM 생성, 선택적)
    key_points JSONB,  -- 핵심 포인트 (구조화된 JSON)
    source VARCHAR(50) DEFAULT 'naver',  -- 수집 소스 (naver, daum, direct 등)
    -- ⭐ P0+ 보강: 처리 상태 및 메타데이터
    processing_status VARCHAR(20) DEFAULT 'WAITING',  -- 처리 상태
    report_uid VARCHAR(64),  -- 원본 단위 식별자
    parser_version VARCHAR(20) DEFAULT 'v1.0',  -- 파서 버전
    extracted_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    
    FOREIGN KEY (ticker) REFERENCES stocks(ticker) ON DELETE SET NULL,  -- ⭐ SET NULL로 변경
    CONSTRAINT unique_report_id UNIQUE (report_id)
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_broker_reports_ticker ON broker_reports(ticker);
CREATE INDEX IF NOT EXISTS idx_broker_reports_date ON broker_reports(report_date DESC);
CREATE INDEX IF NOT EXISTS idx_broker_reports_broker ON broker_reports(broker_name);
CREATE INDEX IF NOT EXISTS idx_broker_reports_type ON broker_reports(report_type);
CREATE INDEX IF NOT EXISTS idx_broker_reports_rating ON broker_reports(rating);
CREATE INDEX IF NOT EXISTS idx_broker_reports_processing_status ON broker_reports(processing_status);
CREATE INDEX IF NOT EXISTS idx_broker_reports_uid ON broker_reports(report_uid);

-- 코멘트 추가
COMMENT ON TABLE broker_reports IS '증권사 리포트 메타데이터 및 본문 저장';
COMMENT ON COLUMN broker_reports.report_id IS '증권사 리포트 고유 ID (소스_날짜_순번 형식)';
COMMENT ON COLUMN broker_reports.report_type IS '리포트 유형: INITIATION(신규), UPDATE(업데이트), REVIEW(검토), EARNINGS(실적) 등';
COMMENT ON COLUMN broker_reports.rating IS '투자의견: BUY(매수), HOLD(보유), SELL(매도), STRONG_BUY(강력매수) 등';
COMMENT ON COLUMN broker_reports.key_points IS '핵심 포인트 JSON 구조: {"points": ["포인트1", "포인트2"], "drivers": ["OIL_PRICE", "INTEREST_RATE"]}';

-- ============================================================================
-- 2. broker_report_evidence 테이블 (Evidence Pool 확장용)
-- ============================================================================

CREATE TABLE IF NOT EXISTS broker_report_evidence (
    id SERIAL PRIMARY KEY,
    report_id INTEGER REFERENCES broker_reports(id) ON DELETE CASCADE,
    ticker VARCHAR(10) NOT NULL,
    driver_code VARCHAR(50),  -- 관련 경제 변수 (OIL_PRICE, INTEREST_RATE 등)
    evidence_snippet TEXT NOT NULL,  -- 증거 문장
    snippet_type VARCHAR(50),  -- 문장 유형 (COST, DEMAND, PRICING, MACRO 등)
    mechanism VARCHAR(50),  -- 메커니즘 (INPUT_COST, PRODUCT_PRICE, DEMAND 등)
    polarity VARCHAR(20),  -- 방향성 (POSITIVE, NEGATIVE, MIXED)
    confidence FLOAT DEFAULT 0.5,  -- 신뢰도 (0.0 ~ 1.0)
    page_number INTEGER,  -- 리포트 내 페이지 번호 (PDF인 경우)
    section_title VARCHAR(200),  -- 리포트 내 섹션 제목
    extracted_at TIMESTAMP DEFAULT NOW(),
    
    FOREIGN KEY (ticker) REFERENCES stocks(ticker) ON DELETE CASCADE
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_evidence_ticker ON broker_report_evidence(ticker);
CREATE INDEX IF NOT EXISTS idx_evidence_driver ON broker_report_evidence(driver_code);
CREATE INDEX IF NOT EXISTS idx_evidence_report ON broker_report_evidence(report_id);
CREATE INDEX IF NOT EXISTS idx_evidence_mechanism ON broker_report_evidence(mechanism);
CREATE INDEX IF NOT EXISTS idx_evidence_polarity ON broker_report_evidence(polarity);

-- 코멘트 추가
COMMENT ON TABLE broker_report_evidence IS '증권사 리포트에서 추출한 Evidence Snippet 저장 (Evidence Pool 확장용)';
COMMENT ON COLUMN broker_report_evidence.snippet_type IS '문장 유형: COST(원가), DEMAND(수요), PRICING(가격), MACRO(거시) 등';
COMMENT ON COLUMN broker_report_evidence.mechanism IS '메커니즘: INPUT_COST, PRODUCT_PRICE, DEMAND, SPREAD, MACRO_SENSITIVITY 등';
COMMENT ON COLUMN broker_report_evidence.polarity IS '방향성: POSITIVE(긍정), NEGATIVE(부정), MIXED(양면)';

-- ============================================================================
-- 3. broker_report_collection_log 테이블 (수집 로그)
-- ============================================================================

CREATE TABLE IF NOT EXISTS broker_report_collection_log (
    id SERIAL PRIMARY KEY,
    collection_date DATE NOT NULL,
    source VARCHAR(50) NOT NULL,  -- 수집 소스 (naver, daum 등)
    total_found INTEGER DEFAULT 0,  -- 발견된 리포트 수
    total_collected INTEGER DEFAULT 0,  -- 수집 성공한 리포트 수
    total_failed INTEGER DEFAULT 0,  -- 수집 실패한 리포트 수
    error_log JSONB,  -- 에러 로그 (선택적)
    created_at TIMESTAMP DEFAULT NOW(),
    
    CONSTRAINT unique_collection_date_source UNIQUE (collection_date, source)
);

-- 인덱스 생성
CREATE INDEX IF NOT EXISTS idx_collection_log_date ON broker_report_collection_log(collection_date DESC);

-- 코멘트 추가
COMMENT ON TABLE broker_report_collection_log IS '증권사 리포트 수집 로그 (일별 수집 현황 추적)';

