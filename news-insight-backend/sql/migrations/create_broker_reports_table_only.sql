-- broker_reports 테이블만 생성 (rc1용)
CREATE TABLE IF NOT EXISTS broker_reports (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10),
    report_id VARCHAR(100) UNIQUE NOT NULL,
    broker_name VARCHAR(100),
    analyst_name VARCHAR(100),
    report_title TEXT,
    report_date DATE,
    report_type VARCHAR(50),
    target_price INTEGER,
    rating VARCHAR(20),
    report_url TEXT,
    report_content TEXT,
    summary TEXT,
    key_points JSONB,
    source VARCHAR(50) DEFAULT 'naver',
    processing_status VARCHAR(20) DEFAULT 'WAITING',
    report_uid VARCHAR(64),
    parser_version VARCHAR(20) DEFAULT 'v1.0',
    extracted_at TIMESTAMP DEFAULT NOW(),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    FOREIGN KEY (ticker) REFERENCES stocks(ticker) ON DELETE SET NULL,
    CONSTRAINT unique_report_id UNIQUE (report_id)
);

CREATE INDEX IF NOT EXISTS idx_broker_reports_ticker ON broker_reports(ticker);
CREATE INDEX IF NOT EXISTS idx_broker_reports_date ON broker_reports(report_date DESC);
CREATE INDEX IF NOT EXISTS idx_broker_reports_broker ON broker_reports(broker_name);
CREATE INDEX IF NOT EXISTS idx_broker_reports_type ON broker_reports(report_type);
CREATE INDEX IF NOT EXISTS idx_broker_reports_rating ON broker_reports(rating);
CREATE INDEX IF NOT EXISTS idx_broker_reports_processing_status ON broker_reports(processing_status);
CREATE INDEX IF NOT EXISTS idx_broker_reports_uid ON broker_reports(report_uid);

