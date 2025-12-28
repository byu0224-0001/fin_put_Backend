-- 드라이버 후보 승인 테이블
-- 모르는 단어/표현이 나오면 후보로 등록하고, 승인 후 master에 반영

CREATE TABLE IF NOT EXISTS driver_candidates (
    id SERIAL PRIMARY KEY,
    candidate_text VARCHAR(200) NOT NULL,
    suggested_driver_code VARCHAR(50),
    confidence FLOAT DEFAULT 0.0,
    method VARCHAR(50),
    status VARCHAR(20) DEFAULT 'PENDING',
    approved_driver_code VARCHAR(50),
    merged_to_driver_code VARCHAR(50),
    synonym_for_driver_code VARCHAR(50),
    source_report_id VARCHAR(100),
    source_report_title TEXT,
    source_report_text TEXT,
    context_sentence TEXT,
    approved_by VARCHAR(100),
    approved_at TIMESTAMP,
    rejection_reason TEXT,
    occurrence_count INTEGER DEFAULT 1,
    first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스
CREATE INDEX IF NOT EXISTS idx_driver_candidates_status ON driver_candidates(status);
CREATE INDEX IF NOT EXISTS idx_driver_candidates_candidate_text ON driver_candidates(candidate_text);
CREATE INDEX IF NOT EXISTS idx_driver_candidates_suggested_driver_code ON driver_candidates(suggested_driver_code);
CREATE INDEX IF NOT EXISTS idx_driver_candidates_source_report_id ON driver_candidates(source_report_id);

-- 중복 방지: 같은 candidate_text + 같은 source_report_id는 1개만
CREATE UNIQUE INDEX IF NOT EXISTS uq_driver_candidates_text_report 
    ON driver_candidates(candidate_text, source_report_id) 
    WHERE status = 'PENDING';

-- 통계 뷰
CREATE OR REPLACE VIEW driver_candidates_stats AS
SELECT 
    status,
    COUNT(*) as count,
    AVG(confidence) as avg_confidence,
    MAX(last_seen_at) as latest_seen
FROM driver_candidates
GROUP BY status;

