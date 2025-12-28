-- broker_reports 테이블에 processing_status 컬럼 추가
-- Phase 2.0 P0+ 보강: 리포트 처리 상태 DB 영속화

-- processing_status 컬럼 추가
ALTER TABLE broker_reports 
ADD COLUMN IF NOT EXISTS processing_status VARCHAR(20) DEFAULT 'WAITING';

-- 인덱스 추가 (조회 성능)
CREATE INDEX IF NOT EXISTS idx_broker_reports_processing_status 
ON broker_reports(processing_status);

-- report_uid 컬럼 추가 (원본 단위 식별자)
ALTER TABLE broker_reports 
ADD COLUMN IF NOT EXISTS report_uid VARCHAR(64);

-- report_uid 인덱스 추가
CREATE INDEX IF NOT EXISTS idx_broker_reports_uid 
ON broker_reports(report_uid);

-- parser_version 컬럼 추가 (fingerprint 재생성용)
ALTER TABLE broker_reports 
ADD COLUMN IF NOT EXISTS parser_version VARCHAR(20) DEFAULT 'v1.0';

-- 코멘트 추가
COMMENT ON COLUMN broker_reports.processing_status IS '처리 상태: WAITING, PARSED_OK, PARSED_HOLD, MATCHED_OK, MATCHED_HOLD, EXTRACTED_OK, EXTRACTED_HOLD, ENRICHED, FAILED_RETRYABLE, FAILED_FATAL';
COMMENT ON COLUMN broker_reports.report_uid IS '리포트 원본 단위 식별자 (sha256(pdf_url or naver_doc_id))';
COMMENT ON COLUMN broker_reports.parser_version IS '파서 버전 (fingerprint 재생성용)';

