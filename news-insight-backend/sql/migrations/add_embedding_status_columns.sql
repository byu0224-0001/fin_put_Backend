-- 임베딩 상태 추적 컬럼 추가
ALTER TABLE company_embeddings
ADD COLUMN IF NOT EXISTS last_status VARCHAR(20),
ADD COLUMN IF NOT EXISTS last_error_type VARCHAR(50),
ADD COLUMN IF NOT EXISTS last_attempted_at TIMESTAMP;

COMMENT ON COLUMN company_embeddings.last_status IS '마지막 상태: SUCCESS, CACHE_HIT, API_ERROR, DB_ERROR';
COMMENT ON COLUMN company_embeddings.last_error_type IS '마지막 오류 유형: RATE_LIMIT, AUTH_ERROR, TIMEOUT, NETWORK_ERROR, UNKNOWN';
COMMENT ON COLUMN company_embeddings.last_attempted_at IS '마지막 시도 시각';

