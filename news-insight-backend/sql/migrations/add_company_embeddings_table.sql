-- company_embeddings 테이블 생성 (PostgreSQL + pgvector)
-- Solar Embedding 벡터 저장용

-- pgvector 확장 설치 (이미 설치되어 있으면 무시)
CREATE EXTENSION IF NOT EXISTS vector;

-- company_embeddings 테이블 생성
CREATE TABLE IF NOT EXISTS company_embeddings (
    ticker VARCHAR(10) PRIMARY KEY,
    text_hash VARCHAR(64) NOT NULL,
    embedding_vector vector(4096) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 인덱스 생성 (벡터 검색 성능 향상)
CREATE INDEX IF NOT EXISTS idx_company_embeddings_text_hash ON company_embeddings(text_hash);
CREATE INDEX IF NOT EXISTS idx_company_embeddings_updated_at ON company_embeddings(updated_at);

-- 참고: 4096차원 벡터는 HNSW 인덱스 미지원 (최대 2000차원)
-- 벡터 검색은 순차 스캔 또는 ivfflat 인덱스 사용 가능 (필요시 추가)

-- updated_at 자동 업데이트 트리거 함수 (한 줄로 작성)
CREATE OR REPLACE FUNCTION update_company_embeddings_updated_at() RETURNS TRIGGER AS $$ BEGIN NEW.updated_at = CURRENT_TIMESTAMP; RETURN NEW; END; $$ LANGUAGE plpgsql;

-- 트리거 생성
CREATE TRIGGER trigger_update_company_embeddings_updated_at BEFORE UPDATE ON company_embeddings FOR EACH ROW EXECUTE FUNCTION update_company_embeddings_updated_at();

-- 코멘트 추가
COMMENT ON TABLE company_embeddings IS 'Solar Embedding 벡터 저장 테이블 (재임베딩 방지를 위한 text_hash 포함)';
COMMENT ON COLUMN company_embeddings.ticker IS '종목코드 (Primary Key)';
COMMENT ON COLUMN company_embeddings.text_hash IS '입력 텍스트의 SHA256 해시 (텍스트 변경 감지용)';
COMMENT ON COLUMN company_embeddings.embedding_vector IS 'Solar Embedding 벡터 (4096차원)';
COMMENT ON COLUMN company_embeddings.created_at IS '생성 시각';
COMMENT ON COLUMN company_embeddings.updated_at IS '수정 시각 (자동 업데이트)';

