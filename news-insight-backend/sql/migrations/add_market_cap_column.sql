-- 시가총액 컬럼 추가 마이그레이션
-- stocks 테이블에 market_cap 컬럼 추가

-- market_cap 컬럼 추가 (BIGINT, NULL 허용)
-- 단위: 원 (KRW)
-- 예: 삼성전자 시가총액 500조원 = 500000000000000
ALTER TABLE stocks 
ADD COLUMN IF NOT EXISTS market_cap BIGINT NULL;

-- 인덱스 추가 (시가총액 기반 정렬/필터링 성능 향상)
CREATE INDEX IF NOT EXISTS idx_stocks_market_cap ON stocks(market_cap);

-- 코멘트 추가
COMMENT ON COLUMN stocks.market_cap IS '시가총액 (원 단위, NULL 허용). 설명용 메타 정보로만 사용하며, 노출도 계산에는 사용하지 않음.';

