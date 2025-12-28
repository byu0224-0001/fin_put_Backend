-- DART 고유번호 컬럼 추가 마이그레이션
-- stocks 테이블에 dart_corp_code 컬럼 추가

-- 컬럼 추가 (이미 존재하면 오류 발생)
ALTER TABLE stocks 
ADD COLUMN IF NOT EXISTS dart_corp_code VARCHAR(8);

-- 인덱스 추가 (성능 향상)
CREATE INDEX IF NOT EXISTS idx_stocks_dart_corp_code ON stocks(dart_corp_code);

-- 코멘트 추가
COMMENT ON COLUMN stocks.dart_corp_code IS 'DART 고유번호 (8자리) - 이중 매핑 전략용';

