-- articles 테이블에 image_url 컬럼 추가
ALTER TABLE articles ADD COLUMN IF NOT EXISTS image_url VARCHAR(1000);

-- 기존 데이터의 image_url 업데이트를 위해 인덱스는 필요 없음 (nullable이므로)

