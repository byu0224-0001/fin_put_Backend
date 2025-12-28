# 필수 스크립트 목록

운영에 필요한 핵심 스크립트만 유지합니다.

## 초기화 스크립트
- `01_create_tables.py` - 데이터베이스 테이블 생성
- `01_import_ontology.py` - 섹터 온톨로지 데이터 초기화
- `04_fetch_dart.py` - DART 기업 데이터 수집

## 운영 스크립트
- `reclassify_all_companies.py` - 전체 기업 섹터 재분류
- `refetch_all_missing_revenue.py` - 매출 데이터 재수집
- `sync_krx_stocks.py` - KRX 주식 데이터 동기화

## 검증 스크립트
- `check_state_consistency.py` - 상태 일관성 체크

## 기타
- `__init__.py` - Python 패키지 초기화

나머지 스크립트는 분석/테스트/디버깅용이므로 삭제합니다.

