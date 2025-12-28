# 기업명 데이터 수집 가이드

## 개요

뉴스 기사에서 기업명을 정확하게 추출하기 위해, 한국 및 미국 상장기업 정보를 DB에 저장하고 서버 시작 시 메모리로 로딩하여 초고속 매칭을 수행합니다.

## 구조

### 데이터베이스 스키마

```sql
CREATE TABLE stocks (
    id SERIAL PRIMARY KEY,
    stock_name VARCHAR(200) NOT NULL,  -- 기업명 (예: "삼성전자", "Apple")
    ticker VARCHAR(20) UNIQUE NOT NULL,  -- 종목코드 (예: "005930", "AAPL")
    market VARCHAR(20),  -- 시장 (KOSPI, KOSDAQ, NASDAQ, NYSE)
    synonyms TEXT[],  -- 약칭, 브랜드명 등 (선택적)
    country VARCHAR(10),  -- 국가 (KR, US)
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 동작 방식

1. **데이터 수집**: `scripts/update_stock_data.py`를 실행하여 한국 및 미국 상장기업 정보를 DB에 저장
2. **서버 시작 시 로딩**: 서버가 시작될 때 DB에서 기업명 데이터를 메모리로 로딩
3. **초고속 매칭**: 모든 요청에서 메모리의 딕셔너리를 사용하여 기업명 매칭 (API 호출 없음)

## 사용 방법

### 1. 기업명 데이터 수집

```bash
# 스크립트 실행
python scripts/update_stock_data.py
```

이 스크립트는:
- 한국 상장기업 정보를 `pykrx`를 통해 수집
- 미국 상장기업 정보를 NASDAQ/NYSE에서 수집
- DB의 `stocks` 테이블에 저장

**실행 주기:**
- 주 1회 실행 권장 (상장/상장폐지 빈도 낮음)
- 또는 수동으로 필요시 실행

### 2. 서버 시작 시 자동 로딩

서버가 시작될 때 자동으로 기업명 딕셔너리를 메모리로 로딩합니다.

```python
# app/main.py
@app.on_event("startup")
async def startup_event():
    """서버 시작 시 실행"""
    load_company_dict_from_db()
```

### 3. 엔티티 추출

뉴스 기사 분석 시 자동으로 기업명이 추출됩니다.

```python
from app.services.pipelines.entities import extract_entities

entities = extract_entities(text)
# 결과: {
#     "ORG": ["삼성전자", "Apple", ...],
#     "PERSON": ["홍길동 대표", ...],
#     "LOCATION": ["서울", "뉴욕", ...]
# }
```

## 성능

### 메모리 사용량
- 한국 기업: 약 3,000개 → 약 5MB
- 미국 기업: 약 8,000개 → 약 10MB
- 총 메모리: 약 15MB

### 매칭 속도
- 기업명 수: 10,000개
- 텍스트 길이: 1,000자
- 매칭 시간: 약 0.01초

### 최적화
- 긴 이름부터 매칭: "삼성전자"가 "삼성"보다 우선 매칭
- 대소문자 무시: 미국 기업명 대응
- 중복 제거: Set을 사용하여 중복 제거
- 조기 종료: 충분한 기업명을 찾으면 중단

## 의존성

### 필수 의존성
- `pykrx>=1.3.0`: 한국 기업명 수집
- `pandas>=2.0.0`: 미국 기업명 수집
- `requests>=2.31.0`: HTTP 요청

### 설치
```bash
pip install pykrx pandas requests
```

또는 `requirements.txt`에 이미 포함되어 있습니다:
```txt
pykrx>=1.3.0  # 한국 기업명 수집
pandas>=2.0.0  # 미국 기업명 수집
```

## 문제 해결

### 1. 기업명 딕셔너리가 비어있음

**원인:**
- DB에 기업명 데이터가 없음
- 스크립트 실행 전 서버 시작

**해결:**
```bash
# 1. 스크립트 실행
python scripts/update_stock_data.py

# 2. 서버 재시작
# 서버를 재시작하면 새로운 기업명 데이터가 로드됩니다.
```

### 2. pykrx 설치 오류

**원인:**
- `pykrx`가 설치되지 않음

**해결:**
```bash
pip install pykrx
```

### 3. DB 연결 오류

**원인:**
- PostgreSQL이 실행되지 않음
- DB 연결 정보가 잘못됨

**해결:**
- `.env` 파일에서 DB 연결 정보 확인
- PostgreSQL이 실행 중인지 확인

## 확장 가능성

### 1. 약칭/브랜드명 추가

```python
# DB에서 synonyms 필드 업데이트
stock.synonyms = ["삼성", "Samsung"]
```

### 2. 다른 국가 기업 추가

```python
# scripts/update_stock_data.py에 다른 국가 추가
def update_japanese_stocks():
    # 일본 기업명 수집
    pass
```

### 3. 성능 최적화

- Trie 구조 사용: 더 빠른 매칭 (복잡하지만 빠름)
- 정규식 사용: 복잡한 패턴 매칭
- 병렬 처리: 대용량 텍스트 처리

## 참고 자료

- [pykrx 문서](https://github.com/sharebook-kr/pykrx)
- [NASDAQ 데이터](https://old.nasdaq.com/screening/companies-by-name.aspx)
- [PostgreSQL 문서](https://www.postgresql.org/docs/)

