# 기업명 데이터 설정 가이드

## 📋 개요

기업명 데이터는 **스크립트를 실행하기만 하면 자동으로 수집되고 DB에 저장됩니다**.
PostgreSQL에 직접 로그인해서 데이터를 넣을 필요가 없습니다.

## 🔧 사전 준비 사항

### 1. PostgreSQL 실행 확인

PostgreSQL이 실행 중인지 확인하세요:

```bash
# Windows
# 서비스 관리자에서 PostgreSQL 서비스 확인
# 또는
psql --version
```

### 2. 데이터베이스 생성 (필수)

**데이터베이스는 미리 생성되어 있어야 합니다.**

```sql
-- PostgreSQL에 접속
psql -U postgres

-- 데이터베이스 생성
CREATE DATABASE newsdb;

-- 사용자 생성 (필요한 경우)
CREATE USER user WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE newsdb TO user;

-- 연결 확인
\c newsdb
```

### 3. .env 파일 설정

프로젝트 루트에 `.env` 파일이 있어야 하고, 다음 내용이 포함되어 있어야 합니다:

```env
# PostgreSQL 설정
POSTGRES_USER=user
POSTGRES_PASSWORD=password
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=newsdb
```

**중요**: `.env` 파일은 **UTF-8 인코딩**으로 저장되어 있어야 합니다.

## 🚀 실행 방법

### 1. 스크립트 실행

```bash
# 프로젝트 루트에서
python scripts/update_stock_data.py
```

### 2. 자동으로 수행되는 작업

스크립트가 자동으로 다음 작업을 수행합니다:

1. **데이터베이스 연결 확인**
   - `.env` 파일의 설정을 사용하여 PostgreSQL에 연결

2. **테이블 자동 생성**
   - `stocks` 테이블이 없으면 자동으로 생성
   - 인덱스 및 트리거도 자동 생성

3. **한국 기업명 데이터 수집**
   - `pykrx`를 사용하여 한국 상장기업 정보 수집
   - KOSPI, KOSDAQ 모든 종목 수집

4. **미국 기업명 데이터 수집**
   - NASDAQ, NYSE 상장기업 정보 수집
   - 공개 API를 통해 데이터 수집

5. **데이터베이스에 자동 저장**
   - 수집한 데이터를 `stocks` 테이블에 자동 저장
   - 기존 데이터는 삭제하고 새 데이터로 업데이트

## 📊 결과 확인

### PostgreSQL에서 직접 확인 (선택 사항)

```sql
-- 데이터베이스에 접속
psql -U user -d newsdb

-- stocks 테이블 확인
SELECT COUNT(*) FROM stocks;

-- 한국 기업 확인
SELECT COUNT(*) FROM stocks WHERE country = 'KR';

-- 미국 기업 확인
SELECT COUNT(*) FROM stocks WHERE country = 'US';

-- 샘플 데이터 확인
SELECT * FROM stocks LIMIT 10;
```

### 서버 로그에서 확인

스크립트 실행 시 다음과 같은 로그가 출력됩니다:

```
==================================================
기업명 데이터 업데이트 시작
==================================================
데이터베이스 호스트: localhost
데이터베이스 포트: 5432
데이터베이스 이름: newsdb
데이터베이스 사용자: user
데이터베이스 연결 확인 중...
데이터베이스 연결 성공
데이터베이스 테이블 생성 중...
데이터베이스 테이블 생성 완료
한국 기업명 데이터 수집 시작...
기준일: 20241113, 상장기업 수: 3000
한국 기업명 데이터 업데이트 완료: 3000개
미국 기업명 데이터 수집 시작...
NASDAQ: 4000개 수집
NYSE: 4000개 수집
미국 기업명 데이터 업데이트 완료: 8000개
==================================================
업데이트 완료!
한국 기업: 3000개
미국 기업: 8000개
총 기업: 11000개
==================================================
```

## 🔄 업데이트 주기

### 권장 실행 주기

- **주 1회 실행 권장** (상장/상장폐지 빈도 낮음)
- 또는 수동으로 필요시 실행

### 자동화 (선택 사항)

#### Windows 작업 스케줄러

1. 작업 스케줄러 열기
2. 기본 작업 만들기
3. 트리거: 매주 월요일 오전 9시
4. 동작: 프로그램 시작
   - 프로그램: `python`
   - 인수: `C:\path\to\scripts\update_stock_data.py`
   - 시작 위치: `C:\path\to\project`

#### Linux Cron

```bash
# 매주 월요일 오전 9시 실행
0 9 * * 1 cd /path/to/project && python scripts/update_stock_data.py
```

## ❓ 문제 해결

### 1. 데이터베이스 연결 실패

**문제**: `데이터베이스 연결 실패` 오류

**해결 방법**:
1. PostgreSQL이 실행 중인지 확인
2. `.env` 파일의 연결 정보 확인
3. 데이터베이스가 생성되어 있는지 확인
4. 사용자 권한 확인

### 2. 인코딩 오류

**문제**: `'utf-8' codec can't decode byte 0xbe` 오류

**해결 방법**:
1. `.env` 파일이 UTF-8 인코딩으로 저장되어 있는지 확인
2. 에디터에서 파일을 열고 인코딩을 UTF-8로 변경
3. 파일을 다시 저장

### 3. 테이블 생성 실패

**문제**: `테이블 생성 실패` 오류

**해결 방법**:
1. 데이터베이스 사용자에게 테이블 생성 권한이 있는지 확인
2. 기존 테이블이 있는지 확인
3. PostgreSQL 로그 확인

### 4. 데이터 수집 실패

**문제**: `한국 기업명 데이터 수집 실패` 또는 `미국 기업명 데이터 수집 실패`

**해결 방법**:
1. 인터넷 연결 확인
2. `pykrx` 라이브러리가 설치되어 있는지 확인: `pip install pykrx`
3. `pandas` 라이브러리가 설치되어 있는지 확인: `pip install pandas`
4. API 접근 제한 확인 (너무 많은 요청 시 일시적으로 차단될 수 있음)

## 📝 요약

### ✅ 자동으로 수행되는 작업

- 데이터베이스 연결
- 테이블 생성
- 데이터 수집
- 데이터 저장

### ❌ 수동으로 수행해야 하는 작업

- PostgreSQL 설치 및 실행
- 데이터베이스 생성 (`CREATE DATABASE newsdb;`)
- `.env` 파일 설정
- 스크립트 실행 (`python scripts/update_stock_data.py`)

### 🎯 결론

**PostgreSQL에 직접 로그인해서 데이터를 넣을 필요가 없습니다!**
스크립트를 실행하기만 하면 모든 작업이 자동으로 수행됩니다.

