# 로컬 개발 환경 설정 (Docker 없이)

Docker를 사용하지 않고 로컬에서 개발 환경을 설정하는 방법입니다.

## 📋 사전 요구사항

1. **Python 3.11+** 설치
2. **PostgreSQL** 설치 및 실행
3. **Neo4j** 설치 및 실행 (선택사항, 그래프 기능 사용 시)
4. **Redis** 설치 및 실행 (Celery 사용 시)

## 🚀 설정 단계

### 1. Python 가상환경 설정

```powershell
# 프로젝트 디렉토리로 이동
cd fintech\news-insight-backend

# 가상환경 생성
python -m venv venv

# 가상환경 활성화 (Windows)
venv\Scripts\activate

# 의존성 설치
pip install -r requirements.txt
```

### 2. PostgreSQL 설정

#### Windows에서 PostgreSQL 설치

1. **다운로드**: https://www.postgresql.org/download/windows/
2. **설치** 후 PostgreSQL 서비스 시작
3. **데이터베이스 생성**:

```sql
-- psql 또는 pgAdmin에서 실행
CREATE DATABASE newsdb;
CREATE USER user WITH PASSWORD 'password';
GRANT ALL PRIVILEGES ON DATABASE newsdb TO user;
```

#### 스키마 생성

```powershell
# PostgreSQL이 PATH에 있는 경우
psql -U user -d newsdb -f sql\schema.sql

# 또는 pgAdmin에서 sql\schema.sql 파일 실행
```

### 3. Neo4j 설정 (선택사항)

#### Windows에서 Neo4j 설치

1. **다운로드**: https://neo4j.com/download/
2. **설치** 후 Neo4j Community Edition 실행
3. **기본 인증**: neo4j / password (첫 실행 시 변경 요청)

### 4. Redis 설정 (Celery 사용 시)

#### Windows에서 Redis 설치

1. **WSL 2 사용** (권장):
   ```powershell
   wsl --install
   # WSL 내부에서 Redis 설치
   ```

2. **또는 Docker로 Redis만 실행**:
   ```powershell
   docker run -d -p 6379:6379 redis:alpine
   ```

3. **또는 Memurai 사용** (Windows 네이티브):
   - https://www.memurai.com/ 다운로드

### 5. 환경 변수 설정

`.env` 파일을 생성하고 다음 내용 추가:

```env
# PostgreSQL 설정
POSTGRES_USER=user
POSTGRES_PASSWORD=password
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=newsdb

# Neo4j 설정 (선택사항)
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=password

# OpenAI 설정 (필수)
OPENAI_API_KEY=sk-your-api-key-here

# Celery & Redis 설정
CELERY_BROKER_URL=redis://localhost:6379/0
CELERY_RESULT_BACKEND=redis://localhost:6379/0

# 애플리케이션 설정
DEBUG=true
```

### 6. 서버 실행

#### FastAPI 서버만 실행 (비동기 작업 제외)

```powershell
# 가상환경 활성화
venv\Scripts\activate

# 서버 실행
python -m uvicorn app.main:app --reload
```

#### Celery Worker 실행 (별도 터미널)

```powershell
# 가상환경 활성화
venv\Scripts\activate

# Celery Worker 실행
celery -A app.celery_worker.celery_app worker --loglevel=info
```

### 7. API 테스트

```powershell
# 헬스 체크
curl http://localhost:8000/health

# API 문서
# 브라우저에서 http://localhost:8000/docs 열기
```

## 🔧 문제 해결

### PostgreSQL 연결 실패

```python
# 연결 테스트
python -c "from app.db import engine; engine.connect(); print('연결 성공')"
```

**해결 방법**:
- PostgreSQL 서비스가 실행 중인지 확인
- 방화벽에서 포트 5432 열기
- `.env` 파일의 연결 정보 확인

### Neo4j 연결 실패

```python
# 연결 테스트
python -c "from app.db import neo4j_driver; neo4j_driver.connect(); print('연결 성공')"
```

**해결 방법**:
- Neo4j가 실행 중인지 확인 (http://localhost:7474)
- 인증 정보 확인 (neo4j/password)
- 방화벽에서 포트 7687, 7474 열기

### Redis 연결 실패

**해결 방법**:
- Redis가 실행 중인지 확인
- Windows에서 WSL 2 또는 Memurai 사용
- 또는 Docker로 Redis만 실행: `docker run -d -p 6379:6379 redis:alpine`

### Celery 작업이 실행되지 않음

**해결 방법**:
- Redis가 정상 실행 중인지 확인
- Celery Worker가 별도 터미널에서 실행 중인지 확인
- 로그 확인: `celery -A app.celery_worker.celery_app worker --loglevel=debug`

## 📝 간단한 테스트 스크립트

`test_setup.py` 파일을 생성하여 모든 연결을 테스트:

```python
# test_setup.py
from app.db import engine, neo4j_driver
from app.config import settings

# PostgreSQL 테스트
try:
    with engine.connect() as conn:
        print("✅ PostgreSQL 연결 성공")
except Exception as e:
    print(f"❌ PostgreSQL 연결 실패: {e}")

# Neo4j 테스트
try:
    driver = neo4j_driver.get_driver()
    driver.verify_connectivity()
    print("✅ Neo4j 연결 성공")
except Exception as e:
    print(f"❌ Neo4j 연결 실패: {e}")

# Redis 테스트
import redis
try:
    r = redis.from_url(settings.CELERY_BROKER_URL)
    r.ping()
    print("✅ Redis 연결 성공")
except Exception as e:
    print(f"❌ Redis 연결 실패: {e}")
```

실행:
```powershell
python test_setup.py
```

## 🎯 다음 단계

모든 서비스가 정상 작동하면:
1. API 문서 확인: http://localhost:8000/docs
2. RSS 피드 수집 테스트: `GET /api/feed`
3. 기사 분석 테스트: `POST /api/article/parse`

