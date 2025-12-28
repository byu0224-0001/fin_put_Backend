# PostgreSQL 데이터베이스 시작 가이드

## 🚀 방법 1: Docker Compose 사용 (권장)

### 1단계: Docker Desktop 시작
1. Windows 시작 메뉴에서 **Docker Desktop** 검색 및 실행
2. Docker Desktop이 완전히 시작될 때까지 대기 (시스템 트레이 아이콘 확인)

### 2단계: PostgreSQL 서비스 시작

**PowerShell 또는 CMD에서 실행:**

```powershell
# 프로젝트 디렉토리로 이동
cd C:\Users\Admin\WORKSPACE\Cursor\fintech\news-insight-backend

# PostgreSQL 데이터베이스만 시작 (다른 서비스는 시작하지 않음)
docker-compose up -d db
```

**또는 전체 서비스 시작:**

```powershell
docker-compose up -d
```

### 3단계: 데이터베이스 상태 확인

```powershell
# 실행 중인 컨테이너 확인
docker-compose ps

# PostgreSQL 로그 확인
docker-compose logs db
```

### 4단계: 데이터베이스 연결 테스트

```powershell
# Python으로 연결 테스트
python -c "from app.db import SessionLocal; from sqlalchemy import text; db = SessionLocal(); db.execute(text('SELECT 1')); print('✅ DB 연결 성공')"
```

---

## 🔧 방법 2: 로컬 PostgreSQL 설치 및 실행

### 1단계: PostgreSQL 설치

1. **PostgreSQL 다운로드**
   - https://www.postgresql.org/download/windows/ 접속
   - PostgreSQL 15 또는 최신 버전 다운로드 및 설치

2. **설치 시 설정**
   - 포트: `5432` (기본값)
   - 사용자: `postgres`
   - 비밀번호: 원하는 비밀번호 설정 (나중에 .env 파일에 입력)

### 2단계: PostgreSQL 서비스 시작

**방법 A: Windows 서비스 관리자**
1. `Win + R` → `services.msc` 입력
2. **postgresql-x64-15** (또는 설치된 버전) 찾기
3. 우클릭 → **시작**

**방법 B: PowerShell (관리자 권한)**
```powershell
# PostgreSQL 서비스 이름 확인
Get-Service | Where-Object {$_.DisplayName -like "*PostgreSQL*"}

# 서비스 시작 (서비스 이름을 실제 이름으로 교체)
Start-Service postgresql-x64-15
```

**방법 C: 명령 프롬프트 (관리자 권한)**
```cmd
# PostgreSQL 설치 경로로 이동 (일반적으로)
cd "C:\Program Files\PostgreSQL\15\bin"

# PostgreSQL 시작
pg_ctl start -D "C:\Program Files\PostgreSQL\15\data"
```

### 3단계: .env 파일 설정

`fintech/news-insight-backend/.env` 파일 생성 또는 수정:

```env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=설치시_설정한_비밀번호
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=newsdb
```

### 4단계: 데이터베이스 생성

```powershell
# psql로 접속
psql -U postgres

# 데이터베이스 생성
CREATE DATABASE newsdb;

# 종료
\q
```

---

## 🔍 문제 해결

### Docker Desktop 오류
- **오류**: `unable to get image 'postgres:15': error during connect`
- **해결**: Docker Desktop이 실행 중인지 확인하고 재시작

### 포트 충돌
- **오류**: `port 5432 is already in use`
- **해결**: 
  ```powershell
  # 포트 사용 중인 프로세스 확인
  netstat -ano | findstr :5432
  
  # 프로세스 종료 (PID는 위 명령어 결과에서 확인)
  taskkill /PID [PID] /F
  ```

### 연결 거부 오류
- **오류**: `connection to server at "localhost" (::1), port 5432 failed: Connection refused`
- **해결**:
  1. PostgreSQL 서비스가 실행 중인지 확인
  2. 방화벽 설정 확인
  3. .env 파일의 연결 정보 확인

---

## ✅ 빠른 확인 명령어

```powershell
# Docker로 실행 중인 경우
docker-compose ps db

# 로컬 PostgreSQL인 경우
Get-Service | Where-Object {$_.DisplayName -like "*PostgreSQL*"}

# 연결 테스트
python -c "from app.db import SessionLocal; from sqlalchemy import text; db = SessionLocal(); db.execute(text('SELECT 1')); print('✅ 연결 성공')"
```

