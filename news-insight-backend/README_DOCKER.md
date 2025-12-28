# Docker 백엔드 서버 실행 가이드

## 문제 해결: 백엔드 서버 연결 오류

### 증상
- 프론트엔드에서 "백엔드 서버에 연결할 수 없습니다" 오류
- `docker-compose` 명령어 실행 시 "The system cannot find the file specified" 오류

### 원인
**Docker Desktop이 실행되지 않았습니다.**

---

## 해결 방법

### 1단계: Docker Desktop 시작

#### Windows에서:
1. 시작 메뉴에서 "Docker Desktop" 검색 후 실행
2. 또는 PowerShell에서:
   ```powershell
   Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe"
   ```
3. Docker Desktop이 완전히 시작될 때까지 대기 (트레이 아이콘이 초록색이 될 때까지)

#### Docker Desktop 시작 확인:
```bash
docker --version
docker info
```

---

### 2단계: 백엔드 서버 시작

#### 방법 1: PowerShell 스크립트 사용 (권장)
```powershell
cd fintech/news-insight-backend
.\start_backend.ps1
```

#### 방법 2: Docker Compose 직접 사용
```bash
cd fintech/news-insight-backend

# 전체 서비스 시작 (DB, Redis, Neo4j 포함)
docker-compose up -d

# 백엔드만 시작
docker-compose up -d api

# 백엔드 재시작 (코드 변경 반영)
docker-compose restart api

# 백엔드 재빌드 후 시작 (코드 변경 + 의존성 변경)
docker-compose up --build -d api
```

---

### 3단계: 서버 상태 확인

#### 서비스 상태 확인:
```bash
docker-compose ps
```

#### 백엔드 로그 확인:
```bash
# 실시간 로그
docker-compose logs -f api

# 최근 100줄 로그
docker-compose logs --tail=100 api
```

#### 서버 응답 확인:
```bash
# 브라우저에서
http://localhost:8000/health

# PowerShell에서
Invoke-WebRequest -Uri http://localhost:8000/health -UseBasicParsing
```

---

## 자주 사용하는 명령어

| 명령어 | 설명 |
|--------|------|
| `docker-compose up -d` | 전체 서비스 시작 (백그라운드) |
| `docker-compose up -d api` | 백엔드만 시작 |
| `docker-compose restart api` | 백엔드 재시작 |
| `docker-compose stop api` | 백엔드 중지 |
| `docker-compose down` | 전체 서비스 중지 및 컨테이너 제거 |
| `docker-compose ps` | 서비스 상태 확인 |
| `docker-compose logs -f api` | 백엔드 로그 실시간 확인 |
| `docker-compose up --build -d api` | 재빌드 후 시작 |

---

## 문제 해결

### 문제 1: "The system cannot find the file specified"
- **원인**: Docker Desktop이 실행되지 않음
- **해결**: Docker Desktop을 시작하세요

### 문제 2: 포트 충돌
```bash
# 포트 사용 중인 프로세스 확인
netstat -ano | findstr :8000

# Docker 컨테이너 재시작
docker-compose restart api
```

### 문제 3: 컨테이너가 시작되지 않음
```bash
# 로그 확인
docker-compose logs api

# 컨테이너 재빌드
docker-compose build --no-cache api
docker-compose up -d api
```

### 문제 4: DB 연결 오류
- Docker Compose로 실행하면 DB, Redis, Neo4j가 자동으로 시작됩니다
- `docker-compose ps`로 모든 서비스가 실행 중인지 확인하세요

---

## 전체 서비스 실행 (처음 시작 시)

```bash
cd fintech/news-insight-backend

# 1. Docker Desktop 시작 확인
docker info

# 2. 전체 서비스 시작
docker-compose up -d

# 3. 서비스 상태 확인
docker-compose ps

# 4. 백엔드 로그 확인
docker-compose logs -f api
```

모든 서비스가 정상적으로 시작되면:
- 백엔드 API: http://localhost:8000
- API 문서: http://localhost:8000/docs
- PostgreSQL: localhost:5432
- Redis: localhost:6379
- Neo4j: http://localhost:7474

