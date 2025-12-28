# 백엔드 서버 빠른 시작 가이드

## 문제: Docker Desktop 엔진 연결 오류

**증상**: `error during connect: The system cannot find the file specified`

**원인**: Docker Desktop이 실행 중이지만 Linux 엔진이 시작되지 않음

---

## 해결 방법

### 방법 1: Docker Desktop 재시작 (권장)

1. **Docker Desktop 완전 종료**
   - 시스템 트레이에서 Docker Desktop 아이콘 우클릭
   - "Quit Docker Desktop" 선택
   - 모든 Docker 프로세스가 종료될 때까지 대기 (약 10초)

2. **Docker Desktop 재시작**
   - 시작 메뉴에서 "Docker Desktop" 실행
   - 또는 PowerShell에서:
     ```powershell
     Start-Process "C:\Program Files\Docker\Docker\Docker Desktop.exe"
     ```

3. **Docker Desktop 완전 시작 대기**
   - 트레이 아이콘이 초록색이 될 때까지 대기 (30초~1분)
   - Docker Desktop UI가 완전히 열릴 때까지 대기

4. **Docker 엔진 연결 확인**
   ```powershell
   docker info
   ```
   - `Server Version`이 표시되면 성공!

5. **백엔드 서버 시작**
   ```bash
   cd fintech/news-insight-backend
   docker-compose up -d
   ```

---

### 방법 2: PowerShell 스크립트 사용

```powershell
cd fintech/news-insight-backend

# Docker Desktop 재시작
.\restart_docker.ps1

# 30초~1분 대기 후
docker info  # 엔진 연결 확인

# 백엔드 시작
docker-compose up -d
```

---

### 방법 3: 관리자 권한으로 Docker 서비스 시작

```powershell
# 관리자 권한 PowerShell에서 실행
Start-Service com.docker.service

# 서비스 상태 확인
Get-Service com.docker.service

# Docker 엔진 연결 확인
docker info
```

---

## 백엔드 서버 시작 명령어

### 전체 서비스 시작 (DB, Redis, Neo4j, 백엔드)
```bash
docker-compose up -d
```

### 백엔드만 시작
```bash
docker-compose up -d api
```

### 백엔드 재시작
```bash
docker-compose restart api
```

### 백엔드 재빌드 후 시작
```bash
docker-compose up --build -d api
```

---

## 서버 상태 확인

### 서비스 상태 확인
```bash
docker-compose ps
```

### 백엔드 로그 확인
```bash
docker-compose logs -f api
```

### 서버 응답 확인
```bash
# 브라우저에서
http://localhost:8000/health

# PowerShell에서
Invoke-WebRequest -Uri http://localhost:8000/health -UseBasicParsing
```

---

## 문제 해결 체크리스트

- [ ] Docker Desktop이 완전히 종료되었는가?
- [ ] Docker Desktop을 재시작했는가?
- [ ] 트레이 아이콘이 초록색인가?
- [ ] `docker info` 명령이 정상 작동하는가?
- [ ] `docker-compose ps`로 서비스가 실행 중인가?
- [ ] 포트 8000이 열려 있는가?

---

## 주의사항

1. **Docker Desktop 완전 시작 대기**: 시작 후 30초~1분 정도 기다려야 엔진이 준비됩니다.
2. **트레이 아이콘 확인**: 초록색이 되어야 정상입니다.
3. **관리자 권한**: Docker 서비스를 수동으로 시작하려면 관리자 권한이 필요할 수 있습니다.

