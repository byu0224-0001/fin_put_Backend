# Docker 설치 및 실행 가이드

## 🐳 Docker가 설치되지 않은 경우

### Windows에서 Docker 설치

1. **Docker Desktop 다운로드**
   - 공식 사이트: https://www.docker.com/products/docker-desktop/
   - Windows용 설치 파일 다운로드

2. **설치 후 확인**
   ```powershell
   # PowerShell에서 확인
   docker --version
   docker compose version
   ```

3. **Docker Desktop 실행**
   - 시작 메뉴에서 "Docker Desktop" 실행
   - 시스템 트레이에 Docker 아이콘 확인

### 설치 후 실행

```powershell
# 프로젝트 디렉토리로 이동
cd fintech\news-insight-backend

# Docker Compose 실행 (최신 버전)
docker compose up --build

# 또는 구버전 (하이픈 포함)
docker-compose up --build
```

## ⚠️ 문제 해결

### 1. `docker-compose` 명령어가 작동하지 않음

**최신 Docker Desktop (v2.0+)**
- `docker compose` 사용 (하이픈 없음)
- Docker CLI의 하위 명령으로 통합됨

**구버전 Docker**
- `docker-compose` 사용 (하이픈 포함)
- 별도 설치 필요: `pip install docker-compose`

### 2. Docker Desktop이 시작되지 않음

- **WSL 2 필요**: Windows 10/11에서 WSL 2 설치 필요
- **가상화 활성화**: BIOS에서 가상화 기능 활성화
- **Hyper-V**: Windows Pro/Enterprise에서 Hyper-V 활성화

### 3. 권한 오류

- PowerShell을 **관리자 권한**으로 실행
- Docker Desktop에 관리자 권한 부여

## 🔄 Docker 없이 실행하는 방법

Docker를 설치하지 않고 로컬에서 실행하려면 `LOCAL_SETUP.md`를 참고하세요.

