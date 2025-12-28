# 백엔드 서버 시작 스크립트 (Docker 사용)

Write-Host "=" * 70
Write-Host "백엔드 서버 시작 (Docker)"
Write-Host "=" * 70
Write-Host ""

# 현재 디렉토리로 이동
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptPath

# Docker Desktop 실행 확인
Write-Host "1. Docker Desktop 실행 확인 중..."
try {
    docker info | Out-Null
    Write-Host "   [OK] Docker Desktop 실행 중" -ForegroundColor Green
} catch {
    Write-Host "   [ERROR] Docker Desktop이 실행되지 않았습니다!" -ForegroundColor Red
    Write-Host ""
    Write-Host "   해결 방법:" -ForegroundColor Yellow
    Write-Host "   1. Docker Desktop을 시작해주세요"
    Write-Host "   2. 또는 다음 명령으로 시작:" -ForegroundColor Yellow
    Write-Host "      Start-Process 'C:\Program Files\Docker\Docker\Docker Desktop.exe'"
    Write-Host ""
    Write-Host "   Docker Desktop이 시작될 때까지 기다린 후 다시 시도하세요."
    exit 1
}

Write-Host ""

# Docker Compose 서비스 상태 확인
Write-Host "2. Docker Compose 서비스 상태 확인 중..."
docker-compose ps

Write-Host ""

# 백엔드 서버 시작/재시작
Write-Host "3. 백엔드 서버 시작/재시작 중..."
Write-Host ""
Write-Host "   명령어: docker-compose up -d api"
Write-Host ""

docker-compose up -d api

if ($LASTEXITCODE -eq 0) {
    Write-Host ""
    Write-Host "   [OK] 백엔드 서버 시작 완료!" -ForegroundColor Green
    Write-Host ""
    Write-Host "   서버 로그 확인:"
    Write-Host "   docker-compose logs -f api"
    Write-Host ""
    Write-Host "   서버 상태 확인:"
    Write-Host "   docker-compose ps"
    Write-Host ""
    Write-Host "   서버 주소: http://localhost:8000"
    Write-Host "   API 문서: http://localhost:8000/docs"
} else {
    Write-Host ""
    Write-Host "   [ERROR] 백엔드 서버 시작 실패" -ForegroundColor Red
    Write-Host ""
    Write-Host "   로그 확인:"
    Write-Host "   docker-compose logs api"
}

Write-Host ""
Write-Host "=" * 70

