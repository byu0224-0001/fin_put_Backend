# Docker Desktop 상태 확인 스크립트

Write-Host "=" * 70
Write-Host "Docker Desktop 상태 확인"
Write-Host "=" * 70
Write-Host ""

# 1. Docker Desktop 프로세스 확인
Write-Host "1. Docker Desktop 프로세스 확인 중..."
$dockerProcesses = Get-Process | Where-Object {$_.ProcessName -like "*Docker*" -or $_.ProcessName -like "*com.docker*"}
if ($dockerProcesses) {
    Write-Host "   [OK] Docker Desktop 프로세스 실행 중:" -ForegroundColor Green
    $dockerProcesses | ForEach-Object {
        Write-Host "      - $($_.ProcessName) (PID: $($_.Id))"
    }
} else {
    Write-Host "   [ERROR] Docker Desktop 프로세스가 실행되지 않았습니다!" -ForegroundColor Red
}

Write-Host ""

# 2. Docker 명령어 확인
Write-Host "2. Docker 명령어 확인 중..."
try {
    $dockerVersion = docker --version 2>&1
    Write-Host "   [OK] Docker 설치됨: $dockerVersion" -ForegroundColor Green
} catch {
    Write-Host "   [ERROR] Docker가 설치되지 않았습니다!" -ForegroundColor Red
    exit 1
}

Write-Host ""

# 3. Docker 엔진 연결 확인
Write-Host "3. Docker 엔진 연결 확인 중..."
try {
    $dockerInfo = docker info 2>&1
    if ($LASTEXITCODE -eq 0) {
        Write-Host "   [OK] Docker 엔진 연결 성공!" -ForegroundColor Green
        Write-Host ""
        Write-Host "   Docker 엔진 정보:"
        $dockerInfo | Select-String -Pattern "Server Version|Operating System|OSType|Architecture" | ForEach-Object {
            Write-Host "      $_"
        }
    } else {
        Write-Host "   [ERROR] Docker 엔진에 연결할 수 없습니다!" -ForegroundColor Red
        Write-Host ""
        Write-Host "   오류 메시지:"
        $dockerInfo | ForEach-Object {
            Write-Host "      $_" -ForegroundColor Yellow
        }
        Write-Host ""
        Write-Host "   해결 방법:" -ForegroundColor Yellow
        Write-Host "   1. Docker Desktop을 완전히 시작해주세요"
        Write-Host "   2. Docker Desktop 트레이 아이콘이 초록색이 될 때까지 기다려주세요"
        Write-Host "   3. Docker Desktop을 재시작해보세요"
        Write-Host ""
        Write-Host "   Docker Desktop 시작:"
        Write-Host "   Start-Process 'C:\Program Files\Docker\Docker\Docker Desktop.exe'"
        exit 1
    }
} catch {
    Write-Host "   [ERROR] Docker 명령어 실행 실패: $_" -ForegroundColor Red
    exit 1
}

Write-Host ""

# 4. Docker Compose 확인
Write-Host "4. Docker Compose 확인 중..."
try {
    $composeVersion = docker-compose --version 2>&1
    Write-Host "   [OK] Docker Compose 설치됨: $composeVersion" -ForegroundColor Green
} catch {
    Write-Host "   [WARNING] Docker Compose를 찾을 수 없습니다. 'docker compose'를 사용해보세요." -ForegroundColor Yellow
}

Write-Host ""
Write-Host "=" * 70
Write-Host "Docker Desktop이 정상적으로 실행 중입니다!" -ForegroundColor Green
Write-Host "이제 'docker-compose up -d' 명령을 실행할 수 있습니다." -ForegroundColor Green
Write-Host "=" * 70

