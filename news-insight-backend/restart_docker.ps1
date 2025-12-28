# Docker Desktop 재시작 스크립트

Write-Host "=" * 70
Write-Host "Docker Desktop 재시작"
Write-Host "=" * 70
Write-Host ""

# 1. Docker Desktop 프로세스 확인
Write-Host "1. Docker Desktop 프로세스 확인 중..."
$dockerProcesses = Get-Process | Where-Object {$_.ProcessName -like "*Docker*" -or $_.ProcessName -like "*com.docker*"}
if ($dockerProcesses) {
    Write-Host "   Docker Desktop 프로세스가 실행 중입니다."
    Write-Host "   재시작을 위해 종료합니다..."
    
    # Docker Desktop 프로세스 종료
    $dockerProcesses | ForEach-Object {
        Stop-Process -Id $_.Id -Force -ErrorAction SilentlyContinue
    }
    
    Write-Host "   종료 완료. 5초 대기 중..."
    Start-Sleep -Seconds 5
} else {
    Write-Host "   Docker Desktop이 실행되지 않았습니다."
}

Write-Host ""

# 2. Docker Desktop 재시작
Write-Host "2. Docker Desktop 시작 중..."
try {
    $dockerPath = "C:\Program Files\Docker\Docker\Docker Desktop.exe"
    if (Test-Path $dockerPath) {
        Start-Process $dockerPath
        Write-Host "   [OK] Docker Desktop 시작 완료!" -ForegroundColor Green
        Write-Host ""
        Write-Host "   Docker Desktop이 완전히 시작될 때까지 기다려주세요..."
        Write-Host "   (보통 30초~1분 정도 소요됩니다)"
        Write-Host "   트레이 아이콘이 초록색이 되면 준비 완료입니다."
        Write-Host ""
        Write-Host "   상태 확인 명령어:"
        Write-Host "   docker info"
        Write-Host ""
        Write-Host "   준비되면 다음 명령으로 백엔드를 시작하세요:"
        Write-Host "   docker-compose up -d"
    } else {
        Write-Host "   [ERROR] Docker Desktop을 찾을 수 없습니다: $dockerPath" -ForegroundColor Red
        Write-Host ""
        Write-Host "   Docker Desktop이 설치되어 있는지 확인해주세요."
        Write-Host "   설치 위치: C:\Program Files\Docker\Docker\Docker Desktop.exe"
    }
} catch {
    Write-Host "   [ERROR] Docker Desktop 시작 실패: $_" -ForegroundColor Red
}

Write-Host ""
Write-Host "=" * 70

