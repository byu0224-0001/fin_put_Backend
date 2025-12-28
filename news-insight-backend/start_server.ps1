# 백엔드 서버 시작 스크립트

Write-Host "=" * 50
Write-Host "백엔드 서버 시작 중..."
Write-Host "=" * 50

# 현재 디렉토리 확인
$scriptPath = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $scriptPath

# Python 가상환경 활성화 (있는 경우)
if (Test-Path "venv\Scripts\Activate.ps1") {
    Write-Host "가상환경 활성화 중..."
    & "venv\Scripts\Activate.ps1"
}

# 필요한 패키지 설치 확인
Write-Host "필요한 패키지 확인 중..."
python -c "import neo4j; import fastapi; import uvicorn; print('✅ 필수 패키지 확인 완료')" 2>&1

if ($LASTEXITCODE -ne 0) {
    Write-Host "⚠️ 필수 패키지가 설치되지 않았습니다."
    Write-Host "requirements.txt에서 패키지를 설치하려면 다음 명령을 실행하세요:"
    Write-Host "pip install -r requirements.txt"
    Write-Host ""
}

# 서버 시작
Write-Host ""
Write-Host "서버 시작 중..."
Write-Host "서버 주소: http://localhost:8000"
Write-Host "문서: http://localhost:8000/docs"
Write-Host ""
Write-Host "서버를 중지하려면 Ctrl+C를 누르세요."
Write-Host ""

python -m app.main

