# 백엔드 서버 실행 가이드

## 문제 해결: 백엔드 서버 연결 오류

### 증상
- 프론트엔드에서 "백엔드 서버에 연결할 수 없습니다" 오류 발생
- 포트 8000은 열려 있지만 HTTP 요청에 응답하지 않음

### 원인
1. **모델 Warm-up 중 멈춤**: AI 모델 로딩에 시간이 오래 걸려 서버가 시작되지 않음
2. **모듈 미설치**: `neo4j` 등 필수 패키지가 설치되지 않음
3. **DB 연결 실패**: PostgreSQL/Neo4j 연결 오류로 서버 시작 실패

## 해결 방법

### 1. 필요한 패키지 설치 확인

```bash
cd fintech/news-insight-backend
pip install -r requirements.txt
```

### 2. 서버 시작

#### 방법 1: Python 직접 실행
```bash
cd fintech/news-insight-backend
python -m app.main
```

#### 방법 2: PowerShell 스크립트 사용
```powershell
cd fintech/news-insight-backend
.\start_server.ps1
```

#### 방법 3: Docker 사용 (권장)
```bash
docker-compose up --build
```

### 3. 서버 로그 확인

서버 시작 시 다음과 같은 로그가 출력됩니다:

```
==================================================
서버 시작 중...
==================================================
기업명 딕셔너리 로딩 시작...
AI 모델 Warm-up 시작...
[1/4] Kiwi 형태소 분석기 로드 중...
[1/4] Kiwi 로드 완료 (시간: X.XX초)
[2/4] KR-SBERT 모델 로드 중...
[2/4] KR-SBERT 로드 완료 (시간: X.XX초)
...
==================================================
✅ 모든 모델 Warm-up 완료! (총 시간: XX.XX초)
==================================================
서버 시작 완료
==================================================
INFO:     Uvicorn running on http://0.0.0.0:8000
```

### 4. 서버 상태 확인

서버가 정상적으로 실행 중인지 확인:

```bash
# 방법 1: 브라우저에서 확인
http://localhost:8000/health

# 방법 2: PowerShell
Invoke-WebRequest -Uri http://localhost:8000/health -UseBasicParsing

# 방법 3: Python
python check_server.py
```

### 5. 포트 충돌 해결

다른 프로세스가 포트 8000을 사용 중인 경우:

```powershell
# 포트 8000을 사용하는 프로세스 확인
netstat -ano | findstr :8000

# 프로세스 종료 (PID 확인 후)
Stop-Process -Id <PID> -Force
```

## 문제가 계속되면

1. **로그 확인**: 서버 실행 시 출력되는 에러 메시지 확인
2. **DB 연결 확인**: PostgreSQL/Neo4j가 실행 중인지 확인
3. **환경 변수 확인**: `.env` 파일이 올바르게 설정되었는지 확인
4. **모듈 설치 확인**: `pip list | findstr neo4j` 등으로 패키지 설치 여부 확인

