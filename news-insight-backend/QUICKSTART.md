# 빠른 시작 가이드

## 🚀 5분 안에 시작하기

### 1. Docker Compose로 실행 (가장 간단)

```bash
# 1. 환경 변수 설정
cp .env.example .env
# .env 파일에서 OPENAI_API_KEY 설정 (필수!)

# 2. 모든 서비스 시작
docker-compose up --build

# 3. 브라우저에서 확인
# - API: http://localhost:8000
# - API Docs: http://localhost:8000/docs
# - Neo4j Browser: http://localhost:7474
```

### 2. API 테스트

```bash
# 뉴스 피드 수집
curl http://localhost:8000/api/feed

# 헬스 체크
curl http://localhost:8000/health
```

### 3. 기사 분석 워크플로우

```bash
# 1. 피드에서 기사 수집
curl http://localhost:8000/api/feed

# 2. 기사 분석 요청 (WebView에서 본문 추출 후)
curl -X POST http://localhost:8000/api/article/parse \
  -H "Content-Type: application/json" \
  -d '{
    "url": "https://www.mk.co.kr/news/...",
    "text": "기사 본문 텍스트..."
  }'

# 3. 분석 상태 확인
curl http://localhost:8000/api/article/status/{task_id}

# 4. 인사이트 조회
curl http://localhost:8000/api/insight/{article_id}
```

## 📋 체크리스트

- [ ] Docker & Docker Compose 설치
- [ ] `.env` 파일 생성 및 `OPENAI_API_KEY` 설정
- [ ] `docker-compose up` 실행
- [ ] http://localhost:8000/docs 에서 API 문서 확인

## ⚠️ 문제 해결

### PostgreSQL 연결 실패
- Docker Compose가 정상 실행되었는지 확인
- `docker-compose logs db`로 로그 확인

### Neo4j 연결 실패
- Neo4j가 시작될 때까지 대기 (약 30초)
- `docker-compose logs graph`로 로그 확인

### Celery 작업이 실행되지 않음
- Celery worker가 실행 중인지 확인: `docker-compose logs celery`
- Redis 연결 확인

### OpenAI API 오류
- `.env` 파일의 `OPENAI_API_KEY` 확인
- API 키 유효성 및 크레딧 확인

## 🔗 유용한 링크

- API 문서: http://localhost:8000/docs
- Neo4j Browser: http://localhost:7474 (neo4j/password)
- Swagger UI: http://localhost:8000/docs

---

**문제가 있으면 이슈를 등록해주세요!**

