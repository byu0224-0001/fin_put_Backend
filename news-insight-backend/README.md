# News Insight Backend

> 뉴스 소비를 단순 정보 읽기가 아닌 '나만의 인사이트 자산'으로 전환시키는 AI 기반 개인 지식 성장 플랫폼의 백엔드 API

## 🎯 개요

이 프로젝트는 뉴스 기사를 수집하고, AI로 요약/분석하여 개인 지식 그래프로 축적하는 백엔드 시스템입니다.

### 핵심 기능

- **RSS 피드 수집**: 언론사 RSS를 통한 자동 뉴스 수집
- **AI 요약/분석**: OpenAI GPT를 활용한 기사 요약 및 키워드/엔티티 추출
- **섹터 분류**: 기업 섹터 자동 분류 (28개 섹터)
- **비동기 처리**: Celery를 통한 백그라운드 분석 작업
- **그래프 데이터베이스**: PostgreSQL + pgvector를 활용한 엔티티 관계 저장
- **연관 뉴스 추천**: 그래프 기반 관련 기사 추천

## 🏗️ 아키텍처

```
RSS Feed → FastAPI → PostgreSQL (기사 저장)
                ↓
         Celery Queue → AI 분석 → PostgreSQL (요약 저장)
                                    ↓
                              pgvector (임베딩)
```

## 📁 프로젝트 구조

```
news-insight-backend/
├── app/
│   ├── __init__.py
│   ├── main.py                # FastAPI Entry Point
│   ├── config.py              # 환경 변수 / 설정
│   ├── db.py                  # PostgreSQL 연결
│   ├── celery_worker.py       # Celery 작업 정의
│   ├── models/                # SQLAlchemy 모델
│   │   ├── article.py
│   │   ├── company_detail.py
│   │   ├── stock.py
│   │   ├── investor_sector.py
│   │   └── ...
│   ├── services/              # 비즈니스 로직
│   │   ├── sector_classifier.py    # 섹터 분류
│   │   ├── dart_parser.py          # DART API 파싱
│   │   ├── revenue_table_parser.py # 매출 테이블 파싱
│   │   └── ...
│   ├── routes/                # API 라우팅
│   │   ├── feed.py
│   │   ├── article.py
│   │   ├── insight.py
│   │   └── scenario.py
│   └── utils/
├── scripts/                   # 유틸리티 스크립트
│   ├── reclassify_all_companies.py
│   ├── refetch_all_missing_revenue.py
│   └── ...
├── migrations/                # DB 마이그레이션
├── sql/                       # SQL 스크립트
├── docs/                     # 문서
│   ├── SECTOR_CLASSIFICATION_ARCHITECTURE.md
│   └── ...
├── requirements.txt
├── docker-compose.yml
├── Dockerfile
├── .env.example
└── README.md
```

## 🚀 시작하기

### 사전 요구사항

- Python 3.11+
- Docker & Docker Compose (권장)
- PostgreSQL 15+ (pgvector 확장)
- Redis 7+
- OpenAI API 키
- DART API 키

### 환경 설정

1. **저장소 클론**

```bash
git clone <repository-url>
cd news-insight-backend
```

2. **환경 변수 설정**

`.env` 파일을 생성하고 `.env.example`을 참고하여 설정하세요:

```bash
cp .env.example .env
# .env 파일을 편집하여 실제 값 입력
```

주요 환경 변수:
- `POSTGRES_*`: PostgreSQL 연결 정보
- `OPENAI_API_KEY`: OpenAI API 키 (필수)
- `DART_API_KEY`: DART API 키 (필수)
- `CELERY_BROKER_URL`: Redis 브로커 URL

3. **실행 방법**

**Docker Compose로 실행 (권장)**
```bash
docker compose up --build
```

이 명령으로 다음 서비스가 실행됩니다:
- FastAPI 서버: http://localhost:8000
- PostgreSQL: localhost:5432
- Redis: localhost:6379
- Celery Worker: 백그라운드 실행

**로컬에서 실행**
```bash
# 의존성 설치
pip install -r requirements.txt

# 데이터베이스 마이그레이션
alembic upgrade head

# 서버 실행
uvicorn app.main:app --reload

# Celery Worker (별도 터미널)
celery -A app.celery_worker.celery_app worker --loglevel=info
```

## 📡 주요 API 엔드포인트

### 피드 (Feed)
- `GET /api/feed` - RSS 피드에서 뉴스 수집

### 기사 (Article)
- `GET /api/article/{article_id}` - 기사 상세 정보
- `GET /api/article/{article_id}/insight` - 기사 인사이트 조회

### 인사이트 (Insight)
- `GET /api/insight` - 내 인사이트 목록
- `GET /api/insight/{article_id}` - 특정 기사 인사이트

### 시나리오 (Scenario)
- `GET /api/scenario` - 시나리오 목록

## 🛠️ 주요 스크립트

### 섹터 분류
```bash
# 전체 기업 재분류
python scripts/reclassify_all_companies.py --apply

# 특정 티커만 재분류
python scripts/reclassify_all_companies.py --ticker 096770 --apply
```

### 매출 데이터 재수집
```bash
# 매출 데이터 없는 기업 재수집
python scripts/refetch_all_missing_revenue.py --apply

# 특정 티커만 재수집
python scripts/refetch_all_missing_revenue.py --ticker 096770 --apply
```

### 상태 확인
```bash
# 상태 일관성 체크
python scripts/check_state_consistency.py

# HOLD 사유 체크
python scripts/check_hold_reason_code.py

# 신뢰도 리포트
python scripts/generate_confidence_report.py
```

## 🛠️ 기술 스택

- **Framework**: FastAPI
- **Database**: PostgreSQL 15+ (pgvector 확장)
- **Queue**: Celery + Redis
- **AI**: OpenAI GPT
- **Parsing**: BeautifulSoup4, DART API

## 📖 문서

- [섹터 분류 아키텍처](docs/SECTOR_CLASSIFICATION_ARCHITECTURE.md)
- [IR Deck 아키텍처](docs/IR_DECK_ARCHITECTURE.md)
- [데이터베이스 시작 가이드](docs/DATABASE_START_GUIDE.md)

## 📄 라이선스

MIT

## 🤝 기여

이슈 및 PR 환영합니다!

---

**Made with ❤️ for Knowledge Growth**
