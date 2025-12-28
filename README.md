<<<<<<< HEAD
# News Insight Platform

> 뉴스 소비를 단순 정보 읽기가 아닌 '나만의 인사이트 자산'으로 전환시키는 AI 기반 개인 지식 성장 플랫폼

## 📁 프로젝트 구조

이 저장소는 모노레포(Monorepo) 구조로 구성되어 있습니다:

```
fintech/
├── news-insight-backend/    # 백엔드 API 서버
│   ├── app/                 # FastAPI 애플리케이션
│   ├── scripts/             # 유틸리티 스크립트
│   ├── migrations/          # DB 마이그레이션
│   └── ...
├── news-insight-frontend/   # 프론트엔드 웹 애플리케이션
│   ├── app/                 # Next.js App Router
│   ├── components/          # React 컴포넌트
│   └── ...
└── README.md                # 이 파일
```

## 🚀 시작하기

### 백엔드 설정

```bash
cd news-insight-backend

# 환경 변수 설정
cp .env.example .env
# .env 파일을 편집하여 실제 값 입력

# 의존성 설치
pip install -r requirements.txt

# Docker Compose로 실행 (권장)
docker compose up --build

# 또는 로컬에서 실행
uvicorn app.main:app --reload
```

자세한 내용은 [백엔드 README](news-insight-backend/README.md)를 참고하세요.

### 프론트엔드 설정

```bash
cd news-insight-frontend

# 의존성 설치
npm install

# 개발 서버 실행
npm run dev
```

자세한 내용은 [프론트엔드 README](news-insight-frontend/README.md)를 참고하세요.

## 🛠️ 기술 스택

### 백엔드
- **Framework**: FastAPI
- **Database**: PostgreSQL 15+ (pgvector 확장)
- **Queue**: Celery + Redis
- **AI**: OpenAI GPT, Gemini
- **Parsing**: BeautifulSoup4, DART API

### 프론트엔드
- **Framework**: Next.js 14+ (App Router)
- **Language**: TypeScript
- **Styling**: Tailwind CSS
- **UI Components**: shadcn/ui
- **State Management**: React Query

## 📖 문서

- [백엔드 아키텍처](news-insight-backend/docs/SECTOR_CLASSIFICATION_ARCHITECTURE.md)
- [IR Deck 아키텍처](news-insight-backend/docs/IR_DECK_ARCHITECTURE.md)
- [데이터베이스 시작 가이드](news-insight-backend/docs/DATABASE_START_GUIDE.md)

## 📄 라이선스

MIT

---

**Made with ❤️ for Knowledge Growth**

=======
# fin_put_Backend
fin_put_Backend
>>>>>>> 904bcef210f44afc24d280d159184219b411dca4
