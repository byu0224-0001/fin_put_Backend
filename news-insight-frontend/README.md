# News Insight Frontend

> 뉴스 소비를 단순 정보 읽기가 아닌 '나만의 인사이트 자산'으로 전환시키는 AI 기반 개인 지식 성장 플랫폼의 프론트엔드

## 🎯 개요

Next.js 기반의 뉴스 인사이트 플랫폼 프론트엔드입니다.

## 🚀 시작하기

### 사전 요구사항

- Node.js 18+
- npm 또는 yarn

### 설치 및 실행

```bash
# 의존성 설치
npm install

# 개발 서버 실행
npm run dev
```

개발 서버는 http://localhost:3000 에서 실행됩니다.

### 빌드

```bash
# 프로덕션 빌드
npm run build

# 프로덕션 서버 실행
npm start
```

## 📁 프로젝트 구조

```
news-insight-frontend/
├── app/                    # Next.js App Router
│   ├── (article)/         # 기사 관련 페이지
│   ├── (feed)/            # 피드 페이지
│   ├── (insight)/         # 인사이트 페이지
│   └── layout.tsx         # 루트 레이아웃
├── components/            # React 컴포넌트
│   ├── common/           # 공통 컴포넌트
│   ├── feed/             # 피드 컴포넌트
│   ├── insight/          # 인사이트 컴포넌트
│   └── ui/               # UI 컴포넌트
├── lib/                  # 유틸리티 및 API 클라이언트
│   ├── api.ts            # API 클라이언트
│   ├── hooks.ts          # React Hooks
│   └── types.ts          # TypeScript 타입
└── public/               # 정적 파일
```

## 🛠️ 기술 스택

- **Framework**: Next.js 14+ (App Router)
- **Language**: TypeScript
- **Styling**: Tailwind CSS
- **UI Components**: shadcn/ui
- **State Management**: React Query

## 📡 API 연동

백엔드 API는 `lib/api.ts`에서 관리됩니다.

기본 API URL은 환경 변수 `NEXT_PUBLIC_API_URL`로 설정할 수 있습니다.

## 📄 라이선스

MIT

---

**Made with ❤️ for Knowledge Growth**
