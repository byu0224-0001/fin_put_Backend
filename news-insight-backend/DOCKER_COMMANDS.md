# Docker 명령어 가이드

## 백엔드 서버 관리

### 전체 서비스 시작
```bash
cd fintech/news-insight-backend
docker-compose up -d
```

### 전체 서비스 중지
```bash
docker-compose down
```

### 전체 서비스 재시작 (빌드 포함)
```bash
docker-compose up --build -d
```

---

## 백엔드 서버만 관리

### 백엔드만 시작
```bash
docker-compose up -d api
```

### 백엔드만 중지
```bash
docker-compose stop api
```

### 백엔드만 재시작
```bash
docker-compose restart api
```

### 백엔드 재빌드 후 재시작
```bash
docker-compose up --build -d api
```

### 백엔드 로그 확인
```bash
docker-compose logs -f api
```

### 백엔드 로그 확인 (최근 100줄)
```bash
docker-compose logs --tail=100 api
```

---

## 문제 해결

### 서비스 상태 확인
```bash
docker-compose ps
```

### 서비스 시작 (포그라운드 - 로그 확인용)
```bash
docker-compose up api
```

### 모든 컨테이너 강제 재시작
```bash
docker-compose restart
```

### 컨테이너 재빌드 (캐시 없이)
```bash
docker-compose build --no-cache api
docker-compose up -d api
```

---

## 환경 변수 설정

`.env` 파일이 있는 경우 자동으로 로드됩니다.
```bash
# .env 파일 예시
POSTGRES_USER=user
POSTGRES_PASSWORD=password
OPENAI_API_KEY=your_key_here
```

