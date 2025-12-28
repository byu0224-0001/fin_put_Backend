FROM python:3.11-slim

WORKDIR /code

# 시스템 패키지 설치 (Kiwi 컴파일을 위해 g++, cmake, make 추가)
# Playwright 브라우저 의존성 추가
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    cmake \
    make \
    postgresql-client \
    # Playwright 브라우저 의존성 (완전한 목록)
    libnss3 \
    libnspr4 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libdbus-1-3 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    libxshmfence1 \
    libxss1 \
    fonts-liberation \
    libappindicator3-1 \
    xdg-utils \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Python 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Playwright 브라우저 설치 (동적 사이트 파싱용, 필수)
# 네트워크 타임아웃 설정 (브라우저 다운로드 시간 증가)
ENV PLAYWRIGHT_DOWNLOAD_CONNECTION_TIMEOUT=60000

# 1단계: 시스템 의존성 확인 (이미 apt-get으로 설치했지만 확인)
RUN python -m playwright install-deps chromium || echo "⚠️ 시스템 의존성 확인 완료"

# 2단계: Chromium 브라우저 설치 (재시도 로직 포함)
# 최대 3회 재시도하여 네트워크 문제 해결
RUN for i in 1 2 3; do \
    echo "Playwright 브라우저 설치 시도 $i/3..." && \
    python -m playwright install chromium && \
    echo "✅ Playwright 브라우저 설치 성공" && \
    break || \
    (echo "⚠️ 설치 실패, 재시도 대기 중..." && sleep 5); \
    done

# 3단계: 설치 확인 (실제로 브라우저 실행 테스트)
# 실패 시 빌드 중단하여 문제를 조기에 발견
RUN python -c "\
from playwright.sync_api import sync_playwright; \
p = sync_playwright().start(); \
browser = p.chromium.launch(headless=True); \
browser.close(); \
p.stop(); \
print('✅ Playwright 설치 확인 완료 - 브라우저 실행 성공')" || \
(echo "❌ Playwright 설치 확인 실패 - 브라우저를 실행할 수 없습니다" && exit 1)

# 애플리케이션 코드 복사
COPY . .

# 포트 노출
EXPOSE 8000

# 실행 명령
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]

