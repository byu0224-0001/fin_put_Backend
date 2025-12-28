#!/bin/bash
# 개발 환경 실행 스크립트

echo "🚀 News Insight Backend 시작..."

# 환경 변수 확인
if [ ! -f .env ]; then
    echo "⚠️  .env 파일이 없습니다. .env.example을 복사하여 생성해주세요."
    exit 1
fi

# 데이터베이스 연결 확인
echo "📡 데이터베이스 연결 확인 중..."
python -c "from app.db import engine; engine.connect(); print('✅ PostgreSQL 연결 성공')" || echo "❌ PostgreSQL 연결 실패"

# 서버 실행
echo "🌐 FastAPI 서버 시작..."
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

