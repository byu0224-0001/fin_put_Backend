.PHONY: help install run dev test clean docker-up docker-down

help:
	@echo "News Insight Backend - Makefile"
	@echo ""
	@echo "사용 가능한 명령어:"
	@echo "  make install     - 의존성 설치"
	@echo "  make run         - 서버 실행"
	@echo "  make dev         - 개발 모드 실행 (reload)"
	@echo "  make celery      - Celery worker 실행"
	@echo "  make test        - 테스트 실행"
	@echo "  make docker-up   - Docker Compose로 모든 서비스 시작"
	@echo "  make docker-down - Docker Compose로 모든 서비스 중지"
	@echo "  make clean       - 캐시 파일 정리"

install:
	pip install -r requirements.txt

run:
	uvicorn app.main:app --host 0.0.0.0 --port 8000

dev:
	uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

celery:
	celery -A app.celery_worker.celery_app worker --loglevel=info

docker-up:
	docker-compose up --build

docker-down:
	docker-compose down

clean:
	find . -type d -name __pycache__ -exec rm -r {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete

