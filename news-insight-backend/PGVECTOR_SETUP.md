# pgvector 설치 가이드

## 📋 개요

Solar Embedding 벡터를 저장하기 위해 PostgreSQL의 `pgvector` 확장이 필요합니다.

## 🐳 Docker 사용 시 (권장)

### 방법 1: pgvector 이미지 사용 (가장 간단)

`docker-compose.yml`에서 PostgreSQL 이미지를 pgvector 포함 이미지로 변경:

```yaml
db:
  image: pgvector/pgvector:pg15  # pgvector 확장 포함
  # ... 나머지 설정
```

그 다음 컨테이너 재시작:

```bash
docker-compose down
docker-compose up -d db
```

### 방법 2: 기존 컨테이너에 pgvector 설치

기존 PostgreSQL 컨테이너에 pgvector를 설치하려면:

```bash
# 컨테이너 접속
docker exec -it <container_name> bash

# 컨테이너 내부에서
apt-get update
apt-get install -y postgresql-15-pgvector

# PostgreSQL 재시작
service postgresql restart
```

## 💻 로컬 PostgreSQL 사용 시

### Ubuntu/Debian

```bash
sudo apt-get update
sudo apt-get install -y postgresql-15-pgvector
```

### macOS (Homebrew)

```bash
brew install pgvector
```

### Windows

1. https://github.com/pgvector/pgvector/releases 에서 최신 릴리스 다운로드
2. PostgreSQL 설치 디렉토리에 복사
3. PostgreSQL 재시작

## ✅ 설치 확인

```bash
python scripts/check_pgvector.py
```

또는 PostgreSQL에 직접 접속:

```sql
-- 확장 설치 확인
SELECT * FROM pg_extension WHERE extname = 'vector';

-- 확장 설치 (아직 설치되지 않은 경우)
CREATE EXTENSION IF NOT EXISTS vector;
```

## 🔧 문제 해결

### "extension vector is not available" 오류

1. **Docker 사용 시**: `pgvector/pgvector:pg15` 이미지 사용 확인
2. **로컬 설치 시**: pgvector 패키지 설치 확인
3. **권한 문제**: PostgreSQL superuser 권한 필요

### 설치 후에도 인식되지 않는 경우

1. PostgreSQL 재시작
2. 데이터베이스에 확장 설치:
   ```sql
   CREATE EXTENSION IF NOT EXISTS vector;
   ```

## 📚 참고 자료

- pgvector GitHub: https://github.com/pgvector/pgvector
- Docker Hub: https://hub.docker.com/r/pgvector/pgvector


