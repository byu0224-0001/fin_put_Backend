"""임베딩 실패 로그 분석"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import json
import re

project_root = Path(__file__).parent.parent
os.chdir(project_root)
sys.path.insert(0, str(project_root))

load_dotenv()

user = quote_plus(os.getenv('POSTGRES_USER', 'postgres'))
pwd = quote_plus(os.getenv('POSTGRES_PASSWORD', 'postgres'))
host = os.getenv('POSTGRES_HOST', 'localhost')
port = os.getenv('POSTGRES_PORT', '5432')
db_name = os.getenv('POSTGRES_DB', 'newsdb')
url = f'postgresql://{user}:{pwd}@{host}:{port}/{db_name}'

engine = create_engine(url)

print("=" * 80)
print("임베딩 실패 로그 분석")
print("=" * 80)

# 로그 파일에서 임베딩 실패 패턴 찾기
log_dir = project_root / "logs"
log_files = sorted(log_dir.glob("sector_reclassification_optimized_*.log"), 
                   key=lambda x: x.stat().st_mtime, reverse=True)

if not log_files:
    print("로그 파일 없음")
    sys.exit(1)

latest_log = log_files[0]
print(f"\n최신 로그 파일: {latest_log.name}")

# 실패 원인 분류
failure_categories = {
    'API_ERROR': [],
    'RATE_LIMIT': [],
    'NETWORK_ERROR': [],
    'TIMEOUT': [],
    'AUTH_ERROR': [],
    'UNKNOWN': []
}

with open(latest_log, 'r', encoding='utf-8', errors='ignore') as f:
    lines = f.readlines()
    
    for i, line in enumerate(lines):
        line_lower = line.lower()
        
        # API 오류
        if 'api' in line_lower and ('error' in line_lower or 'failed' in line_lower or '실패' in line_lower):
            failure_categories['API_ERROR'].append((i+1, line.strip()[:200]))
        
        # Rate Limit
        if 'rate limit' in line_lower or 'rate_limit' in line_lower or '429' in line:
            failure_categories['RATE_LIMIT'].append((i+1, line.strip()[:200]))
        
        # Network 오류
        if 'network' in line_lower or 'connection' in line_lower or 'timeout' in line_lower:
            if 'timeout' in line_lower:
                failure_categories['TIMEOUT'].append((i+1, line.strip()[:200]))
            else:
                failure_categories['NETWORK_ERROR'].append((i+1, line.strip()[:200]))
        
        # 인증 오류
        if 'auth' in line_lower or '401' in line or '403' in line or 'unauthorized' in line_lower:
            failure_categories['AUTH_ERROR'].append((i+1, line.strip()[:200]))
        
        # 임베딩 실패
        if '임베딩' in line and ('실패' in line or 'failed' in line_lower or 'error' in line_lower):
            if not any(cat in line_lower for cat in ['api', 'rate', 'network', 'timeout', 'auth']):
                failure_categories['UNKNOWN'].append((i+1, line.strip()[:200]))

print("\n실패 원인 분류:")
print("-" * 80)

total_failures = 0
for category, failures in failure_categories.items():
    count = len(failures)
    total_failures += count
    print(f"{category}: {count}개")
    
    if count > 0 and count <= 5:
        print("  샘플:")
        for line_num, error_line in failures[:5]:
            print(f"    Line {line_num}: {error_line}")
    elif count > 5:
        print(f"  샘플 (상위 5개):")
        for line_num, error_line in failures[:5]:
            print(f"    Line {line_num}: {error_line}")

print(f"\n총 실패 건수: {total_failures}개")

# DB에서 임베딩 없는 기업 확인
with engine.connect() as conn:
    result = conn.execute(text("""
        SELECT COUNT(*)
        FROM investor_sector i
        LEFT JOIN company_embeddings e ON i.ticker = e.ticker
        WHERE i.is_primary = true
          AND e.ticker IS NULL
    """))
    no_embedding_count = result.fetchone()[0]
    
    print(f"\nDB 상태:")
    print(f"  임베딩 없는 기업: {no_embedding_count}개")
    
    if total_failures > 0:
        print(f"\n권장 조치:")
        if failure_categories['RATE_LIMIT']:
            print("  1. Rate Limit: 재시도 로직 개선 (Exponential Backoff)")
        if failure_categories['TIMEOUT']:
            print("  2. Timeout: 타임아웃 시간 증가 또는 재시도")
        if failure_categories['NETWORK_ERROR']:
            print("  3. Network Error: 재시도 로직 강화")
        if failure_categories['API_ERROR']:
            print("  4. API Error: API 응답 로그 확인 필요")
        if failure_categories['AUTH_ERROR']:
            print("  5. Auth Error: API 키 확인 필요")

print("=" * 80)


