"""임베딩 교차검증 리포트"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus
import json
import random
from datetime import datetime

project_root = Path(__file__).parent.parent
os.chdir(project_root)
sys.path.insert(0, str(project_root))

load_dotenv()

# UTF-8 인코딩 설정
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'
else:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

from app.services.solar_embedding_model import (
    prepare_company_text_for_solar,
    calculate_text_hash
)
from app.models.company_detail import CompanyDetail
from app.utils.stock_query import get_stock_by_ticker_safe
from app.db import SessionLocal

user = quote_plus(os.getenv('POSTGRES_USER', 'postgres'))
pwd = quote_plus(os.getenv('POSTGRES_PASSWORD', 'postgres'))
host = os.getenv('POSTGRES_HOST', 'localhost')
port = os.getenv('POSTGRES_PORT', '5432')
db_name = os.getenv('POSTGRES_DB', 'newsdb')
url = f'postgresql://{user}:{pwd}@{host}:{port}/{db_name}'

engine = create_engine(url)

print("=" * 80)
print("임베딩 교차검증 리포트")
print("=" * 80)

db = SessionLocal()

try:
    # 임베딩 없는 ticker 50개 랜덤 샘플링
    result = engine.connect().execute(text("""
        SELECT i.ticker
        FROM investor_sector i
        LEFT JOIN company_embeddings e ON i.ticker = e.ticker
        WHERE i.is_primary = true
          AND e.ticker IS NULL
        GROUP BY i.ticker
        ORDER BY RANDOM()
        LIMIT 50
    """))
    
    tickers = [row[0] for row in result.fetchall()]
    
    print(f"\n샘플링된 기업 수: {len(tickers)}개\n")
    
    # 배치로 CompanyDetail 조회 (성능 최적화)
    ticker_list = tuple(tickers)
    company_details_map = {}
    if ticker_list:
        company_details = db.query(CompanyDetail).filter(
            CompanyDetail.ticker.in_(ticker_list)
        ).all()
        company_details_map = {cd.ticker: cd for cd in company_details}
    
    # 배치로 Stock 조회 (성능 최적화)
    stocks_map = {}
    if ticker_list:
        stocks_result = db.execute(text("""
            SELECT ticker, stock_name
            FROM stocks
            WHERE ticker IN :tickers
        """), {"tickers": ticker_list})
        stocks_map = {row[0]: row[1] for row in stocks_result.fetchall()}
    
    # 배치로 임베딩 조회 (성능 최적화)
    embeddings_map = {}
    if ticker_list:
        embeddings_result = engine.connect().execute(text("""
            SELECT ticker, text_hash, created_at, updated_at
            FROM company_embeddings
            WHERE ticker IN :tickers
        """), {"tickers": ticker_list})
        for row in embeddings_result.fetchall():
            embeddings_map[row[0]] = {
                'text_hash': row[1],
                'created_at': row[2],
                'updated_at': row[3]
            }
    
    report_data = []
    
    for idx, ticker in enumerate(tickers, 1):
        try:
            # CompanyDetail 존재 여부 (이미 조회됨)
            company_detail = company_details_map.get(ticker)
            company_detail_exists = company_detail is not None
            
            # 텍스트 준비
            if company_detail:
                company_name = stocks_map.get(ticker)
                company_text = prepare_company_text_for_solar(company_detail, company_name)
                text_length = len(company_text) if company_text else 0
                text_hash = calculate_text_hash(company_text) if company_text else None
            else:
                company_text = None
                text_length = 0
                text_hash = None
            
            # DB에서 임베딩 확인 (이미 조회됨)
            embedding_info = embeddings_map.get(ticker)
            db_embedding_exists = embedding_info is not None
            
            report_data.append({
                'ticker': ticker,
                'company_detail_exists': company_detail_exists,
                'text_length': text_length,
                'text_hash': text_hash[:16] + '...' if text_hash else None,
                'db_embedding_exists': db_embedding_exists,
                'embedding_created_at': embedding_info['created_at'].isoformat() if embedding_info and embedding_info.get('created_at') else None,
                'embedding_updated_at': embedding_info['updated_at'].isoformat() if embedding_info and embedding_info.get('updated_at') else None
            })
            
            print(f"[{idx}] {ticker}:")
            print(f"  CompanyDetail: {'있음' if company_detail_exists else '없음'}")
            print(f"  텍스트 길이: {text_length}자")
            print(f"  text_hash: {text_hash[:16] + '...' if text_hash else 'N/A'}")
            print(f"  DB 임베딩: {'있음' if db_embedding_exists else '없음'}")
            if embedding_info:
                print(f"  생성일: {embedding_info.get('created_at')}")
                print(f"  수정일: {embedding_info.get('updated_at')}")
            print("-" * 80)
            
        except Exception as e:
            print(f"[{idx}] {ticker}: 오류 - {e}")
            report_data.append({
                'ticker': ticker,
                'error': str(e)
            })
    
    # 통계
    company_detail_count = sum(1 for r in report_data if r.get('company_detail_exists'))
    db_embedding_count = sum(1 for r in report_data if r.get('db_embedding_exists'))
    
    print(f"\n통계:")
    print(f"  CompanyDetail 있음: {company_detail_count}/{len(tickers)}개")
    print(f"  DB 임베딩 있음: {db_embedding_count}/{len(tickers)}개")
    print(f"  임베딩 없음: {len(tickers) - db_embedding_count}개")
    
    # 리포트 저장
    report_file = project_root / "reports" / f"embedding_cross_validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    report_file.parent.mkdir(exist_ok=True)
    
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'sample_count': len(tickers),
            'statistics': {
                'company_detail_exists': company_detail_count,
                'db_embedding_exists': db_embedding_count,
                'missing_embeddings': len(tickers) - db_embedding_count
            },
            'details': report_data
        }, f, ensure_ascii=False, indent=2)
    
    print(f"\n리포트 저장: {report_file}")
    print("=" * 80)
    
finally:
    db.close()

