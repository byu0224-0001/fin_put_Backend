"""
Step 3: DART 파싱 스크립트

한국 증시 상장 기업 약 2,800개의 사업보고서를 DART API로 가져와서
LLM으로 구조화하여 DB에 저장

실행 방법:
    python scripts/04_fetch_dart.py [--year 2024] [--limit 10] [--ticker 005930]
    
옵션:
    --year: 대상 연도 (기본값: 2024)
    --limit: 처리할 기업 수 제한 (테스트용)
    --ticker: 특정 티커만 처리
    --skip-existing: 이미 처리된 기업 스킵
"""
import sys
import os
from pathlib import Path
from datetime import datetime
import time
import hashlib
import json
import argparse
from typing import Optional, Dict, Any, List

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Windows 환경에서 인코딩 문제 방지
if sys.platform == 'win32':
    import codecs
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

from dotenv import load_dotenv
# 프로젝트 루트의 .env 파일을 명시적으로 로드 (기존 환경 변수 덮어쓰기)
env_path = project_root / '.env'
load_dotenv(dotenv_path=env_path, override=True)

import logging
from sqlalchemy import and_
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models.stock import Stock
from app.models.company_detail import CompanyDetail
from app.models.company_detail_raw import CompanyDetailRaw
from app.models.company_detail_version import CompanyDetailVersion
from app.models.processing_log import ProcessingLog

from app.services.dart_parser import DartParser
from app.services.llm_handler import LLMHandler
from app.services.embedding_filter import select_relevant_chunks
from app.services.memory_manager import memory_manager, cleanup_after_batch, log_memory_usage
from app.services.retry_handler import retry_dart_api
from app.utils.preferred_stock import is_preferred_stock_smart

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# 설정
DART_API_KEY = os.getenv('DART_API_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
TARGET_YEAR = int(os.getenv('TARGET_YEAR', '2024'))
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '50'))
MAX_LLM_CHARS = int(os.getenv('MAX_LLM_CHARS', '50000'))


def create_log_entry(
    db: Session,
    ticker: str,
    step: str,
    status: str,
    duration_ms: Optional[int] = None,
    error_message: Optional[str] = None,
    retry_count: int = 0,
    extra_metadata: Optional[str] = None
):
    """처리 로그 생성"""
    log_id = f"{ticker}_{step}_{datetime.utcnow().isoformat()}"
    log_entry = ProcessingLog(
        id=log_id,
        ticker=ticker,
        step=step,
        status=status,
        duration_ms=duration_ms,
        error_message=error_message,
        retry_count=retry_count,
        extra_metadata=extra_metadata
    )
    db.add(log_entry)
    db.commit()


# Stock 조회 유틸리티 사용
from app.utils.stock_query import SimpleStock, get_stock_by_ticker_safe

def get_stock_by_ticker(db: Session, ticker: str):
    """티커로 Stock 정보 조회 (Raw SQL, DB 컬럼 없어도 작동)"""
    return get_stock_by_ticker_safe(db, ticker)

def get_korean_companies(db: Session, limit: Optional[int] = None, ticker_filter: Optional[str] = None) -> List[Stock]:
    """한국 기업 목록 조회 (컬럼 존재 여부와 무관하게 동작)"""
    from sqlalchemy import text
    
    # Raw SQL로 실제 존재하는 컬럼만 조회
    sql = """
        SELECT ticker, stock_name, market, industry_raw, synonyms, country
        FROM stocks
        WHERE country = 'KR'
    """
    
    params = {}
    if ticker_filter:
        sql += " AND ticker = :ticker"
        params['ticker'] = ticker_filter
    
    sql += " ORDER BY stock_name"
    
    if limit:
        sql += f" LIMIT {limit}"
    
    result = db.execute(text(sql), params)
    rows = result.fetchall()
    
    # SimpleStock 객체로 변환 (DB 컬럼 없어도 작동)
    stocks = []
    for row in rows:
        stock = SimpleStock(
            ticker=row[0],
            stock_name=row[1],
            market=row[2] if len(row) > 2 else None,
            industry_raw=row[3] if len(row) > 3 else None,
            synonyms=row[4] if len(row) > 4 and row[4] else [],
            country=row[5] if len(row) > 5 else 'KR'
        )
        stocks.append(stock)
    
    return stocks


def check_existing_data(db: Session, ticker: str, year: str) -> bool:
    """이미 처리된 데이터가 있는지 확인"""
    existing = db.query(CompanyDetail).filter(
        and_(
            CompanyDetail.ticker == ticker,
            CompanyDetail.source == f"DART_{year}"
        )
    ).first()
    return existing is not None


def save_raw_data(
    db: Session,
    ticker: str,
    year: str,
    raw_html: Optional[str],
    raw_markdown: Optional[str],
    raw_json: Optional[Dict[str, Any]],
    status: str = "PENDING"
) -> CompanyDetailRaw:
    """Raw 데이터 저장"""
    raw_id = f"{ticker}_DART_{year}"
    
    # 기존 데이터 확인
    existing = db.query(CompanyDetailRaw).filter(CompanyDetailRaw.id == raw_id).first()
    
    if existing:
        existing.raw_html = raw_html
        existing.raw_markdown = raw_markdown
        existing.raw_json = raw_json
        existing.processing_status = status
        existing.updated_at = datetime.utcnow()
        if status == "COMPLETED":
            existing.processed_at = datetime.utcnow()
        db.commit()
        return existing
    else:
        raw_data = CompanyDetailRaw(
            id=raw_id,
            ticker=ticker,
            source=f"DART_{year}",
            year=year,
            raw_html=raw_html,
            raw_markdown=raw_markdown,
            raw_json=raw_json,
            processing_status=status
        )
        db.add(raw_data)
        db.commit()
        return raw_data


def save_structured_data(
    db: Session,
    ticker: str,
    year: str,
    structured_data: Dict[str, Any],
    report_meta: Optional[Dict[str, Any]] = None,
    model_version: str = "gpt-5-mini"
) -> CompanyDetail:
    """구조화된 데이터 저장"""
    detail_id = f"{ticker}_DART_{year}"
    
    # 타입 체크: structured_data가 문자열이면 오류
    if not isinstance(structured_data, dict):
        raise ValueError(f"structured_data는 딕셔너리여야 합니다. 현재 타입: {type(structured_data)}. 값: {str(structured_data)[:200]}")
    
    # 금융사 여부 확인 및 데이터 분리
    financial_value_chain = structured_data.get('financial_value_chain')
    supply_chain = structured_data.get('supply_chain', [])
    
    # 금융사인 경우 supply_chain은 빈 배열로 강제
    if financial_value_chain:
        supply_chain = []
        logger.info(f"[{ticker}] 금융사 데이터 - financial_value_chain 저장, supply_chain 비활성화")
    
    # latest_report_date 파싱 (YYYYMMDD -> Date)
    latest_report_date = None
    if report_meta and report_meta.get('rcept_dt'):
        try:
            rcept_dt_str = report_meta['rcept_dt']
            from datetime import datetime as dt
            latest_report_date = dt.strptime(rcept_dt_str, '%Y%m%d').date()
        except (ValueError, TypeError) as e:
            logger.warning(f"[{ticker}] 접수일자 파싱 실패: {report_meta.get('rcept_dt')}, 오류: {e}")
    
    # 우선주 체크 및 notes 준비
    preferred_notes = None
    try:
        stock = get_stock_by_ticker(db, ticker)
        if stock:
            is_pref, parent_ticker = is_preferred_stock_smart(stock.stock_name, db)
            if is_pref and parent_ticker:
                # 본주 이름 찾기
                parent_stock = get_stock_by_ticker(db, parent_ticker)
                parent_name = parent_stock.stock_name if parent_stock else parent_ticker
                preferred_notes = f"본주({parent_ticker}, {parent_name})의 우선주"
                logger.info(f"[{ticker}] 우선주 확인 - notes 추가: {preferred_notes}")
    except Exception as e:
        logger.warning(f"[{ticker}] 우선주 체크 중 오류 (무시): {e}")
    
    # 기존 데이터 확인
    existing = db.query(CompanyDetail).filter(CompanyDetail.id == detail_id).first()
    
    if existing:
        existing.biz_summary = structured_data.get('business_summary')
        existing.products = structured_data.get('major_products', [])
        existing.clients = structured_data.get('major_clients', [])
        existing.supply_chain = supply_chain  # 금융사는 빈 배열
        existing.financial_value_chain = financial_value_chain  # 금융사 전용
        # 하위 호환성: raw_materials도 유지 (supply_chain에서 파생 가능)
        if 'raw_materials' in structured_data:
            existing.raw_materials = structured_data.get('raw_materials', [])
        elif supply_chain and isinstance(supply_chain, list):
            # supply_chain에서 raw_materials 자동 추출
            existing.raw_materials = [
                sc.get('item') if isinstance(sc, dict) else str(sc)
                for sc in supply_chain
                if sc and (isinstance(sc, dict) and sc.get('item') or not isinstance(sc, dict))
            ]
        else:
            existing.raw_materials = []
        existing.cost_structure = structured_data.get('cost_structure')
        existing.keywords = structured_data.get('keywords', [])
        # 매출 비중 데이터 저장
        if 'revenue_by_segment' in structured_data:
            existing.revenue_by_segment = structured_data.get('revenue_by_segment', {})
        if latest_report_date:
            existing.latest_report_date = latest_report_date
        # 우선주 notes 추가
        if preferred_notes and hasattr(existing, 'notes'):
            existing.notes = preferred_notes
        existing.updated_at = datetime.utcnow()
        db.commit()
        return existing
    else:
        # supply_chain에서 raw_materials 자동 추출
        raw_materials = []
        supply_chain = structured_data.get('supply_chain', [])
        if supply_chain:
            # ✅ supply_chain이 리스트인지 확인
            if isinstance(supply_chain, list):
                raw_materials = [
                    sc.get('item') if isinstance(sc, dict) else str(sc)
                    for sc in supply_chain
                    if sc and (isinstance(sc, dict) and sc.get('item') or not isinstance(sc, dict))
                ]
            else:
                logger.warning(f"[{ticker}] supply_chain이 리스트가 아닙니다: {type(supply_chain)}")
                raw_materials = []
        elif structured_data.get('raw_materials'):
            raw_materials = structured_data.get('raw_materials', [])
        
        detail_kwargs = {
            'id': detail_id,
            'ticker': ticker,
            'source': f"DART_{year}",
            'latest_report_date': latest_report_date,
            'biz_summary': structured_data.get('business_summary'),
            'products': structured_data.get('major_products', []),
            'clients': structured_data.get('major_clients', []),
            'supply_chain': supply_chain,  # 금융사는 빈 배열
            'financial_value_chain': financial_value_chain,  # 금융사 전용
            'raw_materials': raw_materials,
            'cost_structure': structured_data.get('cost_structure'),
            'keywords': structured_data.get('keywords', []),
            'revenue_by_segment': structured_data.get('revenue_by_segment', {})
        }
        
        # 우선주 notes 추가 (현재 DB에 notes 컬럼이 없으므로 주석 처리)
        # if preferred_notes and hasattr(CompanyDetail, 'notes'):
        #     detail_kwargs['notes'] = preferred_notes
        
        detail = CompanyDetail(**detail_kwargs)
        db.add(detail)
        db.commit()
        return detail


def save_version_info(
    db: Session,
    ticker: str,
    year: str,
    model_version: str = "gpt-5-mini"
) -> CompanyDetailVersion:
    """버전 정보 저장"""
    # 기존 버전 확인
    existing_versions = db.query(CompanyDetailVersion).filter(
        and_(
            CompanyDetailVersion.ticker == ticker,
            CompanyDetailVersion.year == year
        )
    ).order_by(CompanyDetailVersion.version.desc()).all()
    
    # 기존 버전의 is_current를 N으로 변경
    for ver in existing_versions:
        ver.is_current = "N"
    
    # 새 버전 생성
    next_version = existing_versions[0].version + 1 if existing_versions else 1
    version_id = f"{ticker}_{year}_v{next_version}"
    
    version = CompanyDetailVersion(
        id=version_id,
        ticker=ticker,
        year=year,
        version=next_version,
        model_version=model_version,
        is_current="Y"
    )
    db.add(version)
    db.commit()
    return version


def process_company(
    ticker: str,
    company_name: str,
    year: int,
    dart_parser: DartParser,
    llm_handler: LLMHandler,
    skip_existing: bool = False
) -> Dict[str, Any]:
    """
    단일 기업 처리
    
    Returns:
        {
            'ticker': ticker,
            'status': 'SUCCESS' | 'FAILED' | 'SKIPPED',
            'error': error_message (if failed)
        }
    """
    db = SessionLocal()
    start_time = time.time()
    year_str = str(year)
    
    try:
        # 이미 처리된 데이터 확인
        if skip_existing and check_existing_data(db, ticker, year_str):
            logger.info(f"[{ticker}] {company_name}: 이미 처리된 데이터 존재, 스킵")
            create_log_entry(db, ticker, "CHECK_EXISTING", "SUCCESS", extra_metadata='{"action": "skipped"}')
            return {'ticker': ticker, 'status': 'SKIPPED'}
        
        logger.info(f"[{ticker}] {company_name}: 처리 시작")
        create_log_entry(db, ticker, "START", "SUCCESS")
        
        # 0. 보고서 메타데이터 가져오기 (접수일자 추출용)
        report_info = None
        try:
            report_info = dart_parser.find_business_report(ticker, year)
            if report_info:
                logger.info(f"[{ticker}] 보고서 메타데이터 수집 완료: {report_info.get('rcept_no', 'N/A')}")
        except Exception as e:
            logger.warning(f"[{ticker}] 보고서 메타데이터 수집 실패: {e}")
        
        # 1. DART API로 섹션 추출
        logger.info(f"[{ticker}] DART API 호출 중...")
        fetch_start = time.time()
        
        try:
            combined_text = dart_parser.extract_key_sections(ticker, year)
            fetch_duration = int((time.time() - fetch_start) * 1000)
            
            if not combined_text:
                logger.warning(f"[{ticker}] DART 섹션 추출 실패")
                create_log_entry(
                    db, ticker, "DART_FETCH", "FAILED",
                    duration_ms=fetch_duration,
                    error_message="섹션 추출 실패"
                )
                return {'ticker': ticker, 'status': 'FAILED', 'error': 'DART 섹션 추출 실패'}
            
            logger.info(f"[{ticker}] DART 섹션 추출 성공 ({len(combined_text)}자)")
            create_log_entry(db, ticker, "DART_FETCH", "SUCCESS", duration_ms=fetch_duration)
            
        except Exception as e:
            fetch_duration = int((time.time() - fetch_start) * 1000)
            logger.error(f"[{ticker}] DART API 오류: {e}")
            create_log_entry(
                db, ticker, "DART_FETCH", "FAILED",
                duration_ms=fetch_duration,
                error_message=str(e)
            )
            return {'ticker': ticker, 'status': 'FAILED', 'error': str(e)}
        
        # 2. 임베딩 필터링 (선택적)
        logger.info(f"[{ticker}] 임베딩 필터링 중...")
        filter_start = time.time()
        
        try:
            filtered_text = select_relevant_chunks(combined_text, ticker=ticker)
            effective_text = filtered_text if filtered_text and len(filtered_text) > 200 else combined_text
            
            # 길이 제한 및 정제
            if len(effective_text) > MAX_LLM_CHARS:
                effective_text = effective_text[:MAX_LLM_CHARS]
                logger.warning(f"[{ticker}] 텍스트 길이 제한: {MAX_LLM_CHARS}자로 절단")
            
            # 불필요한 섹션 제거 (간단한 버전)
            # 면책 조항 키워드가 포함된 줄 제거
            disclaimer_keywords = ["예측정보", "주의사항", "면책", "정정보고서를"]
            lines = effective_text.split('\n')
            cleaned_lines = []
            skip_section = False
            for line in lines:
                stripped = line.strip()
                if any(keyword in stripped for keyword in disclaimer_keywords):
                    if stripped.startswith('#'):  # 헤딩인 경우
                        skip_section = True
                        continue
                    elif skip_section:
                        continue
                else:
                    skip_section = False
                cleaned_lines.append(line)
            effective_text = '\n'.join(cleaned_lines)
            
            filter_duration = int((time.time() - filter_start) * 1000)
            logger.info(f"[{ticker}] 필터링 완료 ({len(effective_text)}자)")
            create_log_entry(db, ticker, "EMBEDDING_FILTER", "SUCCESS", duration_ms=filter_duration)
            
        except Exception as e:
            filter_duration = int((time.time() - filter_start) * 1000)
            logger.warning(f"[{ticker}] 임베딩 필터링 실패, 원문 사용: {e}")
            effective_text = combined_text[:MAX_LLM_CHARS]
            create_log_entry(
                db, ticker, "EMBEDDING_FILTER", "FAILED",
                duration_ms=filter_duration,
                error_message=str(e)
            )
        
        # 3. Raw 데이터 저장
        try:
            save_raw_data(
                db, ticker, year_str,
                raw_html=None,  # HTML은 개별 섹션에서 가져오므로 여기서는 저장하지 않음
                raw_markdown=effective_text,
                raw_json=None,
                status="PROCESSING"
            )
            logger.info(f"[{ticker}] Raw 데이터 저장 완료")
        except Exception as e:
            logger.error(f"[{ticker}] Raw 데이터 저장 실패: {e}")
        
        # 4. LLM 요약 및 구조화ㅐ
        logger.info(f"[{ticker}] LLM 처리 중...")
        llm_start = time.time()
        
        try:
            # 긴 텍스트는 먼저 요약
            if len(effective_text) > 50000:
                logger.info(f"[{ticker}] 텍스트가 길어 요약 중... ({len(effective_text)}자)")
                summarized_text = llm_handler.summarize(effective_text)
                effective_text = summarized_text if summarized_text else effective_text
            
            # 구조화된 데이터 추출 (ticker, company_name 전달)
            stock = get_stock_by_ticker(db, ticker)
            company_name = stock.stock_name if stock else None
            structured_data = llm_handler.extract_structured_data(
                effective_text,
                ticker=ticker,
                company_name=company_name
            )
            
            llm_duration = int((time.time() - llm_start) * 1000)
            
            if not structured_data:
                logger.error(f"[{ticker}] LLM 구조화 실패")
                create_log_entry(
                    db, ticker, "LLM_EXTRACT", "FAILED",
                    duration_ms=llm_duration,
                    error_message="구조화된 데이터 추출 실패"
                )
                save_raw_data(db, ticker, year_str, None, effective_text, None, status="FAILED")
                return {'ticker': ticker, 'status': 'FAILED', 'error': 'LLM 구조화 실패'}
            
            logger.info(f"[{ticker}] LLM 구조화 완료")
            create_log_entry(db, ticker, "LLM_EXTRACT", "SUCCESS", duration_ms=llm_duration)
        
        except Exception as e:
            llm_duration = int((time.time() - llm_start) * 1000)
            error_str = str(e)
            
            # OpenAI API quota 오류 감지
            is_quota_error = "QUOTA_ERROR" in error_str or any(
                keyword in error_str.lower() for keyword in [
                    "insufficient_quota", "quota", "rate limit",
                    "exceeded your current quota", "billing"
                ]
            )
            
            if is_quota_error:
                logger.error(f"[{ticker}] ⚠️ OpenAI API Quota 초과로 LLM 구조화 실패")
                create_log_entry(
                    db, ticker, "LLM_EXTRACT", "QUOTA_ERROR",
                    duration_ms=llm_duration,
                    error_message=f"OpenAI API Quota 초과: {error_str[:200]}"
                )
                # Quota 오류 시에도 원본 텍스트는 저장 (부분 성공)
                save_raw_data(db, ticker, year_str, None, effective_text, None, status="QUOTA_ERROR")
                return {
                    'ticker': ticker, 
                    'status': 'QUOTA_ERROR', 
                    'error': f"OpenAI API Quota 초과: {error_str[:200]}"
                }
            
            logger.error(f"[{ticker}] LLM 처리 오류: {e}")
            create_log_entry(
                db, ticker, "LLM_EXTRACT", "FAILED",
                duration_ms=llm_duration,
                error_message=str(e)
            )
            save_raw_data(db, ticker, year_str, None, effective_text, None, status="FAILED")
            return {'ticker': ticker, 'status': 'FAILED', 'error': str(e)}
        
        # 5. 구조화된 데이터 저장
        try:
            # ✅ 추가 타입 검증 (안전장치)
            if not isinstance(structured_data, dict):
                error_msg = f"structured_data가 딕셔너리가 아닙니다. 타입: {type(structured_data)}, 값: {str(structured_data)[:200]}"
                logger.error(f"[{ticker}] {error_msg}")
                raise ValueError(error_msg)
            
            save_structured_data(db, ticker, year_str, structured_data, report_meta=report_info, model_version="gpt-5-mini")
            logger.info(f"[{ticker}] 구조화된 데이터 저장 완료")
            create_log_entry(db, ticker, "DB_SAVE", "SUCCESS")
        except Exception as e:
            logger.error(f"[{ticker}] 구조화된 데이터 저장 실패: {e}")
            create_log_entry(
                db, ticker, "DB_SAVE", "FAILED",
                error_message=str(e)
            ) 
            return {'ticker': ticker, 'status': 'FAILED', 'error': str(e)}
        
        # 6. Raw 데이터 업데이트 (LLM JSON 포함)
        try:
            save_raw_data(
                db, ticker, year_str,
                raw_html=None,
                raw_markdown=effective_text,
                raw_json=structured_data,
                status="COMPLETED"
            )
        except Exception as e:
            logger.warning(f"[{ticker}] Raw 데이터 업데이트 실패: {e}")
        
        # 7. 버전 정보 저장
        try:
            save_version_info(db, ticker, year_str, model_version="gpt-5-mini")
        except Exception as e:
            logger.warning(f"[{ticker}] 버전 정보 저장 실패: {e}")
        
        total_duration = int((time.time() - start_time) * 1000)
        logger.info(f"[{ticker}] {company_name}: 처리 완료 ({total_duration}ms)")
        create_log_entry(db, ticker, "COMPLETE", "SUCCESS", duration_ms=total_duration)
        
        return {'ticker': ticker, 'status': 'SUCCESS'}
        
    except Exception as e:
        total_duration = int((time.time() - start_time) * 1000)
        logger.error(f"[{ticker}] {company_name}: 처리 중 예외 발생: {e}")
        create_log_entry(
            db, ticker, "EXCEPTION", "FAILED",
            duration_ms=total_duration,
            error_message=str(e)
        )
        return {'ticker': ticker, 'status': 'FAILED', 'error': str(e)}
    
    finally:
        db.close()


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='DART 파싱 스크립트')
    parser.add_argument('--year', type=int, default=TARGET_YEAR, help='대상 연도 (기본값: 2024)')
    parser.add_argument('--limit', type=int, default=None, help='처리할 기업 수 제한 (테스트용)')
    parser.add_argument('--ticker', type=str, default=None, help='특정 티커만 처리')
    parser.add_argument('--skip-existing', action='store_true', help='이미 처리된 기업 스킵')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print(f"Step 3: DART 파싱 스크립트 실행")
    print("=" * 60)
    print(f"대상 연도: {args.year}")
    print(f"기업 수 제한: {args.limit if args.limit else '전체'}")
    print(f"특정 티커: {args.ticker if args.ticker else '전체'}")
    print(f"기존 데이터 스킵: {args.skip_existing}")
    print("=" * 60)
    
    # API Key 확인
    if not DART_API_KEY:
        logger.error("DART_API_KEY가 설정되지 않았습니다.")
        print("ERROR: DART_API_KEY가 설정되지 않았습니다.", file=sys.stderr)
        sys.exit(1)
    
    if not OPENAI_API_KEY:
        logger.error("OPENAI_API_KEY가 설정되지 않았습니다.")
        print("ERROR: OPENAI_API_KEY가 설정되지 않았습니다.", file=sys.stderr)
        sys.exit(1)
    
    # API 키 유효성 검증
    if not OPENAI_API_KEY.startswith('sk-'):
        logger.error(f"OPENAI_API_KEY 형식이 올바르지 않습니다. (시작: {OPENAI_API_KEY[:10]}...)")
        print(f"ERROR: OPENAI_API_KEY 형식이 올바르지 않습니다. 'sk-'로 시작해야 합니다.", file=sys.stderr)
        sys.exit(1)
    
    logger.info(f"OPENAI_API_KEY 로드 완료 (길이: {len(OPENAI_API_KEY)}, 시작: {OPENAI_API_KEY[:15]}...)")
    
    # 서비스 초기화
    dart_parser = DartParser(DART_API_KEY)
    llm_handler = LLMHandler(
        analysis_model="gpt-5-mini",
        summary_model="gpt-5-nano",
        api_key=OPENAI_API_KEY
    )
    
    # DB 연결
    db = SessionLocal()
    try:
        # 한국 기업 목록 조회
        companies = get_korean_companies(db, limit=args.limit, ticker_filter=args.ticker)
        total_companies = len(companies)
        
        if total_companies == 0:
            logger.error("처리할 기업이 없습니다.")
            print("ERROR: 처리할 기업이 없습니다.", file=sys.stderr)
            sys.exit(1)  # 명확한 실패 코드 반환
        
        logger.info(f"총 {total_companies}개 기업 처리 시작")
        
        # 배치 처리
        success_count = 0
        failed_count = 0
        skipped_count = 0
        
        for idx, company in enumerate(companies, 1):
            logger.info(f"\n[{idx}/{total_companies}] {company.ticker}: {company.stock_name}")
            
            result = process_company(
                company.ticker,
                company.stock_name,
                args.year,
                dart_parser,
                llm_handler,
                skip_existing=args.skip_existing
            )
            
            if result['status'] == 'SUCCESS':
                success_count += 1
            elif result['status'] == 'SKIPPED':
                skipped_count += 1
            else:
                failed_count += 1
            
            # 배치마다 메모리 정리
            if idx % BATCH_SIZE == 0:
                logger.info(f"배치 완료 ({idx}/{total_companies}), 메모리 정리 중...")
                cleanup_after_batch()
                log_memory_usage(f"배치 {idx // BATCH_SIZE}")
                time.sleep(0.5)  # API Rate Limit 방지 (최소 대기)
        
        # 최종 결과
        print("\n" + "=" * 60)
        print("처리 완료!")
        print(f"  성공: {success_count}개")
        print(f"  실패: {failed_count}개")
        print(f"  스킵: {skipped_count}개")
        print(f"  총계: {total_companies}개")
        print("=" * 60)
        
        # 성공 여부에 따라 종료 코드 반환
        if success_count == 0 and failed_count > 0:
            sys.exit(1)  # 모두 실패한 경우
        elif success_count > 0:
            sys.exit(0)  # 최소 1개 이상 성공한 경우
        else:
            sys.exit(0)  # 스킵만 있는 경우도 성공으로 처리
        
    finally:
        db.close()


if __name__ == "__main__":
    main()

