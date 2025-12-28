# -*- coding: utf-8 -*-
"""
미수집 기업 200개 수집 스크립트 (최신 - 최적화 버전)
- 아직 수집되지 않은 기업 중 이름순으로 200개 선별
- 우선주/스팩 종목 자동 제외
- 병렬 처리 (max_workers=3)
- DART 수집만 병렬 처리, 관계 추출/섹터 분류는 전체 완료 후 1회씩 실행
"""
import sys
sys.path.insert(0, '.')
sys.stdout.reconfigure(encoding='utf-8')
import os
import subprocess
import time
from datetime import datetime
import traceback
from dotenv import load_dotenv
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
import signal
import psutil
import re

# .env 파일 로드
project_root = Path(__file__).parent.parent
env_path = project_root / '.env'
load_dotenv(dotenv_path=env_path, override=True)

# TensorFlow 로깅 억제
env_for_subprocess = os.environ.copy()
env_for_subprocess['TF_CPP_MIN_LOG_LEVEL'] = '3'
env_for_subprocess['TF_CPP_MIN_VLOG_LEVEL'] = '0'
env_for_subprocess['TF_FORCE_GPU_ALLOW_GROWTH'] = 'true'

from app.db import SessionLocal
from app.models import Stock, CompanyDetail
from sqlalchemy import text
from app.utils.preferred_stock import is_preferred_stock_smart

# 로그 파일 (새로운 로그)
LOG_FILE = 'data/collect_200_companies_new_log.txt'
KONEX_SKIP_LOG = 'data/konex_skipped_companies.txt'

# 로그 쓰기용 락
log_lock = threading.Lock()

# 종료 플래그
shutdown_flag = threading.Event()

# Signal 핸들러
def signal_handler(signum, frame):
    log("\n⚠️  종료 신호 수신. 현재 작업 완료 후 종료...")
    shutdown_flag.set()

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def log(msg):
    """로그 출력 (스레드 안전)"""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_msg = f"[{timestamp}] {msg}"
    with log_lock:
        print(log_msg, flush=True)
        try:
            with open(LOG_FILE, 'a', encoding='utf-8') as f:
                f.write(log_msg + '\n')
        except:
            pass

def get_stats():
    """현재 통계 조회"""
    s = SessionLocal()
    try:
        companies = s.execute(text("SELECT COUNT(DISTINCT ticker) FROM company_details")).scalar()
        return companies
    finally:
        s.close()

def is_konex_company(db_session, ticker: str) -> bool:
    """코넥스 상장 기업인지 확인"""
    result = db_session.execute(
        text("SELECT market FROM stocks WHERE ticker = :ticker"),
        {'ticker': ticker}
    ).first()
    return result and result[0] == 'KONEX'

def is_spac_or_preferred(stock_name: str) -> bool:
    """우선주/스팩 종목인지 확인"""
    # 우선주 패턴
    preferred_pattern = re.compile(r'우|우B|우\(|전환\)', re.IGNORECASE)
    
    # 스팩 패턴
    spac_pattern = re.compile(r'스팩', re.IGNORECASE)
    
    # 스마트 체크
    db_session = SessionLocal()
    try:
        is_pref, _ = is_preferred_stock_smart(stock_name, db_session)
        if is_pref:
            return True
    except:
        pass
    finally:
        db_session.close()
    
    # 이름 기반 체크
    if preferred_pattern.search(stock_name) or spac_pattern.search(stock_name):
        return True
    
    return False

def get_uncollected_companies(limit=200):
    """미수집 기업 목록 조회 (이름순 정렬, 우선주/스팩 제외)"""
    db = SessionLocal()
    try:
        # 이미 수집된 기업 티커 목록
        collected_tickers = set(
            row[0] for row in db.execute(
                text("SELECT DISTINCT ticker FROM company_details")
            ).fetchall()
        )
        
        # 모든 한국 증시 상장 기업 (KOSPI/KOSDAQ만)
        all_stocks = db.query(Stock).filter(
            Stock.market.in_(['KOSPI', 'KOSDAQ'])
        ).order_by(Stock.stock_name).all()
        
        # 미수집 + 우선주/스팩 제외 + 맵스리얼티 제외
        uncollected = []
        for stock in all_stocks:
            if stock.ticker in collected_tickers:
                continue
            if stock.ticker == '094800':  # 맵스리얼티 제외
                continue
            if is_spac_or_preferred(stock.stock_name):
                continue
            uncollected.append(stock)
        
        log(f"✅ 전체 미수집 기업 (우선주/스팩 제외): {len(uncollected)}개 발견")
        log(f"📋 상위 {min(limit, len(uncollected))}개 기업 선별")
        
        # 코넥스 기업 제외
        konex_count = 0
        filtered_uncollected = []
        for stock in uncollected[:limit]:
            if is_konex_company(db, stock.ticker):
                konex_count += 1
                try:
                    with open(KONEX_SKIP_LOG, 'a', encoding='utf-8') as f:
                        f.write(f"{stock.ticker}\t{stock.stock_name}\n")
                except:
                    pass
                continue
            filtered_uncollected.append(stock)
        
        if konex_count > 0:
            log(f"⚠️  코넥스 기업 제외: {konex_count}개")
        
        log(f"✅ 실제 수집 대상: {len(filtered_uncollected)}개")
        
        return filtered_uncollected
    
    finally:
        db.close()

def process_single_company(stock, index, total, env_for_subprocess):
    """단일 기업 처리 함수 (병렬 처리용) - DART 수집만"""
    ticker = stock.ticker
    name = stock.stock_name
    result = {
        'ticker': ticker,
        'name': name,
        'status': 'FAILED',
        'error': None,
        'index': index
    }
    
    db_session = SessionLocal()
    
    try:
        log(f"🔄 [{index}/{total}] {ticker}: {name} - DART 수집 시작")
        
        # 1. DART 수집
        dart_script = project_root / "scripts" / "04_fetch_dart.py"
        dart_result = subprocess.run(
            ["python", str(dart_script), "--ticker", ticker],
            cwd=str(project_root),
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=600,  # 10분 (정상 수집 시 충분)
            env=env_for_subprocess
        )
        
        time.sleep(0.5)  # DB 커밋 확인용 최소 대기만
        db_session.expire_all()
        
        db_data_exists = db_session.query(CompanyDetail).filter(
            CompanyDetail.ticker == ticker
        ).first() is not None
        
        if not db_data_exists:
            error_msg = (dart_result.stderr[:500] if dart_result.stderr else dart_result.stdout[:500]) or 'Unknown error'
            # TensorFlow 경고 필터링
            error_lines = error_msg.split('\n')
            filtered_errors = [l for l in error_lines if 'tensorflow' not in l.lower() and 'tf_' not in l.lower() and l.strip()]
            error_msg = '\n'.join(filtered_errors) if filtered_errors else 'Unknown error'
            
            result['status'] = 'FAILED'
            result['error'] = f"DART 수집 실패: {error_msg[:200]}"
            log(f"❌ [{index}/{total}] {ticker}: {name} - DB에 데이터 없음")
            return result
        
        log(f"✅ [{index}/{total}] {ticker}: {name} - DART 수집 완료")
        
        result['status'] = 'SUCCESS'
        return result
    
    except subprocess.TimeoutExpired:
        result['status'] = 'FAILED'
        result['error'] = "Timeout"
        log(f"⏱️  [{index}/{total}] {ticker}: {name} - 타임아웃")
        return result
    except Exception as e:
        result['status'] = 'FAILED'
        result['error'] = str(e)[:200]
        log(f"❌ [{index}/{total}] {ticker}: {name} - 오류: {str(e)[:100]}")
        return result
    finally:
        db_session.close()

def main():
    """메인 함수"""
    try:
        # 로그 파일 초기화
        with open(LOG_FILE, 'w', encoding='utf-8') as f:
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ======================================================================\n")
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 미수집 기업 200개 수집 시작\n")
            f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] ======================================================================\n")
        
        log("=" * 70)
        log("미수집 기업 200개 수집 시작")
        log("=" * 70)
        
        initial_count = get_stats()
        log(f"초기 상태: {initial_count}개 기업")
        
        # 미수집 기업 목록 조회
        uncollected_companies = get_uncollected_companies(limit=200)
        
        if not uncollected_companies:
            log("❌ 수집할 기업이 없습니다!")
            return
        
        log(f"\n{'='*70}")
        log(f"총 {len(uncollected_companies)}개 기업 수집 시작")
        log(f"{'='*70}")
        
        success = 0
        failed = 0
        failed_list = []
        completed_count = 0
        
        start_time = datetime.now()
        
        log(f"🚀 병렬 처리 시작 (max_workers=5, 타임아웃: 10분)\n")
        
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_stock = {
                executor.submit(process_single_company, stock, i, len(uncollected_companies), env_for_subprocess): (stock, i)
                for i, stock in enumerate(uncollected_companies, 1)
            }
            
            log(f"✅ 총 {len(future_to_stock)}개 작업 제출 완료\n")
            
            for future in as_completed(future_to_stock):
                if shutdown_flag.is_set():
                    log("\n⚠️  종료 신호 수신...")
                    break
                
                stock, index = future_to_stock[future]
                completed_count += 1
                
                try:
                    result = future.result(timeout=1)
                    
                    if result['status'] == 'SUCCESS':
                        success += 1
                        log(f"✅ [{completed_count}/{len(uncollected_companies)}] {result['ticker']}: {result['name']} - 성공")
                    else:
                        failed += 1
                        failed_list.append({
                            'ticker': result['ticker'],
                            'name': result['name'],
                            'error': result.get('error', 'Unknown error')
                        })
                        log(f"❌ [{completed_count}/{len(uncollected_companies)}] {result['ticker']}: {result['name']} - 실패")
                    
                    if completed_count % 50 == 0:
                        current_count = get_stats()
                        elapsed = datetime.now() - start_time
                        log("=" * 70)
                        log(f"📍 [{completed_count}/{len(uncollected_companies)}] 중간 체크")
                        log(f"성공: {success}, 실패: {failed}")
                        log(f"현재 총 기업: {current_count}개 (+{current_count - initial_count})")
                        log(f"소요 시간: {elapsed}")
                        log("=" * 70 + "\n")
                
                except Exception as e:
                    failed += 1
                    failed_list.append({
                        'ticker': stock.ticker,
                        'name': stock.stock_name,
                        'error': str(e)[:200]
                    })
                    log(f"❌ [{completed_count}/{len(uncollected_companies)}] {stock.ticker}: {stock.stock_name} - 예외: {str(e)[:100]}")
        
        # 최종 결과
        elapsed = datetime.now() - start_time
        final_count = get_stats()
        
        log("\n" + "=" * 70)
        log("DART 수집 완료!")
        log("=" * 70)
        log(f"성공: {success}개")
        log(f"실패: {failed}개")
        log(f"총 기업: {final_count}개 (+{final_count - initial_count})")
        log(f"소요 시간: {elapsed}")
        
        if failed_list:
            log(f"\n❌ 실패 목록 ({len(failed_list)}개):")
            for item in failed_list[:20]:
                log(f"  - {item['ticker']}: {item['name']}")
            if len(failed_list) > 20:
                log(f"  ... 외 {len(failed_list) - 20}개")
        
        # 섹터 분류 (전체 DB 대상, 1회만) - 관계 추출보다 먼저 실행
        log("\n" + "=" * 70)
        log("섹터 분류 시작 (전체 DB 대상, 1회)")
        log("=" * 70)
        relation_script = project_root / "scripts" / "05_extract_relations.py"
        relation_log_file = project_root / "data" / "relation_extraction_log.txt"
        
        # 관계 추출 전 Edge 수 확인
        db_check = SessionLocal()
        try:
            edges_before = db_check.execute(text("SELECT COUNT(*) FROM edges")).scalar()
            value_chain_before = db_check.execute(text("SELECT COUNT(*) FROM edges WHERE relation_type = 'VALUE_CHAIN_RELATED'")).scalar()
            log(f"관계 추출 전 Edge 수: {edges_before:,}개 (VALUE_CHAIN_RELATED: {value_chain_before:,}개)")
        finally:
            db_check.close()
        
        try:
            # 로그 파일에 기록하면서 실시간 출력도 유지
            with open(relation_log_file, 'w', encoding='utf-8') as log_file:
                relation_result = subprocess.run(
                    ["python", str(relation_script)],
                    cwd=str(project_root),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding='utf-8',
                    errors='replace',
                    timeout=1800,  # 30분
                    env=env_for_subprocess
                )
                
                # 출력을 로그 파일과 화면에 동시에 기록
                output = relation_result.stdout
                log_file.write(output)
                print(output, flush=True)  # 실시간 출력
                
                # 결과 파싱하여 요약 로그에 기록
                import re
                if "관계 추출 완료" in output:
                    # Edge 생성 수 추출
                    edge_match = re.search(r'생성된 Edge:\s*(\d+)개', output)
                    supplies_match = re.search(r'SUPPLIES_TO.*?(\d+)개', output)
                    sells_match = re.search(r'SELLS_TO.*?(\d+)개', output)
                    potential_match = re.search(r'POTENTIAL_SUPPLIES_TO.*?(\d+)개', output)
                    value_chain_match = re.search(r'VALUE_CHAIN_RELATED.*?(\d+)개', output)
                    companies_match = re.search(r'처리 기업 수:\s*(\d+)개', output)
                    
                    if edge_match:
                        log(f"✅ 관계 추출 완료: Edge {edge_match.group(1)}개 생성")
                    if companies_match:
                        log(f"✅ 처리 기업 수: {companies_match.group(1)}개")
                    if supplies_match:
                        log(f"  - SUPPLIES_TO: {supplies_match.group(1)}개")
                    if sells_match:
                        log(f"  - SELLS_TO: {sells_match.group(1)}개")
                    if potential_match:
                        log(f"  - POTENTIAL_SUPPLIES_TO: {potential_match.group(1)}개")
                    if value_chain_match:
                        log(f"  - VALUE_CHAIN_RELATED (밸류체인 기반): {value_chain_match.group(1)}개 ✅")
                    else:
                        log(f"  - VALUE_CHAIN_RELATED: 0개 (밸류체인 Edge 생성 안 됨)")
            
            # 관계 추출 후 Edge 수 확인
            db_check = SessionLocal()
            try:
                edges_after = db_check.execute(text("SELECT COUNT(*) FROM edges")).scalar()
                value_chain_after = db_check.execute(text("SELECT COUNT(*) FROM edges WHERE relation_type = 'VALUE_CHAIN_RELATED'")).scalar()
                edges_created = edges_after - edges_before
                value_chain_created = value_chain_after - value_chain_before
                
                log("\n" + "=" * 70)
                log("관계 추출 결과 확인")
                log("=" * 70)
                log(f"전체 Edge 수: {edges_before:,}개 → {edges_after:,}개 (+{edges_created:,}개)")
                log(f"VALUE_CHAIN_RELATED Edge: {value_chain_before:,}개 → {value_chain_after:,}개 (+{value_chain_created:,}개)")
                
                if value_chain_created > 0:
                    log(f"✅ 밸류체인 분석 완료: {value_chain_created:,}개 VALUE_CHAIN_RELATED Edge 생성됨")
                else:
                    log(f"⚠️  밸류체인 분석: VALUE_CHAIN_RELATED Edge 생성 안 됨 (섹터 분류 후 재실행 필요할 수 있음)")
                log("=" * 70)
            finally:
                db_check.close()
            
            if relation_result.returncode == 0:
                log("✅ 관계 추출 완료")
            else:
                log(f"⚠️  관계 추출 종료 코드: {relation_result.returncode}")
        except subprocess.TimeoutExpired:
            log("⏱️  관계 추출 타임아웃 (30분 초과)")
        except Exception as e:
            log(f"❌ 관계 추출 오류: {str(e)[:200]}")
        
        # 섹터 분류 (전체 DB 대상, 1회만)
        log("\n" + "=" * 70)
        log("섹터 분류 시작 (전체 DB 대상, 1회)")
        log("=" * 70)
        sector_script = project_root / "scripts" / "45_auto_classify_sectors.py"
        sector_log_file = project_root / "data" / "sector_classification_log.txt"
        
        # 섹터 분류 전 상태 확인
        db_check = SessionLocal()
        try:
            # 테이블 존재 여부 확인
            table_exists = db_check.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    AND table_name = 'investor_sector'
                )
            """)).scalar()
            
            if not table_exists:
                log("⚠️  investor_sector 테이블이 없습니다.")
                log("   → 섹터 분류 스크립트가 자동으로 테이블을 생성합니다.")
                log("   → 테이블 생성 후 섹터 분류를 진행합니다.")
                sectors_before = 0
                value_chain_before = 0
            else:
                sectors_before = db_check.execute(text("SELECT COUNT(*) FROM investor_sector")).scalar()
                value_chain_before = db_check.execute(text("SELECT COUNT(*) FROM investor_sector WHERE value_chain IS NOT NULL")).scalar()
                log(f"섹터 분류 전: {sectors_before:,}개 기업 (value_chain 분류: {value_chain_before:,}개)")
        except Exception as e:
            log(f"⚠️  섹터 분류 전 상태 확인 실패: {str(e)[:200]}")
            log("   → 섹터 분류를 계속 진행합니다 (테이블 자동 생성 가능)")
            sectors_before = 0
            value_chain_before = 0
        finally:
            db_check.close()
        
        try:
            # 로그 파일에 기록하면서 실시간 출력도 유지
            with open(sector_log_file, 'w', encoding='utf-8') as log_file:
                sector_result = subprocess.run(
                    ["python", str(sector_script)],
                    cwd=str(project_root),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding='utf-8',
                    errors='replace',
                    timeout=1800,  # 30분
                    env=env_for_subprocess
                )
                
                output = sector_result.stdout
                log_file.write(output)
                print(output, flush=True)
                
                # 결과 파싱
                import re
                if "섹터 분류 완료" in output or "성공" in output:
                    success_match = re.search(r'성공\s*:\s*(\d+)개', output)
                    skip_match = re.search(r'스킵\s*:\s*(\d+)개', output)
                    fail_match = re.search(r'실패\s*:\s*(\d+)개', output)
                    
                    if success_match:
                        log(f"✅ 섹터 분류 성공: {success_match.group(1)}개")
                    if skip_match:
                        log(f"  - 스킵: {skip_match.group(1)}개")
                    if fail_match:
                        log(f"  - 실패: {fail_match.group(1)}개")
            
            # 섹터 분류 후 상태 확인
            db_check = SessionLocal()
            try:
                # 테이블 존재 여부 재확인
                table_exists = db_check.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public'
                        AND table_name = 'investor_sector'
                    )
                """)).scalar()
                
                if not table_exists:
                    log("⚠️  섹터 분류 후에도 investor_sector 테이블이 없습니다.")
                    log("   → 섹터 분류 스크립트 실행 중 오류가 발생했을 수 있습니다.")
                    sectors_after = 0
                    value_chain_after = 0
                    sectors_created = 0
                    value_chain_created = 0
                else:
                    sectors_after = db_check.execute(text("SELECT COUNT(*) FROM investor_sector")).scalar()
                    value_chain_after = db_check.execute(text("SELECT COUNT(*) FROM investor_sector WHERE value_chain IS NOT NULL")).scalar()
                    sectors_created = sectors_after - sectors_before
                    value_chain_created = value_chain_after - value_chain_before
                    
                    log("\n" + "=" * 70)
                    log("섹터 분류 결과 확인")
                    log("=" * 70)
                    log(f"전체 섹터 분류: {sectors_before:,}개 → {sectors_after:,}개 (+{sectors_created:,}개)")
                    log(f"Value Chain 위치 분류: {value_chain_before:,}개 → {value_chain_after:,}개 (+{value_chain_created:,}개)")
                    
                    if value_chain_created > 0:
                        log(f"✅ Value Chain 위치 분류 완료: {value_chain_created:,}개 기업")
                    elif sectors_created > 0:
                        log(f"⚠️  섹터는 분류되었으나 Value Chain 위치는 분류되지 않았습니다.")
                        log(f"   → 45_auto_classify_sectors.py에서 value_chain 분류 로직 확인 필요")
            finally:
                db_check.close()
            
            if sector_result.returncode == 0:
                log("✅ 섹터 분류 완료")
            else:
                log(f"⚠️  섹터 분류 종료 코드: {sector_result.returncode}")
        except subprocess.TimeoutExpired:
            log("⏱️  섹터 분류 타임아웃 (30분 초과)")
        except Exception as e:
            log(f"❌ 섹터 분류 오류: {str(e)[:200]}")
        
        # 관계 추출 (전체 DB 대상, 1회만) - 섹터 분류 후 실행
        log("\n" + "=" * 70)
        log("관계 추출 시작 (전체 DB 대상, 1회)")
        log("=" * 70)
        relation_script = project_root / "scripts" / "05_extract_relations.py"
        relation_log_file = project_root / "data" / "relation_extraction_log.txt"
        
        # 관계 추출 전 Edge 수 확인
        db_check = SessionLocal()
        try:
            edges_before = db_check.execute(text("SELECT COUNT(*) FROM edges")).scalar()
            value_chain_before = db_check.execute(text("SELECT COUNT(*) FROM edges WHERE relation_type = 'VALUE_CHAIN_RELATED'")).scalar()
            
            # 섹터 분류 완료 여부 확인
            sector_table_exists = db_check.execute(text("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'public'
                    AND table_name = 'investor_sector'
                )
            """)).scalar()
            
            if sector_table_exists:
                sectors_count = db_check.execute(text("SELECT COUNT(*) FROM investor_sector")).scalar()
                value_chain_count = db_check.execute(text("SELECT COUNT(*) FROM investor_sector WHERE value_chain IS NOT NULL")).scalar()
                log(f"관계 추출 전 Edge 수: {edges_before:,}개 (VALUE_CHAIN_RELATED: {value_chain_before:,}개)")
                log(f"섹터 분류 상태: {sectors_count:,}개 기업 (value_chain 분류: {value_chain_count:,}개)")
                
                if value_chain_count == 0:
                    log("⚠️  Value Chain 위치 분류가 없습니다. VALUE_CHAIN_RELATED Edge는 생성되지 않을 수 있습니다.")
            else:
                log(f"관계 추출 전 Edge 수: {edges_before:,}개 (VALUE_CHAIN_RELATED: {value_chain_before:,}개)")
                log("⚠️  investor_sector 테이블이 없습니다. VALUE_CHAIN_RELATED Edge는 생성되지 않습니다.")
        finally:
            db_check.close()
        
        try:
            # 로그 파일에 기록하면서 실시간 출력도 유지
            with open(relation_log_file, 'w', encoding='utf-8') as log_file:
                relation_result = subprocess.run(
                    ["python", str(relation_script)],
                    cwd=str(project_root),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding='utf-8',
                    errors='replace',
                    timeout=1800,  # 30분
                    env=env_for_subprocess
                )
                
                # 출력을 로그 파일과 화면에 동시에 기록
                output = relation_result.stdout
                log_file.write(output)
                print(output, flush=True)  # 실시간 출력
                
                # 결과 파싱하여 요약 로그에 기록
                import re
                if "관계 추출 완료" in output:
                    # Edge 생성 수 추출
                    edge_match = re.search(r'생성된 Edge:\s*(\d+)개', output)
                    supplies_match = re.search(r'SUPPLIES_TO.*?(\d+)개', output)
                    sells_match = re.search(r'SELLS_TO.*?(\d+)개', output)
                    potential_match = re.search(r'POTENTIAL_SUPPLIES_TO.*?(\d+)개', output)
                    value_chain_match = re.search(r'VALUE_CHAIN_RELATED.*?(\d+)개', output)
                    companies_match = re.search(r'처리 기업 수:\s*(\d+)개', output)
                    
                    if edge_match:
                        log(f"✅ 관계 추출 완료: Edge {edge_match.group(1)}개 생성")
                    if companies_match:
                        log(f"✅ 처리 기업 수: {companies_match.group(1)}개")
                    if supplies_match:
                        log(f"  - SUPPLIES_TO: {supplies_match.group(1)}개")
                    if sells_match:
                        log(f"  - SELLS_TO: {sells_match.group(1)}개")
                    if potential_match:
                        log(f"  - POTENTIAL_SUPPLIES_TO: {potential_match.group(1)}개")
                    if value_chain_match:
                        log(f"  - VALUE_CHAIN_RELATED (밸류체인 기반): {value_chain_match.group(1)}개 ✅")
                    else:
                        log(f"  - VALUE_CHAIN_RELATED: 0개 (밸류체인 Edge 생성 안 됨)")
            
            # 관계 추출 후 Edge 수 확인
            db_check = SessionLocal()
            try:
                edges_after = db_check.execute(text("SELECT COUNT(*) FROM edges")).scalar()
                value_chain_after = db_check.execute(text("SELECT COUNT(*) FROM edges WHERE relation_type = 'VALUE_CHAIN_RELATED'")).scalar()
                edges_created = edges_after - edges_before
                value_chain_created = value_chain_after - value_chain_before
                
                log("\n" + "=" * 70)
                log("관계 추출 결과 확인")
                log("=" * 70)
                log(f"전체 Edge 수: {edges_before:,}개 → {edges_after:,}개 (+{edges_created:,}개)")
                log(f"VALUE_CHAIN_RELATED Edge: {value_chain_before:,}개 → {value_chain_after:,}개 (+{value_chain_created:,}개)")
                
                if value_chain_created > 0:
                    log(f"✅ 밸류체인 분석 완료: {value_chain_created:,}개 VALUE_CHAIN_RELATED Edge 생성됨")
                else:
                    log(f"⚠️  밸류체인 분석: VALUE_CHAIN_RELATED Edge 생성 안 됨")
                    log(f"   → 섹터 분류가 완료되었는지 확인 필요")
                log("=" * 70)
            finally:
                db_check.close()
            
            if relation_result.returncode == 0:
                log("✅ 관계 추출 완료")
            else:
                log(f"⚠️  관계 추출 종료 코드: {relation_result.returncode}")
        except subprocess.TimeoutExpired:
            log("⏱️  관계 추출 타임아웃 (30분 초과)")
        except Exception as e:
            log(f"❌ 관계 추출 오류: {str(e)[:200]}")
        
        log("\n" + "=" * 70)
        log("전체 작업 완료!")
        log("=" * 70)
        
        log("\n" + "=" * 70)
        log(f"📋 로그 파일: {LOG_FILE}")
        log("=" * 70)
        
    except Exception as e:
        log(f"❌ 오류: {e}")
        traceback.print_exc()
    finally:
        sys.stdout.flush()
        sys.stderr.flush()
        try:
            sys.exit(0)
        except:
            import os
            os._exit(0)

if __name__ == "__main__":
    main()

