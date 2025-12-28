"""
전체 기업 섹터 재분류 스크립트 (Ensemble 방식) - 완전 최적화 버전

주요 개선사항:
1. ✅ 배치 임베딩 처리 (32-64개씩)
2. ✅ DB 배치 커밋 (50개마다)
3. ✅ GPT 비동기 호출 (concurrent.futures 사용)
4. ✅ 전체 파이프라인 병렬화 (섹터 분류만)
5. ✅ 무한 로딩 방지 (강화된 exit code 및 상태 파일 관리)

⚠️ 주의: 밸류체인 분류 및 Edge 연결은 순차 처리 필수
"""
import sys
import os
if sys.platform == 'win32':
    import codecs
    try:
        if hasattr(sys.stdout, 'encoding') and sys.stdout.encoding != 'utf-8':
            sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    except (AttributeError, TypeError):
        # stdout이 BufferedWriter 등인 경우 encoding 속성이 없을 수 있음
        try:
            sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
        except:
            pass
    os.environ['PYTHONIOENCODING'] = 'utf-8'
    os.environ['PYTHONUNBUFFERED'] = '1'

from pathlib import Path
project_root = Path(__file__).parent.parent
os.chdir(project_root)
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
env_path = project_root / '.env'
if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)
    print(f"✅ .env 파일 로드 완료: {env_path}")
else:
    print(f"⚠️  .env 파일을 찾을 수 없습니다: {env_path}")

if 'OPEN_API_KEY' in os.environ and 'OPENAI_API_KEY' not in os.environ:
    os.environ['OPENAI_API_KEY'] = os.environ['OPEN_API_KEY']
    print(f"⚠️  환경 변수 'OPEN_API_KEY'를 'OPENAI_API_KEY'로 복사했습니다.")
elif 'OPEN_API_KEY' in os.environ and 'OPENAI_API_KEY' in os.environ:
    print(f"⚠️  'OPEN_API_KEY'와 'OPENAI_API_KEY' 둘 다 존재합니다. 'OPENAI_API_KEY'를 사용합니다.")

from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
from app.models.investor_sector import InvestorSector
from app.models.stock import Stock
from app.services.sector_classifier_ensemble_won import classify_sector_ensemble_won
from app.services.gemini_handler import GeminiHandler
from app.config import settings
import logging
import argparse
import json
from datetime import datetime
from sqlalchemy.exc import IntegrityError
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Tuple, Optional
from collections import deque
import threading
import atexit
import signal

# 배치 처리 설정
BATCH_EMBEDDING_SIZE = 32  # GPU 메모리에 따라 조정 (32-64)
BATCH_COMMIT_SIZE = 50  # DB 커밋 배치 크기
MAX_WORKERS = 4  # 병렬 처리 워커 수 (CPU 코어 수에 따라 조정)

# 로그 디렉토리 생성
log_dir = project_root / "logs"
log_dir.mkdir(exist_ok=True)

# 상태 파일 디렉토리 생성
status_dir = project_root / "status"
status_dir.mkdir(exist_ok=True)

# 전역 변수 (종료 신호용)
_shutdown_flag = threading.Event()
_status_updated = False

def write_status(step_name, status, details=None):
    """상태 파일 작성 (Auto Workflow 연동용) - 강화 버전"""
    global _status_updated
    try:
        status_file = status_dir / f"{step_name}_status.json"
        data = {
            'step': step_name,
            'status': status,
            'timestamp': datetime.now().isoformat(),
            'details': details or {}
        }
        
        max_retries = 5
        for attempt in range(max_retries):
            try:
                # 임시 파일에 먼저 작성 후 원자적 이동
                temp_file = status_file.with_suffix('.json.tmp')
                with open(temp_file, 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                    f.flush()
                    os.fsync(f.fileno())
                
                # 원자적 이동
                if temp_file.exists():
                    temp_file.replace(status_file)
                    break
            except (IOError, OSError) as e:
                if attempt < max_retries - 1:
                    import time
                    time.sleep(0.2)
                else:
                    logger.error(f"상태 파일 쓰기 실패 (시도 {max_retries}회): {e}")
                    raise
        
        # 완료 플래그 파일 생성
        if status in ['completed', 'failed']:
            _status_updated = True
            flag_file = status_dir / f"{step_name}_{status}.flag"
            try:
                with open(flag_file, 'w', encoding='utf-8') as f:
                    f.write(datetime.now().isoformat())
                    f.flush()
                    os.fsync(f.fileno())
            except Exception as e:
                logger.warning(f"플래그 파일 생성 실패: {e}")
        
        logger.info(f"✅ 상태 파일 업데이트 완료: {status_file} (status: {status})")
    except Exception as e:
        logger.error(f"❌ 상태 파일 업데이트 실패: {e}", exc_info=True)

# 로그 파일 설정 (전역 logger 설정 전에)
log_file = log_dir / f"sector_reclassification_optimized_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# 스레드 안전한 카운터
class ThreadSafeCounter:
    def __init__(self):
        self._value = 0
        self._lock = threading.Lock()
    
    def increment(self, amount=1):
        with self._lock:
            self._value += amount
            return self._value
    
    @property
    def value(self):
        return self._value

def prepare_company_data_batch(
    db,
    tickers: List[str]
) -> List[Dict[str, Any]]:
    """배치로 기업 데이터 준비 (DB 쿼리 최적화)"""
    companies = []
    
    # Stock 정보 일괄 조회
    stocks = {s.ticker: s for s in db.query(Stock).filter(Stock.ticker.in_(tickers)).all()}
    
    # CompanyDetail 일괄 조회
    company_details = {
        cd.ticker: cd 
        for cd in db.query(CompanyDetail).filter(CompanyDetail.ticker.in_(tickers)).all()
    }
    
    for ticker in tickers:
        stock = stocks.get(ticker)
        company_detail = company_details.get(ticker)
        
        if not stock or not company_detail:
            continue
            
        companies.append({
            'ticker': ticker,
            'stock': stock,
            'company_detail': company_detail
        })
    
    return companies

def process_single_company_sector(
    ticker: str,
    stock: Stock,
    company_detail: CompanyDetail,
    gemini_handler: Optional[GeminiHandler],
    use_gpt: bool,
    idx: int,
    total: int,
    args,
    success_counter: ThreadSafeCounter,
    skip_counter: ThreadSafeCounter,
    fail_counter: ThreadSafeCounter,
    stats: Dict[str, int],
    stats_lock: threading.Lock
) -> Tuple[str, Optional[List[Dict[str, Any]]], Optional[Exception]]:
    """단일 기업 섹터 분류 처리 (스레드 안전)"""
    if _shutdown_flag.is_set():
        return ticker, None, Exception("Shutdown requested")
    
    db = SessionLocal()
    try:
        # 기존 분류 확인
        existing = db.query(InvestorSector).filter(
            InvestorSector.ticker == ticker
        ).first()
        
        if existing and args.skip_existing and not args.overwrite:
            logger.debug(f"[{idx}/{total}] [{ticker}] 이미 분류됨 (스킵)")
            skip_counter.increment()
            return ticker, None, None
        
        # 기존 분류 삭제
        if args.overwrite and existing:
            deleted = db.query(InvestorSector).filter(
                InvestorSector.ticker == ticker
            ).delete()
            if deleted > 0:
                logger.debug(f"[{idx}/{total}] [{ticker}] 기존 분류 {deleted}개 삭제")
            db.commit()
            db.flush()
        
        # Ensemble 섹터 분류
        logger.info(f"[{idx}/{total}] [{ticker}] {stock.stock_name} 섹터 분류 중...")
        
        results = classify_sector_ensemble_won(
            db=db,
            ticker=ticker,
            gemini_handler=gemini_handler if use_gpt else None,
            use_embedding=True,  # ⭐ GPU 사용 복원
            use_reranking=True,  # ⭐ GPU 사용 복원
            max_sectors=3,
            force_reclassify=args.overwrite
        )
        
        if not results:
            logger.warning(f"[{idx}/{total}] [{ticker}] 섹터 분류 실패")
            fail_counter.increment()
            return ticker, None, Exception("분류 결과 없음")
        
        # 통계 업데이트
        with stats_lock:
            for result in results:
                method = result.get('method', 'ENSEMBLE')
                if method == 'RULE_BASED':
                    stats['rule_based'] += 1
                else:
                    stats['ensemble'] += 1
        
        return ticker, results, None
        
    except Exception as e:
        logger.error(f"[{idx}/{total}] [{ticker}] 처리 중 오류: {e}", exc_info=True)
        db.rollback()
        fail_counter.increment()
        return ticker, None, e
    finally:
        db.close()

def save_results_batch(
    db: SessionLocal,
    results_batch: List[Tuple[str, Optional[List[Dict[str, Any]]]]],
    args,
    stats: Dict[str, int],
    stats_lock: threading.Lock
) -> Tuple[int, int]:
    """배치로 결과 저장 (DB 커밋 최적화)"""
    success_count = 0
    fail_count = 0
    
    for ticker, results in results_batch:
        if not results:
            fail_count += 1
            continue
        
        try:
            for i, result in enumerate(results):
                # ⭐ NEW: 저장 전 최종 검증
                if not result.get('sector_l1') and not result.get('major_sector'):
                    logger.warning(f"[{ticker}] 저장 전 NULL 섹터 감지, 강제 Fallback 적용")
                    result['sector_l1'] = 'SEC_UNKNOWN'
                    result['major_sector'] = 'SEC_UNKNOWN'
                    result['fallback_used'] = 'TRUE'  # ⭐ VARCHAR에 문자열 저장
                    result['fallback_type'] = 'UNKNOWN'  # ⭐ 타입 분리
                    result['confidence'] = 'VERY_LOW'
                    result['method'] = 'FALLBACK_UNKNOWN'
                    result['ensemble_score'] = 0.0
                    result['reasoning'] = 'NULL 섹터 감지, UNKNOWN 할당'
                
                method = result.get('method', 'ENSEMBLE')
                sub_sector_str = result.get('sub_sector', '') or ''
                sector_id = f"{ticker}_{result['major_sector']}"
                if sub_sector_str:
                    sector_id += f"_{sub_sector_str}"
                if i > 0:
                    sector_id += f"_{i}"
                
                existing_sector = db.query(InvestorSector).filter(
                    InvestorSector.id == sector_id
                ).first()
                
                if existing_sector:
                    existing_sector.major_sector = result['major_sector']
                    existing_sector.sub_sector = result.get('sub_sector')
                    # ⚠️ value_chain 관련 필드는 섹터 재분류에서 건드리지 않음
                    # (밸류체인 분류는 별도 스크립트로 실행)
                    # existing_sector.value_chain = result.get('value_chain')  # 제거: 섹터 재분류는 밸류체인을 덮어쓰지 않음
                    existing_sector.sector_weight = result.get('weight', 0.5)
                    existing_sector.is_primary = result.get('is_primary', (i == 0))
                    existing_sector.classification_method = method
                    existing_sector.confidence = result.get('confidence', 'MEDIUM')
                    existing_sector.fallback_used = result.get('fallback_used') or 'FALSE'  # ⭐ Fallback 사용 여부 (기본값: 'FALSE')
                    existing_sector.fallback_type = result.get('fallback_type')  # ⭐ Fallback 타입
                    existing_sector.rule_score = result.get('rule_score')
                    existing_sector.embedding_score = result.get('embedding_score')
                    existing_sector.bge_score = result.get('bge_score')
                    existing_sector.gpt_score = result.get('gpt_score')
                    existing_sector.ensemble_score = result.get('ensemble_score')
                    existing_sector.classification_reasoning = result.get('reasoning')
                    # ⭐ 레벨 2: 인과 구조 분석 결과 저장
                    existing_sector.causal_structure = result.get('causal_structure')
                    existing_sector.investment_insights = result.get('investment_insights')
                    # ⭐ Rule-based 메타데이터 (학습 데이터 수집용)
                    existing_sector.rule_version = result.get('rule_version')
                    existing_sector.rule_confidence = result.get('rule_confidence')
                    existing_sector.training_label = result.get('training_label', False)
                    # ⭐ 새로운 계층 구조 필드
                    existing_sector.sector_l1 = result.get('sector_l1') or result.get('major_sector')
                    existing_sector.sector_l2 = result.get('sector_l2') or result.get('sub_sector')
                    existing_sector.sector_l3_tags = result.get('sector_l3_tags') or result.get('causal_structure', {}).get('sector_l3_tags', [])
                    # ⭐ Boosting 로그 저장
                    if result.get('boosting_log'):
                        existing_sector.boosting_log = result.get('boosting_log')
                    # ⚠️ Phase 2: 5단계 밸류체인 필드는 섹터 재분류에서 건드리지 않음
                    # (밸류체인 분류는 classify_value_chain_final.py로 별도 실행)
                else:
                    investor_sector = InvestorSector(
                        id=sector_id,
                        ticker=ticker,
                        major_sector=result['major_sector'],
                        sub_sector=result.get('sub_sector'),
                        # ⚠️ value_chain 관련 필드는 섹터 재분류에서 건드리지 않음
                        # (밸류체인 분류는 별도 스크립트로 실행)
                        # value_chain=result.get('value_chain'),  # 제거: 섹터 재분류는 밸류체인을 설정하지 않음
                        sector_weight=result.get('weight', 0.5),
                        is_primary=result.get('is_primary', (i == 0)),
                        classification_method=method,
                        confidence=result.get('confidence', 'MEDIUM'),
                        fallback_used=result.get('fallback_used') or 'FALSE',  # ⭐ Fallback 사용 여부 (기본값: 'FALSE')
                        fallback_type=result.get('fallback_type'),  # ⭐ Fallback 타입
                        rule_score=result.get('rule_score'),
                        embedding_score=result.get('embedding_score'),
                        bge_score=result.get('bge_score'),
                        gpt_score=result.get('gpt_score'),
                        ensemble_score=result.get('ensemble_score'),
                        classification_reasoning=result.get('reasoning'),
                        # ⭐ 레벨 2: 인과 구조 분석 결과 저장
                        causal_structure=result.get('causal_structure'),
                        investment_insights=result.get('investment_insights'),
                        # ⭐ Rule-based 메타데이터 (학습 데이터 수집용)
                        rule_version=result.get('rule_version'),
                        rule_confidence=result.get('rule_confidence'),
                        training_label=result.get('training_label', False),
                        # ⭐ 새로운 계층 구조 필드
                        # ⚠️ Phase 2: 5단계 밸류체인 필드는 섹터 재분류에서 건드리지 않음
                        # (밸류체인 분류는 classify_value_chain_final.py로 별도 실행)
                        sector_l1=result.get('sector_l1') or result.get('major_sector'),
                        sector_l2=result.get('sector_l2') or result.get('sub_sector'),
                        sector_l3_tags=result.get('sector_l3_tags') or result.get('causal_structure', {}).get('sector_l3_tags', []),
                        # ⭐ Boosting 로그 저장
                        boosting_log=result.get('boosting_log')
                    )
                    db.add(investor_sector)
            
            success_count += 1
        except IntegrityError as ie:
            logger.error(f"[{ticker}] IntegrityError 발생: {ie}")
            db.rollback()
            fail_count += 1
        except Exception as e:
            logger.error(f"[{ticker}] 결과 저장 오류: {e}", exc_info=True)
            db.rollback()
            fail_count += 1
    
    return success_count, fail_count

def signal_handler(signum, frame):
    """시그널 핸들러 (무한 로딩 방지)"""
    global _shutdown_flag
    logger.warning(f"시그널 {signum} 수신. 종료 중...")
    _shutdown_flag.set()
    sys.exit(1)

# 시그널 핸들러 등록
if sys.platform != 'win32':
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

def cleanup_and_exit(exit_code, step_name):
    """정리 및 종료 (무한 로딩 방지) - 강화 버전"""
    global _status_updated
    
    max_cleanup_attempts = 3
    cleanup_success = False
    
    for attempt in range(max_cleanup_attempts):
        try:
            # 모든 출력 버퍼 강제 flush
            try:
                sys.stdout.flush()
                sys.stderr.flush()
            except:
                pass
            
            # 로거 핸들러 flush
            try:
                for handler in logger.handlers:
                    if hasattr(handler, 'flush'):
                        handler.flush()
            except:
                pass
            
            # 상태 파일이 업데이트되지 않았다면 강제 업데이트
            if not _status_updated:
                status_file = status_dir / f"{step_name}_status.json"
                if status_file.exists():
                    try:
                        with open(status_file, 'r', encoding='utf-8') as f:
                            current_status = json.load(f)
                            if current_status.get('status') == 'running':
                                final_status = 'completed' if exit_code == 0 else 'failed'
                                final_details = {
                                    'note': 'cleanup_and_exit에서 강제 업데이트',
                                    'exit_code': exit_code
                                } if exit_code == 0 else {
                                    'error': f'exit code {exit_code}로 종료'
                                }
                                write_status(step_name, final_status, final_details)
                                _status_updated = True
                    except Exception as e:
                        if attempt == max_cleanup_attempts - 1:
                            logger.error(f"상태 파일 확인 실패: {e}")
                else:
                    # 상태 파일이 없으면 생성
                    final_status = 'completed' if exit_code == 0 else 'failed'
                    write_status(step_name, final_status, {'exit_code': exit_code})
                    _status_updated = True
            
            # 완료 플래그 파일 강제 생성
            try:
                flag_status = 'completed' if exit_code == 0 else 'failed'
                flag_file = status_dir / f"{step_name}_{flag_status}.flag"
                with open(flag_file, 'w', encoding='utf-8') as f:
                    f.write(datetime.now().isoformat())
                    f.write(f"\nexit_code={exit_code}\n")
                    f.flush()
                    os.fsync(f.fileno())
            except:
                pass
            
            cleanup_success = True
            break
            
        except Exception as e:
            if attempt == max_cleanup_attempts - 1:
                logger.error(f"정리 중 오류 (최종 시도): {e}", exc_info=True)
            else:
                import time
                time.sleep(0.5)
    
    # 최종 로그
    try:
        logger.info(f"✅ 정리 완료. 종료 (exit code: {exit_code}, cleanup_success: {cleanup_success})")
    except:
        pass
    
    # 강제 종료 보장 (무한 로딩 방지)
    try:
        os._exit(exit_code)
    except:
        sys.exit(exit_code)

def main(step_name=None):
    global _status_updated
    if step_name is None:
        step_name = 'sector_reclassification_optimized'
    
    parser = argparse.ArgumentParser(description='전체 기업 섹터 재분류 (Ensemble) - 최적화 버전')
    parser.add_argument('--limit', type=int, help='처리할 기업 수 제한 (테스트용)')
    parser.add_argument('--overwrite', action='store_true', help='기존 분류 덮어쓰기')
    parser.add_argument('--skip-existing', action='store_true', help='기존 분류 스킵')
    parser.add_argument('--batch-size', type=int, default=BATCH_EMBEDDING_SIZE, 
                       help=f'임베딩 배치 크기 (기본값: {BATCH_EMBEDDING_SIZE})')
    parser.add_argument('--commit-size', type=int, default=BATCH_COMMIT_SIZE,
                       help=f'DB 커밋 배치 크기 (기본값: {BATCH_COMMIT_SIZE})')
    parser.add_argument('--workers', type=int, default=MAX_WORKERS,
                       help=f'병렬 처리 워커 수 (기본값: {MAX_WORKERS})')
    args = parser.parse_args()
    
    # step_name이 전달되지 않았거나, limit이 있으면 step_name 재설정
    if step_name == 'sector_reclassification_optimized' and args.limit:
        step_name = f'sector_reclassification_optimized_test_{args.limit}'
    
    # 시작 상태 기록
    write_status(step_name, 'running', {
        'limit': args.limit,
        'overwrite': args.overwrite,
        'batch_size': args.batch_size,
        'commit_size': args.commit_size,
        'workers': args.workers,
        'start_time': datetime.now().isoformat()
    })
    
    # 종료 시 정리 함수 등록
    atexit.register(lambda: cleanup_and_exit(0, step_name))
    
    db = None
    try:
        # OpenAI API 키 검증
        api_key = settings.OPENAI_API_KEY
        if not api_key:
            try:
                from dotenv import dotenv_values
                env_vars = dotenv_values(env_path)
                api_key = env_vars.get('OPENAI_API_KEY', '')
                if api_key:
                    os.environ['OPENAI_API_KEY'] = api_key
                    logger.info("✅ .env 파일에서 OPENAI_API_KEY 로드 성공")
            except Exception as e:
                logger.warning(f".env 파일 직접 읽기 실패: {e}")
        
        use_gpt = bool(api_key and api_key.startswith('sk-'))
        if use_gpt:
            logger.info(f"✅ OpenAI API 키 확인 완료 (시작: {api_key[:15]}...)")
        else:
            logger.warning("⚠️  OpenAI API 키가 올바르지 않습니다. GPT 검증 단계가 스킵됩니다.")
        
        # 모델 캐시 초기화
        import app.services.sector_classifier_embedding as sce
        sce._embedding_model = None
        sce._sector_reference_embeddings = None
        logger.info("✅ 섹터 분류용 임베딩 모델 캐시 초기화 완료")
        
        db = SessionLocal()
        gemini_handler = GeminiHandler(api_key=api_key) if use_gpt else None
        
        # 처리할 티커 목록
        query = db.query(CompanyDetail.ticker).distinct()
        if args.limit:
            query = query.limit(args.limit)
        
        details = query.all()
        tickers = [t[0] for t in details]
        
        logger.info("=" * 80)
        logger.info(f"🚀 섹터 재분류 시작 (완전 최적화 버전)")
        logger.info(f"📊 총 기업 수: {len(tickers)}")
        logger.info(f"⚙️  배치 크기: {args.batch_size}")
        logger.info(f"⚙️  커밋 크기: {args.commit_size}")
        logger.info(f"⚙️  워커 수: {args.workers}")
        logger.info(f"⚙️  GPT 사용: {'예' if use_gpt else '아니오'}")
        logger.info("=" * 80)
        
        # 카운터 및 통계
        success_counter = ThreadSafeCounter()
        skip_counter = ThreadSafeCounter()
        fail_counter = ThreadSafeCounter()
        stats = {'rule_based': 0, 'ensemble': 0}
        stats_lock = threading.Lock()
        
        # 배치 커밋 큐
        batch_commit_queue = deque()
        
        # 배치 처리 루프
        total_batches = (len(tickers) + args.batch_size - 1) // args.batch_size
        
        for batch_idx in range(total_batches):
            if _shutdown_flag.is_set():
                logger.warning("종료 신호 수신. 배치 처리 중단.")
                break
            
            batch_start = batch_idx * args.batch_size
            batch_end = min(batch_start + args.batch_size, len(tickers))
            batch_tickers = tickers[batch_start:batch_end]
            
            logger.info(f"📦 배치 {batch_idx+1}/{total_batches} 처리: {batch_start+1}-{batch_end}/{len(tickers)}")
            
            # 배치로 기업 데이터 준비
            companies_batch = prepare_company_data_batch(db, batch_tickers)
            
            if not companies_batch:
                logger.warning(f"배치 {batch_idx+1}에 처리할 기업이 없습니다.")
                continue
            
            # 병렬로 섹터 분류 처리
            futures = {}
            with ThreadPoolExecutor(max_workers=args.workers) as executor:
                for idx, company_data in enumerate(companies_batch):
                    if _shutdown_flag.is_set():
                        break
                    
                    ticker = company_data['ticker']
                    stock = company_data['stock']
                    company_detail = company_data['company_detail']
                    global_idx = batch_start + idx + 1
                    
                    future = executor.submit(
                        process_single_company_sector,
                        ticker, stock, company_detail,
                        gemini_handler, use_gpt,
                        global_idx, len(tickers),
                        args,
                        success_counter, skip_counter, fail_counter,
                        stats, stats_lock
                    )
                    futures[future] = ticker
                
                # 결과 수집 (타임아웃 없이 모든 작업 완료 대기)
                batch_results = []
                for future in as_completed(futures):
                    if _shutdown_flag.is_set():
                        break
                    
                    ticker = futures[future]
                    try:
                        ticker_result, results, error = future.result(timeout=300)  # 5분 타임아웃
                        if error is None and results:
                            batch_results.append((ticker_result, results))
                    except Exception as e:
                        logger.error(f"[{ticker}] Future 처리 오류: {e}", exc_info=True)
            
            # 결과를 커밋 큐에 추가
            batch_commit_queue.extend(batch_results)
            
            # 배치 커밋
            while len(batch_commit_queue) >= args.commit_size:
                commit_batch = [
                    batch_commit_queue.popleft() 
                    for _ in range(min(args.commit_size, len(batch_commit_queue)))
                ]
                
                commit_db = SessionLocal()
                try:
                    success, fail = save_results_batch(commit_db, commit_batch, args, stats, stats_lock)
                    commit_db.commit()
                    logger.info(f"✅ 배치 커밋 완료: {len(commit_batch)}개 (성공: {success}, 실패: {fail})")
                except Exception as e:
                    logger.error(f"❌ 배치 커밋 실패: {e}", exc_info=True)
                    commit_db.rollback()
                finally:
                    commit_db.close()
            
            # 진행 상황 로그
            processed = success_counter.value + skip_counter.value + fail_counter.value
            if processed > 0 and processed % 100 == 0:
                logger.info(
                    f"📊 진행 상황: {processed}/{len(tickers)} "
                    f"(성공: {success_counter.value}, 스킵: {skip_counter.value}, 실패: {fail_counter.value})"
                )
        
        # 남은 결과 커밋
        if batch_commit_queue:
            commit_db = SessionLocal()
            try:
                success, fail = save_results_batch(commit_db, list(batch_commit_queue), args, stats, stats_lock)
                commit_db.commit()
                logger.info(f"✅ 최종 커밋 완료: {len(batch_commit_queue)}개 (성공: {success}, 실패: {fail})")
            except Exception as e:
                logger.error(f"❌ 최종 커밋 실패: {e}", exc_info=True)
                commit_db.rollback()
            finally:
                commit_db.close()
        
        # 최종 결과
        logger.info("=" * 80)
        logger.info("✅ 섹터 재분류 완료")
        logger.info(f"📊 총 기업 수: {len(tickers)}")
        logger.info(f"✅ 성공: {success_counter.value}")
        logger.info(f"⏭️  스킵: {skip_counter.value}")
        logger.info(f"❌ 실패: {fail_counter.value}")
        if len(tickers) > 0:
            logger.info(f"📈 성공률: {success_counter.value / len(tickers) * 100:.1f}%")
        logger.info(f"📊 Rule-based: {stats['rule_based']}")
        logger.info(f"📊 Ensemble: {stats['ensemble']}")
        logger.info("=" * 80)
        
        # 상태 파일 완료 기록
        write_status(step_name, 'completed', {
            'total': len(tickers),
            'success': success_counter.value,
            'skip': skip_counter.value,
            'failed': fail_counter.value,
            'success_rate': success_counter.value / len(tickers) * 100 if len(tickers) > 0 else 0,
            'rule_based': stats['rule_based'],
            'ensemble': stats['ensemble'],
            'end_time': datetime.now().isoformat()
        })
        
        return 0
        
    except KeyboardInterrupt:
        logger.warning("사용자에 의해 중단됨")
        write_status(step_name, 'failed', {'error': '사용자에 의해 중단됨'})
        return 1
    except Exception as e:
        logger.error(f"전체 처리 중 오류: {e}", exc_info=True)
        if db:
            db.rollback()
        write_status(step_name, 'failed', {'error': str(e)})
        return 1
    finally:
        if db:
            db.close()

if __name__ == "__main__":
    step_name = 'sector_reclassification_optimized'
    
    # 파라미터 파싱 (step_name 결정용)
    parser = argparse.ArgumentParser(description='전체 기업 섹터 재분류 (Ensemble) - 최적화 버전')
    parser.add_argument('--limit', type=int, help='처리할 기업 수 제한 (테스트용)')
    args, _ = parser.parse_known_args()
    
    if args.limit:
        step_name = f'sector_reclassification_optimized_test_{args.limit}'
    
    exit_code = 1  # 기본값은 실패
    try:
        exit_code = main(step_name=step_name)
        logger.info(f"✅ 스크립트 성공적으로 완료 (exit code: {exit_code})")
    except Exception as e:
        logger.error(f"스크립트 실행 중 치명적 오류: {e}", exc_info=True)
        exit_code = 1
        write_status(step_name, 'failed', {'error': str(e)})
    finally:
        # 정리 및 강제 종료 (무한 로딩 방지)
        cleanup_and_exit(exit_code, step_name)
        # 명확한 종료 신호 출력 (Cursor 완료 감지용)
        print("\n" + "="*80)
        print(f"SCRIPT_EXIT_CODE:{exit_code}")
        print("="*80 + "\n")
        sys.stdout.flush()
        sys.stderr.flush()
        sys.exit(exit_code)

