"""
Gemini-Reasoning 기반 섹터 분류 파이프라인

Step 0-3.5: 기존 방식 유지 (Rule + Embedding + Sub-sector)
Step 4A: KF-DeBERTa Driver Signal Extraction
Step 4B: Gemini-Reasoning Industrial Graph (EXAONE/WON 대체)
Step 4.5: Exposure Drivers 추출

LLM: Gemini API로 전환 완료
"""
import logging
from typing import Optional, Dict, Any, List
from sqlalchemy.orm import Session
import time

logger = logging.getLogger(__name__)

# 기존 ensemble 로직 재사용 (Step 0-3.5)
from app.services.sector_classifier_ensemble import (
    classify_sector_ensemble as _classify_sector_ensemble_base,
    apply_anchor_boosting,
    apply_kg_edge_boosting,
    filter_granular_tags_by_sub_sector,
    extract_exposure_drivers
)

# Gemini-Reasoning 및 Signal Extractor
from app.services.gemini_handler import GeminiHandler, get_gemini_handler
from app.services.sentence_signal_extractor import extract_driver_signals_from_sentences
from app.services.l3_tag_enricher import enrich_l3_tags_from_company_detail
from app.services.driver_tag_enricher import enrich_driver_tags, enrich_driver_tags_with_supersession
from app.services.kg_edge_builder import build_edges_from_causal_structure, save_edges_to_db
from app.services.holding_company_classifier import classify_holding_with_multi_sector
from app.services.reit_classifier import classify_reit_with_multi_sector
from app.services.spac_classifier import classify_spac
from app.services.primary_sector_determiner import apply_primary_sector_flags

# 기존 모델들
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock
from app.models.investor_sector import InvestorSector  # ⭐ 기존 태그 조회용
from app.models.sector_reference import (
    SUB_SECTOR_DEFINITIONS,
    classify_l2_by_rule,  # ⭐ L2 규칙 분류
    get_l2_split_type,    # ⭐ L2 분리 타입 확인
    ANALYSIS_STATE        # ⭐ 분석 상태 상수
)
# ECONVAR_MASTER는 필요시 별도 import (현재 사용하지 않음)


def classify_manufacturing_vs_distribution(
    company_detail: CompanyDetail
) -> Optional[str]:
    """
    제조 vs 유통 구분
    
    Returns:
        'DISTRIBUTION', 'MANUFACTURING', 또는 None
    """
    text = (company_detail.biz_summary or "").lower()
    
    # 유통 키워드
    distribution_keywords = [
        '유통', '수입', '수출', '판매', '도매', '소매',
        'import', 'export', 'distribution', 'retail'
    ]
    
    # 제조 키워드
    manufacturing_keywords = [
        '제조', '생산', '공장', '제작', 'manufacturing', 'production'
    ]
    
    dist_score = sum(1 for kw in distribution_keywords if kw in text)
    mfg_score = sum(1 for kw in manufacturing_keywords if kw in text)
    
    if dist_score > mfg_score and dist_score >= 2:
        return 'DISTRIBUTION'
    elif mfg_score > dist_score and mfg_score >= 2:
        return 'MANUFACTURING'
    
    return None

logger.info("Gemini-Reasoning 기반 섹터 분류 모듈 로드 완료")


def classify_sector_ensemble_won(
    db: Session,
    ticker: str,
    gemini_handler: Optional[GeminiHandler] = None,
    use_embedding: bool = True,
    use_reranking: bool = True,
    max_sectors: int = 3,
    force_reclassify: bool = False  # SPAC 재분류용 플래그
) -> Optional[List[Dict[str, Any]]]:
    """
    Gemini-Reasoning 기반 섹터 분류 (로컬 LLM → Gemini API 전환 완료)
    
    프로세스:
    1. Step 0-3.5: 기존 ensemble 방식 (Rule + Embedding + BGE + Sub-sector)
    2. Step 4A: KF-DeBERTa 문장 기반 드라이버 시그널 추출
    3. Step 4B: Gemini-Reasoning 인과 구조 생성 (로컬 LLM 대체)
    4. Step 4C: GPT Style Polishing (비활성화)
    5. Step 4.5: Exposure Drivers 추출
    
    Args:
        db: DB 세션
        ticker: 종목코드
        gemini_handler: GeminiHandler 객체 (None이면 자동 생성)
        use_embedding: 임베딩 모델 사용 여부
        use_reranking: BGE-M3 Re-ranking 사용 여부
        max_sectors: 최대 섹터 개수
    
    Returns:
        섹터 분류 결과 리스트
    """
    # CompanyDetail 조회
    company_detail = db.query(CompanyDetail).filter(
        CompanyDetail.ticker == ticker
    ).first()
    
    if not company_detail:
        logger.warning(f"[{ticker}] CompanyDetail 데이터 없음")
        return None
    
    from app.utils.stock_query import get_stock_by_ticker_safe
    stock = get_stock_by_ticker_safe(db, ticker)
    company_name = stock.stock_name if stock else None
    
    # Step 0.0: SPAC 여부 확인 (섹터 분류 전에 필터링)
    # force_reclassify가 True이면 SPAC 체크 건너뜀 (재분류 시)
    if stock and not force_reclassify:
        spac_result = classify_spac(stock, company_detail)
        if spac_result.get('is_spac'):
            spac_status = spac_result.get('status')
            logger.info(f"[{ticker}] ⚠️ SPAC 감지: 상태={spac_status}, 예상섹터={spac_result.get('expected_sector')}")
            
            # POST_MERGER 상태인 경우 섹터 분류 진행
            if spac_status == 'POST_MERGER':
                logger.info(f"[{ticker}] ℹ️ POST_MERGER SPAC: 섹터 분류 진행 (합병 완료)")
                # SPAC 필터링 건너뛰고 일반 파이프라인 진행
            else:
                # PRE_MERGER 또는 TARGET_ANNOUNCED: SPAC 전용 인사이트 반환
                logger.info(f"[{ticker}] ℹ️ SPAC은 섹터 분류 대상이 아닙니다. 분석을 건너뜁니다.")
                return [{
                    'major_sector': None,
                    'sector_l1': None,
                    'company_type': 'SPAC',
                    'spac_status': spac_status,
                    'expected_sector': spac_result.get('expected_sector'),
                    'classification_method': 'RULE_BASED_SPAC',
                    'rule_version': 'v1.0',  # Rule 버전
                    'rule_confidence': spac_result.get('confidence', 0.5),  # Rule 신뢰도
                    'training_label': spac_result.get('confidence', 0.5) >= 0.7,  # 학습용 라벨 (신뢰도 0.7 이상)
                    'classification_reasoning': f"SPAC 감지: {', '.join(spac_result.get('evidence', []))}",
                    'insight': 'SPAC은 합병 전 단계로, 실제 사업 내용이 없어 섹터 분석이 불가능합니다.',
                    'confidence': spac_result.get('confidence', 0.5),
                }]
    
    # ⭐ Step 0-3.4: 임베딩 강제 생성 (분류 성공 여부와 상관없이)
    # 향후 밸류체인 분석을 위해 모든 기업의 임베딩을 생성하고 저장
    try:
        from app.services.solar_embedding_model import get_or_create_embedding, prepare_company_text_for_solar
        
        embedding_start = time.time()
        logger.info(f"[{ticker}] 🔄 Step 0-3.4: 임베딩 강제 생성 시작 (밸류체인 분석용)")
        
        # 회사 텍스트 준비
        company_text = prepare_company_text_for_solar(company_detail, company_name)
        
        # 임베딩 생성 또는 조회 (이미 있으면 스킵)
        embedding = get_or_create_embedding(
            db=db,
            ticker=ticker,
            text=company_text,
            force_regenerate=False  # 이미 있으면 재생성 안 함
        )
        
        if embedding is not None:
            embedding_time = time.time() - embedding_start
            logger.info(f"[{ticker}] ✅ 임베딩 생성/조회 완료 ({embedding_time:.2f}초, 벡터 DB 저장됨)")
        else:
            embedding_time = time.time() - embedding_start
            logger.warning(f"[{ticker}] ⚠️ 임베딩 생성 실패 ({embedding_time:.2f}초, Rule-based로 진행)")
    except Exception as e:
        logger.warning(f"[{ticker}] ⚠️ 임베딩 강제 생성 중 오류: {e}")
        # 오류가 있어도 분류는 계속 진행
    
    # Step 0-3.5: 기존 ensemble 방식 사용 (GPT 없이)
    # use_gpt=False로 설정하여 Step 4를 스킵하고 Step 3.5까지만 수행
    step035_start = time.time()
    logger.info(f"[{ticker}] 🔄 Step 0-3.5: 기존 Ensemble 방식 시작 (GPT 없이)")
    
    base_results = _classify_sector_ensemble_base(
        db=db,
        ticker=ticker,
        llm_handler=None,  # GPT 사용 안 함
        use_gpt=False,  # GPT 비활성화
        use_embedding=use_embedding,
        use_reranking=use_reranking,
        max_sectors=max_sectors
    )
    
    step035_time = time.time() - step035_start
    if not base_results:
        logger.warning(f"[{ticker}] ❌ Step 0-3.5 결과 없음 ({step035_time:.2f}초)")
        return None
    
    logger.info(f"[{ticker}] ✅ Step 0-3.5 완료: {len(base_results)}개 섹터 ({step035_time:.2f}초)")
    
    # Step 0.5: 지주사 자동 분류 (Multi-sector 지원)
    if stock:
        holding_start = time.time()
        logger.info(f"[{ticker}] 🔄 Step 0.5: 지주사 자동 분류 시작")
        
        existing_sectors = [r.get('major_sector') for r in base_results if r.get('major_sector')]
        holding_result = classify_holding_with_multi_sector(
            stock=stock,
            company_detail=company_detail,
            existing_sectors=existing_sectors
        )
        
        if holding_result.get('is_holding'):
            # 지주사 섹터 추가
            holding_sector = {
                'major_sector': 'SEC_HOLDING',
                'sub_sector': holding_result.get('l2_sector', 'GENERAL_HOLDING'),
                'sector_l1': 'SEC_HOLDING',
                'sector_l2': holding_result.get('l2_sector', 'GENERAL_HOLDING'),
                'confidence': holding_result.get('confidence', 0.5),
                'classification_method': 'RULE_BASED_HOLDING',
                'rule_version': 'v1.0',  # Rule 버전
                'rule_confidence': holding_result.get('confidence', 0.5),  # Rule 신뢰도
                'training_label': holding_result.get('confidence', 0.5) >= 0.7,  # 학습용 라벨 (신뢰도 0.7 이상)
                'classification_reasoning': f"지주사 자동 분류: {', '.join(holding_result.get('evidence', []))}",
                'is_primary': False,  # Multi-sector이므로 primary는 기존 섹터 유지
                'sector_weight': 0.3 if holding_result.get('multi_sector') else 0.5,
            }
            base_results.append(holding_sector)
            holding_time = time.time() - holding_start
            logger.info(f"[{ticker}] ✅ 지주사 분류 완료: {holding_result.get('l2_sector')} (신뢰도: {holding_result.get('confidence', 0):.2f}, {holding_time:.2f}초)")
            if holding_result.get('multi_sector'):
                logger.info(f"[{ticker}] ℹ️ Multi-sector: 지주사 + {', '.join(existing_sectors)}")
    
    # Step 0.6: 리츠(REITs) 자동 분류 (Multi-sector 지원)
    if stock:
        reit_start = time.time()
        logger.info(f"[{ticker}] 🔄 Step 0.6: 리츠 자동 분류 시작")
        
        existing_sectors_after_holding = [r.get('major_sector') for r in base_results if r.get('major_sector')]
        reit_result = classify_reit_with_multi_sector(
            stock=stock,
            company_detail=company_detail,
            existing_sectors=existing_sectors_after_holding
        )
        
        if reit_result.get('is_reit'):
            # 리츠 섹터 추가
            reit_sector = {
                'major_sector': 'SEC_REIT',
                'sub_sector': reit_result.get('l2_sector', 'COMMERCIAL_REIT'),
                'sector_l1': 'SEC_REIT',
                'sector_l2': reit_result.get('l2_sector', 'COMMERCIAL_REIT'),
                'confidence': reit_result.get('confidence', 0.5),
                'classification_method': 'RULE_BASED_REIT',
                'rule_version': 'v1.0',  # Rule 버전
                'rule_confidence': reit_result.get('confidence', 0.5),  # Rule 신뢰도
                'training_label': reit_result.get('confidence', 0.5) >= 0.7,  # 학습용 라벨 (신뢰도 0.7 이상)
                'classification_reasoning': f"리츠 자동 분류: {', '.join(reit_result.get('evidence', []))}",
                'is_primary': False,  # Multi-sector이므로 primary는 기존 섹터 유지
                'sector_weight': 0.3 if reit_result.get('multi_sector') else 0.5,
            }
            base_results.append(reit_sector)
            reit_time = time.time() - reit_start
            logger.info(f"[{ticker}] ✅ 리츠 분류 완료: {reit_result.get('l2_sector')} (신뢰도: {reit_result.get('confidence', 0):.2f}, {reit_time:.2f}초)")
            if reit_result.get('multi_sector'):
                logger.info(f"[{ticker}] ℹ️ Multi-sector: 리츠 + {', '.join(existing_sectors_after_holding)}")
    
    # Step 0.7: Primary 섹터 결정 (Multi-sector 케이스)
    if len(base_results) > 1:
        logger.info(f"[{ticker}] 🔄 Step 0.7: Primary 섹터 결정 (Multi-sector: {len(base_results)}개)")
        base_results = apply_primary_sector_flags(base_results)
        primary_sector = next((r.get('major_sector') or r.get('sector_l1') for r in base_results if r.get('is_primary')), None)
        if primary_sector:
            logger.info(f"[{ticker}] ✅ Primary 섹터: {primary_sector}")
    
    # Step 0.8: L2 분리 (규칙 기반 - 확장성 개선)
    if company_detail:
        logger.info(f"[{ticker}] 🔄 Step 0.8: L2 분리 시작 (규칙 기반)")
        for result in base_results:
            major_sector = result.get('major_sector')
            
            # 이미 L2가 있으면 스킵 (Rule-based 등으로 설정된 경우)
            if result.get('sector_l2'):
                continue
                
            # 규칙 기반으로 L2 분리 여부 확인
            split_type = get_l2_split_type(major_sector)
            if split_type:
                l2_code, l2_conf = classify_l2_by_rule(major_sector, company_detail.biz_summary)
                if l2_code:
                    result['sector_l2'] = l2_code
                    result['sub_sector'] = l2_code  # 하위 호환성
                    result['l2_split_type'] = split_type
                    result['confidence_l2'] = l2_conf  # ⭐ L2 confidence 저장
                    # ⭐ L2 Confidence 로깅 강화 (디버깅/설명자료용)
                    logger.info(f"[L2_CONF] {ticker} {major_sector}→{l2_code} (Rule: {split_type}, Conf: {l2_conf:.2f})")
                    logger.info(f"[{ticker}] ✅ {major_sector} → L2: {l2_code} (규칙: {split_type}, conf: {l2_conf:.2f})")
                else:
                    logger.debug(f"[{ticker}] {major_sector} → L2 분류 불가 (키워드 없음)")
            else:
                logger.debug(f"[{ticker}] {major_sector} → L2 분리 규칙 없음")
    
    # Step 4A: KF-DeBERTa Driver Signal Extraction
    step4a_start = time.time()
    logger.info(f"[{ticker}] 🔄 Step 4A: KF-DeBERTa Driver Signal Extraction 시작")
    
    driver_signals = {}
    for result in base_results:
        major_sector = result.get('major_sector')
        sub_sector = result.get('sub_sector')
        
        if major_sector:
            try:
                sector_start = time.time()
                signals = extract_driver_signals_from_sentences(
                    company_detail=company_detail,
                    major_sector=major_sector,
                    sub_sector=sub_sector,
                    sector_l2=result.get('sector_l2')  # ⭐ L2 정보 전달
                )
                
                # ⭐ Fallback: 드라이버가 없고 Multi-sector인 경우
                is_empty = (
                    not signals.get('price_signals') and 
                    not signals.get('quantity_signals') and 
                    not signals.get('cost_signals')
                )
                
                if is_empty and len(base_results) > 1:
                    logger.warning(f"[{ticker}] ⚠️ {major_sector} 드라이버 없음, 다른 섹터에서 Fallback 시도")
                    # Primary 섹터가 아니면 Primary 섹터의 드라이버 재사용
                    primary_result = next((r for r in base_results if r.get('is_primary')), None)
                    if primary_result and primary_result.get('major_sector') != major_sector:
                        primary_signals = driver_signals.get(primary_result.get('major_sector'), {})
                        if primary_signals and (primary_signals.get('price_signals') or primary_signals.get('quantity_signals') or primary_signals.get('cost_signals')):
                            signals = primary_signals
                            logger.info(f"[{ticker}] ✅ {primary_result.get('major_sector')} 드라이버 재사용")
                sector_time = time.time() - sector_start
                driver_signals[major_sector] = signals
                logger.info(f"[{ticker}] ✅ {major_sector} 드라이버 시그널 추출 완료: P={len(signals.get('price_signals', []))}, Q={len(signals.get('quantity_signals', []))}, C={len(signals.get('cost_signals', []))} ({sector_time:.2f}초)")
            except Exception as e:
                logger.error(f"[{ticker}] ❌ 드라이버 시그널 추출 실패 ({major_sector}): {e}", exc_info=True)
                driver_signals[major_sector] = {
                    "price_signals": [],
                    "quantity_signals": [],
                    "cost_signals": []
                }
    
    step4a_time = time.time() - step4a_start
    logger.info(f"[{ticker}] ✅ Step 4A 완료 (총 소요 시간: {step4a_time:.2f}초)")
    
    # Step 4B: Gemini-Reasoning Industrial Graph (로컬 LLM 대체)
    step4b_start = time.time()
    logger.info(f"[{ticker}] 🔄 Step 4B: Gemini-Reasoning Industrial Graph 시작")
    
    # Gemini Handler 초기화 (없으면 싱글톤으로 가져오기)
    if gemini_handler is None:
        try:
            gemini_init_start = time.time()
            logger.info(f"[{ticker}] 🔄 Gemini-Reasoning Handler 초기화 중...")
            gemini_handler = get_gemini_handler()
            gemini_init_time = time.time() - gemini_init_start
            logger.info(f"[{ticker}] ✅ Gemini-Reasoning Handler 초기화 완료 ({gemini_init_time:.2f}초)")
        except Exception as e:
            logger.error(f"[{ticker}] ❌ Gemini-Reasoning Handler 생성 실패: {e}")
            # Fallback: 기존 결과 반환 (인과 구조 없이)
            return base_results
    
    # 각 섹터별로 Gemini-Reasoning 수행
    for result in base_results:
        major_sector = result.get('major_sector')
        sub_sector = result.get('sub_sector')
        
        if not major_sector:
            continue
        
        try:
            # 해당 섹터의 드라이버 시그널 가져오기
            signals = driver_signals.get(major_sector, {})
            
            # ⭐ Driver-less Reasoning 차단 (INSUFFICIENT_DRIVER_SIGNAL)
            total_signals = sum(len(signals.get(k, [])) for k in ['price_signals', 'quantity_signals', 'cost_signals'])
            
            if total_signals == 0:
                logger.info(f"[{ticker}] ℹ️ {major_sector} driver_signals 없음 → 제한적 분석 모드 ({ANALYSIS_STATE['INSUFFICIENT_DRIVER_SIGNAL']})")
                
                # 제한적 분석 결과 생성 (Gemini 호출 스킵)
                result['causal_structure'] = {
                    'schema_version': 'v1.0',
                    'summary_sentence': f'{company_name or ticker}의 핵심 경제 변수를 식별하지 못해 상세 인과 분석이 제한됩니다.',
                    'easy_explanation': '현재 이 기업에 대한 충분한 드라이버 정보가 없어 상세한 인과 분석이 어렵습니다. 추가 데이터 수집 후 재분석이 필요합니다.',
                    'sentiment_label': 'Neutral',
                    'key_drivers': [],
                    'upstream_impacts': [],
                    'downstream_impacts': [],
                    'risk_factors': [],
                    'opportunity_factors': [],
                    'granular_tags': [],
                    'analysis_state': ANALYSIS_STATE['INSUFFICIENT_DRIVER_SIGNAL'],  # ⭐ 상수 사용
                    'analysis_note': f'L2({result.get("sector_l2")}) 기반 드라이버 추출 실패'
                }
                result['causal_structure_status'] = ANALYSIS_STATE['LIMITED']  # ⭐ 상수 사용
                continue  # Gemini 호출 스킵
            
            # 드라이버 시그널 통계 로깅
            signal_counts = {
                'P': len(signals.get('price_signals', [])),
                'Q': len(signals.get('quantity_signals', [])),
                'C': len(signals.get('cost_signals', []))
            }
            logger.info(f"[{ticker}] 📊 {major_sector} 드라이버 시그널: P={signal_counts['P']}, Q={signal_counts['Q']}, C={signal_counts['C']}")
            
            # Gemini-Reasoning 호출
            sector_reasoning_start = time.time()
            causal_structure = gemini_handler.generate_causal_structure(
                company_detail=company_detail,
                major_sector=major_sector,
                sub_sector=sub_sector,
                driver_signals=signals
            )
            sector_reasoning_time = time.time() - sector_reasoning_start
            
            # 인과 구조 통계 로깅
            upstream_count = len(causal_structure.get('upstream_impacts', []))
            downstream_count = len(causal_structure.get('downstream_impacts', []))
            drivers_count = len(causal_structure.get('key_drivers', []))
            logger.info(f"[{ticker}] ✅ {major_sector} Gemini-Reasoning 완료 ({sector_reasoning_time:.2f}초)")
            logger.info(f"[{ticker}] 📈 인과 구조: 업스트림={upstream_count}, 다운스트림={downstream_count}, 드라이버={drivers_count}")
            
            # 결과에 인과 구조 추가
            result['causal_structure'] = causal_structure
            result['causal_structure_status'] = ANALYSIS_STATE['SUCCESS']  # ⭐ 상수 사용
            
        except Exception as e:
            logger.error(f"[{ticker}] ❌ Gemini-Reasoning 실패 ({major_sector}): {e}", exc_info=True)
            result['causal_structure_status'] = ANALYSIS_STATE['ERROR']  # ⭐ 상수 사용
    
    # Step 4C: GPT Style Polishing (비활성화)
    # 사용자가 요청한 대로 GPT 사용 안 함
    logger.info(f"[{ticker}] Step 4C: GPT Style Polishing 스킵 (비활성화)")
    
    # Step 4.5: Exposure Drivers 추출 및 Granular Tags 필터링
    logger.info(f"[{ticker}] Step 4.5: Exposure Drivers 추출 및 Granular Tags 필터링 시작")
    
    for result in base_results:
        sector_code = result.get('major_sector')
        sub_sector_code = result.get('sub_sector')
        causal_structure = result.get('causal_structure')
        
        if not sector_code:
            continue
        
        # L3 태그 Enrichment (Backend에서 자동 부여)
        if causal_structure:
            # LLM이 생성한 granular_tags는 참고용으로만 사용
            llm_granular_tags = causal_structure.get('granular_tags', [])
            
            # Backend에서 정확한 L3 태그 자동 부여
            enriched_l3_tags = enrich_l3_tags_from_company_detail(
                sector_l1=sector_code,
                company_detail=company_detail,
                causal_structure=causal_structure
            )
            
            # L3 태그 업데이트 (enriched 태그가 있으면 사용, 없으면 LLM 태그 유지)
            if enriched_l3_tags:
                causal_structure['granular_tags'] = enriched_l3_tags
                causal_structure['l3_tags'] = enriched_l3_tags  # 새로운 필드 추가
                logger.info(f"[{ticker}] L3 태그 Enrichment: {len(enriched_l3_tags)}개 태그 부여 ({sector_code})")
            elif llm_granular_tags:
                # LLM 태그는 그대로 유지 (참고용)
                logger.debug(f"[{ticker}] L3 태그 Enrichment 실패, LLM 태그 유지: {llm_granular_tags}")
            
            # ⭐ Driver Tags 부여 (Backend Rule - 100% 시스템 생성)
            key_drivers = causal_structure.get('key_drivers', [])
            sector_l2 = result.get('sector_l2') or result.get('sub_sector')
            sector_l2_confidence = result.get('confidence_l2')
            
            # ⭐ 기존 태그 조회 (Supersession용)
            existing_driver_tags_map = {}
            if db:
                try:
                    existing_investor_sector = db.query(InvestorSector).filter(
                        InvestorSector.ticker == ticker,
                        InvestorSector.major_sector == sector_code
                    ).first()
                    
                    if existing_investor_sector and existing_investor_sector.causal_structure:
                        old_causal = existing_investor_sector.causal_structure
                        old_drivers = old_causal.get('key_drivers', [])
                        for old_driver in old_drivers:
                            old_code = old_driver.get('code')
                            if old_code:
                                existing_driver_tags_map[old_code] = old_driver.get('driver_tags_metadata', [])
                        
                        if existing_driver_tags_map:
                            logger.debug(f"[{ticker}] 기존 Driver Tags 조회: {len(existing_driver_tags_map)}개 드라이버")
                except Exception as e:
                    logger.warning(f"[{ticker}] 기존 Driver Tags 조회 실패: {e}")
            
            if key_drivers:
                driver_tags_count = 0
                for driver in key_drivers:
                    driver_code = driver.get('code')
                    if not driver_code:
                        continue
                    
                    # ⭐ 기존 태그가 있으면 Supersession 함수 사용
                    existing_tags = existing_driver_tags_map.get(driver_code)
                    
                    if existing_tags:
                        # 기존 태그와 비교하여 Supersession 적용
                        driver_tags = enrich_driver_tags_with_supersession(
                            driver_code=driver_code,
                            sector_l1=sector_code,
                            sector_l2=sector_l2,
                            sector_l2_confidence=sector_l2_confidence,
                            company_detail=company_detail,
                            existing_tags=existing_tags
                        )
                    else:
                        # 기존 태그 없음 → 새로 생성
                        driver_tags = enrich_driver_tags(
                            driver_code=driver_code,
                            sector_l1=sector_code,
                            sector_l2=sector_l2,
                            sector_l2_confidence=sector_l2_confidence,
                            company_detail=company_detail
                        )
                    
                    if driver_tags:
                        # driver_tags를 리스트 형태로 저장 (하위 호환성)
                        driver['driver_tags'] = [tag_info['tag'] for tag_info in driver_tags]
                        # 메타데이터도 함께 저장 (수명 관리용)
                        driver['driver_tags_metadata'] = driver_tags
                        driver_tags_count += len(driver_tags)
                        logger.debug(f"[{ticker}] {driver_code}: {len(driver_tags)}개 Driver Tags 부여")
                    else:
                        # Driver Tags가 없는 경우 빈 리스트로 명시
                        driver['driver_tags'] = []
                        driver['driver_tags_metadata'] = []
                        logger.debug(f"[{ticker}] {driver_code}: Driver Tags 없음 (Allowlist 없음 또는 매칭 실패)")
                
                if driver_tags_count > 0:
                    logger.info(f"[{ticker}] Driver Tags Enrichment: 총 {driver_tags_count}개 태그 부여 ({sector_code})")
            
            result['causal_structure'] = causal_structure
        
        # Exposure Drivers 추출 (메모리/로그만 사용, DB 저장 안 함)
        exposure_drivers, supporting_drivers = extract_exposure_drivers(
            sector_code,
            sub_sector_code,
            causal_structure,
            company_detail
        )
        
        # ⚠️ exposure_drivers는 메모리에서만 사용 (Fallback용)
        # DB 저장 안 함 (causal_structure.key_drivers만 저장)
        if exposure_drivers:
            logger.debug(f"[{ticker}] {sector_code}/{sub_sector_code} → Exposure Drivers: {len(exposure_drivers)}개 (메모리만 사용)")
        
        if supporting_drivers:
            logger.debug(f"[{ticker}] {sector_code} → Supporting Drivers: {len(supporting_drivers)}개 (메모리만 사용)")
        
        # ⭐ Fallback: key_drivers가 비어있으면 exposure_drivers를 key_drivers로 변환
        if causal_structure:
            key_drivers = causal_structure.get('key_drivers', [])
            analysis_state = causal_structure.get('analysis_state')
            
            # INSUFFICIENT_DRIVER_SIGNAL 상태가 아닐 때만 Fallback 수행 (정직한 실패 유지)
            if not key_drivers and exposure_drivers and analysis_state != 'INSUFFICIENT_DRIVER_SIGNAL':
                logger.warning(f"[{ticker}] ⚠️ {sector_code} key_drivers가 비어있음, exposure_drivers를 key_drivers로 변환")
                # exposure_drivers를 key_drivers 형식으로 변환
                converted_key_drivers = []
                for ed in exposure_drivers[:5]:  # 최대 5개만
                    converted_key_drivers.append({
                        'var': ed.get('var', ed.get('code', '')),
                        'code': ed.get('code', ''),
                        'type': ed.get('type', ''),
                        'direction': ed.get('direction', ''),
                        'description': ed.get('description', ''),
                        'evidence': ed.get('evidence', [])
                    })
                causal_structure['key_drivers'] = converted_key_drivers
                result['causal_structure'] = causal_structure
                logger.info(f"[{ticker}] ✅ {sector_code} exposure_drivers → key_drivers 변환 완료: {len(converted_key_drivers)}개")
            elif not key_drivers and analysis_state == 'INSUFFICIENT_DRIVER_SIGNAL':
                logger.info(f"[{ticker}] ℹ️ {sector_code} 드라이버 부족 상태이므로 Fallback 수행 안 함 (정직한 실패)")
    
    step4b_time = time.time() - step4b_start
    logger.info(f"[{ticker}] ✅ Step 4B 완료 (총 소요 시간: {step4b_time:.2f}초)")
    
    # Step 4.6: KG Edge 생성 (Post-processing)
    logger.info(f"[{ticker}] Step 4.6: KG Edge 생성 시작")
    kg_edge_start = time.time()
    
    try:
        for result in base_results:
            causal_structure = result.get('causal_structure')
            major_sector = result.get('major_sector')
            sector_l2 = result.get('sector_l2') or result.get('sub_sector')
            
            if causal_structure:
                edges = build_edges_from_causal_structure(
                    ticker=ticker,
                    causal_structure=causal_structure,
                    major_sector=major_sector,
                    sector_l2=sector_l2
                )
                
                if edges:
                    saved_count = save_edges_to_db(db, edges, upsert=True)
                    logger.info(f"[{ticker}] ✅ KG Edge 생성 완료: {saved_count}개 저장 ({major_sector})")
        
        kg_edge_time = time.time() - kg_edge_start
        logger.info(f"[{ticker}] ✅ Step 4.6 완료 (소요 시간: {kg_edge_time:.2f}초)")
    except Exception as e:
        logger.error(f"[{ticker}] ❌ KG Edge 생성 실패: {e}", exc_info=True)
    
    # ⭐ NEW: NULL 섹터 최종 검증
    for result in base_results:
        if result.get('is_primary') or base_results.index(result) == 0:
            if not result.get('sector_l1') and not result.get('major_sector'):
                logger.warning(f"[{ticker}] ⚠️ Primary 섹터가 NULL, 강제 Fallback 적용")
                
                # 확정적 규칙 적용
                result['sector_l1'] = 'SEC_UNKNOWN'
                result['major_sector'] = 'SEC_UNKNOWN'
                result['fallback_used'] = 'TRUE'  # ⭐ VARCHAR에 문자열 저장
                result['fallback_type'] = 'UNKNOWN'  # ⭐ 타입 분리
                result['confidence'] = 'VERY_LOW'
                result['method'] = 'FALLBACK_UNKNOWN'
                result['ensemble_score'] = 0.0
                result['reasoning'] = 'NULL 섹터 감지, UNKNOWN 할당'
                
                logger.info(f"[{ticker}] ✅ NULL 섹터 → SEC_UNKNOWN 할당 완료")
                break
    
    logger.info(f"[{ticker}] ✅ Gemini 기반 섹터 분류 완료: {len(base_results)}개 섹터")
    
    return base_results
    
    logger.info(f"[{ticker}] ✅ Gemini 기반 섹터 분류 완료: {len(base_results)}개 섹터")
    
    return base_results


def classify_sector_ensemble_won_batch(
    db: Session,
    tickers: List[str],
    gemini_handler: Optional[GeminiHandler] = None,
    use_embedding: bool = True,
    use_reranking: bool = True,
    max_sectors: int = 3
) -> Dict[str, Optional[List[Dict[str, Any]]]]:
    """
    배치 처리: 여러 기업을 한 번에 처리 (모델은 한 번만 로드)
    
    Args:
        db: DB 세션
        tickers: 종목코드 리스트
        gemini_handler: GeminiHandler 객체 (None이면 자동 생성)
        use_embedding: 임베딩 모델 사용 여부
        use_reranking: BGE-M3 Re-ranking 사용 여부
        max_sectors: 최대 섹터 개수
    
    Returns:
        {ticker: results} 딕셔너리
    """
    logger.info(f"🔄 배치 처리 시작: {len(tickers)}개 기업")
    batch_start = time.time()
    
    # ========================================================================
    # 모델 사전 로딩 (병렬화 대신 순차 로딩, 하지만 미리 로드)
    # ========================================================================
    logger.info("🔄 [사전 로딩] 필요한 모델들을 미리 로딩 중...")
    preload_start = time.time()
    
    # 1. Gemini Handler 초기화 (한 번만)
    if gemini_handler is None:
        logger.info("  📥 [사전 로딩] Gemini-Reasoning 핸들러 초기화 중...")
        import sys
        sys.stdout.flush()  # 로그 즉시 출력
        gemini_init_start = time.time()
        try:
            gemini_handler = get_gemini_handler()
            gemini_init_time = time.time() - gemini_init_start
            logger.info(f"  ✅ [사전 로딩] Gemini-Reasoning 핸들러 초기화 완료 ({gemini_init_time:.2f}초)")
            sys.stdout.flush()  # 로그 즉시 출력
        except Exception as e:
            logger.error(f"  ❌ [사전 로딩] Gemini-Reasoning 핸들러 초기화 실패: {e}", exc_info=True)
            sys.stdout.flush()
            raise
    
    # 2. KF-DeBERTa 모델 사전 로딩 (Step 4A에서 사용)
    try:
        logger.info("  📥 [사전 로딩] KF-DeBERTa 모델 초기화 중...")
        kf_start = time.time()
        # KF-DeBERTa 모델은 제거됨 (Solar Embedding으로 대체)
        # from app.services.embedding_model_direct import get_direct_embedding_model
        # kf_model = get_direct_embedding_model()  # 싱글톤으로 한 번만 로드
        kf_time = time.time() - kf_start
        logger.info(f"  ✅ [사전 로딩] KF-DeBERTa 모델 제거됨 (Solar Embedding 사용) ({kf_time:.2f}초)")
    except Exception as e:
        logger.warning(f"  ⚠️ [사전 로딩] KF-DeBERTa 모델 제거됨: {e}")
    
    # 3. 임베딩 모델 사전 로딩 (Step 0-3.5에서 사용)
    if use_embedding:
        try:
            logger.info("  📥 [사전 로딩] 임베딩 모델 초기화 중...")
            emb_start = time.time()
            from app.services.sector_classifier_embedding import get_embedding_model
            emb_model = get_embedding_model()  # 싱글톤으로 한 번만 로드
            emb_time = time.time() - emb_start
            logger.info(f"  ✅ [사전 로딩] 임베딩 모델 초기화 완료 ({emb_time:.2f}초)")
        except Exception as e:
            logger.warning(f"  ⚠️ [사전 로딩] 임베딩 모델 로딩 실패 (나중에 로드됨): {e}")
    
    # 4. BGE-M3 모델 사전 로딩 (Step 0-3.5에서 사용)
    if use_reranking:
        try:
            logger.info("  📥 [사전 로딩] BGE-M3 모델 초기화 중...")
            bge_start = time.time()
            from app.services.sector_classifier_reranker import get_bge_model
            bge_model = get_bge_model()  # 싱글톤으로 한 번만 로드
            bge_time = time.time() - bge_start
            logger.info(f"  ✅ [사전 로딩] BGE-M3 모델 초기화 완료 ({bge_time:.2f}초)")
        except Exception as e:
            logger.warning(f"  ⚠️ [사전 로딩] BGE-M3 모델 로딩 실패 (나중에 로드됨): {e}")
    
    preload_time = time.time() - preload_start
    logger.info(f"✅ [사전 로딩] 모든 모델 로딩 완료 (총 소요 시간: {preload_time:.2f}초)")
    import sys
    sys.stdout.flush()  # 로그 즉시 출력
    
    # GPU 메모리 상태 확인
    try:
        import torch
        if torch.cuda.is_available():
            gpu_memory = torch.cuda.get_device_properties(0).total_memory / 1024**3  # GB
            gpu_allocated = torch.cuda.memory_allocated(0) / 1024**3  # GB
            gpu_reserved = torch.cuda.memory_reserved(0) / 1024**3  # GB
            logger.info(f"📊 [GPU 메모리] 총: {gpu_memory:.2f}GB, 할당: {gpu_allocated:.2f}GB, 예약: {gpu_reserved:.2f}GB, 사용률: {(gpu_reserved/gpu_memory)*100:.1f}%")
    except Exception as e:
        logger.debug(f"GPU 메모리 확인 실패: {e}")
    
    # 모든 기업의 CompanyDetail 미리 조회
    logger.info("🔄 [데이터 조회] 기업 데이터 조회 중...")
    data_start = time.time()
    company_details_map = {}
    for ticker in tickers:
        company_detail = db.query(CompanyDetail).filter(
            CompanyDetail.ticker == ticker
        ).first()
        if company_detail:
            company_details_map[ticker] = company_detail
    data_time = time.time() - data_start
    logger.info(f"✅ [데이터 조회] {len(company_details_map)}개 기업 데이터 조회 완료 ({data_time:.2f}초)")
    
    # 각 기업별로 처리 (순차 처리, 하지만 모델은 한 번만 로드)
    results = {}
    for i, ticker in enumerate(tickers, 1):
        logger.info(f"\n[{i}/{len(tickers)}] {ticker} 처리 중...")
        ticker_start = time.time()
        
        try:
            result = classify_sector_ensemble_won(
                db=db,
                ticker=ticker,
                gemini_handler=gemini_handler,  # 재사용
                use_embedding=use_embedding,
                use_reranking=use_reranking,
                max_sectors=max_sectors
            )
            ticker_time = time.time() - ticker_start
            results[ticker] = result
            logger.info(f"✅ {ticker} 완료 (소요 시간: {ticker_time:.2f}초)")
        except Exception as e:
            logger.error(f"❌ {ticker} 처리 실패: {e}", exc_info=True)
            results[ticker] = None
    
    batch_time = time.time() - batch_start
    avg_time = batch_time / len(tickers) if tickers else 0
    logger.info(f"\n✅ 배치 처리 완료: {len(tickers)}개 기업, 총 소요 시간: {batch_time:.2f}초, 기업당 평균: {avg_time:.2f}초")
    
    return results


