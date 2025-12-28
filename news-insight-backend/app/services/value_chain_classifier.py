"""
밸류체인 분석 및 분류 모듈

하이브리드 방식: Rule-based (기존) + Ensemble (신규)
- Rule confidence HIGH → Rule-based 즉시 반환
- Rule confidence LOW → Ensemble 실행
"""
import os
import logging
from typing import Optional, List, Dict, Any, Tuple
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# 기존 Rule-based 분류기
from app.services.sector_classifier import VALUE_CHAIN_KEYWORDS, SECTOR_KEYWORDS
from app.models.company_detail import CompanyDetail
from app.utils.text_chunking import truncate_to_sentences

# Phase 1: 임베딩 모델 기반 후보 생성기
EMBEDDING_AVAILABLE = False
try:
    from app.services.value_chain_classifier_embedding import classify_value_chain_embedding, SENTENCE_TRANSFORMERS_AVAILABLE
    if SENTENCE_TRANSFORMERS_AVAILABLE:
        EMBEDDING_AVAILABLE = True
        logger.info("임베딩 모델 모듈 import 성공 (밸류체인 분류용, lazy loading - 실제 사용 시점에 로드)")
    else:
        EMBEDDING_AVAILABLE = False
        logger.warning("sentence-transformers not installed. Embedding model not available.")
except ImportError as e:
    EMBEDDING_AVAILABLE = False
    logger.warning(f"임베딩 모델 import 실패: {e}. Will skip embedding-based classification.")

# Phase 2: BGE-M3 Re-ranking
try:
    from app.services.value_chain_classifier_reranker import rerank_value_chain_candidates
    BGE_AVAILABLE = True
except ImportError:
    BGE_AVAILABLE = False
    logger.warning("BGE-M3 not available. Will skip reranking.")

# Phase 3: GPT 최종 검증
try:
    from app.services.value_chain_classifier_validator import validate_value_chain_with_gpt
    GPT_AVAILABLE = True
except ImportError:
    GPT_AVAILABLE = False
    logger.warning("GPT validator not available. Will skip final validation.")

from app.services.llm_handler import LLMHandler


# ============================================================================
# 섹터별 밸류체인 키워드 (28개 섹터)
# ============================================================================

SECTOR_SPECIFIC_VALUE_CHAIN_KEYWORDS = {
    # [Tech & Growth] - 5개
    'SEC_SEMI': {
        'UPSTREAM': [
            '웨이퍼', '소재', '화학약품', '가스', '원재료 조달',
            '부품', '장비 부품', '소부장', '화학약품', '실리콘'
        ],
        'MIDSTREAM': [
            '제조', '공정', '패키징', '테스트', '검사',
            '웨이퍼 가공', '회로 형성', '이온 주입', '식각', '증착'
        ],
        'DOWNSTREAM': [
            '판매', '고객사', '납품', '모듈', '시스템',
            '최종 제품', '반도체 판매', '칩 판매'
        ]
    },
    'SEC_BATTERY': {
        'UPSTREAM': [
            '양극재', '음극재', '전해액', '분리막', '리튬',
            '니켈', '코발트', '원재료', '소재 조달'
        ],
        'MIDSTREAM': [
            '셀 제조', '배터리팩 조립', '충방전 테스트',
            '제조', '생산', '가공'
        ],
        'DOWNSTREAM': [
            '배터리 판매', '전기차 납품', '에너지저장시스템',
            '판매', '납품', '고객사'
        ]
    },
    'SEC_IT': {
        'UPSTREAM': [
            '소프트웨어 라이선스', '클라우드 인프라', 'API',
            '개발 도구', '플랫폼'
        ],
        'MIDSTREAM': [
            '소프트웨어 개발', '시스템 구축', '솔루션 개발',
            '개발', '구축', '제작'
        ],
        'DOWNSTREAM': [
            '서비스 제공', '고객 지원', '유지보수',
            '판매', '서비스', '고객'
        ]
    },
    'SEC_GAME': {
        'UPSTREAM': [
            '게임 엔진', '라이선스', 'IP', '개발 도구'
        ],
        'MIDSTREAM': [
            '게임 개발', '퍼블리싱', 'QA', '테스트',
            '개발', '제작'
        ],
        'DOWNSTREAM': [
            '게임 판매', '다운로드', '인앱결제', '서비스',
            '판매', '서비스'
        ]
    },
    'SEC_ELECTRONICS': {
        'UPSTREAM': [
            '부품', '소재', '패널', '모듈', '원재료'
        ],
        'MIDSTREAM': [
            '제조', '조립', '생산', '가공', '검사'
        ],
        'DOWNSTREAM': [
            '제품 판매', '유통', '고객', 'A/S'
        ]
    },
    
    # [Mobility] - 2개
    'SEC_AUTO': {
        'UPSTREAM': [
            '부품', '소재', '강판', '플라스틱', '전자부품',
            '모터', '배터리', '센서', '원재료'
        ],
        'MIDSTREAM': [
            '조립', '용접', '도장', '검사', '완성차 제조',
            '생산라인', '조립라인', '제조'
        ],
        'DOWNSTREAM': [
            '자동차 판매', '딜러', '고객', 'A/S', '서비스',
            '판매', '유통'
        ]
    },
    'SEC_TIRE': {
        'UPSTREAM': [
            '고무', '카본블랙', '원재료', '소재'
        ],
        'MIDSTREAM': [
            '타이어 제조', '가공', '성형', '벌커나이징',
            '제조', '생산'
        ],
        'DOWNSTREAM': [
            '타이어 판매', '자동차사 납품', '유통',
            '판매', '납품'
        ]
    },
    
    # [Industry & Cyclical] - 6개
    'SEC_SHIP': {
        'UPSTREAM': [
            '강판', '엔진', '부품', '기자재', '원재료'
        ],
        'MIDSTREAM': [
            '조선', '선박 건조', '용접', '도장', '시운전',
            '제조', '건조'
        ],
        'DOWNSTREAM': [
            '선박 인도', '해운사 납품', '판매',
            '인도', '판매'
        ]
    },
    'SEC_DEFENSE': {
        'UPSTREAM': [
            '부품', '소재', '전자부품', '원재료'
        ],
        'MIDSTREAM': [
            '무기 제조', '조립', '테스트', '검사',
            '제조', '생산'
        ],
        'DOWNSTREAM': [
            '국방 납품', '수출', '판매',
            '납품', '판매'
        ]
    },
    'SEC_MACH': {
        'UPSTREAM': [
            '부품', '소재', '강판', '원재료'
        ],
        'MIDSTREAM': [
            '기계 제조', '조립', '가공', '검사',
            '제조', '생산'
        ],
        'DOWNSTREAM': [
            '기계 판매', '설치', 'A/S', '서비스',
            '판매', '서비스'
        ]
    },
    'SEC_CONST': {
        'UPSTREAM': [
            '건축자재', '시멘트', '철강', '원재료'
        ],
        'MIDSTREAM': [
            '건설', '공사', '시공', '착공',
            '건축', '토목'
        ],
        'DOWNSTREAM': [
            '준공', '인도', '판매', '임대',
            '완공', '인도'
        ]
    },
    'SEC_STEEL': {
        'UPSTREAM': [
            '철광석', '석탄', '원재료', '원료'
        ],
        'MIDSTREAM': [
            '제철', '압연', '강판 제조', '가공',
            '제조', '생산'
        ],
        'DOWNSTREAM': [
            '강판 판매', '납품', '유통',
            '판매', '납품'
        ]
    },
    'SEC_CHEM': {
        'UPSTREAM': [
            '원유', '나프타', '가스', '원재료'
        ],
        'MIDSTREAM': [
            '정유', '석유화학', '합성', '가공',
            '제조', '생산'
        ],
        'DOWNSTREAM': [
            '화학제품 판매', '납품', '유통',
            '판매', '납품'
        ]
    },
    
    # [Consumer & K-Culture] - 6개
    'SEC_ENT': {
        'UPSTREAM': [
            'IP', '저작권', '음원', '콘텐츠'
        ],
        'MIDSTREAM': [
            '콘텐츠 제작', '앨범 제작', '영화 제작',
            '제작', '개발'
        ],
        'DOWNSTREAM': [
            '음악 판매', '스트리밍', '방송', '서비스',
            '판매', '서비스'
        ]
    },
    'SEC_COSMETIC': {
        'UPSTREAM': [
            '원료', '향료', '포장재', '소재'
        ],
        'MIDSTREAM': [
            '화장품 제조', '생산', '포장',
            '제조', '생산'
        ],
        'DOWNSTREAM': [
            '화장품 판매', '유통', '고객',
            '판매', '유통'
        ]
    },
    'SEC_TRAVEL': {
        'UPSTREAM': [
            '항공기', '연료', '인프라'
        ],
        'MIDSTREAM': [
            '운항', '서비스 운영', '관리',
            '운영', '서비스'
        ],
        'DOWNSTREAM': [
            '항공권 판매', '고객 서비스', '여행 상품',
            '판매', '서비스'
        ]
    },
    'SEC_FOOD': {
        'UPSTREAM': [
            '농산물', '축산물', '수산물', '원재료'
        ],
        'MIDSTREAM': [
            '식품 가공', '제조', '생산', '포장',
            '가공', '제조'
        ],
        'DOWNSTREAM': [
            '식품 판매', '유통', '고객',
            '판매', '유통'
        ]
    },
    'SEC_RETAIL': {
        'UPSTREAM': [
            '상품 조달', '매입', '공급'
        ],
        'MIDSTREAM': [
            '재고 관리', '물류', '창고'
        ],
        'DOWNSTREAM': [
            '판매', '고객', '서비스', '유통'
        ]
    },
    'SEC_CONSUMER': {
        'UPSTREAM': [
            '원재료', '부품', '소재'
        ],
        'MIDSTREAM': [
            '제조', '생산', '가공'
        ],
        'DOWNSTREAM': [
            '판매', '유통', '고객', '서비스'
        ]
    },
    
    # [Healthcare] - 2개
    'SEC_BIO': {
        'UPSTREAM': [
            '원료', '시약', '세포주', '원재료'
        ],
        'MIDSTREAM': [
            '제약', '신약 개발', '임상', '생산',
            '개발', '제조'
        ],
        'DOWNSTREAM': [
            '의약품 판매', '병원 납품', '유통',
            '판매', '납품'
        ]
    },
    'SEC_MEDDEV': {
        'UPSTREAM': [
            '부품', '소재', '원재료'
        ],
        'MIDSTREAM': [
            '의료기기 제조', '조립', '검사',
            '제조', '생산'
        ],
        'DOWNSTREAM': [
            '의료기기 판매', '병원 납품', '서비스',
            '판매', '납품'
        ]
    },
    
    # [Finance] - 5개
    'SEC_BANK': {
        'UPSTREAM': [
            '예금', '저축', '자금 조달', '차입', '자금 확보'
        ],
        'MIDSTREAM': [
            '대출', '투자', '자산운용', '리스크 관리', '신용 평가',
            '운용', '관리'
        ],
        'DOWNSTREAM': [
            '이자 수익', '수수료', '고객 서비스', '금융 상품 판매',
            '수익', '서비스'
        ]
    },
    'SEC_SEC': {
        'UPSTREAM': [
            '자금 조달', '펀드 모집'
        ],
        'MIDSTREAM': [
            '투자', '자산운용', '브로커리지', '상장',
            '운용', '중개'
        ],
        'DOWNSTREAM': [
            '수수료 수익', '고객 서비스', '상담',
            '수익', '서비스'
        ]
    },
    'SEC_INS': {
        'UPSTREAM': [
            '보험료 수집', '자금 조달'
        ],
        'MIDSTREAM': [
            '보험 운영', '리스크 관리', '재보험',
            '운영', '관리'
        ],
        'DOWNSTREAM': [
            '보상', '고객 서비스', '상담',
            '보상', '서비스'
        ]
    },
    'SEC_CARD': {
        'UPSTREAM': [
            '자금 조달', '차입'
        ],
        'MIDSTREAM': [
            '결제 처리', '승인', '리스크 관리',
            '처리', '관리'
        ],
        'DOWNSTREAM': [
            '수수료 수익', '고객 서비스',
            '수익', '서비스'
        ]
    },
    'SEC_HOLDING': {
        'UPSTREAM': [
            '자금 조달', '투자 유치'
        ],
        'MIDSTREAM': [
            '투자', '경영', '관리', '지배구조',
            '운영', '관리'
        ],
        'DOWNSTREAM': [
            '배당', '수익', '가치 창출',
            '수익', '배당'
        ]
    },
    
    # [Utility] - 2개
    'SEC_UTIL': {
        'UPSTREAM': [
            '연료', '원자력', '가스', '원재료'
        ],
        'MIDSTREAM': [
            '발전', '송전', '배전', '공급',
            '생산', '공급'
        ],
        'DOWNSTREAM': [
            '전력 판매', '고객', '서비스',
            '판매', '서비스'
        ]
    },
    'SEC_TELECOM': {
        'UPSTREAM': [
            '장비', '인프라', '네트워크'
        ],
        'MIDSTREAM': [
            '통신망 구축', '운영', '관리',
            '구축', '운영'
        ],
        'DOWNSTREAM': [
            '통신 서비스', '고객', '요금',
            '서비스', '고객'
        ]
    }
}


# ============================================================================
# Rule-based 밸류체인 분류 (기존 로직 개선)
# ============================================================================

# 🆕 P1-1: Revenue Segment → Value Chain 매핑
REVENUE_TO_VALUE_CHAIN_MAP = {
    # 배터리 관련
    '배터리': 'VC_BATTERY_MATERIALS',
    '배터리재료': 'VC_BATTERY_MATERIALS',
    '양극재': 'VC_BATTERY_MATERIALS',
    '음극재': 'VC_BATTERY_MATERIALS',
    '전해액': 'VC_BATTERY_MATERIALS',
    '분리막': 'VC_BATTERY_MATERIALS',
    '셀': 'VC_BATTERY_MIDSTREAM',
    '배터리팩': 'VC_BATTERY_MIDSTREAM',
    '배터리시스템': 'VC_BATTERY_DOWNSTREAM',
    
    # 재활용 관련
    '재활용': 'VC_BATTERY_RECYCLING',
    '리사이클링': 'VC_BATTERY_RECYCLING',
    '폐배터리': 'VC_BATTERY_RECYCLING',
    
    # 동박 관련
    '동박': 'VC_BATTERY_MATERIALS',
    '구리박': 'VC_BATTERY_MATERIALS',
    
    # 반도체 관련
    '반도체': 'VC_SEMI_MIDSTREAM',
    '웨이퍼': 'VC_SEMI_UPSTREAM',
    '패키징': 'VC_SEMI_MIDSTREAM',
    
    # 화학 관련
    '화학': 'VC_CHEMICAL_MIDSTREAM',
    '석유화학': 'VC_CHEMICAL_MIDSTREAM',
    '정유': 'VC_CHEMICAL_MIDSTREAM',
    
    # 철강 관련
    '철강': 'VC_STEEL_MIDSTREAM',
    '제철': 'VC_STEEL_MIDSTREAM',
    
    # 건설 관련
    '건설': 'VC_CONST_DOWNSTREAM',
    '공사': 'VC_CONST_DOWNSTREAM',
    
    # 유통 관련
    '유통': 'VC_DIST_DOWNSTREAM',
    '판매': 'VC_DIST_DOWNSTREAM',
    '소매': 'VC_DIST_DOWNSTREAM',
}

def classify_value_chain_rule_based(
    company_detail: CompanyDetail,
    sector: str,
    company_name: Optional[str] = None,
    sector_l2: Optional[str] = None,
    driver_tags: Optional[List[str]] = None
) -> Tuple[Optional[str], float, List[Dict[str, Any]]]:
    """
    Rule-based 밸류체인 분류 (Confidence 추가, L2 및 Driver Tags 정보 활용)
    
    Args:
        company_detail: CompanyDetail 객체
        sector: 섹터 코드
        company_name: 회사명 (선택)
        sector_l2: L2 섹터 코드 (선택, 예: 'DISTRIBUTION', 'MANUFACTURING')
        driver_tags: Driver Tags 리스트 (선택, 예: ['IMPORT_DEPENDENT', 'EXPORT_DRIVEN'])
    
    Returns:
        (value_chain, confidence_score, vc_candidates)
        - value_chain: 'UPSTREAM', 'MIDSTREAM', 'DOWNSTREAM' 또는 None
        - confidence_score: 0.0 ~ 1.0
        - vc_candidates: [{'value_chain': 'UPSTREAM', 'weight': 0.8, 'confidence': 'HIGH', 'evidence': [...], 'source': 'revenue_segment'}, ...]
    """
    if not company_detail:
        return None, 0.0, []
    
    # 텍스트 수집
    text_parts = []
    if company_detail.biz_summary:
        text_parts.append(company_detail.biz_summary.lower())
    if company_detail.products:
        products_text = ' '.join([str(p) for p in company_detail.products]).lower()
        text_parts.append(products_text)
    if company_detail.keywords:
        keywords_text = ' '.join([str(k) for k in company_detail.keywords]).lower()
        text_parts.append(keywords_text)
    if company_detail.clients:
        if isinstance(company_detail.clients, list):
            clients_text = ' '.join([str(c) for c in company_detail.clients if c]).lower()
        else:
            clients_text = str(company_detail.clients).lower()
        text_parts.append(clients_text)
    if company_detail.supply_chain:
        if isinstance(company_detail.supply_chain, list):
            supply_chain_text = ' '.join([
                f"{item.get('item','')} {item.get('supplier','')}".strip()
                if isinstance(item, dict) else str(item)
                for item in company_detail.supply_chain if item
            ]).lower()
        else:
            supply_chain_text = str(company_detail.supply_chain).lower()
        if supply_chain_text:
            text_parts.append(supply_chain_text)
    if company_detail.raw_materials:
        raw_materials_text = ' '.join([str(m) for m in company_detail.raw_materials if m]).lower()
        text_parts.append(raw_materials_text)
    # 🆕 P1-1: revenue_by_segment를 텍스트뿐만 아니라 구조화된 분석에도 사용
    revenue_segments_for_vc = {}  # 밸류체인 매핑용
    revenue_vc_candidates = []  # Revenue 기반 밸류체인 후보
    if company_detail.revenue_by_segment:
        if isinstance(company_detail.revenue_by_segment, dict):
            revenue_text = ' '.join([
                f"{segment}:{value}" for segment, value in company_detail.revenue_by_segment.items()
            ]).lower()
            revenue_segments_for_vc = company_detail.revenue_by_segment
            
            # 🆕 P1-1: Revenue Segment → Value Chain 매핑
            for segment_name, pct in revenue_segments_for_vc.items():
                segment_lower = segment_name.lower().strip()
                # REVENUE_TO_VALUE_CHAIN_MAP에서 매칭
                for revenue_key, vc_code in REVENUE_TO_VALUE_CHAIN_MAP.items():
                    if revenue_key in segment_lower:
                        # VC 코드를 UPSTREAM/MIDSTREAM/DOWNSTREAM으로 변환
                        if 'UPSTREAM' in vc_code or 'RECYCLING' in vc_code:
                            vc_type = 'UPSTREAM'
                        elif 'DOWNSTREAM' in vc_code:
                            vc_type = 'DOWNSTREAM'
                        else:
                            vc_type = 'MIDSTREAM'
                        
                        revenue_vc_candidates.append({
                            'vc': vc_type,
                            'pct': pct,
                            'evidence': f"revenue_segment: {segment_name} ({pct}%)"
                        })
                        break
        else:
            revenue_text = str(company_detail.revenue_by_segment).lower()
        text_parts.append(revenue_text)
    if company_name:
        text_parts.append(company_name.lower())
    
    combined_text = ' '.join(text_parts)
    
    # 섹터별 특화 키워드 사용 (있으면)
    if sector in SECTOR_SPECIFIC_VALUE_CHAIN_KEYWORDS:
        vc_keywords_map = SECTOR_SPECIFIC_VALUE_CHAIN_KEYWORDS[sector]
    else:
        # 일반 키워드 사용
        vc_keywords_map = {
            'UPSTREAM': VALUE_CHAIN_KEYWORDS['UPSTREAM'],
            'MIDSTREAM': VALUE_CHAIN_KEYWORDS['MIDSTREAM'],
            'DOWNSTREAM': VALUE_CHAIN_KEYWORDS['DOWNSTREAM']
        }
    
    # 각 밸류체인 위치별 점수 계산
    vc_scores = {}
    for vc_type, vc_keywords in vc_keywords_map.items():
        score = 0
        matched_keywords = []
        
        for keyword in vc_keywords:
            if keyword.lower() in combined_text:
                score += 2
                matched_keywords.append(keyword)
        
        # 제품 필드에서 추가 매칭
        if company_detail.products:
            for product in company_detail.products:
                product_lower = str(product).lower()
                for keyword in vc_keywords:
                    if keyword.lower() in product_lower:
                        score += 3
                        matched_keywords.append(keyword)
                        break
        
        if score > 0:
            vc_scores[vc_type] = {
                'score': score,
                'matched_keywords': matched_keywords
            }
    
    if not vc_scores:
        return None, 0.0, []
    
    # 최고 점수 밸류체인 선택
    best_vc = max(vc_scores.items(), key=lambda x: x[1]['score'])
    value_chain = best_vc[0]
    score = best_vc[1]['score']
    matched_keywords = best_vc[1]['matched_keywords']
    
    # Confidence 계산 (개선: 더 정확한 confidence 계산)
    total_keywords = len(vc_keywords_map[value_chain])
    matched_keywords_unique = len(set(matched_keywords))
    matched_ratio = matched_keywords_unique / total_keywords if total_keywords > 0 else 0.0
    
    # 점수 기반 confidence (키워드 매칭 점수에 따라)
    score_based_confidence = min(score / (total_keywords * 3) * 2, 1.0)  # 최대 점수: 키워드당 3점
    
    # 문맥 보너스 (확장)
    context_bonus = 0.0
    if company_detail.biz_summary:
        summary_lower = company_detail.biz_summary.lower()
        if value_chain == 'UPSTREAM':
            upstream_phrases = [
                '원재료를', '부품을', '소재를', '조달', '구매', '수입',
                '원료', '물자', '자재', '공급받', '도입'
            ]
            if any(phrase in summary_lower for phrase in upstream_phrases):
                context_bonus = 0.25
        elif value_chain == 'MIDSTREAM':
            midstream_phrases = [
                '제조하고', '생산하고', '가공하여', '조립', '가공', '제작',
                '생산', '제조', '가공', '조립', '공정', '제작'
            ]
            if any(phrase in summary_lower for phrase in midstream_phrases):
                context_bonus = 0.25
        elif value_chain == 'DOWNSTREAM':
            downstream_phrases = [
                '판매하고', '고객에게', '납품하여', '유통', '판매', '공급',
                '납품', '출고', '전달', '인도', '서비스', '운영'
            ]
            if any(phrase in summary_lower for phrase in downstream_phrases):
                context_bonus = 0.25
    
    # 제품명에서도 추가 검색 (보너스)
    product_bonus = 0.0
    if company_detail.products and matched_keywords_unique >= 1:
        product_bonus = 0.1  # 제품에서 키워드 매칭 시 보너스
    
    # ⭐ L2 기반 힌트 추가
    l2_bonus = 0.0
    if sector_l2:
        if sector_l2 == 'DISTRIBUTION':
            if value_chain == 'DOWNSTREAM':
                l2_bonus = 0.2
            elif value_chain == 'MIDSTREAM':
                l2_bonus = 0.1
        elif sector_l2 == 'MANUFACTURING':
            if value_chain == 'MIDSTREAM':
                l2_bonus = 0.2
            elif value_chain == 'UPSTREAM':
                l2_bonus = 0.1
        elif sector_l2 in ['PG', 'PLATFORM']:
            if value_chain in ['MIDSTREAM', 'DOWNSTREAM']:
                l2_bonus = 0.15
    
    # ⭐ Driver Tags 기반 힌트 추가
    driver_tag_bonus = 0.0
    if driver_tags:
        for tag in driver_tags:
            if tag == 'IMPORT_DEPENDENT':
                if value_chain == 'UPSTREAM':
                    driver_tag_bonus = max(driver_tag_bonus, 0.2)
            elif tag == 'EXPORT_DRIVEN':
                if value_chain == 'DOWNSTREAM':
                    driver_tag_bonus = max(driver_tag_bonus, 0.2)
            elif tag == 'DISTRIBUTION':
                if value_chain == 'DOWNSTREAM':
                    driver_tag_bonus = max(driver_tag_bonus, 0.15)
            elif tag == 'MANUFACTURING':
                if value_chain == 'MIDSTREAM':
                    driver_tag_bonus = max(driver_tag_bonus, 0.15)
            elif tag == 'PLATFORM_BIZ':
                if value_chain in ['MIDSTREAM', 'DOWNSTREAM']:
                    driver_tag_bonus = max(driver_tag_bonus, 0.1)
            elif tag == 'RECURRING_REVENUE':
                if value_chain == 'DOWNSTREAM':
                    driver_tag_bonus = max(driver_tag_bonus, 0.1)
    
    # 최종 confidence: 매칭 비율, 점수, 문맥, 제품 보너스, L2 보너스, Driver Tags 보너스 조합
    confidence = min(
        (matched_ratio * 0.35 + score_based_confidence * 0.25 + context_bonus + product_bonus + l2_bonus + driver_tag_bonus),
        1.0
    )
    
    # 최소 confidence 보장 (키워드 매칭이 하나라도 있으면 최소 0.15)
    if matched_keywords_unique >= 1:
        confidence = max(confidence, 0.15)
    
    logger.debug(
        f"Rule-based 밸류체인 분류: {value_chain} "
        f"(confidence={confidence:.2f}, score={score}, "
        f"matched={len(matched_keywords)}/{total_keywords})"
    )
    
    # 🆕 P1-1: Revenue Segment 기반 밸류체인 후보 추가
    if revenue_vc_candidates:
        # revenue 기반 밸류체인을 점수에 반영
        for candidate in revenue_vc_candidates:
            vc_code = candidate['vc']
            pct = candidate['pct']
            # revenue 비중을 점수로 변환 (최대 0.3 가산)
            revenue_score = min(0.3, pct / 100.0 * 0.5)
            
            # VC 코드를 UPSTREAM/MIDSTREAM/DOWNSTREAM으로 변환
            if 'UPSTREAM' in vc_code or 'RECYCLING' in vc_code:
                vc_type = 'UPSTREAM'
            elif 'DOWNSTREAM' in vc_code:
                vc_type = 'DOWNSTREAM'
            else:
                vc_type = 'MIDSTREAM'
            
            if vc_type not in vc_scores:
                vc_scores[vc_type] = {'score': 0, 'matched_keywords': []}
            vc_scores[vc_type]['score'] += revenue_score * 10  # 점수 스케일 조정
    
    # 🆕 P1-3: Value Chain 후보 저장 (밸류체인 점수 기반)
    vc_candidates = []
    if vc_scores:
        sorted_vc = sorted(vc_scores.items(), key=lambda x: x[1]['score'], reverse=True)
        for vc_type, vc_data in sorted_vc:
            score = vc_data['score']
            matched_keywords = vc_data.get('matched_keywords', [])
            evidence_list = []
            # 키워드 기반 근거
            if matched_keywords:
                evidence_list.append(f"keywords: {', '.join(matched_keywords[:3])}")
            # Revenue 기반 근거
            revenue_evidence = [c['evidence'] for c in revenue_vc_candidates if vc_type in c['vc']]
            if revenue_evidence:
                evidence_list.extend(revenue_evidence[:2])
            
            # 점수를 weight로 변환 (0.0 ~ 1.0)
            weight = min(1.0, score / 20.0)  # 최대 점수 20 기준
            
            vc_candidates.append({
                'value_chain': vc_type,
                'weight': weight,
                'confidence': 'HIGH' if weight >= 0.5 else ('MEDIUM' if weight >= 0.3 else 'LOW'),
                'evidence': evidence_list,
                'source': 'revenue_segment' if revenue_evidence else 'keywords'
            })
    
    # 최종 밸류체인 결정 (기존 로직 유지)
    if not vc_scores:
        return None, 0.0, []
    
    best_vc = max(vc_scores.items(), key=lambda x: x[1]['score'])
    value_chain = best_vc[0]
    score = best_vc[1]['score']
    matched_keywords = best_vc[1]['matched_keywords']
    
    # Confidence 재계산 (revenue 반영 후)
    total_keywords = len(vc_keywords_map[value_chain])
    matched_keywords_unique = len(set(matched_keywords))
    matched_ratio = matched_keywords_unique / total_keywords if total_keywords > 0 else 0.0
    score_based_confidence = min(score / (total_keywords * 3) * 2, 1.0)
    
    # 최종 confidence
    confidence = min(
        (matched_ratio * 0.35 + score_based_confidence * 0.25 + context_bonus + product_bonus + l2_bonus + driver_tag_bonus),
        1.0
    )
    
    if matched_keywords_unique >= 1:
        confidence = max(confidence, 0.15)
    
    return value_chain, confidence, vc_candidates


# ============================================================================
# 하이브리드 밸류체인 분류 (기존 + 신규)
# ============================================================================

def classify_value_chain_hybrid(
    company_detail: CompanyDetail,
    sector: str,
    company_name: Optional[str] = None,
    use_ensemble: bool = True,
    use_gpt: bool = True,
    sector_l2: Optional[str] = None,
    driver_tags: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """
    하이브리드 밸류체인 분류
    
    Step 1: Rule-based (기존 방법)
    Step 2: Confidence 확인
        - HIGH (>0.85) → 즉시 반환 (빠름)
        - LOW/MEDIUM → Ensemble 실행 (정확도)
    
    Args:
        company_detail: CompanyDetail 객체
        sector: 섹터 코드
        company_name: 회사명 (선택)
        use_ensemble: Ensemble 사용 여부 (기본값: True)
    
    Returns:
        [
            {
                'value_chain': 'MIDSTREAM',
                'weight': 0.6,
                'confidence': 'HIGH',
                'method': 'RULE_BASED' or 'ENSEMBLE',
                'rule_score': 0.85,
                ...
            },
            ...
        ]
    """
    # Step 1: Rule-based (기존 방법, L2 및 Driver Tags 정보 활용)
    rule_vc, rule_conf, rule_candidates = classify_value_chain_rule_based(
        company_detail, sector, company_name, sector_l2, driver_tags
    )
    
    # Step 2: Confidence 확인
    if rule_conf > 0.85:
        # 기존 방법으로 충분 → 빠르게 반환
        logger.info(f"Rule-based HIGH confidence ({rule_conf:.2f}) → 즉시 반환")
        return [{
            'value_chain': rule_vc,
            'weight': 1.0,
            'confidence': 'HIGH',
            'method': 'RULE_BASED',
            'rule_score': rule_conf,
            'is_primary': True
        }]
    
    # Step 3: Ensemble 실행 (신규)
    if use_ensemble:
        logger.info(f"Rule-based confidence 낮음 ({rule_conf:.2f}) → Ensemble 실행")
        try:
            return classify_value_chain_ensemble(
                company_detail,
                sector,
                company_name,
                use_embedding=True,  # ⭐ GPU 사용 복원
                use_reranking=True,  # ⭐ GPU 사용 복원
                use_gpt=use_gpt
            )
        except Exception as e:
            logger.error(f"Ensemble 실행 실패, Rule 결과 반환: {e}", exc_info=True)
            # Ensemble 실패 시 Rule 결과 반환
            return [{
                'value_chain': rule_vc,
                'weight': 1.0,
                'confidence': 'MEDIUM' if rule_conf > 0.5 else 'LOW',
                'method': 'RULE_BASED',
                'rule_score': rule_conf,
                'is_primary': True
            }]
    else:
        # Ensemble 미사용 시 Rule 결과 반환
        return [{
            'value_chain': rule_vc,
            'weight': 1.0,
            'confidence': 'MEDIUM' if rule_conf > 0.5 else 'LOW',
            'method': 'RULE_BASED',
            'rule_score': rule_conf,
            'is_primary': True
        }]


# ============================================================================
# Ensemble 밸류체인 분류 (4단계 파이프라인)
# ============================================================================

def _prepare_company_text_for_vc(
    company_detail: CompanyDetail,
    company_name: Optional[str] = None
) -> str:
    """
    밸류체인 분류를 위한 회사 텍스트 준비
    
    Args:
        company_detail: CompanyDetail 객체
        company_name: 회사명
    
    Returns:
        결합된 텍스트
    """
    text_parts = []
    
    if company_name:
        text_parts.append(f"회사명: {company_name}")
    
    if company_detail.biz_summary:
        # biz_summary 전체 사용 (밸류체인 분석은 문맥이 중요)
        text_parts.append(f"사업 개요: {company_detail.biz_summary}")
    
    if company_detail.products:
        products_text = ', '.join([str(p) for p in company_detail.products[:20]])
        text_parts.append(f"주요 제품: {products_text}")
    
    if company_detail.keywords:
        keywords_text = ', '.join([str(k) for k in company_detail.keywords[:20]])
        text_parts.append(f"키워드: {keywords_text}")
    
    if company_detail.clients:
        if isinstance(company_detail.clients, list):
            clients_text = ', '.join([str(c) for c in company_detail.clients[:20] if c])
        else:
            clients_text = str(company_detail.clients)
        if clients_text:
            text_parts.append(f"주요 고객: {clients_text}")
    
    if company_detail.supply_chain:
        if isinstance(company_detail.supply_chain, list):
            supply_chain_text = ', '.join([
                f"{item.get('item','')}/{item.get('supplier','')}".strip('/')
                if isinstance(item, dict) else str(item)
                for item in company_detail.supply_chain[:20] if item
            ])
        else:
            supply_chain_text = str(company_detail.supply_chain)
        if supply_chain_text:
            text_parts.append(f"공급망: {supply_chain_text}")
    
    if company_detail.raw_materials:
        raw_materials_text = ', '.join([str(m) for m in company_detail.raw_materials[:20] if m])
        if raw_materials_text:
            text_parts.append(f"원재료: {raw_materials_text}")
    
    if company_detail.revenue_by_segment:
        if isinstance(company_detail.revenue_by_segment, dict):
            revenue_text = ', '.join([
                f"{segment}:{value}" for segment, value in list(company_detail.revenue_by_segment.items())[:10]
            ])
        else:
            revenue_text = str(company_detail.revenue_by_segment)
        if revenue_text:
            text_parts.append(f"매출 비중: {revenue_text}")
    
    return ' '.join(text_parts)


def _trim_text(text: str, max_chars: int) -> str:
    text = str(text).strip()
    if len(text) > max_chars:
        return text[:max_chars].rstrip() + "..."
    return text


def _normalize_simple_list(values: Any, limit: int) -> List[str]:
    if not values:
        return []
    normalized = []
    if isinstance(values, list):
        iterable = values
    else:
        iterable = [values]
    for value in iterable:
        if value is None:
            continue
        text = str(value).strip()
        if not text:
            continue
        normalized.append(text)
        if len(normalized) >= limit:
            break
    return normalized


def _format_supply_chain_items(supply_chain: Any, limit: int) -> List[str]:
    if not supply_chain:
        return []
    formatted = []
    items = supply_chain if isinstance(supply_chain, list) else [supply_chain]
    for entry in items:
        if isinstance(entry, dict):
            item = entry.get('item') or entry.get('raw_material') or entry.get('material')
            supplier = entry.get('supplier') or entry.get('vendor') or entry.get('source')
            if item and supplier:
                text = f"{item} 공급: {supplier}"
            elif item:
                text = str(item)
            elif supplier:
                text = f"공급사: {supplier}"
            else:
                text = ''
        else:
            text = str(entry)
        text = text.strip()
        if not text:
            continue
        formatted.append(text)
        if len(formatted) >= limit:
            break
    return formatted


def get_value_chain_embedding_segments(
    company_detail: CompanyDetail,
    company_name: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    임베딩 모델 입력용 텍스트 세그먼트 생성 (필드별 제한 적용)
    """
    segments: List[Dict[str, Any]] = []

    def add_segment(label: str, text: str, weight: float, segment_type: str):
        trimmed = _trim_text(text, 500)
        if trimmed:
            segments.append({
                'text': f"{label}: {trimmed}" if label else trimmed,
                'weight': weight,
                'type': segment_type
            })

    if company_name:
        add_segment("회사명", company_name, 0.05, "company")

    if company_detail.biz_summary:
        summary = truncate_to_sentences(
            company_detail.biz_summary,
            max_chars=500,
            prefer_paragraphs=True
        )
        add_segment("사업 개요", summary, 0.35, "summary")

    products = _normalize_simple_list(company_detail.products, limit=5)
    if products:
        add_segment("주요 제품", ', '.join(products), 0.15, "products")

    clients = _normalize_simple_list(company_detail.clients, limit=10)
    if clients:
        add_segment("주요 고객", '; '.join(clients), 0.2, "clients")

    supply_chain_entries = _format_supply_chain_items(company_detail.supply_chain, limit=8)
    if supply_chain_entries:
        add_segment("공급망", '; '.join(supply_chain_entries), 0.22, "supply_chain")

    raw_materials = _normalize_simple_list(company_detail.raw_materials, limit=8)
    if raw_materials:
        add_segment("원재료", ', '.join(raw_materials), 0.2, "raw_materials")

    revenue_text = ""
    if company_detail.revenue_by_segment:
        if isinstance(company_detail.revenue_by_segment, dict):
            items = list(company_detail.revenue_by_segment.items())
            try:
                items.sort(key=lambda x: float(x[1]), reverse=True)
            except Exception:
                pass
            top_items = items[:5]
            revenue_text = ', '.join([f"{seg}:{val}" for seg, val in top_items])
        else:
            revenue_text = str(company_detail.revenue_by_segment)
    if revenue_text:
        add_segment("매출 비중", revenue_text, 0.2, "revenue")

    return segments


def classify_value_chain_ensemble(
    company_detail: CompanyDetail,
    sector_code: str,
    company_name: Optional[str] = None,
    db: Optional[Session] = None,
    llm_handler: Optional[LLMHandler] = None,
    use_embedding: bool = True,  # ⭐ 추가
    use_reranking: bool = True,
    use_gpt: bool = True,
    sector_l2: Optional[str] = None,
    driver_tags: Optional[List[str]] = None
) -> List[Dict[str, Any]]:
    """
    4단계 멀티 모델 앙상블 밸류체인 분류
    
    Step 1: Rule-based (Confidence HIGH면 즉시 반환)
    Step 2: 임베딩 모델 Embedding (Top-3 후보)
    Step 3: BGE-M3 Re-ranking (Top-3 → Top-2, 선택적)
    Step 4: GPT 최종 검증 (Top-2 → 최종 1~3개, 선택적)
    
    Args:
        company_detail: CompanyDetail 객체
        sector_code: 섹터 코드
        company_name: 회사명 (선택)
        db: DB 세션 (선택)
        llm_handler: LLMHandler 인스턴스 (GPT 사용 시 필요)
        use_reranking: BGE-M3 Re-ranking 사용 여부 (기본값: True)
        use_gpt: GPT 최종 검증 사용 여부 (기본값: True)
    
    Returns:
        [
            {
                'value_chain': 'MIDSTREAM',
                'weight': 0.6,
                'confidence': 'HIGH',
                'method': 'ENSEMBLE',
                'rule_score': 0.85,
                'embedding_score': 0.82,
                'bge_score': 0.80,
                'gpt_score': 0.88,
                'reasoning': '...',
                'is_primary': True
            },
            ...
        ]
    """
    # Step 1: Rule-based (빠른 체크, L2 및 Driver Tags 정보 활용)
    rule_vc, rule_conf, rule_candidates = classify_value_chain_rule_based(
        company_detail, sector_code, company_name, sector_l2, driver_tags
    )
    
    # Rule confidence가 매우 높으면 즉시 반환
    if rule_conf > 0.90:
        logger.info(f"[{company_name}] Rule-based 매우 높은 confidence ({rule_conf:.2f}) → 즉시 반환")
        return [{
            'value_chain': rule_vc,
            'weight': 1.0,
            'confidence': 'HIGH',
            'method': 'RULE_BASED',
            'rule_score': rule_conf,
            'is_primary': True
        }]
    
    # Step 2: 임베딩 모델 기반 Embedding (후보 생성)
    company_text = _prepare_company_text_for_vc(company_detail, company_name)
    embedding_segments = get_value_chain_embedding_segments(company_detail, company_name)
    if not embedding_segments and company_text:
        embedding_segments = [{'text': company_text, 'weight': 1.0, 'type': 'combined'}]
    
    # 동적으로 모델 사용 가능 여부 확인 및 시도
    is_embedding_active = False
    classify_value_chain_embedding_func = None
    
    if use_embedding:
        if EMBEDDING_AVAILABLE:
            is_embedding_active = True
            # 이미 import된 함수 사용 시도
            try:
                # 전역 네임스페이스에서 함수 확인
                if 'classify_value_chain_embedding' in globals():
                    classify_value_chain_embedding_func = classify_value_chain_embedding
                else:
                    # 함수가 없으면 다시 import
                    from app.services.value_chain_classifier_embedding import classify_value_chain_embedding
                    classify_value_chain_embedding_func = classify_value_chain_embedding
            except (NameError, ImportError):
                # 함수가 정의되지 않은 경우 다시 import
                from app.services.value_chain_classifier_embedding import classify_value_chain_embedding
                classify_value_chain_embedding_func = classify_value_chain_embedding
        else:
            # 플래그가 False여도 실제로 모델을 로드할 수 있는지 시도
            try:
                from app.services.value_chain_classifier_embedding import get_embedding_model, classify_value_chain_embedding as classify_value_chain_embedding_func
                try:
                    model = get_embedding_model()
                    if model is not None:
                        is_embedding_active = True
                        logger.info(f"[{company_name}] Value Chain 분류용 임베딩 모델 동적 로드 성공")
                except Exception as model_error:
                    logger.debug(f"[{company_name}] 임베딩 모델 동적 로드 실패: {model_error}")
            except ImportError as import_error:
                logger.debug(f"[{company_name}] Value Chain 임베딩 모듈 import 실패: {import_error}")
    
    if not is_embedding_active or classify_value_chain_embedding_func is None:
        logger.warning(f"[{company_name}] 임베딩 모델 not available (available={EMBEDDING_AVAILABLE}). Falling back to Rule-based.")
        return [{
            'value_chain': rule_vc,
            'weight': 1.0,
            'confidence': 'MEDIUM' if rule_conf > 0.5 else 'LOW',
            'method': 'RULE_BASED',
            'rule_score': rule_conf,
            'is_primary': True
        }]
    
    try:
        total_segment_chars = sum(len(seg.get('text', '')) for seg in embedding_segments)
        logger.debug(
            f"[{company_name}] 임베딩 모델 밸류체인 분류 시도... "
            f"(segments={len(embedding_segments)}, text 길이={total_segment_chars}자)"
        )
        
        # 함수가 None이거나 정의되지 않은 경우 다시 import 시도
        if classify_value_chain_embedding_func is None:
            from app.services.value_chain_classifier_embedding import classify_value_chain_embedding
            classify_value_chain_embedding_func = classify_value_chain_embedding
        
        candidates = classify_value_chain_embedding_func(
            embedding_segments,
            sector_code,
            top_k=3,
            min_threshold=0.3  # 0.4 → 0.3으로 조정 (더 많은 후보 생성)
        )
        if candidates:
            logger.info(f"[{company_name}] ✅ 임베딩 모델 후보 생성 성공: {len(candidates)}개")
        else:
            logger.warning(f"[{company_name}] ⚠️ 임베딩 모델 후보 생성 실패 또는 후보 없음 (임계값 미달 또는 텍스트 품질 문제)")
    except Exception as e:
        logger.error(f"[{company_name}] 임베딩 모델 분류 실패: {e}", exc_info=True)
        candidates = []
    
    if not candidates:
        logger.warning(f"[{company_name}] 임베딩 모델 후보 없음. Rule 결과 반환.")
        return [{
            'value_chain': rule_vc,
            'weight': 1.0,
            'confidence': 'MEDIUM' if rule_conf > 0.5 else 'LOW',
            'method': 'RULE_BASED',
            'rule_score': rule_conf,
            'is_primary': True
        }]
    
    # Step 3: BGE-M3 Re-ranking (선택적)
    # 환경변수 DISABLE_BGE_RERANKER=1로 완전 비활성화 가능
    bge_disabled_by_env = os.environ.get('DISABLE_BGE_RERANKER', '0') == '1'
    if use_reranking and BGE_AVAILABLE and not bge_disabled_by_env:
        try:
            reranked_candidates = rerank_value_chain_candidates(
                company_text,
                sector_code,
                candidates,
                top_k=2
            )
            if reranked_candidates:
                candidates = reranked_candidates
                logger.debug(f"BGE-M3 Re-ranking 완료: {len(candidates)}개 후보")
        except Exception as e:
            logger.warning(f"BGE-M3 Re-ranking 실패, 원본 후보 사용: {e}")
    else:
        # Re-ranking 미사용 시 Top-2만 선택
        candidates = candidates[:2]
    
    # Step 4: GPT 최종 검증 (선택적, 조건부 사용)
    # 조건부 GPT 호출: Rule confidence에 따라 결정
    # - Rule confidence > 0.85: GPT 스킵 (비용 절감)
    # - Rule confidence 0.70-0.85: BGE-M3만 사용 (GPT 스킵)
    # - Rule confidence < 0.70: GPT 사용 (정확도 향상)
    should_use_gpt = use_gpt and GPT_AVAILABLE
    if should_use_gpt:
        if rule_conf > 0.85:
            logger.info(f"[{company_name}] Rule confidence 높음 ({rule_conf:.2f}) → GPT 스킵 (비용 절감)")
            should_use_gpt = False
        elif rule_conf > 0.70:
            logger.info(f"[{company_name}] Rule confidence 중간 ({rule_conf:.2f}) → GPT 스킵 (BGE-M3만 사용)")
            should_use_gpt = False
        else:
            logger.info(f"[{company_name}] Rule confidence 낮음 ({rule_conf:.2f}) → GPT 사용 (정확도 향상)")
    
    if should_use_gpt:
        # llm_handler가 없으면 자동 생성
        if not llm_handler:
            try:
                # OpenAI API 키를 명시적으로 string으로 가져오기
                api_key = os.getenv('OPENAI_API_KEY')
                if api_key and callable(api_key):
                    try:
                        api_key = api_key()
                    except Exception as key_error:
                        logger.warning(f"API key callable 호출 실패: {key_error}, 환경변수에서 직접 가져오기")
                        api_key = os.getenv('OPENAI_API_KEY')
                
                if api_key:
                    llm_handler = LLMHandler(api_key=str(api_key))  # string으로 명시적 변환
                else:
                    llm_handler = LLMHandler()  # 환경변수에서 자동 가져오기
            except Exception as e:
                logger.warning(f"LLMHandler 생성 실패, GPT 검증 스킵: {e}")
                llm_handler = None
        
        if llm_handler:
            try:
                validated_results = validate_value_chain_with_gpt(
                    company_text,
                    sector_code,
                    company_name,
                    candidates,
                    llm_handler,
                    max_positions=3
                )
                if validated_results:
                    # Rule score 추가
                    for result in validated_results:
                        result['rule_score'] = rule_conf
                    logger.info(f"GPT 검증 완료: {len(validated_results)}개 위치")
                    return validated_results
            except Exception as e:
                logger.warning(f"GPT 검증 실패, 후보 결과 사용: {e}")
    
    # GPT 미사용 또는 실패 시 후보 결과 반환
    results = []
    total_score = sum(c.get('similarity', 0.0) or c.get('score', 0.0) for c in candidates)
    
    # ⭐ Phase 2: value_chain_confidence 및 혼합 비율 계산
    vc_confidence = None
    vc_mix = {}
    is_hybrid = False
    
    if len(candidates) >= 2:
        top1_score = candidates[0].get('similarity', 0.0) or candidates[0].get('score', 0.0)
        top2_score = candidates[1].get('similarity', 0.0) or candidates[1].get('score', 0.0)
        # ⭐ 음수 방지: max(0.0, top1 - top2)
        vc_confidence = max(0.0, top1_score - top2_score)
        is_hybrid = vc_confidence < 0.1  # gap < 0.1이면 Hybrid
        
        # 혼합 비율 계산
        if is_hybrid:
            total = top1_score + top2_score
            if total > 0:
                vc_mix[candidates[0].get('value_chain', 'MIDSTREAM')] = top1_score / total
                vc_mix[candidates[1].get('value_chain', 'MIDSTREAM')] = top2_score / total
    elif len(candidates) == 1:
        vc_confidence = 1.0  # 후보가 1개면 confidence 최대
        is_hybrid = False
    
    for i, candidate in enumerate(candidates):
        score = candidate.get('similarity', 0.0) or candidate.get('score', 0.0)
        weight = score / total_score if total_score > 0 else 1.0 / len(candidates)
        vc_code = candidate.get('value_chain', 'MIDSTREAM')
        
        result = {
            'value_chain': vc_code,  # 하위 호환성 유지
            'weight': float(weight),
            'confidence': candidate.get('confidence', 'MEDIUM'),
            'method': 'ENSEMBLE',
            'rule_score': rule_conf,
            'embedding_score': score,
            'bge_score': candidate.get('bge_score', None),
            'is_primary': (i == 0),
            'reasoning': f"임베딩 유사도 기반 분류 (similarity={score:.2f})"
        }
        
        # ⭐ Phase 2: 새로운 5단계 밸류체인 필드 추가 (단순화 버전)
        if i == 0:  # Primary 결과에만 추가
            result['value_chain'] = vc_code  # top1
            result['value_chain_confidence'] = vc_confidence
            if is_hybrid and len(candidates) >= 2:
                result['value_chain_detail'] = candidates[1].get('value_chain', 'MIDSTREAM')  # top2 (gap < 0.1일 때만)
            else:
                result['value_chain_detail'] = None
            result['is_hybrid'] = is_hybrid
        else:
            # Secondary 결과는 기존 value_chain만 유지
            result['value_chain'] = vc_code
        
        results.append(result)
    
    return results

