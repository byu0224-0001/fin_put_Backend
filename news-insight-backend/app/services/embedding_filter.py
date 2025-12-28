"""
Embedding Filter Service

BGE-M3 임베딩 모델을 사용한 시맨틱 필터링 및 청크 선택
"""
import logging
import re
import numpy as np
import torch
from typing import Optional, List, Dict, Any
from functools import lru_cache
from threading import Lock
from collections import defaultdict

try:
    from FlagEmbedding import BGEM3FlagModel
except ImportError:
    BGEM3FlagModel = None

try:
    from langchain_text_splitters import MarkdownHeaderTextSplitter
except ImportError:
    MarkdownHeaderTextSplitter = None

logger = logging.getLogger(__name__)

# 설정
USE_EMBEDDING_FILTER = True
EMBEDDING_MODEL_NAME = 'BAAI/bge-m3'
EMBEDDING_BATCH_SIZE = 24
EMBEDDING_TOP_K = 6
EMBEDDING_MIN_SIM = 0.28
HEADER_MIN_SIM = 0.45
HEADER_TOP_K = 8  # 6 → 8로 증가 (2차/3차 헤더 청크 더 많이 포함)

# 전역 변수
_embedding_model = None
_topic_vectors = None
_header_vectors = None
_embedding_lock = Lock()

# 주제 설명 및 헤더 타겟 (test_file.py에서 가져옴)
TOPIC_DESCRIPTIONS = {
    'products_services': [
        "이 회사는 어떤 제품이나 서비스를 어떤 고객에게 제공하며 경쟁 우위를 어떻게 설명하는지 언급한 문단",
        "주요 사업부별 제품 라인업과 판매 채널, 대표 고객 사례를 서술한 문단",
        "매출 비중, 품목별 매출, 내수 및 수출 구성, 주요 상표, 상품 및 용역 내역을 나열한 문단",
    ],
    'supply_chain': [
        "핵심 원재료나 부품, 조달처, 공급 리스크, 생산설비 현황을 설명한 문단",
        "원재료 매입액, 가격 변동 추이, 주요 매입처, 공급 계약, 가동률, 생산능력, CAPEX를 다룬 문단",
    ],
    'sales_orders': [
        "매출 구성, 수주 잔고, 지역 및 고객별 매출 비중 변화를 설명한 문단",
        "수주 잔고, 수주 상황, 판매 경로, 판매 방법 및 조건, 주요 매출처 비중을 나열한 문단",
    ],
    'strategy_outlook': [
        "경영진이 제시한 중장기 전략, 신사업 계획, 투자 계획, 성장 동력을 다룬 문단",
        "이사의 경영진단 및 분석의견, 재무상태 및 영업실적 분석, 시장 전망, 외부 변수 대응을 설명한 문단",
    ],
    'financial_summary': [
        "사업부문별 실적 요약, 손익 및 자산지표, 자본/유동성 관리 내용을 정리한 문단",
        "EBITDA, 영업이익률, CAPEX, 배당 정책, 현금흐름, 레버리지 비율 등 재무 요약을 기술한 문단"
    ],
    'risk_management': [
        "위험 관리 정책, 파생상품·헷지 전략, 환율 및 금리 민감도에 대해 설명한 문단",
        "시장위험관리, 신용위험, 유동성위험, 자본관리, 헷지 비율, 충당금 정책을 나열한 문단",
    ],
    'revenue_segment': [
        "사업부문별 매출 비중, 매출 구성, 부문별 실적을 나열한 문단",
        "각 사업부의 매출액과 비중을 설명한 표",
        "매출 및 수주상황, 주요 제품 매출, 제품별 매출실적 표",
        "제품군별 또는 사업부문별 매출액 및 매출 비중",
        # 🆕 금융사 전용 키워드 추가
        "영업의 현황, 영업의 종류, 사업부문별, 영업종류별",
        "부문정보, 영업부문, 부문별 손익, 당기손익, 영업이익, 세그먼트 정보, 연결부문",
        "이자이익, 수수료이익, 보험손익, 투자손익",
        # 🆕 보험사 전용 키워드 추가
        "보험료수익, 보험수익, 보험서비스수익, 보험금, 손해율, 사업비, 상품별, 종목별",
        "장기보험, 자동차보험, 일반보험, 생명보험, 손해보험",
    ],
}

HEADER_TARGETS = {
    'products_services': [
        "주요 제품 및 서비스, 영업의 현황, 매출 비중, 상품, 용역, 수수료 수익",
        "서비스 데이터, 플랫폼 지표, 사업부별 매출 구성",
    ],
    'supply_chain': [
        "원재료 및 생산설비, 매입, 조달, 자금조달 및 운용, 비용 구조",
        "후판 가격, 웨이퍼, 리튬, 양극재 등 공급망 관련 현황",
    ],
    'sales_orders': [
        "매출 및 수주상황, 판매 경로, 수주 잔고, 영업실적",
        "판매 계획, 인도 일정, 고객별 매출 비중",
    ],
    'strategy_outlook': [
        "이사의 경영진단 및 분석의견, 사업의 개요, 중점 추진 전략, 신규 사업 계획",
        "경영진이 언급한 시장 전망, 산업 환경, 성장 전략"
    ],
    'risk_management': [
        "위험관리, 시장위험, 파생상품 거래 현황, 우발채무, 제재 및 기타 위험",
        "환율, 유가, 금리 민감도 및 헷지 정책"
    ],
    'financial_summary': [
        "재무에 관한 사항, 자금 조달 및 운용, 재무상태 요약, 자본 관리",
        "부문별 실적 요약, 손익 및 자산 지표"
    ],
    'revenue_segment': [
        "매출 및 수주상황, 매출실적, 부문별 매출, 주요 제품 및 서비스",
        "제품별 매출, 사업부문별 실적, 매출 구성, 영업 현황",
        # 🆕 금융사 전용 키워드 추가
        "영업의 현황, 영업의 종류, 부문정보, 영업부문, 부문별 손익, 당기손익",
        "이자이익, 수수료이익, 보험손익, 투자손익, 세그먼트 정보, 연결부문",
        # 🆕 보험사 전용 키워드 추가
        "보험료수익, 보험수익, 보험서비스수익, 보험금, 손해율, 사업비",
        "상품별, 종목별, 장기보험, 자동차보험, 일반보험",
    ],
}


def ensure_embedding_model() -> bool:
    """임베딩 모델 로드 (전역 싱글톤)"""
    global _embedding_model, _topic_vectors, _header_vectors
    
    if not USE_EMBEDDING_FILTER:
        return False
    
    if BGEM3FlagModel is None:
        logger.warning("FlagEmbedding 모듈이 설치되어 있지 않습니다.")
        return False
    
    if _embedding_model is None:
        with _embedding_lock:
            if _embedding_model is None:
                logger.info("임베딩 모델 로드 중...")
                device = "cuda:0" if torch.cuda.is_available() else "cpu"
                use_fp16 = device != "cpu"
                try:
                    _embedding_model = BGEM3FlagModel(
                        EMBEDDING_MODEL_NAME,
                        use_fp16=use_fp16,
                        devices=device
                    )
                    logger.info(f"{device} 장치에 모델 로드 완료 (fp16={use_fp16})")
                except Exception as exc:
                    logger.error(f"임베딩 모델 로드 실패: {exc}")
                    return False
                
                # 벡터 빌드
                def build_vectors(source_dict):
                    entries = []
                    for key, sentences in source_dict.items():
                        for sentence in sentences:
                            entries.append((key, sentence))
                    if not entries:
                        return []
                    texts = [entry[1] for entry in entries]
                    encoded = _embedding_model.encode(texts, batch_size=len(texts), max_length=8192)
                    dense = encoded['dense_vecs']
                    vectors = []
                    for idx, vec in enumerate(dense):
                        norm = np.linalg.norm(vec)
                        normalized = vec if norm == 0 else vec / norm
                        vectors.append({'topic': entries[idx][0], 'vector': normalized})
                    return vectors
                
                _topic_vectors = build_vectors(TOPIC_DESCRIPTIONS)
                _header_vectors = build_vectors(HEADER_TARGETS)
                logger.info("임베딩 모델 및 주제 벡터 준비 완료")
    
    return True


def embed_texts(texts: List[str]) -> Optional[np.ndarray]:
    """텍스트 리스트를 임베딩으로 변환"""
    if not ensure_embedding_model():
        return None
    
    if not texts:
        return None
    
    encoded = _embedding_model.encode(
        texts,
        batch_size=min(EMBEDDING_BATCH_SIZE, len(texts)),
        max_length=8192
    )
    dense = encoded['dense_vecs']
    dense = np.array(dense)
    norms = np.linalg.norm(dense, axis=1, keepdims=True)
    norms[norms == 0] = 1e-9
    return dense / norms


def split_markdown_into_chunks(markdown_text: str) -> List[Dict[str, Any]]:
    """
    헤더 기반으로 마크다운을 청크 단위로 분리 (계층 구조 보존)
    
    LangChain MarkdownHeaderTextSplitter 사용:
    - 계층 구조 메타데이터 보존 (Header_1 > Header_2 > Header_3)
    - DART 보고서의 복잡한 구조 처리
    - 검증된 라이브러리로 유지보수성 향상
    """
    if not markdown_text or not markdown_text.strip():
        return []
    
    # Fallback: LangChain이 없으면 기존 로직 사용
    if MarkdownHeaderTextSplitter is None:
        logger.warning("MarkdownHeaderTextSplitter가 없습니다. 기본 파싱 사용.")
        return _split_markdown_simple(markdown_text)
    
    try:
        # DART 보고서 구조에 맞춘 헤더 레벨 설정
        headers_to_split_on = [
            ("#", "Header_1"),      # 대분류 (예: II. 사업의 내용)
            ("##", "Header_2"),     # 중분류 (예: 2. 주요 제품 및 서비스)
            ("###", "Header_3"),    # 소분류 (예: 가. 주요 제품 현황)
        ]
        
        splitter = MarkdownHeaderTextSplitter(
            headers_to_split_on=headers_to_split_on,
            strip_headers=False  # 헤더 텍스트를 본문에도 유지 (문맥 보존)
        )
        
        # 마크다운 분리
        docs = splitter.split_text(markdown_text)
        
        chunks = []
        for doc in docs:
            # 메타데이터에서 계층 경로 생성
            metadata = doc.metadata
            header_path_parts = []
            
            # Header_1 > Header_2 > Header_3 순서로 경로 구성
            for level in ["Header_1", "Header_2", "Header_3"]:
                if level in metadata and metadata[level]:
                    header_path_parts.append(metadata[level].strip())
            
            # 경로 문자열 생성 (예: "II. 사업의 내용 > 2. 주요 제품")
            header_path = " > ".join(header_path_parts) if header_path_parts else "개요"
            
            # 최상위 헤더만 추출 (기존 'heading' 필드 호환성)
            top_heading = header_path_parts[0] if header_path_parts else "개요"
            
            # 텍스트 정리
            content = doc.page_content.strip()
            if not content:
                continue
            
            chunks.append({
                'heading': top_heading,          # 기존 호환성 (최상위 헤더만)
                'header_path': header_path,      # 계층 경로 (New!)
                'text': content,                 # 청크 내용
                'metadata': metadata,            # 전체 메타데이터 (New!)
                'full_text': f"[{header_path}]\n{content}"  # 임베딩용 (문맥 포함)
            })
        
        logger.debug(f"마크다운 청킹 완료: {len(chunks)}개 청크 생성")
        return chunks
        
    except Exception as e:
        logger.warning(f"MarkdownHeaderTextSplitter 실패, 기본 파싱 사용: {e}")
        return _split_markdown_simple(markdown_text)


def _split_markdown_simple(markdown_text: str) -> List[Dict[str, Any]]:
    """기본 마크다운 파싱 (Fallback)"""
    lines = markdown_text.split('\n')
    chunks = []
    current_heading = "개요"
    buffer = []
    
    for line in lines:
        stripped = line.strip()
        if stripped.startswith('#'):
            if buffer:
                chunk_text = '\n'.join(buffer).strip()
                if chunk_text:
                    chunks.append({
                        'heading': current_heading,
                        'header_path': current_heading,
                        'text': chunk_text,
                        'metadata': {'Header_1': current_heading},
                        'full_text': f"[{current_heading}]\n{chunk_text}"
                    })
                buffer = []
            current_heading = stripped
            buffer.append(line)
        else:
            buffer.append(line)
    
    if buffer:
        chunk_text = '\n'.join(buffer).strip()
        if chunk_text:
            chunks.append({
                'heading': current_heading,
                'header_path': current_heading,
                'text': chunk_text,
                'metadata': {'Header_1': current_heading},
                'full_text': f"[{current_heading}]\n{chunk_text}"
            })
    
    return chunks


def clean_heading_label(heading_line: str) -> str:
    """헤딩 라벨 정리"""
    if not heading_line:
        return "개요"
    label = heading_line.replace('#', '').strip()
    label = re.sub(r'^\d+[\.\)]\s*', '', label)
    label = re.sub(r'^[IVXLCDM]+\.\s*', '', label, flags=re.IGNORECASE)
    label = re.sub(r'^[가-힣]+\.\s*', '', label)
    return label.strip() or "개요"


def semantic_select_sections(markdown_text: str, ticker: Optional[str] = None) -> Optional[str]:
    """
    헤더 임베딩 기반으로 핵심 섹션 선별 (계층 구조 활용)
    
    개선사항:
    - header_path를 활용하여 더 정확한 문맥 인식
    - 계층 구조 메타데이터로 부모-자식 관계 파악
    """
    if not USE_EMBEDDING_FILTER or not markdown_text.strip():
        return None
    
    if not ensure_embedding_model() or not _header_vectors:
        return None
    
    chunks = split_markdown_into_chunks(markdown_text)
    if not chunks:
        return None
    
    # header_path를 우선적으로 사용 (계층 구조 정보 포함)
    heading_texts = []
    valid_chunks = []
    
    for chunk in chunks:
        # header_path가 있으면 우선 사용, 없으면 heading 사용
        heading = chunk.get('header_path') or chunk.get('heading', '')
        if heading:
            heading_texts.append(heading)
            valid_chunks.append(chunk)
    
    if not heading_texts:
        return None
    
    # 계층 경로 정보를 포함한 헤더 텍스트를 임베딩
    embeddings = embed_texts(heading_texts)
    if embeddings is None:
        return None
    
    scored = []
    for idx, emb in enumerate(embeddings):
        scores = [(entry['topic'], float(np.dot(emb, entry['vector']))) for entry in _header_vectors]
        if not scores:
            continue
        best_topic, best_score = max(scores, key=lambda x: x[1])
        if best_score >= HEADER_MIN_SIM:
            chunk = valid_chunks[idx]
            scored.append({
                'topic': best_topic,
                'score': best_score,
                'heading': chunk.get('header_path') or chunk.get('heading', ''),
                'text': chunk.get('text', ''),
                'metadata': chunk.get('metadata', {})
            })
    
    if not scored:
        logger.debug("헤더 시맨틱 라우팅 결과 없음, 백업 경로 사용")
        return None
    
    scored.sort(key=lambda x: x['score'], reverse=True)
    selected = scored[:HEADER_TOP_K]
    
    logger.info("헤더 기반 선택 결과 (계층 구조 활용):")
    for item in selected:
        preview = item['heading'][:80]
        logger.info(f"  • {preview} -> {item['topic']} (score={item['score']:.3f})")
    
    combined = '\n\n'.join(item['text'] for item in selected if item['text'].strip())
    return combined if combined.strip() else None


def select_relevant_chunks(markdown_text: str, ticker: Optional[str] = None) -> str:
    """
    임베딩 기반으로 핵심 청크 선별 (계층 구조 활용)
    
    개선사항:
    - full_text (문맥 포함)를 임베딩하여 더 정확한 의미 매칭
    - header_path 메타데이터로 출처 추적 가능
    """
    if not markdown_text or not markdown_text.strip():
        return ""
    
    # 헤더 기반 시맨틱 선택 시도 (계층 구조 활용)
    semantic_text = None
    if USE_EMBEDDING_FILTER:
        semantic_text = semantic_select_sections(markdown_text, ticker=ticker)
        if semantic_text and len(semantic_text) > 200:
            return semantic_text
    
    # 청크 기반 백업 선택
    chunks = split_markdown_into_chunks(markdown_text)
    if not chunks:
        return markdown_text
    
    # full_text 사용 (문맥 포함): "[II. 사업의 내용 > 2. 주요 제품]\n내용..."
    # 이렇게 하면 임베딩 시 문맥 정보가 함께 고려됨
    chunk_texts_for_embedding = [
        chunk.get('full_text', chunk.get('text', '')) for chunk in chunks
    ]
    embeddings = embed_texts(chunk_texts_for_embedding) if USE_EMBEDDING_FILTER else None
    
    if embeddings is None:
        return markdown_text
    
    scored_chunks = []
    topic_buckets = defaultdict(list)
    
    for idx, emb in enumerate(embeddings):
        topic_scores = [(entry['topic'], float(np.dot(emb, entry['vector']))) for entry in _topic_vectors]
        best_topic, best_score = max(topic_scores, key=lambda x: x[1])
        chunk = chunks[idx]
        chunk_info = {
            'heading': chunk.get('header_path') or chunk.get('heading', ''),
            'text': chunk.get('text', ''),
            'score': best_score,
            'topic': best_topic,
            'metadata': chunk.get('metadata', {})
        }
        scored_chunks.append(chunk_info)
        topic_buckets[best_topic].append(chunk_info)
    
    if not scored_chunks:
        return markdown_text
    
    scored_chunks.sort(key=lambda x: x['score'], reverse=True)
    filtered = [chunk for chunk in scored_chunks if chunk['score'] >= EMBEDDING_MIN_SIM]
    
    if not filtered:
        filtered = scored_chunks[:EMBEDDING_TOP_K]
    else:
        filtered = filtered[:EMBEDDING_TOP_K]
    
    selected_text = '\n\n'.join(chunk['text'] for chunk in filtered)
    return selected_text if selected_text.strip() else markdown_text

