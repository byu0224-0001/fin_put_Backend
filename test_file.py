# -*- coding: utf-8 -*-
# =============================================================================
# 0. 라이브러리 설치 및 초기화
# =============================================================================

import sys
import io
import hashlib

# Windows에서 UTF-8 출력 지원
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import OpenDartReader
import pandas as pd
from bs4 import BeautifulSoup
from markdownify import markdownify as md
from langchain_openai import ChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
import json
import re
import time
import os
import requests
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
import numpy as np
import torch
import importlib.metadata as importlib_metadata
from packaging import version as pkg_version
from functools import lru_cache
from threading import Lock
from collections import defaultdict

try:
    from transformers.utils import import_utils as hf_import_utils
except ImportError:
    hf_import_utils = None

# Hugging Face가 torch>=2.6 정식 버전을 요구하므로, nightly(dev) 빌드를 사용 중이면
# 버전 문자열을 2.6.0으로 보정해 안전장치를 우회한다.
if hasattr(torch, "__version__"):
    _torch_ver = torch.__version__
    if isinstance(_torch_ver, str) and "dev" in _torch_ver and _torch_ver.startswith("2.6.0"):
        torch.__version__ = "2.6.0"

def _allow_dev_torch_for_hf():
    if hf_import_utils is None or not hasattr(hf_import_utils, "is_torch_greater_or_equal"):
        return
    original_fn = hf_import_utils.is_torch_greater_or_equal

    def _is_dev_sufficient(target_version: str) -> bool:
        try:
            current_ver = importlib_metadata.version("torch")
        except importlib_metadata.PackageNotFoundError:
            return False
        if "dev" not in current_ver:
            return False
        try:
            base_ver = pkg_version.parse(current_ver).base_version
            return pkg_version.parse(base_ver) >= pkg_version.parse(target_version)
        except Exception:
            return False

    @lru_cache(maxsize=None)
    def _patched(library_version: str, accept_dev: bool = False) -> bool:
        if _is_dev_sufficient(library_version):
            return True
        return original_fn(library_version, accept_dev=accept_dev)

    hf_import_utils.is_torch_greater_or_equal = _patched

_allow_dev_torch_for_hf()

try:
    from FlagEmbedding import BGEM3FlagModel  # type: ignore
except ImportError:
    BGEM3FlagModel = None

# .env 파일에서 환경 변수 로드
load_dotenv()

# ==============================================================================
# 1. 설정 (API Key 입력)
# ==============================================================================
# .env 파일에서 API Key 로드
DART_API_KEY = os.getenv('DART_API_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# 객체 초기화
dart = OpenDartReader(DART_API_KEY)
# 분석용 LLM (gpt-5-mini)
llm = ChatOpenAI(model="gpt-5-mini", openai_api_key=OPENAI_API_KEY)
# 요약용 LLM (gpt-5-nano)
summary_llm = ChatOpenAI(model="gpt-5-nano", openai_api_key=OPENAI_API_KEY)

# 테스트 모드 플래그 (LLM 호출 비활성화용)
ENABLE_LLM = False

# 디버깅용: 특정 티커의 마크다운을 파일로 덤프
DEBUG_DUMP_TICKERS = {'005930','000660','009540', '207940', '010140'}  # 필요 시 다른 티커 추가

# 캐시 및 배치 처리 설정
CACHE_ENABLED = True
CACHE_DIR = "cache"
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '50'))
BATCH_PAUSE_SECONDS = float(os.getenv('BATCH_PAUSE_SECONDS', '2.0'))
MAX_WORKERS = int(os.getenv('MAX_WORKERS', '3'))
MAX_LLM_CHARS = int(os.getenv('MAX_LLM_CHARS', '50000'))

# 면책/주의 섹션 제거 관련 상수
DISALLOWED_HEADING_KEYWORDS = [
    "예측정보",
    "주의사항",
    "면책",
    "투자의사결정에필요한사항",
    "투자의사결정에필요한",
    "주의사항입니다",
]

ALLOW_HEADINGS_AFTER_SKIP = [
    "개요",
    "재무상태",
    "영업실적",
    "유동성",
    "부외거래",
    "중점추진전략",
    "경영진단",
    "시장전망",
    "사업전략",
    "핵심전략",
    "리스크관리",
]

TOXIC_CONTENT_KEYWORDS = [
    "예측정보",
    "미래사건",
    "보장할수없으며",
    "면책",
    "잠재적위험",
    "정정보고서를공시할의무",
]

DISCLAIMER_REGEXES = [
    r"예측정보[^#]+?정정보고서를\s*공시할\s*의무는\s*없습니다\.",
    r"본\s+자료는\s+미래에\s+대한\s+예측정보[^#]+?(?:않습니다|없습니다)\.",
]

FINAL_DISCLAIMER_KEYWORDS = ["예측정보", "주의사항", "정정보고서를", "면책"]

# ==============================================================================
# 1.5 임베딩 기반 필터링 설정
# ==============================================================================
USE_EMBEDDING_FILTER = True
EMBEDDING_MODEL_NAME = 'BAAI/bge-m3'
EMBEDDING_BATCH_SIZE = 24
EMBEDDING_TOP_K = 6
EMBEDDING_MIN_SIM = 0.28
HEADER_MIN_SIM = 0.45
HEADER_TOP_K = 6

TOPIC_DESCRIPTIONS = {
    'products_services': [
        "이 회사는 어떤 제품이나 서비스를 어떤 고객에게 제공하며 경쟁 우위를 어떻게 설명하는지 언급한 문단",
        "주요 사업부별 제품 라인업과 판매 채널, 대표 고객 사례를 서술한 문단",
        "매출 비중, 품목별 매출, 내수 및 수출 구성, 주요 상표, 상품 및 용역 내역을 나열한 문단",
        "반도체 메모리, 비메모리, 파운드리, 패키징 공정 및 주력 제품군 설명",
        "2차전지 양극재, 음극재, 분리막, 전해액, 셀, 모듈, 팩 제품 라인업",
        "자동차 전동화 모델(EV), 내연기관, PBV, 상용차 및 부품 포트폴리오",
        "조선 LNG선, 컨테이너선, VLCC, 친환경 선박, 해양플랜트 건조 실적",
        "바이오 의약품 파이프라인, 임상 단계, 라이선스 아웃, CDMO 서비스 현황",
        "인터넷 플랫폼 서비스, 광고, 커머스, 콘텐츠, 게임 IP 및 퍼블리싱 현황",
        "금융 예적금, 대출 상품, 신용카드, 보험, 증권 중개 및 IB 서비스",
        "건설 주택 분양, 토목 공사, 플랜트 수주, 개발사업 포트폴리오"
    ],
    'supply_chain': [
        "핵심 원재료나 부품, 조달처, 공급 리스크, 생산설비 현황을 설명한 문단",
        "원재료 매입액, 가격 변동 추이, 주요 매입처, 공급 계약, 가동률, 생산능력, CAPEX를 다룬 문단",
        "반도체 웨이퍼, 노광장비, 특수가스 조달 및 공급망 관리",
        "배터리 핵심광물(리튬, 니켈, 코발트) 조달 및 전구체 공급망",
        "자동차 차량용 반도체, 구동모터, 배터리 셀 수급 및 부품 협력사 관리",
        "조선 후판(강재), 선박 엔진, 기자재 조달 및 도크 가동률 현황",
        "건설 철근, 시멘트, 레미콘 등 주요 자재 가격 변동 및 수급 추이"
    ],
    'sales_orders': [
        "매출 구성, 수주 잔고, 지역 및 고객별 매출 비중 변화를 설명한 문단",
        "수주 잔고, 수주 상황, 판매 경로, 판매 방법 및 조건, 주요 매출처 비중을 나열한 문단",
        "수주 산업(조선, 건설, 방산)의 신규 수주, 수주 잔고, 인도 일정 및 진행률",
        "자동차 판매대수, 지역별 판매 실적, ASP(평균판매단가) 변동 추이",
        "플랫폼 MAU, DAU, ARPU, 유료 가입자 수, GMV(거래액) 등 핵심 지표",
        "금융 순이자마진(NIM), 예대율, 수수료 수익, 보험료 수익 추이"
    ],
    'research_development': [
        "연구개발(R&D) 조직 구성, 연구개발비 지출 추이 및 매출액 대비 비중",
        "주요 연구 실적, 신제품 개발 현황, 특허 및 지식재산권 보유 현황",
        "신약 후보물질 탐색, 임상시험 진행 경과, 신기술 상용화 로드맵",
        "자동차/배터리 차세대 플랫폼 개발, 고체 배터리, 자율주행 기술 연구"
    ],
    'strategy_outlook': [
        "경영진이 제시한 중장기 전략, 신사업 계획, 투자 계획, 성장 동력을 다룬 문단",
        "이사의 경영진단 및 분석의견, 재무상태 및 영업실적 분석, 시장 전망, 외부 변수 대응을 설명한 문단",
        "플랫폼/콘텐츠 기업의 신작 출시 일정, IP 확장, 글로벌 진출 전략, AI·클라우드 투자 계획을 다룬 문단"
    ],
    'financial_summary': [
        "사업부문별 실적 요약, 손익 및 자산지표, 자본/유동성 관리 내용을 정리한 문단",
        "순이자마진(NIM), 예대마진, 수수료 수익, 예상손실충당금, BIS 비율 등 금융지표를 나열한 문단",
        "EBITDA, 영업이익률, CAPEX, 배당 정책, 현금흐름, 레버리지 비율 등 재무 요약을 기술한 문단"
    ],
    'risk_management': [
        "위험 관리 정책, 파생상품·헷지 전략, 환율 및 금리 민감도에 대해 설명한 문단",
        "시장위험관리, 신용위험, 유동성위험, 자본관리, 헷지 비율, 충당금 정책을 나열한 문단",
        "이사의 경영진단 및 분석의견에서 재무상태와 영업실적을 분석하며 중요 위험요인을 언급한 문단"
    ]
}

HEADER_TARGETS = {
    'products_services': [
        "주요 제품 및 서비스", "영업의 현황", "매출 비중", "상품", "용역", "수수료 수익",
        "서비스 데이터", "플랫폼 지표", "사업부별 매출 구성",
        "차종 라인업", "플랫폼 이용지표", "콘텐츠", "게임 포트폴리오",
        "신탁", "대출", "예수금", "보험료"
    ],
    'supply_chain': [
        "원재료 및 생산설비", "매입", "조달", "자금조달 및 운용", "비용 구조",
        "후판", "웨이퍼", "리튬", "양극재", "공급망",
        "부품 조달", "도크 가동률", "기자재", "엔진 수급", "원재료 가격",
        "생산능력", "생산실적", "가동률"
    ],
    'sales_orders': [
        "매출 및 수주상황", "판매 경로", "수주 잔고", "영업실적",
        "판매 계획", "인도 일정", "고객별 매출 비중",
        "판매대수", "도·소매 판매 실적", "선가", "수주잔고", "발주 현황",
        "가입자", "이용자", "트래픽", "지표"
    ],
    'research_development': [
        "연구개발활동", "연구개발비", "연구개발실적", "지적재산권",
        "특허", "임상시험", "신약", "파이프라인", "기술제휴"
    ],
    'strategy_outlook': [
        "이사의 경영진단 및 분석의견", "사업의 개요", "중점 추진 전략", "신규 사업",
        "시장 전망", "산업 환경", "성장 전략", "경쟁 우위"
    ],
    'risk_management': [
        "위험관리", "시장위험", "파생상품", "우발채무", "제재",
        "환율", "유가", "금리", "리스크", "자본적정성"
    ],
    'financial_summary': [
        "재무에 관한 사항", "자금 조달 및 운용", "재무상태", "자본 관리",
        "요약 재무정보", "손익", "배당"
    ]
}

_embedding_model = None
_topic_vectors = None
_header_vectors = None
_embedding_lock = Lock()

def ensure_embedding_model():
    global _embedding_model, _topic_vectors, _header_vectors
    if not USE_EMBEDDING_FILTER:
        return False
    if BGEM3FlagModel is None:
        print("  -> [Embed] FlagEmbedding 모듈이 설치되어 있지 않습니다. `pip install FlagEmbedding` 이후 다시 시도해주세요.")
        return False
    if _embedding_model is None:
        with _embedding_lock:
            if _embedding_model is None:
                print("  -> [Embed] 임베딩 모델 로드 중... (이 작업은 1회만 수행됩니다)")
                device = "cuda:0" if torch.cuda.is_available() else "cpu"
                use_fp16 = device != "cpu"
                try:
                    _embedding_model = BGEM3FlagModel(
                        EMBEDDING_MODEL_NAME,
                        use_fp16=use_fp16,
                        devices=device
                    )
                    print(f"  -> [Embed] {device} 장치에 모델 로드 완료 (fp16={use_fp16})")
                except Exception as exc:
                    print(f"  -> [Error] 임베딩 모델 로드 실패: {exc}")
                    return False

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
                print("  -> [Embed] 임베딩 모델 및 주제 벡터 준비 완료")
    return True

def embed_texts(texts):
    """텍스트 리스트를 임베딩으로 변환"""
    if not ensure_embedding_model():
        return None
    encoded = _embedding_model.encode(texts, batch_size=min(EMBEDDING_BATCH_SIZE, len(texts)), max_length=8192)
    dense = encoded['dense_vecs']
    dense = np.array(dense)
    norms = np.linalg.norm(dense, axis=1, keepdims=True)
    norms[norms == 0] = 1e-9
    return dense / norms

def split_markdown_into_chunks(markdown_text):
    """헤더(#)를 기준으로 마크다운을 청크 단위로 분리"""
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
                    chunks.append({'heading': current_heading, 'text': chunk_text})
                buffer = []
            current_heading = stripped
            buffer.append(line)
        else:
            buffer.append(line)
    if buffer:
        chunk_text = '\n'.join(buffer).strip()
        if chunk_text:
            chunks.append({'heading': current_heading, 'text': chunk_text})
    return chunks

def clean_heading_label(heading_line):
    if not heading_line:
        return "개요"
    label = heading_line.replace('#', '').strip()
    label = re.sub(r'^\d+[\.\)]\s*', '', label)
    label = re.sub(r'^[IVXLCDM]+\.\s*', '', label, flags=re.IGNORECASE)
    label = re.sub(r'^[가-힣]+\.\s*', '', label)
    return label.strip() or "개요"

def semantic_select_sections(markdown_text, ticker=None):
    """헤더 임베딩 기반으로 핵심 섹션 선별"""
    if not USE_EMBEDDING_FILTER or not markdown_text.strip():
        return None
    if not ensure_embedding_model():
        return None
    if not _header_vectors:
        return None
    chunks = split_markdown_into_chunks(markdown_text)
    chunks = filter_disallowed_chunks(chunks)
    headings = []
    valid_chunks = []
    for chunk in chunks:
        heading = chunk.get('heading')
        if heading:
            headings.append(clean_heading_label(heading))
            valid_chunks.append(chunk)
    if not headings:
        return None
    embeddings = embed_texts(headings)
    if embeddings is None:
        return None
    scored = []
    for idx, emb in enumerate(embeddings):
        scores = [(entry['topic'], float(np.dot(emb, entry['vector']))) for entry in _header_vectors]
        if not scores:
            continue
        best_topic, best_score = max(scores, key=lambda x: x[1])
        if best_score >= HEADER_MIN_SIM:
            scored.append({
                'topic': best_topic,
                'score': best_score,
                'heading': headings[idx],
                'text': valid_chunks[idx]['text']
            })
    if not scored:
        print("  -> [Embed] 헤더 시맨틱 라우팅 결과 없음, 백업 경로 사용")
        return None
    scored.sort(key=lambda x: x['score'], reverse=True)
    selected = scored[:HEADER_TOP_K]
    print("  -> [Embed] 헤더 기반 선택 결과:")
    for item in selected:
        preview = item['heading'][:60]
        print(f"     • {preview} -> {item['topic']} (score={item['score']:.3f}, len={len(item['text'])}자)")
    combined = '\n\n'.join(item['text'] for item in selected if item['text'].strip())
    if ticker:
        print(f"  -> [Embed] {ticker} 헤더 기반 추출 길이: {len(combined)}자")
    return combined if combined.strip() else None

def select_relevant_chunks(markdown_text, ticker=None):
    """임베딩 기반으로 핵심 청크 선별"""
    if not markdown_text or not markdown_text.strip():
        return ""

    semantic_text = None
    if USE_EMBEDDING_FILTER:
        semantic_text = semantic_select_sections(markdown_text, ticker=ticker)
        if semantic_text and len(semantic_text) > 200:
            return semantic_text

    chunks = split_markdown_into_chunks(markdown_text)
    chunks = filter_disallowed_chunks(chunks)
    if not chunks:
        return markdown_text

    chunk_texts = [chunk['text'] for chunk in chunks]
    embeddings = embed_texts(chunk_texts) if USE_EMBEDDING_FILTER else None
    if embeddings is None:
        return markdown_text

    scored_chunks = []
    topic_buckets = defaultdict(list)
    for idx, emb in enumerate(embeddings):
        topic_scores = [(entry['topic'], float(np.dot(emb, entry['vector']))) for entry in _topic_vectors]
        best_topic, best_score = max(topic_scores, key=lambda x: x[1])
        chunk_info = {
            'heading': chunks[idx]['heading'],
            'text': chunks[idx]['text'],
            'score': best_score,
            'topic': best_topic
        }
        scored_chunks.append(chunk_info)
        topic_buckets[best_topic].append(chunk_info)

    if not scored_chunks:
        return markdown_text

    if topic_buckets:
        print("  -> [Embed] 주제별 후보 청크 요약:")
        for topic, items in topic_buckets.items():
            items.sort(key=lambda x: x['score'], reverse=True)
            total_chars = sum(len(it['text']) for it in items)
            top_preview = ", ".join(
                f"{clean_heading_label(it['heading'])[:25]}({it['score']:.3f})"
                for it in items[:3]
            )
            print(f"     • {topic}: {len(items)}개 / {total_chars}자 | 상위: {top_preview}")

    scored_chunks.sort(key=lambda x: x['score'], reverse=True)
    filtered = [chunk for chunk in scored_chunks if chunk['score'] >= EMBEDDING_MIN_SIM]
    if not filtered:
        filtered = scored_chunks[:EMBEDDING_TOP_K]
    else:
        filtered = filtered[:EMBEDDING_TOP_K]

    print("  -> [Embed] 청크 기반 백업 선택 결과:")
    for chunk in filtered:
        preview = clean_heading_label(chunk['heading'])[:40]
        print(f"     • {preview} | topic={chunk['topic']} | score={chunk['score']:.3f} | len={len(chunk['text'])}자")

    selected_text = '\n\n'.join(chunk['text'] for chunk in filtered)
    selected_len = sum(len(chunk['text']) for chunk in filtered)
    original_len = len(markdown_text)
    ratio = (selected_len / original_len * 100) if original_len else 0
    print(f"  -> [Embed] 선택 텍스트 길이: {selected_len}자 (원문 대비 {ratio:.1f}%)")

    if ticker:
        print(f"  -> [Embed] {ticker} 백업 선택 청크 수: {len(filtered)} (총 {len(chunks)}개 중)")
        debug_dump_markdown(ticker, "임베딩선택", selected_text, prefix='selected_')
    final_text = selected_text if selected_text.strip() else markdown_text
    return drop_unwanted_sections(final_text)

# 테스트할 10개 기업 리스트 (다양한 섹터 구성)
DEFAULT_TARGET_COMPANIES = {
    '005930': '삼성전자',       # 반도체/가전
    '000660': 'SK하이닉스',     # 반도체
    '373220': 'LG에너지솔루션',  # 2차전지
    '005380': '현대차',         # 자동차
    '035420': 'NAVER',         # 플랫폼
    '003490': '대한항공',       # 항공/운송 (유가 민감)
    '009540': 'HD현대중공업',    # 조선 (수주 산업)
    '207940': '삼성바이오로직스', # 바이오 (CDMO)
    '010140': '삼성중공업',      # 조선
    '035720': '카카오'          # 플랫폼
}

# 추가 검증용 10개 기업 (다른 섹터/기업 구성)
ADDITIONAL_TARGET_COMPANIES = {
    '051910': 'LG화학',        # 화학/2차전지
    '068270': '셀트리온',      # 바이오
    '000270': '기아',          # 자동차
    '028260': '삼성물산',      # 복합상사/건설
    '105560': 'KB금융',        # 금융지주
    '055550': '신한지주',      # 금융지주
    '017670': 'SK텔레콤',      # 통신
    '034730': 'SK',            # 지주/에너지
    '096770': 'SK이노베이션',  # 에너지/화학
    '051900': 'LG생활건강'     # 소비재
}

KRX_LIST_PATH = "news-insight-backend/data/krx_sector_industry.csv"

def load_all_companies():
    """KRX 전체 상장사 리스트 로드"""
    # 경로가 현재 스크립트 기준 상대경로인지 확인
    # test_file.py는 fintech 폴더에 있고, news-insight-backend는 그 하위에 있음
    # 따라서 fintech/news-insight-backend/... 가 아니라 news-insight-backend/... 가 맞음 (실행 위치 기준)
    # 하지만 절대 경로로 찾는게 안전함
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(script_dir, KRX_LIST_PATH)
    
    if not os.path.exists(file_path):
        # 혹시 실행 위치가 다를 수 있으니 절대 경로 재시도
        print(f"  -> [Warn] 파일 찾기 실패: {file_path}")
        # workspace root 기준 시도
        workspace_root = os.path.dirname(script_dir) # C:\Users\Admin\WORKSPACE\Cursor
        file_path_v2 = os.path.join(workspace_root, "fintech", KRX_LIST_PATH)
        if os.path.exists(file_path_v2):
            file_path = file_path_v2
        else:
            print(f"  -> [Error] 기업 목록 파일이 없습니다: {KRX_LIST_PATH}")
            return {}
    
    try:
        df = pd.read_csv(file_path, dtype={'Code': str})
        companies = {}
        for _, row in df.iterrows():
            code = str(row['Code']).strip()
            name = str(row['Name']).strip()
            # 우선주 제외 (코드 끝이 0이 아닌 경우)
            if len(code) == 6 and code.endswith('0'):
                companies[code] = name
                
        print(f"  -> [Info] 총 {len(companies)}개 기업 로드 완료 (우선주 제외)")
        return companies
    except Exception as e:
        print(f"  -> [Error] 기업 목록 로드 실패: {e}")
        return {}

# 환경 변수로 실행 대상 세트를 전환할 수 있도록 설정
TARGET_SET = os.getenv('TARGET_SET', 'default').lower()

if TARGET_SET == 'all':
    print(f"  -> [Mode] 전체 기업 분석 모드 (Source: CSV)")
    TARGET_COMPANIES = load_all_companies()
    if not TARGET_COMPANIES:
        print("  -> [Warn] 전체 기업 로드 실패, 기본 리스트 사용")
        TARGET_COMPANIES = DEFAULT_TARGET_COMPANIES
elif TARGET_SET == 'additional':
    TARGET_COMPANIES = ADDITIONAL_TARGET_COMPANIES
else:
    TARGET_COMPANIES = DEFAULT_TARGET_COMPANIES

# ==============================================================================
# 2. [Cleaning] HTML -> Markdown 변환
# ==============================================================================
def clean_html_to_markdown(html_content):
    if not html_content:
        return ""

    soup = BeautifulSoup(html_content, 'html.parser')
    for tag in soup(['script', 'style', 'img', 'svg', 'path']):
        tag.decompose()
    
    cleaned_html = str(soup)
    text = md(cleaned_html, heading_style="ATX", strip=['a'], newline_style="BACKSLASH")
    text = re.sub(r'\n\s+\n', '\n\n', text)
    text = re.sub(r' +', ' ', text)
    
    # 추가: 반복 구조 제거 (토큰 절약)
    # 1. 연속된 빈 줄 압축
    text = re.sub(r'\n{3,}', '\n\n', text)
    # 2. 표의 빈 셀 제거
    text = re.sub(r'\|\s*\|\s*\|', '|', text)
    
    text = filter_boilerplate_references(text)
    
    return text

def filter_boilerplate_references(text):
    """'~참조 바랍니다' 등 네비게이션 문장을 제거하여 토큰 절약"""
    if not text:
        return ""
    
    nav_patterns = [
        r"참고하시기 바랍니다",
        r"참조하시기 바랍니다",
        r"참조 바랍니다",
        r"참조바랍니다",
        r"참고 바랍니다",
        r"참고바랍니다",
        r"보시기 바랍니다",
        r"확인하시기 바랍니다",
        r"기재되어 있습니다",
        r"기재되어있습니다"
    ]
    value_keywords = [
        "통화선도", "스왑", "스와프", "선물", "옵션", "파생", "헷지", "헤지",
        "위험회피", "매매", "계약", "체결", "평가", "손익", "잔액",
        "%", "원", "달러", "억원", "배럴", "톤"
    ]
    ref_patterns = [
        r"['\"「].+['\"」]\s*(을|를)?\s*참(조|고)",
        r"상세 내용은\s*['\"「].+['\"」]"
    ]
    
    lines = text.split('\n')
    filtered_lines = []
    for line in lines:
        clean_line = line.strip()
        if len(clean_line) < 2:
            continue
        is_nav = any(re.search(pat, clean_line) for pat in nav_patterns)
        has_value = any(keyword in clean_line for keyword in value_keywords)
        ref_hit = any(re.search(pat, clean_line) for pat in ref_patterns)
        if (is_nav or ref_hit):
            if has_value:
                ref_positions = [idx for idx in (clean_line.find("참고"), clean_line.find("참조")) if idx != -1]
                if ref_positions:
                    cut_idx = min(ref_positions)
                    trimmed = clean_line[:cut_idx].rstrip(" ,.-")
                    if trimmed:
                        filtered_lines.append(trimmed)
                    continue
            else:
                continue
        filtered_lines.append(line)
    
    cleaned = '\n'.join(filtered_lines).strip()
    return cleaned or ""

def ensure_directory(path):
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def cache_file_path(ticker, text_hash):
    ensure_directory(CACHE_DIR)
    return os.path.join(CACHE_DIR, f"{ticker}_{text_hash}.json")

def load_cached_result(ticker, text_hash):
    if not CACHE_ENABLED:
        return None
    path = cache_file_path(ticker, text_hash)
    if not os.path.exists(path):
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return None

def save_cached_result(ticker, text_hash, data):
    if not CACHE_ENABLED or not data:
        return
    path = cache_file_path(ticker, text_hash)
    payload = {
        "ticker": ticker,
        "hash": text_hash,
        "data": data
    }
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

HEADING_REGEXES = [
    r'^#{1,6}\s+',
    r'^\d+[\.\)]\s*',
    r'^[IVXLCDM]+\.\s*',
    r'^[가-힣]\.\s*',
    r'^[A-Z]\.\s*'
]

UNWANTED_SECTION_KEYWORDS = [
    "예측정보에 대한 주의사항",
    "법규상의 규제",
    "법규상의 규제에 관한 사항",
    "환경 및 기타 법규상의 규제",
    "파생상품 및 위험관리정책",
    "파생상품 및 위험 관리정책"
]

def _looks_like_heading(stripped_line):
    if not stripped_line:
        return False
    for pattern in HEADING_REGEXES:
        if re.match(pattern, stripped_line):
            return True
    return False

def _normalize_heading_text(raw_heading):
    if not raw_heading:
        return ""
    normalized = raw_heading.replace('#', ' ').strip()
    normalized = re.sub(r'^\d+[\.\)]\s*', '', normalized)
    normalized = re.sub(r'^[IVXLCDM]+\.\s*', '', normalized, flags=re.IGNORECASE)
    normalized = re.sub(r'^[가-힣]\.\s*', '', normalized)
    normalized = re.sub(r'\s+', '', normalized)
    return normalized

def is_disallowed_heading(heading_line):
    normalized = _normalize_heading_text(heading_line)
    return any(keyword in normalized for keyword in DISALLOWED_HEADING_KEYWORDS)

def filter_disallowed_chunks(chunks):
    if not chunks:
        return []
    filtered = []
    for chunk in chunks:
        heading = chunk.get('heading')
        if heading and is_disallowed_heading(heading):
            continue
        filtered.append(chunk)
    return filtered

def remove_disclaimer_paragraphs(text):
    if not text:
        return text
    cleaned = text
    for pattern in DISCLAIMER_REGEXES:
        cleaned = re.sub(pattern, '\n', cleaned, flags=re.S)
    return cleaned

def final_disclaimer_sweep(text, ticker=None):
    if not text or not text.strip():
        return text
    cleaned = drop_unwanted_sections(text)
    if any(keyword in cleaned for keyword in FINAL_DISCLAIMER_KEYWORDS):
        cleaned = remove_disclaimer_paragraphs(cleaned)
        if any(keyword in cleaned for keyword in FINAL_DISCLAIMER_KEYWORDS):
            label = ticker or "문서"
            print(f"  -> [Warn] {label} 면책 문단이 남아있어 재차 제거했으나 일부가 잔존할 수 있습니다.")
    return cleaned

def truncate_text(text, limit=MAX_LLM_CHARS, label=None):
    if not text or limit <= 0:
        return text
    if len(text) <= limit:
        return text
    tag = f"{label} " if label else ""
    print(f"  -> [Info] {tag}텍스트 길이 {len(text)}자 → {limit}자로 절단")
    return text[:limit]

def drop_unwanted_sections(text):
    if not text or not text.strip():
        return text
    lines = text.split('\n')
    result = []
    skip = False
    for line in lines:
        stripped = line.strip()
        if not stripped:
            continue
        if _looks_like_heading(stripped):
            normalized = _normalize_heading_text(stripped)
            if any(keyword in normalized for keyword in DISALLOWED_HEADING_KEYWORDS):
                skip = True
                continue
            if skip and not any(keyword in normalized for keyword in ALLOW_HEADINGS_AFTER_SKIP):
                continue
            if skip and any(keyword in normalized for keyword in ALLOW_HEADINGS_AFTER_SKIP):
                skip = False
        if any(keyword in stripped for keyword in UNWANTED_SECTION_KEYWORDS):
            skip = True
            continue
        if skip:
            continue
        result.append(line)
    cleaned = '\n'.join(result).strip()
    cleaned = remove_disclaimer_paragraphs(cleaned)
    return cleaned or text

def debug_dump_markdown(ticker, section_name, content, prefix="", force=False):
    """지정된 티커에 대해 마크다운을 파일로 저장하고 앞부분을 출력"""
    if not force and ticker not in DEBUG_DUMP_TICKERS:
        return
    safe_section = section_name.replace(' ', '_').replace('/', '_')
    filename = f"debug_{ticker}_{prefix}{safe_section}.md"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(content)
    preview = content[:500].replace('\n', ' ')  # 한 줄로 출력
    print(f"  -> [Debug] {ticker} {section_name} 마크다운 저장: {filename}")
    print(f"     미리보기: {preview}...")

# ==============================================================================
# 2.5. [Extraction] 핵심 하위 섹션만 추출 (토큰 절약)
# ==============================================================================
def extract_key_subsections(markdown_text, section_type='business', debug_context=None):
    """
    사업보고서의 핵심 하위 섹션만 추출하여 토큰 절약
    
    Args:
        markdown_text: 전체 마크다운 텍스트
        section_type: 'business' (사업의 내용) 또는 'mda' (이사의 경영진단)
    
    Returns:
        추출된 핵심 섹션만 포함한 마크다운 텍스트
    """
    def parse_heading(line):
        stripped = line.strip()
        if not stripped:
            return None
        
        heading_patterns = [
            (r'^(#{1,6})\s*(.+)$', lambda m: len(m.group(1))),   # Markdown 헤딩
            (r'^([IVXLCDM]+)\.\s*(.+)$', lambda m: 2),           # 로마숫자 (대문자)
            (r'^([ivxlcdm]+)\.\s*(.+)$', lambda m: 2),           # 로마숫자 (소문자)
            (r'^(\d+)\.\s*(.+)$', lambda m: 3),                  # 숫자.
            (r'^(\d+)\)\s*(.+)$', lambda m: 4),                  # 숫자) → 하위 레벨로 간주
            (r'^([A-Z])\.\s*(.+)$', lambda m: 4),                # 알파벳.
            (r'^([가-힣])\.\s*(.+)$', lambda m: 5),              # 한글.
        ]
        
        for pattern, level_fn in heading_patterns:
            match = re.match(pattern, stripped)
            if match:
                return level_fn(match), match.group(2)

        complex_match = re.match(r'^((?:\d+|[IVXLCDM]+|[ivxlcdm]+|[가-힣]|[A-Z])[\-\.]\s*(?:\d+|[IVXLCDM]+|[ivxlcdm]+|[가-힣]|[A-Z]))\s*(.+)$', stripped)
        if complex_match:
            return 4, complex_match.group(2)
        return None
    
    context_ticker = debug_context.get('ticker') if debug_context else None
    context_section_label = debug_context.get('label') if debug_context else section_type
    
    if section_type == 'mda':
        # 이사의 경영진단: '5. 회계감사인의 감사의견 등' 이전까지만 추출 + 특정 섹션/표 제외
        lines = markdown_text.split('\n')
        result_lines = []
        stop_patterns = [
            r'회계감사인의\s*감사의견'
        ]
        skip_patterns = [
            r'예측정보에\s*대한\s*주의사항',
            r'중요한\s*회계.*추정',
            r'회계정책.*추정',
            r'환경.*종업원'
        ]
        in_skip_section = False
        
        for line in lines:
            stripped = line.strip()
            
            # 표(마크다운 테이블)는 모두 제외
            if stripped.startswith('|'):
                continue
            
            heading_info = parse_heading(line)
            if heading_info:
                _, heading_text = heading_info
                clean_heading = re.sub(r'\([^)]*\)', '', heading_text).strip()
                clean_heading = re.sub(r'^\d+[\.\)]?\s*', '', clean_heading)
                clean_heading = re.sub(r'^[IVXLCDM]+\.\s*', '', clean_heading, flags=re.IGNORECASE)
                clean_heading = re.sub(r'\s+', '', clean_heading)
                # 중지 패턴 확인
                if any(re.search(pattern, clean_heading, re.IGNORECASE) for pattern in stop_patterns):
                    break
                
                # 스킵 대상 섹션 (예측정보 주의사항 등)
                if any(re.search(pattern, clean_heading, re.IGNORECASE) for pattern in skip_patterns):
                    in_skip_section = True
                    continue
                else:
                    in_skip_section = False
            
            if in_skip_section:
                continue
            
            result_lines.append(line)
        
        extracted_text = '\n'.join(result_lines)
        extracted_text = drop_unwanted_sections(extracted_text)
        if len(extracted_text.strip()) < 100:
            msg = "경영진단 섹션 추출 실패, 원문 사용"
            if context_ticker:
                msg = f"{msg} ({context_ticker})"
            print(f"  -> [Warning] {msg}")
            if context_ticker:
                debug_dump_markdown(context_ticker, context_section_label, markdown_text, prefix='failure_', force=True)
            return markdown_text
        
        print(f"  -> [Extract] 경영진단 핵심 섹션 추출 완료 (원본: {len(markdown_text)}자 → 추출: {len(extracted_text)}자)")
        # 디버깅: 추출된 텍스트의 마지막 300자 출력 (중지 지점 확인)
        if len(extracted_text) > 300:
            print(f"  -> [Debug] 추출된 텍스트 마지막 부분:\n...{extracted_text[-300:]}")
        return extracted_text
    
    elif section_type == 'business':
        # 사업의 내용: 특정 하위 섹션만 추출
        # 키워드 조합으로 더 유연하게 매칭 (정확한 키워드 포함 여부로 판단)
        target_keyword_sets = [
            ['주요', '제품'], ['주요', '서비스'], ['영업', '현황'],
            ['원재료', '생산'], ['생산', '설비'], ['생산', '실적'], ['가동률'],
            ['매출', '수주'], ['판매', '경로'], ['판매', '전략'],
            ['판매', '대수'], ['도매', '판매'], ['소매', '판매'],
            ['선가'], ['수주', '잔고'], ['수주', '현황'], ['발주', '현황'], ['인도', '일정'],
            ['연구', '개발'], ['R&D'], ['시장', '위험'], ['위험', '관리'],
            ['파생', '상품'], ['재무', '상태'], ['자금', '조달'],
            ['신규', '사업'], ['플랫폼'], ['가입자'], ['이용자'],
            ['임상'], ['파이프라인'], ['품목', '허가'], ['수익'], ['보험료']
        ]
        
        lines = markdown_text.split('\n')
        result_lines = []
        in_target_section = False
        current_section_level = 0
        found_sections = []
        
        # 디버깅: 처음 50줄에서 헤딩 확인
        debug_headings = []
        for i, line in enumerate(lines[:50]):
            if line.strip().startswith('#'):
                debug_headings.append(f"줄{i+1}: {line.strip()[:80]}")
        if debug_headings:
            print(f"  -> [Debug] 처음 50줄 내 헤딩 예시: {debug_headings[:5]}")
        
        for i, line in enumerate(lines):
            # 헤딩 레벨 확인 (# 개수)
            heading_info = parse_heading(line)
            
            if heading_info:
                heading_level, heading_text = heading_info
                
                # 괄호 및 번호 제거
                clean_heading = re.sub(r'\([^)]*\)', '', heading_text).strip()
                clean_heading = re.sub(r'^\d+[\.\)]?\s*', '', clean_heading)
                clean_heading = re.sub(r'^[IVXLCDM]+\.\s*', '', clean_heading, flags=re.IGNORECASE)
                clean_heading_no_space = re.sub(r'\s+', '', clean_heading)
                
                def match_keywords(keyword_set):
                    return all(keyword in clean_heading_no_space for keyword in keyword_set)
                
                is_target = any(match_keywords(keyword_set) for keyword_set in target_keyword_sets)
                
                if is_target:
                    in_target_section = True
                    current_section_level = heading_level
                    result_lines.append(line)
                    found_sections.append(heading_text)
                    print(f"  -> [Extract] ✅ 하위 섹션 발견: {heading_text}")
                else:
                    # 다른 섹션 시작 확인
                    if heading_level <= 2:  # ## 레벨 이하로 가면 다른 대섹션
                        if in_target_section:
                            in_target_section = False
                            current_section_level = 0
                    elif in_target_section and heading_level <= current_section_level:
                        # 같은 레벨 또는 상위 레벨의 다른 섹션 시작
                        in_target_section = False
                        current_section_level = 0
            elif in_target_section:
                # 타겟 섹션 내의 내용은 모두 포함
                result_lines.append(line)
        
        extracted_text = '\n'.join(result_lines)
        extracted_text = drop_unwanted_sections(extracted_text)
        
        # 추출된 내용이 없으면 원문 반환 (안전장치)
        if len(extracted_text.strip()) < 100:
            msg = f"핵심 섹션 추출 실패 (발견된 섹션: {found_sections}), 원문 사용"
            if context_ticker:
                msg = f"{msg} ({context_ticker})"
            print(f"  -> [Warning] {msg}")
            if context_ticker:
                debug_dump_markdown(context_ticker, context_section_label, markdown_text, prefix='failure_', force=True)
            return markdown_text
        
        # 추출된 섹션의 헤딩 확인
        extracted_headings = [line for line in extracted_text.split('\n') if line.strip().startswith('#')]
        print(f"  -> [Extract] 핵심 섹션 추출 완료 (원본: {len(markdown_text)}자 → 추출: {len(extracted_text)}자)")
        print(f"  -> [Extract] 추출된 섹션 목록: {extracted_headings[:5]}")
        return extracted_text
    else:
        return markdown_text

# ==============================================================================
# 3. [Targeting] DART 목차별 핀셋 추출 (로직 강화)
# ==============================================================================
def fetch_dart_sections(ticker, company_name):
    print(f"--- [{company_name}] ({ticker}) 보고서 탐색 중 ---")
    
    try:
        # 현재 날짜 기준 1년 전 (2024년 고정)
        target_year = 2024
        # 사업보고서는 보통 다음 해 3월에 제출되므로 접수일자 범위 확장
        start_date = f'{target_year}-01-01'  # 2024-01-01
        end_date = f'{target_year + 1}-06-30'  # 2025-06-30 (2024년 사업보고서는 2025년 3월 제출)
        
        # 정기공시(A) 검색
        reports = dart.list(ticker, start=start_date, end=end_date, kind='A', final=True)
        
        if reports is None or len(reports) == 0:
            print(f"  -> [Pass] {target_year}년 보고서 없음")
            return None
        
        # 사업보고서만 필터링 (pblntf_detail_ty='A001' 또는 report_nm에 '사업보고서' 포함)
        # report_nm 필드에서 "사업보고서"가 포함된 것만 선택
        business_reports = reports[reports['report_nm'].str.contains('사업보고서', na=False)]
        
        if len(business_reports) == 0:
            print(f"  -> [Alert] {target_year}년 사업보고서를 찾지 못했습니다.")
            print(f"  -> [Debug] 사용 가능한 보고서: {reports['report_nm'].tolist()[:5]}")
            return None
        
        # 최신 사업보고서 선택 (일반적으로 첫 번째가 최신)
        target_report = business_reports.iloc[0]
        rcept_no = target_report['rcept_no']
        report_title = target_report['report_nm']
        print(f"  -> 대상 보고서: {report_title} (No: {rcept_no})")
        
        sub_docs = dart.sub_docs(rcept_no)
        
        # 디버깅: sub_docs 구조 확인
        print(f"  -> [Debug] sub_docs 구조: {type(sub_docs)}, 컬럼: {list(sub_docs.columns)}")
        if len(sub_docs) > 0:
            print(f"  -> [Debug] 총 {len(sub_docs)}개 섹션 발견")
            if 'title' in sub_docs.columns:
                # 처음 10개 섹션 제목 출력
                titles = sub_docs['title'].head(10).tolist()
                print(f"  -> [Debug] 섹션 제목 예시: {titles}")

        combined_text = ""
        found_count = 0
        
        # 올바른 접근: 각 행(row)을 순회하면서 'title' 컬럼에서 섹션 찾기
        if 'title' in sub_docs.columns and 'url' in sub_docs.columns:
            for idx, row in sub_docs.iterrows():
                title_str = str(row['title'])
                clean_title = title_str.replace(" ", "").replace(".", "").strip()
                
                # 1) 사업의 내용
                if '사업의내용' in clean_title or '사업내용' in clean_title:
                    if pd.notna(row['url']) and row['url']:
                        try:
                            # URL에서 실제 내용 가져오기
                            doc_url = row['url']
                            response = requests.get(doc_url, timeout=30)
                            response.raise_for_status()
                            html = response.text
                            if html:
                                md_text = clean_html_to_markdown(html)
                                # 핵심 하위 섹션만 추출 (토큰 절약)
                                md_text = extract_key_subsections(
                                    md_text,
                                    section_type='business',
                                    debug_context={'ticker': ticker, 'label': f'{report_title}_사업의내용'}
                                )
                                debug_dump_markdown(ticker, '사업의 내용', md_text, prefix='business_')
                                combined_text += f"# 1. 사업의 내용\n{md_text}\n\n"
                                found_count += 1
                                print(f"  -> [V] '사업의 내용' 추출 성공 (섹션: {title_str[:50]}, 길이: {len(md_text)}자)")
                        except Exception as e:
                            print(f"  -> [Warning] 섹션 내용 가져오기 실패 ({title_str[:30]}): {e}")
                            continue
                
                # 2) 이사의 경영진단 (MD&A)
                elif '이사의경영진단' in clean_title or '경영진단' in clean_title or '분석의견' in clean_title:
                    if pd.notna(row['url']) and row['url']:
                        try:
                            # URL에서 실제 내용 가져오기
                            doc_url = row['url']
                            response = requests.get(doc_url, timeout=30)
                            response.raise_for_status()
                            html = response.text
                            if html:
                                md_text = clean_html_to_markdown(html)
                                # '5. 회계감사인의 감사의견 등' 이전까지만 추출 (토큰 절약)
                                md_text = extract_key_subsections(
                                    md_text,
                                    section_type='mda',
                                    debug_context={'ticker': ticker, 'label': f'{report_title}_이사의경영진단'}
                                )
                                debug_dump_markdown(ticker, '이사의 경영진단', md_text, prefix='mda_')
                                combined_text += f"# 2. 이사의 경영진단\n{md_text}\n\n"
                                found_count += 1
                                print(f"  -> [V] '이사의 경영진단' 추출 성공 (섹션: {title_str[:50]}, 길이: {len(md_text)}자)")
                        except Exception as e:
                            print(f"  -> [Warning] 섹션 내용 가져오기 실패 ({title_str[:30]}): {e}")
                            continue
        else:
            print(f"  -> [Error] sub_docs에 'title' 또는 'url' 컬럼이 없습니다.")
            return None
                
        if found_count == 0:
            print("  -> [Alert] 타겟 목차를 찾지 못했습니다.")
            if 'title' in sub_docs.columns:
                all_titles = sub_docs['title'].tolist()
                print(f"  -> [Debug] 전체 섹션 목록 ({len(all_titles)}개):")
                for i, t in enumerate(all_titles[:20], 1):  # 처음 20개만 출력
                    print(f"      {i}. {t}")
            return None
            
        return combined_text

    except Exception as e:
        print(f"  -> [Error] DART API 오류: {e}")
        return None

# ==============================================================================
# 3.5. [Summarization] 긴 텍스트 요약 (토큰 절약)
# ==============================================================================
def summarize_with_llm(markdown_text):
    """긴 텍스트를 요약하는 함수 (gpt-5-nano 사용) - 동적 길이 조정"""
    # 1. 요약 품질 개선: 텍스트 길이에 따라 동적으로 목표 길이 조정
    text_length = len(markdown_text)
    if text_length > 50000:
        max_length = 15000  # 매우 긴 경우
    elif text_length > 30000:
        max_length = 12000  # 긴 경우
    else:
        max_length = 10000  # 보통
    
    print(f"  -> 📝 긴 텍스트 요약 중... (원본: {text_length}자 → 목표: {max_length}자)")
    
    prompt = f"""
    다음 사업보고서 내용을 {max_length}자 이내로 핵심만 요약해줘.
    
    [포함할 핵심 정보]
    - 사업 내용 및 주요 제품/서비스
    - 주요 고객사/매출처
    - 핵심 원재료
    - 설비투자 계획
    - 비용 구조
    
    [주의사항]
    - 제공된 내용에서만 추출할 것 (추측하지 말 것)
    - 구체적인 숫자와 사실만 포함할 것
    
    원문:
    {markdown_text[:50000]}  # 최대 50,000자만 전달
    """
    
    try:
        response = summary_llm.invoke([HumanMessage(content=prompt)])
        summarized = response.content.strip()
        print(f"  -> ✅ 요약 완료 (요약본: {len(summarized)}자)")
        return summarized
    except Exception as e:
        # 2. 에러 처리 강화: 요약 실패 시 원문의 앞부분만 사용 (가장 중요한 정보가 앞에 있음)
        print(f"  -> [Warning] 요약 실패: {e}, 원문 앞부분 사용")
        # 원문의 앞부분만 사용 (가장 중요한 정보가 앞에 있음)
        return markdown_text[:max_length]

# ==============================================================================
# 4. [Extraction] LLM 구조화
# ==============================================================================
def extract_data_with_llm(markdown_text):
    print("  -> AI 분석 및 JSON 구조화 중...")
    
    # 디버깅: LLM에 전달될 텍스트의 구조 확인
    lines = markdown_text.split('\n')
    headings = [line for line in lines if line.strip().startswith('#')]
    print(f"  -> [Debug] 추출된 섹션 헤딩: {headings[:10]}")  # 처음 10개 헤딩만 출력
    
    # 1단계: 긴 텍스트를 먼저 요약 (토큰 절약)
    if len(markdown_text) > 20000:  # 20,000자 이상이면 요약
        summarized_text = summarize_with_llm(markdown_text)
        # 요약 후에도 헤딩 확인
        summary_headings = [line for line in summarized_text.split('\n') if line.strip().startswith('#')]
        print(f"  -> [Debug] 요약 후 헤딩: {summary_headings[:10]}")
    else:
        summarized_text = markdown_text
    
    prompt = """
    너는 10년 차 펀드매니저야. 제공된 기업의 [사업보고서] 내용을 분석해서 
    투자 판단에 필요한 핵심 정보를 아래 JSON 포맷으로 정확하게 추출해.
    
    [중요 지침]
    - 제공된 텍스트에서만 정보를 추출할 것 (추측하지 말것)
    - 명확히 언급되지 않은 정보는 "정보없음"으로 표시
    - 구체적인 숫자, 이름, 사실만 포함할 것
    
    [추출 항목]
    1. business_summary: 동사가 영위하는 사업 내용을 초등학생도 이해하게 3줄 요약. (제공된 내용에서만 추출)
    2. major_products: 주요 제품 및 서비스 리스트 (구체적 브랜드나 모델명 포함). 보고서에 명시된 것만.
    3. major_clients: 주요 매출처/고객사 실명 (예: Apple, 현대차). 보고서에 명시된 것만. 없으면 "정보없음".
    4. raw_materials: 제품 생산에 필요한 핵심 원재료. 보고서에 명시된 것만.
    5. capax_investment: 설비투자(CAPEX)나 신규 시설 투자 계획 언급 요약. 보고서에 명시된 것만.
    6. cost_structure: 비용 구조에서 가장 큰 비중을 차지하는 것 (예: 원재료비, 인건비). 보고서에 명시된 것만.
    7. keywords: 기업을 설명하는 핵심 해시태그 5개. (보고서 내용 기반)

    [반환 형식]
    오직 JSON 형식만 반환할 것. (Markdown code block 없이)
    JSON 형식:
    {
        "business_summary": "...",
        "major_products": [...],
        "major_clients": "...",
        "raw_materials": [...],
        "capax_investment": "...",
        "cost_structure": "...",
        "keywords": [...]
    }
    """

    messages = [
        SystemMessage(content="You are a precise financial analyst. Extract only factual information from the provided text. Do not hallucinate or make assumptions. Output JSON only."),
        HumanMessage(content=prompt + "\n\n" + summarized_text)
    ]
    
    try:
        response = llm.invoke(messages)
        content = response.content.replace("```json", "").replace("```", "").strip()
        parsed_data = json.loads(content)
        
        # risk_factors 필드가 있으면 제거 (토큰 절약)
        if 'risk_factors' in parsed_data:
            del parsed_data['risk_factors']
        
        # 3. 결과 검증 로직 추가
        required_fields = ['business_summary', 'major_products', 'major_clients', 
                          'raw_materials', 'capax_investment', 'cost_structure', 'keywords']
        missing_fields = [field for field in required_fields if field not in parsed_data]
        
        if missing_fields:
            print(f"  -> [Warning] 필수 필드 누락: {missing_fields}")
            # 누락된 필드를 기본값으로 채우기
            for field in missing_fields:
                if field in ['major_products', 'raw_materials', 'keywords']:
                    parsed_data[field] = []
                else:
                    parsed_data[field] = "정보없음"
        
        return parsed_data
    except json.JSONDecodeError as e:
        print(f"  -> [Error] JSON 파싱 실패: {e}")
        if 'response' in locals():
            print(f"  -> [Debug] 응답 내용: {response.content[:500]}")
        return None
    except Exception as e:
        print(f"  -> [Error] LLM 응답 처리 실패: {e}")
        if 'response' in locals():
            print(f"  -> [Debug] 응답 내용: {response.content[:500]}")
        return None

# ==============================================================================
# 5. 메인 실행 루프 (병렬 처리)
# ==============================================================================
def process_company(ticker, name):
    """단일 기업 처리 함수"""
    try:
        # 1. DART 파싱
        report_text = fetch_dart_sections(ticker, name)
        
        # 데이터가 없으면 LLM 호출 스킵
        if report_text:
            filtered_text = select_relevant_chunks(report_text, ticker)
            effective_text = filtered_text if filtered_text else report_text
            effective_text = drop_unwanted_sections(effective_text)
            effective_text = final_disclaimer_sweep(effective_text, ticker)
            effective_text = truncate_text(effective_text, MAX_LLM_CHARS, label=name)
            text_hash = hashlib.sha256(effective_text.encode('utf-8')).hexdigest()
            if ENABLE_LLM and CACHE_ENABLED:
                cached_packet = load_cached_result(ticker, text_hash)
                if cached_packet and 'data' in cached_packet:
                    cached_data = cached_packet['data']
                    cached_data['ticker'] = ticker
                    cached_data['company_name'] = name
                    print(f"  -> [Cache] {name} 결과 재사용 (hash={text_hash[:8]})")
                    return cached_data
            if ENABLE_LLM:
                # 2. LLM 구조화
                structured_data = extract_data_with_llm(effective_text)
                
                if structured_data:
                    structured_data['ticker'] = ticker
                    structured_data['company_name'] = name
                    if CACHE_ENABLED:
                        save_cached_result(ticker, text_hash, structured_data)
                    print(f"  -> ✅ {name} 데이터 저장 완료!")
                    return structured_data
            else:
                # 테스트 모드: LLM 호출 대신 추출된 텍스트 정보만 출력
                original_len = len(report_text)
                filtered_len = len(effective_text)
                print(f"  -> [Test] LLM 호출 생략. 원문 {original_len}자 → 임베딩 선택 {filtered_len}자")
                snippet = effective_text[:800]
                print(f"  -> [Test] 선택 텍스트 앞부분:\n{snippet}\n...")
                return None
        else:
            print(f"  -> ⏭️ {name} 데이터 없음, 스킵합니다.")
            return None
    except Exception as e:
        print(f"  -> [Error] {name} 처리 중 오류: {e}")
        return None

def chunk_companies(items, size):
    for idx in range(0, len(items), size):
        yield items[idx:idx + size], (idx // size) + 1

def save_results(data, suffix=None):
    if not data:
        return
    filename = "dart_analysis_result.json" if suffix is None else f"dart_analysis_result_{suffix}.json"
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    label = "최종" if suffix is None else suffix
    print(f"  -> [Save] {label} 결과 저장 ({len(data)}개)")

def run_batch(batch_items, batch_index, total_batches):
    print(f"\n===== Batch {batch_index}/{total_batches} (기업 {len(batch_items)}개) =====")
    batch_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_company = {
            executor.submit(process_company, ticker, name): (ticker, name)
            for ticker, name in batch_items
        }
        for future in as_completed(future_to_company):
            ticker, name = future_to_company[future]
            try:
                result = future.result()
                if result:
                    batch_results.append(result)
            except Exception as e:
                print(f"  -> [Error] {name} 처리 중 예외 발생: {e}")
            time.sleep(0.3)
    print(f"===== Batch {batch_index} 완료: {len(batch_results)}개 성공 =====")
    return batch_results

final_database = []

print("🚀 [DART x LLM] 데이터 댐 구축 시작 (Sample 10)")

company_items = list(TARGET_COMPANIES.items())
if not company_items:
    print("처리할 기업이 없습니다.")
else:
    total_batches = (len(company_items) + BATCH_SIZE - 1) // BATCH_SIZE
    for batch_items, batch_index in chunk_companies(company_items, BATCH_SIZE):
        batch_results = run_batch(batch_items, batch_index, total_batches)
        if batch_results:
            final_database.extend(batch_results)
            save_results(final_database, suffix=f"batch_{batch_index}")
        if batch_index < total_batches:
            print(f"  -> [Batch] 다음 배치까지 {BATCH_PAUSE_SECONDS}초 대기")
            time.sleep(BATCH_PAUSE_SECONDS)

    if final_database:
        save_results(final_database)
        print("\n" + "="*50)
        print(f"✅ 작업 완료! 총 {len(final_database)}개 기업 데이터가 저장되었습니다.")
    else:
        print("\n❌ 저장된 데이터가 없습니다.")
