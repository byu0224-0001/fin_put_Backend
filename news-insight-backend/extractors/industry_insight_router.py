"""
Industry Insight Router

산업 리포트 인사이트를 industry_edges 테이블에 저장

v2 변경사항:
- 260자 하드코딩 제거 → 문장 단위 자르기
- MAX_LOGIC_SUMMARY_LEN = 180 (권장)
"""
import sys
from pathlib import Path
from typing import Dict, Optional, Any, Tuple
import logging
from datetime import datetime, timedelta
from enum import Enum
import re

# ✅ P0-4: logic_summary 최대 길이 설정 (260자 하드코딩 제거)
MAX_LOGIC_SUMMARY_LEN = 180  # 권장: 80~180자
MIN_LOGIC_SUMMARY_LEN = 30   # 최소 길이


def truncate_to_sentence(text: str, max_len: int = MAX_LOGIC_SUMMARY_LEN) -> str:
    """
    문장 단위로 텍스트를 자르기 (단어 중간에서 자르지 않음)
    
    Args:
        text: 원본 텍스트
        max_len: 최대 길이
    
    Returns:
        잘린 텍스트 (문장 단위)
    """
    if not text:
        return ""
    
    text = text.strip()
    
    # 이미 짧으면 그대로 반환
    if len(text) <= max_len:
        return text
    
    # 문장 분리 시도
    sentences = re.split(r'([.!?])\s+', text)
    
    # 문장 분리 결과 재조합 (구두점 포함)
    combined_sentences = []
    i = 0
    while i < len(sentences):
        if i + 1 < len(sentences) and sentences[i + 1] in '.!?':
            combined_sentences.append(sentences[i] + sentences[i + 1])
            i += 2
        else:
            combined_sentences.append(sentences[i])
            i += 1
    
    # 문장 단위로 누적하면서 최대 길이 내에서 선택
    result = ""
    for sent in combined_sentences:
        sent = sent.strip()
        if not sent:
            continue
        
        if len(result) + len(sent) + 1 <= max_len:
            if result:
                result += " " + sent
            else:
                result = sent
        else:
            # 다음 문장을 추가하면 초과 → 여기서 중단
            break
    
    # 결과가 없거나 너무 짧으면 단어 단위로 자르기
    if not result or len(result) < MIN_LOGIC_SUMMARY_LEN:
        # 단어 단위로 자르기 (max_len 지점에서 가장 가까운 공백 찾기)
        if len(text) > max_len:
            truncated = text[:max_len]
            last_space = truncated.rfind(' ')
            if last_space > max_len * 0.5:  # 절반 이상 위치에 공백이 있으면
                result = truncated[:last_space].strip()
            else:
                result = truncated.strip()
            
            # 끝이 구두점이 아니면 ... 추가
            if not result.endswith(('.', '!', '?')):
                result = result.rstrip('.,;:') + "..."
        else:
            result = text
    
    return result


class SaveResult(Enum):
    """저장 결과 타입 - 성공/스킵/실패 구분 (세분화)"""
    CREATED = "CREATED"                              # 새로 생성
    UPDATED = "UPDATED"                              # 기존 edge 업데이트
    # ⭐ 중복 스킵 세분화 (변화 유무 구분)
    SKIPPED_DEDUPE_APPENDED = "SKIPPED_DEDUPE_APPENDED"    # 중복 + 출처 추가됨
    SKIPPED_DEDUPE_EVICTED = "SKIPPED_DEDUPE_EVICTED"      # 중복 + 오래된 출처 제거 후 추가
    SKIPPED_DEDUPE_NOOP = "SKIPPED_DEDUPE_NOOP"            # 중복 + 변화 없음 (이미 존재)
    SKIPPED_DEDUPE = "SKIPPED_DEDUPE"                      # (레거시 호환) 일반 중복 스킵
    SKIPPED_DB_CONFLICT = "SKIPPED_DB_CONFLICT"            # DB 제약 충돌
    SKIPPED_EMPTY = "SKIPPED_EMPTY"                        # 빈 요약으로 스킵
    SKIPPED_SANITY = "SKIPPED_SANITY"                      # Sanity Check 실패
    FAILED = "FAILED"                                      # 실패

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from sqlalchemy.orm import Session
from typing import List
from app.models.industry_edge import IndustryEdge

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def save_industry_insight(
    db: Session,
    report_id: str,
    industry_logic: Dict[str, Any],
    report_metadata: Dict[str, Any],
    section_fingerprint: Optional[str] = None
) -> Tuple[SaveResult, Optional[str]]:
    """
    산업 인사이트를 industry_edges 테이블에 저장
    
    ⭐ P0+ 보강: 키 구조 명확화 (source_driver_code → target_sector_code)
    ⭐ Evidence 누적: 중복 시 sources 배열에 추가
    
    Args:
        db: DB 세션
        report_id: 리포트 ID
        industry_logic: 산업 Logic 데이터
        report_metadata: 리포트 메타데이터
        section_fingerprint: 섹션 단위 fingerprint (dedup용)
    
    Returns:
        (SaveResult, skip_reason) 튜플
        - SaveResult: 저장 결과 타입
        - skip_reason: 스킵 사유 (스킵인 경우에만)
    """
    try:
        # ⭐ 키 구조: source_driver_code → target_sector_code
        source_driver_code = None
        if industry_logic.get("target_drivers"):
            source_driver_code = industry_logic["target_drivers"][0]
        
        target_sector_code = industry_logic.get("target_sector_code")
        
        # source나 target이 없으면 저장 불가
        if not source_driver_code or not target_sector_code:
            logger.warning(f"Industry Insight 저장 실패: source_driver={source_driver_code}, target_sector={target_sector_code}")
            return (SaveResult.SKIPPED_EMPTY, "missing_driver_or_sector")
        
        # ⭐ 논리 단위 dedup은 압축 후에 수행 (압축된 logic_summary 기반)
        # 이유: 서로 다른 리포트라도 "1월 효과로 중소형주 강세"라는 같은 논리면 KG에는 1번만 있어야 함
        # ⭐ 강화: "내용이 왕이다" - driver_code와 무관하게 logic_summary가 같으면 중복으로 간주
        # ⭐ P0+: 텍스트 정규화를 통한 fingerprint 안정성 확보
        # ⭐ P0-H 디버깅: 중복 체크는 압축 후에 수행하도록 이동 (아래에서 처리)
        import hashlib
        from utils.text_normalizer import normalize_text_for_fingerprint
        
        # ⭐ 섹션 단위 dedup (fallback): 같은 리포트의 같은 섹션 fingerprint가 이미 있으면 스킵
        if section_fingerprint:
            existing = db.query(IndustryEdge).filter(
                IndustryEdge.report_id == report_id,
                IndustryEdge.source_driver_code == source_driver_code,
                IndustryEdge.target_sector_code == target_sector_code
            ).first()
            
            if existing:
                logger.info(f"Industry Insight 중복 감지 (섹션 fingerprint), 스킵: report_id={report_id}")
                return (SaveResult.SKIPPED_DEDUPE, "section_fingerprint")
        
        # 유효 시작일/종료일 계산 (산업 인사이트는 12개월 유효)
        report_date_str = report_metadata.get("report_date", "")
        try:
            if report_date_str:
                report_date = datetime.strptime(report_date_str, "%Y-%m-%d")
                valid_from = report_date.date()
                valid_to = (report_date + timedelta(days=365)).date()  # 12개월 유효
            else:
                valid_from = datetime.now().date()
                valid_to = (datetime.now() + timedelta(days=365)).date()
        except ValueError:
            valid_from = datetime.now().date()
            valid_to = (datetime.now() + timedelta(days=365)).date()
        
        # ⭐ P0-H: logic_summary 저장 시점 압축/게이트
        logic_summary_raw = industry_logic.get("logic_summary", "")
        logic_summary = logic_summary_raw  # 기본값
        key_sentence = industry_logic.get("key_sentence", "")
        
        # ⭐ P0-H 디버깅: 초기 값 기록
        logic_summary_raw_len = len(logic_summary_raw) if logic_summary_raw else 0
        compression_used = False
        compress_called = False  # ⭐ P0: 압축 호출 여부 boolean
        gate_passed = False
        compressed_len = 0
        final_len_before_save = 0
        
        # ⭐ P0-H 디버깅: 압축 조건 체크 전 로그
        logger.info(
            f"[P0-H 디버깅] 압축 조건 체크: "
            f"raw_len={logic_summary_raw_len}, "
            f"조건(>350)={'충족' if logic_summary_raw_len > 350 else '불충족'}, "
            f"report_id={report_id[:16]}..."
        )
        
        # ⭐ P2-9: 압축 목표 단위 고정
        MAX_LOGIC_SUMMARY_LENGTH = 260  # 공백 포함 문자수 (Python len() 기준, UTF-8)
        THRESHOLD_VALUE = 350  # 압축 강제 임계값
        
        # Quality Gate: logic_summary_raw_len > 350이면 압축 강제
        if logic_summary_raw and len(logic_summary_raw) > THRESHOLD_VALUE:
            compress_called = True  # ⭐ P0: 압축 호출 플래그
            try:
                import re
                from utils.semantic_compression import compress_semantically
                
                # 1. bullet/table/template 제거 전처리
                logic_summary_clean = logic_summary_raw
                # 리포트 템플릿 문구 제거
                template_patterns = [
                    r'투자유의.*?면책',
                    r'본.*?자료는.*?제공.*?목적',
                    r'무단복제.*?배포.*?금지',
                    r'담당자.*?연락처',
                    r'Analyst.*?@.*?\.com',
                    r'ComplianceNotice'
                ]
                for pattern in template_patterns:
                    logic_summary_clean = re.sub(pattern, '', logic_summary_clean, flags=re.IGNORECASE | re.DOTALL)
                
                # ⭐ P0-H 디버깅: 압축 호출 직전 로그 (조건 변수 일치 확인)
                clean_len = len(logic_summary_clean)
                compress_input_len = len(logic_summary_clean)  # 실제 압축 입력 길이
                
                # ⭐ P0-2: threshold 판정 근거 명시 (실제 사용 변수와 일치)
                threshold_used = "raw_len"  # 실제 코드에서 사용하는 변수
                threshold_value = logic_summary_raw_len  # 실제 판정에 사용된 값
                
                logger.info(
                    f"[P0-H 디버깅] 압축 호출 직전: "
                    f"raw_len={logic_summary_raw_len}, "
                    f"clean_len={clean_len}, "
                    f"compress_input_len={compress_input_len}, "
                    f"key_sentence_len={len(key_sentence) if key_sentence else 0}, "
                    f"threshold_used={threshold_used}, "
                    f"threshold_value={threshold_value}, "
                    f"threshold={THRESHOLD_VALUE}, "
                    f"조건(>{THRESHOLD_VALUE})={'충족' if threshold_value > THRESHOLD_VALUE else '불충족'}, "
                    f"변수_일치={'일치' if logic_summary_raw_len == clean_len == compress_input_len else '불일치'}"
                )
                
                # 2. semantic compression 수행
                logic_summary_compressed = compress_semantically(
                    industry_logic=logic_summary_clean,
                    key_sentence=key_sentence,
                    logic_summary=logic_summary_clean,
                    max_sentences=2,
                    max_length=260  # 저장용은 260자까지 허용
                )
                
                # ⭐ P0-H 디버깅: 압축 호출 직후 로그 + None/빈값 검증
                # ⭐ 민감정보 리스크 방지: preview 대신 hash 위주로 변경
                compressed_type = type(logic_summary_compressed).__name__
                compressed_len = len(logic_summary_compressed) if logic_summary_compressed else 0
                compressed_hash = hashlib.md5(str(logic_summary_compressed).encode('utf-8')).hexdigest()[:8] if logic_summary_compressed else "None"
                compressed_is_none_or_empty = (logic_summary_compressed is None or 
                                               (isinstance(logic_summary_compressed, str) and len(logic_summary_compressed.strip()) == 0))
                
                logger.info(
                    f"[P0-H 디버깅] 압축 호출 직후: "
                    f"compressed_type={compressed_type}, "
                    f"compressed_len={compressed_len}, "
                    f"compressed_hash={compressed_hash}, "
                    f"compressed_is_none_or_empty={compressed_is_none_or_empty}"
                )
                
                # ⭐ P2: 압축 결과 guardrail - None/빈값이면 fallback으로 전환
                if compressed_is_none_or_empty:
                    logger.warning(
                        f"[P0-H 디버깅] 압축 결과가 None/빈값, fallback으로 전환: "
                        f"report_id={report_id[:16]}..."
                    )
                    # fallback 로직으로 전환 (아래 except 블록과 동일)
                    raise ValueError("압축 결과가 None/빈값")
                
                # 3. 압축 결과 검증 + guardrail
                out_len = len(logic_summary_compressed) if logic_summary_compressed else 0
                compressed_len = out_len
                compression_used = True
                ratio = out_len / len(logic_summary_raw) if logic_summary_raw else 0.0
                
                # ⭐ P2: 최소 길이 guardrail (40자 미만이면 fallback)
                min_chars = 40
                if out_len < min_chars and logic_summary_raw_len > min_chars:
                    logger.warning(
                        f"[P0-H 디버깅] 압축 결과가 너무 짧음 ({out_len}자 < {min_chars}자), fallback으로 전환"
                                        )
# fallback으로 전환
                    raise ValueError(f"압축 결과가 너무 짧음: {out_len}자")
                
                # ✅ P0-4: 260자 하드코딩 제거 → 문장 단위 자르기
                if out_len > MAX_LOGIC_SUMMARY_LEN or ratio > 0.75:
                    # key_sentence 기반 문장 단위 자르기
                    if key_sentence and len(key_sentence) > 15:
                        logic_summary = truncate_to_sentence(key_sentence, MAX_LOGIC_SUMMARY_LEN)
                    else:
                        # ⭐ P0-3: 원문 fallback 시 문장 단위 자르기
                        cleaned = re.sub(r'[\n\r]+', ' ', logic_summary_raw)
                        cleaned = re.sub(r'[•\-\*○▶→]\s*', '', cleaned)
                        cleaned = re.sub(r'\|.*?\|', '', cleaned)
                        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
                        logic_summary = truncate_to_sentence(cleaned, MAX_LOGIC_SUMMARY_LEN)
                else:
                    logic_summary = logic_summary_compressed
                    logger.info(f"logic_summary 압축 성공: {len(logic_summary_raw)}자 → {out_len}자 (ratio: {ratio:.2f})")
                    
            except Exception as e:
                logger.warning(f"logic_summary 압축 실패, key_sentence 기반으로 fallback: {e}")
                # ✅ P0-4: Fallback: 문장 단위 자르기
                if key_sentence and len(key_sentence) > 15:
                    logic_summary = truncate_to_sentence(key_sentence, MAX_LOGIC_SUMMARY_LEN)
                else:
                    logic_summary = truncate_to_sentence(logic_summary_raw, MAX_LOGIC_SUMMARY_LEN)
        
        # ⭐ P0-H 디버깅: 저장 직전 최종 값 확인
        # ⭐ 민감정보 리스크 방지: preview 제거, hash 위주로 변경
        final_len_before_save = len(logic_summary) if logic_summary else 0
        final_hash = hashlib.md5(str(logic_summary).encode('utf-8')).hexdigest()[:8] if logic_summary else "None"
        
        # ⭐ P0-1: final_len == 0 가드레일 (저장 방지)
        if final_len_before_save == 0:
            logger.error(
                f"[P0-H 경고] final_len이 0입니다! 저장을 차단합니다. "
                f"raw_len={logic_summary_raw_len}, "
                f"compressed_len={compressed_len}, "
                f"report_id={report_id[:16]}..."
            )
            # Fallback: clean 버전 또는 원문 첫 200자 사용
            if 'logic_summary_clean' in locals() and logic_summary_clean and len(logic_summary_clean) > 0:
                logic_summary = logic_summary_clean[:200].strip() + "..."
                final_len_before_save = len(logic_summary)
                logger.warning(f"[P0-H 복구] clean 버전으로 fallback: len={final_len_before_save}")
            elif logic_summary_raw and len(logic_summary_raw) > 0:
                logic_summary = logic_summary_raw[:200].strip() + "..."
                final_len_before_save = len(logic_summary)
                logger.warning(f"[P0-H 복구] raw 버전으로 fallback: len={final_len_before_save}")
            else:
                logger.error(f"[P0-H 실패] 저장 불가: 모든 fallback 실패")
                return (SaveResult.SKIPPED_EMPTY, "all_fallback_failed")
        
        # ⭐ P0-3: 메타 정보 추가 (실용적 메타 - 한국어 환경 고려)
        import re
        if logic_summary:
            sentence_count = len(re.split(r'[.!?]\s+', logic_summary))
            word_count_est = len(logic_summary.split())  # 대략치 명시
            newline_count = logic_summary.count('\n')
            comma_count = logic_summary.count(',')
            bullet_like_count = sum(logic_summary.count(c) for c in ['•', '-', '*', '1)', '2)'])
            contains_url = bool(re.search(r'https?://', logic_summary))
            parentheses_ratio = (logic_summary.count('(') + logic_summary.count(')')) / max(len(logic_summary), 1)
        else:
            sentence_count = word_count_est = newline_count = comma_count = bullet_like_count = 0
            contains_url = False
            parentheses_ratio = 0.0
        
        # ⭐ P0-H 디버깅: 저장 직전 로그 출력 (강화 - 결정적 관측 5종 세트)
        logger.info(
            f"[P0-H 디버깅] 저장 직전 최종: "
            f"raw_len={logic_summary_raw_len}, "
            f"compressed_len={compressed_len}, "
            f"final_len={final_len_before_save}, "
            f"final_sentence_count={sentence_count}, "
            f"final_word_count_est={word_count_est}, "
            f"final_newline_count={newline_count}, "
            f"final_comma_count={comma_count}, "
            f"final_bullet_like_count={bullet_like_count}, "
            f"final_contains_url={contains_url}, "
            f"final_parentheses_ratio={parentheses_ratio:.2f}, "
            f"final_hash={final_hash}, "
            f"compress_called={compress_called}, "
            f"compression_used={compression_used}, "
            f"gate_passed={gate_passed}, "
            f"max_length={MAX_LOGIC_SUMMARY_LENGTH}, "
            f"report_id={report_id[:16]}..."
        )
        
        # ⭐ logic_fingerprint 계산 (압축된 logic_summary 기반)
        logic_fingerprint = None
        if logic_summary:
            normalized_logic = normalize_text_for_fingerprint(logic_summary)
            logic_fingerprint = hashlib.sha256(normalized_logic.encode('utf-8')).hexdigest()[:16]
            
            # ⭐ P0-H 디버깅: 압축 후 중복 체크 (압축된 logic_summary 기반)
            # ⭐ 수정: (target_sector_code, logic_fingerprint) 조합만 체크 (driver_code 제거)
            existing = db.query(IndustryEdge).filter(
                IndustryEdge.target_sector_code == target_sector_code
            ).all()
            
            # 기존 레코드의 logic_summary와 비교하여 논리적으로 동일한지 확인
            for existing_edge in existing:
                existing_logic = existing_edge.logic_summary or ""
                if existing_logic:
                    # 동일한 정규화 적용
                    normalized_existing = normalize_text_for_fingerprint(existing_logic)
                    existing_logic_fp = hashlib.sha256(normalized_existing.encode('utf-8')).hexdigest()[:16]
                    if existing_logic_fp == logic_fingerprint:
                        # ⭐ P0-2: FOR UPDATE로 동시성 제어 (lost update 방지)
                        from sqlalchemy import and_
                        locked_edge = db.query(IndustryEdge).filter(
                            IndustryEdge.id == existing_edge.id
                        ).with_for_update().first()
                        
                        if not locked_edge:
                            logger.warning(f"[Evidence 누적] 락 획득 실패: edge_id={existing_edge.id}")
                            return (SaveResult.SKIPPED_DEDUPE, f"lock_failed:edge_id={existing_edge.id}")
                        
                        # ⭐ P0-5: sources 스키마 고정 (SourceItem 계약)
                        # 필수: report_id, broker, report_date, added_at
                        # 선택: title, url, source_type
                        sources = locked_edge.conditions.get("sources", []) if locked_edge.conditions else []
                        broker_name = report_metadata.get("broker", "unknown")
                        report_date_raw = report_metadata.get("report_date", "")
                        report_title = report_metadata.get("title", "")
                        report_url = report_metadata.get("url", "") or report_metadata.get("pdf_url", "")
                        source_type = report_metadata.get("source_type", "NAVER_PDF")
                        
                        # ⭐ (C) report_date 정규화 (YYYY-MM-DD 고정) - 실패 시 None
                        def normalize_date(date_str: str) -> Tuple[Optional[str], bool]:
                            """
                            다양한 날짜 형식을 YYYY-MM-DD로 정규화
                            Returns: (정규화된 날짜 또는 None, 정규화 실패 여부)
                            """
                            if not date_str:
                                return (None, False)  # 원본이 없으면 실패 아님
                            import re
                            # 이미 YYYY-MM-DD 형식이면 그대로
                            if re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
                                return (date_str, False)
                            # YY.MM.DD 또는 YYYY.MM.DD
                            m = re.match(r'^(\d{2,4})\.(\d{2})\.(\d{2})$', date_str)
                            if m:
                                year = m.group(1)
                                if len(year) == 2:
                                    year = "20" + year  # 2000년대 가정
                                return (f"{year}-{m.group(2)}-{m.group(3)}", False)
                            # YYYYMMDD
                            m = re.match(r'^(\d{4})(\d{2})(\d{2})$', date_str)
                            if m:
                                return (f"{m.group(1)}-{m.group(2)}-{m.group(3)}", False)
                            # ⭐ 파싱 실패 시 None 반환 + 실패 플래그 True
                            logger.warning(f"[날짜 정규화 실패] 원본: '{date_str}' → None 처리")
                            return (None, True)
                        
                        report_date_norm, date_normalize_failed = normalize_date(report_date_raw)
                        
                        # ⭐ source_uid 패턴 (소스 확장 대비)
                        source_uid = f"{source_type}:{report_id}"
                        
                        new_source = {
                            "source_uid": source_uid,  # ⭐ 유니크 식별자 (source_type:report_id)
                            "report_id": report_id,
                            "broker": broker_name,
                            "report_date": report_date_norm,  # ⭐ 정규화된 날짜 (None 가능)
                            "date_normalize_failed": date_normalize_failed,  # ⭐ 정규화 실패 플래그
                            "title": report_title,
                            "url": report_url,
                            "source_type": source_type,
                            "added_at": datetime.now().isoformat()
                        }
                        
                        # ⭐ 중복 방지: source_uid 기준 (소스 확장 대비)
                        existing_source_uids = [s.get("source_uid") or f"{s.get('source_type', 'UNKNOWN')}:{s.get('report_id')}" for s in sources if isinstance(s, dict)]
                        if source_uid not in existing_source_uids:
                            sources.append(new_source)
                            
                            # ⭐ P0-4: 정렬 기준 고정 (report_date 우선, None은 맨 뒤) - 최신 먼저
                            def sort_key(s):
                                rd = s.get("report_date")  # None 허용
                                aa = s.get("added_at", "") or ""
                                # None은 "0000-00-00"으로 처리하여 맨 뒤로
                                return (rd if rd else "0000-00-00", aa)
                            
                            sources = sorted(sources, key=sort_key, reverse=True)
                            
                            # ⭐ (B) cap을 evict(DROP_OLDEST)로 변경 - 최신성 보장
                            SOURCES_CAP = 50
                            evicted_count = 0
                            if len(sources) > SOURCES_CAP:
                                evicted_count = len(sources) - SOURCES_CAP
                                sources = sources[:SOURCES_CAP]  # 최신 50개만 유지
                                logger.info(
                                    f"[Evidence 누적] sources cap 초과, 오래된 {evicted_count}개 제거: "
                                    f"edge_id={locked_edge.id}, final_count={len(sources)}"
                                )
                            
                            # conditions에 sources 업데이트
                            if not locked_edge.conditions:
                                locked_edge.conditions = {}
                            locked_edge.conditions["sources"] = sources
                            locked_edge.updated_at = datetime.now()
                            db.commit()
                            
                            # ⭐ P0-H: enum으로 직접 분기 (문자열 파싱 제거)
                            if evicted_count > 0:
                                save_result = SaveResult.SKIPPED_DEDUPE_EVICTED
                                logger.info(
                                    f"[Evidence 누적] 중복 논리에 출처 추가 (EVICTED): "
                                    f"edge_id={locked_edge.id}, new_report={report_id}, "
                                    f"total_sources={len(sources)}, evicted={evicted_count}"
                                )
                            else:
                                save_result = SaveResult.SKIPPED_DEDUPE_APPENDED
                                logger.info(
                                    f"[Evidence 누적] 중복 논리에 출처 추가 (APPENDED): "
                                    f"edge_id={locked_edge.id}, new_report={report_id}, "
                                    f"total_sources={len(sources)}"
                                )
                            return (save_result, f"edge_id={locked_edge.id}:sources={len(sources)}:evicted={evicted_count}")
                        else:
                            logger.info(
                                f"[P0-H 디버깅] 압축 후 중복 감지, 스킵 (출처 이미 존재): "
                                f"report_id={report_id}, driver={source_driver_code}, sector={target_sector_code}"
                            )
                            # ⭐ P0-H: NOOP enum 사용
                            return (SaveResult.SKIPPED_DEDUPE_NOOP, f"edge_id={locked_edge.id}:already_exists")
        
        # ⭐ Step C: Idempotent Upsert 정책 - 기존 edge 존재 여부 확인
        # ⭐ P1: 키 설계 수정 - fingerprint 제거 (증식 방지)
        # 기본 키: report_id + target_sector_code (+ source_driver_code)
        # fingerprint는 버전/변경 감지로만 활용 (키에 포함하지 않음)
        from sqlalchemy import and_
        existing_edge = db.query(IndustryEdge).filter(
            and_(
                IndustryEdge.report_id == report_id,
                IndustryEdge.target_sector_code == target_sector_code,
                IndustryEdge.source_driver_code == source_driver_code  # ⭐ driver_code도 키에 포함
            )
        ).first()
        
        # ⭐ P1: 기존 edge의 fingerprint와 비교하여 내용 변경 감지
        content_changed = False
        if existing_edge and existing_edge.logic_fingerprint:
            content_changed = (existing_edge.logic_fingerprint != logic_fingerprint)
            if content_changed:
                logger.info(
                    f"[P0-H 디버깅] 기존 edge 내용 변경 감지: "
                    f"기존_fp={existing_edge.logic_fingerprint}, "
                    f"새_fp={logic_fingerprint}"
                )
        
        is_update = False
        if existing_edge:
            # ⭐ Idempotent Upsert: 기존 edge 업데이트
            is_update = True
            logger.info(
                f"[P0-H 디버깅] 기존 edge 발견, 업데이트 경로: "
                f"edge_id={existing_edge.id}, "
                f"기존_len={len(existing_edge.logic_summary) if existing_edge.logic_summary else 0}, "
                f"새_len={final_len_before_save}, "
                f"내용_변경={content_changed}, "
                f"조회_키=(report_id={report_id[:16]}..., sector={target_sector_code}, driver={source_driver_code})"
            )
            
            # ⭐ P1: 업데이트 merge policy - 새 요약이 gate 통과/품질 기준 만족할 때만 overwrite
            # 현재는 항상 업데이트 (나중에 품질 기준 추가 가능)
            existing_edge.logic_summary = logic_summary
            existing_edge.logic_fingerprint = logic_fingerprint
            existing_edge.updated_at = datetime.now()
            # source_driver_code는 이미 키에 포함되어 있으므로 업데이트 불필요
            
            industry_edge = existing_edge
        else:
            # 새 edge 생성
            is_update = False
            
            # ⭐ P0-Final: CREATED 시 sources 초기화 (현재 report 1개로 시작)
            # sources>=3 게이트 달성을 위해 필수
            # 정책: CREATED=sources[1개], DEDUPE_APPENDED=sources에 추가
            initial_conditions = industry_logic.get("conditions", {})
            
            # report_date 정규화 (인라인)
            report_date_raw = report_metadata.get("report_date", "")
            report_date_norm = report_date_raw  # 기본값
            import re as re_inline
            if report_date_raw:
                if re_inline.match(r'^\d{4}-\d{2}-\d{2}$', report_date_raw):
                    report_date_norm = report_date_raw
                else:
                    m = re_inline.match(r'^(\d{2,4})\.(\d{2})\.(\d{2})$', report_date_raw)
                    if m:
                        year = m.group(1)
                        if len(year) == 2:
                            year = "20" + year
                        report_date_norm = f"{year}-{m.group(2)}-{m.group(3)}"
                    else:
                        m = re_inline.match(r'^(\d{4})(\d{2})(\d{2})$', report_date_raw)
                        if m:
                            report_date_norm = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
            
            # ⭐ SourceItem 스키마 (DEDUPE_APPENDED와 동일하게 통일)
            # 키: report_id, broker, report_date, title, url, source_type, added_at
            broker_name = report_metadata.get("broker_name", "")
            report_title = report_metadata.get("title", "")
            report_url = report_metadata.get("url", "") or report_metadata.get("pdf_url", "")
            source_type = report_metadata.get("source_type", "NAVER_PDF")
            
            # ⭐ source_uid 패턴 (소스 확장 대비)
            source_uid = f"{source_type}:{report_id}"
            
            initial_source = {
                "source_uid": source_uid,  # ⭐ 유니크 식별자 (source_type:report_id)
                "report_id": report_id,
                "broker": broker_name,  # ⭐ DEDUPE와 동일하게 'broker'로 통일
                "report_date": report_date_norm,
                "title": report_title[:200] if report_title else "",  # 최대 200자
                "url": report_url,
                "source_type": source_type,
                "added_at": datetime.now().isoformat() + "Z"
            }
            initial_conditions["sources"] = [initial_source]
            
            industry_edge = IndustryEdge(
                report_id=report_id,
                source_driver_code=source_driver_code,  # ⭐ 소스: 드라이버 코드
                target_sector_code=target_sector_code,  # ⭐ 타겟: 섹터 코드
                target_type=industry_logic.get("target_type", "SECTOR"),
                relation_type="INDUSTRY_DRIVEN_BY",  # ⭐ 관계 타입 고정
                logic_summary=logic_summary,  # ⭐ 압축된 버전
                conditions=initial_conditions,  # ⭐ sources 초기화 포함
                key_sentence=key_sentence,
                extraction_confidence="MED",  # KIRS 리포트는 중간 신뢰도
                valid_from=valid_from,
                valid_to=valid_to,
                logic_fingerprint=logic_fingerprint  # ⭐ 추가: 논리 요약 해시
            )
            
            logger.info(
                f"[P0-Final] 새 edge 생성 시 sources 초기화: "
                f"report_id={report_id[:16]}..., sources_count=1"
            )
        
        try:
            if not is_update:
                db.add(industry_edge)
            db.flush()  # ⭐ P0-H 디버깅: flush 후 값 확인
            
            # ⭐ P0-H 디버깅: flush 직후 값 확인
            if hasattr(industry_edge, 'id') and industry_edge.id:
                db.refresh(industry_edge)
                db_len_after_flush = len(industry_edge.logic_summary) if industry_edge.logic_summary else 0
                logger.info(
                    f"[P0-H 디버깅] flush 직후: "
                    f"db_len={db_len_after_flush}, "
                    f"is_update={is_update}, "
                    f"edge_id={industry_edge.id}"
                )
            
            db.commit()
            
            # ⭐ P0-H 디버깅: commit 직후 재조회로 최종 확인 (같은 세션 + 새 세션)
            if hasattr(industry_edge, 'id') and industry_edge.id:
                # 같은 세션 재조회
                db.refresh(industry_edge)
                db_len_same_session = len(industry_edge.logic_summary) if industry_edge.logic_summary else 0
                
                # 새 세션으로 재조회 (캐시 무효화)
                from app.db import SessionLocal as NewSessionLocal
                new_db = NewSessionLocal()
                try:
                    fresh_edge = new_db.query(IndustryEdge).filter(IndustryEdge.id == industry_edge.id).first()
                    db_len_new_session = len(fresh_edge.logic_summary) if fresh_edge and fresh_edge.logic_summary else 0
                    
                    logger.info(
                        f"[P0-H 디버깅] commit 직후 재조회: "
                        f"db_len_same_session={db_len_same_session}, "
                        f"db_len_new_session={db_len_new_session}, "
                        f"final_len_before_save={final_len_before_save}, "
                        f"일치 여부(같은 세션)={'일치' if db_len_same_session == final_len_before_save else '불일치'}, "
                        f"일치 여부(새 세션)={'일치' if db_len_new_session == final_len_before_save else '불일치'}"
                    )
                finally:
                    new_db.close()
        except Exception as db_error:
            # ⭐ DB 제약 충돌 시 중복으로 처리 (UNIQUE 제약 위반)
            db.rollback()
            if "unique_industry_edge_sector_logic" in str(db_error) or "duplicate key" in str(db_error).lower():
                logger.info(
                    f"Industry Insight DB 제약 충돌 (중복으로 처리): "
                    f"report_id={report_id}, sector={target_sector_code}, logic_fp={logic_fingerprint}"
                )
                return (SaveResult.SKIPPED_DB_CONFLICT, f"db_unique_constraint:{target_sector_code}")
            else:
                raise  # 다른 오류는 재발생
        
        result_type = SaveResult.UPDATED if is_update else SaveResult.CREATED
        logger.info(f"Industry Insight 저장 완료 ({result_type.value}): report_id={report_id}, {source_driver_code}→{target_sector_code}")
        return (result_type, None)
    
    except Exception as e:
        logger.error(f"Industry Insight 저장 실패: {e}", exc_info=True)
        db.rollback()
        return (SaveResult.FAILED, str(e))


def find_industry_edges_for_rebuild(
    db: Session,
    report: Dict[str, Any],
    report_metadata: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Rebuild 대상 Industry Edge 조회 (fallback 로직 포함)
    
    ⭐ P0-X1: report_id 단일 의존 제거, 2중/3중 키로 fallback
    ⭐ P0-추가2: 구조화된 반환값 (edges + 메타데이터)
    
    Args:
        db: DB 세션
        report: 리포트 데이터
        report_metadata: 리포트 메타데이터
    
    Returns:
        {
            "edges": List[IndustryEdge],
            "matched_by": str | None,  # "report_id" | "fuzzy" | "recent_fuzzy" | None
            "skipped_reason": str | None,  # "MULTIPLE_CANDIDATES" | "DATE_TOO_FAR" | "SOURCE_MISMATCH" | "LOW_SIMILARITY" | None
            "candidate_count": int,
            "similarity_score": float | None
        }
    """
    result = {
        "edges": [],
        "matched_by": None,
        "skipped_reason": None,
        "candidate_count": 0,
        "similarity_score": None
    }
    from app.models.broker_report import BrokerReport
    from sqlalchemy import and_, or_
    from difflib import SequenceMatcher
    
    # 1순위: report_id로 직접 조회
    report_id = report.get("report_id", "")
    if report_id:
        edges = db.query(IndustryEdge).filter(
            IndustryEdge.report_id == report_id
        ).all()
        if edges:
            result["edges"] = edges
            result["matched_by"] = "report_id"
            logger.info(f"Rebuild 대상 조회 성공 (report_id): {len(edges)}개")
            return result
    
    # 2순위: broker_reports에서 report_id 찾아 연결
    broker_name = report.get("broker_name", "") or report_metadata.get("broker_name", "")
    report_title = report.get("title", "") or report.get("report_title", "")
    report_date_str = report.get("date", "") or report_metadata.get("report_date", "")
    report_url = report.get("url", "") or report.get("report_url", "")
    
    if broker_name and report_title:
        # broker_reports에서 매칭되는 리포트 찾기
        broker_report_query = db.query(BrokerReport).filter(
            BrokerReport.broker_name == broker_name
        )
        
        # ⭐ P0-B 가드 1: source 일치 강제
        source = report.get("source", "") or report_metadata.get("source", "")
        if source:
            broker_report_query = broker_report_query.filter(
                BrokerReport.source == source
            )
        
        # 제목 유사도로 필터링
        matching_reports = []
        for br in broker_report_query.all():
            title_similarity = SequenceMatcher(None, br.report_title or "", report_title).ratio()
            if title_similarity > 0.8:  # 80% 이상 유사
                # ⭐ P0-B 가드 2: 날짜 차이 체크 (±3일)
                if report_date_str:
                    try:
                        report_date = datetime.strptime(report_date_str, "%Y-%m-%d")
                        br_date = br.report_date
                        if br_date:
                            date_diff = abs((report_date.date() - br_date).days)
                            if date_diff > 3:  # 3일 초과 차이면 제외
                                logger.debug(
                                    f"Rebuild 매칭 제외: 날짜 차이 {date_diff}일 "
                                    f"(report_date={report_date_str}, br_date={br_date})"
                                )
                                continue
                    except (ValueError, TypeError) as e:
                        logger.debug(f"날짜 파싱 실패, 날짜 체크 스킵: {e}")
                
                matching_reports.append((br, title_similarity))
        
        # ⭐ P0-B 가드 3: 매칭 후보가 2개 이상이면 HOLD (스킵)
        if len(matching_reports) > 1:
            result["skipped_reason"] = "MULTIPLE_CANDIDATES"
            result["candidate_count"] = len(matching_reports)
            logger.warning(
                f"Rebuild 매칭 후보 다중 발견 ({len(matching_reports)}개), "
                f"안전을 위해 스킵: report_title={report_title[:50]}"
            )
            return result  # 안전을 위해 스킵
        
        if matching_reports:
            # 가장 유사한 리포트 선택
            matching_reports.sort(key=lambda x: x[1], reverse=True)
            matched_report_id = matching_reports[0][0].report_id
            similarity_score = matching_reports[0][1]
            
            edges = db.query(IndustryEdge).filter(
                IndustryEdge.report_id == matched_report_id
            ).all()
            if edges:
                result["edges"] = edges
                result["matched_by"] = "fuzzy"
                result["similarity_score"] = similarity_score
                logger.info(f"Rebuild 대상 조회 성공 (broker_name + title 유사도): {len(edges)}개")
                return result
    
    # 3순위: 최근 N일 + source=naver + title 유사 (안전한 범위 제한)
    from datetime import datetime, timedelta
    cutoff_date = datetime.now() - timedelta(days=7)
    source = report.get("source", "") or report_metadata.get("source", "naver")
    
    if report_title:
        # 최근 7일 이내 네이버 리포트에서 제목 유사도로 찾기
        recent_broker_reports = db.query(BrokerReport).filter(
            and_(
                BrokerReport.source == source,
                BrokerReport.created_at >= cutoff_date
            )
        ).all()
        
        matching_reports = []
        for br in recent_broker_reports:
            title_similarity = SequenceMatcher(None, br.report_title or "", report_title).ratio()
            if title_similarity > 0.7:  # 70% 이상 유사
                # ⭐ P0-B 가드 2: 날짜 차이 체크 (±3일)
                if report_date_str:
                    try:
                        report_date = datetime.strptime(report_date_str, "%Y-%m-%d")
                        br_date = br.report_date
                        if br_date:
                            date_diff = abs((report_date.date() - br_date).days)
                            if date_diff > 3:  # 3일 초과 차이면 제외
                                logger.debug(
                                    f"Rebuild 매칭 제외: 날짜 차이 {date_diff}일 "
                                    f"(report_date={report_date_str}, br_date={br_date})"
                                )
                                continue
                    except (ValueError, TypeError) as e:
                        logger.debug(f"날짜 파싱 실패, 날짜 체크 스킵: {e}")
                
                matching_reports.append((br, title_similarity))
        
        # ⭐ P0-B 가드 3: 매칭 후보가 2개 이상이면 HOLD (스킵)
        if len(matching_reports) > 1:
            result["skipped_reason"] = "MULTIPLE_CANDIDATES"
            result["candidate_count"] = len(matching_reports)
            logger.warning(
                f"Rebuild 매칭 후보 다중 발견 ({len(matching_reports)}개), "
                f"안전을 위해 스킵: report_title={report_title[:50]}"
            )
            return result  # 안전을 위해 스킵
        
        if matching_reports:
            matching_reports.sort(key=lambda x: x[1], reverse=True)
            matched_report_id = matching_reports[0][0].report_id
            similarity_score = matching_reports[0][1]
            
            edges = db.query(IndustryEdge).filter(
                IndustryEdge.report_id == matched_report_id
            ).all()
            if edges:
                result["edges"] = edges
                result["matched_by"] = "recent_fuzzy"
                result["similarity_score"] = similarity_score
                logger.info(f"Rebuild 대상 조회 성공 (최근 7일 + title 유사도): {len(edges)}개")
                return result
    
    logger.warning(f"Rebuild 대상 조회 실패: report_id={report_id}, broker_name={broker_name}, title={report_title[:50]}")
    result["skipped_reason"] = "NO_MATCH_FOUND"
    return result


def rebuild_industry_insight_logic_summary(
    db: Session,
    industry_edge: IndustryEdge,
    industry_logic: Dict[str, Any]
) -> bool:
    """
    기존 Industry Edge의 logic_summary만 재계산/업데이트
    
    ⭐ P0-X2: 원자적 업데이트 (logic_summary, logic_fingerprint, updated_at)
    ⭐ P0-1: 재처리 모드 - 텍스트 필드만 갱신 (KG edge는 안 건드림)
    
    Args:
        db: DB 세션
        industry_edge: 기존 Industry Edge
        industry_logic: 산업 Logic 데이터 (원본)
    
    Returns:
        성공 여부
    """
    try:
        import re
        import hashlib
        from utils.text_normalizer import normalize_text_for_fingerprint
        from utils.semantic_compression import compress_semantically
        
        logic_summary_raw = industry_logic.get("logic_summary", "")
        key_sentence = industry_logic.get("key_sentence", "")
        
        # ⭐ P0-H: logic_summary 저장 시점 압축/게이트 (save_industry_insight와 동일 로직)
        logic_summary = logic_summary_raw  # 기본값
        
        if logic_summary_raw and len(logic_summary_raw) > 350:
            try:
                # 1. bullet/table/template 제거 전처리
                logic_summary_clean = logic_summary_raw
                template_patterns = [
                    r'투자유의.*?면책',
                    r'본.*?자료는.*?제공.*?목적',
                    r'무단복제.*?배포.*?금지',
                    r'담당자.*?연락처',
                    r'Analyst.*?@.*?\.com',
                    r'ComplianceNotice'
                ]
                for pattern in template_patterns:
                    logic_summary_clean = re.sub(pattern, '', logic_summary_clean, flags=re.IGNORECASE | re.DOTALL)
                
                # 2. semantic compression 수행
                logic_summary_compressed = compress_semantically(
                    industry_logic=logic_summary_clean,
                    key_sentence=key_sentence,
                    logic_summary=logic_summary_clean,
                    max_sentences=2,
                    max_length=260
                )
                
                # 3. 압축 결과 검증
                out_len = len(logic_summary_compressed)
                ratio = out_len / len(logic_summary_raw) if logic_summary_raw else 0.0
                
                # ✅ P0-4: 260자 하드코딩 제거 → 문장 단위 자르기
                if out_len > MAX_LOGIC_SUMMARY_LEN or ratio > 0.75:
                    if key_sentence and len(key_sentence) > 15:
                        logic_summary = truncate_to_sentence(key_sentence, MAX_LOGIC_SUMMARY_LEN)
                    else:
                        # 원문 fallback 시 문장 단위 자르기
                        cleaned = re.sub(r'[\n\r]+', ' ', logic_summary_raw)
                        cleaned = re.sub(r'[•\-\*○▶→]\s*', '', cleaned)
                        cleaned = re.sub(r'\|.*?\|', '', cleaned)
                        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
                        logic_summary = truncate_to_sentence(cleaned, MAX_LOGIC_SUMMARY_LEN)
                else:
                    logic_summary = logic_summary_compressed
                    logger.info(f"logic_summary 압축 성공: {len(logic_summary_raw)}자 → {out_len}자 (ratio: {ratio:.2f})")
                    
            except Exception as e:
                logger.warning(f"logic_summary 압축 실패, key_sentence 기반으로 fallback: {e}")
                # ✅ P0-4: Fallback: 문장 단위 자르기
                if key_sentence and len(key_sentence) > 15:
                    logic_summary = truncate_to_sentence(key_sentence, MAX_LOGIC_SUMMARY_LEN)
                else:
                    logic_summary = truncate_to_sentence(logic_summary_raw, MAX_LOGIC_SUMMARY_LEN)
        
        # ⭐ P0-X2: 원자적 업데이트 (트랜잭션으로 edge 단위 commit)
        # logic_summary 업데이트
        industry_edge.logic_summary = logic_summary
        
        # logic_fingerprint 재계산
        if logic_summary:
            normalized_logic = normalize_text_for_fingerprint(logic_summary)
            logic_fingerprint = hashlib.sha256(normalized_logic.encode('utf-8')).hexdigest()[:16]
            industry_edge.logic_fingerprint = logic_fingerprint
        else:
            industry_edge.logic_fingerprint = None
        
        # updated_at 업데이트
        industry_edge.updated_at = datetime.now()
        
        # edge 단위 commit
        db.commit()
        logger.info(f"Industry Insight logic_summary 재빌드 완료: {industry_edge.id} (report_id={industry_edge.report_id})")
        return True
        
    except Exception as e:
        logger.error(f"Industry Insight 재빌드 실패: {e}", exc_info=True)
        db.rollback()
        return False


def enrich_company_edge_with_kirs(
    db: Session,
    ticker: str,
    driver_code: Optional[str],
    company_logic: Dict[str, Any],
    report_metadata: Dict[str, Any],
    section_fingerprint: Optional[str] = None
) -> tuple[bool, str]:
    """
    KIRS 기업 리포트를 edges 테이블에 연결
    
    ⭐ P0+ 보강: TECH_MOAT를 mechanism이 아닌 힌트 타입으로만 저장
    - relation_type은 DRIVEN_BY 유지
    - evidence_layer에 source_type="KIRS_HINT", hint_type="MOAT" 추가
    - 유효기간 2-3년으로 설정 (기술 해자는 장기적)
    
    Args:
        db: DB 세션
        ticker: 티커
        driver_code: 드라이버 코드 (선택)
        company_logic: 기업 Logic 데이터
        report_metadata: 리포트 메타데이터
        section_fingerprint: 섹션 단위 fingerprint (dedup용)
    
    Returns:
        (성공 여부, alignment)
    """
    from extractors.edge_enricher import enrich_edge_with_evidence
    import hashlib
    
    # driver_code가 없으면 첫 번째 target_driver 사용
    if not driver_code and company_logic.get("target_drivers"):
        driver_code = company_logic["target_drivers"][0]
    
    if not driver_code:
        logger.warning(f"KIRS 리포트에 드라이버 코드 없음: ticker={ticker}")
        return False, None
    
    # ⭐ 섹션 단위 dedup: 같은 리포트의 같은 섹션 fingerprint가 이미 있으면 스킵
    if section_fingerprint:
        from app.models.edge import Edge
        edge_id = f"{ticker}_{driver_code}_DRIVEN_BY"
        edge = db.query(Edge).filter(Edge.id == edge_id).first()
        
        if edge:
            props = edge.properties or {}
            evidence_layer = props.get("evidence_layer", [])
            # 같은 섹션 fingerprint가 이미 있는지 확인
            for evidence in evidence_layer:
                if isinstance(evidence, dict) and evidence.get("section_fingerprint") == section_fingerprint:
                    logger.info(f"KIRS 기업 섹션 중복 감지 (섹션 fingerprint), 스킵: ticker={ticker}")
                    return True, evidence.get("alignment", "UNKNOWN")
    
    # Insight 형식 변환 (compliance: PARAPHRASE 중심)
    analyst_logic = company_logic.get("analyst_logic", "")
    key_sentence = company_logic.get("key_sentence", "")
    
    # ⭐ Compliance: 원문 그대로가 아니라 요약/파라프레이즈
    # 이미 extract_company_logic에서 요약된 형태이므로 그대로 사용
    
    insights = {
        "analyst_logic": analyst_logic[:300] if len(analyst_logic) > 300 else analyst_logic,  # 최대 300자
        "conditions": company_logic.get("conditions", {}),
        "key_sentence": key_sentence[:100] if len(key_sentence) > 100 else key_sentence,  # 최대 100자
        "extraction_confidence": "MED",
        "tech_moats": company_logic.get("tech_moats", [])[:3],  # 최대 3개만
        "sentiment": "NEUTRAL",  # KIRS 리포트는 투자의견 없음
        # ⭐ KIRS 힌트 타입 표시
        "source_type": "KIRS_HINT",
        "hint_type": "MOAT",  # 기술 해자 힌트
        "section_fingerprint": section_fingerprint  # 섹션 단위 dedup용
    }
    
    # 리포트 메타데이터에 KIRS 표시 추가
    kirs_metadata = report_metadata.copy()
    kirs_metadata["source_type"] = "KIRS"
    kirs_metadata["hint_type"] = "MOAT"
    
    # ⭐ 유효기간 2-3년으로 설정 (기술 해자는 장기적)
    report_date_str = report_metadata.get("report_date", "")
    try:
        if report_date_str:
            from datetime import datetime
            report_date = datetime.strptime(report_date_str, "%Y-%m-%d")
            # 2년 유효 (기술 해자는 장기적)
            kirs_metadata["valid_to"] = (report_date + timedelta(days=730)).strftime("%Y-%m-%d")
        else:
            kirs_metadata["valid_to"] = (datetime.now() + timedelta(days=730)).strftime("%Y-%m-%d")
    except ValueError:
        kirs_metadata["valid_to"] = (datetime.now() + timedelta(days=730)).strftime("%Y-%m-%d")
    
    # Edge Enrichment 호출 (기존 DRIVEN_BY 엣지에 evidence_layer 추가)
    # ⚠️ 중요: mechanism을 변경하지 않음, 힌트 타입으로만 저장
    success, alignment = enrich_edge_with_evidence(
        db=db,
        ticker=ticker,
        driver_code=driver_code,
        insights=insights,
        report_metadata=kirs_metadata
    )
    
    return success, alignment

