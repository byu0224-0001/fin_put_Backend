"""
Semantic Compression 유틸리티

긴 논리 텍스트에서 전체 의미를 가장 잘 대표하는 문장 1~2개만 임베딩 거리로 선택
"""
import sys
from pathlib import Path
import logging
import re
import time
from typing import List, Tuple, Optional

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

logger = logging.getLogger(__name__)

# 임베딩 모델 설정
MODEL_NAME = "solar-embedding-1-large-passage"
MODEL_VERSION = "v0.1-rc2"
API_TIMEOUT = 30.0  # 초
MAX_RETRIES = 3
RETRY_DELAY_BASE = 2.0  # 지수 백오프 기본값

# 캐시 (개선된 키 사용)
_embedding_cache = {}  # cache_key 기준 캐싱

# 메트릭 (실패 모드 추적)
_metrics = {
    "api_fail_count": 0,
    "timeout_count": 0,
    "fallback_used_count": 0,
    "cache_hit_count": 0,
    "cache_miss_count": 0,
    # ⭐ P0.5-2: API 관측 지표
    "api_call_count": 0,  # API 호출 횟수
    "response_times": [],  # 응답 시간 리스트 (ms)
    "cache_hit_rate": 0.0,  # 캐시 히트율
    "fallback_rate": 0.0  # fallback 발생률
}


# ⭐ 체크: 동일 입력에 대한 중복 호출 방지 (동시성/재시도 중복 방지)
_encode_in_progress = set()  # 진행 중인 텍스트 해시

def _encode_with_retry(text: str, max_retries: int = MAX_RETRIES) -> Optional[List[float]]:
    """
    Solar Embedding API 호출 (재시도 로직 포함)
    
    ⭐ P0-1: 실패 모드 정의
    - timeout + retry (지수 백오프)
    - 실패 시 None 반환 (fallback으로 전환)
    - ⭐ 체크: 동일 입력에 대한 중복 호출 방지
    
    Args:
        text: 임베딩할 텍스트
        max_retries: 최대 재시도 횟수
    
    Returns:
        임베딩 벡터 또는 None (실패 시)
    """
    import hashlib
    from app.services.solar_embedding_model import encode_solar_embedding
    import numpy as np
    
    # 동일 입력 중복 호출 방지
    text_hash = hashlib.md5(text.encode()).hexdigest()
    if text_hash in _encode_in_progress:
        logger.warning(f"동일 입력에 대한 중복 호출 감지, 대기 중...")
        # 짧은 대기 후 재시도 (동시성 문제 완화)
        time.sleep(0.1)
        if text_hash in _encode_in_progress:
            logger.warning(f"중복 호출이 계속 진행 중, 스킵")
            return None
    
    _encode_in_progress.add(text_hash)
    
    try:
        start_time = time.time()
        _metrics["api_call_count"] += 1
        
        for attempt in range(max_retries):
            try:
                # timeout은 encode_solar_embedding 내부에서 처리됨
                embedding = encode_solar_embedding(text, batch_size=1, max_retries=1)
                
                # 응답 시간 기록
                response_time = (time.time() - start_time) * 1000  # ms
                _metrics["response_times"].append(response_time)
                
                if isinstance(embedding, np.ndarray):
                    return embedding.tolist()
                elif isinstance(embedding, list):
                    return embedding[0] if len(embedding) > 0 else None
                return None
            except Exception as e:
                error_str = str(e).lower()
                is_timeout = "timeout" in error_str or "timed out" in error_str
                
                if is_timeout:
                    _metrics["timeout_count"] += 1
                    logger.warning(f"Semantic Compression API timeout (시도 {attempt + 1}/{max_retries}): {e}")
                else:
                    _metrics["api_fail_count"] += 1
                    logger.warning(f"Semantic Compression API 실패 (시도 {attempt + 1}/{max_retries}): {e}")
                
                if attempt < max_retries - 1:
                    # 지수 백오프
                    delay = RETRY_DELAY_BASE * (2 ** attempt)
                    time.sleep(delay)
                else:
                    logger.error(f"Semantic Compression API 최종 실패: {e}")
                    return None
        
        return None
    finally:
        _encode_in_progress.discard(text_hash)


def compress_semantically(
    industry_logic: str,
    key_sentence: Optional[str] = None,
    logic_summary: Optional[str] = None,
    logic_fingerprint: Optional[str] = None,
    max_sentences: int = 2,
    max_length: int = 220
) -> str:
    """
    Semantic Compression: 의미 중심 문장 선택
    
    ⚠️ 중요: 이 함수는 "읽기 전용"입니다. KG/DB에 저장하지 않습니다.
    
    Args:
        industry_logic: 원본 논리 텍스트 (길고 상세)
        key_sentence: 이미 추출된 핵심 문장 (우선 포함)
        logic_summary: 요약형 텍스트 (의미 벡터 생성용)
        logic_fingerprint: 캐싱 키 (선택적)
        max_sentences: 최대 선택 문장 수 (기본 2개)
        max_length: 최대 길이 (기본 220자)
    
    Returns:
        압축된 문장 (1~2개)
    """
    # ⭐ P0: 전처리 (줄바꿈, 특수문자 제거)
    original_length = len(industry_logic) if industry_logic else 0
    
    # 전처리: 줄바꿈, 탭, 연속 공백 정리
    if industry_logic:
        # 줄바꿈/탭을 공백으로 변환
        industry_logic = re.sub(r'[\n\r\t]+', ' ', industry_logic)
        # 연속 공백을 단일 공백으로
        industry_logic = re.sub(r'\s+', ' ', industry_logic).strip()
        # 불필요한 특수문자 제거 (이메일, URL 등은 유지하되 정리)
        industry_logic = re.sub(r'[^\w\s가-힣.,!?%()\-:@/]', '', industry_logic)
    
    if key_sentence:
        key_sentence = re.sub(r'[\n\r\t]+', ' ', key_sentence)
        key_sentence = re.sub(r'\s+', ' ', key_sentence).strip()
    
    if logic_summary:
        logic_summary = re.sub(r'[\n\r\t]+', ' ', logic_summary)
        logic_summary = re.sub(r'\s+', ' ', logic_summary).strip()
    
    # ⭐ P0-2: 개선된 캐시 키 생성
    cache_key = None
    if logic_fingerprint:
        cache_key = f"{logic_fingerprint}|model:{MODEL_NAME}|v:{MODEL_VERSION}|len:{max_length}|sent:{max_sentences}"
    
    # 캐싱 확인
    if cache_key and cache_key in _embedding_cache:
        cached_result = _embedding_cache[cache_key]
        _metrics["cache_hit_count"] += 1
        logger.debug(f"Semantic Compression 캐시 히트: {cache_key[:50]}...")
        # 메트릭 기록 (cache도 동일한 게이트 적용)
        out_len = len(cached_result)
        ratio = out_len / original_length if original_length > 0 else 0.0
        
        if original_length < 200:
            gate_passed = out_len <= 180
        else:
            gate_passed = (ratio <= 0.65) or (out_len <= 220)
        
        compression_effective = (ratio <= 0.85) or (out_len <= 260)
        
        # ⭐ Fix 2: compression_input_source 메트릭 추가 (cache)
        compression_input_source_cache = "unknown"
        if logic_summary and len(logic_summary) > 200:
            compression_input_source_cache = "logic_summary_raw"
        elif key_sentence and len(key_sentence) > 200:
            compression_input_source_cache = "key_sentence"
        elif industry_logic and len(industry_logic) > 200:
            compression_input_source_cache = "industry_logic"
        else:
            compression_input_source_cache = "short_text"
        
        _metrics.setdefault("compression_stats", []).append({
            "compression_used": "cache",
            "in_len": original_length,
            "out_len": out_len,
            "ratio": ratio,
            "gate_passed": gate_passed,
            "compression_effective": compression_effective,
            "compression_input_source": compression_input_source_cache  # ⭐ Fix 2 추가
        })
        return cached_result
    
    _metrics["cache_miss_count"] += 1
    
    try:
        # 3. 문장 풀(pool) 구성
        sentences = _build_sentence_pool(industry_logic, key_sentence)
        
        if len(sentences) == 0:
            return industry_logic[:max_length] if industry_logic else ""
        
        # ⭐ P0-F: Solar Embedding 비용/지연 제어 (3단계 라우팅)
        # 조건 1: in_len < 120 (짧은 건 굳이 임베딩 압축할 가치 낮음)
        if original_length < 120:
            logger.debug(f"입력 길이 짧음 ({original_length}자), rule-based로 종료")
            _metrics["solar_skipped_short"] = _metrics.get("solar_skipped_short", 0) + 1
            result = _rule_based_compression(industry_logic, key_sentence, max_length)
            if cache_key:
                _embedding_cache[cache_key] = result
            # 메트릭 기록
            out_len = len(result)
            ratio = out_len / original_length if original_length > 0 else 0.0
            if original_length < 200:
                gate_passed = out_len <= 180
            else:
                gate_passed = (ratio <= 0.65) or (out_len <= 220)
            compression_effective = (ratio <= 0.85) or (out_len <= 260)
            _metrics.setdefault("compression_stats", []).append({
                "compression_used": "rule_short",
                "in_len": original_length,
                "out_len": out_len,
                "ratio": ratio,
                "gate_passed": gate_passed,
                "compression_effective": compression_effective
            })
            return result
        
        # 조건 2: sentence_pool에서 max_sentence_len < 180이고 num_sentences >= 3 (이미 잘 쪼개져 있으면 rule로 충분)
        if sentences:
            max_sent_len = max(len(s) for s in sentences)
            num_sents = len(sentences)
            if max_sent_len < 180 and num_sentences >= 3:
                logger.debug(f"문장 풀이 이미 잘 분리됨 (max_len={max_sent_len}, num={num_sents}), rule-based로 종료")
                _metrics["solar_skipped_well_split"] = _metrics.get("solar_skipped_well_split", 0) + 1
                result = _rule_based_compression(industry_logic, key_sentence, max_length)
                if cache_key:
                    _embedding_cache[cache_key] = result
                # 메트릭 기록
                out_len = len(result)
                ratio = out_len / original_length if original_length > 0 else 0.0
                if original_length < 200:
                    gate_passed = out_len <= 180
                else:
                    gate_passed = (ratio <= 0.65) or (out_len <= 220)
                compression_effective = (ratio <= 0.85) or (out_len <= 260)
                _metrics.setdefault("compression_stats", []).append({
                    "compression_used": "rule_well_split",
                    "in_len": original_length,
                    "out_len": out_len,
                    "ratio": ratio,
                    "gate_passed": gate_passed,
                    "compression_effective": compression_effective
                })
                return result
        
        # 조건 3: 그 외 케이스만 solar 호출
        # 4. Solar Embedding 사용 (재시도 로직 포함)
        import numpy as np
        
        # ⭐ Fix 2: compression_input_source 메트릭 추가
        compression_input_source = "unknown"
        if logic_summary and len(logic_summary) > 200:
            compression_input_source = "logic_summary_raw"
        elif key_sentence and len(key_sentence) > 200:
            compression_input_source = "key_sentence"
        elif industry_logic and len(industry_logic) > 200:
            compression_input_source = "industry_logic"
        else:
            compression_input_source = "short_text"
        
        # 의미 벡터 생성
        summary_text = logic_summary or key_sentence or industry_logic[:200]
        summary_vec_list = _encode_with_retry(summary_text)
        
        if summary_vec_list is None:
            # API 실패 시 fallback
            _metrics["fallback_used_count"] += 1
            logger.warning("Semantic Compression API 실패, rule-based fallback 사용")
            result = _rule_based_compression(industry_logic, key_sentence, max_length)
            # 캐싱 (fallback 결과도 캐싱)
            if cache_key:
                _embedding_cache[cache_key] = result
            # ⭐ 메트릭 기록 (fallback도 동일한 게이트 적용)
            out_len = len(result)
            ratio = out_len / original_length if original_length > 0 else 0.0
            
            if original_length < 200:
                gate_passed = out_len <= 180
            else:
                gate_passed = (ratio <= 0.65) or (out_len <= 220)
            
            compression_effective = (ratio <= 0.85) or (out_len <= 260)
            
            # ⭐ Fix 2: compression_input_source 메트릭 추가 (fallback)
            compression_input_source_fallback = "unknown"
            if logic_summary and len(logic_summary) > 200:
                compression_input_source_fallback = "logic_summary_raw"
            elif key_sentence and len(key_sentence) > 200:
                compression_input_source_fallback = "key_sentence"
            elif industry_logic and len(industry_logic) > 200:
                compression_input_source_fallback = "industry_logic"
            else:
                compression_input_source_fallback = "short_text"
            
            _metrics.setdefault("compression_stats", []).append({
                "compression_used": "fallback",
                "in_len": original_length,
                "out_len": out_len,
                "ratio": ratio,
                "gate_passed": gate_passed,
                "compression_effective": compression_effective,
                "compression_input_source": compression_input_source_fallback  # ⭐ Fix 2 추가
            })
            return result
        
        summary_vec = np.array(summary_vec_list)
        
        # 각 문장 벡터 생성
        sent_vecs = []
        for sent in sentences:
            vec_list = _encode_with_retry(sent)
            if vec_list is None:
                # 일부 문장 실패 시 해당 문장 스킵
                logger.warning(f"문장 임베딩 실패 (스킵): {sent[:50]}...")
                continue
            sent_vecs.append((sent, np.array(vec_list)))
        
        if len(sent_vecs) == 0:
            # 모든 문장 실패 시 fallback
            _metrics["fallback_used_count"] += 1
            logger.warning("모든 문장 임베딩 실패, rule-based fallback 사용")
            result = _rule_based_compression(industry_logic, key_sentence, max_length)
            if cache_key:
                _embedding_cache[cache_key] = result
            # ⭐ 메트릭 기록 (fallback도 동일한 게이트 적용)
            out_len = len(result)
            ratio = out_len / original_length if original_length > 0 else 0.0
            
            if original_length < 200:
                gate_passed = out_len <= 180
            else:
                gate_passed = (ratio <= 0.65) or (out_len <= 220)
            
            compression_effective = (ratio <= 0.85) or (out_len <= 260)
            
            _metrics.setdefault("compression_stats", []).append({
                "compression_used": "fallback",
                "in_len": original_length,
                "out_len": out_len,
                "ratio": ratio,
                "gate_passed": gate_passed,
                "compression_effective": compression_effective
            })
            return result
        
        # 5. cosine similarity 계산
        from numpy import dot
        from numpy.linalg import norm
        
        similarities = []
        for sent, sent_vec in sent_vecs:
            similarity = dot(summary_vec, sent_vec) / (norm(summary_vec) * norm(sent_vec))
            similarities.append((sent, similarity))
        
        # 6. 상위 k개 선택
        similarities.sort(key=lambda x: x[1], reverse=True)
        selected_sentences = []
        
        # key_sentence가 있으면 항상 포함
        if key_sentence:
            selected_sentences.append(key_sentence)
        
        for sent, _ in similarities[:max_sentences]:
            if sent != key_sentence:  # 중복 제거
                selected_sentences.append(sent)
        
        # 7. 결과 조합 및 길이 제한
        result = " ".join(selected_sentences[:max_sentences])
        result = result[:max_length].strip()
        
        # 8. 캐싱
        if cache_key:
            _embedding_cache[cache_key] = result
        
        # ⭐ 메트릭 기록 (길이 기반 2단계 게이트 + compression_effective)
        out_len = len(result)
        ratio = out_len / original_length if original_length > 0 else 0.0
        
        # ⭐ P0-추가 1: 길이 기반 2단계 게이트
        if original_length < 200:
            # 짧은 텍스트: ratio 대신 out_len만 체크
            gate_passed = out_len <= 180
        else:
            # 긴 텍스트: ratio <= 0.65 또는 out_len <= 220
            gate_passed = (ratio <= 0.65) or (out_len <= 220)
        
        # ⭐ P0-추가 2: compression_effective (압축이 실제로 효과적이었는지)
        compression_effective = (ratio <= 0.85) or (out_len <= 260)
        
        _metrics.setdefault("compression_stats", []).append({
            "compression_used": "solar",
            "in_len": original_length,
            "out_len": out_len,
            "ratio": ratio,
            "gate_passed": gate_passed,
            "compression_effective": compression_effective,
            "compression_input_source": compression_input_source  # ⭐ Fix 2 추가
        })
        
        return result
        
    except Exception as e:
        logger.error(f"Semantic Compression 실패: {e}", exc_info=True)
        _metrics["fallback_used_count"] += 1
        # 실패 시 rule-based fallback
        result = _rule_based_compression(industry_logic, key_sentence, max_length)
        # 캐싱 (fallback 결과도 캐싱)
        if cache_key:
            _embedding_cache[cache_key] = result
        # ⭐ 메트릭 기록 (fallback도 동일한 게이트 적용)
        out_len = len(result)
        ratio = out_len / original_length if original_length > 0 else 0.0
        
        if original_length < 200:
            gate_passed = out_len <= 180
        else:
            gate_passed = (ratio <= 0.65) or (out_len <= 220)
        
        compression_effective = (ratio <= 0.85) or (out_len <= 260)
        
        _metrics.setdefault("compression_stats", []).append({
            "compression_used": "fallback",
            "in_len": original_length,
            "out_len": out_len,
            "ratio": ratio,
            "gate_passed": gate_passed,
            "compression_effective": compression_effective
        })
        return result


def _build_sentence_pool(industry_logic: str, key_sentence: Optional[str] = None) -> List[str]:
    """
    문장 풀 구성
    
    ⭐ P0-B: bullet/table 강건화 (라인 기반 처리)
    
    Args:
        industry_logic: 원본 논리 텍스트
        key_sentence: 핵심 문장
    
    Returns:
        문장 리스트
    """
    sentences = []
    
    # key_sentence 우선 포함
    if key_sentence:
        key_sentence_clean = key_sentence.strip()
        if key_sentence_clean and len(key_sentence_clean) > 10:
            sentences.append(key_sentence_clean)
    
    if not industry_logic:
        return sentences
    
    # ⭐ P0-B: 라인 기반 bullet/table 처리
    # 1. 라인 단위로 분리
    lines = industry_logic.splitlines()
    
    # 2. 불릿 패턴: 라인 시작 부분에 불릿이 있으면 새 문장 시작
    bullet_pattern = re.compile(r'^\s*[•\-\*○▶→]|^\s*\d+[\.\)]|^\s*\([0-9가-힣]+\)')
    
    current_sentence = []
    processed_lines = []
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # 표 형태 제거 (| 포함 라인)
        if '|' in line:
            # 표는 제거하거나 첫 줄만 남기기
            if not processed_lines or '|' not in processed_lines[-1]:
                # 표의 첫 줄만 남기기 (요약용)
                table_summary = ' '.join(line.split('|')[:3])  # 최대 3컬럼만
                if table_summary.strip() and len(table_summary.strip()) > 10:
                    processed_lines.append(table_summary.strip())
            continue
        
        # 리포트 템플릿 문구 제거
        template_patterns = [
            r'투자유의.*?면책',
            r'본.*?자료는.*?제공.*?목적',
            r'무단복제.*?배포.*?금지',
            r'담당자.*?연락처',
            r'Analyst.*?@.*?\.com',
            r'ComplianceNotice'
        ]
        skip_line = False
        for pattern in template_patterns:
            if re.search(pattern, line, re.IGNORECASE):
                skip_line = True
                break
        if skip_line:
            continue
        
        # 불릿 라인 체크
        if bullet_pattern.match(line):
            # 이전 문장 저장
            if current_sentence:
                processed_lines.append(' '.join(current_sentence))
                current_sentence = []
            # 새 문장 시작 (불릿 제거)
            line_clean = bullet_pattern.sub('', line).strip()
            if line_clean:
                current_sentence.append(line_clean)
        else:
            # 일반 라인: 현재 문장에 추가
            current_sentence.append(line)
    
    # 마지막 문장 저장
    if current_sentence:
        processed_lines.append(' '.join(current_sentence))
    
    # 3. 처리된 라인들을 문장 단위로 다시 분리
    for line in processed_lines:
        # 문장 단위로 split
        logic_sentences = re.split(r'[.!?][\s\n\r]+', line)
        
        for sent in logic_sentences:
            sent = sent.strip()
            
            # ⭐ Fix 1: 긴 문장 강제 분할 (200자 초과 시)
            if sent and len(sent) > 200:
                # 접속사(하지만, 또한, 반면, 그리고, 그러나 등) 또는 쉼표(,) 기준으로 강제 분할
                split_patterns = [
                    r'[,\s]+(?:하지만|그러나|반면|또한|그리고|그런데|다만|그러므로|따라서|그래서)\s+',
                    r'[,\s]+(?:그리고|또한|또|그런데)\s+',
                    r',\s+',  # 쉼표 기준 (마지막 우선순위)
                ]
                
                split_sentences = [sent]
                for pattern in split_patterns:
                    new_splits = []
                    for s in split_sentences:
                        if len(s) > 200:
                            parts = re.split(pattern, s, maxsplit=1)
                            if len(parts) > 1:
                                new_splits.extend([p.strip() for p in parts if p.strip()])
                            else:
                                new_splits.append(s)
                        else:
                            new_splits.append(s)
                    split_sentences = new_splits
                    if all(len(s) <= 200 for s in split_sentences):
                        break
                
                # 여전히 200자 초과면 무식하게 200자 단위로 자르기
                final_sentences = []
                for s in split_sentences:
                    if len(s) > 200:
                        # 200자 단위로 자르되, 단어 단위로 자르기
                        chunks = []
                        for i in range(0, len(s), 200):
                            chunk = s[i:i+200]
                            # 마지막 단어가 잘리지 않도록
                            if i + 200 < len(s):
                                chunk = chunk.rsplit(' ', 1)[0]
                            chunks.append(chunk.strip())
                        final_sentences.extend(chunks)
                    else:
                        final_sentences.append(s)
                
                # 분할된 문장들을 처리
                for split_sent in final_sentences:
                    if split_sent and len(split_sent) > 15:
                        if len([c for c in split_sent if c.isalnum() or c in '가-힣']) < len(split_sent) * 0.5:
                            continue
                        if key_sentence and split_sent == key_sentence.strip():
                            continue
                        if split_sent not in sentences:
                            sentences.append(split_sent)
            else:
                # 기존 로직 (15자 이상 문장 처리)
                if sent and len(sent) > 15:
                    # 불필요한 특수문자/기호만 있는 문장 제외
                    if len([c for c in sent if c.isalnum() or c in '가-힣']) < len(sent) * 0.5:
                        continue
                    # key_sentence와 중복 제거
                    if key_sentence and sent == key_sentence.strip():
                        continue
                    # 이미 추가된 문장과 중복 제거
                    if sent not in sentences:
                        sentences.append(sent)
    
    # 문장이 없으면 원문을 최대 길이로 자르기
    if not sentences and industry_logic:
        # 원문을 200자 단위로 나누기
        chunk_size = 200
        for i in range(0, len(industry_logic), chunk_size):
            chunk = industry_logic[i:i+chunk_size].strip()
            if chunk and len(chunk) > 15:
                sentences.append(chunk)
    
    return sentences


def _rule_based_compression(
    industry_logic: str,
    key_sentence: Optional[str] = None,
    max_length: int = 220
) -> str:
    """
    Rule-based 압축 (임베딩 실패 시 fallback)
    
    ⚠️ 중요: rule은 "보조"만. 의미 결정은 하지 않음.
    ⭐ 개선: 원문을 그대로 반환하지 않고 첫 문장만 반환
    
    Args:
        industry_logic: 원본 논리 텍스트
        key_sentence: 핵심 문장
        max_length: 최대 길이
    
    Returns:
        압축된 문장
    """
    # key_sentence 우선
    if key_sentence:
        result = key_sentence.strip()
        if len(result) <= max_length:
            return result
    
    # ⭐ P0-C: 문맥 왜곡 방지 (첫+마지막 문장 전략)
    if not industry_logic:
        return ""
    
    # 문장 분리
    sentences = re.split(r'[.!?]\s+', industry_logic)
    valid_sentences = [s.strip() for s in sentences if s.strip() and len(s.strip()) > 15]
    
    if not valid_sentences:
        # 문장 분리가 안 되면 첫 50자만 반환
        result = industry_logic[:max_length].strip()
        if len(result) > 50:
            result = result[:50].rsplit(' ', 1)[0] + "..."
        return result
    
    # ⭐ P0-C: 첫 문장 + 마지막 문장 전략
    first_sent = valid_sentences[0]
    last_sent = valid_sentences[-1] if len(valid_sentences) > 1 else None
    
    selected = []
    
    # 첫 문장 추가
    if first_sent:
        if len(first_sent) > max_length:
            first_sent = first_sent[:max_length].rsplit(' ', 1)[0]
        if not first_sent.endswith(('.', '!', '?')):
            first_sent += "."
        selected.append(first_sent)
    
    # 마지막 문장이 첫 문장과 다르고 정보량이 크면 추가
    if last_sent and last_sent != first_sent:
        # 마지막 문장의 정보량 체크 (길이, 키워드 등)
        last_info_density = len(last_sent) + (10 if any(kw in last_sent for kw in ['결론', '전망', '예상', '기대', '가능성']) else 0)
        first_info_density = len(first_sent)
        
        # 마지막 문장이 더 정보량이 크거나, 두 문장을 합쳐도 max_length 이하면 포함
        combined = f"{first_sent} {last_sent}"
        if (last_info_density > first_info_density * 1.2) or (len(combined) <= max_length):
            if len(last_sent) > max_length:
                last_sent = last_sent[:max_length].rsplit(' ', 1)[0]
            if not last_sent.endswith(('.', '!', '?')):
                last_sent += "."
            if last_sent not in selected:
                selected.append(last_sent)
    
    # 최대 2문장까지만 합치기
    result = ". ".join(selected[:2])
    if len(result) > max_length:
        # 첫 문장만 반환
        result = selected[0] if selected else first_sent
    
    return result


def clear_cache():
    """캐시 초기화 (테스트용)"""
    global _embedding_cache, _metrics
    _embedding_cache = {}
    _metrics = {
        "api_fail_count": 0,
        "timeout_count": 0,
        "fallback_used_count": 0,
        "cache_hit_count": 0,
        "cache_miss_count": 0
    }


def get_metrics() -> dict:
    """
    Semantic Compression 메트릭 조회
    
    ⭐ P0.5-2: API 관측 지표 포함
    """
    metrics = _metrics.copy()
    
    # 캐시 히트율 계산
    total_cache_requests = metrics["cache_hit_count"] + metrics["cache_miss_count"]
    if total_cache_requests > 0:
        metrics["cache_hit_rate"] = metrics["cache_hit_count"] / total_cache_requests
    else:
        metrics["cache_hit_rate"] = 0.0
    
    # fallback 발생률 계산
    total_compressions = metrics["cache_hit_count"] + metrics["cache_miss_count"]
    if total_compressions > 0:
        metrics["fallback_rate"] = metrics["fallback_used_count"] / total_compressions
    else:
        metrics["fallback_rate"] = 0.0
    
    # 평균 응답 시간 계산 (p50, p95)
    if metrics["response_times"]:
        sorted_times = sorted(metrics["response_times"])
        n = len(sorted_times)
        metrics["p50_response_time_ms"] = sorted_times[n // 2] if n > 0 else 0
        metrics["p95_response_time_ms"] = sorted_times[int(n * 0.95)] if n > 1 else sorted_times[-1] if n > 0 else 0
        metrics["avg_response_time_ms"] = sum(sorted_times) / n
    else:
        metrics["p50_response_time_ms"] = 0
        metrics["p95_response_time_ms"] = 0
        metrics["avg_response_time_ms"] = 0
    
    # response_times 리스트는 너무 커질 수 있으므로 요약만 반환
    if "response_times" in metrics:
        del metrics["response_times"]
    
    # ⭐ compression_stats 요약 계산
    if "compression_stats" in metrics and metrics["compression_stats"]:
        stats = metrics["compression_stats"]
        total = len(stats)
        compression_used_counts = {}
        ratios = []
        for stat in stats:
            method = stat.get("compression_used", "unknown")
            compression_used_counts[method] = compression_used_counts.get(method, 0) + 1
            ratios.append(stat.get("ratio", 0.0))
        
        # ⭐ P0-추가 2: compression_effective 통계
        effective_count = sum(1 for stat in stats if stat.get("compression_effective", False))
        gate_passed_count = sum(1 for stat in stats if stat.get("gate_passed", False))
        
        metrics["compression_summary"] = {
            "total_compressions": total,
            "compression_methods": compression_used_counts,
            "avg_ratio": sum(ratios) / len(ratios) if ratios else 0.0,
            "min_ratio": min(ratios) if ratios else 0.0,
            "max_ratio": max(ratios) if ratios else 0.0,
            "ratio_below_0_6": sum(1 for r in ratios if r < 0.6),
            "compression_effective_count": effective_count,
            "compression_effective_rate": effective_count / total if total > 0 else 0.0,
            "gate_passed_count": gate_passed_count,
            "gate_passed_rate": gate_passed_count / total if total > 0 else 0.0
        }
        # 원본 stats는 너무 커질 수 있으므로 삭제 (요약만 유지)
        del metrics["compression_stats"]
    else:
        metrics["compression_summary"] = {
            "total_compressions": 0,
            "compression_methods": {},
            "avg_ratio": 0.0,
            "min_ratio": 0.0,
            "max_ratio": 0.0,
            "ratio_below_0_6": 0
        }
    
    return metrics

