"""
뉴스 기사 중복 제거 및 다양성 확보 서비스
TF-IDF + BERT Hybrid 방식으로 유사한 기사를 그룹화하고 대표 기사를 선정합니다.
"""
from typing import List, Dict, Tuple, Set, Optional
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.cluster import AgglomerativeClustering
import logging
from collections import defaultdict
import time
from datetime import datetime, timedelta

from app.utils.text_cleaner import normalize_article_text

logger = logging.getLogger(__name__)

# BERT 모델 로딩 (한국어 지원) - 지연 로딩
bert_model = None

def get_device():
    """GPU 사용 가능 여부 확인"""
    try:
        import torch
        if torch.cuda.is_available():
            device = torch.device("cuda")
            logger.info(f"GPU 사용 가능: {torch.cuda.get_device_name(0)}")
            return device
        else:
            device = torch.device("cpu")
            logger.info("GPU 사용 불가. CPU 사용.")
            return device
    except ImportError:
        logger.warning("PyTorch가 설치되지 않음. CPU 사용.")
        return None

def get_bert_model():
    """BERT 모델 지연 로딩 (GPU 지원)"""
    global bert_model
    if bert_model is None:
        try:
            from sentence_transformers import SentenceTransformer
            device = get_device()
            
            # GPU/CPU 자동 선택
            if device:
                device_str = str(device)
            else:
                device_str = 'cpu'
            
            bert_model = SentenceTransformer(
                'paraphrase-multilingual-MiniLM-L12-v2',
                device=device_str
            )
            logger.info(f"BERT 모델 로드 완료: {bert_model.device} 사용")
        except Exception as e:
            logger.warning(f"BERT 모델 로드 실패: {e}. TF-IDF만 사용합니다.")
            bert_model = False  # False로 설정하여 재시도 방지
    return bert_model if bert_model else None


def prepare_text(article: Dict) -> str:
    """
    기사에서 텍스트 추출 (제목 + 요약)
    
    Args:
        article: 기사 딕셔너리
    
    Returns:
        결합된 텍스트
    """
    title = normalize_article_text(article.get("title", ""))
    summary = normalize_article_text(article.get("summary", ""))
    # 제목과 요약 결합 (제목에 더 높은 가중치)
    text = f"{title} {title} {summary}"  # 제목을 2번 반복하여 가중치 증가
    return text.strip()


def calculate_tfidf_similarity(articles: List[Dict]) -> np.ndarray:
    """
    TF-IDF 기반 유사도 계산 (1단계: 빠른 필터링)
    
    Args:
        articles: 기사 리스트
    
    Returns:
        유사도 행렬
    """
    texts = [prepare_text(article) for article in articles]
    
    # TF-IDF 벡터화 (한국어 최적화, 메모리 절약)
    vectorizer = TfidfVectorizer(
        max_features=1500,  # 2000 -> 1500 (메모리 절약)
        ngram_range=(1, 2),  # 단어 + 2-gram
        min_df=1,
        max_df=0.95,
    )
    
    try:
        tfidf_vectors = vectorizer.fit_transform(texts)
        similarity_matrix = cosine_similarity(tfidf_vectors)
        logger.info(f"TF-IDF 유사도 계산 완료: {len(articles)}개 기사")
        return similarity_matrix
    except Exception as e:
        logger.error(f"TF-IDF 계산 실패: {e}")
        # 단위 행렬 반환 (모든 기사가 다름)
        return np.eye(len(articles))


def calculate_bert_similarity(articles: List[Dict]) -> Optional[np.ndarray]:
    """
    BERT Embedding 기반 의미 유사도 계산 (2단계: 정밀 측정)
    GPU 가속 지원
    
    Args:
        articles: 기사 리스트
    
    Returns:
        유사도 행렬 (None일 수 있음)
    """
    model = get_bert_model()
    if model is None:
        logger.warning("BERT 모델이 없어 TF-IDF 유사도만 사용합니다.")
        return None
    
    texts = [prepare_text(article) for article in articles]
    
    try:
        # GPU 메모리 효율을 위한 배치 크기 조정
        device = get_device()
        if device and 'cuda' in str(device):
            batch_size = 64  # GPU는 더 큰 배치 가능
            device_str = str(device)
        else:
            batch_size = 32  # CPU는 작은 배치
            device_str = 'cpu'
        
        # BERT Embedding 생성
        embeddings = model.encode(
            texts,
            convert_to_numpy=True,
            show_progress_bar=False,
            batch_size=batch_size,
            device=device_str
        )
        
        # 코사인 유사도 계산
        similarity_matrix = cosine_similarity(embeddings)
        logger.info(f"BERT 유사도 계산 완료: {len(articles)}개 기사 (Device: {device_str}, Batch: {batch_size})")
        return similarity_matrix
    except Exception as e:
        logger.error(f"BERT 유사도 계산 실패: {e}")
        return None


def combine_similarity_matrices(
    tfidf_sim: np.ndarray,
    bert_sim: np.ndarray = None,
    tfidf_weight: float = None,
    bert_weight: float = None
) -> np.ndarray:
    """
    TF-IDF와 BERT 유사도를 가중 평균으로 결합
    
    Args:
        tfidf_sim: TF-IDF 유사도 행렬
        bert_sim: BERT 유사도 행렬 (None일 수 있음)
        tfidf_weight: TF-IDF 가중치 (기본값: 0.6)
        bert_weight: BERT 가중치 (기본값: 0.4)
    
    Returns:
        결합된 유사도 행렬
    """
    if bert_sim is None:
        logger.info("BERT 유사도 없음. TF-IDF만 사용합니다.")
        return tfidf_sim
    
    # 기본 가중치
    if tfidf_weight is None:
        tfidf_weight = 0.6
    if bert_weight is None:
        bert_weight = 0.4
    
    # 가중치 정규화
    total_weight = tfidf_weight + bert_weight
    if total_weight > 0:
        tfidf_weight = tfidf_weight / total_weight
        bert_weight = bert_weight / total_weight
    
    # 가중 평균
    combined = tfidf_weight * tfidf_sim + bert_weight * bert_sim
    logger.info(f"유사도 행렬 결합 완료 (TF-IDF: {tfidf_weight:.2f}, BERT: {bert_weight:.2f})")
    return combined


def find_similar_groups(
    articles: List[Dict],
    similarity_matrix: np.ndarray,
    threshold: float = 0.75
) -> List[List[int]]:
    """
    유사도 행렬을 기반으로 유사한 기사 그룹 찾기
    AgglomerativeClustering 사용으로 연쇄형 클러스터 방지
    
    Args:
        articles: 기사 리스트
        similarity_matrix: 유사도 행렬
        threshold: 유사도 임계값
    
    Returns:
        기사 인덱스 그룹 리스트
    """
    n = len(articles)
    if n <= 1:
        return [[0]] if n == 1 else []
    
    try:
        # 거리 행렬로 변환 (유사도 -> 거리)
        distance_matrix = 1 - similarity_matrix
        
        # AgglomerativeClustering 적용 (연쇄 효과 방지)
        # scikit-learn 1.3.0+ 버전에서는 'affinity' 대신 'metric' 사용
        clustering = AgglomerativeClustering(
            metric='precomputed',  # affinity -> metric (scikit-learn 1.3.0+)
            linkage='average',  # average linkage가 연쇄 효과 방지
            distance_threshold=1 - threshold,
            n_clusters=None  # threshold 기반 클러스터링
        )
        
        labels = clustering.fit_predict(distance_matrix)
        
        # 라벨별로 그룹화
        clusters = defaultdict(list)
        for idx, label in enumerate(labels):
            clusters[label].append(idx)
        
        groups = list(clusters.values())
        
        # 통계 로깅
        avg_cluster_size = np.mean([len(g) for g in groups]) if groups else 0
        logger.info(
            f"클러스터링 완료: {len(groups)}개 그룹 (임계값: {threshold}, "
            f"평균 크기: {avg_cluster_size:.2f})"
        )
        return groups
        
    except Exception as e:
        logger.error(f"AgglomerativeClustering 실패: {e}. 기본 방법 사용.")
        # Fallback: 기본 방법
        visited = set()
        groups = []
        
        for i in range(n):
            if i in visited:
                continue
            
            similar_indices = []
            for j in range(n):
                if i != j and similarity_matrix[i][j] >= threshold:
                    similar_indices.append(j)
            
            if similar_indices:
                group = [i] + similar_indices
                groups.append(group)
                visited.update(group)
            else:
                groups.append([i])
                visited.add(i)
        
        logger.info(f"기본 방법으로 그룹 찾기 완료: {len(groups)}개 그룹")
        return groups


def select_representative_article(
    group_indices: List[int],
    articles: List[Dict]
) -> int:
    """
    그룹에서 대표 기사 선정
    
    선정 기준:
    1. 최신 기사 우선
    2. 정보량 (요약 길이 + 제목 품질)
    
    Args:
        group_indices: 그룹 내 기사 인덱스 리스트
        articles: 전체 기사 리스트
    
    Returns:
        대표 기사 인덱스
    """
    if len(group_indices) == 1:
        return group_indices[0]
    
    group_articles = [(idx, articles[idx]) for idx in group_indices]
    
    # 1. 최신 기사 우선
    try:
        group_articles.sort(
            key=lambda x: x[1].get("published_at", ""),
            reverse=True
        )
    except:
        pass
    
    # 2. 정보량 점수 계산 (요약 길이 + 제목 품질)
    def score_article(idx_article: Tuple[int, Dict]) -> float:
        """기사 점수 계산 (정보량 기준)"""
        idx, article = idx_article
        summary = article.get("summary", "")
        title = article.get("title", "")
        
        # 요약 길이 점수 (0~1)
        summary_score = min(len(summary) / 500, 1.0) if summary else 0
        
        # 제목 길이 점수 (너무 짧거나 길면 감점)
        title_len = len(title)
        if 10 <= title_len <= 100:
            title_score = 1.0
        else:
            title_score = 0.5
        
        # 정보량 점수 (요약 70% + 제목 30%)
        info_score = summary_score * 0.7 + title_score * 0.3
        
        return info_score
    
    # 점수 기준으로 정렬
    group_articles.sort(key=score_article, reverse=True)
    
    return group_articles[0][0]


def ensure_diversity(
    selected_indices: List[int],
    articles: List[Dict],
    max_same_source: int = 3
) -> List[int]:
    """
    선택된 기사들의 다양성 보장 (개선된 버전: 시간차 고려)
    
    Args:
        selected_indices: 선택된 기사 인덱스 리스트
        articles: 전체 기사 리스트
        max_same_source: 같은 출처 최대 연속 개수
    
    Returns:
        다양성이 보장된 기사 인덱스 리스트
    """
    if not selected_indices:
        return []
    
    diversified = []
    source_counts = defaultdict(int)
    last_source = None
    last_time = None
    consecutive_same_source = 0
    
    # 시간 순 정렬 (최신순)
    sorted_indices = sorted(
        selected_indices,
        key=lambda idx: articles[idx].get("published_at", ""),
        reverse=True
    )
    
    for idx in sorted_indices:
        article = articles[idx]
        source = article.get("source", "unknown")
        
        # 발행 시간 파싱
        pub_time = None
        try:
            pub_time_str = article.get("published_at", "")
            if pub_time_str:
                if isinstance(pub_time_str, str):
                    # ISO 형식 파싱
                    if 'T' in pub_time_str:
                        pub_time = datetime.fromisoformat(pub_time_str.replace('Z', '+00:00'))
                    else:
                        pub_time = datetime.fromisoformat(pub_time_str)
                elif isinstance(pub_time_str, datetime):
                    pub_time = pub_time_str
        except Exception:
            pass
        
        # 시간차 계산 (시간 단위)
        time_diff_hours = None
        if last_time and pub_time:
            try:
                time_diff = abs((pub_time - last_time).total_seconds())
                time_diff_hours = time_diff / 3600
            except Exception:
                pass
        
        # 같은 출처 체크
        if source == last_source:
            consecutive_same_source += 1
            # 시간차가 3시간 이상이면 연속 카운트 리셋 (다른 기사로 간주)
            if time_diff_hours and time_diff_hours >= 3:
                consecutive_same_source = 1  # 리셋
                logger.debug(f"시간차 {time_diff_hours:.1f}시간으로 연속 카운트 리셋: {source}")
        else:
            consecutive_same_source = 1
            last_source = source
        
        # 다양성 조건 확인
        if consecutive_same_source <= max_same_source:
            diversified.append(idx)
            source_counts[source] += 1
            last_time = pub_time
        else:
            # 같은 출처가 너무 많으면 스킵
            logger.debug(f"다양성 확보를 위해 기사 스킵: {source} (연속 {consecutive_same_source}개)")
    
    logger.info(
        f"다양성 확보 완료: {len(selected_indices)} -> {len(diversified)}개 기사 "
        f"(출처 수: {len(source_counts)})"
    )
    return diversified


def deduplicate_articles(
    articles: List[Dict],
    similarity_threshold: float = 0.75,
    max_results: int = 50,
    enable_bert: bool = True,
    tfidf_weight: float = 0.6,
    bert_weight: float = 0.4,
    max_same_source: int = 3
) -> List[Dict]:
    """
    기사 중복 제거 및 다양성 확보
    
    Args:
        articles: 기사 리스트
        similarity_threshold: 유사도 임계값 (0.0 ~ 1.0)
        max_results: 최대 반환 기사 수
        enable_bert: BERT 사용 여부
        tfidf_weight: TF-IDF 가중치 (기본값: 0.6)
        bert_weight: BERT 가중치 (기본값: 0.4)
        max_same_source: 같은 출처 최대 연속 개수 (기본값: 3)
    
    Returns:
        중복이 제거되고 다양성이 보장된 기사 리스트
    """
    if not articles:
        return []
    
    if len(articles) == 1:
        single_article = articles[0].copy()
        single_article.pop("hash_key", None)
        single_article.setdefault("related_articles", [])
        return [single_article]
    
    start_time = time.perf_counter()
    original_total = len(articles)
    logger.info(f"중복 제거 시작: 원본 {original_total}개 기사")
    
    try:
        # 0단계: 해시/링크 기반 1차 그룹화 (제목+이미지, 링크 동일 기사 사전 병합)
        processed_articles: List[Dict] = []
        hash_related_map: Dict[int, List[Dict]] = defaultdict(list)
        hash_index_map: Dict[str, int] = {}
        link_index_map: Dict[str, int] = {}
        
        for article in articles:
            article_copy = article.copy()
            link = article_copy.get("link")
            hash_key = article_copy.get("hash_key")
            
            existing_idx = None
            if link and link in link_index_map:
                existing_idx = link_index_map[link]
            elif hash_key and hash_key in hash_index_map:
                existing_idx = hash_index_map[hash_key]
            
            if existing_idx is not None:
                hash_related_map[existing_idx].append(article_copy)
                continue
            
            processed_articles.append(article_copy)
            new_idx = len(processed_articles) - 1
            if hash_key:
                hash_index_map[hash_key] = new_idx
            if link:
                link_index_map[link] = new_idx
        
        hash_collapse_count = sum(len(v) for v in hash_related_map.values())
        if hash_collapse_count:
            logger.info("해시/링크 기반 1차 병합: %d건 사전 제거", hash_collapse_count)
        
        articles = processed_articles
        if not articles:
            logger.info("모든 기사가 해시 단계에서 병합되어 결과가 없습니다.")
            return []
        
        logger.info(f"유사도 기반 분석 대상: {len(articles)}개 기사")
        
        # 1단계: TF-IDF 유사도 계산 (빠른 필터링)
        tfidf_start = time.perf_counter()
        tfidf_sim = calculate_tfidf_similarity(articles)
        tfidf_time = time.perf_counter() - tfidf_start
        
        # TF-IDF 통계
        tfidf_top_pairs = int((tfidf_sim > similarity_threshold).sum() / 2)
        
        # 2단계: BERT 유사도 계산 (정밀 측정)
        bert_sim = None
        bert_time = 0
        if enable_bert:
            # TF-IDF에서 유사도가 높은 후보가 있는지 확인
            # 성능 최적화: TF-IDF > 0.6인 쌍이 있으면 BERT 적용
            has_candidates = False
            for i in range(len(articles)):
                for j in range(i + 1, len(articles)):
                    if tfidf_sim[i][j] > 0.6:
                        has_candidates = True
                        break
                if has_candidates:
                    break
            
            if has_candidates:
                logger.info("BERT 유사도 계산 시작 (TF-IDF 후보 발견)")
                bert_start = time.perf_counter()
                bert_sim = calculate_bert_similarity(articles)
                bert_time = time.perf_counter() - bert_start
            else:
                logger.info("BERT 재측정 대상 없음. TF-IDF만 사용합니다.")
        
        # BERT 통계
        bert_top_pairs = int((bert_sim > similarity_threshold).sum() / 2) if bert_sim is not None else 0
        
        # 3단계: 유사도 행렬 결합
        combine_start = time.perf_counter()
        combined_sim = combine_similarity_matrices(
            tfidf_sim, 
            bert_sim,
            tfidf_weight=tfidf_weight,
            bert_weight=bert_weight
        )
        combine_time = time.perf_counter() - combine_start
        
        # 4단계: 유사 그룹 찾기
        cluster_start = time.perf_counter()
        groups = find_similar_groups(articles, combined_sim, similarity_threshold)
        cluster_time = time.perf_counter() - cluster_start
        
        # 5단계: 각 그룹에서 대표 기사 선정 및 클러스터 정보 생성
        select_start = time.perf_counter()
        representative_indices = []
        cluster_info_map = {}  # 클러스터 정보 저장 (idx -> cluster_info)
        cluster_members_map: Dict[str, List[int]] = {}
        
        for cluster_id, group in enumerate(groups):
            cluster_label = f"C_{cluster_id}"
            rep_idx = select_representative_article(group, articles)
            representative_indices.append(rep_idx)
            
            # 그룹 내 모든 기사에 클러스터 정보 추가
            for idx in group:
                cluster_info_map[idx] = {
                    "cluster_id": cluster_label,
                    "representative": (idx == rep_idx)
                }
            cluster_members_map[cluster_label] = list(group)
        select_time = time.perf_counter() - select_start
        
        # 6단계: 다양성 확보
        diversity_start = time.perf_counter()
        diversified_indices = ensure_diversity(
            representative_indices, 
            articles,
            max_same_source=max_same_source
        )
        diversity_time = time.perf_counter() - diversity_start
        
        # 7단계: 최대 개수 제한 (max_results가 None이 아니고 충분히 클 때만)
        if max_results and max_results > 0:
            final_indices = diversified_indices[:max_results]
        else:
            final_indices = diversified_indices
        
        # 최종 기사 리스트 생성 (클러스터 정보 및 관련 기사 포함)
        result = []
        
        def build_related_payload(article_dict: Dict, info: Dict) -> Dict:
            return {
                "title": article_dict.get("title"),
                "source": article_dict.get("source"),
                "link": article_dict.get("link"),
                "url": article_dict.get("link"),
                "published_at": article_dict.get("published_at"),
                "image_url": article_dict.get("image_url"),
                "cluster_id": info.get("cluster_id"),
                "representative": info.get("representative", False),
            }
        
        for idx in final_indices:
            article = articles[idx].copy()
            # 클러스터 정보 추가 (이미 cluster_info_map에 저장됨)
            cluster_info = cluster_info_map.get(idx, {"cluster_id": None, "representative": False})
            article["cluster_id"] = cluster_info["cluster_id"]
            article["representative"] = cluster_info["representative"]
            
            related_candidates: List[Dict] = []
            cluster_label = cluster_info.get("cluster_id")
            
            if cluster_label and cluster_label in cluster_members_map:
                for other_idx in cluster_members_map[cluster_label]:
                    if other_idx == idx:
                        continue
                    other_article = articles[other_idx].copy()
                    other_info = cluster_info_map.get(
                        other_idx,
                        {"cluster_id": cluster_label, "representative": False},
                    )
                    related_candidates.append(build_related_payload(other_article, other_info))
                    
                    for extra in hash_related_map.get(other_idx, []):
                        extra_info = {
                            "cluster_id": other_info.get("cluster_id"),
                            "representative": False,
                        }
                        related_candidates.append(build_related_payload(extra, extra_info))
            
            for extra in hash_related_map.get(idx, []):
                extra_info = {
                    "cluster_id": cluster_info.get("cluster_id"),
                    "representative": False,
                }
                related_candidates.append(build_related_payload(extra, extra_info))
            
            # 중복 제거 (link 기준)
            seen_links: Set[str] = set()
            filtered_related: List[Dict] = []
            for rel in related_candidates:
                link = rel.get("link")
                if not link or link in seen_links:
                    continue
                seen_links.add(link)
                filtered_related.append(rel)
            
            article.pop("hash_key", None)
            article["related_articles"] = filtered_related
            result.append(article)
        
        total_time = time.perf_counter() - start_time
        reduction_rate = (1 - len(result)/original_total)*100 if original_total else 0
        avg_cluster_size = np.mean([len(g) for g in groups]) if groups else 0
        
        # 상세 로깅
        logger.info({
            "articles_count": original_total,
            "unique_after_hash": len(articles),
            "result_count": len(result),
            "reduction_rate": f"{reduction_rate:.1f}%",
            "num_clusters": len(groups),
            "avg_cluster_size": f"{avg_cluster_size:.2f}",
            "tfidf_top_pairs": tfidf_top_pairs,
            "bert_top_pairs": bert_top_pairs,
            "performance": {
                "tfidf_time": f"{tfidf_time:.2f}s",
                "bert_time": f"{bert_time:.2f}s",
                "combine_time": f"{combine_time:.3f}s",
                "cluster_time": f"{cluster_time:.2f}s",
                "select_time": f"{select_time:.3f}s",
                "diversity_time": f"{diversity_time:.3f}s",
                "total_time": f"{total_time:.2f}s"
            }
        })
        
        logger.info(
            f"중복 제거 완료: {original_total} -> {len(result)}개 기사 "
            f"(제거율: {reduction_rate:.1f}%, 소요시간: {total_time:.2f}s)"
        )
        
        return result
        
    except Exception as e:
        logger.error(f"중복 제거 실패: {e}")
        # 오류 시 원본 반환 (최신순으로 정렬)
        fallback_sorted = sorted(
            articles,
            key=lambda x: x.get("published_at", ""),
            reverse=True
        )
        fallback_result = []
        for article in fallback_sorted[:max_results]:
            article_copy = article.copy()
            article_copy.pop("hash_key", None)
            article_copy.setdefault("related_articles", [])
            fallback_result.append(article_copy)
        return fallback_result

