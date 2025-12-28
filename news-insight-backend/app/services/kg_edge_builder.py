"""
Causal Structure JSON을 파싱하여 edges 테이블에 관계를 저장

Driver Tags를 properties에 포함하여 KG 연결을 명시화합니다.
Deterministic ID 생성으로 중복 방지 및 Upsert 가능.
"""
import logging
import hashlib
from typing import List, Dict, Any, Optional
from datetime import datetime
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def generate_deterministic_edge_id(
    source_id: str,
    target_id: str,
    relation_type: str
) -> str:
    """
    Deterministic Edge ID 생성
    
    Args:
        source_id: 소스 엔티티 ID
        target_id: 타겟 엔티티 ID
        relation_type: 관계 타입
    
    Returns:
        결정적 ID (Hash 기반)
    """
    # Hash 생성: source_id + target_id + relation_type
    hash_input = f"{source_id}_{target_id}_{relation_type}"
    hash_value = hashlib.md5(hash_input.encode()).hexdigest()[:16]
    
    # ID 형식: {source_id}_{target_id}_{relation_type}_{hash}
    edge_id = f"{source_id}_{target_id}_{relation_type}_{hash_value}"
    
    return edge_id


def build_edges_from_causal_structure(
    ticker: str,
    causal_structure: Dict[str, Any],
    major_sector: Optional[str] = None,
    sector_l2: Optional[str] = None
) -> List[Dict[str, Any]]:
    """
    causal_structure JSON을 파싱하여 edges 생성
    
    Args:
        ticker: 기업 티커
        causal_structure: 인과 구조 JSON
        major_sector: L1 섹터 코드 (선택적)
        sector_l2: L2 섹터 코드 (선택적)
    
    Returns:
        edges 리스트 (DB 저장용)
    """
    edges = []
    
    if not causal_structure:
        return edges
    
    # 1. 기업 → 드라이버 관계 (DRIVEN_BY)
    key_drivers = causal_structure.get('key_drivers', [])
    for driver in key_drivers:
        driver_code = driver.get('code')
        if not driver_code:
            continue
        
        # Driver Tags 추출
        driver_tags = driver.get('driver_tags', [])
        driver_tags_metadata = driver.get('driver_tags_metadata', [])
        
        # ⭐ Driver Tags의 평균 confidence 계산 (Edge weight에 반영)
        driver_confidence = driver.get('confidence', 0.5)
        if driver_tags_metadata:
            tag_confidences = [t.get('confidence', 0.5) for t in driver_tags_metadata if isinstance(t, dict)]
            if tag_confidences:
                avg_tag_confidence = sum(tag_confidences) / len(tag_confidences)
                # Driver confidence와 Tag confidence의 가중 평균
                driver_confidence = (driver_confidence * 0.6) + (avg_tag_confidence * 0.4)
        
        # properties 구성
        properties = {
            'direction': driver.get('direction'),
            'driver_type': driver.get('type'),  # P/Q/C
            'confidence': driver.get('confidence', 0.5),
            'computed_weight': driver_confidence,  # ⭐ 계산된 weight 저장
        }
        
        # Driver Tags 추가 (있는 경우)
        if driver_tags:
            properties['driver_tags'] = driver_tags
        
        # Driver Tags 메타데이터 추가 (있는 경우)
        if driver_tags_metadata:
            properties['driver_tags_metadata'] = driver_tags_metadata
        
        # Edge 생성
        edge_id = generate_deterministic_edge_id(
            source_id=ticker,
            target_id=driver_code,
            relation_type='DRIVEN_BY'
        )
        
        edge = {
            'id': edge_id,
            'source_id': ticker,
            'target_id': driver_code,
            'source_type': 'COMPANY',
            'target_type': 'DRIVER',
            'relation_type': 'DRIVEN_BY',
            'weight': driver_confidence,  # ⭐ Edge weight에 confidence 반영
            'properties': properties,
            'direction': 'DIRECTED',
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        
        edges.append(edge)
    
    # 2. 기업 → L3 태그 관계 (HAS_TAG)
    l3_tags = causal_structure.get('granular_tags', []) or causal_structure.get('l3_tags', [])
    for tag in l3_tags:
        if not tag:
            continue
        
        edge_id = generate_deterministic_edge_id(
            source_id=ticker,
            target_id=tag,
            relation_type='HAS_TAG'
        )
        
        edge = {
            'id': edge_id,
            'source_id': ticker,
            'target_id': tag,
            'source_type': 'COMPANY',
            'target_type': 'TAG',
            'relation_type': 'HAS_TAG',
            'properties': {},
            'direction': 'DIRECTED',
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        
        edges.append(edge)
    
    # 3. 기업 → 섹터 관계 (BELONGS_TO)
    if major_sector:
        edge_id = generate_deterministic_edge_id(
            source_id=ticker,
            target_id=major_sector,
            relation_type='BELONGS_TO'
        )
        
        properties = {}
        if sector_l2:
            properties['sector_l2'] = sector_l2
        
        edge = {
            'id': edge_id,
            'source_id': ticker,
            'target_id': major_sector,
            'source_type': 'COMPANY',
            'target_type': 'SECTOR',
            'relation_type': 'BELONGS_TO',
            'properties': properties,
            'direction': 'DIRECTED',
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        
        edges.append(edge)
    
    return edges


def save_edges_to_db(
    db: Session,
    edges: List[Dict[str, Any]],
    upsert: bool = True
) -> int:
    """
    Edges를 DB에 저장 (Upsert 지원)
    
    Args:
        db: SQLAlchemy Session
        edges: Edge 리스트
        upsert: True면 ON CONFLICT UPDATE, False면 INSERT만
    
    Returns:
        저장된 Edge 개수
    """
    from app.models.edge import Edge
    
    saved_count = 0
    
    for edge_data in edges:
        try:
            edge_id = edge_data['id']
            
            # 기존 Edge 확인
            existing_edge = db.query(Edge).filter(Edge.id == edge_id).first()
            
            if existing_edge:
                if upsert:
                    # 업데이트
                    for key, value in edge_data.items():
                        if key != 'id':  # ID는 업데이트하지 않음
                            setattr(existing_edge, key, value)
                    existing_edge.updated_at = datetime.now()
                    saved_count += 1
                    logger.debug(f"[KG] Edge 업데이트: {edge_id}")
                else:
                    logger.debug(f"[KG] Edge 중복 스킵: {edge_id}")
            else:
                # 새 Edge 생성
                new_edge = Edge(**edge_data)
                db.add(new_edge)
                saved_count += 1
                logger.debug(f"[KG] Edge 생성: {edge_id}")
        
        except Exception as e:
            logger.error(f"[KG] Edge 저장 실패: {edge_data.get('id', 'unknown')} - {e}")
            continue
    
    try:
        db.commit()
        logger.info(f"[KG] 총 {saved_count}개 Edge 저장 완료")
    except Exception as e:
        db.rollback()
        logger.error(f"[KG] Edge 저장 롤백: {e}")
        raise
    
    return saved_count

