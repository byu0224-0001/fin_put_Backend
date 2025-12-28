from typing import Dict, List, Optional
from neo4j import Transaction
import logging

logger = logging.getLogger(__name__)


def create_entity_node(tx: Transaction, entity_type: str, entity_name: str, properties: Optional[Dict] = None):
    """
    Neo4j에 엔티티 노드 생성 (유틸리티 함수, 현재는 직접 쿼리 사용)
    
    Args:
        tx: Neo4j 트랜잭션
        entity_type: 엔티티 타입 (Company, Policy, Sector, Event 등)
        entity_name: 엔티티 이름
        properties: 추가 속성
    """
    properties = properties or {}
    properties["name"] = entity_name
    normalized_type = entity_type.upper() if entity_type else "ENTITY"
    
    query = f"""
    MERGE (e:{normalized_type} {{name: $name}})
    SET e += $properties
    RETURN e
    """
    
    tx.run(query, name=entity_name, properties=properties)


def create_relationship(
    tx: Transaction,
    from_type: str,
    from_name: str,
    to_type: str,
    to_name: str,
    rel_type: str,
    properties: Optional[Dict] = None
):
    """
    Neo4j에 관계 생성
    
    Args:
        tx: Neo4j 트랜잭션
        from_type: 시작 노드 타입
        from_name: 시작 노드 이름
        to_type: 종료 노드 타입
        to_name: 종료 노드 이름
        rel_type: 관계 타입 (AFFECTS, REGULATES, COMPETES_WITH 등)
        properties: 관계 속성
    """
    properties = properties or {}
    
    query = f"""
    MATCH (a:{from_type} {{name: $from_name}})
    MATCH (b:{to_type} {{name: $to_name}})
    MERGE (a)-[r:{rel_type}]->(b)
    SET r += $properties
    RETURN r
    """
    
    tx.run(
        query,
        from_name=from_name,
        to_name=to_name,
        properties=properties
    )


def update_article_graph(session, article_id: int, entities: Dict, keywords: List[str]):
    """
    기사 분석 결과를 Neo4j 그래프에 반영
    
    Args:
        session: Neo4j 세션
        article_id: 기사 ID
        entities: 엔티티 딕셔너리 (타입별로 분류)
        keywords: 키워드 리스트
    """
    try:
        def work(tx):
            # Article 노드 생성
            tx.run(
                "MERGE (a:Article {id: $article_id})",
                article_id=article_id
            )
            
            # 엔티티 노드 생성 및 관계 설정
            for entity_type, entity_list in entities.items():
                if not isinstance(entity_list, list):
                    continue
                    
                # 엔티티 타입 정규화 (대문자로)
                normalized_type = entity_type.upper() if entity_type else "ENTITY"
                
                for entity_name in entity_list:
                    if not entity_name or len(entity_name.strip()) == 0:
                        continue
                    
                    entity_name_clean = entity_name.strip()
                    
                    # 엔티티 노드 생성 (타입을 라벨로 사용)
                    tx.run(
                        f"""
                        MERGE (e:{normalized_type} {{name: $name}})
                        SET e.type = $type
                        """,
                        name=entity_name_clean,
                        type=normalized_type
                    )
                    
                    # Article과 엔티티 연결
                    tx.run(
                        f"""
                        MATCH (a:Article {{id: $article_id}})
                        MATCH (e:{normalized_type} {{name: $entity_name}})
                        MERGE (a)-[:MENTIONS]->(e)
                        """,
                        article_id=article_id,
                        entity_name=entity_name_clean
                    )
            
            # 키워드도 노드로 생성 (선택적)
            for keyword in keywords[:10]:  # 최대 10개만
                if keyword and len(keyword.strip()) > 0:
                    tx.run(
                        """
                        MERGE (k:Keyword {name: $keyword})
                        WITH k
                        MATCH (a:Article {id: $article_id})
                        MERGE (a)-[:HAS_KEYWORD]->(k)
                        """,
                        keyword=keyword.strip(),
                        article_id=article_id
                    )
        
        session.write_transaction(work)
        logger.info(f"그래프 업데이트 완료: article_id={article_id}")
        
    except Exception as e:
        logger.error(f"그래프 업데이트 실패: {e}")


def get_related_articles(session, entity_name: str, limit: int = 10) -> List[int]:
    """
    특정 엔티티와 관련된 기사 ID 목록 반환
    
    Args:
        session: Neo4j 세션
        entity_name: 엔티티 이름
        limit: 최대 반환 개수
    
    Returns:
        기사 ID 리스트
    """
    try:
        def work(tx):
            # 모든 라벨에서 엔티티 찾기
            result = tx.run(
                """
                MATCH (e {name: $entity_name})<-[:MENTIONS]-(a:Article)
                RETURN DISTINCT a.id AS article_id
                ORDER BY a.id DESC
                LIMIT $limit
                """,
                entity_name=entity_name,
                limit=limit
            )
            return [record["article_id"] for record in result]
        
        article_ids = session.read_transaction(work)
        return article_ids
        
    except Exception as e:
        logger.error(f"관련 기사 조회 실패: {e}")
        return []

