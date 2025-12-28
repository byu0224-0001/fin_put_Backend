"""
Step 5: 관계 추출 스크립트

company_details의 supply_chain 및 clients 데이터를 활용하여 KG Edge 생성
- supply_chain → SUPPLIES_TO edges 변환 (Upstream: 누구한테 사오는가)
- clients → SELLS_TO edges 변환 (Downstream: 누구한테 파는가)
- 공급업체/고객사 이름 정규화 및 Entity Resolution
- 비상장사 처리 포함

실행 방법:
    python scripts/05_extract_relations.py [--ticker 005930] [--use-llm]
    
옵션:
    --ticker: 특정 티커만 처리 (테스트용)
    --use-llm: LLM Fallback 활성화 (비용 증가, 기본 False)
"""
import sys
import os
import json
import hashlib
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any, Optional

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Windows 환경에서 인코딩 문제 방지
if sys.platform == 'win32':
    import codecs
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

from dotenv import load_dotenv
load_dotenv()

import logging
import argparse
from sqlalchemy.orm import Session

from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
from app.models.edge import Edge
from app.models.stock import Stock
from app.models.investor_sector import InvestorSector
from app.services.entity_resolver import resolve_entity_comprehensive, normalize_company_name

logging.basicConfig(
    level=logging.INFO,  # INFO 레벨로 변경 (과도한 DEBUG 로그 방지)
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def create_supply_edges(
    db: Session,
    company_detail: CompanyDetail,
    use_llm: bool = False
) -> List[Edge]:
    """
    supply_chain 데이터에서 Edge 생성
    
    Args:
        db: DB 세션
        company_detail: CompanyDetail 객체
        use_llm: LLM Fallback 사용 여부
    
    Returns:
        생성된 Edge 리스트
    """
    edges = []
    
    if not company_detail.supply_chain:
        logger.debug(f"[{company_detail.ticker}] supply_chain 데이터 없음")
        return edges
    
    for sc_item in company_detail.supply_chain:
        item = sc_item.get('item', '')
        suppliers_str = sc_item.get('supplier', '')
        
        if not suppliers_str or suppliers_str == '정보없음':
            continue
        
        # 쉼표로 분리
        suppliers = [s.strip() for s in suppliers_str.split(',')]
        
        for supplier in suppliers:
            if not supplier:
                continue
            
            # Entity Resolution (5단계 전략)
            resolved_id, company_type, confidence = resolve_entity_comprehensive(
                db, supplier, use_llm=use_llm
            )
            
            if not resolved_id:
                logger.warning(f"[{company_detail.ticker}] 공급업체 '{supplier}' 매칭 실패")
                continue
            
            # Edge ID 생성
            if company_type == "LISTED":
                source_id = resolved_id  # ticker
            else:
                # 비상장사: 해시 기반 ID 생성
                normalized_name = normalize_company_name(resolved_id)
                source_id = f"UNLISTED_{hashlib.md5(normalized_name.encode()).hexdigest()[:8]}"
            
            # Edge ID: source_target_relation_item_hash
            item_hash = hashlib.md5(item.encode()).hexdigest()[:8]
            edge_id = f"{source_id}_{company_detail.ticker}_SUPPLIES_TO_{item_hash}"
            
            # 중복 확인
            existing = db.query(Edge).filter(Edge.id == edge_id).first()
            if existing:
                logger.debug(f"[{company_detail.ticker}] Edge 중복 스킵: {edge_id}")
                continue
            
            # 메타데이터 생성 (JSONB로 저장할 dict 형태)
            metadata = {
                "company_type": company_type,
                "item": item,
                "original_name": supplier,
                "resolved_name": resolved_id,
                "confidence": confidence,
                "source_detail": company_detail.source
            }
            
            # Edge 생성
            edge = Edge(
                id=edge_id,
                source_id=source_id,
                target_id=company_detail.ticker,
                relation_type="SUPPLIES_TO",
                weight=confidence,  # 신뢰도 기반 가중치
                evidence=metadata,  # dict 형태로 저장 (JSONB 자동 변환)
                source=company_detail.source,  # "DART_2024"
                direction="DIRECTED"
            )
            
            edges.append(edge)
            logger.debug(f"[{company_detail.ticker}] Edge 생성: {supplier} -> {company_detail.ticker} ({item})")
    
    return edges


def create_client_edges(
    db: Session,
    company_detail: CompanyDetail,
    use_llm: bool = False
) -> List[Edge]:
    """
    clients 데이터에서 Edge 생성 (SELLS_TO 관계)
    
    Args:
        db: DB 세션
        company_detail: CompanyDetail 객체
        use_llm: LLM Fallback 사용 여부
    
    Returns:
        생성된 Edge 리스트
    """
    edges = []
    
    if not company_detail.clients:
        logger.debug(f"[{company_detail.ticker}] clients 데이터 없음")
        return edges
    
    # clients 필드가 문자열인지 리스트인지 확인
    clients_data = company_detail.clients
    
    # 문자열인 경우 처리 ("정보없음" 또는 "Apple, 현대차" 형태)
    if isinstance(clients_data, str):
        if clients_data == "정보없음" or not clients_data.strip():
            return edges
        # 쉼표로 분리
        clients_list = [c.strip() for c in clients_data.split(',')]
    # 리스트인 경우
    elif isinstance(clients_data, list):
        clients_list = clients_data
    else:
        logger.warning(f"[{company_detail.ticker}] clients 데이터 형식 오류: {type(clients_data)}")
        return edges
    
    for client in clients_list:
        if not client or client == "정보없음":
            continue
        
        # Entity Resolution (5단계 전략)
        resolved_id, company_type, confidence = resolve_entity_comprehensive(
            db, client, use_llm=use_llm
        )
        
        if not resolved_id:
            logger.warning(f"[{company_detail.ticker}] 고객사 '{client}' 매칭 실패 (resolved_id가 None)")
            continue
        else:
            logger.debug(f"[{company_detail.ticker}] 고객사 '{client}' -> {resolved_id} ({company_type}, confidence={confidence:.2f})")
        
        # Edge ID 생성
        if company_type == "LISTED":
            target_id = resolved_id  # ticker
        else:
            # 비상장사: 해시 기반 ID 생성
            normalized_name = normalize_company_name(resolved_id)
            target_id = f"UNLISTED_{hashlib.md5(normalized_name.encode()).hexdigest()[:8]}"
        
        # Edge ID: source_target_relation_hash
        # SELLS_TO는 품목 정보가 없으므로 해시만 사용
        edge_id = f"{company_detail.ticker}_{target_id}_SELLS_TO_{hashlib.md5(client.encode()).hexdigest()[:8]}"
        
        # 중복 확인
        existing = db.query(Edge).filter(Edge.id == edge_id).first()
        if existing:
            logger.debug(f"[{company_detail.ticker}] Edge 중복 스킵: {edge_id}")
            continue
        
        # 메타데이터 생성 (JSONB로 저장할 dict 형태)
        metadata = {
            "company_type": company_type,
            "original_name": client,
            "resolved_name": resolved_id,
            "confidence": confidence,
            "source_detail": company_detail.source
        }
        
        # Edge 생성 (SELLS_TO: 기업 -> 고객사)
        edge = Edge(
            id=edge_id,
            source_id=company_detail.ticker,
            target_id=target_id,
            relation_type="SELLS_TO",
            weight=confidence,  # 신뢰도 기반 가중치
            evidence=metadata,  # dict 형태로 저장 (JSONB 자동 변환)
            source=company_detail.source,  # "DART_2024"
            direction="DIRECTED"
        )
        
        edges.append(edge)
        logger.debug(f"[{company_detail.ticker}] Edge 생성: {company_detail.ticker} -> {client} (SELLS_TO)")
    
    return edges


def create_reverse_supply_edges(
    db: Session,
    company_detail: CompanyDetail
) -> List[Edge]:
    """
    Phase 1: 역방향 추론
    
    기업 A의 supply_chain에 있는 원재료를
    다른 기업 B의 products에서 검색하여 관계 추론
    
    예: LG에너지솔루션(supply_chain: 양극재) ← 포스코퓨처엠(products: 양극재)
    
    Args:
        db: DB 세션
        company_detail: CompanyDetail 객체 (수요자)
    
    Returns:
        생성된 Edge 리스트
    """
    edges = []
    
    if not company_detail.supply_chain:
        return edges
    
    # 필요한 원재료 추출
    needed_items = []
    for sc_item in company_detail.supply_chain:
        item = sc_item.get('item', '')
        if item and item != '정보없음':
            needed_items.append(item.lower().strip())
    
    if not needed_items:
        return edges
    
    # 다른 기업들의 products 검색
    other_companies = db.query(CompanyDetail).filter(
        CompanyDetail.ticker != company_detail.ticker
    ).all()
    
    for other in other_companies:
        if not other.products:
            continue
        
        other_products = [str(p).lower().strip() for p in other.products if p]
        
        # 매칭 검사
        matched_items = []
        for item in needed_items:
            for product in other_products:
                # 부분 매칭 (양방향)
                if item in product or product in item:
                    matched_items.append(item)
                    break
        
        if not matched_items:
            continue
        
        # Edge ID 생성
        edge_id = f"{other.ticker}_{company_detail.ticker}_POTENTIAL_SUPPLIES_TO_{hashlib.md5('_'.join(matched_items).encode()).hexdigest()[:8]}"
        
        # 중복 확인
        existing = db.query(Edge).filter(Edge.id == edge_id).first()
        if existing:
            continue
        
        # 메타데이터 생성
        metadata = {
            "inference_type": "REVERSE_SUPPLY",
            "matched_items": matched_items[:5],  # 최대 5개
            "confidence": "MEDIUM",
            "source_products": other_products[:5],
            "target_supply_chain": needed_items[:5]
        }
        
        # Edge 생성 (POTENTIAL_SUPPLIES_TO: 공급 가능성)
        edge = Edge(
            id=edge_id,
            source_id=other.ticker,  # 공급자 (products에 있는 기업)
            target_id=company_detail.ticker,  # 수요자 (supply_chain에 있는 기업)
            relation_type="POTENTIAL_SUPPLIES_TO",
            weight=0.6,  # 중간 신뢰도
            evidence=metadata,
            source="INFERENCE_REVERSE",
            direction="DIRECTED"
        )
        
        edges.append(edge)
        logger.debug(f"[역방향 추론] {other.ticker} -> {company_detail.ticker} (items: {matched_items[:3]})")
    
    return edges


def create_value_chain_edges(
    db: Session,
    company_detail: CompanyDetail
) -> List[Edge]:
    """
    Phase 2: 섹터/밸류체인 기반 관계 추론
    
    같은 섹터 내에서 밸류체인 순서에 따라 관계 추론
    UPSTREAM → MIDSTREAM → DOWNSTREAM
    
    Args:
        db: DB 세션
        company_detail: CompanyDetail 객체
    
    Returns:
        생성된 Edge 리스트
    """
    edges = []
    
    # 현재 기업의 섹터/밸류체인 정보 조회
    company_sector = db.query(InvestorSector).filter(
        InvestorSector.ticker == company_detail.ticker,
        InvestorSector.is_primary == True
    ).first()
    
    if not company_sector or not company_sector.value_chain:
        return edges
    
    # 밸류체인 순서 정의
    VALUE_CHAIN_ORDER = {
        'UPSTREAM': 0,
        'MIDSTREAM': 1,
        'DOWNSTREAM': 2
    }
    
    my_order = VALUE_CHAIN_ORDER.get(company_sector.value_chain, -1)
    if my_order < 0:
        return edges
    
    # 같은 섹터의 다른 기업 검색
    same_sector_companies = db.query(InvestorSector).filter(
        InvestorSector.major_sector == company_sector.major_sector,
        InvestorSector.ticker != company_detail.ticker,
        InvestorSector.is_primary == True,
        InvestorSector.value_chain != None
    ).all()
    
    for other_sector in same_sector_companies:
        other_order = VALUE_CHAIN_ORDER.get(other_sector.value_chain, -1)
        if other_order < 0:
            continue
        
        # UPSTREAM → MIDSTREAM → DOWNSTREAM 순서 확인
        # other가 나보다 upstream이면 → other가 나에게 공급 가능
        if other_order < my_order:
            edge_id = f"{other_sector.ticker}_{company_detail.ticker}_VALUE_CHAIN_RELATED_{company_sector.major_sector}"
            
            # 중복 확인
            existing = db.query(Edge).filter(Edge.id == edge_id).first()
            if existing:
                continue
            
            # 메타데이터 생성
            metadata = {
                "inference_type": "VALUE_CHAIN",
                "sector": company_sector.major_sector,
                "source_value_chain": other_sector.value_chain,
                "target_value_chain": company_sector.value_chain,
                "confidence": "LOW"
            }
            
            # Edge 생성
            edge = Edge(
                id=edge_id,
                source_id=other_sector.ticker,
                target_id=company_detail.ticker,
                relation_type="VALUE_CHAIN_RELATED",
                weight=0.4,  # 낮은 신뢰도
                evidence=metadata,
                source="INFERENCE_VALUE_CHAIN",
                direction="DIRECTED"
            )
            
            edges.append(edge)
            logger.debug(f"[밸류체인] {other_sector.ticker}({other_sector.value_chain}) -> {company_detail.ticker}({company_sector.value_chain})")
    
    return edges


def extract_relations(
    db: Session,
    ticker: Optional[str] = None,
    use_llm: bool = False
) -> Dict[str, Any]:
    """
    관계 추출 메인 함수
    
    Args:
        db: DB 세션
        ticker: 특정 티커만 처리 (None이면 전체)
        use_llm: LLM Fallback 사용 여부
    
    Returns:
        처리 결과 통계
    """
    # company_details 조회 (supply_chain 또는 clients가 있고, 실제 데이터가 있는 기업)
    # 먼저 모든 후보를 가져온 후 Python에서 필터링 (빈 배열/빈 문자열 제외)
    query = db.query(CompanyDetail).filter(
        (CompanyDetail.supply_chain.isnot(None)) | (CompanyDetail.clients.isnot(None))
    )
    
    if ticker:
        query = query.filter(CompanyDetail.ticker == ticker)
    
    all_candidates = query.all()
    
    logger.info(f"SQL 쿼리 결과: {len(all_candidates)}개 후보 기업")
    
    # 실제로 유효한 데이터가 있는 기업만 필터링
    company_details = []
    filtered_out_count = 0
    for detail in all_candidates:
        has_valid_supply = False
        has_valid_clients = False
        
        # supply_chain 검증
        if detail.supply_chain:
            if isinstance(detail.supply_chain, list):
                # 빈 배열이 아니고, 실제 supplier 데이터가 있는지 확인
                for sc_item in detail.supply_chain:
                    supplier = sc_item.get('supplier', '') if isinstance(sc_item, dict) else ''
                    if supplier and supplier != '정보없음' and supplier.strip():
                        has_valid_supply = True
                        break
        
        # clients 검증
        if detail.clients:
            if isinstance(detail.clients, list):
                # 빈 배열이 아니면 유효
                if len(detail.clients) > 0:
                    has_valid_clients = True
            elif isinstance(detail.clients, str):
                # 빈 문자열이 아니고 "정보없음"이 아니면 유효
                if detail.clients.strip() and detail.clients != "정보없음":
                    has_valid_clients = True
        
        if has_valid_supply or has_valid_clients:
            company_details.append(detail)
        else:
            filtered_out_count += 1
            # DEBUG 로그 제거 (과도한 로그 출력 방지)
            # logger.debug(f"[{detail.ticker}] Python 필터링에서 제외: has_valid_supply={has_valid_supply}, has_valid_clients={has_valid_clients}")
    
    logger.info(f"Python 필터링 결과: {len(company_details)}개 기업 (제외: {filtered_out_count}개)")
    
    if not company_details:
        logger.warning("처리할 company_details 데이터가 없습니다. (빈 배열/빈 문자열 제외)")
        return {
            "total": 0, 
            "created": 0, 
            "skipped": 0, 
            "duplicate": 0,
            "supplies_to": 0,
            "sells_to": 0,
            "potential_supplies_to": 0,
            "value_chain_related": 0,
            "companies": 0
        }
    
    logger.info(f"총 {len(company_details)}개 기업의 관계 추출 시작")
    
    total_edges = 0
    created_edges = 0
    skipped_edges = 0
    duplicate_edges = 0
    supplies_to_count = 0
    sells_to_count = 0
    potential_supplies_count = 0
    value_chain_count = 0
    
    for detail in company_details:
        logger.info(f"[{detail.ticker}] 관계 추출 중...")
        
        # 1. SUPPLIES_TO Edge 생성 (Upstream: 누구한테 사오는가)
        supply_edges = create_supply_edges(db, detail, use_llm=use_llm)
        supplies_to_count += len(supply_edges)
        
        # 2. SELLS_TO Edge 생성 (Downstream: 누구한테 파는가)
        client_edges = create_client_edges(db, detail, use_llm=use_llm)
        sells_to_count += len(client_edges)
        if len(client_edges) > 0:
            logger.info(f"[{detail.ticker}] SELLS_TO Edge {len(client_edges)}개 생성")
        else:
            logger.debug(f"[{detail.ticker}] SELLS_TO Edge 생성 없음 (clients: {detail.clients is not None})")
        
        # 3. POTENTIAL_SUPPLIES_TO Edge 생성 (Phase 1: 역방향 추론)
        reverse_edges = create_reverse_supply_edges(db, detail)
        potential_supplies_count += len(reverse_edges)
        
        # 4. VALUE_CHAIN_RELATED Edge 생성 (Phase 2: 섹터/밸류체인 기반)
        value_chain_edges = create_value_chain_edges(db, detail)
        value_chain_count += len(value_chain_edges)
        
        # 모든 Edge 합치기
        all_edges = supply_edges + client_edges + reverse_edges + value_chain_edges
        
        if not all_edges:
            logger.debug(f"[{detail.ticker}] 생성된 Edge 없음")
        
        # DB 저장
        for edge in all_edges:
            try:
                db.add(edge)
                created_edges += 1
            except Exception as e:
                # 중복 키 오류인지 확인
                if "duplicate key" in str(e).lower() or "unique constraint" in str(e).lower():
                    duplicate_edges += 1
                    logger.debug(f"[{detail.ticker}] Edge 중복 스킵: {edge.id}")
                else:
                    logger.error(f"[{detail.ticker}] Edge 저장 실패: {e}")
                skipped_edges += 1
        
        total_edges += len(all_edges)
        
        # 중간 커밋 (메모리 관리) - 0개일 때는 로그 출력 안 함
        if created_edges > 0 and created_edges % 50 == 0:
            db.commit()
            logger.info(f"중간 커밋: {created_edges}개 Edge 저장")
    
    # 최종 커밋
    db.commit()
    
    return {
        "total": total_edges,
        "created": created_edges,
        "skipped": skipped_edges,
        "duplicate": duplicate_edges,
        "supplies_to": supplies_to_count,
        "sells_to": sells_to_count,
        "potential_supplies_to": potential_supplies_count,
        "value_chain_related": value_chain_count,
        "companies": len(company_details)
    }


def main():
    """메인 함수"""
    parser = argparse.ArgumentParser(description='공급망 관계 추출 스크립트')
    parser.add_argument('--ticker', type=str, default=None, help='특정 티커만 처리 (테스트용)')
    parser.add_argument('--use-llm', action='store_true', help='LLM Fallback 활성화 (비용 증가)')
    args = parser.parse_args()
    
    logger.info("=" * 60)
    logger.info("Step 5: 공급망 관계 추출 (supply_chain + clients → edges)")
    logger.info("=" * 60)
    logger.info(f"특정 티커: {args.ticker if args.ticker else '전체'}")
    logger.info(f"LLM Fallback: {'활성화' if args.use_llm else '비활성화'}")
    logger.info("=" * 60)
    
    db = SessionLocal()
    try:
        results = extract_relations(db, ticker=args.ticker, use_llm=args.use_llm)
        
        logger.info("=" * 60)
        logger.info("관계 추출 완료!")
        logger.info(f"처리 기업 수: {results['companies']}개")
        logger.info(f"생성된 Edge: {results['created']}개")
        logger.info(f"  - SUPPLIES_TO (명시적 공급): {results.get('supplies_to', 0)}개")
        logger.info(f"  - SELLS_TO (명시적 판매): {results.get('sells_to', 0)}개")
        logger.info(f"  - POTENTIAL_SUPPLIES_TO (역방향 추론): {results.get('potential_supplies_to', 0)}개")
        logger.info(f"  - VALUE_CHAIN_RELATED (밸류체인 기반): {results.get('value_chain_related', 0)}개")
        logger.info(f"중복 스킵: {results.get('duplicate', 0)}개")
        logger.info(f"스킵된 Edge: {results['skipped']}개")
        logger.info(f"총 Edge 생성 시도: {results['total']}개")
        logger.info("=" * 60)
        
    except Exception as e:
        logger.error(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()


if __name__ == "__main__":
    exit_code = 0
    try:
        main()
    except KeyboardInterrupt:
        logger.warning("사용자에 의해 중단됨")
        exit_code = 1
    except Exception as e:
        logger.error(f"오류 발생: {e}")
        import traceback
        traceback.print_exc()
        exit_code = 1
    finally:
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        except:
            pass
    
    sys.exit(exit_code)

