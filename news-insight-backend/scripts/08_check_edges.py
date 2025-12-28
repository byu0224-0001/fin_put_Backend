"""
생성된 Edge 확인 스크립트
"""
import sys
import os
import json
from pathlib import Path
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

from app.db import SessionLocal
from app.models.edge import Edge
from app.models.stock import Stock

db = SessionLocal()
try:
    print("=" * 80)
    print("📊 생성된 Edge 확인")
    print("=" * 80)
    
    # 전체 Edge 수
    total_count = db.query(Edge).count()
    print(f"\n총 Edge 수: {total_count}개\n")
    
    # SUPPLIES_TO 관계만 필터링
    supply_edges = db.query(Edge).filter(
        Edge.relation_type == "SUPPLIES_TO"
    ).all()
    
    print(f"SUPPLIES_TO 관계: {len(supply_edges)}개\n")
    
    if supply_edges:
        print("-" * 80)
        print("생성된 공급망 관계:")
        print("-" * 80)
        
        for edge in supply_edges:
            # source 기업명 조회
            source_stock = db.query(Stock).filter(Stock.ticker == edge.source_id).first()
            source_name = source_stock.stock_name if source_stock else edge.source_id
            
            # target 기업명 조회
            target_stock = db.query(Stock).filter(Stock.ticker == edge.target_id).first()
            target_name = target_stock.stock_name if target_stock else edge.target_id
            
            # 메타데이터 파싱
            try:
                metadata = json.loads(edge.evidence) if edge.evidence else {}
                item = metadata.get('item', 'N/A')
                company_type = metadata.get('company_type', 'N/A')
                original_name = metadata.get('original_name', 'N/A')
                confidence = metadata.get('confidence', 0.0)
            except:
                item = 'N/A'
                company_type = 'N/A'
                original_name = 'N/A'
                confidence = 0.0
            
            print(f"\n🔗 {source_name} ({edge.source_id})")
            print(f"   → [{edge.relation_type}] →")
            print(f"   {target_name} ({edge.target_id})")
            print(f"   품목: {item}")
            print(f"   원본 이름: {original_name}")
            print(f"   기업 유형: {company_type}")
            print(f"   신뢰도: {confidence:.2f}")
            print(f"   가중치: {edge.weight:.2f}")
            print(f"   출처: {edge.source}")
    
    # 통계 정보
    print("\n" + "=" * 80)
    print("📈 통계 정보")
    print("=" * 80)
    
    from sqlalchemy import func
    
    # 관계 유형별 통계
    relation_stats = db.query(
        Edge.relation_type,
        func.count(Edge.id)
    ).group_by(Edge.relation_type).all()
    
    print("\n관계 유형별 Edge 수:")
    for rel_type, count in relation_stats:
        print(f"   - {rel_type}: {count}개")
    
    # 상위 공급사 (source_id 기준)
    top_suppliers = db.query(
        Edge.source_id,
        func.count(Edge.id).label('edge_count')
    ).filter(
        Edge.relation_type == "SUPPLIES_TO"
    ).group_by(Edge.source_id).order_by(func.count(Edge.id).desc()).limit(10).all()
    
    print("\n상위 공급사 (Edge 수 기준):")
    for supplier_id, count in top_suppliers:
        supplier_stock = db.query(Stock).filter(Stock.ticker == supplier_id).first()
        supplier_name = supplier_stock.stock_name if supplier_stock else supplier_id
        print(f"   - {supplier_name} ({supplier_id}): {count}개")
    
finally:
    db.close()

print("\n" + "=" * 80)
print("✅ 확인 완료!")
print("=" * 80)

