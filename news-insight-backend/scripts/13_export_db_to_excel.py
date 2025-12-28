"""
DB 데이터를 Excel 파일로 내보내기 스크립트

실행 방법:
    python scripts/13_export_db_to_excel.py

생성 파일:
    - data/export_economic_variables.xlsx
    - data/export_stocks.xlsx
    - data/export_company_details.xlsx
"""
import sys
import os
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

import pandas as pd
from app.db import SessionLocal
from app.models.economic_variable import EconomicVariable
from app.models.stock import Stock
from app.models.company_detail import CompanyDetail
from app.models.company_detail_raw import CompanyDetailRaw
from app.models.edge import Edge
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def export_economic_variables():
    """경제 변수 온톨로지를 Excel로 내보내기"""
    db = SessionLocal()
    try:
        logger.info("경제 변수 온톨로지 조회 중...")
        vars = db.query(EconomicVariable).all()
        
        data = []
        for var in vars:
            data.append({
                'code': var.code,
                'name_ko': var.name_ko,
                'category': var.category,
                'layer': var.layer,
                'synonyms': '; '.join(var.synonyms) if var.synonyms else '',
                'description': var.description,
                'created_at': var.created_at.isoformat() if var.created_at else '',
                'updated_at': var.updated_at.isoformat() if var.updated_at else ''
            })
        
        df = pd.DataFrame(data)
        output_path = project_root / 'data' / 'export_economic_variables.xlsx'
        df.to_excel(output_path, index=False, engine='openpyxl')
        logger.info(f"✅ 경제 변수 온톨로지 내보내기 완료: {output_path} ({len(df)}개)")
        return df
        
    except Exception as e:
        logger.error(f"❌ 경제 변수 온톨로지 내보내기 실패: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        db.close()

def export_stocks():
    """기업 DB를 Excel로 내보내기"""
    db = SessionLocal()
    try:
        logger.info("기업 DB 조회 중...")
        stocks = db.query(Stock).all()
        
        data = []
        for stock in stocks:
            synonyms_str = '; '.join(stock.synonyms) if stock.synonyms else ''
            data.append({
                'ticker': stock.ticker,
                'stock_name': stock.stock_name,
                'market': stock.market,
                'country': stock.country,
                'industry': stock.industry if hasattr(stock, 'industry') else '',
                'synonyms': synonyms_str,
                'updated_at': stock.updated_at.isoformat() if hasattr(stock, 'updated_at') and stock.updated_at else ''
            })
        
        df = pd.DataFrame(data)
        output_path = project_root / 'data' / 'export_stocks.xlsx'
        df.to_excel(output_path, index=False, engine='openpyxl')
        logger.info(f"✅ 기업 DB 내보내기 완료: {output_path} ({len(df)}개)")
        return df
        
    except Exception as e:
        logger.error(f"❌ 기업 DB 내보내기 실패: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        db.close()

def export_company_details():
    """기업 정성 정보를 Excel로 내보내기"""
    db = SessionLocal()
    try:
        logger.info("기업 정성 정보 조회 중...")
        details = db.query(CompanyDetail).all()
        
        data = []
        for detail in details:
            # supply_chain 항목 수와 실제 Edge 수 계산
            supply_chain_items = len(detail.supply_chain) if detail.supply_chain else 0
            
            # 실제 생성된 Edge 수 조회
            edges_count = db.query(Edge).filter(
                Edge.target_id == detail.ticker,
                Edge.relation_type == "SUPPLIES_TO"
            ).count()
            
            # supply_chain에서 추정 가능한 총 공급사 수 (쉼표로 분리)
            estimated_suppliers = 0
            if detail.supply_chain:
                for sc_item in detail.supply_chain:
                    suppliers_str = sc_item.get('supplier', '')
                    if suppliers_str and suppliers_str != '정보없음':
                        suppliers = [s.strip() for s in suppliers_str.split(',') if s.strip()]
                        estimated_suppliers += len(suppliers)
            
            data.append({
                'id': detail.id,
                'ticker': detail.ticker,
                'source': detail.source,
                'biz_summary': detail.biz_summary,
                'products': ', '.join(detail.products) if detail.products else '',
                'clients': ', '.join(detail.clients) if detail.clients else '',
                'supply_chain_items': supply_chain_items,  # 원본 항목 수
                'supply_chain_suppliers_estimated': estimated_suppliers,  # 추정 공급사 수 (쉼표 분리)
                'edges_count_actual': edges_count,  # 실제 생성된 Edge 수
                'supply_chain': str(detail.supply_chain) if detail.supply_chain else '',
                'risk_factors': detail.risk_factors,
                'cost_structure': detail.cost_structure,
                'keywords': ', '.join(detail.keywords) if detail.keywords else '',
                'extracted_at': detail.extracted_at.isoformat() if detail.extracted_at else '',
                'updated_at': detail.updated_at.isoformat() if detail.updated_at else ''
            })
        
        if not data:
            logger.info("⚠️  기업 정성 정보가 없습니다.")
            return None
        
        df = pd.DataFrame(data)
        output_path = project_root / 'data' / 'export_company_details.xlsx'
        df.to_excel(output_path, index=False, engine='openpyxl')
        logger.info(f"✅ 기업 정성 정보 내보내기 완료: {output_path} ({len(df)}개)")
        return df
        
    except Exception as e:
        logger.error(f"❌ 기업 정성 정보 내보내기 실패: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        db.close()

def export_edges():
    """관계(Edge) 데이터를 Excel로 내보내기"""
    db = SessionLocal()
    try:
        logger.info("관계(Edge) 데이터 조회 중...")
        edges = db.query(Edge).all()
        
        data = []
        for edge in edges:
            data.append({
                'id': edge.id,
                'source_id': edge.source_id,
                'target_id': edge.target_id,
                'relation_type': edge.relation_type,
                'weight': edge.weight,
                'evidence': edge.evidence,
                'source': edge.source,
                'direction': edge.direction,
                'created_at': edge.created_at.isoformat() if edge.created_at else ''
            })
        
        if not data:
            logger.info("⚠️  관계(Edge) 데이터가 없습니다.")
            return None
        
        df = pd.DataFrame(data)
        output_path = project_root / 'data' / 'export_edges.xlsx'
        df.to_excel(output_path, index=False, engine='openpyxl')
        logger.info(f"✅ 관계(Edge) 데이터 내보내기 완료: {output_path} ({len(df)}개)")
        return df
        
    except Exception as e:
        logger.error(f"❌ 관계(Edge) 데이터 내보내기 실패: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        db.close()

def main():
    """메인 함수"""
    print("=" * 80)
    print("📊 DB 데이터 Excel 내보내기")
    print("=" * 80)
    print(f"시작 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    # 출력 디렉토리 생성
    output_dir = project_root / 'data'
    output_dir.mkdir(exist_ok=True)
    
    # 각 테이블 내보내기
    results = {}
    
    results['economic_variables'] = export_economic_variables()
    print()
    
    results['stocks'] = export_stocks()
    print()
    
    results['company_details'] = export_company_details()
    print()
    
    results['edges'] = export_edges()
    print()
    
    # 요약 정보
    print("=" * 80)
    print("📋 내보내기 요약")
    print("=" * 80)
    for table_name, df in results.items():
        if df is not None:
            print(f"  ✅ {table_name}: {len(df)}개 → data/export_{table_name}.xlsx")
        else:
            print(f"  ⚠️  {table_name}: 데이터 없음")
    
    print()
    print("=" * 80)
    print(f"✅ 내보내기 완료!")
    print(f"   파일 위치: {output_dir.absolute()}")
    print("=" * 80)

if __name__ == "__main__":
    main()

