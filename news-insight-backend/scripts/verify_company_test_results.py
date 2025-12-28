"""
COMPANY 리포트 테스트 결과 검증 스크립트
"""
import sys
import os
import json
from pathlib import Path
from collections import defaultdict

# Windows 인코딩 처리
if sys.platform == 'win32':
    import codecs
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

from app.db import SessionLocal
from app.models.edge import Edge
from app.models.industry_edge import IndustryEdge
from app.models.investor_sector import InvestorSector
from sqlalchemy import and_


def verify_company_test_results():
    """
    COMPANY 리포트 테스트 결과 검증
    """
    print("=" * 80)
    print("[COMPANY 리포트 테스트 결과 검증]")
    print("=" * 80)
    
    # 1. 최신 enrichment_results 파일 찾기
    result_files = list(Path("reports").glob("enrichment_results_*.json"))
    if not result_files:
        print("[오류] enrichment_results 파일을 찾을 수 없습니다.")
        return
    
    latest_file = max(result_files, key=lambda p: p.stat().st_mtime)
    print(f"[분석 파일] {latest_file}\n")
    
    with open(latest_file, 'r', encoding='utf-8') as f:
        results = json.load(f)
    
    # 2. COMPANY 리포트 필터링
    company_results = [r for r in results if r.get('route_type') == 'COMPANY']
    
    print(f"[전체 리포트] {len(results)}개")
    print(f"[COMPANY 리포트] {len(company_results)}개\n")
    
    if not company_results:
        print("  ⚠️  COMPANY 리포트가 없습니다.")
        return
    
    # 3. Ticker 매칭 성공률
    ticker_matched = [r for r in company_results if r.get('ticker')]
    ticker_match_rate = len(ticker_matched) / len(company_results) * 100 if company_results else 0
    
    print(f"[Ticker 매칭]")
    print(f"  - 성공: {len(ticker_matched)}개")
    print(f"  - 실패: {len(company_results) - len(ticker_matched)}개")
    print(f"  - 성공률: {ticker_match_rate:.1f}%\n")
    
    # 4. Company Edges 저장 확인
    db = SessionLocal()
    try:
        company_edges = db.query(Edge).filter(
            Edge.relation_type == "DRIVEN_BY"
        ).all()
        
        print(f"[Company Edges]")
        print(f"  - 총 저장된 Edges: {len(company_edges)}개\n")
        
        # 5. Industry Edges와 연결 확인
        print(f"[Industry-Company 연결 확인]")
        
        # 각 Company Edge에 대해 Industry Edge 찾기
        connected_count = 0
        connection_samples = []
        
        for ce in company_edges[:10]:  # 최대 10개 샘플
            ticker = ce.source_id
            
            # InvestorSector에서 섹터 찾기
            sectors = db.query(InvestorSector).filter(
                and_(
                    InvestorSector.ticker == ticker,
                    InvestorSector.is_primary == True
                )
            ).all()
            
            if sectors:
                for sector in sectors:
                    # 같은 섹터의 Industry Edge 찾기
                    industry_edges = db.query(IndustryEdge).filter(
                        and_(
                            IndustryEdge.target_sector_code == sector.sector_l1,
                            IndustryEdge.is_active == "TRUE"
                        )
                    ).limit(1).all()
                    
                    if industry_edges:
                        connected_count += 1
                        connection_samples.append({
                            "ticker": ticker,
                            "company_sector": sector.sector_l1,
                            "industry_edge": industry_edges[0].target_sector_code,
                            "driver": ce.target_id
                        })
                        break
        
        print(f"  - 연결된 Company: {connected_count}개")
        print(f"  - 샘플 연결:")
        for i, sample in enumerate(connection_samples[:3], 1):
            print(f"    [{i}] Ticker: {sample['ticker']}, Sector: {sample['company_sector']}, Driver: {sample['driver']}")
        
    finally:
        db.close()
    
    # 6. 최종 평가
    print(f"\n" + "=" * 80)
    print("[최종 평가]")
    print("=" * 80)
    
    print(f"  - Ticker 매칭 성공률: {ticker_match_rate:.1f}%")
    if ticker_match_rate >= 60:
        print(f"    ✅ 목표 달성 (60%+)")
    else:
        print(f"    ⚠️  목표 미달 (60%+ 필요)")
    
    print(f"  - Company Edges 저장: {len(company_edges)}개")
    if len(company_edges) > 0:
        print(f"    ✅ 저장 성공")
    else:
        print(f"    ⚠️  저장 실패")
    
    print(f"  - Industry-Company 연결: {connected_count}개")
    if connected_count >= 3:
        print(f"    ✅ 목표 달성 (3개+)")
    else:
        print(f"    ⚠️  목표 미달 (3개+ 필요)")


if __name__ == "__main__":
    verify_company_test_results()

