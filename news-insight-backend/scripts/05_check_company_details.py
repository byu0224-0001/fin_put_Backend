"""
기업 정성 정보 확인 스크립트

company_details와 company_details_raw 테이블의 데이터를 확인
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
from app.models.company_detail import CompanyDetail
from app.models.company_detail_raw import CompanyDetailRaw
from app.models.stock import Stock
from app.models.investor_sector import InvestorSector

db = SessionLocal()
try:
    # 1. company_details 테이블 확인
    print("=" * 80)
    print("📊 company_details 테이블 (구조화된 정성 데이터)")
    print("=" * 80)
    
    
    details_count = db.query(CompanyDetail).count()
    print(f"\n총 레코드 수: {details_count}개\n")
    
    if details_count > 0:
        # 삼성전자(005930) 데이터 조회
        samsung = db.query(CompanyDetail).filter(
            CompanyDetail.ticker == '005930'
        ).first()
        
        if samsung:
            print(f"✅ 티커: {samsung.ticker}")
            print(f"   ID: {samsung.id}")
            print(f"   소스: {samsung.source}")
            print(f"\n📝 사업 요약:")
            if samsung.biz_summary:
                summary_lines = samsung.biz_summary.split('\n')
                
                for line in summary_lines[:5]:  # 최대 5줄만 표시
                    print(f"   {line}")
                if len(summary_lines) > 5:
                    print(f"   ... (총 {len(summary_lines)}줄)")
            else:
                print("   (없음)")
            
            print(f"\n📦 주요 제품:")
            if samsung.products:
                for i, product in enumerate(samsung.products[:10], 1):
                    print(f"   {i}. {product}")
                if len(samsung.products) > 10:
                    print(f"   ... (총 {len(samsung.products)}개)")
            else:
                print("   (없음)")
            
            print(f"\n👥 주요 고객사:")
            if samsung.clients:
                if isinstance(samsung.clients, list):
                    for i, client in enumerate(samsung.clients[:10], 1):
                        print(f"   {i}. {client}")
                    if len(samsung.clients) > 10:
                        print(f"   ... (총 {len(samsung.clients)}개)")
                else:
                    print(f"   {samsung.clients}")
            else:
                print("   (없음)")
            
            print(f"\n🔗 공급망 (Supply Chain):")
            if samsung.supply_chain:
                for i, sc_item in enumerate(samsung.supply_chain[:10], 1):
                    item = sc_item.get('item', 'N/A')
                    supplier = sc_item.get('supplier', '정보없음')
                    print(f"   {i}. {item} → 공급사: {supplier}")
                if len(samsung.supply_chain) > 10:
                    print(f"   ... (총 {len(samsung.supply_chain)}개)")
            else:
                print("   (없음)")
            
            print(f"\n🔧 핵심 원재료 (하위 호환성):")
            if samsung.raw_materials:
                for i, material in enumerate(samsung.raw_materials[:10], 1):
                    print(f"   {i}. {material}")
                if len(samsung.raw_materials) > 10:
                    print(f"   ... (총 {len(samsung.raw_materials)}개)")
            else:
                print("   (없음)")
            
            print(f"\n💰 비용 구조:")
            if samsung.cost_structure:
                cost_lines = samsung.cost_structure.split('\n')
                for line in cost_lines[:5]:
                    print(f"   {line}")
                if len(cost_lines) > 5:
                    print(f"   ... (총 {len(cost_lines)}줄)")
            else:
                print("   (없음)")
            
            print(f"\n🏷️  키워드:")
            if samsung.keywords:
                print(f"   {', '.join(samsung.keywords[:20])}")
                if len(samsung.keywords) > 20:
                    print(f"   ... (총 {len(samsung.keywords)}개)")
            else:
                print("   (없음)")
            
            print(f"\n🏷️  섹터 분류:")
            inv_sector = db.query(InvestorSector).filter(
                InvestorSector.ticker == samsung.ticker
            ).first()
            if inv_sector:
                print(f"   Major Sector: {inv_sector.major_sector}")
                print(f"   Sub Sector: {inv_sector.sub_sector}")
                print(f"   Value Chain: {inv_sector.value_chain}")
                print(f"   Confidence: {inv_sector.confidence} ({inv_sector.classification_method})")
            else:
                print("   (섹터 분류 없음)")
            
            print(f"\n💰 금융사 밸류체인:")
            if samsung.financial_value_chain:
                fvc = samsung.financial_value_chain
                if fvc.get('funding_structure'):
                    print(f"   Funding: {fvc['funding_structure'].get('sources', [])}")
                if fvc.get('asset_structure'):
                    print(f"   Asset: {fvc['asset_structure'].get('industry_exposure', [])}")
                if fvc.get('risk_exposure'):
                    re = fvc['risk_exposure']
                    if re.get('credit_risk'):
                        print(f"   Credit Risk: NPL={re['credit_risk'].get('npl_ratio')}%")
            else:
                print("   (금융사 아님)")
            
            print(f"\n⏰ 추출 시간: {samsung.extracted_at}")
            print(f"   업데이트 시간: {samsung.updated_at}")
        else:
            print("⚠️  티커 005930 데이터를 찾을 수 없습니다.")
        
        # 전체 목록 (최근 5개)
        print("\n" + "-" * 80)
        print("📋 최근 저장된 기업 목록 (최대 5개):")
        recent = db.query(CompanyDetail).order_by(
            CompanyDetail.extracted_at.desc()
        ).limit(5).all()
        
        for detail in recent:
            stock = db.query(Stock).filter(Stock.ticker == detail.ticker).first()
            company_name = stock.stock_name if stock else detail.ticker
            print(f"   - {detail.ticker} ({company_name}): {detail.source}")
    else:
        print("⚠️  company_details 테이블에 데이터가 없습니다.")
    
    # 2. company_details_raw 테이블 확인
    print("\n" + "=" * 80)
    print("📄 company_details_raw 테이블 (원본 Markdown 및 LLM JSON)")
    print("=" * 80)
    
    raw_count = db.query(CompanyDetailRaw).count()
    print(f"\n총 레코드 수: {raw_count}개\n")
    
    if raw_count > 0:
        # 삼성전자(005930) Raw 데이터 조회
        samsung_raw = db.query(CompanyDetailRaw).filter(
            CompanyDetailRaw.ticker == '005930'
        ).first()
        
        if samsung_raw:
            print(f"✅ 티커: {samsung_raw.ticker}")
            print(f"   ID: {samsung_raw.id}")
            print(f"   소스: {samsung_raw.source}")
            print(f"   연도: {samsung_raw.year}")
            print(f"   처리 상태: {samsung_raw.processing_status}")
            
            print(f"\n📝 Raw Markdown (처음 1000자):")
            if samsung_raw.raw_markdown:
                preview = samsung_raw.raw_markdown[:1000]
                print(f"   {preview}")
                print(f"\n   ... (전체 길이: {len(samsung_raw.raw_markdown):,}자)")
            else:
                print("   (없음)")
            
            print(f"\n🤖 LLM JSON (raw_json):")
            if samsung_raw.raw_json:
                json_str = json.dumps(samsung_raw.raw_json, ensure_ascii=False, indent=2)
                # 처음 2000자만 표시
                if len(json_str) > 2000:
                    print(json_str[:2000])
                    print(f"\n   ... (전체 길이: {len(json_str):,}자)")
                else:
                    print(json_str)
            else:
                print("   (없음)")
            
            print(f"\n⏰ 가져온 시간: {samsung_raw.fetched_at}")
            print(f"   처리 시간: {samsung_raw.processed_at}")
            print(f"   생성 시간: {samsung_raw.created_at}")
        else:
            print("⚠️  티커 005930 Raw 데이터를 찾을 수 없습니다.")
        
        # 전체 목록 (최근 5개)
        print("\n" + "-" * 80)
        print("📋 최근 저장된 Raw 데이터 목록 (최대 5개):")
        recent_raw = db.query(CompanyDetailRaw).order_by(
            CompanyDetailRaw.fetched_at.desc()
        ).limit(5).all()
        
        for raw in recent_raw:
            stock = db.query(Stock).filter(Stock.ticker == raw.ticker).first()
            company_name = stock.stock_name if stock else raw.ticker
            print(f"   - {raw.ticker} ({company_name}): {raw.source} [{raw.processing_status}]")
    else:
        print("⚠️  company_details_raw 테이블에 데이터가 없습니다.")
    
    # 3. 섹터 분류 통계
    print("\n" + "=" * 80)
    print("🏷️  섹터 분류 통계")
    print("=" * 80)
    
    from sqlalchemy import func
    sector_count = db.query(InvestorSector).count()
    print(f"\n분류된 기업 수: {sector_count}개")
    
    if sector_count > 0:
        sector_dist = db.query(
            InvestorSector.major_sector,
            func.count(InvestorSector.id)
        ).group_by(InvestorSector.major_sector).order_by(
            func.count(InvestorSector.id).desc()
        ).limit(10).all()
        
        print("\nMajor Sector 상위 10개:")
        for sector, count in sector_dist:
            print(f"   {sector}: {count}개")
    
    # 4. 통계 정보
    print("\n" + "=" * 80)
    print("📈 통계 정보")
    print("=" * 80)
    
    from sqlalchemy import func
    status_counts = db.query(
        CompanyDetailRaw.processing_status,
        func.count(CompanyDetailRaw.id)
    ).group_by(CompanyDetailRaw.processing_status).all()
    
    print("\n처리 상태별 Raw 데이터 수:")
    for status, count in status_counts:
        print(f"   - {status}: {count}개")
    
    # 데이터 일치성 확인
    print("\n데이터 일치성 확인:")
    details_tickers = set(db.query(CompanyDetail.ticker).distinct().all())
    raw_tickers = set(db.query(CompanyDetailRaw.ticker).distinct().all())
    details_tickers = {t[0] for t in details_tickers}
    raw_tickers = {t[0] for t in raw_tickers}
    
    print(f"   - company_details 티커 수: {len(details_tickers)}개")
    print(f"   - company_details_raw 티커 수: {len(raw_tickers)}개")
    
    if details_tickers == raw_tickers:
        print("   ✅ 두 테이블의 티커가 일치합니다.")
    else:
        only_details = details_tickers - raw_tickers
        only_raw = raw_tickers - details_tickers
        if only_details:
            print(f"   ⚠️  company_details에만 있는 티커: {only_details}")
        if only_raw:
            print(f"   ⚠️  company_details_raw에만 있는 티커: {only_raw}")
    
finally:
    db.close()

print("\n" + "=" * 80)
print("✅ 확인 완료!")
print("=" * 80)

