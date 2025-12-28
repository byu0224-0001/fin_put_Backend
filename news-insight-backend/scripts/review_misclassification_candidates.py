# -*- coding: utf-8 -*-
"""
오분류 후보 검토 및 수정 스크립트
"""
import sys
import os

# Windows 환경에서 UTF-8 인코딩 설정
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'
else:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

sys.path.insert(0, '.')

from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock
from app.models.investor_sector import InvestorSector
from app.services.sector_classifier import calculate_revenue_sector_scores

def main():
    try:
        db = SessionLocal()
        
        print("=" * 80, flush=True)
        print("오분류 후보 검토 및 수정", flush=True)
        print("=" * 80, flush=True)
    
    # 오분류 후보 조회 (매출 1등 섹터 vs 현재 섹터 충돌)
    all_details = db.query(CompanyDetail, Stock).join(
        Stock, CompanyDetail.ticker == Stock.ticker
    ).all()
    
    misclassification_candidates = []
    
    for detail, stock in all_details:
        if not detail.revenue_by_segment or not isinstance(detail.revenue_by_segment, dict):
            continue
        
        # 현재 분류 조회
        current_sector = db.query(InvestorSector).filter(
            InvestorSector.ticker == detail.ticker,
            InvestorSector.is_primary == True
        ).first()
        current_major = current_sector.major_sector if current_sector else None
        
        # 매출 비중 -> 섹터 점수 계산
        revenue_scores, _ = calculate_revenue_sector_scores(detail.revenue_by_segment)
        
        # 매출 기반 최고 섹터
        revenue_best_sector = max(revenue_scores.items(), key=lambda x: x[1])[0] if revenue_scores else None
        
        # 오분류 후보 체크
        if revenue_best_sector and current_major and revenue_best_sector != current_major:
            best_score = revenue_scores.get(revenue_best_sector, 0)
            if best_score >= 0.3:  # 30% 이상 매출 비중인 경우만
                misclassification_candidates.append({
                    'ticker': detail.ticker,
                    'name': stock.stock_name,
                    'current_sector': current_major,
                    'revenue_best_sector': revenue_best_sector,
                    'revenue_score': best_score,
                    'revenue_by_segment': detail.revenue_by_segment,
                    'current_sub_sector': current_sector.sub_sector if current_sector else None,
                    'current_value_chain': current_sector.value_chain if current_sector else None
                })
    
    # 점수순 정렬
    sorted_misclass = sorted(misclassification_candidates, key=lambda x: x['revenue_score'], reverse=True)
    
    print(f"\n총 오분류 후보: {len(sorted_misclass)}개", flush=True)
    print("\n" + "=" * 80, flush=True)
    print("Top 20 오분류 후보 상세", flush=True)
    print("=" * 80, flush=True)
    
    for i, mc in enumerate(sorted_misclass[:20], 1):
        print(f"\n{i}. {mc['name']} ({mc['ticker']})", flush=True)
        print(f"   현재: {mc['current_sector']} (sub: {mc['current_sub_sector']}, chain: {mc['current_value_chain']})", flush=True)
        print(f"   매출기반: {mc['revenue_best_sector']} (score: {mc['revenue_score']:.3f})", flush=True)
        print(f"   매출비중 Top 3:", flush=True)
        if mc['revenue_by_segment']:
            sorted_rev = sorted(
                mc['revenue_by_segment'].items(), 
                key=lambda x: x[1] if isinstance(x[1], (int, float)) else 0, 
                reverse=True
            )[:3]
            for seg, pct in sorted_rev:
                print(f"     - {seg}: {pct}%", flush=True)
    
    # 수정 제안
    print("\n" + "=" * 80, flush=True)
    print("수정 제안 (자동 재분류 가능)", flush=True)
    print("=" * 80, flush=True)
    
    auto_fix_candidates = []
    for mc in sorted_misclass[:20]:
        # 지주회사 → 실제 사업 섹터는 자동 수정 가능
        if mc['current_sector'] == 'SEC_HOLDING' and mc['revenue_score'] >= 0.7:
            auto_fix_candidates.append(mc)
        # 명확한 오분류 (score >= 0.8)
        elif mc['revenue_score'] >= 0.8:
            auto_fix_candidates.append(mc)
    
    print(f"\n자동 수정 가능 후보: {len(auto_fix_candidates)}개", flush=True)
    for mc in auto_fix_candidates:
        print(f"  - {mc['name']} ({mc['ticker']}): {mc['current_sector']} → {mc['revenue_best_sector']}", flush=True)
    
    print("\n" + "=" * 80, flush=True)
    print("수동 검토 필요 후보", flush=True)
    print("=" * 80, flush=True)
    
    manual_review = [mc for mc in sorted_misclass[:20] if mc not in auto_fix_candidates]
    print(f"\n수동 검토 필요: {len(manual_review)}개", flush=True)
    for mc in manual_review:
        print(f"  - {mc['name']} ({mc['ticker']}): {mc['current_sector']} → {mc['revenue_best_sector']} (score: {mc['revenue_score']:.3f})", flush=True)
    
    db.close()
    except Exception as e:
        import traceback
        print(f"❌ 오류 발생: {e}", flush=True)
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n사용자에 의해 중단되었습니다.", flush=True)
        sys.exit(0)
    except Exception as e:
        import traceback
        print(f"\n\n❌ 치명적 오류: {e}", flush=True)
        traceback.print_exc()
        sys.exit(1)

