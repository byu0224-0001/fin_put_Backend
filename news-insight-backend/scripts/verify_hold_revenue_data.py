# -*- coding: utf-8 -*-
"""
Top200 HOLD 기업 매출 데이터 실제 보유율 확인 스크립트

리포트의 has_revenue_data와 실제 DB 데이터를 비교하여 불일치 확인
"""
import sys
import os
import json

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

def verify_hold_revenue_data():
    """Top200 HOLD 기업 매출 데이터 실제 보유율 확인"""
    db = SessionLocal()
    
    try:
        print("=" * 80, flush=True)
        print("Top200 HOLD 기업 매출 데이터 실제 보유율 확인", flush=True)
        print("=" * 80, flush=True)
        
        # DRY RUN 결과 파일 로드
        report_file = 'reports/reclassify_all_companies_report.json'
        if not os.path.exists(report_file):
            print(f"\n❌ DRY RUN 결과 파일이 없습니다: {report_file}", flush=True)
            return None
        
        with open(report_file, 'r', encoding='utf-8') as f:
            report = json.load(f)
        
        top100_hold = report.get('stats', {}).get('top100_hold', [])
        top200_hold = [h for h in top100_hold if h.get('market_cap', 0) >= 1000000000000]
        
        print(f"\nTop200 HOLD 기업: {len(top200_hold)}개", flush=True)
        
        # 실제 매출 데이터 확인
        verified_data = []
        mismatch_count = 0
        actual_has_revenue = 0
        
        for company in top200_hold:
            ticker = company['ticker']
            name = company['name']
            reported_has_revenue = company.get('has_revenue_data', False)
            
            # 실제 DB에서 확인
            detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == ticker
            ).order_by(CompanyDetail.updated_at.desc()).first()
            
            actual_has_revenue_data = bool(
                detail and detail.revenue_by_segment 
                and isinstance(detail.revenue_by_segment, dict) 
                and len(detail.revenue_by_segment) > 0
            )
            
            if actual_has_revenue_data:
                actual_has_revenue += 1
            
            # 불일치 확인
            is_mismatch = (reported_has_revenue != actual_has_revenue_data)
            if is_mismatch:
                mismatch_count += 1
                print(f"  ⚠️  불일치: {name} ({ticker}) - 리포트: {reported_has_revenue}, 실제: {actual_has_revenue_data}", flush=True)
            
            verified_data.append({
                'ticker': ticker,
                'name': name,
                'market_cap': company.get('market_cap'),
                'hold_reason': company.get('hold_reason'),
                'reported_has_revenue': reported_has_revenue,
                'actual_has_revenue': actual_has_revenue_data,
                'is_mismatch': is_mismatch,
                'revenue_by_segment': detail.revenue_by_segment if detail else None
            })
        
        # 결과 리포트
        print("\n" + "=" * 80, flush=True)
        print("검증 결과", flush=True)
        print("=" * 80, flush=True)
        
        print(f"\n[매출 데이터 보유율]", flush=True)
        print(f"  리포트 기준 (has_revenue_data=false): {len(top200_hold) - sum(1 for c in top200_hold if c.get('has_revenue_data', False))}개", flush=True)
        print(f"  실제 DB 기준: {len(top200_hold) - actual_has_revenue}개 (매출 데이터 없음)", flush=True)
        print(f"  실제 매출 데이터 보유: {actual_has_revenue}개", flush=True)
        print(f"  불일치: {mismatch_count}개", flush=True)
        
        if mismatch_count > 0:
            print(f"\n⚠️  불일치 발견: {mismatch_count}개", flush=True)
            print(f"  → 리포트의 has_revenue_data와 실제 DB 데이터가 다릅니다.", flush=True)
            print(f"  → 재수집 스크립트의 필터링 로직을 확인하세요.", flush=True)
        else:
            print(f"\n✅ 불일치 없음: 리포트와 실제 DB 데이터가 일치합니다.", flush=True)
        
        # 실제 매출 데이터 보유 기업 목록
        if actual_has_revenue > 0:
            print(f"\n[실제 매출 데이터 보유 기업 목록]", flush=True)
            for company in verified_data:
                if company['actual_has_revenue']:
                    print(f"  - {company['name']} ({company['ticker']})", flush=True)
        
        # 리포트 저장
        result = {
            'total_top200_hold': len(top200_hold),
            'reported_no_revenue': len([c for c in top200_hold if not c.get('has_revenue_data', False)]),
            'actual_has_revenue': actual_has_revenue,
            'actual_no_revenue': len(top200_hold) - actual_has_revenue,
            'mismatch_count': mismatch_count,
            'verified_data': verified_data
        }
        
        os.makedirs('reports', exist_ok=True)
        output_file = 'reports/hold_revenue_verification.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n✅ 검증 결과 저장: {output_file}", flush=True)
        print("=" * 80, flush=True)
        
        return result
        
    except Exception as e:
        import traceback
        print(f"\n❌ 오류 발생: {e}", flush=True)
        traceback.print_exc()
        raise
    finally:
        db.close()

if __name__ == '__main__':
    verify_hold_revenue_data()

