# -*- coding: utf-8 -*-
"""
HOLD 기업 모니터링 리포트/큐 생성 (GPT 피드백: HOLD 기업 관리 방안)

HOLD로 분류된 기업들을 별도로 모니터링할 수 있는 리포트 생성
"""
import sys
import os
import json
from datetime import datetime

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

def generate_hold_companies_report():
    """HOLD 기업 모니터링 리포트 생성"""
    db = SessionLocal()
    
    try:
        print("=" * 80, flush=True)
        print("HOLD 기업 모니터링 리포트 생성", flush=True)
        print("=" * 80, flush=True)
        
        # HOLD 기업 조회
        hold_sectors = db.query(InvestorSector).filter(
            InvestorSector.is_primary == True,
            InvestorSector.confidence.like('HOLD%')
        ).all()
        
        print(f"\nHOLD 기업 총 {len(hold_sectors)}개 발견", flush=True)
        
        hold_companies = []
        hold_reason_stats = {}
        
        for sector in hold_sectors:
            stock = db.query(Stock).filter(Stock.ticker == sector.ticker).first()
            detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == sector.ticker
            ).order_by(CompanyDetail.updated_at.desc()).first()
            
            if not stock:
                continue
            
            # HOLD 사유 코드 추출
            hold_reason = 'HOLD_UNKNOWN'
            if sector.confidence and ':' in sector.confidence:
                hold_reason = sector.confidence.split(':', 1)[1]
            
            hold_reason_stats[hold_reason] = hold_reason_stats.get(hold_reason, 0) + 1
            
            # 분류 메타데이터에서 추가 정보 추출
            boosting_log = sector.boosting_log or {}
            classification_meta = boosting_log.get('classification_meta', {})
            entity_type = classification_meta.get('entity_type', 'UNKNOWN')
            
            # 매출 데이터 유무 확인
            has_revenue_data = bool(detail and detail.revenue_by_segment and isinstance(detail.revenue_by_segment, dict) and len(detail.revenue_by_segment) > 0)
            
            # 우선순위 점수 계산 (높을수록 우선 검토 필요)
            priority_score = 0
            if not has_revenue_data:
                priority_score += 10  # 매출 데이터 없으면 우선순위 높음
            if stock.market_cap and stock.market_cap > 1000000000000:  # 시가총액 1조 이상
                priority_score += 5
            if entity_type and entity_type != 'OPERATING':
                priority_score += 3
            
            hold_companies.append({
                'ticker': sector.ticker,
                'name': stock.stock_name,
                'industry': stock.industry_raw,
                'market_cap': stock.market_cap,
                'hold_reason': hold_reason,
                'entity_type': entity_type,
                'has_revenue_data': has_revenue_data,
                'revenue_by_segment': detail.revenue_by_segment if detail else None,
                'priority_score': priority_score,
                'last_updated': detail.updated_at.isoformat() if detail and detail.updated_at else None,
                'classification_method': sector.classification_method,
                'notes': _generate_hold_notes(hold_reason, has_revenue_data, entity_type)
            })
        
        # 우선순위별 정렬
        hold_companies.sort(key=lambda x: x['priority_score'], reverse=True)
        
        # 섹터별 통계
        sector_stats = {}
        for company in hold_companies:
            sector = company.get('industry', 'UNKNOWN')
            sector_stats[sector] = sector_stats.get(sector, 0) + 1
        
        # 리포트 생성
        report = {
            'generated_at': datetime.now().isoformat(),
            'total_hold_companies': len(hold_companies),
            'hold_reason_stats': hold_reason_stats,
            'sector_stats': sector_stats,
            'priority_queue': hold_companies[:50],  # Top 50 우선순위
            'all_hold_companies': hold_companies,
            'summary': {
                'high_priority_count': len([c for c in hold_companies if c['priority_score'] >= 10]),
                'no_revenue_data_count': len([c for c in hold_companies if not c['has_revenue_data']]),
                'holding_companies_count': len([c for c in hold_companies if c['entity_type'] and c['entity_type'] != 'OPERATING'])
            }
        }
        
        # 콘솔 출력
        print("\n" + "=" * 80, flush=True)
        print("HOLD 기업 모니터링 리포트 요약", flush=True)
        print("=" * 80, flush=True)
        
        print(f"\n[전체 통계]", flush=True)
        print(f"  총 HOLD 기업: {len(hold_companies)}개", flush=True)
        print(f"  고우선순위 (점수 >= 10): {report['summary']['high_priority_count']}개", flush=True)
        print(f"  매출 데이터 없음: {report['summary']['no_revenue_data_count']}개", flush=True)
        print(f"  지주사 성격: {report['summary']['holding_companies_count']}개", flush=True)
        
        print(f"\n[HOLD 사유별 통계]", flush=True)
        for reason, count in sorted(hold_reason_stats.items(), key=lambda x: x[1], reverse=True):
            print(f"  {reason}: {count}개", flush=True)
        
        print(f"\n[섹터별 통계 (Top 10)]", flush=True)
        sorted_sectors = sorted(sector_stats.items(), key=lambda x: x[1], reverse=True)[:10]
        for sector, count in sorted_sectors:
            print(f"  {sector}: {count}개", flush=True)
        
        print(f"\n[우선순위 큐 (Top 10)]", flush=True)
        for i, company in enumerate(hold_companies[:10], 1):
            print(f"  {i}. {company['name']} ({company['ticker']}) - 점수: {company['priority_score']}, 사유: {company['hold_reason']}", flush=True)
            print(f"     → {company['notes']}", flush=True)
        
        # 파일 저장
        os.makedirs('reports', exist_ok=True)
        json_file = 'reports/hold_companies_monitoring.json'
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        # CSV 파일 생성 (우선순위 큐)
        csv_file = 'reports/hold_companies_priority_queue.csv'
        with open(csv_file, 'w', encoding='utf-8-sig') as f:
            f.write('순위,종목코드,회사명,업종,HOLD사유,우선순위점수,매출데이터유무,EntityType,비고\n')
            for i, company in enumerate(hold_companies[:100], 1):
                f.write(f"{i},{company['ticker']},{company['name']},{company['industry'] or 'N/A'},"
                        f"{company['hold_reason']},{company['priority_score']},"
                        f"{'Y' if company['has_revenue_data'] else 'N'},"
                        f"{company['entity_type'] or 'N/A'},{company['notes']}\n")
        
        print(f"\n✅ 리포트 저장 완료:", flush=True)
        print(f"  - JSON: {json_file}", flush=True)
        print(f"  - CSV (우선순위 큐): {csv_file}", flush=True)
        print("=" * 80, flush=True)
        
        return report
        
    except Exception as e:
        import traceback
        print(f"\n❌ 오류 발생: {e}", flush=True)
        traceback.print_exc()
        raise
    finally:
        db.close()

def _generate_hold_notes(hold_reason, has_revenue_data, entity_type):
    """HOLD 기업 비고 생성"""
    notes = []
    
    if hold_reason == 'HOLD_LOW_CONF':
        notes.append('신뢰할 만한 데이터 부족')
    elif hold_reason == 'HOLD_UNMAPPED_REVENUE_HIGH':
        notes.append('매출 데이터 누락')
    elif hold_reason == 'HOLD_LOW_COVERAGE_A':
        notes.append('매핑 커버리지 낮음')
    
    if not has_revenue_data:
        notes.append('매출 데이터 수집 필요')
    
    if entity_type and entity_type != 'OPERATING':
        notes.append(f'Entity Type: {entity_type}')
    
    return ' | '.join(notes) if notes else '추가 검토 필요'

if __name__ == '__main__':
    generate_hold_companies_report()

