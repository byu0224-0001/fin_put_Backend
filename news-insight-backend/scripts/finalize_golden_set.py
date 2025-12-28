# -*- coding: utf-8 -*-
"""
회귀 테스트 골든셋 확정 스크립트 (GPT 피드백: 회귀 테스트 골든셋 확정)

regression_test_bundle.json에서 절대 틀리면 안 되는 'Top 50' 기업 확정
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
from app.models.stock import Stock
from app.models.investor_sector import InvestorSector
from app.services.sector_classifier import classify_sector_rule_based, MAJOR_COMPANY_SECTORS
from app.models.company_detail import CompanyDetail

def finalize_golden_set():
    """회귀 테스트 골든셋 확정"""
    db = SessionLocal()
    
    try:
        print("=" * 80, flush=True)
        print("회귀 테스트 골든셋 확정", flush=True)
        print("=" * 80, flush=True)
        
        # regression_test_bundle.json 로드
        bundle_file = 'reports/regression_test_bundle.json'
        if not os.path.exists(bundle_file):
            print(f"\n❌ {bundle_file} 파일이 없습니다.", flush=True)
            print("먼저 python scripts/regression_test_bundle.py를 실행하세요.", flush=True)
            return
        
        with open(bundle_file, 'r', encoding='utf-8') as f:
            bundle = json.load(f)
        
        print(f"\n회귀 테스트 번들 로드 완료: {len(bundle.get('test_cases', []))}개 케이스", flush=True)
        
        # 골든셋 선정 기준
        golden_set = []
        
        # 1. 대기업 (MAJOR_COMPANY_SECTORS)
        print("\n[1/4] 대기업 선정 중...", flush=True)
        for company_name, (sector, sub, vc, conf) in MAJOR_COMPANY_SECTORS.items():
            stock = db.query(Stock).filter(Stock.stock_name.like(f'%{company_name}%')).first()
            if stock:
                detail = db.query(CompanyDetail).filter(
                    CompanyDetail.ticker == stock.ticker
                ).order_by(CompanyDetail.updated_at.desc()).first()
                
                if detail:
                    golden_set.append({
                        'ticker': stock.ticker,
                        'name': stock.stock_name,
                        'category': 'MAJOR_COMPANY',
                        'expected_sector': sector,
                        'expected_sub_sector': sub,
                        'expected_value_chain': vc,
                        'priority': 'CRITICAL',  # 절대 틀리면 안 됨
                        'reason': f'대기업 - {company_name}'
                    })
        
        print(f"  선정: {len([c for c in golden_set if c['category'] == 'MAJOR_COMPANY'])}개", flush=True)
        
        # 2. 오분류 Top 20 (FIXED/IMPROVED 예상)
        print("\n[2/4] 오분류 Top 20 선정 중...", flush=True)
        misclassification_cases = [c for c in bundle.get('test_cases', []) if c.get('category') == 'MISCLASSIFICATION']
        
        for case in misclassification_cases[:20]:
            if case.get('expected_sector'):  # 예상 섹터가 설정된 경우만
                golden_set.append({
                    'ticker': case['ticker'],
                    'name': case['name'],
                    'category': 'MISCLASSIFICATION',
                    'expected_sector': case['expected_sector'],
                    'current_sector': case.get('current_sector'),
                    'priority': 'HIGH',
                    'reason': '오분류 후보 - 수정 필요'
                })
        
        print(f"  선정: {len([c for c in golden_set if c['category'] == 'MISCLASSIFICATION'])}개", flush=True)
        
        # 3. 섹터별 대표기업 (각 섹터당 2-3개)
        print("\n[3/4] 섹터별 대표기업 선정 중...", flush=True)
        sector_representatives = {}
        
        for case in bundle.get('test_cases', []):
            sector = case.get('current_sector')
            if sector and sector not in sector_representatives:
                sector_representatives[sector] = []
            
            if sector and len(sector_representatives[sector]) < 3:
                stock = db.query(Stock).filter(Stock.ticker == case['ticker']).first()
                if stock and stock.market_cap and stock.market_cap > 100000000000:  # 시가총액 1000억 이상
                    sector_representatives[sector].append({
                        'ticker': case['ticker'],
                        'name': case['name'],
                        'category': 'SECTOR_REPRESENTATIVE',
                        'expected_sector': sector,
                        'priority': 'MEDIUM',
                        'reason': f'{sector} 섹터 대표기업'
                    })
        
        for sector, companies in sector_representatives.items():
            golden_set.extend(companies[:2])  # 섹터당 최대 2개
        
        print(f"  선정: {len([c for c in golden_set if c['category'] == 'SECTOR_REPRESENTATIVE'])}개", flush=True)
        
        # 4. 특수 구조 기업 (지주사, SPAC, REIT)
        print("\n[4/4] 특수 구조 기업 선정 중...", flush=True)
        special_cases = [c for c in bundle.get('test_cases', []) 
                        if c.get('category') in ['HOLDING', 'SPAC', 'REIT']]
        
        for case in special_cases[:10]:  # 최대 10개
            golden_set.append({
                'ticker': case['ticker'],
                'name': case['name'],
                'category': case.get('category', 'SPECIAL'),
                'expected_sector': case.get('current_sector'),
                'priority': 'MEDIUM',
                'reason': f'특수 구조 - {case.get("category")}'
            })
        
        print(f"  선정: {len([c for c in golden_set if c['category'] in ['HOLDING', 'SPAC', 'REIT']])}개", flush=True)
        
        # 중복 제거 및 정렬
        seen = {}
        unique_golden_set = []
        for case in golden_set:
            ticker = case['ticker']
            if ticker not in seen:
                seen[ticker] = case
                unique_golden_set.append(case)
        
        # 우선순위별 정렬
        priority_order = {'CRITICAL': 0, 'HIGH': 1, 'MEDIUM': 2, 'LOW': 3}
        unique_golden_set.sort(key=lambda x: (priority_order.get(x.get('priority', 'LOW'), 3), x['ticker']))
        
        # Top 50 선정
        final_golden_set = unique_golden_set[:50]
        
        # 검증: 현재 분류와 예상 분류 비교
        print("\n[검증] 골든셋 검증 중...", flush=True)
        validation_results = []
        
        for case in final_golden_set:
            ticker = case['ticker']
            expected_sector = case.get('expected_sector')
            
            if not expected_sector:
                continue
            
            stock = db.query(Stock).filter(Stock.ticker == ticker).first()
            sector = db.query(InvestorSector).filter(
                InvestorSector.ticker == ticker,
                InvestorSector.is_primary == True
            ).first()
            detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == ticker
            ).order_by(CompanyDetail.updated_at.desc()).first()
            
            if not stock or not detail:
                continue
            
            # 재분류
            new_sector, new_sub, new_vc, new_conf, _ = classify_sector_rule_based(
                detail, stock.stock_name
            )
            
            is_correct = (new_sector == expected_sector)
            
            # 🆕 상태 판정
            status = None
            if is_correct:
                status = 'CORRECT'
            elif new_conf and (new_conf == 'HOLD' or new_conf.startswith('HOLD:')):
                status = 'HOLD'
            elif new_sector and new_sector != expected_sector:
                # 예상과 다른 섹터로 분류됨
                status = 'WORSENED'  # 골든셋에서는 WORSENED로 간주
            else:
                status = 'UNKNOWN'
            
            validation_results.append({
                'ticker': ticker,
                'name': case['name'],
                'category': case['category'],
                'expected': expected_sector,
                'actual': new_sector,
                'correct': is_correct,
                'status': status,  # 🆕 상태 추가
                'confidence': new_conf,
                'priority': case.get('priority')
            })
        
        correct_count = len([r for r in validation_results if r['correct']])
        accuracy = (correct_count / len(validation_results) * 100) if validation_results else 0
        
        # 🆕 골든셋 경보 시스템
        worsened_count = len([r for r in validation_results if not r['correct'] and r.get('status') == 'WORSENED'])
        improved_count = len([r for r in validation_results if not r['correct'] and r.get('status') == 'IMPROVED'])
        
        # WORSENED 확인
        has_worsened = worsened_count > 0
        golden_set_status = 'PASS' if not has_worsened and accuracy == 100 else 'FAIL'
        
        # 결과 저장
        golden_set_result = {
            'version': '1.0',
            'created_at': datetime.now().isoformat(),
            'total_cases': len(final_golden_set),
            'validation_accuracy': accuracy,
            'golden_set': final_golden_set,
            'validation_results': validation_results,
            'statistics': {
                'by_category': {},
                'by_priority': {}
            }
        }
        
        for case in final_golden_set:
            cat = case['category']
            golden_set_result['statistics']['by_category'][cat] = golden_set_result['statistics']['by_category'].get(cat, 0) + 1
            
            priority = case.get('priority', 'LOW')
            golden_set_result['statistics']['by_priority'][priority] = golden_set_result['statistics']['by_priority'].get(priority, 0) + 1
        
        # 콘솔 출력
        print("\n" + "=" * 80, flush=True)
        print("골든셋 확정 결과", flush=True)
        print("=" * 80, flush=True)
        
        print(f"\n[골든셋 통계]", flush=True)
        print(f"  총 케이스: {len(final_golden_set)}개", flush=True)
        print(f"  검증 정확도: {accuracy:.1f}% ({correct_count}/{len(validation_results)})", flush=True)
        print(f"  상태: {'✅ PASS' if golden_set_status == 'PASS' else '❌ FAIL'}", flush=True)
        
        # 🆕 골든셋 경보
        if has_worsened:
            print(f"\n  ⚠️  경보: WORSENED {worsened_count}개 발생! --apply 금지", flush=True)
        elif improved_count > 0:
            print(f"\n  ⚠️  주의: IMPROVED {improved_count}개 (검토 필요)", flush=True)
        else:
            print(f"\n  ✅ 골든셋 통과", flush=True)
        
        print(f"\n[카테고리별]", flush=True)
        for cat, count in sorted(golden_set_result['statistics']['by_category'].items(), key=lambda x: x[1], reverse=True):
            print(f"  {cat}: {count}개", flush=True)
        
        print(f"\n[우선순위별]", flush=True)
        for priority, count in sorted(golden_set_result['statistics']['by_priority'].items(), key=lambda x: x[1], reverse=True):
            print(f"  {priority}: {count}개", flush=True)
        
        if accuracy < 100:
            print(f"\n[❌ 검증 실패 케이스]", flush=True)
            for result in validation_results:
                if not result['correct']:
                    print(f"  {result['name']} ({result['ticker']}): 예상 {result['expected']} → 실제 {result['actual']}", flush=True)
        
        # 파일 저장
        os.makedirs('reports', exist_ok=True)
        output_file = 'reports/golden_set.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(golden_set_result, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n✅ 골든셋 저장: {output_file}", flush=True)
        print("=" * 80, flush=True)
        
        return golden_set_result
        
    except Exception as e:
        import traceback
        print(f"\n❌ 오류 발생: {e}", flush=True)
        traceback.print_exc()
        raise
    finally:
        db.close()

if __name__ == '__main__':
    from datetime import datetime
    finalize_golden_set()

