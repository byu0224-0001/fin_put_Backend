# -*- coding: utf-8 -*-
"""
MVP 품질 리포트 생성 스크립트 (합격/불합격 지표 6개)

1. Coverage 비교(동일 분모)
2. HOLD 비율
3. 오분류 Top20 이동 결과
4. SEC_MACH 게이트 영향
5. 대표기업 회귀 성공률
6. classification_meta 영속 저장률
"""
import sys
import os
import json
import subprocess
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
from app.services.sector_classifier import (
    SEGMENT_TO_SECTOR_MAP,
    normalize_segment_name,
    is_neutral_segment,
    classify_sector_rule_based,
    SEC_MACH_REQUIRED_KEYWORDS,
    MAJOR_COMPANY_SECTORS
)
from app.services.entity_type_classifier import classify_entity_type

# 오분류 Top 20
MISCLASSIFICATION_TOP20 = [
    {'ticker': '000210', 'name': 'DL', 'current': 'SEC_CONST', 'expected': 'SEC_MACH'},
    {'ticker': '000230', 'name': '일동홀딩스', 'current': 'SEC_HOLDING', 'expected': 'SEC_BIO'},
    {'ticker': '000480', 'name': 'CR홀딩스', 'current': 'SEC_HOLDING', 'expected': 'SEC_MACH'},
    {'ticker': '000650', 'name': '천일고속', 'current': 'SEC_CARD', 'expected': 'SEC_RETAIL'},
    {'ticker': '000680', 'name': 'LS네트웍스', 'current': 'SEC_AUTO', 'expected': 'SEC_FINANCE'},
    {'ticker': '0008Z0', 'name': '에스엔시스', 'current': 'SEC_MACH', 'expected': 'SEC_IT'},
    {'ticker': '001080', 'name': '만호제강', 'current': 'SEC_ELECTRONICS', 'expected': 'SEC_STEEL'},
    {'ticker': '001140', 'name': '국보', 'current': 'SEC_MACH', 'expected': 'SEC_RETAIL'},
    {'ticker': '001250', 'name': 'GS글로벌', 'current': 'SEC_MACH', 'expected': 'SEC_RETAIL'},
    {'ticker': '001540', 'name': '안국약품', 'current': 'SEC_RETAIL', 'expected': 'SEC_BIO'},
    {'ticker': '001620', 'name': '케이비아이동국실업', 'current': 'SEC_AUTO', 'expected': 'SEC_MACH'},
    {'ticker': '001770', 'name': 'SHD', 'current': 'SEC_STEEL', 'expected': 'SEC_MACH'},
    {'ticker': '001810', 'name': '무림SP', 'current': 'SEC_TIRE', 'expected': 'SEC_MACH'},
    {'ticker': '002350', 'name': '넥센타이어', 'current': 'SEC_TIRE', 'expected': 'SEC_AUTO'},
    {'ticker': '002360', 'name': 'SH에너지화학', 'current': 'SEC_CHEM', 'expected': 'SEC_MACH'},
    {'ticker': '002620', 'name': '제일파마홀딩스', 'current': 'SEC_HOLDING', 'expected': 'SEC_BIO'},
    {'ticker': '002870', 'name': '신풍', 'current': 'SEC_AUTO', 'expected': 'SEC_RETAIL'},
    {'ticker': '002900', 'name': 'TYM', 'current': 'SEC_TELECOM', 'expected': 'SEC_MACH'},
    {'ticker': '003030', 'name': '세아제강지주', 'current': 'SEC_HOLDING', 'expected': 'SEC_STEEL'},
    {'ticker': '003280', 'name': '흥아해운', 'current': 'SEC_MACH', 'expected': 'SEC_SHIP'},
]

# 대표기업 (대기업) 목록
MAJOR_COMPANIES = [
    {'ticker': '005930', 'name': '삼성전자', 'expected_sector': 'SEC_SEMI'},
    {'ticker': '000660', 'name': 'SK하이닉스', 'expected_sector': 'SEC_SEMI'},
    {'ticker': '005380', 'name': '현대자동차', 'expected_sector': 'SEC_AUTO'},
    {'ticker': '000270', 'name': '기아', 'expected_sector': 'SEC_AUTO'},
    {'ticker': '373220', 'name': 'LG에너지솔루션', 'expected_sector': 'SEC_BATTERY'},
    {'ticker': '006280', 'name': '삼성바이오로직스', 'expected_sector': 'SEC_BIO'},
]

def measure_coverage_comparison(db):
    """1. Coverage 비교(동일 분모)"""
    print("\n[1/6] Coverage 비교 측정 중...", flush=True)
    
    all_details = db.query(CompanyDetail, Stock).join(
        Stock, CompanyDetail.ticker == Stock.ticker
    ).all()
    
    stats_before = {
        'total_segments': 0,
        'mapped_segments': 0,
        'total_revenue_pct': 0.0,
        'mapped_revenue_pct': 0.0,
        'neutral_segments': 0,
        'neutral_revenue_pct': 0.0
    }
    
    stats_after = {
        'total_segments': 0,
        'mapped_segments': 0,
        'total_revenue_pct': 0.0,
        'mapped_revenue_pct': 0.0
    }
    
    companies_with_revenue = 0
    
    for detail, stock in all_details:
        if not detail.revenue_by_segment or not isinstance(detail.revenue_by_segment, dict):
            continue
        
        companies_with_revenue += 1
        
        # Before: Neutral 포함
        for segment, pct in detail.revenue_by_segment.items():
            if not isinstance(pct, (int, float)) or pct <= 0:
                continue
            
            stats_before['total_segments'] += 1
            stats_before['total_revenue_pct'] += pct
            
            if is_neutral_segment(segment):
                stats_before['neutral_segments'] += 1
                stats_before['neutral_revenue_pct'] += pct
            
            normalized = normalize_segment_name(segment)
            matched = any(kw in normalized or kw in str(segment).lower() 
                         for kw in SEGMENT_TO_SECTOR_MAP.keys())
            
            if matched:
                stats_before['mapped_segments'] += 1
                stats_before['mapped_revenue_pct'] += pct
        
        # After: Neutral 제외
        for segment, pct in detail.revenue_by_segment.items():
            if not isinstance(pct, (int, float)) or pct <= 0:
                continue
            
            if is_neutral_segment(segment):
                continue
            
            stats_after['total_segments'] += 1
            stats_after['total_revenue_pct'] += pct
            
            normalized = normalize_segment_name(segment)
            matched = any(kw in normalized or kw in str(segment).lower() 
                         for kw in SEGMENT_TO_SECTOR_MAP.keys())
            
            if matched:
                stats_after['mapped_segments'] += 1
                stats_after['mapped_revenue_pct'] += pct
    
    coverage_before_a = (stats_before['mapped_revenue_pct'] / stats_before['total_revenue_pct'] * 100) if stats_before['total_revenue_pct'] > 0 else 0
    coverage_after_a_same_denom = (stats_after['mapped_revenue_pct'] / stats_before['total_revenue_pct'] * 100) if stats_before['total_revenue_pct'] > 0 else 0
    improvement = coverage_after_a_same_denom - coverage_before_a
    
    return {
        'before_coverage_a': coverage_before_a,
        'after_coverage_a_same_denom': coverage_after_a_same_denom,
        'improvement_pct': improvement,
        'companies_with_revenue': companies_with_revenue,
        'neutral_segments': stats_before['neutral_segments'],
        'neutral_revenue_pct': stats_before['neutral_revenue_pct']
    }


def measure_hold_ratio(db):
    """2. HOLD 비율"""
    print("\n[2/6] HOLD 비율 측정 중...", flush=True)
    
    all_sectors = db.query(InvestorSector).filter(
        InvestorSector.is_primary == True
    ).all()
    
    total_companies = len(all_sectors)
    hold_companies = [s for s in all_sectors if s.confidence and (s.confidence == 'HOLD' or s.confidence.startswith('HOLD:'))]
    hold_count = len(hold_companies)
    hold_ratio = (hold_count / total_companies * 100) if total_companies > 0 else 0
    
    # HOLD 사유 코드별 통계
    hold_reason_stats = {}
    for sector in hold_companies:
        if sector.confidence and ':' in sector.confidence:
            reason_code = sector.confidence.split(':', 1)[1]
            hold_reason_stats[reason_code] = hold_reason_stats.get(reason_code, 0) + 1
        else:
            hold_reason_stats['HOLD_UNKNOWN'] = hold_reason_stats.get('HOLD_UNKNOWN', 0) + 1
    
    # 섹터별 HOLD 비율
    sector_hold_ratio = {}
    for sector in all_sectors:
        major = sector.major_sector
        if major not in sector_hold_ratio:
            sector_hold_ratio[major] = {'total': 0, 'hold': 0}
        sector_hold_ratio[major]['total'] += 1
        if sector.confidence and (sector.confidence == 'HOLD' or sector.confidence.startswith('HOLD:')):
            sector_hold_ratio[major]['hold'] += 1
    
    sector_hold_pct = {
        sector: (stats['hold'] / stats['total'] * 100) if stats['total'] > 0 else 0
        for sector, stats in sector_hold_ratio.items()
    }
    
    # HOLD 폭증 경고 (30% 이상)
    hold_warning = hold_ratio >= 30
    
    return {
        'total_companies': total_companies,
        'hold_count': hold_count,
        'hold_ratio': hold_ratio,
        'hold_warning': hold_warning,
        'hold_reason_stats': hold_reason_stats,  # 🆕 HOLD 사유 코드별 통계
        'sector_hold_ratio': sector_hold_pct
    }


def measure_top20_movement(db):
    """3. 오분류 Top20 이동 결과"""
    print("\n[3/6] 오분류 Top20 이동 결과 측정 중...", flush=True)
    
    movement_results = {
        'FIXED': [],
        'IMPROVED': [],
        'UNCHANGED': [],
        'WORSENED': [],
        'HOLD_SENT': []
    }
    
    for case in MISCLASSIFICATION_TOP20:
        ticker = case['ticker']
        name = case['name']
        current_sector = case['current']
        expected_sector = case['expected']
        
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
        
        # 상태 판정
        if new_conf and (new_conf == 'HOLD' or new_conf.startswith('HOLD:')):
            status = 'HOLD_SENT'
        elif new_sector == expected_sector:
            status = 'FIXED'
        elif new_sector and new_sector != current_sector:
            # 개선되었으나 예상과 다름
            status = 'IMPROVED'
        elif new_sector == current_sector:
            status = 'UNCHANGED'
        elif new_sector and new_sector not in [current_sector, expected_sector]:
            # 악화: 예상도 현재도 아닌 다른 섹터로 이동
            status = 'WORSENED'
        else:
            status = 'UNCHANGED'
        
        movement_results[status].append({
            'ticker': ticker,
            'name': name,
            'before': current_sector,
            'after': new_sector,
            'expected': expected_sector,
            'confidence': new_conf
        })
    
    total = sum(len(v) for v in movement_results.values())
    fixed_hold_ratio = ((len(movement_results['FIXED']) + len(movement_results['HOLD_SENT'])) / total * 100) if total > 0 else 0
    
    return {
        'movement_results': {k: len(v) for k, v in movement_results.items()},
        'movement_details': movement_results,
        'fixed_hold_ratio': fixed_hold_ratio,
        'has_worsened': len(movement_results['WORSENED']) > 0
    }


def measure_sec_mach_gate_impact(db):
    """4. SEC_MACH 게이트 영향"""
    print("\n[4/6] SEC_MACH 게이트 영향 측정 중...", flush=True)
    
    # Before: 기존 로직으로 SEC_MACH 분류된 기업 (시뮬레이션)
    # After: 현재 로직으로 SEC_MACH 분류된 기업
    sec_mach_sectors = db.query(InvestorSector).filter(
        InvestorSector.major_sector == 'SEC_MACH',
        InvestorSector.is_primary == True
    ).all()
    
    sec_mach_count_after = len(sec_mach_sectors)
    
    # 게이트 통과율 (SEC_MACH로 분류된 기업 중 기계 고유 단서 충족률)
    gate_pass_count = 0
    gate_fail_count = 0
    
    for sector in sec_mach_sectors:
        detail = db.query(CompanyDetail).filter(
            CompanyDetail.ticker == sector.ticker
        ).order_by(CompanyDetail.updated_at.desc()).first()
        stock = db.query(Stock).filter(Stock.ticker == sector.ticker).first()
        
        if not detail or not stock:
            continue
        
        # 기계 고유 단서 확인
        text_parts = []
        if detail.biz_summary:
            text_parts.append(detail.biz_summary.lower())
        if detail.products:
            text_parts.append(' '.join([str(p) for p in detail.products]).lower())
        if detail.keywords:
            text_parts.append(' '.join([str(k) for k in detail.keywords]).lower())
        if stock.stock_name:
            text_parts.append(stock.stock_name.lower())
        
        combined_text = ' '.join(text_parts)
        has_mach_keyword = any(kw.lower() in combined_text for kw in SEC_MACH_REQUIRED_KEYWORDS)
        
        if has_mach_keyword:
            gate_pass_count += 1
        else:
            gate_fail_count += 1
    
    gate_pass_rate = (gate_pass_count / sec_mach_count_after * 100) if sec_mach_count_after > 0 else 0
    
    return {
        'sec_mach_count_after': sec_mach_count_after,
        'gate_pass_count': gate_pass_count,
        'gate_fail_count': gate_fail_count,
        'gate_pass_rate': gate_pass_rate
    }


def measure_major_companies_regression(db):
    """5. 대표기업 회귀 성공률"""
    print("\n[5/6] 대표기업 회귀 성공률 측정 중...", flush=True)
    
    regression_results = []
    success_count = 0
    
    for company in MAJOR_COMPANIES:
        ticker = company['ticker']
        name = company['name']
        expected_sector = company['expected_sector']
        
        stock = db.query(Stock).filter(Stock.ticker == ticker).first()
        sector = db.query(InvestorSector).filter(
            InvestorSector.ticker == ticker,
            InvestorSector.is_primary == True
        ).first()
        detail = db.query(CompanyDetail).filter(
            CompanyDetail.ticker == ticker
        ).order_by(CompanyDetail.updated_at.desc()).first()
        
        if not stock or not detail:
            regression_results.append({
                'ticker': ticker,
                'name': name,
                'expected': expected_sector,
                'actual': None,
                'success': False,
                'reason': '데이터 없음'
            })
            continue
        
        # 재분류
        new_sector, new_sub, new_vc, new_conf, _ = classify_sector_rule_based(
            detail, stock.stock_name
        )
        
        success = (new_sector == expected_sector)
        if success:
            success_count += 1
        
        regression_results.append({
            'ticker': ticker,
            'name': name,
            'expected': expected_sector,
            'actual': new_sector,
            'confidence': new_conf,
            'success': success
        })
    
    success_rate = (success_count / len(MAJOR_COMPANIES) * 100) if MAJOR_COMPANIES else 0
    
    return {
        'total_companies': len(MAJOR_COMPANIES),
        'success_count': success_count,
        'success_rate': success_rate,
        'details': regression_results
    }


def measure_classification_meta_storage(db):
    """6. classification_meta 영속 저장률"""
    print("\n[6/6] classification_meta 영속 저장률 측정 중...", flush=True)
    
    # 지주회사 후보
    holdings = db.query(Stock).filter(
        Stock.stock_name.like('%홀딩스%') | 
        Stock.stock_name.like('%지주%') |
        Stock.stock_name.like('%Holdings%')
    ).all()
    
    total_holdings = len(holdings)
    classified_count = 0
    stored_count = 0
    null_count = 0
    missing_count = 0
    
    for stock in holdings:
        detail = db.query(CompanyDetail).filter(
            CompanyDetail.ticker == stock.ticker
        ).order_by(CompanyDetail.updated_at.desc()).first()
        
        sector = db.query(InvestorSector).filter(
            InvestorSector.ticker == stock.ticker,
            InvestorSector.is_primary == True
        ).first()
        
        if not sector:
            continue
        
        # Entity Type 분류 시도
        entity_type, entity_conf, entity_meta = classify_entity_type(stock, detail)
        
        if entity_type and entity_type != 'OPERATING':
            classified_count += 1
            
            # 저장 여부 확인
            boosting_log = sector.boosting_log
            if boosting_log and isinstance(boosting_log, dict):
                classification_meta = boosting_log.get('classification_meta')
                if classification_meta and isinstance(classification_meta, dict):
                    if classification_meta.get('entity_type'):
                        stored_count += 1
                    else:
                        null_count += 1
                else:
                    missing_count += 1
            else:
                missing_count += 1
    
    storage_rate = (stored_count / classified_count * 100) if classified_count > 0 else 0
    
    return {
        'total_holdings': total_holdings,
        'classified_count': classified_count,
        'stored_count': stored_count,
        'null_count': null_count,
        'missing_count': missing_count,
        'storage_rate': storage_rate
    }


def get_git_sha() -> str:
    """현재 Git 커밋 해시 가져오기"""
    try:
        result = subprocess.run(
            ['git', 'rev-parse', '--short', 'HEAD'],
            capture_output=True,
            text=True,
            cwd=os.path.dirname(os.path.abspath(__file__))
        )
        if result.returncode == 0:
            return result.stdout.strip()
        else:
            return "UNKNOWN"
    except Exception:
        return "UNKNOWN"


def generate_mvp_quality_report():
    """MVP 품질 리포트 생성"""
    db = SessionLocal()
    
    try:
        # 🆕 P0-C: Git SHA 가져오기
        git_sha = get_git_sha()
        
        print("=" * 80, flush=True)
        print("MVP 품질 리포트 생성", flush=True)
        print("=" * 80, flush=True)
        print(f"생성 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
        print(f"Git SHA: {git_sha}", flush=True)
        
        # 6개 지표 측정
        coverage_result = measure_coverage_comparison(db)
        hold_result = measure_hold_ratio(db)
        movement_result = measure_top20_movement(db)
        sec_mach_result = measure_sec_mach_gate_impact(db)
        regression_result = measure_major_companies_regression(db)
        storage_result = measure_classification_meta_storage(db)
        
        # MVP 합격 기준 체크
        mvp_criteria = {
            'top20_fixed_hold_ratio': {
                'value': movement_result['fixed_hold_ratio'],
                'threshold': 70.0,
                'passed': movement_result['fixed_hold_ratio'] >= 70.0,
                'worsened': movement_result['has_worsened']
            },
            'hold_ratio': {
                'value': hold_result['hold_ratio'],
                'threshold': 25.0,
                'passed': hold_result['hold_ratio'] <= 25.0,
                'warning': hold_result['hold_ratio'] >= 30.0
            },
            'major_companies_regression': {
                'value': regression_result['success_rate'],
                'threshold': 100.0,
                'passed': regression_result['success_rate'] == 100.0
            },
            'classification_meta_storage': {
                'value': storage_result['storage_rate'],
                'threshold': 95.0,
                'passed': storage_result['storage_rate'] >= 95.0
            }
        }
        
        # 전체 합격 여부
        all_passed = all(
            criteria['passed'] and not criteria.get('worsened', False)
            for criteria in mvp_criteria.values()
        )
        
        # 리포트 생성
        report = {
            'generated_at': datetime.now().isoformat(),
            'git_sha': git_sha,  # 🆕 P0-C: Git 커밋 해시
            'mvp_status': 'PASSED' if all_passed else 'FAILED',
            'indicators': {
                '1_coverage_comparison': coverage_result,
                '2_hold_ratio': hold_result,
                '3_top20_movement': movement_result,
                '4_sec_mach_gate_impact': sec_mach_result,
                '5_major_companies_regression': regression_result,
                '6_classification_meta_storage': storage_result
            },
            'mvp_criteria': mvp_criteria,
            'summary': {
                'coverage_improvement': coverage_result['improvement_pct'],
                'hold_ratio': hold_result['hold_ratio'],
                'top20_fixed_hold_ratio': movement_result['fixed_hold_ratio'],
                'major_companies_success_rate': regression_result['success_rate'],
                'classification_meta_storage_rate': storage_result['storage_rate']
            }
        }
        
        # 콘솔 출력
        print("\n" + "=" * 80, flush=True)
        print("MVP 품질 리포트 요약", flush=True)
        print("=" * 80, flush=True)
        
        print(f"\n[1. Coverage 비교 (동일 분모)]", flush=True)
        print(f"  Before Coverage-A: {coverage_result['before_coverage_a']:.2f}%", flush=True)
        print(f"  After Coverage-A (same denom): {coverage_result['after_coverage_a_same_denom']:.2f}%", flush=True)
        print(f"  개선폭 (Δ): {coverage_result['improvement_pct']:+.2f}%p", flush=True)
        
        print(f"\n[2. HOLD 비율]", flush=True)
        print(f"  전체 HOLD 비율: {hold_result['hold_ratio']:.2f}% ({hold_result['hold_count']}/{hold_result['total_companies']})", flush=True)
        if hold_result['hold_warning']:
            print(f"  ⚠️  경고: HOLD 비율이 30% 이상입니다!", flush=True)
        if hold_result.get('hold_reason_stats'):
            print(f"  HOLD 사유 코드별 통계:", flush=True)
            for reason, count in sorted(hold_result['hold_reason_stats'].items(), key=lambda x: x[1], reverse=True):
                print(f"    {reason}: {count}개", flush=True)
        
        print(f"\n[3. 오분류 Top20 이동 결과]", flush=True)
        for status, count in movement_result['movement_results'].items():
            print(f"  {status}: {count}개", flush=True)
        print(f"  FIXED + HOLD_SENT 비율: {movement_result['fixed_hold_ratio']:.1f}%", flush=True)
        if movement_result['has_worsened']:
            print(f"  ❌ 경고: WORSENED가 발생했습니다! 원인 분석 필요", flush=True)
        
        print(f"\n[4. SEC_MACH 게이트 영향]", flush=True)
        print(f"  SEC_MACH 분류 기업 수: {sec_mach_result['sec_mach_count_after']}개", flush=True)
        print(f"  게이트 통과율: {sec_mach_result['gate_pass_rate']:.1f}% ({sec_mach_result['gate_pass_count']}/{sec_mach_result['sec_mach_count_after']})", flush=True)
        
        print(f"\n[5. 대표기업 회귀 성공률]", flush=True)
        print(f"  성공률: {regression_result['success_rate']:.1f}% ({regression_result['success_count']}/{regression_result['total_companies']})", flush=True)
        for detail in regression_result['details']:
            status = "✅" if detail['success'] else "❌"
            print(f"  {status} {detail['name']}: {detail['expected']} → {detail['actual']}", flush=True)
        
        print(f"\n[6. classification_meta 영속 저장률]", flush=True)
        print(f"  저장률: {storage_result['storage_rate']:.1f}% ({storage_result['stored_count']}/{storage_result['classified_count']})", flush=True)
        print(f"  분류된 지주사: {storage_result['classified_count']}개", flush=True)
        print(f"  저장 누락: {storage_result['missing_count']}개", flush=True)
        
        print("\n" + "=" * 80, flush=True)
        print("MVP 합격 기준 체크", flush=True)
        print("=" * 80, flush=True)
        
        for name, criteria in mvp_criteria.items():
            status = "✅" if criteria['passed'] and not criteria.get('worsened', False) else "❌"
            print(f"{status} {name}: {criteria['value']:.1f}% (기준: {criteria['threshold']}%)", flush=True)
            if criteria.get('warning'):
                print(f"  ⚠️  경고: {name} 기준 초과", flush=True)
            if criteria.get('worsened'):
                print(f"  ❌ WORSENED 발생", flush=True)
        
        print(f"\n{'=' * 80}", flush=True)
        print(f"최종 판정: {'✅ MVP 합격' if all_passed else '❌ MVP 불합격'}", flush=True)
        print(f"{'=' * 80}", flush=True)
        
        # 파일 저장
        os.makedirs('reports', exist_ok=True)
        # 🆕 P0-C: Git SHA 포함 파일명
        date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        json_file = f'reports/mvp_quality_report_{date_str}_{git_sha}.json'
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        # Markdown 리포트 생성 (Git SHA 포함)
        md_file = f'reports/mvp_quality_report_{date_str}_{git_sha}.md'
        with open(md_file, 'w', encoding='utf-8') as f:
            f.write("# MVP 품질 리포트\n\n")
            f.write(f"**생성 일시**: {report['generated_at']}\n\n")
            f.write(f"**Git SHA**: {git_sha}\n\n")
            f.write(f"**MVP 상태**: {'✅ 합격' if all_passed else '❌ 불합격'}\n\n")
            f.write("---\n\n")
            
            f.write("## 1. Coverage 비교 (동일 분모)\n\n")
            f.write(f"- Before Coverage-A: {coverage_result['before_coverage_a']:.2f}%\n")
            f.write(f"- After Coverage-A (same denom): {coverage_result['after_coverage_a_same_denom']:.2f}%\n")
            f.write(f"- 개선폭 (Δ): {coverage_result['improvement_pct']:+.2f}%p\n\n")
            
            f.write("## 2. HOLD 비율\n\n")
            f.write(f"- 전체 HOLD 비율: {hold_result['hold_ratio']:.2f}% ({hold_result['hold_count']}/{hold_result['total_companies']})\n")
            if hold_result['hold_warning']:
                f.write("- ⚠️ 경고: HOLD 비율이 30% 이상\n\n")
            
            f.write("## 3. 오분류 Top20 이동 결과\n\n")
            for status, count in movement_result['movement_results'].items():
                f.write(f"- {status}: {count}개\n")
            f.write(f"- FIXED + HOLD_SENT 비율: {movement_result['fixed_hold_ratio']:.1f}%\n\n")
            
            f.write("## 4. SEC_MACH 게이트 영향\n\n")
            f.write(f"- SEC_MACH 분류 기업 수: {sec_mach_result['sec_mach_count_after']}개\n")
            f.write(f"- 게이트 통과율: {sec_mach_result['gate_pass_rate']:.1f}%\n\n")
            
            f.write("## 5. 대표기업 회귀 성공률\n\n")
            f.write(f"- 성공률: {regression_result['success_rate']:.1f}% ({regression_result['success_count']}/{regression_result['total_companies']})\n\n")
            
            f.write("## 6. classification_meta 영속 저장률\n\n")
            f.write(f"- 저장률: {storage_result['storage_rate']:.1f}% ({storage_result['stored_count']}/{storage_result['classified_count']})\n\n")
            
            f.write("## MVP 합격 기준\n\n")
            for name, criteria in mvp_criteria.items():
                status = "✅" if criteria['passed'] and not criteria.get('worsened', False) else "❌"
                f.write(f"- {status} {name}: {criteria['value']:.1f}% (기준: {criteria['threshold']}%)\n")
        
        print(f"\n✅ 리포트 저장 완료:", flush=True)
        print(f"  - JSON: {json_file}", flush=True)
        print(f"  - Markdown: {md_file}", flush=True)
        print("=" * 80, flush=True)
        
    except Exception as e:
        import traceback
        print(f"\n❌ 오류 발생: {e}", flush=True)
        traceback.print_exc()
        raise
    finally:
        db.close()

if __name__ == '__main__':
    generate_mvp_quality_report()

