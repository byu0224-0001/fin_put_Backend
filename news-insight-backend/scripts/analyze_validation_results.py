# -*- coding: utf-8 -*-
"""
검증 결과 분석 및 브리핑 스크립트
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
from app.models.investor_sector import InvestorSector
from app.services.sector_classifier import calculate_revenue_sector_scores

def analyze_misclassification():
    """오분류 후보 분석"""
    db = SessionLocal()
    
    try:
        print("=" * 80, flush=True)
        print("오분류 후보 검토 및 분석", flush=True)
        print("=" * 80, flush=True)
        
        all_details = db.query(CompanyDetail, Stock).join(
            Stock, CompanyDetail.ticker == Stock.ticker
        ).all()
        
        misclassification_candidates = []
        
        print(f"\n[1/3] 데이터 조회 중... (총 {len(all_details)}개 기업)", flush=True)
        
        for idx, (detail, stock) in enumerate(all_details):
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
            
            if (idx + 1) % 500 == 0:
                print(f"  진행: {idx + 1}/{len(all_details)} ({((idx+1)/len(all_details)*100):.1f}%)", flush=True)
        
        print(f"✅ 데이터 처리 완료", flush=True)
        
        # 점수순 정렬
        sorted_misclass = sorted(misclassification_candidates, key=lambda x: x['revenue_score'], reverse=True)
        
        print(f"\n[2/3] 오분류 후보 분석 중...", flush=True)
        print(f"\n총 오분류 후보: {len(sorted_misclass)}개", flush=True)
        print("\n" + "=" * 80, flush=True)
        print("Top 20 오분류 후보 상세", flush=True)
        print("=" * 80, flush=True)
        
        for i, mc in enumerate(sorted_misclass[:20], 1):
            print(f"\n{i}. {mc['name']} ({mc['ticker']})", flush=True)
            print(f"   현재: {mc['current_sector']} (sub: {mc.get('current_sub_sector', 'N/A')}, chain: {mc.get('current_value_chain', 'N/A')})", flush=True)
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
        
        print("\n[3/3] 수정 제안 분석 중...", flush=True)
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
        
        # 결과 저장
        result = {
            'total_candidates': len(sorted_misclass),
            'auto_fix_candidates': [
                {
                    'ticker': mc['ticker'],
                    'name': mc['name'],
                    'current_sector': mc['current_sector'],
                    'revenue_best_sector': mc['revenue_best_sector'],
                    'revenue_score': mc['revenue_score']
                }
                for mc in auto_fix_candidates
            ],
            'manual_review_candidates': [
                {
                    'ticker': mc['ticker'],
                    'name': mc['name'],
                    'current_sector': mc['current_sector'],
                    'revenue_best_sector': mc['revenue_best_sector'],
                    'revenue_score': mc['revenue_score']
                }
                for mc in manual_review
            ],
            'top20_details': sorted_misclass[:20]
        }
        
        os.makedirs('reports', exist_ok=True)
        with open('reports/misclassification_analysis.json', 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)
        
        # 텍스트 리포트도 저장 (파일 읽기 없이 바로 확인 가능)
        report_text_file = 'reports/validation_analysis_report.txt'
        with open(report_text_file, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("검증 결과 분석 리포트\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"총 오분류 후보: {len(sorted_misclass)}개\n\n")
            f.write("=" * 80 + "\n")
            f.write("Top 20 오분류 후보 상세\n")
            f.write("=" * 80 + "\n\n")
            for i, mc in enumerate(sorted_misclass[:20], 1):
                f.write(f"{i}. {mc['name']} ({mc['ticker']})\n")
                f.write(f"   현재: {mc['current_sector']} (sub: {mc.get('current_sub_sector', 'N/A')}, chain: {mc.get('current_value_chain', 'N/A')})\n")
                f.write(f"   매출기반: {mc['revenue_best_sector']} (score: {mc['revenue_score']:.3f})\n")
                f.write(f"   매출비중 Top 3:\n")
                if mc['revenue_by_segment']:
                    sorted_rev = sorted(
                        mc['revenue_by_segment'].items(), 
                        key=lambda x: x[1] if isinstance(x[1], (int, float)) else 0, 
                        reverse=True
                    )[:3]
                    for seg, pct in sorted_rev:
                        f.write(f"     - {seg}: {pct}%\n")
                f.write("\n")
            f.write("=" * 80 + "\n")
            f.write("수정 제안 (자동 재분류 가능)\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"자동 수정 가능 후보: {len(auto_fix_candidates)}개\n")
            for mc in auto_fix_candidates:
                f.write(f"  - {mc['name']} ({mc['ticker']}): {mc['current_sector']} → {mc['revenue_best_sector']}\n")
            f.write("\n" + "=" * 80 + "\n")
            f.write("수동 검토 필요 후보\n")
            f.write("=" * 80 + "\n\n")
            f.write(f"수동 검토 필요: {len(manual_review)}개\n")
            for mc in manual_review:
                f.write(f"  - {mc['name']} ({mc['ticker']}): {mc['current_sector']} → {mc['revenue_best_sector']} (score: {mc['revenue_score']:.3f})\n")
        
        print(f"\n✅ 결과 저장:", flush=True)
        print(f"  - JSON: reports/misclassification_analysis.json", flush=True)
        print(f"  - 텍스트: {report_text_file}", flush=True)
        print("=" * 80, flush=True)
        
    except Exception as e:
        import traceback
        print(f"\n❌ 오류 발생: {e}", flush=True)
        traceback.print_exc()
        raise
    finally:
        db.close()

def load_validation_report():
    """검증 리포트 로드 및 요약"""
    report_file = 'reports/segment_mapping_validation.json'
    
    if not os.path.exists(report_file):
        print(f"⚠️  리포트 파일이 없습니다: {report_file}", flush=True)
        return None
    
    with open(report_file, 'r', encoding='utf-8') as f:
        report = json.load(f)
    
    cov = report.get('coverage', {})
    
    print("\n" + "=" * 80, flush=True)
    print("검증 리포트 요약", flush=True)
    print("=" * 80, flush=True)
    
    print(f"\n[커버리지 지표]", flush=True)
    print(f"  Coverage-A (전체): {cov.get('coverage_a', 0):.1f}%", flush=True)
    print(f"  Coverage-A (실제): {cov.get('coverage_a_actual', 0):.1f}%", flush=True)
    print(f"  Coverage-B (세그먼트 카운트): {cov.get('coverage_b', 0):.1f}%", flush=True)
    print(f"  전체 기업: {cov.get('total_companies', 0)}", flush=True)
    print(f"  매출 비중 있는 기업: {cov.get('companies_with_revenue', 0)}", flush=True)
    print(f"  전체 세그먼트: {cov.get('total_segments', 0)}", flush=True)
    print(f"  매핑된 세그먼트: {cov.get('mapped_segments', 0)}", flush=True)
    
    print(f"\n[미매핑 세그먼트 Top 10]", flush=True)
    unmapped = report.get('unmapped_top100', [])[:10]
    for i, (seg, info) in enumerate(unmapped, 1):
        print(f"  {i:2}. {seg:30} | 빈도: {info['count']:4} | 총 비중: {info['total_pct']:.1f}%", flush=True)
    
    return report

def main():
    try:
        # 검증 리포트 요약
        load_validation_report()
        
        # 오분류 후보 분석
        analyze_misclassification()
        
    except KeyboardInterrupt:
        print("\n\n사용자에 의해 중단되었습니다.", flush=True)
        sys.exit(0)
    except Exception as e:
        import traceback
        print(f"\n\n❌ 치명적 오류: {e}", flush=True)
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main()

