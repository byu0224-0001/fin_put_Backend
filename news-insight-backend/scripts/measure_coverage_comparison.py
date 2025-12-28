# -*- coding: utf-8 -*-
"""
Neutral 제외 전/후 커버리지 비교 측정 (GPT 피드백: 지표의 착시 조심)

같은 기준(분모)으로 비교하여 진짜 매핑 능력 향상 확인
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
from app.services.sector_classifier import (
    SEGMENT_TO_SECTOR_MAP, 
    normalize_segment_name,
    is_neutral_segment,
    NEUTRAL_SEGMENTS
)

def measure_coverage_comparison():
    """Neutral 제외 전/후 커버리지 비교"""
    db = SessionLocal()
    
    try:
        print("=" * 80, flush=True)
        print("Neutral 제외 전/후 커버리지 비교 측정", flush=True)
        print("=" * 80, flush=True)
        
        # 전체 기업 조회
        all_details = db.query(CompanyDetail, Stock).join(
            Stock, CompanyDetail.ticker == Stock.ticker
        ).all()
        
        print(f"\n[1/3] 데이터 조회 중... (총 {len(all_details)}개 기업)", flush=True)
        
        # 통계 초기화
        stats_before = {
            'total_companies': 0,
            'companies_with_revenue': 0,
            'total_segments': 0,
            'mapped_segments': 0,
            'total_revenue_pct': 0.0,
            'mapped_revenue_pct': 0.0,
            'neutral_segments': 0,
            'neutral_revenue_pct': 0.0
        }
        
        stats_after = {
            'total_companies': 0,
            'companies_with_revenue': 0,
            'total_segments': 0,
            'mapped_segments': 0,
            'total_revenue_pct': 0.0,
            'mapped_revenue_pct': 0.0
        }
        
        # Neutral 세그먼트 상세
        neutral_segments_detail = {}
        
        for idx, (detail, stock) in enumerate(all_details):
            if not detail.revenue_by_segment or not isinstance(detail.revenue_by_segment, dict):
                continue
            
            stats_before['companies_with_revenue'] += 1
            stats_after['companies_with_revenue'] += 1
            
            # Before: Neutral 포함
            for segment, pct in detail.revenue_by_segment.items():
                if not isinstance(pct, (int, float)) or pct <= 0:
                    continue
                
                stats_before['total_segments'] += 1
                stats_before['total_revenue_pct'] += pct
                
                # Neutral 판정
                is_neutral = is_neutral_segment(segment)
                if is_neutral:
                    stats_before['neutral_segments'] += 1
                    stats_before['neutral_revenue_pct'] += pct
                    neutral_segments_detail[segment] = neutral_segments_detail.get(segment, 0) + pct
                
                # 매핑 시도
                normalized = normalize_segment_name(segment)
                matched = False
                
                for keyword, sector in SEGMENT_TO_SECTOR_MAP.items():
                    if keyword in normalized or keyword in str(segment).lower():
                        matched = True
                        break
                
                if matched:
                    stats_before['mapped_segments'] += 1
                    stats_before['mapped_revenue_pct'] += pct
            
            # After: Neutral 제외
            for segment, pct in detail.revenue_by_segment.items():
                if not isinstance(pct, (int, float)) or pct <= 0:
                    continue
                
                # Neutral 제외
                if is_neutral_segment(segment):
                    continue
                
                stats_after['total_segments'] += 1
                stats_after['total_revenue_pct'] += pct
                
                # 매핑 시도
                normalized = normalize_segment_name(segment)
                matched = False
                
                for keyword, sector in SEGMENT_TO_SECTOR_MAP.items():
                    if keyword in normalized or keyword in str(segment).lower():
                        matched = True
                        break
                
                if matched:
                    stats_after['mapped_segments'] += 1
                    stats_after['mapped_revenue_pct'] += pct
            
            if (idx + 1) % 500 == 0:
                print(f"  진행: {idx + 1}/{len(all_details)} ({((idx+1)/len(all_details)*100):.1f}%)", flush=True)
        
        stats_before['total_companies'] = len(all_details)
        stats_after['total_companies'] = len(all_details)
        
        # 커버리지 계산
        coverage_before_a = (stats_before['mapped_revenue_pct'] / stats_before['total_revenue_pct'] * 100) if stats_before['total_revenue_pct'] > 0 else 0
        coverage_before_b = (stats_before['mapped_segments'] / stats_before['total_segments'] * 100) if stats_before['total_segments'] > 0 else 0
        
        coverage_after_a = (stats_after['mapped_revenue_pct'] / stats_after['total_revenue_pct'] * 100) if stats_after['total_revenue_pct'] > 0 else 0
        coverage_after_b = (stats_after['mapped_segments'] / stats_after['total_segments'] * 100) if stats_after['total_segments'] > 0 else 0
        
        # 🎯 핵심: 같은 기준(분모)으로 비교
        # Before의 전체 매출을 기준으로 After의 매핑된 매출 비율 계산
        coverage_after_a_same_denom = (stats_after['mapped_revenue_pct'] / stats_before['total_revenue_pct'] * 100) if stats_before['total_revenue_pct'] > 0 else 0
        
        print("\n[2/3] 결과 분석 중...", flush=True)
        print("\n" + "=" * 80, flush=True)
        print("커버리지 비교 (Before: Neutral 포함, After: Neutral 제외)", flush=True)
        print("=" * 80, flush=True)
        
        print(f"\n[Before - Neutral 포함]", flush=True)
        print(f"  Coverage-A (매출 가중): {coverage_before_a:.2f}%", flush=True)
        print(f"  Coverage-B (세그먼트 카운트): {coverage_before_b:.2f}%", flush=True)
        print(f"  전체 세그먼트: {stats_before['total_segments']:,}개", flush=True)
        print(f"  매핑된 세그먼트: {stats_before['mapped_segments']:,}개", flush=True)
        print(f"  전체 매출 비중: {stats_before['total_revenue_pct']:.2f}%", flush=True)
        print(f"  매핑된 매출 비중: {stats_before['mapped_revenue_pct']:.2f}%", flush=True)
        print(f"  Neutral 세그먼트: {stats_before['neutral_segments']:,}개 ({stats_before['neutral_revenue_pct']:.2f}%)", flush=True)
        
        print(f"\n[After - Neutral 제외]", flush=True)
        print(f"  Coverage-A (매출 가중): {coverage_after_a:.2f}%", flush=True)
        print(f"  Coverage-B (세그먼트 카운트): {coverage_after_b:.2f}%", flush=True)
        print(f"  전체 세그먼트: {stats_after['total_segments']:,}개", flush=True)
        print(f"  매핑된 세그먼트: {stats_after['mapped_segments']:,}개", flush=True)
        print(f"  전체 매출 비중: {stats_after['total_revenue_pct']:.2f}%", flush=True)
        print(f"  매핑된 매출 비중: {stats_after['mapped_revenue_pct']:.2f}%", flush=True)
        
        print(f"\n[🎯 핵심 지표: 같은 기준(분모) 비교]", flush=True)
        print(f"  Coverage-A (Before 분모 기준): {coverage_after_a_same_denom:.2f}%", flush=True)
        print(f"  → Before: {coverage_before_a:.2f}% → After: {coverage_after_a_same_denom:.2f}%", flush=True)
        print(f"  → 실제 향상: {coverage_after_a_same_denom - coverage_before_a:.2f}%p", flush=True)
        
        print(f"\n[Neutral 세그먼트 Top 20]", flush=True)
        sorted_neutral = sorted(neutral_segments_detail.items(), key=lambda x: x[1], reverse=True)[:20]
        for i, (seg, pct) in enumerate(sorted_neutral, 1):
            print(f"  {i:2}. {seg:30} | {pct:8.2f}%", flush=True)
        
        # 결과 저장
        result = {
            'before': {
                'coverage_a': coverage_before_a,
                'coverage_b': coverage_before_b,
                'total_segments': stats_before['total_segments'],
                'mapped_segments': stats_before['mapped_segments'],
                'total_revenue_pct': stats_before['total_revenue_pct'],
                'mapped_revenue_pct': stats_before['mapped_revenue_pct'],
                'neutral_segments': stats_before['neutral_segments'],
                'neutral_revenue_pct': stats_before['neutral_revenue_pct']
            },
            'after': {
                'coverage_a': coverage_after_a,
                'coverage_b': coverage_after_b,
                'total_segments': stats_after['total_segments'],
                'mapped_segments': stats_after['mapped_segments'],
                'total_revenue_pct': stats_after['total_revenue_pct'],
                'mapped_revenue_pct': stats_after['mapped_revenue_pct']
            },
            'comparison_same_denom': {
                'coverage_a': coverage_after_a_same_denom,
                'improvement_pct': coverage_after_a_same_denom - coverage_before_a
            },
            'neutral_segments_top20': [
                {'segment': seg, 'total_pct': pct}
                for seg, pct in sorted_neutral
            ]
        }
        
        os.makedirs('reports', exist_ok=True)
        output_file = 'reports/coverage_comparison.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n✅ 결과 저장: {output_file}", flush=True)
        print("=" * 80, flush=True)
        
    except Exception as e:
        import traceback
        print(f"\n❌ 오류 발생: {e}", flush=True)
        traceback.print_exc()
        raise
    finally:
        db.close()

if __name__ == '__main__':
    measure_coverage_comparison()

