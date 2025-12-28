# -*- coding: utf-8 -*-
"""
P0-1: SEGMENT_TO_SECTOR_MAP 품질 검증 리포트
전체 재분류 전 필수 검증
"""
import sys
sys.path.insert(0, '.')

import json
import os
from datetime import datetime
from collections import defaultdict, Counter
from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock
from app.models.investor_sector import InvestorSector
from app.services.sector_classifier import (
    SEGMENT_TO_SECTOR_MAP, 
    calculate_revenue_sector_scores,
    normalize_segment_name
)

PROGRESS_FILE = 'reports/segment_mapping_validation_progress.json'
REPORT_FILE = 'reports/segment_mapping_validation.json'

def load_progress():
    """진행 상황 로드"""
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return None
    return None

def save_progress(stats, unmapped_segments, sector_segments, misclassification_candidates, processed_count, total_count):
    """진행 상황 저장"""
    progress = {
        'processed_count': processed_count,
        'total_count': total_count,
        'last_updated': datetime.now().isoformat(),
        'stats': stats,
        'unmapped_segments': dict(unmapped_segments),
        'sector_segments': {k: dict(v) for k, v in sector_segments.items()},
        'misclassification_candidates': misclassification_candidates
    }
    os.makedirs('reports', exist_ok=True)
    with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
        json.dump(progress, f, ensure_ascii=False, indent=2, default=str)

def main():
    db = SessionLocal()
    
    print("=" * 80)
    print("SEGMENT_TO_SECTOR_MAP 품질 검증 리포트")
    print("=" * 80)
    
    # 진행 상황 로드
    progress = load_progress()
    if progress and progress.get('processed_count') == progress.get('total_count'):
        print(f"✅ 이미 완료된 리포트 발견 (처리일: {progress.get('last_updated', 'N/A')})")
        if os.path.exists(REPORT_FILE):
            print(f"📄 기존 리포트 파일: {REPORT_FILE}")
            print("⚠️  기존 리포트가 있습니다. 재실행하려면 리포트 파일을 삭제하거나 --force 옵션을 사용하세요.")
            print("\n기존 리포트 요약:")
            try:
                with open(REPORT_FILE, 'r', encoding='utf-8') as f:
                    existing_report = json.load(f)
                    cov = existing_report.get('coverage', {})
                    print(f"  - Coverage-A: {cov.get('coverage_a', 0):.1f}%")
                    print(f"  - Coverage-B: {cov.get('coverage_b', 0):.1f}%")
                    print(f"  - 매출 비중 있는 기업: {cov.get('companies_with_revenue', 0)}/{cov.get('total_companies', 0)}")
            except:
                pass
            db.close()
            return
    
    # 모든 CompanyDetail 조회
    print("\n[1/5] 데이터베이스 조회 중...")
    all_details = db.query(CompanyDetail, Stock).join(
        Stock, CompanyDetail.ticker == Stock.ticker
    ).all()
    
    # 통계 수집
    total_companies = len(all_details)
    companies_with_revenue = 0
    total_segments = 0
    mapped_segments = 0
    unmapped_segments = defaultdict(lambda: {'count': 0, 'total_pct': 0.0})
    sector_segments = defaultdict(lambda: defaultdict(lambda: {'count': 0, 'total_pct': 0.0}))
    
    # 🆕 P0-2: Coverage-A/B 분리 측정
    total_revenue_pct = 0.0  # 전체 매출 비중 합계
    mapped_revenue_pct = 0.0  # 매핑된 세그먼트의 매출 비중 합계
    
    # 🆕 P0-2: 범용 토큰 제외 측정
    generic_keywords = ['기타', 'other', '그외', '상품', '제품', '용역', '서비스', 
                       '기타매출', '기타부문', '기타사업', '기타사업부문', '기타제품',
                       '상품매출', '제품매출', '용역매출', '기 타', '기  타', '기   타']
    total_revenue_pct_excl_generic = 0.0  # 범용 토큰 제외한 전체 매출 비중
    mapped_revenue_pct_excl_generic = 0.0  # 범용 토큰 제외한 매핑된 매출 비중
    
    # 오분류 후보
    misclassification_candidates = []
    
    print(f"[2/5] 기업 데이터 처리 중... (총 {total_companies}개)")
    batch_size = 100
    for idx, (detail, stock) in enumerate(all_details):
        if not detail.revenue_by_segment or not isinstance(detail.revenue_by_segment, dict):
            continue
        
        companies_with_revenue += 1
        
        # 현재 분류 조회
        current_sector = db.query(InvestorSector).filter(
            InvestorSector.ticker == detail.ticker,
            InvestorSector.is_primary == True
        ).first()
        current_major = current_sector.major_sector if current_sector else None
        
        # 매출 비중 -> 섹터 점수 계산 (튜플 반환: scores, audit_info)
        revenue_scores, _ = calculate_revenue_sector_scores(detail.revenue_by_segment)
        
        # 매출 기반 최고 섹터
        revenue_best_sector = max(revenue_scores.items(), key=lambda x: x[1])[0] if revenue_scores else None
        
        for segment, pct in detail.revenue_by_segment.items():
            if not isinstance(pct, (int, float)) or pct <= 0:
                continue
            
            total_segments += 1
            total_revenue_pct += pct  # 🆕 전체 매출 비중 누적
            
            # 🆕 범용 토큰 여부 확인
            is_generic = False
            segment_lower = str(segment).lower().strip()
            normalized_for_check = normalize_segment_name(segment)
            for kw in generic_keywords:
                if kw in segment_lower or kw in normalized_for_check:
                    is_generic = True
                    break
            
            # 범용 토큰이 아니면 범용 토큰 제외 측정에 포함
            if not is_generic:
                total_revenue_pct_excl_generic += pct
            
            # 🆕 P0-1: 세그먼트명 표준화
            normalized_segment = normalize_segment_name(segment)
            
            # 매핑 확인 (정규화된 세그먼트명 우선 사용)
            matched_sector = None
            for keyword, sector in SEGMENT_TO_SECTOR_MAP.items():
                if keyword in normalized_segment:
                    matched_sector = sector
                    break
            
            # 정규화된 세그먼트명으로 매핑 실패 시 원본으로 재시도
            if not matched_sector:
                segment_lower = str(segment).lower()
                for keyword, sector in SEGMENT_TO_SECTOR_MAP.items():
                    if keyword in segment_lower:
                        matched_sector = sector
                        break
            
            if matched_sector:
                mapped_segments += 1
                mapped_revenue_pct += pct  # 🆕 매핑된 매출 비중 누적
                # 범용 토큰이 아니면 범용 토큰 제외 측정에 포함
                if not is_generic:
                    mapped_revenue_pct_excl_generic += pct
                sector_segments[matched_sector][segment]['count'] += 1
                sector_segments[matched_sector][segment]['total_pct'] += pct
            else:
                unmapped_segments[segment]['count'] += 1
                unmapped_segments[segment]['total_pct'] += pct
        
        # 오분류 후보 체크 (매출 1등 섹터 vs 현재 섹터 충돌)
        if revenue_best_sector and current_major and revenue_best_sector != current_major:
            best_score = revenue_scores.get(revenue_best_sector, 0)
            if best_score >= 0.3:  # 30% 이상 매출 비중인 경우만
                misclassification_candidates.append({
                    'ticker': detail.ticker,
                    'name': stock.stock_name,
                    'current_sector': current_major,
                    'revenue_best_sector': revenue_best_sector,
                    'revenue_score': best_score,
                    'revenue_by_segment': detail.revenue_by_segment
                })
        
        # 배치 단위로 진행 상황 저장 및 출력
        if (idx + 1) % batch_size == 0:
            stats = {
                'total_companies': total_companies,
                'companies_with_revenue': companies_with_revenue,
                'total_segments': total_segments,
                'mapped_segments': mapped_segments,
                'total_revenue_pct': total_revenue_pct,
                'mapped_revenue_pct': mapped_revenue_pct
            }
            save_progress(stats, unmapped_segments, sector_segments, misclassification_candidates, idx + 1, total_companies)
            print(f"  진행: {idx + 1}/{total_companies} ({((idx+1)/total_companies*100):.1f}%) | "
                  f"매출기업: {companies_with_revenue} | 세그먼트: {total_segments} | 매핑: {mapped_segments}", flush=True)
    
    print(f"✅ 데이터 처리 완료: {total_companies}개 기업")
    
    # 1. 매핑 커버리지 (P0-2: Coverage-A/B 분리)
    print("\n" + "=" * 80)
    print("[1] 매핑 커버리지 (Coverage-A/B 분리)")
    print("=" * 80)
    
    # Coverage-B: 세그먼트 카운트 커버리지
    coverage_b = (mapped_segments / total_segments * 100) if total_segments > 0 else 0
    
    # Coverage-A: 매출 가중 커버리지 (MVP에서 더 중요!)
    coverage_a = (mapped_revenue_pct / total_revenue_pct * 100) if total_revenue_pct > 0 else 0
    
    # 🆕 Coverage-A (실제): 범용 토큰 제외한 실제 매핑 가능한 세그먼트 기준
    coverage_a_actual = (mapped_revenue_pct_excl_generic / total_revenue_pct_excl_generic * 100) if total_revenue_pct_excl_generic > 0 else 0
    
    print(f"전체 기업: {total_companies}")
    print(f"매출 비중 있는 기업: {companies_with_revenue} ({companies_with_revenue/total_companies*100:.1f}%)")
    print(f"\n[Coverage-B] 세그먼트 카운트 커버리지:")
    print(f"  전체 세그먼트 수: {total_segments}")
    print(f"  매핑된 세그먼트: {mapped_segments} ({coverage_b:.1f}%)")
    print(f"  미매핑 세그먼트: {total_segments - mapped_segments} ({100-coverage_b:.1f}%)")
    print(f"\n[Coverage-A] 매출 가중 커버리지 (🔥 MVP 핵심 지표):")
    print(f"  전체 매출 비중 합계: {total_revenue_pct:.1f}%")
    print(f"  매핑된 매출 비중 합계: {mapped_revenue_pct:.1f}%")
    print(f"  Coverage-A (전체): {coverage_a:.1f}%")
    print(f"\n[Coverage-A (실제)] 범용 토큰 제외한 실제 매핑 가능한 세그먼트 기준:")
    print(f"  범용 토큰 제외 전체 매출 비중: {total_revenue_pct_excl_generic:.1f}%")
    print(f"  범용 토큰 제외 매핑된 매출 비중: {mapped_revenue_pct_excl_generic:.1f}%")
    print(f"  Coverage-A (실제): {coverage_a_actual:.1f}%")
    print(f"\n💡 해석: Coverage-A (실제)가 {coverage_a_actual:.1f}%이므로, {'✅ 핵심 세그먼트는 대부분 매핑됨' if coverage_a_actual >= 50 else '⚠️ 핵심 세그먼트 매핑 부족'}")
    
    # 2. Unmapped 세그먼트 Top 100 (잔여물 분석)
    print("\n[3/5] Unmapped 세그먼트 분석 중...")
    sorted_unmapped = sorted(unmapped_segments.items(), key=lambda x: x[1]['count'], reverse=True)[:100]
    
    # 섹터 힌트가 될 만한 단어 추출
    sector_hint_keywords = []
    for segment, info in sorted_unmapped[:50]:  # Top 50만 분석
        normalized = normalize_segment_name(segment)
        # 범용 토큰이 아닌 핵심 단어 추출
        if normalized and len(normalized) >= 2:
            words = normalized.split()
            for word in words:
                if word not in ['기타', '상품', '제품', '용역', '서비스', '사업', '부문'] and len(word) >= 2:
                    sector_hint_keywords.append((word, info['count']))
    
    # 중복 제거 및 빈도순 정렬
    hint_counter = Counter([word for word, _ in sector_hint_keywords])
    top_hints = hint_counter.most_common(20)
    
    print("=" * 80)
    print("[2] Unmapped 세그먼트 Top 100 (잔여물 분석)")
    print("=" * 80)
    print(f"\n[섹터 힌트 키워드 Top 20] (정규화 후 추출)")
    for i, (hint, count) in enumerate(top_hints, 1):
        print(f"  {i:2}. {hint:20} (빈도: {count:4})")
    
    print(f"\n[Unmapped 세그먼트 Top 30]")
    for i, (segment, info) in enumerate(sorted_unmapped[:30]):
        print(f"{i+1:2}. {segment:40} | 빈도: {info['count']:4} | 총 비중: {info['total_pct']:.1f}%")
    
    # 3. 오분류 후보 Top 20
    print("\n[4/5] 오분류 후보 분석 중...")
    sorted_misclass = sorted(misclassification_candidates, key=lambda x: x['revenue_score'], reverse=True)[:20]
    
    print("\n" + "=" * 80)
    print("[3] 오분류 후보 Top 20 (매출 1등 섹터 vs 현재 섹터 충돌)")
    print("=" * 80)
    for i, mc in enumerate(sorted_misclass):
        print(f"\n{i+1}. {mc['name']} ({mc['ticker']})")
        print(f"   현재: {mc['current_sector']} | 매출기반: {mc['revenue_best_sector']} (score: {mc['revenue_score']:.3f})")
        # 상위 3개 세그먼트 출력
        if mc['revenue_by_segment']:
            sorted_rev = sorted(mc['revenue_by_segment'].items(), key=lambda x: x[1] if isinstance(x[1], (int, float)) else 0, reverse=True)[:3]
            segs_str = ', '.join([f"{seg}:{pct}%" for seg, pct in sorted_rev])
            print(f"   매출비중: {segs_str}")
    
    # 4. 섹터별 대표 세그먼트 Top 10
    print("\n[5/5] 섹터별 세그먼트 분석 중...")
    print("\n" + "=" * 80)
    print("[4] 섹터별 대표 세그먼트 Top 10")
    print("=" * 80)
    for sector in sorted(sector_segments.keys()):
        segments = sector_segments[sector]
        if not segments:
            continue
        sorted_segs = sorted(segments.items(), key=lambda x: x[1]['count'], reverse=True)[:10]
        print(f"\n{sector}:")
        for seg, info in sorted_segs:
            print(f"  - {seg}: {info['count']}회 (총 {info['total_pct']:.1f}%)")
    
    # 결과 저장
    print("\n결과 저장 중...")
    report = {
        'coverage': {
            'total_companies': total_companies,
            'companies_with_revenue': companies_with_revenue,
            'total_segments': total_segments,
            'mapped_segments': mapped_segments,
            'coverage_b': coverage_b,  # 🆕 세그먼트 카운트 커버리지
            'coverage_a': coverage_a,  # 🆕 매출 가중 커버리지 (MVP 핵심)
            'coverage_a_actual': coverage_a_actual,  # 🆕 범용 토큰 제외한 실제 커버리지
            'total_revenue_pct': total_revenue_pct,
            'mapped_revenue_pct': mapped_revenue_pct,
            'total_revenue_pct_excl_generic': total_revenue_pct_excl_generic,
            'mapped_revenue_pct_excl_generic': mapped_revenue_pct_excl_generic,
            'coverage_pct': coverage_b  # 하위 호환성 (기존 필드)
        },
        'sector_hint_keywords': [{'keyword': hint, 'count': count} for hint, count in top_hints],  # 🆕 섹터 힌트
        'unmapped_top100': [(seg, dict(info)) for seg, info in sorted_unmapped],  # 🆕 Top 100으로 확장
        'unmapped_top30': [(seg, dict(info)) for seg, info in sorted_unmapped[:30]],  # 하위 호환성
        'misclassification_candidates': sorted_misclass,
        'sector_segments': {sector: list(segs.items())[:10] for sector, segs in sector_segments.items()}
    }
    
    os.makedirs('reports', exist_ok=True)
    with open(REPORT_FILE, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2, default=str)
    
    # 최종 진행 상황 저장
    stats = {
        'total_companies': total_companies,
        'companies_with_revenue': companies_with_revenue,
        'total_segments': total_segments,
        'mapped_segments': mapped_segments,
        'total_revenue_pct': total_revenue_pct,
        'mapped_revenue_pct': mapped_revenue_pct
    }
    save_progress(stats, unmapped_segments, sector_segments, misclassification_candidates, total_companies, total_companies)
    
    print("\n" + "=" * 80)
    print("✅ 결과 저장 완료")
    print("=" * 80)
    print(f"📄 리포트: {REPORT_FILE}")
    print(f"📄 진행상황: {PROGRESS_FILE}")
    print("=" * 80)
    
    db.close()


if __name__ == '__main__':
    import sys
    force = '--force' in sys.argv or '-f' in sys.argv
    if force:
        # 강제 재실행: 기존 리포트 삭제
        if os.path.exists(REPORT_FILE):
            os.remove(REPORT_FILE)
            print(f"🗑️  기존 리포트 삭제: {REPORT_FILE}")
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
            print(f"🗑️  기존 진행 상황 삭제: {PROGRESS_FILE}")
    main()

