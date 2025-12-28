# -*- coding: utf-8 -*-
"""
매출 세그먼트 매핑 원인 규명 스크립트 (P0 피드백 반영)
- 실제 매칭된 keyword 확인
- 매칭 후보 Top5 출력
- 원본 데이터 상세 확인 (repr, 문자 코드)
"""
import sys
sys.path.insert(0, '.')

from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock
from app.services.sector_classifier import (
    calculate_revenue_sector_scores, 
    SEGMENT_TO_SECTOR_MAP, 
    normalize_segment_name
)
import json
import unicodedata

def diagnose_segment_mapping(segment: str, pct: float):
    """
    단일 세그먼트의 매핑 과정을 상세 진단
    
    Returns:
        dict: 진단 결과
    """
    result = {
        'segment_raw': segment,
        'segment_repr': repr(segment),
        'pct': pct,
        'char_codes': [hex(ord(c)) for c in segment[:20]],
        'normalized': None,
        'normalized_repr': None,
        'matching_candidates': [],
        'matched_keyword': None,
        'matched_sector': None,
        'match_rule': None,
        'match_method': None,
    }
    
    # 정규화
    normalized = normalize_segment_name(segment)
    result['normalized'] = normalized
    result['normalized_repr'] = repr(normalized)
    
    # 매칭 후보 수집 (Top5)
    normalized_lower = normalized.lower()
    segment_lower = segment.lower()
    
    candidates = []
    
    # 키워드를 길이 내림차순으로 정렬 (긴 키워드 우선)
    sorted_keywords = sorted(SEGMENT_TO_SECTOR_MAP.items(), key=lambda x: len(x[0]), reverse=True)
    
    for keyword, sector in sorted_keywords:
        keyword_lower = keyword.lower()
        keyword_len = len(keyword)
        
        match_score = 0
        match_rule = None
        match_method = None
        
        # 짧은 토큰(len<3)은 exact만 허용
        if keyword_len < 3:
            if keyword_lower == normalized_lower:
                match_score = 100  # exact match (normalized)
                match_rule = 'exact'
                match_method = 'normalized'
            elif keyword_lower == segment_lower:
                match_score = 90  # exact match (raw)
                match_rule = 'exact'
                match_method = 'raw'
        else:
            # contains 매칭
            if keyword_lower in normalized_lower:
                match_score = 80  # contains (normalized)
                match_rule = 'contains'
                match_method = 'normalized'
            elif keyword_lower in segment_lower:
                match_score = 70  # contains (raw)
                match_rule = 'contains'
                match_method = 'raw'
        
        if match_score > 0:
            candidates.append({
                'keyword': keyword,
                'sector': sector,
                'keyword_len': keyword_len,
                'match_score': match_score,
                'match_rule': match_rule,
                'match_method': match_method,
            })
    
    # 점수 내림차순 정렬
    candidates.sort(key=lambda x: x['match_score'], reverse=True)
    result['matching_candidates'] = candidates[:5]  # Top5
    
    # 최고 점수 후보가 실제 매칭 결과
    if candidates:
        top_candidate = candidates[0]
        result['matched_keyword'] = top_candidate['keyword']
        result['matched_sector'] = top_candidate['sector']
        result['match_rule'] = top_candidate['match_rule']
        result['match_method'] = top_candidate['match_method']
    
    return result


def diagnose_company_revenue_mapping(ticker: str, limit_output: int = None):
    """
    특정 기업의 매출 세그먼트 매핑을 상세 진단
    
    Args:
        ticker: 종목코드
        limit_output: 출력 제한 (None이면 전체 출력)
    """
    db = SessionLocal()
    
    output_lines = []
    
    def print_line(text: str):
        """출력 버퍼에 추가"""
        output_lines.append(text)
        if limit_output is None or len(output_lines) <= limit_output:
            print(text)
    
    try:
        print_line("=" * 80)
        print_line(f"매출 세그먼트 매핑 원인 규명: {ticker}")
        print_line("=" * 80)
        
        # Stock 조회
        stock = db.query(Stock).filter(Stock.ticker == ticker).first()
        if not stock:
            print_line(f"\n[오류] Stock 레코드 없음")
            return
        
        company_name = stock.stock_name
        print_line(f"\n회사명: {company_name}")
        
        # CompanyDetail 조회
        detail = db.query(CompanyDetail).filter(
            CompanyDetail.ticker == ticker
        ).first()
        
        if not detail:
            print_line(f"\n[오류] CompanyDetail 레코드 없음")
            return
        
        # revenue_by_segment 확인
        raw_revenue = detail.revenue_by_segment
        
        if raw_revenue is None:
            print_line(f"\n[오류] revenue_by_segment가 None")
            return
        
        # 타입 변환
        if isinstance(raw_revenue, str):
            try:
                revenue_data = json.loads(raw_revenue) if raw_revenue else {}
            except:
                revenue_data = {}
        elif isinstance(raw_revenue, dict):
            revenue_data = raw_revenue
        else:
            print_line(f"\n[오류] 알 수 없는 타입: {type(raw_revenue)}")
            return
        
        print_line(f"\n[원본 매출 데이터]")
        print_line(f"  총 세그먼트 수: {len(revenue_data)}개")
        total_pct = sum(pct for pct in revenue_data.values() if isinstance(pct, (int, float)) and pct > 0)
        print_line(f"  전체 비중 합계: {total_pct:.2f}%")
        
        # 각 세그먼트 상세 진단
        print_line(f"\n[세그먼트별 상세 진단]")
        print_line("=" * 80)
        
        all_diagnoses = []
        
        for segment, pct in sorted(revenue_data.items(), key=lambda x: x[1] if isinstance(x[1], (int, float)) else 0, reverse=True):
            if not isinstance(pct, (int, float)) or pct <= 0:
                continue
            
            diagnosis = diagnose_segment_mapping(segment, pct)
            all_diagnoses.append(diagnosis)
            
            print_line(f"\n세그먼트: {segment} ({pct}%)")
            print_line(f"  원본 (repr): {diagnosis['segment_repr']}")
            print_line(f"  문자 코드: {', '.join(diagnosis['char_codes'][:10])}...")
            print_line(f"  정규화: {diagnosis['normalized']}")
            print_line(f"  정규화 (repr): {diagnosis['normalized_repr']}")
            
            if diagnosis['matched_keyword']:
                print_line(f"  [OK] 매칭 성공:")
                print_line(f"    - 키워드: {diagnosis['matched_keyword']}")
                print_line(f"    - 섹터: {diagnosis['matched_sector']}")
                print_line(f"    - 규칙: {diagnosis['match_rule']}")
                print_line(f"    - 방법: {diagnosis['match_method']}")
            else:
                print_line(f"  [FAIL] 매칭 실패")
            
            if diagnosis['matching_candidates']:
                print_line(f"  매칭 후보 Top5:")
                for i, candidate in enumerate(diagnosis['matching_candidates'], 1):
                    print_line(f"    {i}. {candidate['keyword']} ({candidate['keyword_len']}자) → {candidate['sector']}")
                    print_line(f"       점수: {candidate['match_score']}, 규칙: {candidate['match_rule']}, 방법: {candidate['match_method']}")
        
        # 실제 매핑 결과 확인
        print_line(f"\n" + "=" * 80)
        print_line(f"[실제 매핑 결과 확인]")
        print_line("=" * 80)
        
        revenue_scores, revenue_audit = calculate_revenue_sector_scores(revenue_data)
        
        print_line(f"\n섹터별 점수:")
        for sector, score in sorted(revenue_scores.items(), key=lambda x: x[1], reverse=True):
            print_line(f"  - {sector}: {score:.3f}")
        
        print_line(f"\nAudit 정보:")
        print_line(f"  - Top1: {revenue_audit.get('top1', 'N/A')} ({revenue_audit.get('top1_pct', 0):.2f}%)")
        print_line(f"  - Top2: {revenue_audit.get('top2', 'N/A')} ({revenue_audit.get('top2_pct', 0):.2f}%)")
        print_line(f"  - Margin: {revenue_audit.get('margin', 0):.2f}%")
        print_line(f"  - Coverage: {revenue_audit.get('coverage', 0):.2f}%")
        
        # 세그먼트 매핑 상세
        segment_mapping_detail = revenue_audit.get('segment_mapping', {})
        if segment_mapping_detail:
            print_line(f"\n세그먼트 매핑 상세:")
            for seg, mapping_info in segment_mapping_detail.items():
                print_line(f"  - {seg}: {mapping_info.get('pct', 0)}%")
                print_line(f"    → 섹터: {mapping_info.get('sector', 'N/A')}")
                print_line(f"    → 키워드: {mapping_info.get('matched_keyword', 'N/A')}")
                print_line(f"    → 규칙: {mapping_info.get('match_rule', 'N/A')}")
                print_line(f"    → 방법: {mapping_info.get('match_method', 'N/A')}")
        
        # 치명 오탐 섹터 확인
        print_line(f"\n" + "=" * 80)
        print_line(f"[치명 오탐 섹터 확인]")
        print_line("=" * 80)
        
        critical_sectors = ['SEC_TRAVEL', 'SEC_ENT']
        for sector in critical_sectors:
            if sector in revenue_scores:
                print_line(f"\n[WARN] {sector} 매핑 발견 (점수: {revenue_scores[sector]:.3f})")
                print_line(f"  매핑된 세그먼트:")
                for seg, mapping_info in segment_mapping_detail.items():
                    if mapping_info.get('sector') == sector:
                        print_line(f"    - {seg}: {mapping_info.get('pct', 0)}%")
                        print_line(f"      키워드: {mapping_info.get('matched_keyword', 'N/A')}")
                        print_line(f"      규칙: {mapping_info.get('match_rule', 'N/A')}")
        
        if limit_output and len(output_lines) > limit_output:
            print_line(f"\n... (출력 제한: {limit_output}줄, 총 {len(output_lines)}줄)")
        
    finally:
        db.close()


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='매출 세그먼트 매핑 원인 규명')
    parser.add_argument('--ticker', type=str, default='096770', help='종목코드 (기본값: 096770)')
    parser.add_argument('--limit', type=int, default=None, help='출력 줄 수 제한 (기본값: 제한 없음)')
    args = parser.parse_args()
    
    diagnose_company_revenue_mapping(args.ticker, limit_output=args.limit)
