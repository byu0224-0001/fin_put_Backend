# -*- coding: utf-8 -*-
"""
SK이노베이션 (096770) 매출 데이터 세그먼트 매핑 확인
- 석유 vs 석유제품 매핑 확인
- 전체 합계에서의 비중 계산 방식 확인
"""
import sys
sys.path.insert(0, '.')

from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock
from app.services.sector_classifier import calculate_revenue_sector_scores, SEGMENT_TO_SECTOR_MAP, normalize_segment_name
import json

def check_sk_innovation_revenue_mapping():
    """SK이노베이션 매출 데이터 세그먼트 매핑 확인"""
    db = SessionLocal()
    
    try:
        ticker = '096770'
        
        print("=" * 80)
        print(f"SK이노베이션 ({ticker}) 매출 데이터 세그먼트 매핑 확인")
        print("=" * 80)
        
        # Stock 조회
        stock = db.query(Stock).filter(Stock.ticker == ticker).first()
        if not stock:
            print(f"\n[오류] Stock 레코드 없음")
            return
        
        # CompanyDetail 조회
        detail = db.query(CompanyDetail).filter(
            CompanyDetail.ticker == ticker
        ).first()
        
        if not detail:
            print(f"\n[오류] CompanyDetail 레코드 없음")
            return
        
        # revenue_by_segment 확인
        raw_revenue = detail.revenue_by_segment
        
        if raw_revenue is None:
            print(f"\n[오류] revenue_by_segment가 None")
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
            print(f"\n[오류] 알 수 없는 타입: {type(raw_revenue)}")
            return
        
        print(f"\n[원본 매출 데이터 (revenue_by_segment)]")
        print(f"  총 세그먼트 수: {len(revenue_data)}개")
        
        total_pct = sum(pct for pct in revenue_data.values() if isinstance(pct, (int, float)) and pct > 0)
        print(f"  전체 비중 합계: {total_pct:.2f}%")
        
        print(f"\n  세그먼트별 비중:")
        for seg, pct in sorted(revenue_data.items(), key=lambda x: x[1] if isinstance(x[1], (int, float)) else 0, reverse=True):
            print(f"    - '{seg}': {pct}% (타입: {type(seg)}, 길이: {len(seg) if seg else 0})")
            # 각 문자 확인
            if seg:
                print(f"      문자 코드: {[ord(c) for c in seg[:10]]}")
        
        # 세그먼트 매핑 확인
        print(f"\n[세그먼트 → 섹터 매핑 확인]")
        
        segment_mapping_result = {}
        
        for segment, pct in revenue_data.items():
            if not isinstance(pct, (int, float)) or pct <= 0:
                continue
            
            # 정규화
            normalized_segment = normalize_segment_name(segment)
            
            # 매핑 시도
            matched_sector = None
            matched_keyword = None
            
            # 정규화된 세그먼트명으로 매핑 시도
            for keyword, sector in SEGMENT_TO_SECTOR_MAP.items():
                if keyword in normalized_segment:
                    matched_sector = sector
                    matched_keyword = keyword
                    break
            
            # 정규화된 세그먼트명으로 매핑 실패 시 원본으로 재시도
            if not matched_sector:
                segment_lower = str(segment).lower()
                for keyword, sector in SEGMENT_TO_SECTOR_MAP.items():
                    if keyword in segment_lower:
                        matched_sector = sector
                        matched_keyword = keyword
                        break
            
            segment_mapping_result[segment] = {
                'pct': pct,
                'normalized': normalized_segment,
                'matched_sector': matched_sector,
                'matched_keyword': matched_keyword
            }
            
            if matched_sector:
                print(f"  [OK] {segment} ({pct}%) → {matched_sector} (키워드: {matched_keyword})")
            else:
                print(f"  [X] {segment} ({pct}%) → 매핑 실패 (정규화: {normalized_segment})")
        
        # 석유 관련 세그먼트 확인
        print(f"\n[석유 관련 세그먼트 분석]")
        oil_segments = [seg for seg in revenue_data.keys() if '석유' in seg or '정제' in seg or 'PX' in seg or 'B-C' in seg or '화학' in seg]
        
        for seg in oil_segments:
            mapping = segment_mapping_result.get(seg, {})
            print(f"  - {seg}: {revenue_data[seg]}%")
            print(f"    정규화: {mapping.get('normalized', 'N/A')}")
            print(f"    매핑 섹터: {mapping.get('matched_sector', 'N/A')}")
            print(f"    매핑 키워드: {mapping.get('matched_keyword', 'N/A')}")
        
        # 섹터별 점수 계산
        print(f"\n[섹터별 점수 계산 결과]")
        revenue_scores, revenue_audit = calculate_revenue_sector_scores(revenue_data)
        
        print(f"  섹터별 점수:")
        for sector, score in sorted(revenue_scores.items(), key=lambda x: x[1], reverse=True):
            print(f"    - {sector}: {score:.3f}")
        
        print(f"\n  Audit 정보:")
        print(f"    - Top1: {revenue_audit.get('top1', 'N/A')} ({revenue_audit.get('top1_pct', 0):.2f}%)")
        print(f"    - Top2: {revenue_audit.get('top2', 'N/A')} ({revenue_audit.get('top2_pct', 0):.2f}%)")
        print(f"    - Margin: {revenue_audit.get('margin', 0):.2f}%")
        print(f"    - Coverage: {revenue_audit.get('coverage', 0):.2f}%")
        print(f"    - Mapped Pct: {revenue_audit.get('mapped_pct', 0):.2f}%")
        print(f"    - Total Pct: {revenue_audit.get('total_pct', 0):.2f}%")
        
        # 세그먼트 매핑 상세
        segment_mapping_detail = revenue_audit.get('segment_mapping', {})
        if segment_mapping_detail:
            print(f"\n  세그먼트 매핑 상세:")
            for seg, mapping_info in segment_mapping_detail.items():
                print(f"    - {seg}: {mapping_info.get('pct', 0)}% → {mapping_info.get('sector', 'N/A')}")
        
        # 사용자 질문에 대한 답변
        print(f"\n" + "=" * 80)
        print(f"[사용자 질문에 대한 답변]")
        print("=" * 80)
        
        print(f"\n1. 전체 합계에서 해당 매출만큼의 비중을 계산한 것 맞는지?")
        print(f"  [답변] 네, 맞습니다.")
        print(f"  - 현재 로직은 각 세그먼트의 비율(%)을 그대로 사용합니다.")
        print(f"  - total_pct = sum(pct for pct in revenue_by_segment.values())")
        print(f"  - 각 세그먼트의 비율을 100으로 나눠서 섹터별 점수로 누적합니다.")
        print(f"  - 예: '석유: 23.36%' → SEC_CHEM에 0.2336 점수 추가")
        print(f"  - 예: '석유제품: 16.81%' → SEC_CHEM에 0.1681 점수 추가")
        print(f"  - 따라서 '석유'와 '석유제품'은 모두 SEC_CHEM으로 매핑되면 합산됩니다.")
        print(f"  - 단, 실제 매출액 규모는 고려하지 않고 비율만 사용합니다.")
        
        print(f"\n2. 석유와 석유제품 같은 거 아닌가?")
        print(f"  [답변] 네, 맞습니다. 같은 섹터(SEC_CHEM)로 매핑되어야 합니다.")
        print(f"  - '석유'는 SEGMENT_TO_SECTOR_MAP에 '석유': 'SEC_CHEM'으로 매핑되어 있습니다.")
        print(f"  - '석유제품'은 명시적으로 매핑되어 있지 않지만, '석유' 키워드가 포함되어 있어")
        print(f"    매핑 로직에서 '석유' 키워드로 인식되어 SEC_CHEM으로 매핑됩니다.")
        print(f"  - 따라서 두 세그먼트 모두 SEC_CHEM으로 매핑되어 합산됩니다.")
        
        # 실제 매핑 확인
        oil_mapped = [seg for seg, mapping in segment_mapping_result.items() 
                     if ('석유' in seg or '정제' in seg or 'PX' in seg or 'B-C' in seg or '화학' in seg) 
                     and mapping.get('matched_sector') == 'SEC_CHEM']
        if oil_mapped:
            print(f"\n  [확인] 석유/화학 관련 세그먼트 매핑:")
            total_chem_pct = 0
            for seg in oil_mapped:
                mapping = segment_mapping_result[seg]
                total_chem_pct += mapping['pct']
                print(f"    - {seg}: {mapping['pct']}% → {mapping['matched_sector']} (키워드: {mapping['matched_keyword']})")
            print(f"    → SEC_CHEM 총합: {total_chem_pct:.2f}%")
        
        # 매핑 실패한 세그먼트 확인
        unmapped = [seg for seg, mapping in segment_mapping_result.items() 
                   if mapping.get('matched_sector') is None]
        if unmapped:
            print(f"\n  [경고] 매핑 실패한 세그먼트:")
            for seg in unmapped:
                mapping = segment_mapping_result[seg]
                print(f"    - {seg}: {mapping['pct']}% (정규화: {mapping['normalized']})")
            print(f"    → 이 세그먼트들은 섹터 점수 계산에 포함되지 않습니다.")
        
    finally:
        db.close()

if __name__ == '__main__':
    check_sk_innovation_revenue_mapping()

