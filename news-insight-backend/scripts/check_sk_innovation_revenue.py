# -*- coding: utf-8 -*-
"""
SK이노베이션 (096770) 매출 데이터 수집 상태 확인
"""
import sys
sys.path.insert(0, '.')

from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock
from app.services.sector_classifier import classify_sector_rule_based
import json

def check_sk_innovation_revenue():
    """SK이노베이션 매출 데이터 수집 상태 확인"""
    db = SessionLocal()
    
    try:
        ticker = '096770'
        
        print("=" * 80)
        print(f"SK이노베이션 ({ticker}) 매출 데이터 수집 상태 확인")
        print("=" * 80)
        
        # Stock 조회
        stock = db.query(Stock).filter(Stock.ticker == ticker).first()
        if not stock:
            print(f"\n[오류] Stock 레코드 없음")
            return
        
        print(f"\n[기본 정보]")
        print(f"  회사명: {stock.stock_name}")
        print(f"  티커: {stock.ticker}")
        print(f"  시가총액: {stock.market_cap/1e12:.1f}조원" if stock.market_cap else "  시가총액: N/A")
        
        # CompanyDetail 조회
        detail = db.query(CompanyDetail).filter(
            CompanyDetail.ticker == ticker
        ).first()
        
        if not detail:
            print(f"\n[오류] CompanyDetail 레코드 없음")
            return
        
        print(f"\n[매출 데이터 (revenue_by_segment) 상태]")
        
        # revenue_by_segment 확인
        raw_revenue = detail.revenue_by_segment
        
        if raw_revenue is None:
            print(f"  상태: [X] None (매출 데이터 없음)")
            print(f"  결론: 매출 데이터 수집 실패")
            revenue_data = {}
        elif isinstance(raw_revenue, str):
            print(f"  상태: [WARN] JSON 문자열 형태")
            print(f"  원본 문자열 길이: {len(raw_revenue)}자")
            print(f"  원본 문자열 미리보기: {raw_revenue[:200]}...")
            try:
                revenue_data = json.loads(raw_revenue) if raw_revenue else {}
                if revenue_data and isinstance(revenue_data, dict) and len(revenue_data) > 0:
                    print(f"  파싱 결과: [OK] 성공 ({len(revenue_data)}개 세그먼트)")
                    for seg, pct in revenue_data.items():
                        print(f"    - {seg}: {pct}%")
                    print(f"  결론: 매출 데이터 수집 성공 (JSON 문자열 형태로 저장됨)")
                else:
                    print(f"  파싱 결과: [X] 빈 데이터")
                    print(f"  결론: 매출 데이터 수집 실패 (JSON은 있으나 내용 없음)")
            except json.JSONDecodeError as e:
                print(f"  파싱 결과: [X] JSON 파싱 실패")
                print(f"  오류 메시지: {str(e)}")
                print(f"  결론: 매출 데이터 수집 실패 (JSON_PARSE_FAIL)")
                revenue_data = {}
        elif isinstance(raw_revenue, dict):
            revenue_data = raw_revenue
            if len(revenue_data) > 0:
                print(f"  상태: [OK] Dict 형태 ({len(revenue_data)}개 세그먼트)")
                for seg, pct in revenue_data.items():
                    print(f"    - {seg}: {pct}%")
                print(f"  결론: 매출 데이터 수집 성공")
            else:
                print(f"  상태: [X] 빈 Dict")
                print(f"  결론: 매출 데이터 수집 실패 (빈 데이터)")
        else:
            print(f"  상태: [WARN] 알 수 없는 타입: {type(raw_revenue)}")
            print(f"  원본 값: {raw_revenue}")
            print(f"  결론: 매출 데이터 수집 실패 (타입 오류)")
            revenue_data = {}
        
        # 분류 결과 확인
        print(f"\n[분류 결과]")
        major, sub, vc, conf, boosting_log = classify_sector_rule_based(
            detail, stock.stock_name, ticker=ticker
        )
        
        classification_meta = boosting_log.get('classification_meta', {}) if boosting_log else {}
        revenue_quality = classification_meta.get('revenue_quality', 'N/A')
        quality_reason = classification_meta.get('quality_reason', 'N/A')
        override_hit = classification_meta.get('override_hit', False)
        override_reason = classification_meta.get('override_reason', '')
        primary_sector_source = classification_meta.get('primary_sector_source', 'N/A')
        decision_trace = classification_meta.get('decision_trace', {})
        
        print(f"  Major Sector: {major}")
        print(f"  Sub Sector: {sub}")
        print(f"  Value Chain: {vc}")
        print(f"  Confidence: {conf}")
        print(f"  Primary Sector Source: {primary_sector_source}")
        print(f"  Revenue Quality: {revenue_quality}")
        print(f"  Quality Reason: {quality_reason}")
        print(f"  Override Hit: {override_hit}")
        if override_reason:
            print(f"  Override Reason: {override_reason}")
        
        # decision_trace 확인
        if decision_trace:
            print(f"\n[Decision Trace (인과 구조)]")
            if 'revenue' in decision_trace:
                revenue_trace = decision_trace['revenue']
                print(f"  Revenue Quality: {revenue_trace.get('quality', 'N/A')}")
                print(f"  Revenue Reason: {revenue_trace.get('reason', 'N/A')}")
                print(f"  Has Data: {revenue_trace.get('has_data', False)}")
                print(f"  Segments Count: {revenue_trace.get('segments_count', 0)}")
                print(f"  Sum Pct: {revenue_trace.get('sum_pct', 0.0)}%")
            
            if 'sector' in decision_trace:
                sector_trace = decision_trace['sector']
                print(f"  Sector Final: {sector_trace.get('final', 'N/A')}")
                print(f"  Sector Source: {sector_trace.get('source', 'N/A')}")
                print(f"  Final Reason: {sector_trace.get('final_reason', 'N/A')}")
                print(f"  Confidence Band: {sector_trace.get('confidence_band', 'N/A')}")
                print(f"  Confidence Why: {sector_trace.get('confidence_why', 'N/A')}")
                if 'candidates_top3' in sector_trace:
                    print(f"  Candidates Top3:")
                    for cand in sector_trace['candidates_top3']:
                        print(f"    - {cand.get('sector', 'N/A')}: {cand.get('score', 0.0):.3f} (source: {cand.get('source', 'N/A')})")
            
            if 'override' in decision_trace:
                override_trace = decision_trace['override']
                print(f"  Override Hit: {override_trace.get('hit', False)}")
                print(f"  Override Reason: {override_trace.get('reason', 'N/A')}")
                print(f"  Override Key Used: {override_trace.get('key_used', 'N/A')}")
        
        # 최종 결론
        print(f"\n" + "=" * 80)
        print(f"[최종 결론]")
        print("=" * 80)
        
        has_revenue_data = bool(revenue_data and isinstance(revenue_data, dict) and len(revenue_data) > 0)
        
        if has_revenue_data:
            print(f"[OK] 매출 데이터 수집 성공")
            print(f"  - {len(revenue_data)}개 세그먼트 확인됨")
            print(f"  - 분류에 매출 데이터가 사용될 수 있음")
        else:
            print(f"[X] 매출 데이터 수집 실패")
            print(f"  - Quality Reason: {quality_reason}")
            if override_hit:
                print(f"  - 하지만 Override 정책으로 인해 SEC_CHEM으로 분류됨")
                print(f"  - Override Reason: {override_reason}")
            else:
                print(f"  - 매출 데이터 없이 분류됨 (신뢰도 낮을 수 있음)")
        
        return {
            'ticker': ticker,
            'name': stock.stock_name,
            'has_revenue_data': has_revenue_data,
            'revenue_data': revenue_data,
            'revenue_quality': revenue_quality,
            'quality_reason': quality_reason,
            'override_hit': override_hit,
            'major_sector': major,
            'confidence': conf
        }
        
    finally:
        db.close()

if __name__ == '__main__':
    check_sk_innovation_revenue()

