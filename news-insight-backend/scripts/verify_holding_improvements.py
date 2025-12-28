"""
P0 지주회사 개선 검증 스크립트

피드백 기반 검증:
- R1: SK이노 override_hit 확인
- R2: Top100 오탐 폭발 확인
- R3: sector_evidence 누락 확인
- R4: detect_holding_company 시그니처 전파 확인
"""
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from app.db import SessionLocal
from app.models.stock import Stock
from app.models.company_detail import CompanyDetail
from app.models.investor_sector import InvestorSector
from app.services.sector_classifier import classify_sector_rule_based
from sqlalchemy import text

def test_r1_override_hit():
    """R1: SK이노 override_hit 확인"""
    print("="*80)
    print("R1: SK이노 override_hit 확인")
    print("="*80)
    
    db = SessionLocal()
    ticker = '096770'
    stock = db.query(Stock).filter(Stock.ticker == ticker).first()
    company_detail = db.query(CompanyDetail).filter(CompanyDetail.ticker == ticker).first()
    
    if not stock or not company_detail:
        print(f"⚠️  {ticker}: 데이터 없음")
        db.close()
        return False
    
    major, sub, vc, conf, boosting_log = classify_sector_rule_based(company_detail, stock.stock_name, ticker=ticker)
    meta = boosting_log.get('classification_meta', {}) if boosting_log else {}
    
    override_hit = meta.get('override_hit', False)
    entity_type_evidence = meta.get('entity_type_evidence', {})
    evidence_override_hit = entity_type_evidence.get('override_hit', False) if isinstance(entity_type_evidence, dict) else False
    
    print(f"\n{ticker} ({stock.stock_name}):")
    print(f"  - Entity Type: {meta.get('entity_type', 'UNKNOWN')}")
    print(f"  - override_hit (meta): {override_hit}")
    print(f"  - override_hit (evidence): {evidence_override_hit}")
    
    if override_hit or evidence_override_hit:
        print(f"  [PASS] override_hit이 정상적으로 설정됨")
        db.close()
        return True
    else:
        print(f"  [FAIL] override_hit이 설정되지 않음 (DB 재분류 필요)")
        print(f"  - 참고: 새로 분류하면 override_hit이 설정됨")
        db.close()
        return False

def test_r2_top100_false_positives():
    """R2: Top100 오탐 폭발 확인"""
    print("\n" + "="*80)
    print("R2: Top100 오탐 폭발 확인")
    print("="*80)
    
    db = SessionLocal()
    
    # 시총 Top100 조회
    result = db.execute(text("""
        SELECT s.ticker, s.stock_name, s.market_cap
        FROM stocks s
        WHERE s.market_cap IS NOT NULL
        ORDER BY s.market_cap DESC
        LIMIT 100
    """))
    
    top100 = [(row[0], row[1], row[2]) for row in result]
    
    # entity_type 분포 확인
    entity_type_counts = {}
    false_positives = []  # 홀딩/지주 없는데 BIZ_HOLDCO
    
    holding_keywords = ['지주', '홀딩스', '홀딩', 'Holdings']
    
    # 🆕 거대 제조사 확인 (삼성전자, 현대차, LG전자)
    major_manufacturers = {
        '005930': '삼성전자',
        '005380': '현대자동차',
        '066570': 'LG전자'
    }
    major_manufacturer_results = {}
    
    for ticker, stock_name, market_cap in top100:
        sector = db.query(InvestorSector).filter(InvestorSector.ticker == ticker).first()
        # classification_meta는 boosting_log 안에 저장됨
        boosting_log = sector.boosting_log if sector else None
        meta = boosting_log.get('classification_meta', {}) if boosting_log and isinstance(boosting_log, dict) else {}
        if meta:
            entity_type = meta.get('entity_type', 'REGULAR')
            
            entity_type_counts[entity_type] = entity_type_counts.get(entity_type, 0) + 1
            
            # 오탐 후보: 홀딩/지주 없는데 BIZ_HOLDCO
            if entity_type in ('BIZ_HOLDCO', 'HOLDCO') and not any(kw in stock_name for kw in holding_keywords):
                false_positives.append((ticker, stock_name, entity_type, market_cap))
            
            # 🆕 거대 제조사 확인
            if ticker in major_manufacturers:
                major_manufacturer_results[ticker] = {
                    'name': stock_name,
                    'entity_type': entity_type,
                    'major_sector': sector.major_sector if sector else None,
                    'primary_source': meta.get('primary_sector_source', 'UNKNOWN')
                }
    
    print(f"\n[Entity Type 분포 (Top100)]")
    for entity_type, count in sorted(entity_type_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {entity_type}: {count}개")
    
    # 🆕 거대 제조사 확인 결과 출력
    print(f"\n[거대 제조사 Entity Type 확인]")
    for ticker, info in major_manufacturer_results.items():
        name = info['name']
        entity_type = info['entity_type']
        major_sector = info['major_sector']
        primary_source = info['primary_source']
        
        status = "[OK]" if entity_type == 'REGULAR' else "[WARN]"
        print(f"  {status} {ticker} ({name}):")
        print(f"    - Entity Type: {entity_type}")
        print(f"    - Major Sector: {major_sector}")
        print(f"    - Primary Source: {primary_source}")
        
        if entity_type in ('BIZ_HOLDCO', 'HOLDCO'):
            print(f"    - [주의] BIZ_HOLDCO로 분류됨 - MAJOR_COMPANY_SECTORS에 entity_type=REGULAR 강제 필요")
    
    print(f"\n[오탐 후보 (홀딩/지주 없는데 BIZ_HOLDCO/HOLDCO)]")
    if false_positives:
        print(f"  [WARN] {len(false_positives)}개 발견:")
        for ticker, name, entity_type, market_cap in false_positives[:20]:  # 상위 20개만
            print(f"    - {ticker} ({name}): {entity_type}")
        if len(false_positives) > 20:
            print(f"    ... 외 {len(false_positives) - 20}개")
        
        if len(false_positives) >= 10:
            print(f"\n  [FAIL] 오탐 후보가 10개 이상 (위험 신호)")
            db.close()
            return False
        else:
            print(f"\n  [WARN] 오탐 후보 {len(false_positives)}개 (eye-check 필요)")
    else:
        print(f"\n  [PASS] 오탐 후보 없음")
    
    db.close()
    return len(false_positives) < 10

def test_r3_sector_evidence_coverage():
    """R3: HOLD 케이스에서 sector_evidence 누락 확인"""
    print("\n" + "="*80)
    print("R3: HOLD 케이스에서 sector_evidence 누락 확인")
    print("="*80)
    
    db = SessionLocal()
    
    # confidence가 HOLD로 시작하는 레코드 조회
    # classification_meta는 boosting_log 안에 저장됨
    result = db.execute(text("""
        SELECT ticker, confidence, boosting_log
        FROM investor_sector
        WHERE confidence LIKE 'HOLD%'
        LIMIT 100
    """))
    
    hold_records = []
    missing_evidence = []
    
    for row in result:
        ticker, confidence, boosting_log = row[0], row[1], row[2]
        # boosting_log에서 classification_meta 추출
        meta = {}
        if boosting_log and isinstance(boosting_log, dict):
            meta = boosting_log.get('classification_meta', {})
        hold_records.append((ticker, confidence, meta))
        
        if meta:
            sector_evidence = meta.get('sector_evidence', {})
            if not sector_evidence or (isinstance(sector_evidence, dict) and len(sector_evidence) == 0):
                missing_evidence.append(ticker)
    
    print(f"\n[HOLD 레코드 분석]")
    print(f"  - 총 HOLD 레코드: {len(hold_records)}개")
    print(f"  - sector_evidence 누락: {len(missing_evidence)}개")
    
    if missing_evidence:
        print(f"  ⚠️  누락된 ticker (최대 10개): {missing_evidence[:10]}")
        if len(missing_evidence) > 0:
            missing_ratio = len(missing_evidence) / len(hold_records) * 100
            print(f"  - 누락 비율: {missing_ratio:.1f}%")
            if missing_ratio > 5:
                print(f"  [FAIL] 누락 비율이 5% 초과")
                db.close()
                return False
            else:
                print(f"  [WARN] 일부 누락 있음 (확인 필요)")
    else:
        print(f"  [PASS] sector_evidence 누락 없음")
    
    db.close()
    return len(missing_evidence) == 0

def test_r4_signature_propagation():
    """R4: detect_holding_company 시그니처 전파 확인"""
    print("\n" + "="*80)
    print("R4: detect_holding_company 시그니처 전파 확인")
    print("="*80)
    
    # 간단한 런타임 테스트
    from app.services.krx_sector_filter import detect_holding_company
    from app.models.company_detail import CompanyDetail
    
    try:
        # company_detail 없이 호출 (기본값 None)
        result = detect_holding_company(
            company_name="테스트",
            industry_raw=None,
            keywords=None,
            products=None,
            revenue_by_segment=None,
            company_detail=None
        )
        
        if len(result) == 4:  # (is_holding, confidence, reason, holding_type)
            print(f"  [PASS] 시그니처 정상 (반환값 4개)")
            print(f"    - 결과: {result}")
            return True
        else:
            print(f"  [FAIL] 시그니처 불일치 (반환값 {len(result)}개, 예상 4개)")
            return False
    except Exception as e:
        print(f"  [FAIL] 시그니처 오류 - {e}")
        return False

def test_sk_innovation_override_on_off():
    """A) SK이노 override ON/OFF A/B 테스트"""
    print("\n" + "="*80)
    print("A) SK이노 override ON/OFF A/B 테스트")
    print("="*80)
    
    db = SessionLocal()
    ticker = '096770'
    stock = db.query(Stock).filter(Stock.ticker == ticker).first()
    company_detail = db.query(CompanyDetail).filter(CompanyDetail.ticker == ticker).first()
    
    if not stock or not company_detail:
        print(f"⚠️  {ticker}: 데이터 없음")
        db.close()
        return
    
    # ON: 현재 상태 (override 적용)
    print(f"\n[ON: override 적용]")
    major, sub, vc, conf, boosting_log = classify_sector_rule_based(company_detail, stock.stock_name, ticker=ticker)
    meta = boosting_log.get('classification_meta', {}) if boosting_log else {}
    print(f"  - Entity Type: {meta.get('entity_type', 'UNKNOWN')}")
    print(f"  - override_hit: {meta.get('override_hit', False)}")
    
    # OFF: override 주석 처리 후 테스트는 수동으로 해야 함
    print(f"\n[OFF: override 비활성화 테스트]")
    print(f"  [WARN] 수동 테스트 필요: entity_type_classifier.py에서 SK이노 특별 처리 주석 처리 후 재실행")
    print(f"  - 예상 결과: consolidated_structure_score로도 BIZ_HOLDCO가 잡히는지 확인")
    
    db.close()

def test_c_guardrail_conservatism():
    """C) 가드레일 과도 보수성 확인 (consolidated_score 높은데 REGULAR 케이스)"""
    print("\n" + "="*80)
    print("C) 가드레일 과도 보수성 확인")
    print("="*80)
    
    db = SessionLocal()
    
    # Top200 조회
    result = db.execute(text("""
        SELECT s.ticker, s.stock_name, s.market_cap
        FROM stocks s
        WHERE s.market_cap IS NOT NULL
        ORDER BY s.market_cap DESC
        LIMIT 200
    """))
    
    top200 = [(row[0], row[1], row[2]) for row in result]
    
    missed_cases = []  # consolidated_score 높은데 REGULAR
    
    for ticker, stock_name, market_cap in top200:
        sector = db.query(InvestorSector).filter(InvestorSector.ticker == ticker).first()
        # classification_meta는 boosting_log 안에 저장됨
        boosting_log = sector.boosting_log if sector else None
        meta = boosting_log.get('classification_meta', {}) if boosting_log and isinstance(boosting_log, dict) else {}
        if meta:
            entity_type = meta.get('entity_type', 'REGULAR')
            entity_type_evidence = meta.get('entity_type_evidence', {})
            
            # consolidated_structure_score 확인
            if isinstance(entity_type_evidence, dict):
                consolidated_score = entity_type_evidence.get('consolidated_structure_score', 0.0)
                if isinstance(consolidated_score, (int, float)) and consolidated_score >= 0.3:
                    if entity_type == 'REGULAR':
                        missed_cases.append({
                            'ticker': ticker,
                            'name': stock_name,
                            'consolidated_score': consolidated_score,
                            'entity_type': entity_type,
                            'market_cap': market_cap
                        })
    
    print(f"\n[놓친 후보 (consolidated_score >= 0.3인데 REGULAR)]")
    if missed_cases:
        print(f"  [WARN] {len(missed_cases)}개 발견:")
        for case in missed_cases[:20]:  # 상위 20개만
            print(f"    - {case['ticker']} ({case['name']}): consolidated_score={case['consolidated_score']:.2f}")
        if len(missed_cases) > 20:
            print(f"    ... 외 {len(missed_cases) - 20}개")
    else:
        print(f"  [PASS] 놓친 후보 없음")
    
    db.close()
    return len(missed_cases) < 10

def test_top20_consolidated_regression():
    """No-Go 기준: Top20에서 REGULAR인데 consolidated_score >= 0.3인 케이스 3개 이상"""
    print("\n" + "="*80)
    print("No-Go 기준: Top20 consolidated_score >= 0.3인 REGULAR 확인")
    print("="*80)
    
    db = SessionLocal()
    
    # Top20 조회
    result = db.execute(text("""
        SELECT s.ticker, s.stock_name, s.market_cap
        FROM stocks s
        WHERE s.market_cap IS NOT NULL
        ORDER BY s.market_cap DESC
        LIMIT 20
    """))
    
    top20 = [(row[0], row[1], row[2]) for row in result]
    
    regression_cases = []
    
    for ticker, stock_name, market_cap in top20:
        sector = db.query(InvestorSector).filter(InvestorSector.ticker == ticker).first()
        # classification_meta는 boosting_log 안에 저장됨
        boosting_log = sector.boosting_log if sector else None
        meta = boosting_log.get('classification_meta', {}) if boosting_log and isinstance(boosting_log, dict) else {}
        if meta:
            entity_type = meta.get('entity_type', 'REGULAR')
            entity_type_evidence = meta.get('entity_type_evidence', {})
            
            if entity_type == 'REGULAR' and isinstance(entity_type_evidence, dict):
                consolidated_score = entity_type_evidence.get('consolidated_structure_score', 0.0)
                if isinstance(consolidated_score, (int, float)) and consolidated_score >= 0.3:
                    regression_cases.append({
                        'ticker': ticker,
                        'name': stock_name,
                        'consolidated_score': consolidated_score,
                        'market_cap': market_cap
                    })
    
    print(f"\n[Top20 REGULAR인데 consolidated_score >= 0.3]")
    if regression_cases:
        print(f"  [WARN] {len(regression_cases)}개 발견:")
        for case in regression_cases:
            print(f"    - {case['ticker']} ({case['name']}): consolidated_score={case['consolidated_score']:.2f}")
        
        if len(regression_cases) >= 3:
            print(f"\n  [FAIL] Top20에서 {len(regression_cases)}개 발견 (No-Go 조건)")
            db.close()
            return False
    else:
        print(f"  [PASS] 해당 케이스 없음")
    
    db.close()
    return len(regression_cases) < 3

def test_override_hit_ratio():
    """override_hit 비율 게이트 (Top200 + 전체) + HOLD 비율 함께 확인"""
    print("\n" + "="*80)
    print("override_hit 비율 게이트 (Top200 + 전체) + HOLD 비율")
    print("="*80)
    
    db = SessionLocal()
    
    # Top200 조회
    result = db.execute(text("""
        SELECT s.ticker, s.stock_name, s.market_cap
        FROM stocks s
        WHERE s.market_cap IS NOT NULL
        ORDER BY s.market_cap DESC
        LIMIT 200
    """))
    
    top200_tickers = {row[0] for row in result}
    
    # 전체 조회
    all_sectors = db.query(InvestorSector).filter(
        InvestorSector.is_primary == True
    ).all()
    
    top200_override_count = 0
    top200_hold_count = 0
    top200_total = 0
    all_override_count = 0
    all_hold_count = 0
    all_total = 0
    
    for sector in all_sectors:
        confidence = sector.confidence or ''
        boosting_log = sector.boosting_log or {}
        classification_meta = boosting_log.get('classification_meta', {})
        override_hit = classification_meta.get('override_hit', False)
        
        all_total += 1
        if override_hit:
            all_override_count += 1
        if confidence.startswith('HOLD'):
            all_hold_count += 1
        
        if sector.ticker in top200_tickers:
            top200_total += 1
            if override_hit:
                top200_override_count += 1
            if confidence.startswith('HOLD'):
                top200_hold_count += 1
    
    top200_override_ratio = (top200_override_count / top200_total * 100) if top200_total > 0 else 0
    top200_hold_ratio = (top200_hold_count / top200_total * 100) if top200_total > 0 else 0
    all_override_ratio = (all_override_count / all_total * 100) if all_total > 0 else 0
    all_hold_ratio = (all_hold_count / all_total * 100) if all_total > 0 else 0
    
    print(f"\n[Top200]")
    print(f"  Override Hit: {top200_override_count}개 / {top200_total}개 ({top200_override_ratio:.1f}%)")
    print(f"  HOLD: {top200_hold_count}개 / {top200_total}개 ({top200_hold_ratio:.1f}%)")
    print(f"  목표: Override ≤ 5%, HOLD ≤ 10%")
    
    print(f"\n[전체]")
    print(f"  Override Hit: {all_override_count}개 / {all_total}개 ({all_override_ratio:.1f}%)")
    print(f"  HOLD: {all_hold_count}개 / {all_total}개 ({all_hold_ratio:.1f}%)")
    print(f"  참고: 전체는 Override ≤ 10%, HOLD는 제한 없음")
    
    # 게이트 판정
    top200_override_pass = top200_override_ratio <= 5.0
    top200_hold_pass = top200_hold_ratio <= 10.0
    all_override_pass = all_override_ratio <= 10.0
    
    # 🆕 해석: Override 0%가 좋은 신호인지 확인
    if top200_override_ratio == 0.0 and top200_hold_ratio > 10.0:
        print(f"\n  [WARN] Top200 Override 0%이지만 HOLD 비율이 높음 ({top200_hold_ratio:.1f}%)")
        print(f"  -> 해석: Override가 없어서 좋은 게 아니라, HOLD로 방치된 상태일 수 있음")
        print(f"  -> 조치: Top200 재수집 필요")
    elif top200_override_ratio == 0.0 and top200_hold_ratio <= 10.0:
        print(f"\n  [OK] Top200 Override 0% + HOLD {top200_hold_ratio:.1f}%")
        print(f"  -> 해석: 대형주가 정상 데이터/정상 룰로 커버된 상태")
    
    if top200_override_pass:
        print(f"\n  [PASS] Top200 override 비율: {top200_override_ratio:.1f}% ≤ 5%")
    else:
        print(f"\n  [WARN] Top200 override 비율: {top200_override_ratio:.1f}% > 5% (목표 초과)")
    
    if top200_hold_pass:
        print(f"  [PASS] Top200 HOLD 비율: {top200_hold_ratio:.1f}% ≤ 10%")
    else:
        print(f"  [WARN] Top200 HOLD 비율: {top200_hold_ratio:.1f}% > 10% (목표 초과)")
    
    if all_override_pass:
        print(f"  [PASS] 전체 override 비율: {all_override_ratio:.1f}% ≤ 10%")
    else:
        print(f"  [WARN] 전체 override 비율: {all_override_ratio:.1f}% > 10% (높음)")
    
    db.close()
    return top200_override_pass and top200_hold_pass

def main():
    """모든 검증 실행"""
    print("="*80)
    print("P0 지주회사 개선 검증 스크립트 (3개 버킷 요약)")
    print("="*80)
    
    results = {}
    
    # R1-R4 검증
    results['R1'] = test_r1_override_hit()
    results['R2'] = test_r2_top100_false_positives()
    results['R3'] = test_r3_sector_evidence_coverage()
    results['R4'] = test_r4_signature_propagation()
    
    # 추가 검증
    results['C'] = test_c_guardrail_conservatism()
    results['NoGo'] = test_top20_consolidated_regression()
    
    # 🆕 override_hit 비율 게이트
    results['OverrideRatio'] = test_override_hit_ratio()
    
    # A) SK이노 override 테스트
    test_sk_innovation_override_on_off()
    
    # 3개 버킷 요약
    print("\n" + "="*80)
    print("3개 버킷 요약")
    print("="*80)
    print("  (1) 오탐 후보: 홀딩/지주 없는데 BIZ_HOLDCO (R2에서 확인)")
    print("  (2) 놓친 후보: consolidated_score 높은데 REGULAR (C에서 확인)")
    print("  (3) evidence 누락: HOLD 케이스에서 sector_evidence 없음 (R3에서 확인)")
    
    # 최종 결과
    print("\n" + "="*80)
    print("최종 검증 결과")
    print("="*80)
    for test_name, passed in results.items():
        status = "[PASS]" if passed else "[FAIL]"
        print(f"  {test_name}: {status}")
    
    all_passed = all(results.values())
    if all_passed:
        print(f"\n[PASS] 모든 검증 통과")
    else:
        print(f"\n[WARN] 일부 검증 실패 - 확인 필요")

if __name__ == '__main__':
    main()

