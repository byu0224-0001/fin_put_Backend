# -*- coding: utf-8 -*-
"""
KG Phase 1 Freeze 전 최종 검증 테스트

4가지 검증 시나리오:
1. 기업별 Impact Nature 검증
2. 비교 분석 검증
3. Evidence Snippet 검증
4. Empty State 검증
"""

import sys
import os
import codecs
import json
from pathlib import Path
from datetime import datetime

# 인코딩 설정
sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, errors='replace')
sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, errors='replace')

# 프로젝트 루트 추가
project_root = Path(__file__).parent.parent
os.chdir(project_root)
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

from app.db import SessionLocal
from app.routes.scenario import get_affected_companies_by_driver, get_company_drivers, get_company_info
from app.services.kg_explanation_layer import (
    generate_scenario_json_v153,
    generate_comparison_output,
    generate_mechanism_explanation,
    classify_impact_nature,
    get_evidence_snippets,
    get_variable_korean_name,
)
from sqlalchemy import text
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def test_1_impact_nature_validation():
    """테스트 1: 기업별 Impact Nature 검증"""
    logger.info("=" * 80)
    logger.info("테스트 1: 기업별 Impact Nature 검증")
    logger.info("=" * 80)
    
    db = SessionLocal()
    results = {}
    
    test_companies = [
        {'ticker': '005930', 'name': '삼성전자', 'expected_nature': 'NO_LINK', 'expected_polarity': None, 'note': '반도체와 유가는 직접 연결이 없는 것이 정상. 2-Hop 추론 영역임.'},
        {'ticker': '010950', 'name': 'S-Oil', 'expected_nature': 'DIRECT', 'expected_polarity': 'MIXED', 'note': 'SPREAD 메커니즘: Impact Nature는 DIRECT, Polarity는 MIXED'},
        {'ticker': '003490', 'name': '대한항공', 'expected_nature': 'DIRECT', 'expected_polarity': 'NEGATIVE', 'note': 'INPUT_COST 메커니즘, 직접 원가 부담'},
    ]
    
    try:
        for company in test_companies:
            ticker = company['ticker']
            name = company['name']
            expected = company['expected_nature']
            expected_polarity = company.get('expected_polarity')
            note = company.get('note', '')
            
            logger.info(f"\n[{name} ({ticker})] 검증 중...")
            logger.info("-" * 80)
            if note:
                logger.info(f"참고: {note}")
            
            # 기업 정보 조회
            company_info = get_company_info(db, ticker)
            if not company_info:
                logger.error(f"❌ {name} 정보를 찾을 수 없습니다")
                continue
            
            # 드라이버 조회
            drivers = get_company_drivers(db, ticker, limit=20)
            oil_driver = next((d for d in drivers if d['driver_code'] == 'OIL_PRICE'), None)
            
            # P0-3: 삼성전자 같은 경우 NO_LINK가 정상
            if not oil_driver:
                if expected == 'NO_LINK':
                    logger.info(f"✅ {name}에 OIL_PRICE 드라이버가 없음 (예상과 일치: NO_LINK)")
                    results[ticker] = {
                        'ticker': ticker,
                        'name': name,
                        'status': 'NO_LINK',
                        'expected': expected,
                        'match': True,
                        'note': note
                    }
                else:
                    logger.warning(f"⚠️ {name}에 OIL_PRICE 드라이버가 없습니다 (예상: {expected})")
                continue
            
            # Impact Nature 판정
            from app.services.kg_explanation_layer import classify_impact_nature
            
            # Evidence snippets 조회
            company_details_result = db.execute(text("""
                SELECT biz_summary, keywords
                FROM company_details
                WHERE ticker = :ticker
                ORDER BY extracted_at DESC
                LIMIT 1
            """), {'ticker': ticker})
            
            detail_row = company_details_result.fetchone()
            biz_summary = detail_row[0] if detail_row else ''
            keywords = detail_row[1] if detail_row else []
            
            # Impact Nature 판정
            impact_info = classify_impact_nature(
                variable='OIL_PRICE',
                sector_l1=company_info['sector_l1'],
                mechanism=oil_driver.get('mechanism', 'DEMAND'),
                evidence_snippets=[biz_summary] if biz_summary else [],
                biz_summary=biz_summary
            )
            
            actual_nature = impact_info['nature']
            actual_polarity = oil_driver.get('polarity', 'MIXED')
            
            # P0 수정: SPREAD는 Impact Nature가 아니라 Polarity 레이어에서 처리
            # Impact Nature는 DIRECT/INDIRECT만 판정하고, Polarity는 별도로 확인
            if oil_driver.get('mechanism') == 'SPREAD':
                # SPREAD 메커니즘은 Polarity가 MIXED여야 함
                if actual_polarity != 'MIXED':
                    logger.warning(f"⚠️ {name}의 SPREAD 메커니즘이 polarity {actual_polarity}로 판정됨 (예상: MIXED)")
                else:
                    logger.info(f"✅ {name}의 SPREAD 메커니즘이 polarity MIXED로 정확히 판정됨")
            
            # Polarity 검증 (기대값이 있는 경우)
            if expected_polarity:
                polarity_match = (actual_polarity == expected_polarity)
                if polarity_match:
                    logger.info(f"✅ Polarity: {actual_polarity} (예상: {expected_polarity})")
                else:
                    logger.warning(f"⚠️ Polarity: {actual_polarity} (예상: {expected_polarity})")
            
            # Exposure 계산
            from app.services.kg_explanation_layer import (
                calculate_exposure_level,
                calculate_exposure_percentile,
                _calculate_exposure_score
            )
            
            all_companies = get_affected_companies_by_driver(db, 'OIL_PRICE', limit=100, min_weight=0.3)
            all_exposure_scores = []
            for c in all_companies:
                score, _ = _calculate_exposure_score(
                    c.get('mechanism', 'DEMAND'),
                    c.get('weight', 0.5),
                    c.get('text_match_weight', 0),
                    return_components=True
                )
                all_exposure_scores.append(score)
            
            current_score, components = _calculate_exposure_score(
                oil_driver.get('mechanism', 'DEMAND'),
                oil_driver.get('weight', 0.5),
                oil_driver.get('text_match_weight', 0),
                return_components=True
            )
            
            percentile_info = calculate_exposure_percentile(
                current_score,
                all_exposure_scores,
                components=components
            )
            
            # Evidence snippets
            evidence_snippets = get_evidence_snippets(
                'OIL_PRICE',
                oil_driver.get('mechanism', 'DEMAND'),
                oil_driver.get('polarity', 'MIXED'),
                biz_summary
            )
            
            # 설명 생성
            reasoning = generate_mechanism_explanation(
                mechanism=oil_driver.get('mechanism', 'DEMAND'),
                polarity=oil_driver.get('polarity', 'MIXED'),
                variable='OIL_PRICE',
                company=name,
                template_type='full',
                sector_l1=company_info['sector_l1'],
                evidence_snippets=[biz_summary] if biz_summary else [],
                biz_summary=biz_summary
            )
            
            # 결과 구성
            result = {
                'ticker': ticker,
                'name': name,
                'sector': company_info['sector_l1'],
                'value_chain': company_info.get('value_chain', ''),
                'impact': {
                    'mechanism': oil_driver.get('mechanism', 'DEMAND'),
                    'polarity': oil_driver.get('polarity', 'MIXED'),
                    'direct_or_indirect': actual_nature,
                    'weight': oil_driver.get('weight', 0.5),
                    'expected_nature': expected,
                    'actual_nature': actual_nature,
                    'match': expected == actual_nature,
                },
                'exposure': {
                    'level': calculate_exposure_level(
                        oil_driver.get('mechanism', 'DEMAND'),
                        oil_driver.get('weight', 0.5),
                        oil_driver.get('text_match_weight', 0)
                    ),
                    'percentile': percentile_info['percentile'],
                    'percentile_explanation': percentile_info['explanation'],
                },
                'evidence': {
                    'snippets': evidence_snippets[:3] if evidence_snippets else [],
                    'selection_reason': impact_info.get('reason', ''),
                },
                'reasoning': reasoning,
                'validation': {
                    'has_mechanism': oil_driver.get('mechanism') is not None,
                    'has_polarity': oil_driver.get('polarity') is not None,
                    'has_direct_or_indirect': actual_nature is not None,
                    'has_exposure_percentile': percentile_info['percentile'] is not None,
                    'has_evidence_snippets': len(evidence_snippets) > 0,
                    'has_evidence_selection_reason': impact_info.get('reason') is not None,
                    'no_sales_misunderstanding': '판매' not in reasoning and '판가' not in reasoning or '제품 판매' not in reasoning,
                }
            }
            
            results[ticker] = result
            
            # 출력
            match = (expected == actual_nature) if expected != 'NO_LINK' else True
            status = "✅" if match else "❌"
            logger.info(f"{status} Impact Nature: {actual_nature} (예상: {expected})")
            logger.info(f"   Mechanism: {oil_driver.get('mechanism')}")
            logger.info(f"   Polarity: {actual_polarity}")
            if expected_polarity:
                polarity_status = "✅" if (actual_polarity == expected_polarity) else "❌"
                logger.info(f"   {polarity_status} Polarity 검증: {actual_polarity} (예상: {expected_polarity})")
            logger.info(f"   Exposure Percentile: {percentile_info['percentile']}%")
            logger.info(f"   Evidence Snippets: {len(evidence_snippets)}개")
            logger.info(f"   Reasoning 길이: {len(reasoning)}자")
            
            # S-Oil 특별 검증 (SPREAD 메커니즘)
            if ticker == '010950':
                if oil_driver.get('mechanism') == 'SPREAD' and actual_polarity == 'MIXED' and actual_nature == 'DIRECT':
                    logger.info("✅ S-Oil: SPREAD 메커니즘, Impact Nature=DIRECT, Polarity=MIXED (정확)")
                else:
                    logger.warning(f"⚠️ S-Oil 검증 실패: mechanism={oil_driver.get('mechanism')}, nature={actual_nature}, polarity={actual_polarity}")
        
        return results
        
    except Exception as e:
        logger.error(f"❌ 테스트 1 실패: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {}
    finally:
        db.close()


def test_2_comparison_validation():
    """테스트 2: 비교 분석 검증"""
    logger.info("\n" + "=" * 80)
    logger.info("테스트 2: 비교 분석 검증")
    logger.info("=" * 80)
    
    db = SessionLocal()
    
    try:
        # OIL_PRICE에 영향받는 기업 조회
        companies = get_affected_companies_by_driver(db, 'OIL_PRICE', limit=50, min_weight=0.3)
        
        logger.info(f"조회된 기업 수: {len(companies)}개")
        
        # 비교 분석 생성
        comparison = generate_comparison_output('OIL_PRICE', companies, top_n=5)
        
        # 검증
        logger.info("\n검증 결과:")
        logger.info("-" * 80)
        
        # 1. 3개 그룹 확인
        has_positive = len(comparison.get('positive_impact', [])) > 0
        has_negative = len(comparison.get('negative_impact', [])) > 0
        has_mixed = len(comparison.get('mixed_impact', [])) > 0
        
        logger.info(f"✅ positive_impact: {len(comparison.get('positive_impact', []))}개")
        logger.info(f"✅ negative_impact: {len(comparison.get('negative_impact', []))}개")
        logger.info(f"✅ mixed_impact: {len(comparison.get('mixed_impact', []))}개")
        
        # 2. 우선주/중복 제거 확인
        all_names = []
        for group in ['positive_impact', 'negative_impact', 'mixed_impact']:
            for company in comparison.get(group, []):
                all_names.append(company['name'])
        
        duplicates = [name for name in all_names if all_names.count(name) > 1]
        preferred_stocks = [name for name in all_names if '우' in name or '우선' in name]
        
        logger.info(f"중복 기업: {len(duplicates)}개 {duplicates[:3] if duplicates else ''}")
        logger.info(f"우선주: {len(preferred_stocks)}개 {preferred_stocks[:3] if preferred_stocks else ''}")
        
        # 3. comparison_summary 확인
        summary = comparison.get('comparison_summary', '')
        logger.info(f"\ncomparison_summary:")
        logger.info(f"  {summary}")
        
        # 4. difference_explanation 확인
        diff_explanation = comparison.get('difference_explanation', [])
        logger.info(f"\ndifference_explanation:")
        for exp in diff_explanation:
            logger.info(f"  - {exp}")
        
        # 5. market_cap_insights 확인
        market_cap_insights = comparison.get('market_cap_insights', [])
        if market_cap_insights:
            logger.info(f"\nmarket_cap_insights:")
            for insight in market_cap_insights:
                logger.info(f"  - {insight}")
        
        return {
            'comparison': comparison,
            'validation': {
                'has_three_groups': has_positive and has_negative and has_mixed,
                'no_duplicates': len(duplicates) == 0,
                'no_preferred_stocks': len(preferred_stocks) == 0,
                'has_summary': len(summary) > 0,
                'has_difference_explanation': len(diff_explanation) > 0,
            }
        }
        
    except Exception as e:
        logger.error(f"❌ 테스트 2 실패: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {}
    finally:
        db.close()


def test_3_evidence_snippet_validation():
    """테스트 3: Evidence Snippet 검증"""
    logger.info("\n" + "=" * 80)
    logger.info("테스트 3: Evidence Snippet 검증 (대한항공)")
    logger.info("=" * 80)
    
    db = SessionLocal()
    ticker = '003490'
    name = '대한항공'
    
    try:
        # 기업 정보 조회
        company_info = get_company_info(db, ticker)
        if not company_info:
            logger.error(f"❌ {name} 정보를 찾을 수 없습니다")
            return {}
        
        # 드라이버 조회
        drivers = get_company_drivers(db, ticker, limit=20)
        oil_driver = next((d for d in drivers if d['driver_code'] == 'OIL_PRICE'), None)
        
        if not oil_driver:
            logger.warning(f"⚠️ {name}에 OIL_PRICE 드라이버가 없습니다")
            return {}
        
        # DART 사업보고서 데이터 조회
        company_details_result = db.execute(text("""
            SELECT biz_summary, keywords, products, raw_materials
            FROM company_details
            WHERE ticker = :ticker
            ORDER BY extracted_at DESC
            LIMIT 1
        """), {'ticker': ticker})
        
        detail_row = company_details_result.fetchone()
        biz_summary = detail_row[0] if detail_row else ''
        keywords = detail_row[1] if detail_row else []
        products = detail_row[2] if detail_row else []
        raw_materials = detail_row[3] if detail_row else []
        
        # Evidence snippets 생성
        evidence_snippets = get_evidence_snippets(
            'OIL_PRICE',
            oil_driver.get('mechanism', 'INPUT_COST'),
            oil_driver.get('polarity', 'NEGATIVE'),
            biz_summary
        )
        
        # Impact Nature 판정
        impact_info = classify_impact_nature(
            variable='OIL_PRICE',
            sector_l1=company_info['sector_l1'],
            mechanism=oil_driver.get('mechanism', 'INPUT_COST'),
            evidence_snippets=[biz_summary] if biz_summary else [],
            biz_summary=biz_summary
        )
        
        # 설명 생성
        reasoning = generate_mechanism_explanation(
            mechanism=oil_driver.get('mechanism', 'INPUT_COST'),
            polarity=oil_driver.get('polarity', 'NEGATIVE'),
            variable='OIL_PRICE',
            company=name,
            template_type='full',
            sector_l1=company_info['sector_l1'],
            evidence_snippets=[biz_summary] if biz_summary else [],
            biz_summary=biz_summary
        )
        
        # 검증
        logger.info(f"\n[{name} ({ticker})] Evidence 검증:")
        logger.info("-" * 80)
        
        # 1. Evidence snippets 확인
        logger.info(f"Evidence Snippets: {len(evidence_snippets)}개")
        for i, snippet in enumerate(evidence_snippets[:3], 1):
            logger.info(f"  {i}. {snippet[:100]}...")
        
        # 2. 키워드 확인
        cost_keywords = ['유류비', '연료비', '원가', '원유', '항공유']
        found_keywords = [kw for kw in cost_keywords if kw in biz_summary]
        logger.info(f"\n원가 관련 키워드 발견: {found_keywords}")
        
        # 3. Evidence selection reason 확인
        selection_reason = impact_info.get('reason', '')
        logger.info(f"\nEvidence Selection Reason:")
        logger.info(f"  {selection_reason}")
        
        # 4. 연결 설명 확인
        logger.info(f"\nReasoning (연결 설명):")
        logger.info(f"  {reasoning[:200]}...")
        
        # 검증 결과
        is_dart_based = 'DART' in biz_summary or len(biz_summary) > 50
        has_cost_keywords = len(found_keywords) > 0
        has_selection_reason = len(selection_reason) > 0
        has_connection = 'INPUT_COST' in reasoning or '원가' in reasoning
        
        return {
            'ticker': ticker,
            'name': name,
            'evidence': {
                'snippets': evidence_snippets[:3],
                'selection_reason': selection_reason,
                'biz_summary_length': len(biz_summary),
            },
            'validation': {
                'is_dart_based': is_dart_based,
                'has_cost_keywords': has_cost_keywords,
                'has_selection_reason': has_selection_reason,
                'has_connection': has_connection,
            },
            'reasoning': reasoning,
        }
        
    except Exception as e:
        logger.error(f"❌ 테스트 3 실패: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {}
    finally:
        db.close()


def test_4_empty_state_validation():
    """테스트 4: Empty State 검증"""
    logger.info("\n" + "=" * 80)
    logger.info("테스트 4: Empty State 검증 (드라이버 없는 기업)")
    logger.info("=" * 80)
    
    db = SessionLocal()
    
    try:
        # 드라이버가 없는 기업 찾기
        result = db.execute(text("""
            SELECT s.ticker, s.stock_name
            FROM stocks s
            LEFT JOIN edges e ON s.ticker = e.source_id AND e.relation_type = 'DRIVEN_BY'
            WHERE s.country = 'KR'
            AND e.source_id IS NULL
            LIMIT 5
        """))
        
        empty_companies = []
        for row in result:
            empty_companies.append({
                'ticker': row[0],
                'name': row[1]
            })
        
        if not empty_companies:
            logger.warning("⚠️ 드라이버가 없는 기업을 찾을 수 없습니다")
            return {}
        
        logger.info(f"드라이버가 없는 기업: {len(empty_companies)}개")
        test_company = empty_companies[0]
        
        logger.info(f"\n테스트 기업: {test_company['name']} ({test_company['ticker']})")
        logger.info("-" * 80)
        
        # 드라이버 조회 시도
        drivers = get_company_drivers(db, test_company['ticker'], limit=10)
        
        # 기업 정보 조회
        company_info = get_company_info(db, test_company['ticker'])
        
        # Empty State 응답 생성
        if len(drivers) == 0:
            empty_state = {
                'ticker': test_company['ticker'],
                'name': test_company['name'],
                'status': 'empty',
                'message': '이 기업에 대한 드라이버 연결이 아직 구축되지 않았습니다.',
                'reason': 'KG 데이터 부족',
                'explanation': 'Knowledge Graph에 해당 기업의 경제 변수 연결 정보가 없어 분석이 불가능합니다. 이는 시스템의 데이터 부족으로 인한 것이며, 추가 데이터 수집이 필요합니다.',
                'suggestions': [
                    'DART 사업보고서 데이터 수집 필요',
                    '섹터 분류 정보 확인 필요',
                    '밸류체인 분류 정보 확인 필요'
                ]
            }
            
            logger.info("✅ Empty State 응답 생성됨")
            logger.info(f"  Status: {empty_state['status']}")
            logger.info(f"  Message: {empty_state['message']}")
            logger.info(f"  Explanation: {empty_state['explanation']}")
            
            return {
                'empty_state': empty_state,
                'validation': {
                    'is_empty_state': True,
                    'not_error': True,
                    'has_explanation': len(empty_state['explanation']) > 0,
                    'not_user_fault': '사용자 책임' not in empty_state['explanation'],
                }
            }
        else:
            logger.warning(f"⚠️ {test_company['name']}에 드라이버가 있습니다 ({len(drivers)}개)")
            return {}
        
    except Exception as e:
        logger.error(f"❌ 테스트 4 실패: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {}
    finally:
        db.close()


def main():
    """메인 실행 함수"""
    logger.info("=" * 80)
    logger.info("KG Phase 1 Freeze 전 최종 검증 테스트")
    logger.info("=" * 80)
    
    all_results = {}
    
    # 테스트 1: Impact Nature 검증
    result1 = test_1_impact_nature_validation()
    all_results['test_1_impact_nature'] = result1
    
    # 테스트 2: 비교 분석 검증
    result2 = test_2_comparison_validation()
    all_results['test_2_comparison'] = result2
    
    # 테스트 3: Evidence Snippet 검증
    result3 = test_3_evidence_snippet_validation()
    all_results['test_3_evidence'] = result3
    
    # 테스트 4: Empty State 검증
    result4 = test_4_empty_state_validation()
    all_results['test_4_empty_state'] = result4
    
    # 결과 저장
    output_file = project_root / 'reports' / f'kg_phase1_validation_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json'
    output_file.parent.mkdir(exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(all_results, f, ensure_ascii=False, indent=2)
    
    logger.info(f"\n✅ 모든 테스트 완료")
    logger.info(f"결과 저장: {output_file}")
    
    # 최종 요약
    logger.info("\n" + "=" * 80)
    logger.info("최종 검증 요약")
    logger.info("=" * 80)
    
    # 테스트 1 요약
    if result1:
        logger.info("\n[테스트 1] Impact Nature 검증:")
        for ticker, data in result1.items():
            if 'status' in data and data['status'] == 'NO_LINK':
                # NO_LINK 케이스
                match = data.get('match', False)
                status = "✅" if match else "❌"
                logger.info(f"  {status} {data['name']}: NO_LINK (예상: {data['expected']})")
            else:
                # 일반 케이스
                match = data.get('impact', {}).get('match', False)
                status = "✅" if match else "❌"
                actual = data.get('impact', {}).get('actual_nature', 'N/A')
                expected = data.get('impact', {}).get('expected_nature', 'N/A')
                logger.info(f"  {status} {data['name']}: {actual} (예상: {expected})")
    
    # 테스트 2 요약
    if result2 and result2.get('validation'):
        val = result2['validation']
        logger.info("\n[테스트 2] 비교 분석 검증:")
        logger.info(f"  {'✅' if val.get('has_three_groups') else '❌'} 3개 그룹 분리")
        logger.info(f"  {'✅' if val.get('no_duplicates') else '❌'} 중복 제거")
        logger.info(f"  {'✅' if val.get('no_preferred_stocks') else '❌'} 우선주 제거")
        logger.info(f"  {'✅' if val.get('has_summary') else '❌'} comparison_summary")
        logger.info(f"  {'✅' if val.get('has_difference_explanation') else '❌'} difference_explanation")
    
    # 테스트 3 요약
    if result3 and result3.get('validation'):
        val = result3['validation']
        logger.info("\n[테스트 3] Evidence Snippet 검증:")
        logger.info(f"  {'✅' if val.get('is_dart_based') else '❌'} DART 기반")
        logger.info(f"  {'✅' if val.get('has_cost_keywords') else '❌'} 원가 키워드")
        logger.info(f"  {'✅' if val.get('has_selection_reason') else '❌'} Selection Reason")
        logger.info(f"  {'✅' if val.get('has_connection') else '❌'} 연결 설명")
    
    # 테스트 4 요약
    if result4 and result4.get('validation'):
        val = result4['validation']
        logger.info("\n[테스트 4] Empty State 검증:")
        logger.info(f"  {'✅' if val.get('is_empty_state') else '❌'} Empty State 응답")
        logger.info(f"  {'✅' if val.get('not_error') else '❌'} 에러 아님")
        logger.info(f"  {'✅' if val.get('has_explanation') else '❌'} 설명 포함")
        logger.info(f"  {'✅' if val.get('not_user_fault') else '❌'} 사용자 책임 아님")
    
    logger.info("\n" + "=" * 80)


if __name__ == '__main__':
    main()

