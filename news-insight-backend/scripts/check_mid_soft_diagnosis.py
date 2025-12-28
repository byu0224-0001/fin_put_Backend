#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MID_SOFT Diagnosis Script (Check 1~4)

Check 1: MID_SOFT Top-30 Partiality Analysis
Check 2: MID_SOFT Top2 Distribution
Check 3: MID_SOFT Exclusion Redistribution (Virtual Experiment)
Check 4: Pure SW Only Distribution
"""

import os
import sys
import codecs

# Windows UTF-8 encoding fix
if sys.platform == 'win32':
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, errors='replace')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, errors='replace')

from pathlib import Path

project_root = Path(__file__).parent.parent
os.chdir(project_root)
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

import numpy as np
import ast
from sqlalchemy import text
from collections import Counter

from app.db import get_db
from app.services.value_chain_classifier_embedding import (
    get_centroid_anchors,
    get_value_chain_reference_embeddings,
    compute_cosine_similarity,
    clear_anchor_cache
)

PRIORITY_VC_CODES = ['UPSTREAM', 'MID_HARD', 'MID_SOFT', 'DOWN_BIZ', 'DOWN_SERVICE']
CENTROID_WEIGHT = 0.6
TEXT_WEIGHT = 0.4


def get_all_company_embeddings(db):
    """모든 기업 임베딩 로드"""
    result = db.execute(text("""
        SELECT ce.ticker, ce.embedding_vector, s.stock_name, 
               i.sector_l1, i.sector_l2, i.value_chain, i.value_chain_detail,
               i.value_chain_confidence
        FROM company_embeddings ce
        LEFT JOIN stocks s ON ce.ticker = s.ticker
        LEFT JOIN investor_sector i ON ce.ticker = i.ticker AND i.is_primary = true
        WHERE ce.embedding_vector IS NOT NULL
    """))
    
    companies = []
    for row in result:
        try:
            if isinstance(row[1], str):
                vec_list = ast.literal_eval(row[1])
                embedding = np.array(vec_list, dtype=np.float32)
            else:
                continue
                
            companies.append({
                'ticker': row[0],
                'embedding': embedding,
                'name': row[2] or 'Unknown',
                'sector_l1': row[3],
                'sector_l2': row[4],
                'value_chain': row[5],
                'value_chain_detail': row[6],
                'confidence': float(row[7]) if row[7] else 0.0
            })
        except:
            continue
    
    return companies


def calculate_hybrid_scores(company_emb, centroid_anchors, text_anchors, vc_codes):
    """하이브리드 스코어 계산"""
    scores = {}
    for vc in vc_codes:
        if vc not in centroid_anchors:
            continue
        centroid_sim = compute_cosine_similarity(company_emb, centroid_anchors[vc])
        if text_anchors and vc in text_anchors:
            text_sim = compute_cosine_similarity(company_emb, text_anchors[vc])
            scores[vc] = (centroid_sim * CENTROID_WEIGHT) + (text_sim * TEXT_WEIGHT)
        else:
            scores[vc] = centroid_sim
    return scores


def check_1_mid_soft_partiality(companies, centroid_anchors, text_anchors):
    """
    Check 1: MID_SOFT Top-30 기업 "부분성" 분석
    이 기업들이 순수 MID_SOFT인지, 아니면 혼합인지 확인
    """
    print("=" * 80)
    print("CHECK 1: MID_SOFT Top-30 기업 '부분성' 분석")
    print("=" * 80)
    
    # MID_SOFT로 분류된 기업들
    mid_soft_companies = [c for c in companies if c['value_chain'] == 'MID_SOFT']
    mid_soft_companies.sort(key=lambda x: x['confidence'], reverse=True)
    
    print(f"\nMID_SOFT 기업 수: {len(mid_soft_companies)}")
    print(f"\n{'Ticker':10} {'Name':18} {'Conf':>8} {'Top1':>10} {'Top2':>10} {'Top3':>10} {'Gap1-2':>8} {'판정':10}")
    print("-" * 95)
    
    pure_count = 0
    mixed_count = 0
    
    for c in mid_soft_companies[:30]:
        scores = calculate_hybrid_scores(c['embedding'], centroid_anchors, text_anchors, PRIORITY_VC_CODES)
        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        
        top1_vc, top1_score = sorted_scores[0]
        top2_vc, top2_score = sorted_scores[1]
        top3_vc, top3_score = sorted_scores[2]
        gap = top1_score - top2_score
        
        # 판정: gap < 0.03이면 "혼합", 아니면 "순수"
        if gap < 0.03:
            judgment = "혼합"
            mixed_count += 1
        else:
            judgment = "순수"
            pure_count += 1
        
        name = c['name'][:16] if c['name'] else 'N/A'
        print(f"{c['ticker']:10} {name:18} {c['confidence']:>8.3f} "
              f"{top1_vc:>10} {top2_vc:>10} {top3_vc:>10} {gap:>8.3f} {judgment:10}")
    
    print("-" * 95)
    print(f"\n[결과] 순수: {pure_count}개 ({pure_count/30*100:.1f}%), 혼합: {mixed_count}개 ({mixed_count/30*100:.1f}%)")
    
    if mixed_count > pure_count:
        print("[해석] MID_SOFT 기업 대부분이 '경계 기업'입니다. 이는 오분류가 아니라 정직한 반영입니다.")
    else:
        print("[해석] MID_SOFT 기업 대부분이 '순수 기업'입니다. 분류가 명확합니다.")
    
    return pure_count, mixed_count


def check_2_mid_soft_top2_distribution(companies, centroid_anchors, text_anchors):
    """
    Check 2: MID_SOFT Top2 분포 확인
    MID_SOFT 기업들의 Top2가 어디로 가는지 분석
    """
    print("\n" + "=" * 80)
    print("CHECK 2: MID_SOFT 기업들의 Top2 분포")
    print("=" * 80)
    
    mid_soft_companies = [c for c in companies if c['value_chain'] == 'MID_SOFT']
    
    top2_counter = Counter()
    
    for c in mid_soft_companies:
        scores = calculate_hybrid_scores(c['embedding'], centroid_anchors, text_anchors, PRIORITY_VC_CODES)
        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        top2_vc = sorted_scores[1][0]
        top2_counter[top2_vc] += 1
    
    print(f"\nMID_SOFT 기업 {len(mid_soft_companies)}개의 Top2 분포:")
    print("-" * 40)
    for vc, count in top2_counter.most_common():
        pct = count / len(mid_soft_companies) * 100
        bar = "#" * int(pct / 2)
        print(f"  {vc:15} {count:>5} ({pct:>5.1f}%) {bar}")
    
    # 해석
    print("\n[해석]")
    most_common_top2 = top2_counter.most_common(1)[0][0]
    if most_common_top2 == 'MID_HARD':
        print("  → MID_SOFT 기업 대부분이 '설계형 제조' 경계에 있습니다.")
    elif most_common_top2 == 'DOWN_BIZ':
        print("  → MID_SOFT 기업 대부분이 '솔루션 유통' 경계에 있습니다.")
    elif most_common_top2 == 'DOWN_SERVICE':
        print("  → MID_SOFT 기업 대부분이 'IT 서비스' 경계에 있습니다.")
    
    return dict(top2_counter)


def check_3_exclude_mid_soft_redistribution(companies, centroid_anchors, text_anchors):
    """
    Check 3: MID_SOFT 제외 후 재분포 (가상 실험)
    MID_SOFT를 빼고 나머지 4개로만 분류했을 때 분포
    """
    print("\n" + "=" * 80)
    print("CHECK 3: MID_SOFT 제외 후 재분포 (가상 실험)")
    print("=" * 80)
    
    # 현재 MID_SOFT로 분류된 기업들
    mid_soft_companies = [c for c in companies if c['value_chain'] == 'MID_SOFT']
    
    # MID_SOFT 제외한 4개 VC로만 재분류
    vc_codes_no_mid_soft = ['UPSTREAM', 'MID_HARD', 'DOWN_BIZ', 'DOWN_SERVICE']
    
    redistribution = Counter()
    
    for c in mid_soft_companies:
        scores = calculate_hybrid_scores(c['embedding'], centroid_anchors, text_anchors, vc_codes_no_mid_soft)
        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        new_top1 = sorted_scores[0][0]
        redistribution[new_top1] += 1
    
    print(f"\nMID_SOFT 기업 {len(mid_soft_companies)}개를 4개 VC로 재분류:")
    print("-" * 40)
    for vc, count in redistribution.most_common():
        pct = count / len(mid_soft_companies) * 100
        bar = "#" * int(pct / 2)
        print(f"  {vc:15} {count:>5} ({pct:>5.1f}%) {bar}")
    
    # 해석
    print("\n[해석]")
    most_common = redistribution.most_common(1)[0][0]
    print(f"  → MID_SOFT가 없으면 대부분이 {most_common}로 흡수됩니다.")
    
    if most_common == 'MID_HARD':
        print("  → 이는 MID_SOFT가 '설계/기술 중심 제조'를 흡수하고 있음을 의미합니다.")
    elif most_common == 'DOWN_BIZ':
        print("  → 이는 MID_SOFT가 '솔루션 유통/판매'를 흡수하고 있음을 의미합니다.")
    elif most_common == 'DOWN_SERVICE':
        print("  → 이는 MID_SOFT가 'IT 서비스/플랫폼'을 흡수하고 있음을 의미합니다.")
    
    return dict(redistribution)


def check_4_pure_sw_only_distribution(db, companies, text_anchors):
    """
    Check 4: 순수 MID_SOFT (SW Only) Golden Set으로 재시뮬레이션
    """
    print("\n" + "=" * 80)
    print("CHECK 4: 순수 SW Only Golden Set으로 재시뮬레이션")
    print("=" * 80)
    
    # 순수 SW 기업만으로 구성된 Golden Set
    pure_sw_golden_set = [
        '053800',  # 안랩: 보안 SW
        '012510',  # 더존비즈온: ERP SW
        '030520',  # 한글과컴퓨터: 오피스 SW
        '035760',  # CJ ENM: 콘텐츠 IP (참고용)
    ]
    
    print(f"\n[Pure SW Golden Set]: {pure_sw_golden_set}")
    
    # Pure SW Golden Set으로 새 Centroid 계산
    pure_sw_embeddings = []
    found_tickers = []
    
    for ticker in pure_sw_golden_set:
        result = db.execute(text("""
            SELECT embedding_vector FROM company_embeddings WHERE ticker = :ticker
        """), {'ticker': ticker})
        row = result.fetchone()
        if row and row[0]:
            try:
                if isinstance(row[0], str):
                    vec_list = ast.literal_eval(row[0])
                    embedding = np.array(vec_list, dtype=np.float32)
                    pure_sw_embeddings.append(embedding)
                    found_tickers.append(ticker)
            except:
                continue
    
    if len(pure_sw_embeddings) < 2:
        print("[경고] Pure SW 기업 임베딩이 부족합니다.")
        return None
    
    print(f"[발견된 기업]: {found_tickers} ({len(found_tickers)}개)")
    
    # Pure SW Centroid 계산
    pure_sw_centroid = np.mean(pure_sw_embeddings, axis=0)
    pure_sw_centroid = pure_sw_centroid / np.linalg.norm(pure_sw_centroid)
    
    # 기존 Centroid 가져오기
    clear_anchor_cache()
    centroid_anchors = get_centroid_anchors(db, force_regenerate=True)
    
    # MID_SOFT만 Pure SW Centroid로 교체
    modified_centroids = centroid_anchors.copy()
    modified_centroids['MID_SOFT'] = pure_sw_centroid
    
    # 재분류
    new_distribution = Counter()
    
    for c in companies:
        scores = calculate_hybrid_scores(c['embedding'], modified_centroids, text_anchors, PRIORITY_VC_CODES)
        sorted_scores = sorted(scores.items(), key=lambda x: x[1], reverse=True)
        new_top1 = sorted_scores[0][0]
        new_distribution[new_top1] += 1
    
    total = sum(new_distribution.values())
    print(f"\n[Pure SW Only 재분류 결과] (총 {total:,}개 기업)")
    print("-" * 50)
    for vc in PRIORITY_VC_CODES:
        count = new_distribution.get(vc, 0)
        pct = count / total * 100
        bar = "#" * int(pct / 2)
        print(f"  {vc:15} {count:>6} ({pct:>5.1f}%) {bar}")
    
    mid_soft_pct = new_distribution.get('MID_SOFT', 0) / total * 100
    print(f"\n[결과] MID_SOFT 비율: {mid_soft_pct:.1f}%")
    
    if mid_soft_pct <= 20:
        print("[해석] ✅ Pure SW Only Golden Set으로 MID_SOFT 비율이 정상화됩니다!")
        print("       → Golden Set을 Pure SW 중심으로 교체하는 것이 효과적입니다.")
    else:
        print("[해석] ⚠️ Pure SW Only로도 MID_SOFT가 여전히 높습니다.")
        print("       → 한국 시장 구조상 이 정도가 현실적인 하한일 수 있습니다.")
    
    return dict(new_distribution)


def main():
    print("MID_SOFT 심층 진단 시작")
    print("=" * 80)
    
    db = next(get_db())
    
    try:
        # 앵커 로드
        print("[준비] 앵커 로딩...")
        clear_anchor_cache()
        centroid_anchors = get_centroid_anchors(db, force_regenerate=True)
        text_anchors = get_value_chain_reference_embeddings(use_centroid=False)
        print(f"[OK] Centroid: {len(centroid_anchors)}, Text: {len(text_anchors)}")
        
        # 기업 임베딩 로드
        print("[준비] 기업 임베딩 로딩...")
        companies = get_all_company_embeddings(db)
        print(f"[OK] {len(companies):,}개 기업 로드 완료")
        
        # Check 1
        check_1_mid_soft_partiality(companies, centroid_anchors, text_anchors)
        
        # Check 2
        check_2_mid_soft_top2_distribution(companies, centroid_anchors, text_anchors)
        
        # Check 3
        check_3_exclude_mid_soft_redistribution(companies, centroid_anchors, text_anchors)
        
        # Check 4
        check_4_pure_sw_only_distribution(db, companies, text_anchors)
        
    finally:
        db.close()
    
    print("\n" + "=" * 80)
    print("진단 완료")
    print("=" * 80)


if __name__ == '__main__':
    main()

