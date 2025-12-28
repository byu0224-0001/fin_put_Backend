#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Final Validation Checklist (Phase 7)

Check 1: MID_SOFT 복합 기업의 2순위(Detail) 정합성
Check 2: DOWN_SERVICE의 경계 확인
Check 3: UI 시뮬레이션 (IR 대응용)
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

from sqlalchemy import text
from app.db import get_db


def check_1_mid_soft_detail_consistency(db):
    """Check 1: MID_SOFT 복합 기업의 2순위(Detail) 정합성"""
    print("=" * 80)
    print("CHECK 1: MID_SOFT 복합 기업의 2순위(Detail) 정합성")
    print("=" * 80)
    
    # MID_SOFT이고 conf < 0.25인 기업의 Detail 분포
    result1 = db.execute(text("""
        SELECT value_chain_detail, COUNT(*) as cnt,
               ROUND(COUNT(*)::numeric * 100 / SUM(COUNT(*)) OVER(), 1) as pct
        FROM investor_sector
        WHERE is_primary = true 
          AND value_chain = 'MID_SOFT'
          AND value_chain_confidence < 0.25
          AND value_chain_detail IS NOT NULL
        GROUP BY value_chain_detail
        ORDER BY cnt DESC
    """))
    
    print("")
    print("[MID_SOFT 경계/복합 기업의 Top2 분포]")
    print("-" * 50)
    total = 0
    for row in result1:
        bar = "#" * int(float(row[2]) / 2)
        print(f"  {row[0]:15} {row[1]:>6} ({row[2]}%) {bar}")
        total += row[1]
    print(f"  Total: {total}")
    
    # 가설 검증: MID_SOFT + MID_HARD 조합 비율
    result1b = db.execute(text("""
        SELECT 
            SUM(CASE WHEN value_chain_detail = 'MID_HARD' THEN 1 ELSE 0 END)::float / COUNT(*) * 100 as mid_hard_pct
        FROM investor_sector
        WHERE is_primary = true 
          AND value_chain = 'MID_SOFT'
          AND value_chain_confidence < 0.25
          AND value_chain_detail IS NOT NULL
    """))
    row = result1b.fetchone()
    print(f"")
    print(f"[검증] MID_SOFT 경계/복합 중 Top2=MID_HARD 비율: {row[0]:.1f}%")
    if row[0] > 35:
        print("  -> 정상: '설계형 제조' 경계를 정확히 포착")
    else:
        print("  -> 주의: MID_HARD 비율이 예상보다 낮음")
    
    # 샘플 확인: 대표 기업
    print("")
    print("[MID_SOFT 경계 기업 샘플 (Top 15)]")
    print("-" * 80)
    result2 = db.execute(text("""
        SELECT i.ticker, s.stock_name, i.value_chain, i.value_chain_detail, 
               ROUND(i.value_chain_confidence::numeric, 3) as conf
        FROM investor_sector i
        JOIN stocks s ON i.ticker = s.ticker
        WHERE i.is_primary = true 
          AND i.value_chain = 'MID_SOFT'
          AND i.value_chain_confidence < 0.25
          AND i.value_chain_detail IS NOT NULL
        ORDER BY i.value_chain_confidence DESC
        LIMIT 15
    """))
    print(f"{'Ticker':10} {'Name':20} {'VC':12} {'Detail':12} {'Conf'}")
    print("-" * 80)
    for row in result2:
        name = row[1][:18] if row[1] else 'N/A'
        print(f"{row[0]:10} {name:20} {row[2]:12} {row[3]:12} {row[4]}")


def check_2_down_service_boundary(db):
    """Check 2: DOWN_SERVICE의 경계 확인"""
    print("")
    print("=" * 80)
    print("CHECK 2: DOWN_SERVICE의 경계 확인 (플랫폼 vs 유통)")
    print("=" * 80)
    
    # 주요 플랫폼 기업 확인
    platform_tickers = [
        ('035420', 'NAVER'),
        ('035720', 'Kakao'),
        ('035900', 'JYP Ent.'),
        ('352820', 'HYBE'),
        ('036570', 'NCSOFT'),
        ('251270', 'Netmarble'),
        ('377300', 'KakaoPay'),
        ('323410', 'KakaoBank'),
        ('403870', 'PLLAB'),  # 쏘카
        ('322000', 'Yanolja'),  # 야놀자
    ]
    
    ticker_list = ",".join([f"'{t[0]}'" for t in platform_tickers])
    
    result = db.execute(text(f"""
        SELECT i.ticker, s.stock_name, i.value_chain, i.value_chain_detail,
               ROUND(i.value_chain_confidence::numeric, 3) as conf
        FROM investor_sector i
        JOIN stocks s ON i.ticker = s.ticker
        WHERE i.ticker IN ({ticker_list})
          AND i.is_primary = true
    """))
    
    print("")
    print("[주요 플랫폼/서비스 기업 분류 결과]")
    print("-" * 90)
    print(f"{'Ticker':10} {'Name':20} {'VC':15} {'Detail':15} {'Conf':>8} {'판정'}")
    print("-" * 90)
    
    down_service_count = 0
    correct_count = 0
    
    for row in result:
        ticker = row[0]
        name = row[1][:18] if row[1] else 'N/A'
        vc = row[2] or 'N/A'
        detail = row[3] or '-'
        conf = row[4] if row[4] else 0
        
        # 판정
        if vc == 'DOWN_SERVICE':
            judgment = "OK (정확)"
            down_service_count += 1
            correct_count += 1
        elif detail == 'DOWN_SERVICE':
            judgment = "OK (경계)"
            correct_count += 1
        else:
            judgment = "확인 필요"
        
        print(f"{ticker:10} {name:20} {vc:15} {detail:15} {conf:>8.3f} {judgment}")
    
    print("-" * 90)
    print(f"[결과] DOWN_SERVICE 단일: {down_service_count}개, 정확/경계: {correct_count}개")
    
    # DOWN_SERVICE 전체 분포
    print("")
    print("[DOWN_SERVICE 기업 Top2 분포]")
    print("-" * 50)
    result2 = db.execute(text("""
        SELECT value_chain_detail, COUNT(*) as cnt,
               ROUND(COUNT(*)::numeric * 100 / SUM(COUNT(*)) OVER(), 1) as pct
        FROM investor_sector
        WHERE is_primary = true 
          AND value_chain = 'DOWN_SERVICE'
          AND value_chain_detail IS NOT NULL
        GROUP BY value_chain_detail
        ORDER BY cnt DESC
    """))
    for row in result2:
        bar = "#" * int(float(row[2]) / 2)
        print(f"  {row[0]:15} {row[1]:>6} ({row[2]}%) {bar}")


def check_3_ui_simulation(db):
    """Check 3: UI 시뮬레이션 (IR 대응용)"""
    print("")
    print("=" * 80)
    print("CHECK 3: UI 시뮬레이션 (IR 대응용)")
    print("=" * 80)
    
    # 한국어 밸류체인 이름
    vc_names = {
        'UPSTREAM': '소재/원자재',
        'MID_HARD': '제조/부품',
        'MID_SOFT': '설계/기술',
        'DOWN_BIZ': '유통/판매',
        'DOWN_SERVICE': '플랫폼/서비스'
    }
    
    # 대표 기업 샘플
    samples = db.execute(text("""
        SELECT i.ticker, s.stock_name, i.value_chain, i.value_chain_detail,
               i.value_chain_confidence
        FROM investor_sector i
        JOIN stocks s ON i.ticker = s.ticker
        WHERE i.is_primary = true
          AND i.value_chain IS NOT NULL
        ORDER BY RANDOM()
        LIMIT 20
    """))
    
    print("")
    print("[IR 장표용 문장 예시]")
    print("-" * 80)
    
    single_examples = []
    hybrid_examples = []
    
    for row in samples:
        ticker = row[0]
        name = row[1] or 'Unknown'
        vc = row[2]
        detail = row[3]
        conf = float(row[4]) if row[4] else 0
        
        vc_kr = vc_names.get(vc, vc)
        
        if conf >= 0.25:
            # 단일 기업
            sentence = f"{name}은(는) {vc_kr}({vc}) 중심 기업입니다."
            single_examples.append((ticker, name, conf, sentence))
        elif detail:
            # 복합 기업
            detail_kr = vc_names.get(detail, detail)
            sentence = f"{name}은(는) {vc_kr}({vc}) 기반이지만 {detail_kr}({detail}) 역량을 보유한 복합 기업입니다."
            hybrid_examples.append((ticker, name, conf, sentence))
    
    print("")
    print("[단일 기업 (Confident) 예시]")
    for ex in single_examples[:5]:
        print(f"  {ex[0]:10} (conf={ex[2]:.3f})")
        print(f"    -> \"{ex[3]}\"")
    
    print("")
    print("[복합 기업 (Hybrid) 예시]")
    for ex in hybrid_examples[:5]:
        print(f"  {ex[0]:10} (conf={ex[2]:.3f})")
        print(f"    -> \"{ex[3]}\"")
    
    # 특정 유명 기업 문장 생성
    print("")
    print("[주요 기업 IR 문장]")
    print("-" * 80)
    
    famous_tickers = ['000660', '005380', '035420', '035720', '058470', '053800']
    for ticker in famous_tickers:
        result = db.execute(text("""
            SELECT s.stock_name, i.value_chain, i.value_chain_detail, 
                   i.value_chain_confidence
            FROM investor_sector i
            JOIN stocks s ON i.ticker = s.ticker
            WHERE i.ticker = :ticker AND i.is_primary = true
        """), {'ticker': ticker})
        row = result.fetchone()
        if row:
            name = row[0] or 'Unknown'
            vc = row[1]
            detail = row[2]
            conf = float(row[3]) if row[3] else 0
            
            vc_kr = vc_names.get(vc, vc)
            
            if conf >= 0.25:
                sentence = f"\"{name}은(는) {vc_kr}({vc}) 중심 기업입니다.\""
            elif detail:
                detail_kr = vc_names.get(detail, detail)
                sentence = f"\"{name}은(는) {vc_kr}({vc}) 기반이지만 {detail_kr}({detail}) 역량을 보유한 복합 기업입니다.\""
            else:
                sentence = f"\"{name}은(는) {vc_kr}({vc}) 기업입니다.\""
            
            status = "단일" if conf >= 0.25 else "복합"
            print(f"  {ticker} {name:15} conf={conf:.3f} [{status}]")
            print(f"    -> {sentence}")


def check_additional_top2_patterns(db):
    """추가: 복합 기업의 Top2 조합 패턴"""
    print("")
    print("=" * 80)
    print("추가: 복합 기업의 Top1+Top2 조합 패턴")
    print("=" * 80)
    
    result = db.execute(text("""
        SELECT value_chain || ' + ' || value_chain_detail as combo,
               COUNT(*) as cnt,
               ROUND(COUNT(*)::numeric * 100 / SUM(COUNT(*)) OVER(), 1) as pct
        FROM investor_sector
        WHERE is_primary = true 
          AND value_chain IS NOT NULL
          AND value_chain_detail IS NOT NULL
        GROUP BY combo
        ORDER BY cnt DESC
        LIMIT 15
    """))
    
    print("")
    print("[Top1 + Top2 조합 분포 (상위 15개)]")
    print("-" * 60)
    for row in result:
        bar = "#" * int(float(row[2]) / 2)
        print(f"  {row[0]:30} {row[1]:>6} ({row[2]}%) {bar}")
    
    # 이상한 조합 체크
    print("")
    print("[정합성 평가]")
    print("  - MID_SOFT + MID_HARD: 설계형 제조 (정상)")
    print("  - MID_HARD + MID_SOFT: 기술 제조 (정상)")
    print("  - DOWN_BIZ + DOWN_SERVICE: 유통 플랫폼 (정상)")
    print("  - DOWN_SERVICE + DOWN_BIZ: 플랫폼 유통 (정상)")
    print("  - UPSTREAM + MID_HARD: 소재 제조 (정상)")


def main():
    print("Final Validation Checklist - Phase 7")
    print("=" * 80)
    
    db = next(get_db())
    
    try:
        check_1_mid_soft_detail_consistency(db)
        check_2_down_service_boundary(db)
        check_3_ui_simulation(db)
        check_additional_top2_patterns(db)
    finally:
        db.close()
    
    print("")
    print("=" * 80)
    print("Final Validation Complete")
    print("=" * 80)


if __name__ == '__main__':
    main()

