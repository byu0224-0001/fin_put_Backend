# -*- coding: utf-8 -*-
"""
Confidence 리포트 생성 (HIGH_MODEL vs HIGH_OVERRIDE 분리)
"""
import sys
sys.path.insert(0, '.')

from app.db import SessionLocal
from app.models.investor_sector import InvestorSector
from sqlalchemy import text

def generate_confidence_report():
    """HIGH_MODEL vs HIGH_OVERRIDE 분리 리포트"""
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print("Confidence 리포트 (HIGH_MODEL vs HIGH_OVERRIDE)")
        print("=" * 80)
        
        # 전체 조회
        all_sectors = db.query(InvestorSector).filter(
            InvestorSector.is_primary == True
        ).all()
        
        # Top200 조회
        result = db.execute(text("""
            SELECT s.ticker
            FROM stocks s
            WHERE s.market_cap IS NOT NULL
            ORDER BY s.market_cap DESC
            LIMIT 200
        """))
        
        top200_tickers = {row[0] for row in result}
        
        stats = {
            'HIGH_MODEL': {'all': 0, 'top200': 0},
            'HIGH_OVERRIDE': {'all': 0, 'top200': 0},
            'MEDIUM': {'all': 0, 'top200': 0},
            'LOW': {'all': 0, 'top200': 0},
            'HOLD': {'all': 0, 'top200': 0}
        }
        
        for sector in all_sectors:
            confidence = sector.confidence or 'UNKNOWN'
            boosting_log = sector.boosting_log or {}
            classification_meta = boosting_log.get('classification_meta', {})
            
            # 🆕 집계 기준 명확화: override_hit 기반으로 판단
            # override_hit은 classification_meta에서 가져오거나, override 객체에서 가져옴
            override_hit = classification_meta.get('override_hit', False)
            if not override_hit and 'override' in classification_meta:
                override_obj = classification_meta.get('override', {})
                if isinstance(override_obj, dict):
                    override_hit = override_obj.get('hit', False)
            
            # HIGH 분리 (집계 기준 명확화)
            if confidence == 'HIGH':
                if override_hit:
                    key = 'HIGH_OVERRIDE'
                else:
                    key = 'HIGH_MODEL'
            elif confidence.startswith('HOLD'):
                key = 'HOLD'
            else:
                key = confidence
            
            if key in stats:
                stats[key]['all'] += 1
                if sector.ticker in top200_tickers:
                    stats[key]['top200'] += 1
        
        print("\n[전체]")
        total_all = sum(s['all'] for s in stats.values())
        for key, values in stats.items():
            count = values['all']
            ratio = (count / total_all * 100) if total_all > 0 else 0
            print(f"  {key}: {count}개 ({ratio:.1f}%)")
        
        print("\n[Top200]")
        total_top200 = sum(s['top200'] for s in stats.values())
        for key, values in stats.items():
            count = values['top200']
            ratio = (count / total_top200 * 100) if total_top200 > 0 else 0
            print(f"  {key}: {count}개 ({ratio:.1f}%)")
        
        # Override 비율
        override_all = stats['HIGH_OVERRIDE']['all']
        override_top200 = stats['HIGH_OVERRIDE']['top200']
        override_ratio_all = (override_all / total_all * 100) if total_all > 0 else 0
        override_ratio_top200 = (override_top200 / total_top200 * 100) if total_top200 > 0 else 0
        
        print("\n[Override 비율]")
        print(f"  전체: {override_ratio_all:.1f}% ({override_all}/{total_all})")
        print(f"  Top200: {override_ratio_top200:.1f}% ({override_top200}/{total_top200})")
        print(f"  목표: Top200 ≤ 5%")
        
        if override_ratio_top200 <= 5.0:
            print(f"  [PASS] Top200 override 비율 목표 달성")
        else:
            print(f"  [WARN] Top200 override 비율 목표 초과")
        
        return {
            'stats': stats,
            'override_ratio': {
                'all': override_ratio_all,
                'top200': override_ratio_top200
            }
        }
        
    finally:
        db.close()

if __name__ == '__main__':
    generate_confidence_report()

