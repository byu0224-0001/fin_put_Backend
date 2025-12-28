"""
현재 DB에 저장된 industry_edges 상태 요약 및 스킵 리포트 분석
"""
import sys
import os
from pathlib import Path
import hashlib
from collections import defaultdict

# Windows 인코딩 처리
if sys.platform == 'win32':
    import codecs
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

from app.db import SessionLocal
from app.models.industry_edge import IndustryEdge

def summarize_industry_edges_status():
    """현재 DB 상태 요약 및 중복 분석"""
    db = SessionLocal()
    
    try:
        all_edges = db.query(IndustryEdge).all()
        print("=" * 80)
        print("[현재 DB 상태 요약]")
        print("=" * 80)
        print(f"총 저장된 Industry Edges: {len(all_edges)}개\n")
        
        # MACRO vs INDUSTRY 분류
        macro_edges = [e for e in all_edges if e.target_sector_code == "MARKET"]
        industry_edges = [e for e in all_edges if e.target_sector_code != "MARKET"]
        
        print(f"MACRO (MARKET): {len(macro_edges)}개")
        print(f"INDUSTRY (SEC_*): {len(industry_edges)}개\n")
        
        # 논리 단위 그룹화
        logic_groups = defaultdict(list)
        for edge in all_edges:
            logic_summary = edge.logic_summary or ""
            if logic_summary:
                logic_fp = hashlib.sha256(logic_summary.encode('utf-8')).hexdigest()[:16]
                key = f"{logic_fp}_{edge.source_driver_code}_{edge.target_sector_code}"
                logic_groups[key].append(edge)
        
        # 중복 그룹 찾기
        duplicate_groups = {k: v for k, v in logic_groups.items() if len(v) > 1}
        
        print("=" * 80)
        print("[중복 분석]")
        print("=" * 80)
        print(f"총 논리 그룹: {len(logic_groups)}개")
        print(f"중복 그룹 (2개 이상): {len(duplicate_groups)}개\n")
        
        if duplicate_groups:
            print("[중복 그룹 상세]")
            for i, (key, group) in enumerate(duplicate_groups.items(), 1):
                first_edge = group[0]
                print(f"\n[{i}] 중복 그룹 ({len(group)}개 리포트):")
                print(f"    Driver: {first_edge.source_driver_code}")
                print(f"    Sector: {first_edge.target_sector_code}")
                print(f"    Logic Summary: {first_edge.logic_summary[:150]}...")
                print(f"    리포트 목록:")
                for idx, edge in enumerate(group, 1):
                    status = "저장됨" if idx == 1 else "중복 스킵됨"
                    print(f"       {idx}. [{status}] report_id={edge.report_id or 'N/A'}")
                    if edge.key_sentence:
                        print(f"          Key: {edge.key_sentence[:80]}...")
        else:
            print("중복 그룹 없음 (모든 리포트가 고유한 논리)\n")
        
        # 저장된 리포트 상세 (중복이 아닌 것들)
        print("=" * 80)
        print("[저장된 리포트 상세 (중복 아님)]")
        print("=" * 80)
        
        unique_edges = []
        for key, group in logic_groups.items():
            if len(group) == 1:
                unique_edges.extend(group)
        
        macro_unique = [e for e in unique_edges if e.target_sector_code == "MARKET"]
        industry_unique = [e for e in unique_edges if e.target_sector_code != "MARKET"]
        
        print(f"\n[MACRO 리포트] (중복 아님): {len(macro_unique)}개")
        for i, edge in enumerate(macro_unique, 1):
            print(f"\n{i}. report_id={edge.report_id or 'N/A'}")
            print(f"   Driver: {edge.source_driver_code}")
            if edge.logic_summary:
                print(f"   Logic: {edge.logic_summary[:150]}...")
            if edge.key_sentence:
                print(f"   Key Sentence: {edge.key_sentence[:100]}...")
        
        print(f"\n[INDUSTRY 리포트] (중복 아님): {len(industry_unique)}개")
        for i, edge in enumerate(industry_unique, 1):
            print(f"\n{i}. report_id={edge.report_id or 'N/A'}")
            print(f"   Driver: {edge.source_driver_code}")
            print(f"   Sector: {edge.target_sector_code}")
            if edge.logic_summary:
                print(f"   Logic: {edge.logic_summary[:150]}...")
            if edge.key_sentence:
                print(f"   Key Sentence: {edge.key_sentence[:100]}...")
        
        print("\n" + "=" * 80)
        print("[결론]")
        print("=" * 80)
        print(f"총 저장: {len(all_edges)}개")
        print(f"  - MACRO: {len(macro_edges)}개")
        print(f"  - INDUSTRY: {len(industry_edges)}개")
        print(f"중복 스킵: {sum(len(g) - 1 for g in duplicate_groups.values())}개")
        print(f"  - MACRO: {sum(len(g) - 1 for g in duplicate_groups.values() if g[0].target_sector_code == 'MARKET')}개")
        print(f"  - INDUSTRY: {sum(len(g) - 1 for g in duplicate_groups.values() if g[0].target_sector_code != 'MARKET')}개")
        
    finally:
        db.close()

if __name__ == "__main__":
    summarize_industry_edges_status()

