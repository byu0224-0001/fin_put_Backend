"""
DB에서 실제 저장된 industry_edges를 확인하고 중복 리포트를 찾는 스크립트
"""
import sys
from pathlib import Path
import hashlib
from collections import defaultdict

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv()

from app.db import SessionLocal
from app.models.industry_edge import IndustryEdge

def check_industry_edges_duplicates():
    """DB에서 industry_edges를 확인하고 중복 리포트 찾기"""
    db = SessionLocal()
    
    try:
        # 모든 industry_edges 조회
        all_edges = db.query(IndustryEdge).all()
        print(f"[총 저장된 Industry Edges] {len(all_edges)}개\n")
        
        if not all_edges:
            print("저장된 industry_edges가 없습니다.")
            return
        
        # logic_summary 기반으로 그룹화
        logic_groups = defaultdict(list)
        
        for edge in all_edges:
            logic_summary = edge.logic_summary or ""
            if logic_summary:
                logic_fp = hashlib.sha256(logic_summary.encode('utf-8')).hexdigest()[:16]
                key = f"{logic_fp}_{edge.source_driver_code}_{edge.target_sector_code}"
                logic_groups[key].append(edge)
        
        # 중복 그룹 찾기 (2개 이상인 경우)
        duplicate_groups = {k: v for k, v in logic_groups.items() if len(v) > 1}
        
        print("=" * 80)
        print("[중복 그룹 분석]")
        print("=" * 80)
        print(f"\n총 논리 그룹: {len(logic_groups)}개")
        print(f"중복 그룹 (2개 이상): {len(duplicate_groups)}개")
        
        # MACRO vs INDUSTRY 분류
        macro_edges = [e for e in all_edges if e.target_sector_code == "MARKET"]
        industry_edges = [e for e in all_edges if e.target_sector_code != "MARKET"]
        
        print(f"\n[MACRO 리포트] (target_sector_code = MARKET): {len(macro_edges)}개")
        print(f"[INDUSTRY 리포트] (target_sector_code != MARKET): {len(industry_edges)}개")
        
        # 중복 그룹 변수 초기화
        macro_duplicates = []
        industry_duplicates = []
        
        # 중복 그룹 상세 정보
        if duplicate_groups:
            print("\n" + "=" * 80)
            print("[중복 그룹 상세 정보]")
            print("=" * 80)
            
            for key, group in duplicate_groups.items():
                first_edge = group[0]
                is_macro = first_edge.target_sector_code == "MARKET"
                
                duplicate_info = {
                    'logic_summary': first_edge.logic_summary,
                    'source_driver': first_edge.source_driver_code,
                    'target_sector': first_edge.target_sector_code,
                    'edges': group,
                    'count': len(group)
                }
                
                if is_macro:
                    macro_duplicates.append(duplicate_info)
                else:
                    industry_duplicates.append(duplicate_info)
            
            # MACRO 중복 그룹
            print(f"\n[MACRO 리포트 중복 그룹] {len(macro_duplicates)}개")
            for i, dup_info in enumerate(macro_duplicates[:10], 1):  # 상위 10개만
                print(f"\n[{i}] 중복 그룹 ({dup_info['count']}개 리포트):")
                print(f"    Source Driver: {dup_info['source_driver']}")
                print(f"    Target Sector: {dup_info['target_sector']}")
                print(f"    Logic Summary: {dup_info['logic_summary'][:150]}...")
                print(f"    리포트 목록:")
                
                # report_id로 리포트 정보 표시
                for idx, edge in enumerate(dup_info['edges'], 1):
                    status = "저장됨" if idx == 1 else "중복 스킵됨"
                    print(f"       {idx}. [{status}] report_id={edge.report_id}")
                    if edge.key_sentence:
                        print(f"          Key Sentence: {edge.key_sentence[:80]}...")
            
            # INDUSTRY 중복 그룹
            print(f"\n[INDUSTRY 리포트 중복 그룹] {len(industry_duplicates)}개")
            for i, dup_info in enumerate(industry_duplicates[:10], 1):  # 상위 10개만
                print(f"\n[{i}] 중복 그룹 ({dup_info['count']}개 리포트):")
                print(f"    Source Driver: {dup_info['source_driver']}")
                print(f"    Target Sector: {dup_info['target_sector']}")
                print(f"    Logic Summary: {dup_info['logic_summary'][:150]}...")
                print(f"    리포트 목록:")
                
                # report_id로 리포트 정보 표시
                for idx, edge in enumerate(dup_info['edges'], 1):
                    status = "저장됨" if idx == 1 else "중복 스킵됨"
                    print(f"       {idx}. [{status}] report_id={edge.report_id}")
                    if edge.key_sentence:
                        print(f"          Key Sentence: {edge.key_sentence[:80]}...")
        
        # 저장된 리포트 목록 (중복이 아닌 것들)
        print("\n" + "=" * 80)
        print("[저장된 리포트 목록 (중복이 아닌 것들)]")
        print("=" * 80)
        
        unique_edges = []
        for key, group in logic_groups.items():
            if len(group) == 1:  # 중복이 아닌 경우
                unique_edges.extend(group)
        
        macro_unique = [e for e in unique_edges if e.target_sector_code == "MARKET"]
        industry_unique = [e for e in unique_edges if e.target_sector_code != "MARKET"]
        
        print(f"\n[MACRO 리포트] (중복 아님): {len(macro_unique)}개")
        for i, edge in enumerate(macro_unique[:5], 1):  # 상위 5개만
            print(f"   {i}. report_id={edge.report_id}")
            if edge.logic_summary:
                print(f"      Logic: {edge.logic_summary[:80]}...")
            if edge.key_sentence:
                print(f"      Key Sentence: {edge.key_sentence[:60]}...")
        
        print(f"\n[INDUSTRY 리포트] (중복 아님): {len(industry_unique)}개")
        for i, edge in enumerate(industry_unique[:5], 1):  # 상위 5개만
            print(f"   {i}. report_id={edge.report_id}")
            if edge.logic_summary:
                print(f"      Logic: {edge.logic_summary[:80]}...")
            if edge.key_sentence:
                print(f"      Key Sentence: {edge.key_sentence[:60]}...")
        
        # 통계 요약
        print("\n" + "=" * 80)
        print("[통계 요약]")
        print("=" * 80)
        print(f"\n총 저장된 Industry Edges: {len(all_edges)}개")
        print(f"  - MACRO (MARKET): {len(macro_edges)}개")
        print(f"  - INDUSTRY: {len(industry_edges)}개")
        print(f"\n중복 그룹: {len(duplicate_groups)}개")
        print(f"  - MACRO 중복 그룹: {len(macro_duplicates)}개")
        print(f"  - INDUSTRY 중복 그룹: {len(industry_duplicates)}개")
        
        # 중복으로 스킵된 리포트 수 계산
        skipped_macro = sum(len(g['edges']) - 1 for g in macro_duplicates)
        skipped_industry = sum(len(g['edges']) - 1 for g in industry_duplicates)
        
        print(f"\n중복으로 스킵된 리포트:")
        print(f"  - MACRO: {skipped_macro}개 (총 {len(macro_edges)}개 중)")
        print(f"  - INDUSTRY: {skipped_industry}개 (총 {len(industry_edges)}개 중)")
        
    finally:
        db.close()

if __name__ == "__main__":
    check_industry_edges_duplicates()

