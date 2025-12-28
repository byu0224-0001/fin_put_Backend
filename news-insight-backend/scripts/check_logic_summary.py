"""logic_summary 진단 스크립트"""
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from app.db import SessionLocal
from app.models.industry_edge import IndustryEdge

def main():
    db = SessionLocal()
    
    # 전체 edge 조회
    edges = db.query(IndustryEdge).all()
    print(f'Total edges: {len(edges)}')
    
    # logic_summary 길이 분포
    lengths = [len(e.logic_summary) if e.logic_summary else 0 for e in edges]
    print(f'logic_summary avg length: {sum(lengths)/len(lengths):.0f}')
    print(f'logic_summary min/max: {min(lengths)}/{max(lengths)}')
    
    # 드라이버별 분포
    driver_counts = {}
    for e in edges:
        d = e.source_driver_code or 'UNKNOWN'
        driver_counts[d] = driver_counts.get(d, 0) + 1
    
    print('\nDriver distribution:')
    for d, c in sorted(driver_counts.items(), key=lambda x: -x[1])[:10]:
        print(f'  {d}: {c}')
    
    # 샘플 logic_summary 출력 (ASCII 안전)
    print('\nSample logic_summaries:')
    for i, e in enumerate(edges[:10]):
        ls = e.logic_summary[:80] if e.logic_summary else 'None'
        # 비ASCII 문자 제거
        ls_safe = ''.join(c if ord(c) < 128 else '?' for c in ls)
        print(f'{i+1}. [{e.source_driver_code}] {ls_safe}...')
    
    # 중복 여부 확인 (logic_summary 첫 50자 기준)
    print('\n=== 중복 진단 ===')
    prefixes = {}
    for e in edges:
        if e.logic_summary:
            prefix = e.logic_summary[:50]
            if prefix not in prefixes:
                prefixes[prefix] = []
            prefixes[prefix].append(e.id)
    
    duplicates = [(k, v) for k, v in prefixes.items() if len(v) > 1]
    print(f'Duplicate prefixes (50 chars): {len(duplicates)}')
    
    if duplicates:
        print('Duplicates found:')
        for prefix, ids in duplicates[:5]:
            print(f'  "{prefix}..." -> {len(ids)} edges')
    
    db.close()

if __name__ == "__main__":
    main()

