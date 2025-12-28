"""
3-hop 문장 근거 링크 확인 스크립트
"""
import json
from pathlib import Path

# 최신 JSON 파일 찾기
files = list(Path("reports").glob("3hop_sentences_*.json"))
if files:
    f = max(files, key=lambda x: x.stat().st_mtime)
    data = json.load(open(f, 'r', encoding='utf-8'))
    
    print(f"파일: {f.name}")
    print(f"문장 수: {len(data)}")
    print("\n첫 번째 문장의 근거 링크:")
    if data:
        s = data[0]
        print(f"  industry_edge_id: {s.get('industry_edge_id')}")
        print(f"  company_edge_id: {s.get('company_edge_id')}")
        print(f"  evidence_ids: {s.get('evidence_ids')}")
        print(f"  report_ids: {s.get('report_ids')}")
        print(f"  evidence_count: {s.get('evidence_count')}")
else:
    print("3-hop 문장 JSON 파일을 찾을 수 없습니다.")

