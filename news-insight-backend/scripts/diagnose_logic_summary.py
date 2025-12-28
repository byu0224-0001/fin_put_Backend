"""
logic_summary 문제 진단 스크립트

목적:
1. 파싱된 텍스트의 품질 확인
2. logic_summary와 원본 텍스트 비교
3. 메타데이터 오염 여부 확인
"""
import sys
from pathlib import Path
import json
import re

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from app.db import SessionLocal
from app.models.industry_edge import IndustryEdge

# 메타데이터 패턴 (오염 감지용)
METADATA_PATTERNS = [
    r'\d{4}\.\d{2}\.\d{2}',  # 날짜 패턴 (2025.12.24)
    r'\d{2}\.\d{2}\.\d{2}',  # 날짜 패턴 (25.12.24)
    r'Analyst',  # 애널리스트
    r'@\w+\.com',  # 이메일
    r'02-\d{4}-\d{4}',  # 전화번호
    r'Weekly',  # Weekly 리포트 표시
    r'hana\w*\.com',  # 증권사 도메인
    r'I\d{4}\.\d{2}\.\d{2}',  # 리포트 코드
]

def detect_metadata_leak(text: str) -> dict:
    """메타데이터 오염 감지"""
    if not text:
        return {"leaked": False, "patterns": []}
    
    leaked_patterns = []
    for pattern in METADATA_PATTERNS:
        if re.search(pattern, text, re.IGNORECASE):
            leaked_patterns.append(pattern)
    
    return {
        "leaked": len(leaked_patterns) > 0,
        "patterns": leaked_patterns,
        "leak_count": len(leaked_patterns)
    }


def main():
    print("=" * 80)
    print("[logic_summary 문제 진단]")
    print("=" * 80)
    
    # 1. DB에서 logic_summary 확인
    db = SessionLocal()
    edges = db.query(IndustryEdge).order_by(IndustryEdge.created_at.desc()).limit(10).all()
    
    print(f"\n총 {len(edges)}개 Edge 분석")
    
    # 통계
    total_len = []
    leaked_count = 0
    
    print("\n[샘플 logic_summary 분석]")
    print("-" * 80)
    
    for i, edge in enumerate(edges[:5]):
        ls = edge.logic_summary or ""
        ls_len = len(ls)
        total_len.append(ls_len)
        
        leak_result = detect_metadata_leak(ls)
        if leak_result["leaked"]:
            leaked_count += 1
        
        print(f"\n[{i+1}] Driver: {edge.source_driver_code}, Sector: {edge.target_sector_code}")
        print(f"    Length: {ls_len}자")
        print(f"    Leaked: {leak_result['leaked']} ({leak_result['patterns']})")
        print(f"    Text: {ls[:150]}...")
    
    print("\n" + "=" * 80)
    print("[진단 결과]")
    print("=" * 80)
    
    # 길이 분포
    if total_len:
        avg_len = sum(total_len) / len(total_len)
        min_len = min(total_len)
        max_len = max(total_len)
        
        print(f"\n  길이 분포:")
        print(f"    - 평균: {avg_len:.0f}자")
        print(f"    - 최소/최대: {min_len}/{max_len}자")
        
        if min_len == max_len == 260:
            print(f"    [경고] 모든 logic_summary가 260자로 고정됨 - 강제 트렁케이션 발생")
    
    # 메타데이터 오염률
    leak_rate = leaked_count / len(edges) * 100 if edges else 0
    print(f"\n  메타데이터 오염:")
    print(f"    - 오염된 Edge: {leaked_count}/{len(edges)} ({leak_rate:.1f}%)")
    
    if leak_rate > 50:
        print(f"    [치명적] 메타데이터 오염률이 50% 초과 - insight_extractor 수정 필요")
    
    # 2. 파싱된 텍스트 확인
    print("\n" + "=" * 80)
    print("[파싱된 텍스트 품질 확인]")
    print("=" * 80)
    
    parsed_file = project_root / "reports" / "parsed_reports_20251224_133732.json"
    if parsed_file.exists():
        with open(parsed_file, 'r', encoding='utf-8') as f:
            parsed_data = json.load(f)
        
        print(f"\n  파싱된 리포트: {len(parsed_data)}개")
        
        # 샘플 텍스트 분석
        for i, r in enumerate(parsed_data[:3]):
            title = r.get('title', 'N/A')
            text = r.get('text', '')
            
            print(f"\n  [{i+1}] {title[:50]}...")
            print(f"      텍스트 길이: {len(text)}자")
            
            # 첫 200자 메타데이터 오염 확인
            first_200 = text[:200]
            leak = detect_metadata_leak(first_200)
            print(f"      첫 200자 오염: {leak['leaked']} ({leak['patterns']})")
            
            # 첫 200자 출력 (ASCII 안전)
            safe_text = ''.join(c if ord(c) < 128 or c.isalnum() else '?' for c in first_200[:100])
            print(f"      Preview: {safe_text}...")
    
    db.close()
    
    print("\n" + "=" * 80)
    print("[권장 조치]")
    print("=" * 80)
    print("""
  P0-1: PDF 파싱 시 상단 15-20% 영역 제거 (헤더/메타데이터)
  P0-2: insight_extractor 프롬프트에 "메타데이터 무시" 명령 추가
  P0-3: 260자 강제 트렁케이션 해제, 문장 단위로 자르기
  P0-4: kiwipiepy 문장 분리 적용
""")


if __name__ == "__main__":
    main()

