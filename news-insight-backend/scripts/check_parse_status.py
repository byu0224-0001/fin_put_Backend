"""파싱 파일 상태 확인"""
import json
import glob
from collections import Counter

def main():
    parsed_files = sorted(glob.glob("reports/parsed_*.json"))
    if not parsed_files:
        print("파싱된 파일이 없습니다.")
        return
    
    latest = parsed_files[-1]
    print(f"파일: {latest}")
    
    with open(latest, "r", encoding="utf-8") as fp:
        parsed = json.load(fp)
    
    print(f"총 리포트: {len(parsed)}개")
    
    # parse_quality 분포
    quality = Counter(r.get("parse_quality", "?") for r in parsed)
    print(f"\nparse_quality 분포:")
    for q, count in quality.most_common():
        print(f"  - {q}: {count}개")
    
    # full_text 있는 리포트
    has_text = [r for r in parsed if r.get("full_text") and len(r.get("full_text", "")) > 100]
    print(f"\nfull_text 있는 리포트: {len(has_text)}개")
    
    # 카테고리별 분포
    cats = Counter(r.get("category", "?") for r in has_text)
    print(f"\nfull_text 있는 카테고리 분포:")
    for cat, count in cats.most_common():
        print(f"  - {cat}: {count}개")
    
    # acquisition_status 분포
    acq = Counter(r.get("acquisition_status", "?") for r in parsed)
    print(f"\nacquisition_status 분포:")
    for a, count in acq.most_common():
        print(f"  - {a}: {count}개")
    
    # 첫 번째 텍스트 있는 리포트 확인
    if has_text:
        r = has_text[0]
        print(f"\n첫 번째 리포트 텍스트 미리보기:")
        print(f"  - 제목: {r.get('title', '')[:50]}")
        print(f"  - 길이: {len(r.get('full_text', ''))}자")
        print(f"  - 처음 200자: {r.get('full_text', '')[:200]}...")

if __name__ == "__main__":
    main()

