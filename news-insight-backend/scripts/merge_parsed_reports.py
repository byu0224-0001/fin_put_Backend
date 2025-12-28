"""파싱된 리포트 병합"""
import json
import glob
from collections import Counter
from datetime import datetime

def main():
    parsed_files = sorted(glob.glob("reports/parsed_*.json"))
    print(f"파싱 파일 수: {len(parsed_files)}개")
    
    all_reports = []
    seen_titles = set()
    
    for f in parsed_files:
        try:
            with open(f, "r", encoding="utf-8") as fp:
                data = json.load(fp)
            
            for r in data:
                # 중복 제거 (제목 기준)
                title = r.get("title", "")
                if title and title not in seen_titles:
                    # full_text가 있는 것만
                    if r.get("full_text") and len(r.get("full_text", "")) > 100:
                        all_reports.append(r)
                        seen_titles.add(title)
        except Exception as e:
            print(f"  오류: {f} - {e}")
    
    print(f"\n중복 제거 후 리포트 수: {len(all_reports)}개")
    
    # 카테고리 분포
    cats = Counter(r.get("category", "?") for r in all_reports)
    print(f"\n카테고리 분포:")
    for cat, count in cats.most_common():
        print(f"  - {cat}: {count}개")
    
    # parse_quality 분포
    quality = Counter(r.get("parse_quality", "?") for r in all_reports)
    print(f"\nparse_quality 분포:")
    for q, count in quality.most_common():
        print(f"  - {q}: {count}개")
    
    # 병합 파일 저장
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"reports/merged_parsed_{timestamp}.json"
    with open(output_file, "w", encoding="utf-8") as fp:
        json.dump(all_reports, fp, ensure_ascii=False, indent=2)
    
    print(f"\n병합 파일 저장: {output_file}")
    print(f"총 {len(all_reports)}개 리포트")

if __name__ == "__main__":
    main()

