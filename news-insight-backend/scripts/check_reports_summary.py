"""현재 수집된 리포트 요약"""
import json
import glob
from pathlib import Path
from collections import Counter

def main():
    # 1. 수집 파일 확인
    files = sorted(glob.glob("reports/naver_reports_*.json"))
    print(f"총 수집 파일: {len(files)}개")
    
    all_reports = []
    for f in files[-5:]:  # 최근 5개 파일만
        try:
            with open(f, "r", encoding="utf-8") as fp:
                data = json.load(fp)
                all_reports.extend(data)
        except Exception as e:
            print(f"  오류: {f} - {e}")
    
    print(f"\n최근 5개 파일의 리포트 수: {len(all_reports)}개")
    
    # 카테고리 분포
    cats = Counter(r.get("category", "?") for r in all_reports)
    print(f"\n카테고리 분포:")
    for cat, count in cats.most_common():
        print(f"  - {cat}: {count}개")
    
    # acquisition_status 분포
    status = Counter(r.get("acquisition_status", "?") for r in all_reports)
    print(f"\nacquisition_status 분포:")
    for s, count in status.most_common():
        print(f"  - {s}: {count}개")
    
    # 2. 파싱 파일 확인
    print("\n" + "="*50)
    print("[파싱된 파일 확인]")
    parsed_files = sorted(glob.glob("reports/parsed_*.json"))
    if parsed_files:
        latest = parsed_files[-1]
        print(f"최신 파싱 파일: {latest}")
        with open(latest, "r", encoding="utf-8") as fp:
            parsed = json.load(fp)
        
        print(f"총 리포트: {len(parsed)}개")
        success = [r for r in parsed if r.get("parse_status") == "success"]
        print(f"파싱 성공: {len(success)}개")
        
        # 카테고리별 분포
        cats = Counter(r.get("category", "?") for r in success)
        print(f"\n파싱 성공 카테고리 분포:")
        for cat, count in cats.most_common():
            print(f"  - {cat}: {count}개")
    else:
        print("파싱된 파일이 없습니다.")

if __name__ == "__main__":
    main()

