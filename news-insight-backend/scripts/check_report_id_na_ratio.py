"""
report_id 생성 입력값 NA 비율 확인 스크립트

새로 생성한 report_id의 입력 4요소(broker_name, title, date, url) 중 NA 비율 체크
"""
import sys
from pathlib import Path
import json
import glob
import hashlib

# Windows 인코딩 처리
if sys.platform == 'win32':
    import codecs
    import os
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.text_normalizer import normalize_date_for_report_id, canonicalize_url

def check_na_ratio():
    """NA 비율 확인"""
    print("=" * 80)
    print("[report_id 생성 입력값 NA 비율 확인]")
    print("=" * 80)
    
    # 최신 parsed 리포트 파일 찾기
    reports_dir = project_root / "reports"
    report_files = list(reports_dir.glob("parsed_*.json"))
    if not report_files:
        print("[ERROR] parsed 리포트 파일을 찾을 수 없습니다.")
        print(f"  검색 경로: {reports_dir}")
        return
    
    latest_file = max(report_files, key=lambda x: x.stat().st_mtime)
    print(f"\n파일: {latest_file.name}")
    
    with open(latest_file, 'r', encoding='utf-8') as f:
        reports = json.load(f)
    
    print(f"리포트 수: {len(reports)}")
    
    # NA 비율 체크
    na_counts = {
        "broker_name": 0,
        "title": 0,
        "date": 0,
        "url": 0
    }
    
    total = 0
    sample_report_ids = []
    
    for report in reports[:20]:  # 최대 20개 샘플
        broker_name = report.get("broker_name", "")
        title = report.get("title", "")
        date = report.get("date", "")
        url = report.get("url", "")
        
        # NA 체크
        if not broker_name or broker_name.strip() == "":
            na_counts["broker_name"] += 1
        if not title or title.strip() == "":
            na_counts["title"] += 1
        
        normalized_date = normalize_date_for_report_id(date)
        if normalized_date == "NA":
            na_counts["date"] += 1
        
        canonical_url = canonicalize_url(url)
        if canonical_url == "NA":
            na_counts["url"] += 1
        
        # report_id 생성 (샘플)
        report_id = hashlib.sha256(
            f"{broker_name or 'NA'}|{title or 'NA'}|{normalized_date}|{canonical_url}".encode()
        ).hexdigest()[:32]
        sample_report_ids.append(report_id[:16])
        
        total += 1
    
    # 결과 출력
    print("\n[NA 비율 분석]")
    print(f"  샘플 수: {total}")
    for key, count in na_counts.items():
        ratio = count / total * 100 if total > 0 else 0
        print(f"  {key}: {count}/{total} ({ratio:.1f}%)")
        if ratio > 50:
            print(f"    [WARNING] {key}의 NA 비율이 50%를 초과합니다!")
    
    # 충돌 위험 평가
    unique_ids = len(set(sample_report_ids))
    collision_risk = (total - unique_ids) / total * 100 if total > 0 else 0
    
    print(f"\n[충돌 위험 평가]")
    print(f"  고유 report_id: {unique_ids}/{total}")
    print(f"  충돌 위험: {collision_risk:.1f}%")
    
    if collision_risk > 10:
        print(f"  [ERROR] report_id 충돌 위험이 높습니다!")
    elif collision_risk > 5:
        print(f"  [WARNING] report_id 충돌 위험이 있습니다.")
    else:
        print(f"  [OK] report_id 충돌 위험이 낮습니다.")
    
    # 샘플 report_id 출력
    print(f"\n[샘플 report_id (처음 16자리)]")
    for i, rid in enumerate(sample_report_ids[:5], 1):
        print(f"  {i}. {rid}")

if __name__ == "__main__":
    check_na_ratio()

