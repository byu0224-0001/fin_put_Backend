"""
종목분석 리포트 필터링 스크립트

COMPANY 리포트 테스트를 위해 종목분석 카테고리만 필터링
"""
import sys
import os
import json
from pathlib import Path

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


def filter_company_reports(input_file: str, output_file: str, limit: int = 20):
    """
    종목분석 리포트만 필터링
    
    Args:
        input_file: 입력 파일 경로
        output_file: 출력 파일 경로
        limit: 최대 개수
    """
    print("=" * 80)
    print("[종목분석 리포트 필터링]")
    print("=" * 80)
    
    # 입력 파일 읽기
    with open(input_file, 'r', encoding='utf-8') as f:
        all_reports = json.load(f)
    
    print(f"\n[전체 리포트] {len(all_reports)}개\n")
    
    # 종목분석 리포트 필터링
    company_reports = []
    for report in all_reports:
        category = report.get('category', '')
        title = report.get('title', '')
        
        # 종목분석 카테고리 또는 제목에 종목 관련 키워드
        if '종목분석' in category or '종목' in category:
            company_reports.append(report)
        elif any(keyword in title for keyword in ['종목', '기업', '주식', '회사']):
            # 추가 필터: 제목에 종목 관련 키워드가 있으면 포함
            company_reports.append(report)
    
    print(f"[필터링 결과]")
    print(f"  - 종목분석 리포트: {len(company_reports)}개\n")
    
    # Limit 적용
    if len(company_reports) > limit:
        company_reports = company_reports[:limit]
        print(f"  - Limit 적용: {limit}개로 제한\n")
    
    # 출력 파일 저장
    output_path = Path(output_file)
    output_path.parent.mkdir(exist_ok=True)
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(company_reports, f, ensure_ascii=False, indent=2)
    
    print(f"[저장 완료] {output_path}")
    print(f"  - 총 {len(company_reports)}개 리포트 저장됨")
    
    # 샘플 출력
    print(f"\n[샘플 리포트]")
    for i, report in enumerate(company_reports[:5], 1):
        print(f"  [{i}] {report.get('title', 'N/A')[:60]}...")
        print(f"      Category: {report.get('category', 'N/A')}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="종목분석 리포트 필터링")
    parser.add_argument("--input", required=True, help="입력 파일 경로")
    parser.add_argument("--output", help="출력 파일 경로 (기본값: reports/company_reports_filtered.json)")
    parser.add_argument("--limit", type=int, default=20, help="최대 개수")
    args = parser.parse_args()
    
    output_file = args.output or f"reports/company_reports_filtered_{Path(args.input).stem}.json"
    filter_company_reports(args.input, output_file, args.limit)

