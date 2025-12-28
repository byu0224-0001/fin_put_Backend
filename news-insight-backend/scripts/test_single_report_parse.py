"""단일 리포트 파싱 테스트"""
import sys
from pathlib import Path
import json

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from scripts.parse_broker_report_pdf import parse_report

# 리포트 파일 로드
input_file = project_root / "reports" / "naver_reports_20251220_125301.json"
with open(input_file, 'r', encoding='utf-8') as f:
    reports = json.load(f)

# PDF URL이 있는 첫 번째 리포트 찾기
test_report = None
for r in reports:
    if r.get('pdf_url'):
        test_report = r
        break

if not test_report:
    print("PDF URL이 있는 리포트를 찾을 수 없습니다.")
    sys.exit(1)

print(f"테스트 리포트: {test_report.get('title', 'N/A')[:50]}...")
print(f"PDF URL: {test_report.get('pdf_url', 'N/A')[:80]}...")

# 파싱 실행
parsed = parse_report(test_report)

print(f"\n파싱 결과:")
print(f"- 소스 타입: {parsed.get('source_type', 'N/A')}")
print(f"- 총 문단 수: {parsed.get('total_paragraphs', 0)}")
print(f"- 전체 텍스트 길이: {len(parsed.get('full_text', ''))}자")
print(f"- 에러: {parsed.get('error', 'None')}")

if parsed.get('total_paragraphs', 0) > 0:
    print("\n첫 번째 문단 샘플:")
    paragraphs = parsed.get('paragraphs', [])
    if paragraphs:
        print(paragraphs[0].get('text', '')[:200] + "...")
else:
    print("\n파싱 실패!")

