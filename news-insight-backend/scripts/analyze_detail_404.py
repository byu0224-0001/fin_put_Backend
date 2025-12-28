#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""DETAIL_404 샘플 분석 스크립트 (v12 Final + body_hash)"""

import json
import sys
import glob

# 최신 리포트 파일 찾기
report_files = sorted(glob.glob("reports/naver_reports_*.json"), reverse=True)
if not report_files:
    print("No report files found")
    sys.exit(1)

latest_file = report_files[0]
print(f"Analyzing: {latest_file}")

with open(latest_file, "r", encoding="utf-8") as f:
    data = json.load(f)

d404 = [r for r in data if r.get("acquisition_status") == "DETAIL_404"]
print(f"\n=== DETAIL_404 Complete Analysis ({len(d404)} total) ===\n")

for i, r in enumerate(d404[:5]):
    print(f"[{i+1}] {r.get('broker_name', '?')}")
    print(f"    title: {r.get('list_row_title', '')[:40]}...")
    print(f"    row_links_count: {r.get('row_links_count', 'N/A')}")
    print(f"    row_has_pdf_candidate: {r.get('row_has_pdf_candidate')}")
    print(f"    detail_fetch_status: {r.get('detail_fetch_status', 'N/A')}")
    print(f"    detail_content_type: {r.get('detail_content_type', 'N/A')}")
    print(f"    detail_body_hash_8: {r.get('detail_body_hash_8', 'N/A')}")  # 404 시그니처
    print()

# 통계
has_pdf_candidate = sum(1 for r in d404 if r.get("row_has_pdf_candidate"))
avg_links = sum(r.get("row_links_count", 0) for r in d404) / len(d404) if d404 else 0
has_html_ct = sum(1 for r in d404 if r.get("detail_content_type") and "html" in r.get("detail_content_type", "").lower())
unique_hashes = set(r.get("detail_body_hash_8") for r in d404 if r.get("detail_body_hash_8"))

print(f"=== Summary (v12 Final + body_hash) ===")
print(f"DETAIL_404 total: {len(d404)}")
print(f"row_has_pdf_candidate=True: {has_pdf_candidate}")
print(f"avg row_links_count: {avg_links:.1f}")
print(f"detail_content_type contains 'html': {has_html_ct}")
print(f"unique body_hash_8 count: {len(unique_hashes)}")
print(f"body_hash_8 values: {unique_hashes}")
print()
print(f"=== Conclusion ===")
if has_pdf_candidate == 0 and has_html_ct == len(d404) and len(unique_hashes) == 1:
    print("FULLY CONFIRMED: All 404s have same body hash = consistent 404 error page")
elif has_pdf_candidate == 0 and has_html_ct == len(d404):
    print("MOSTLY CONFIRMED: All are 'no PDF in list' + 'real 404'")
else:
    print("NEEDS INVESTIGATION")

