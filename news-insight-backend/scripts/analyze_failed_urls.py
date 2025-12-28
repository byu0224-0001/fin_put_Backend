"""
실패한 리포트 URL 패턴 분석 스크립트

파싱 실패 원인 중 HTML_DOWNLOAD_FAIL (404) 리포트의 URL 패턴을 분석
"""
import sys
from pathlib import Path
import json
import glob
from collections import Counter
from urllib.parse import urlparse, parse_qs

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

def analyze_failed_urls(sample_size: int = 5):
    """
    실패한 리포트 URL 패턴 분석
    
    Args:
        sample_size: 분석할 샘플 개수
    """
    # 최신 parsed 파일 찾기
    parsed_files = glob.glob(str(project_root / "reports" / "parsed_reports_*.json"))
    if not parsed_files:
        print("Parsed 파일을 찾을 수 없습니다.")
        return
    
    latest_file = max(parsed_files, key=lambda p: Path(p).stat().st_mtime)
    print(f"분석 대상 파일: {latest_file}")
    print("")
    
    with open(latest_file, 'r', encoding='utf-8') as f:
        parsed_reports = json.load(f)
    
    # HTML_DOWNLOAD_FAIL 리포트 필터링
    failed_reports = [
        r for r in parsed_reports
        if r.get("parse_fail_reason") == "HTML_DOWNLOAD_FAIL" or 
           (r.get("total_paragraphs", 0) == 0 and r.get("error", "").find("404") >= 0)
    ]
    
    print(f"총 리포트: {len(parsed_reports)}개")
    print(f"HTML_DOWNLOAD_FAIL: {len(failed_reports)}개")
    print("")
    
    if not failed_reports:
        print("분석할 실패 리포트가 없습니다.")
        return
    
    # 샘플 출력
    print("=" * 80)
    print(f"[실패 리포트 샘플 {min(sample_size, len(failed_reports))}개]")
    print("=" * 80)
    for i, report in enumerate(failed_reports[:sample_size], 1):
        print(f"\n[{i}] {report.get('title', 'N/A')[:60]}...")
        print(f"  - URL: {report.get('url', 'N/A')}")
        print(f"  - PDF URL: {report.get('pdf_url', 'N/A')}")
        print(f"  - Error: {report.get('error', 'N/A')[:100]}")
        print(f"  - parse_fail_reason: {report.get('parse_fail_reason', 'N/A')}")
    
    print("")
    print("=" * 80)
    print("[URL 패턴 분석]")
    print("=" * 80)
    
    # URL 도메인/경로 패턴 분석
    url_patterns = Counter()
    domain_patterns = Counter()
    path_patterns = Counter()
    query_params = Counter()
    
    for report in failed_reports:
        url = report.get('url', '')
        if url:
            parsed = urlparse(url)
            domain_patterns[parsed.netloc] += 1
            path_patterns[parsed.path] += 1
            
            # 쿼리 파라미터 분석
            if parsed.query:
                params = parse_qs(parsed.query)
                for key in params.keys():
                    query_params[key] += 1
    
    print(f"\n도메인 분포:")
    for domain, count in domain_patterns.most_common(5):
        print(f"  - {domain}: {count}개")
    
    print(f"\n경로 패턴 (상위 5개):")
    for path, count in path_patterns.most_common(5):
        print(f"  - {path}: {count}개")
    
    if query_params:
        print(f"\n쿼리 파라미터 (상위 5개):")
        for param, count in query_params.most_common(5):
            print(f"  - {param}: {count}개")
    
    # 공통 패턴 확인
    print("")
    print("=" * 80)
    print("[공통 패턴 확인]")
    print("=" * 80)
    
    # 만료 토큰/세션 파라미터 확인
    session_params = ['token', 'session', 'sid', 'auth', 'key', 'expire']
    has_session_params = sum(
        1 for r in failed_reports
        if any(param in ((r.get('url', '') or '') + (r.get('pdf_url', '') or '')).lower() for param in session_params)
    )
    print(f"  - 세션/토큰 파라미터 포함: {has_session_params}개")
    
    # 리다이렉트 확인 (URL에 redirect 포함)
    has_redirect = sum(
        1 for r in failed_reports
        if 'redirect' in ((r.get('url', '') or '') + (r.get('pdf_url', '') or '')).lower()
    )
    print(f"  - 리다이렉트 관련: {has_redirect}개")
    
    # PDF 직접 링크 vs 뷰어 링크
    pdf_direct = sum(1 for r in failed_reports if r.get('pdf_url') and r.get('pdf_url', '').endswith('.pdf'))
    pdf_viewer = sum(1 for r in failed_reports if r.get('pdf_url') and not r.get('pdf_url', '').endswith('.pdf'))
    print(f"  - PDF 직접 링크: {pdf_direct}개")
    print(f"  - PDF 뷰어 링크: {pdf_viewer}개")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="실패한 리포트 URL 패턴 분석")
    parser.add_argument('--sample-size', type=int, default=5,
                        help='분석할 샘플 개수 (기본: 5개)')
    
    args = parser.parse_args()
    
    analyze_failed_urls(sample_size=args.sample_size)


if __name__ == "__main__":
    main()

