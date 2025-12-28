"""
404 URL 샘플 확인 스크립트

브라우저 vs 코드 비교를 통해 404의 성격을 확정:
- 브라우저에서도 404 → 진짜 만료/삭제 (소스/링크 생성 변경 필요)
- 브라우저는 열림 → 봇 차단/헤더/세션 문제 (헤더/쿠키 재현 필요)
"""
import sys
from pathlib import Path
import json
import requests
import hashlib
from typing import List, Dict, Optional

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

import logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# run_daily_briefing.py와 동일한 헤더
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Cache-Control": "max-age=0"
}


def check_url_with_requests(url: str, with_referer: bool = False) -> Dict[str, any]:
    """requests로 URL 확인 (Referer 옵션 포함)"""
    # ⭐ P1-5: headers 중복 전달 버그 수정
    headers = HEADERS.copy()
    if with_referer:
        headers["Referer"] = "https://finance.naver.com/research/"
    
    result = {
        "url": url,
        "method": "requests",
        "with_referer": with_referer,
        "status_code": None,
        "final_url": None,
        "redirect_history": [],
        "redirect_count": 0,
        "content_type": None,
        "content_length": None,
        "content_sample_hash": None,  # ⭐ 위장 탐지용
        "possible_blocked": False,
        "server_header": "",
        "via_header": "",
        "error": None
    }
    
    try:
        # HEAD 먼저 시도
        head_response = requests.head(url, headers=headers, timeout=10, allow_redirects=True)
        result["status_code"] = head_response.status_code
        result["final_url"] = head_response.url
        result["redirect_history"] = [r.url for r in head_response.history]
        result["redirect_count"] = len(head_response.history)
        result["content_type"] = head_response.headers.get('content-type', '')
        
        # HEAD가 404면 GET으로 재시도
        if head_response.status_code == 404:
            with requests.get(url, headers=headers, timeout=10, allow_redirects=True, stream=True) as get_response:
                result["status_code"] = get_response.status_code
                result["final_url"] = get_response.url
                result["redirect_history"] = [r.url for r in get_response.history]
                result["redirect_count"] = len(get_response.history)
                result["content_type"] = get_response.headers.get('content-type', '')
                result["content_length"] = get_response.headers.get('content-length', '')
                
                # ⭐ 403/429/404 위장 확인 (HTML title 해시)
                # 404도 HTML 차단 페이지일 수 있음
                content_type_lower = result["content_type"].lower() if result["content_type"] else ''
                if get_response.status_code in [403, 429] or (get_response.status_code == 404 and 'html' in content_type_lower):
                    try:
                        content_sample = get_response.content[:1000]
                        result["content_sample_hash"] = hashlib.md5(content_sample).hexdigest()[:8]
                        result["possible_blocked"] = get_response.status_code in [403, 429]
                        
                        # ⭐ P1: HTML title 추출 (위장 탐지용)
                        if 'html' in content_type_lower:
                            import re
                            title_match = re.search(rb'<title[^>]*>([^<]+)</title>', content_sample, re.IGNORECASE)
                            if title_match:
                                try:
                                    result["html_title"] = title_match.group(1).decode('utf-8', errors='replace')[:50]
                                    result["html_title_hash"] = hashlib.md5(title_match.group(1)).hexdigest()[:8]
                                except:
                                    pass
                            # ⭐ 404인데 HTML이면 위장 가능성 있음
                            if get_response.status_code == 404:
                                result["possible_blocked_404"] = True
                    except:
                        pass
                
                # ⭐ Server, Via, X-Cache 헤더 (민감정보 아님)
                result["server_header"] = get_response.headers.get('Server', '')
                result["via_header"] = get_response.headers.get('Via', '')
                
    except requests.exceptions.Timeout:
        result["error"] = "TIMEOUT"
    except requests.exceptions.ConnectionError:
        result["error"] = "CONNECTION_ERROR"
    except Exception as e:
        result["error"] = str(e)[:100]
    
    return result


def check_urls_from_collected_reports(sample_size: int = 5) -> List[Dict]:
    """수집된 리포트에서 404 URL 샘플 추출 및 확인"""
    # 최신 naver_reports 파일 찾기
    reports_dir = project_root / "reports"
    naver_files = list(reports_dir.glob("naver_reports_*.json"))
    
    if not naver_files:
        logger.warning("수집된 리포트 파일이 없습니다.")
        return []
    
    latest_file = max(naver_files, key=lambda p: p.stat().st_mtime)
    logger.info(f"최신 리포트 파일 사용: {latest_file.name}")
    
    with open(latest_file, 'r', encoding='utf-8') as f:
        reports = json.load(f)
    
    # url_status가 INVALID_URL인 리포트 필터링
    invalid_urls = [
        r for r in reports 
        if r.get("url_status") == "INVALID_URL" or 
           (r.get("url") and "404" in str(r.get("url_validation_error", "")))
    ]
    
    if not invalid_urls:
        logger.warning("INVALID_URL 리포트가 없습니다.")
        return []
    
    # 샘플 추출
    sample_urls = invalid_urls[:sample_size]
    logger.info(f"404 URL 샘플 {len(sample_urls)}개 확인 시작...")
    
    results = []
    for i, report in enumerate(sample_urls, 1):
        url = report.get("url", "")
        title = report.get("title", "")[:50]
        
        logger.info(f"\n[{i}/{len(sample_urls)}] {title}...")
        logger.info(f"URL: {url[:80]}...")
        
        # ⭐ Referer OFF/ON 두 번 시도
        result_no_referer = check_url_with_requests(url, with_referer=False)
        result_with_referer = check_url_with_requests(url, with_referer=True)
        
        result = {
            "title": title,
            "url": url,
            "url_validation_error": report.get("url_validation_error", ""),
            "no_referer": result_no_referer,
            "with_referer": result_with_referer,
            "referer_effect": "차이없음" if result_no_referer["status_code"] == result_with_referer["status_code"] else "차이있음"
        }
        results.append(result)
        
        # 결과 출력
        logger.info(f"  Referer OFF: status={result_no_referer['status_code']}, final_url={result_no_referer['final_url'][:60]}...")
        logger.info(f"  Referer ON:  status={result_with_referer['status_code']}, final_url={result_with_referer['final_url'][:60]}...")
        logger.info(f"  Referer 효과: {result['referer_effect']}")
        if result_with_referer.get("possible_blocked"):
            logger.warning(f"  ⚠️ 차단 가능성: content_hash={result_with_referer.get('content_sample_hash')}")
    
    return results


def print_summary(results: List[Dict]):
    """결과 요약 출력 (Referer 비교 포함)"""
    print("\n" + "=" * 80)
    print("[404 URL 확인 결과 요약]")
    print("=" * 80)
    
    total = len(results)
    no_referer_404 = sum(1 for r in results if r.get("no_referer", {}).get("status_code") == 404)
    no_referer_200 = sum(1 for r in results if r.get("no_referer", {}).get("status_code") == 200)
    with_referer_404 = sum(1 for r in results if r.get("with_referer", {}).get("status_code") == 404)
    with_referer_200 = sum(1 for r in results if r.get("with_referer", {}).get("status_code") == 200)
    referer_effect_count = sum(1 for r in results if r.get("referer_effect") == "차이있음")
    possible_blocked = sum(1 for r in results if r.get("with_referer", {}).get("possible_blocked", False))
    
    print(f"총 확인: {total}개")
    print(f"\n[Referer OFF 결과]")
    print(f"  - 코드 404: {no_referer_404}개")
    print(f"  - 코드 200: {no_referer_200}개")
    print(f"\n[Referer ON 결과]")
    print(f"  - 코드 404: {with_referer_404}개")
    print(f"  - 코드 200: {with_referer_200}개")
    possible_blocked_404 = sum(1 for r in results if r.get("no_referer", {}).get("possible_blocked_404", False) or r.get("with_referer", {}).get("possible_blocked_404", False))
    
    print(f"\n[Referer 효과]")
    print(f"  - 차이 있음: {referer_effect_count}개")
    print(f"  - 차단 가능성 (403/429): {possible_blocked}개")
    print(f"  - 404 위장 가능성 (HTML): {possible_blocked_404}개")
    
    print("\n[판정]")
    if no_referer_404 == total and with_referer_404 == total:
        print("  → 모든 URL이 Referer ON/OFF 모두 404")
        print("  → 브라우저에서 직접 확인 필요:")
        print("     - 브라우저도 404 → 진짜 만료/삭제 (소스/링크 생성 변경 필요)")
        print("     - 브라우저는 열림 → 봇 차단/헤더/세션 문제 (헤더/쿠키 재현 필요)")
    elif referer_effect_count > 0:
        print(f"  → Referer 효과 확인됨 ({referer_effect_count}개)")
        print("  → 네이버 보안 정책(Anti-Bot) 수준 확인")
        print("  → 수집 시 Referer 헤더 포함 필요")
    elif with_referer_200 > 0:
        print(f"  → 일부 URL이 Referer ON 시 200 (HEAD→GET fallback 성공)")
        print("  → URL 검증 게이트 개선 효과 확인")
    
    print("\n[수동 확인 필요]")
    print("  ⚠️ 중요: 브라우저는 쿠키/세션/로그인이 있을 수 있습니다.")
    print("  아래 URL을 브라우저에서 직접 열어 확인하세요:")
    for i, r in enumerate(results[:3], 1):
        print(f"  {i}. {r.get('url', '')[:80]}...")
        if r.get("referer_effect") == "차이있음":
            print(f"     → Referer 효과 확인됨 (차단 가능성)")


def main():
    """메인 함수"""
    import argparse
    parser = argparse.ArgumentParser(description="404 URL 샘플 확인")
    parser.add_argument("--sample-size", type=int, default=5, help="확인할 URL 샘플 수 (기본: 5)")
    args = parser.parse_args()
    
    results = check_urls_from_collected_reports(sample_size=args.sample_size)
    
    if results:
        print_summary(results)
    else:
        print("확인할 URL이 없습니다.")


if __name__ == "__main__":
    main()

