"""
백엔드 서버 상태 확인 스크립트
"""
import requests
import sys

BASE_URL = "http://localhost:8000"

def check_server():
    """백엔드 서버 상태 확인"""
    print("=" * 70)
    print("백엔드 서버 상태 확인")
    print("=" * 70)
    print()
    
    # 1. 헬스 체크
    print(f"1. 헬스 체크 엔드포인트 확인: {BASE_URL}/health")
    try:
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        if response.status_code == 200:
            print(f"   ✅ 서버 실행 중: {response.json()}")
        else:
            print(f"   ⚠️  서버 응답 이상: {response.status_code}")
            sys.exit(1)
    except requests.exceptions.ConnectionError:
        print(f"   ❌ 서버에 연결할 수 없습니다.")
        print(f"      - 백엔드 서버가 실행 중인지 확인해주세요.")
        print(f"      - 명령어: cd fintech/news-insight-backend && python -m app.main")
        sys.exit(1)
    except requests.exceptions.Timeout:
        print(f"   ⚠️  서버 응답 시간 초과")
        sys.exit(1)
    except Exception as e:
        print(f"   ❌ 오류 발생: {e}")
        sys.exit(1)
    
    print()
    
    # 2. 루트 엔드포인트
    print(f"2. 루트 엔드포인트 확인: {BASE_URL}/")
    try:
        response = requests.get(f"{BASE_URL}/", timeout=5)
        if response.status_code == 200:
            print(f"   ✅ 서버 정상: {response.json()}")
        else:
            print(f"   ⚠️  서버 응답 이상: {response.status_code}")
    except Exception as e:
        print(f"   ⚠️  오류: {e}")
    
    print()
    
    # 3. 피드 API 엔드포인트
    print(f"3. 피드 API 엔드포인트 확인: {BASE_URL}/api/feed")
    try:
        response = requests.get(f"{BASE_URL}/api/feed", params={"limit": 1}, timeout=10)
        if response.status_code == 200:
            data = response.json()
            print(f"   ✅ 피드 API 정상")
            print(f"      - 기사 수: {len(data.get('articles', []))}")
            print(f"      - 총 기사 수: {data.get('total', 0)}")
        else:
            print(f"   ⚠️  피드 API 응답 이상: {response.status_code}")
            print(f"      - 응답: {response.text[:200]}")
    except requests.exceptions.Timeout:
        print(f"   ⚠️  피드 API 응답 시간 초과 (모델 로딩 중일 수 있습니다)")
    except Exception as e:
        print(f"   ⚠️  오류: {e}")
    
    print()
    print("=" * 70)
    print("서버 상태 확인 완료")
    print("=" * 70)


if __name__ == "__main__":
    check_server()

