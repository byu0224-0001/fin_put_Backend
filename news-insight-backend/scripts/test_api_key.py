"""OpenAI API 키 한도 테스트"""
import os
import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

def test_openai_api():
    """OpenAI API 호출 테스트"""
    try:
        from openai import OpenAI
        
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            print("[오류] OPENAI_API_KEY가 설정되지 않았습니다.")
            return False
        
        print(f"[정보] API 키: {api_key[:8]}...{api_key[-4:]}")
        
        client = OpenAI(api_key=api_key)
        
        # 간단한 테스트 호출
        print("[테스트] gpt-4o-mini 호출 중...")
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[{"role": "user", "content": "Say 'API works' in 3 words or less."}],
            max_tokens=10
        )
        
        result = response.choices[0].message.content
        usage = response.usage
        
        print(f"[성공] 응답: {result}")
        print(f"[사용량] 입력: {usage.prompt_tokens}, 출력: {usage.completion_tokens}, 총: {usage.total_tokens}")
        
        return True
        
    except Exception as e:
        error_str = str(e)
        print(f"[오류] {error_str}")
        
        if "insufficient_quota" in error_str.lower() or "rate_limit" in error_str.lower():
            print("[한도] API 한도 초과 또는 잔액 부족")
        elif "invalid_api_key" in error_str.lower():
            print("[한도] API 키가 유효하지 않음")
        elif "billing" in error_str.lower():
            print("[한도] 결제 정보 확인 필요")
        
        return False

if __name__ == "__main__":
    success = test_openai_api()
    sys.exit(0 if success else 1)

