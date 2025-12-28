# -*- coding: utf-8 -*-
"""
지주회사 매출 비중 데이터 재수집

revenue_by_segment가 없는 지주회사에 대해 DART에서 재수집
"""
import sys
import os
from pathlib import Path
import time
import json

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Windows 환경 인코딩
if sys.platform == 'win32':
    os.environ['PYTHONIOENCODING'] = 'utf-8'

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock
from app.services.dart_parser import DartParser

from openai import OpenAI
import json_repair

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# OpenAI 클라이언트
client = OpenAI(api_key=os.getenv('OPENAI_API_KEY'))

# 매출 비중이 없는 지주회사 (테스트용 2개)
TARGETS = [
    "SK", "LG"  # 먼저 2개만 테스트
]

# 전체 대상 (나중에 활성화)
# TARGETS_FULL = [
#     "POSCO홀딩스", "SK", "LG", "효성", "롯데지주", "대웅",
#     "HD현대", "두산", "에코프로", "한미사이언스"
# ]


def get_dart_revenue_prompt():
    """매출 비중 전용 프롬프트"""
    return """
    너는 10년 차 애널리스트야. 아래 사업보고서에서 **사업부문별 매출 비중**을 추출해.
    
    [추출 지침]
    1. "II. 사업의 내용" 또는 "매출 및 수주상황" 섹션에서 매출 구성을 찾아라
    2. 지주회사의 경우 자회사별 매출 기여도를 추출해도 됨
    3. 비중(%)이 명시되면 그대로 사용, 없으면 매출액으로 비중 계산
    4. "배당금수익", "임대수익", "브랜드사용료", "로열티" 등이 있으면 반드시 포함
    
    [반환 형식]
    JSON만 반환:
    {
        "revenue_by_segment": {
            "부문명1": 비중(숫자),
            "부문명2": 비중(숫자),
            ...
        },
        "holding_revenue": {
            "배당금수익": 비중(숫자, 있으면),
            "임대수익": 비중(숫자, 있으면),
            "브랜드사용료": 비중(숫자, 있으면),
            "기타지주수익": 비중(숫자, 있으면)
        }
    }
    
    없으면 빈 객체 {} 반환.
    """


def extract_revenue_with_llm(text: str, company_name: str) -> dict:
    """OpenAI API로 매출 비중 추출"""
    prompt = get_dart_revenue_prompt()
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a precise financial analyst. Extract only factual information from the provided text. Output JSON only, no markdown."},
                {"role": "user", "content": f"{prompt}\n\n[{company_name} 사업보고서]\n{text[:40000]}"}
            ],
            temperature=0.1,
            max_tokens=1000
        )
        
        content = response.choices[0].message.content
        content = content.replace("```json", "").replace("```", "").strip()
        
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            repaired = json_repair.repair_json(content)
            return json.loads(repaired)
            
    except Exception as e:
        logger.error(f"LLM 호출 실패: {e}")
        return {}


def refetch_holding_revenue():
    """지주회사 매출 비중 재수집"""
    db = SessionLocal()
    
    dart_api_key = os.getenv('DART_API_KEY')
    if not dart_api_key:
        print("❌ DART_API_KEY 환경변수가 설정되지 않았습니다.")
        return []
    
    dart_parser = DartParser(dart_api_key)
    
    try:
        print("=" * 80)
        print("[지주회사 매출 비중 재수집]")
        print("=" * 80)
        
        results = []
        
        for company_name in TARGETS:
            print(f"\n{'='*60}")
            print(f"[{company_name}] 처리 시작")
            
            # Stock 검색
            stock = db.query(Stock).filter(
                Stock.stock_name == company_name
            ).first()
            
            if not stock:
                stock = db.query(Stock).filter(
                    Stock.stock_name.contains(company_name)
                ).first()
            
            if not stock:
                print(f"  ❌ Stock 미발견")
                continue
            
            ticker = stock.ticker
            print(f"  티커: {ticker}")
            
            # CompanyDetail 확인
            detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == ticker
            ).first()
            
            if detail and detail.revenue_by_segment and len(detail.revenue_by_segment) > 0:
                print(f"  ✅ 이미 매출 비중 있음: {detail.revenue_by_segment}")
                continue
            
            # DART 보고서 가져오기
            print(f"  📄 DART 사업보고서 조회 중...")
            
            try:
                # DART API로 사업보고서 섹션 추출 (04_fetch_dart.py와 동일 방식)
                combined_text = dart_parser.extract_key_sections(ticker, 2024)
                
                if not combined_text:
                    # 2023년 시도
                    combined_text = dart_parser.extract_key_sections(ticker, 2023)
                
                if not combined_text or len(combined_text) < 500:
                    print(f"  ❌ 사업보고서 추출 실패 (길이: {len(combined_text) if combined_text else 0})")
                    continue
                
                business_section = combined_text
                print(f"  📊 텍스트 길이: {len(business_section):,}자")
                
                # LLM으로 매출 비중 추출
                print(f"  🤖 LLM 매출 비중 추출 중...")
                
                response = extract_revenue_with_llm(business_section, stock.stock_name)
                
                if response and 'revenue_by_segment' in response:
                    revenue_data = response.get('revenue_by_segment', {})
                    holding_revenue = response.get('holding_revenue', {})
                    
                    print(f"  ✅ 추출 성공:")
                    print(f"     매출 비중: {revenue_data}")
                    print(f"     지주 수익: {holding_revenue}")
                    
                    # DB 업데이트
                    if detail:
                        detail.revenue_by_segment = revenue_data
                        db.commit()
                        print(f"  💾 DB 업데이트 완료")
                    
                    results.append({
                        'company': company_name,
                        'ticker': ticker,
                        'revenue_by_segment': revenue_data,
                        'holding_revenue': holding_revenue,
                        'status': 'SUCCESS'
                    })
                else:
                    print(f"  ❌ LLM 응답 없음 또는 빈 결과")
                    results.append({
                        'company': company_name,
                        'ticker': ticker,
                        'status': 'NO_DATA'
                    })
                
            except Exception as e:
                print(f"  ❌ 오류: {e}")
                results.append({
                    'company': company_name,
                    'ticker': ticker,
                    'status': 'ERROR',
                    'error': str(e)
                })
            
            # API 속도 제한
            time.sleep(2)
        
        # 결과 요약
        print("\n" + "=" * 80)
        print("[결과 요약]")
        print("=" * 80)
        
        success = [r for r in results if r.get('status') == 'SUCCESS']
        print(f"\n성공: {len(success)}개")
        for r in success:
            print(f"  ✅ {r['company']}: {r.get('revenue_by_segment', {})}")
        
        failed = [r for r in results if r.get('status') != 'SUCCESS']
        print(f"\n실패: {len(failed)}개")
        for r in failed:
            print(f"  ❌ {r['company']}: {r.get('status')}")
        
        # 결과 저장
        output_path = project_root / 'reports' / 'holding_revenue_refetch.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        
        print(f"\n결과 저장: {output_path}")
        
        return results
        
    finally:
        db.close()


if __name__ == "__main__":
    refetch_holding_revenue()

