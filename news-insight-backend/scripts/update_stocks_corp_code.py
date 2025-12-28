# -*- coding: utf-8 -*-
"""
stocks 테이블에 DART 고유번호 업데이트 스크립트

corpCode.xml을 다운로드하여 모든 상장 기업의 고유번호를 stocks 테이블에 저장
"""
import sys
sys.path.insert(0, '.')
sys.stdout.reconfigure(encoding='utf-8')
import os
from pathlib import Path
from dotenv import load_dotenv
from sqlalchemy import text

# .env 파일 로드
project_root = Path(__file__).parent.parent
env_path = project_root / '.env'
load_dotenv(dotenv_path=env_path, override=True)

from app.db import SessionLocal
from app.models.stock import Stock
from app.services.dart_corp_code_mapper import DartCorpCodeMapper

def main():
    """메인 함수"""
    db = SessionLocal()
    api_key = os.getenv('DART_API_KEY')
    
    if not api_key:
        print("❌ DART_API_KEY가 설정되지 않았습니다.")
        return
    
    try:
        print("=" * 80)
        print("DART 고유번호 매핑 테이블 업데이트 시작")
        print("=" * 80)
        
        # 매퍼 초기화
        mapper = DartCorpCodeMapper(api_key)
        
        # 전체 매핑 테이블 로드
        print("\n[1/3] DART 고유번호 매핑 테이블 다운로드 중...")
        all_mappings = mapper.get_all_mappings(force_reload=True)
        print(f"✅ {len(all_mappings)}개 기업의 고유번호 로드 완료")
        
        # stocks 테이블의 모든 한국 상장 기업 조회
        print("\n[2/3] stocks 테이블에서 한국 상장 기업 조회 중...")
        stocks = db.query(Stock).filter(
            Stock.market.in_(['KOSPI', 'KOSDAQ'])
        ).all()
        print(f"✅ {len(stocks)}개 기업 조회 완료")
        
        # 고유번호 업데이트
        print("\n[3/3] 고유번호 업데이트 중...")
        updated = 0
        not_found = 0
        
        for stock in stocks:
            corp_code = all_mappings.get(stock.ticker)
            if corp_code:
                stock.dart_corp_code = corp_code
                updated += 1
            else:
                not_found += 1
                if not_found <= 10:  # 처음 10개만 출력
                    print(f"  ⚠️  {stock.ticker}: {stock.stock_name} - 고유번호 없음")
        
        db.commit()
        
        print("\n" + "=" * 80)
        print("업데이트 완료!")
        print("=" * 80)
        print(f"✅ 업데이트된 기업: {updated}개")
        print(f"⚠️  고유번호 없는 기업: {not_found}개")
        if not_found > 10:
            print(f"   (처음 10개만 출력, 총 {not_found}개)")
        print("=" * 80)
        
    except Exception as e:
        db.rollback()
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
    finally:
        db.close()
        sys.exit(0)

if __name__ == "__main__":
    main()

