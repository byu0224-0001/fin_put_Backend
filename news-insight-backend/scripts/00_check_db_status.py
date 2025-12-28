"""
Step 0: 현재 상태 확인 스크립트

기존 DB에 한국 기업이 몇 개인지 확인
"""
import sys
import os
from pathlib import Path

# 프로젝트 루트를 Python 경로에 추가
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Windows 환경에서 인코딩 문제 방지
if sys.platform == 'win32':
    import codecs
    if sys.stdout.encoding != 'utf-8':
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

from dotenv import load_dotenv
load_dotenv()

from app.db import SessionLocal
from app.models.stock import Stock

def main():
    """DB 상태 확인"""
    print("=" * 60)
    print("Step 0: 현재 상태 확인")
    print("=" * 60)
    
    db = SessionLocal()
    try:
        # 전체 기업 수 확인
        total_count = db.query(Stock).count()
        print(f"\n[1] 전체 기업 수: {total_count}개")
        
        # 한국 기업 수 확인
        kr_count = db.query(Stock).filter(Stock.country == "KR").count()
        print(f"[2] 한국 기업 수: {kr_count}개")
        
        # 미국 기업 수 확인
        us_count = db.query(Stock).filter(Stock.country == "US").count()
        print(f"[3] 미국 기업 수: {us_count}개")
        
        # 샘플 10개 기업 리스트 확인
        print(f"\n[4] 한국 기업 샘플 (상위 10개):")
        print("-" * 60)
        kr_stocks = db.query(Stock).filter(Stock.country == "KR").limit(10).all()
        for idx, stock in enumerate(kr_stocks, 1):
            print(f"  {idx:2d}. [{stock.ticker}] {stock.stock_name} ({stock.market})")
        
        # 시장별 분포 확인
        print(f"\n[5] 한국 기업 시장별 분포:")
        print("-" * 60)
        kospi_count = db.query(Stock).filter(
            Stock.country == "KR",
            Stock.market == "KOSPI"
        ).count()
        kosdaq_count = db.query(Stock).filter(
            Stock.country == "KR",
            Stock.market == "KOSDAQ"
        ).count()
        print(f"  - KOSPI: {kospi_count}개")
        print(f"  - KOSDAQ: {kosdaq_count}개")
        
        print("\n" + "=" * 60)
        print("✅ Step 0 완료!")
        print("=" * 60)
        
        if kr_count == 0:
            print("\n⚠️  경고: 한국 기업 데이터가 없습니다.")
            print("   먼저 'python scripts/update_stock_data.py'를 실행하세요.")
        elif kr_count < 1000:
            print(f"\n⚠️  경고: 한국 기업 수가 예상보다 적습니다 ({kr_count}개).")
            print("   'python scripts/update_stock_data.py'를 실행하여 업데이트하세요.")
        else:
            print(f"\n✅ 한국 기업 데이터 준비 완료 ({kr_count}개)")
            print("   다음 단계: Step 1 (DB 스키마 확장) 진행 가능")
        
    except Exception as e:
        print(f"\n❌ 오류 발생: {e}")
        import traceback
        traceback.print_exc()
        print("\n문제 해결 방법:")
        print("1. PostgreSQL이 실행 중인지 확인하세요.")
        print("2. .env 파일의 데이터베이스 연결 정보를 확인하세요.")
        print("3. Docker 컨테이너가 실행 중인지 확인하세요.")
        print("   (docker-compose up -d)")
    finally:
        db.close()

if __name__ == "__main__":
    main()

