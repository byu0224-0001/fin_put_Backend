"""
Step 2.1: 경제 변수 온톨로지 Import 스크립트

Economic_Ontology_V5.xlsx 파일을 읽어서 economic_variables 테이블에 저장

실행 방법:
    python scripts/01_import_ontology.py
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

import pandas as pd
from app.db import SessionLocal
from app.models.economic_variable import EconomicVariable
import json
import logging
from tqdm import tqdm

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_synonyms(synonyms_str):
    """
    동의어 문자열을 JSON 배열로 변환
    
    Args:
        synonyms_str: 세미콜론으로 구분된 문자열 (예: "FFR;Fed 금리;연준 금리")
    
    Returns:
        list: JSON 배열 (예: ["FFR", "Fed 금리", "연준 금리"])
    """
    if pd.isna(synonyms_str) or not synonyms_str:
        return []
    
    # 문자열로 변환
    synonyms_str = str(synonyms_str).strip()
    
    if not synonyms_str:
        return []
    
    # 세미콜론으로 분리
    synonyms_list = [s.strip() for s in synonyms_str.split(';') if s.strip()]
    
    return synonyms_list


def import_economic_variables(excel_path=None):
    """
    Excel 파일에서 경제 변수를 읽어서 DB에 저장
    
    Args:
        excel_path: Excel 파일 경로 (기본값: data/Economic_Ontology_V1.xlsx)
    
    Returns:
        tuple: (성공 개수, 실패 개수)
    """
    if excel_path is None:
        excel_path = project_root / "data" / "Economic_Ontology_V5.xlsx"
    
    if not os.path.exists(excel_path):
        logger.error(f"Excel 파일을 찾을 수 없습니다: {excel_path}")
        return 0, 0
    
    logger.info(f"Excel 파일 읽기 중: {excel_path}")
    
    try:
        # Excel 파일 읽기
        df = pd.read_excel(excel_path)
        logger.info(f"총 {len(df)}개 행 발견")
        
        # 컬럼 확인
        required_columns = ['code', 'name_ko', 'category', 'layer', 'synonyms', 'description']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logger.error(f"필수 컬럼이 없습니다: {missing_columns}")
            return 0, 0
        
        db = SessionLocal()
        success_count = 0
        fail_count = 0
        
        try:
            # 각 행 처리 (개별 커밋으로 중복 처리)
            for idx, row in tqdm(df.iterrows(), total=len(df), desc="Import 진행 중"):
                try:
                    code = str(row['code']).strip()
                    
                    # 동의어 파싱
                    synonyms_list = parse_synonyms(row.get('synonyms'))
                    
                    # EconomicVariable 객체 생성
                    economic_var = EconomicVariable(
                        code=code,
                        name_ko=str(row['name_ko']).strip(),
                        category=str(row['category']).strip() if pd.notna(row.get('category')) else None,
                        layer=str(row['layer']).strip() if pd.notna(row.get('layer')) else None,
                        synonyms=synonyms_list,  # JSONB로 자동 변환
                        description=str(row['description']).strip() if pd.notna(row.get('description')) else None
                    )
                    
                    # Upsert: merge 사용 (있으면 수정, 없으면 추가)
                    db.merge(economic_var)
                    db.commit()  # 각 행마다 커밋 (중복 처리 안전)
                    
                    success_count += 1
                    
                    # 주기적으로 로그 출력 (50개마다)
                    if (idx + 1) % 50 == 0:
                        logger.info(f"진행: {idx + 1}/{len(df)}")
                
                except Exception as e:
                    db.rollback()  # 에러 발생 시 롤백
                    logger.warning(f"행 {idx + 1} 처리 실패 ({row.get('code', 'N/A')}): {e}")
                    fail_count += 1
                    continue
            logger.info(f"Import 완료! 성공: {success_count}개, 실패: {fail_count}개")
            
            return success_count, fail_count
            
        finally:
            db.close()
    
    except Exception as e:
        logger.error(f"Excel 파일 읽기 실패: {e}")
        import traceback
        traceback.print_exc()
        return 0, 0


def main():
    """메인 함수"""
    print("=" * 60)
    print("Step 2.1: 경제 변수 온톨로지 Import")
    print("=" * 60)
    
    success, fail = import_economic_variables()
    
    print("\n" + "=" * 60)
    if success > 0:
        print(f"✅ Import 완료!")
        print(f"   성공: {success}개")
        if fail > 0:
            print(f"   실패: {fail}개")
    else:
        print("❌ Import 실패!")
    print("=" * 60)
    
    return success > 0


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

