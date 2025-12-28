"""
지주사 기업 분석 스크립트

이미지에서 확인한 지주사 목록 110개 기업의 데이터를 수집하고 공통점을 분석하여
자동 분류 로직을 구축합니다.
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy.orm import Session
from app.db import get_db
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock
import logging
from typing import List, Dict, Any
import re
from collections import Counter

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# 이미지에서 확인한 지주사 목록 (110개)
HOLDING_COMPANY_NAMES = [
    # 1-23
    "HD한국조선해양", "POSCO홀딩스", "SK", "두산", "HD현대", "에코프로", "삼성에피스홀딩스",
    "LG", "한진칼", "한화", "CJ", "LS", "GS", "롯데지주", "영원무역홀딩스", "한국앤컴퍼니",
    "한미사이언스", "아모레퍼시픽홀딩스", "원익홀딩스", "효성", "한화비전", "대웅", "오리온홀딩스",
    # 24-46
    "하림지주", "SK디스커버리", "LS에코에너지", "쿠쿠홀딩스", "HDC", "SNT홀딩스", "DL",
    "솔브레인홀딩스", "F&F홀딩스", "녹십자홀딩스", "동아쏘시오홀딩스", "아세아", "휴온스글로벌",
    "코오롱", "LX홀딩스", "세아제강지주", "풍산홀딩스", "한일홀딩스", "풀무원", "NICE",
    "세아홀딩스", "삼양홀딩스", "HL홀딩스",
    # 47-69
    "농심홀딩스", "대덕", "KG케미칼", "INVENI", "BGF", "일진홀딩스", "대상홀딩스",
    "KISCO홀딩스", "넥센", "콜마홀딩스", "KPX홀딩스", "노루홀딩스", "이지홀딩스", "JW홀딩스",
    "종근당홀딩스", "HS효성", "하이트진로홀딩스", "서연", "골프존홀딩스", "동성케미컬",
    "네오위즈홀딩스", "웅진", "유비쿼스홀딩스",
    # 70-92
    "진양홀딩스", "일동홀딩스", "한세예스24홀딩스", "그래디언트", "미원홀딩스", "유수홀딩스",
    "심텍홀딩스", "매일홀딩스", "티와이홀딩스", "샘표", "한진중공업홀딩스", "대성홀딩스",
    "코아시아", "코스맥스비티아이", "제일파마홀딩스", "한솔홀딩스", "아이디스홀딩스", "경동인베스트",
    "비트플래닛", "AK홀딩스", "디와이", "컴투스홀딩스", "현대코퍼레이션홀딩스",
    # 93-110
    "크라운해태홀딩스", "성창기업지주", "솔본", "DRB동일", "이건홀딩스", "CS홀딩스",
    "HC홈센타", "이녹스", "APS", "신송홀딩스", "슈프리마에이치큐", "우리산업홀딩스",
    "DSR", "평화홀딩스", "SJM홀딩스", "윙입푸드", "한국전자홀딩스", "휴맥스홀딩스"
]


def normalize_company_name(name: str) -> str:
    """회사명 정규화 (공백 제거, 대소문자 통일 등)"""
    # 공백 제거
    name = name.replace(" ", "").replace("　", "")
    # 괄호 제거 (예: "HD한국조선해양(주)" -> "HD한국조선해양")
    name = re.sub(r'\([^)]*\)', '', name)
    name = re.sub(r'\[[^\]]*\]', '', name)
    return name.strip()


def find_companies_by_name(db: Session, company_names: List[str]) -> List[Dict[str, Any]]:
    """회사명으로 기업 정보 조회"""
    results = []
    
    for name in company_names:
        normalized = normalize_company_name(name)
        
        # 정확한 매칭 시도
        stock = db.query(Stock).filter(
            Stock.stock_name == name
        ).first()
        
        if not stock:
            # 부분 매칭 시도
            stock = db.query(Stock).filter(
                Stock.stock_name.like(f"%{normalized}%")
            ).first()
        
        if stock:
            company_detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == stock.ticker
            ).first()
            
            results.append({
                'ticker': stock.ticker,
                'company_name': stock.stock_name,
                'search_name': name,
                'biz_summary': company_detail.biz_summary if company_detail else None,
                'products': company_detail.products if company_detail else None,
                'keywords': company_detail.keywords if company_detail else None,
                'raw_materials': company_detail.raw_materials if company_detail else None,
            })
        else:
            logger.warning(f"⚠️ 회사명을 찾을 수 없음: {name}")
            results.append({
                'ticker': None,
                'company_name': None,
                'search_name': name,
                'biz_summary': None,
                'products': None,
                'keywords': None,
                'raw_materials': None,
            })
    
    return results


def analyze_holding_company_patterns(companies: List[Dict[str, Any]]) -> Dict[str, Any]:
    """지주사 기업들의 공통 패턴 분석"""
    
    # 키워드 수집
    all_keywords = []
    all_biz_summaries = []
    all_products = []
    
    for company in companies:
        if company.get('biz_summary'):
            all_biz_summaries.append(company['biz_summary'])
        
        if company.get('keywords'):
            if isinstance(company['keywords'], list):
                all_keywords.extend([str(k).lower() for k in company['keywords']])
            else:
                all_keywords.append(str(company['keywords']).lower())
        
        if company.get('products'):
            if isinstance(company['products'], list):
                all_products.extend([str(p).lower() for p in company['products']])
            else:
                all_products.append(str(company['products']).lower())
    
    # 지주사 관련 키워드 패턴 찾기
    holding_keywords = [
        '지주', '홀딩', 'holding', '지주사', '홀딩스', '지주회사',
        '계열사', '자회사', '관리', '경영', '투자', '자산관리',
        '경영지원', '사업관리', '기업지배구조'
    ]
    
    # 회사명 패턴
    name_patterns = [
        r'홀딩스?$',
        r'지주$',
        r'홀딩$',
    ]
    
    # biz_summary에서 지주사 관련 문구 찾기
    holding_phrases = []
    for summary in all_biz_summaries:
        if summary:
            summary_lower = summary.lower()
            for keyword in holding_keywords:
                if keyword in summary_lower:
                    # 주변 문맥 추출
                    idx = summary_lower.find(keyword)
                    start = max(0, idx - 20)
                    end = min(len(summary_lower), idx + len(keyword) + 20)
                    holding_phrases.append(summary_lower[start:end])
    
    # 키워드 빈도 분석
    keyword_counter = Counter(all_keywords)
    product_counter = Counter(all_products)
    
    return {
        'total_companies': len(companies),
        'found_companies': len([c for c in companies if c.get('ticker')]),
        'holding_keywords': holding_keywords,
        'name_patterns': name_patterns,
        'holding_phrases': holding_phrases[:20],  # 상위 20개
        'top_keywords': keyword_counter.most_common(30),
        'top_products': product_counter.most_common(30),
        'sample_biz_summaries': [c.get('biz_summary') for c in companies[:10] if c.get('biz_summary')]
    }


def main():
    """메인 실행 함수"""
    db: Session = next(get_db())
    
    logger.info(f"지주사 기업 분석 시작: {len(HOLDING_COMPANY_NAMES)}개 기업")
    
    # 회사 정보 조회
    companies = find_companies_by_name(db, HOLDING_COMPANY_NAMES)
    
    # 패턴 분석
    patterns = analyze_holding_company_patterns(companies)
    
    # 결과 출력
    logger.info(f"\n{'='*80}")
    logger.info("지주사 기업 분석 결과")
    logger.info(f"{'='*80}")
    logger.info(f"총 기업 수: {patterns['total_companies']}")
    logger.info(f"조회된 기업 수: {patterns['found_companies']}")
    
    logger.info(f"\n지주사 관련 키워드:")
    for keyword in patterns['holding_keywords']:
        logger.info(f"  - {keyword}")
    
    logger.info(f"\n회사명 패턴:")
    for pattern in patterns['name_patterns']:
        logger.info(f"  - {pattern}")
    
    logger.info(f"\n상위 키워드 (30개):")
    for keyword, count in patterns['top_keywords']:
        logger.info(f"  - {keyword}: {count}회")
    
    logger.info(f"\n지주사 관련 문구 샘플:")
    for phrase in patterns['holding_phrases'][:10]:
        logger.info(f"  - {phrase}")
    
    logger.info(f"\n샘플 biz_summary (10개):")
    for i, summary in enumerate(patterns['sample_biz_summaries'], 1):
        logger.info(f"\n[{i}] {summary[:200]}...")
    
    # 결과를 파일로 저장
    import json
    output_file = "docs/holding_company_analysis.json"
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump({
            'companies': companies,
            'patterns': patterns
        }, f, ensure_ascii=False, indent=2)
    
    logger.info(f"\n✅ 분석 결과 저장: {output_file}")
    
    # 조회되지 않은 기업 목록
    not_found = [c['search_name'] for c in companies if not c.get('ticker')]
    if not_found:
        logger.warning(f"\n⚠️ 조회되지 않은 기업 ({len(not_found)}개):")
        for name in not_found:
            logger.warning(f"  - {name}")


if __name__ == "__main__":
    main()

