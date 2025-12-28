# -*- coding: utf-8 -*-
"""
전체 섹터 재분류 스크립트

SEC_FASHION, SEC_COSMETIC, SEC_HOLDING 재분류 수행
- KRX 업종 기반 사전 필터링
- 매출 비중 기반 지주회사 유형 분류
- 토스증권 분류 기준 반영
"""
import sys
import os
from pathlib import Path
from datetime import datetime
import json
from collections import Counter

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

if sys.platform == 'win32':
    os.environ['PYTHONIOENCODING'] = 'utf-8'

from dotenv import load_dotenv
load_dotenv(project_root / '.env')

from app.db import SessionLocal
from app.models.investor_sector import InvestorSector
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock
from app.services.krx_sector_filter import (
    filter_sector_by_krx,
    detect_holding_company,
    classify_holding_type,
    is_sector_blocked_by_krx,
    KRX_TIER1_EXACT,
    KRX_TIER2_MODERATE
)

import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# 토스증권 기준 섬유/의류 KRX 업종
FASHION_KRX_INDUSTRIES = [
    '봉제의복 제조업',
    '직물직조 및 직물제품 제조업',
    '방적 및 가공사 제조업',
    '섬유제품 염색, 정리 및 마무리 가공업',
    '화학섬유 제조업',
    '의복 액세서리 제조업',
]

# 토스증권 기준 화장품 키워드 (섬유/의류 제외)
COSMETIC_MUST_KEYWORDS = ['화장품', '기초화장품', '색조화장품', '스킨케어', '코스메틱']

# 지주회사 KRX 업종
HOLDING_KRX_INDUSTRIES = ['기타 금융업', '회사 본부 및 경영 컨설팅 서비스업']


def get_company_data(db, ticker: str) -> dict:
    """기업 정보 조회"""
    stock = db.query(Stock).filter(Stock.ticker == ticker).first()
    detail = db.query(CompanyDetail).filter(CompanyDetail.ticker == ticker).first()
    sector = db.query(InvestorSector).filter(
        InvestorSector.ticker == ticker,
        InvestorSector.is_primary == True
    ).first()
    
    return {
        'ticker': ticker,
        'name': stock.stock_name if stock else None,
        'krx': stock.industry_raw if stock else None,
        'keywords': detail.keywords if detail else [],
        'products': detail.products if detail else [],
        'revenue_by_segment': detail.revenue_by_segment if detail else {},
        'current_sector': sector.major_sector if sector else None,
        'current_sub_sector': sector.sub_sector if sector else None,
        'sector_obj': sector,
        'detail_obj': detail,
        'stock_obj': stock
    }


def should_be_fashion(data: dict) -> tuple:
    """SEC_FASHION으로 분류해야 하는지 판정"""
    krx = data.get('krx') or ''
    keywords = data.get('keywords') or []
    products = data.get('products') or []
    name = data.get('name') or ''
    
    # 1. KRX 업종이 명확히 섬유/의류
    if krx in FASHION_KRX_INDUSTRIES:
        # Sub-sector 결정
        if '봉제' in krx or '의복' in krx:
            sub = 'FASHION_OEM'
        elif '직물' in krx or '방적' in krx or '섬유' in krx:
            sub = 'TEXTILE'
        else:
            sub = 'FASHION_OEM'
        return True, sub, f"KRX({krx[:20]})"
    
    # 2. 키워드가 섬유/의류 관련
    fashion_keywords = ['의류', '섬유', '봉제', '패션', '니트', '원단', '직물', '방적', 
                       '어패럴', '스포츠웨어', '란제리', '내의', '여성복', '남성복']
    
    for kw in keywords:
        if isinstance(kw, str):
            for fk in fashion_keywords:
                if fk in kw:
                    return True, 'FASHION_OEM', f"키워드({kw})"
    
    # 3. 제품이 섬유/의류 관련
    for prod in products:
        if isinstance(prod, str):
            for fk in fashion_keywords:
                if fk in prod:
                    return True, 'FASHION_OEM', f"제품({prod[:15]})"
    
    return False, None, ""


def should_be_cosmetic(data: dict) -> tuple:
    """SEC_COSMETIC으로 분류해야 하는지 판정"""
    krx = data.get('krx') or ''
    keywords = data.get('keywords') or []
    products = data.get('products') or []
    
    # KRX가 섬유/의류면 화장품 아님
    if krx in FASHION_KRX_INDUSTRIES:
        return False, None, ""
    
    # 화장품 핵심 키워드 필수
    has_cosmetic_keyword = False
    for kw in keywords:
        if isinstance(kw, str):
            for ck in COSMETIC_MUST_KEYWORDS:
                if ck in kw:
                    has_cosmetic_keyword = True
                    break
    
    if has_cosmetic_keyword:
        # 건강기능식품이 주력이면 SEC_BIO
        bio_keywords = ['건강기능식품', '제약', '바이오', '의약품', '캡슐']
        bio_count = sum(1 for kw in keywords if isinstance(kw, str) and any(bk in kw for bk in bio_keywords))
        cosmetic_count = sum(1 for kw in keywords if isinstance(kw, str) and any(ck in kw for ck in COSMETIC_MUST_KEYWORDS))
        
        if bio_count > cosmetic_count:
            return False, None, "건강기능식품 주력"
        
        # Sub-sector 결정
        if any(kw for kw in keywords if isinstance(kw, str) and ('OEM' in kw or 'ODM' in kw)):
            sub = 'COSMETIC_OEM'
        else:
            sub = 'COSMETIC_BRAND'
        
        return True, sub, "화장품 키워드"
    
    return False, None, ""


def should_be_holding(data: dict) -> tuple:
    """SEC_HOLDING으로 분류해야 하는지 판정"""
    name = data.get('name') or ''
    krx = data.get('krx') or ''
    keywords = data.get('keywords') or []
    products = data.get('products') or []
    revenue = data.get('revenue_by_segment') or {}
    
    is_holding, conf, reason, holding_type = detect_holding_company(
        company_name=name,
        industry_raw=krx,
        keywords=keywords,
        products=products,
        revenue_by_segment=revenue,
        company_detail=None  # 🆕 R4: 시그니처 변경 반영 (스크립트에서는 None)
    )
    
    if is_holding:
        return True, holding_type, reason
    
    return False, None, ""


def reclassify_all():
    """전체 섹터 재분류"""
    db = SessionLocal()
    
    try:
        print("=" * 80)
        print(f"[전체 섹터 재분류] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        
        # 현재 InvestorSector 조회
        all_sectors = db.query(InvestorSector).filter(
            InvestorSector.is_primary == True
        ).all()
        
        print(f"\n전체 기업: {len(all_sectors)}개")
        
        # 현재 분포
        current_dist = Counter(s.major_sector for s in all_sectors)
        print(f"\n[현재 섹터 분포]")
        for sector, count in current_dist.most_common(20):
            print(f"  {sector}: {count}개")
        
        # 재분류 결과 저장
        changes = {
            'to_fashion': [],
            'to_cosmetic': [],
            'to_holding': [],
            'holding_type_changes': [],
            'no_change': 0,
            'errors': []
        }
        
        # 모든 기업 검사
        print(f"\n[재분류 진행 중...]")
        
        for idx, sector_obj in enumerate(all_sectors):
            if (idx + 1) % 500 == 0:
                print(f"  진행: {idx + 1}/{len(all_sectors)}")
            
            ticker = sector_obj.ticker
            data = get_company_data(db, ticker)
            
            if not data['stock_obj']:
                continue
            
            current_sector = data['current_sector']
            current_sub = data['current_sub_sector']
            new_sector = current_sector
            new_sub = current_sub
            change_reason = ""
            
            try:
                # 1. 지주회사 판정 (최우선)
                is_holding, holding_sub, holding_reason = should_be_holding(data)
                if is_holding:
                    new_sector = 'SEC_HOLDING'
                    new_sub = holding_sub
                    change_reason = holding_reason
                
                # 2. 현재 SEC_COSMETIC인데 실제로 FASHION인 경우
                elif current_sector == 'SEC_COSMETIC':
                    is_fashion, fashion_sub, fashion_reason = should_be_fashion(data)
                    if is_fashion:
                        new_sector = 'SEC_FASHION'
                        new_sub = fashion_sub
                        change_reason = fashion_reason
                    else:
                        # 화장품 맞는지 재확인
                        is_cosmetic, cosmetic_sub, cosmetic_reason = should_be_cosmetic(data)
                        if is_cosmetic:
                            new_sub = cosmetic_sub
                        elif cosmetic_reason == "건강기능식품 주력":
                            new_sector = 'SEC_BIO'
                            new_sub = 'HEALTH_FOOD'
                            change_reason = cosmetic_reason
                
                # 3. KRX 업종이 명확히 섬유/의류인데 다른 섹터인 경우
                elif data['krx'] in FASHION_KRX_INDUSTRIES and current_sector != 'SEC_FASHION':
                    is_fashion, fashion_sub, fashion_reason = should_be_fashion(data)
                    if is_fashion:
                        new_sector = 'SEC_FASHION'
                        new_sub = fashion_sub
                        change_reason = fashion_reason
                
                # 4. 지주회사 sub_sector 업데이트
                elif current_sector == 'SEC_HOLDING':
                    holding_type = classify_holding_type(
                        data['name'], 
                        data['revenue_by_segment'], 
                        data['keywords']
                    )
                    if holding_type != current_sub:
                        new_sub = holding_type
                        change_reason = f"지주유형변경({current_sub}→{holding_type})"
                
                # 변경 사항 기록
                if new_sector != current_sector:
                    change_record = {
                        'ticker': ticker,
                        'name': data['name'],
                        'krx': data['krx'],
                        'from_sector': current_sector,
                        'from_sub': current_sub,
                        'to_sector': new_sector,
                        'to_sub': new_sub,
                        'reason': change_reason
                    }
                    
                    if new_sector == 'SEC_FASHION':
                        changes['to_fashion'].append(change_record)
                    elif new_sector == 'SEC_COSMETIC':
                        changes['to_cosmetic'].append(change_record)
                    elif new_sector == 'SEC_HOLDING':
                        changes['to_holding'].append(change_record)
                    
                    # DB 업데이트
                    sector_obj.major_sector = new_sector
                    sector_obj.sub_sector = new_sub
                    sector_obj.classification_method = 'KRX_RULE_RECLASSIFY'
                    sector_obj.classification_reasoning = change_reason
                    
                elif new_sub != current_sub and current_sector == 'SEC_HOLDING':
                    changes['holding_type_changes'].append({
                        'ticker': ticker,
                        'name': data['name'],
                        'from_sub': current_sub,
                        'to_sub': new_sub,
                        'reason': change_reason
                    })
                    sector_obj.sub_sector = new_sub
                    sector_obj.classification_reasoning = change_reason
                else:
                    changes['no_change'] += 1
                    
            except Exception as e:
                changes['errors'].append({
                    'ticker': ticker,
                    'error': str(e)
                })
        
        # DB 커밋
        db.commit()
        
        # 결과 출력
        print("\n" + "=" * 80)
        print("[재분류 결과]")
        print("=" * 80)
        
        print(f"\n→ SEC_FASHION으로 변경: {len(changes['to_fashion'])}개")
        for c in changes['to_fashion'][:20]:
            print(f"  {c['name']}: {c['from_sector']} → SEC_FASHION/{c['to_sub']} ({c['reason']})")
        if len(changes['to_fashion']) > 20:
            print(f"  ... 외 {len(changes['to_fashion']) - 20}개")
        
        print(f"\n→ SEC_COSMETIC으로 변경: {len(changes['to_cosmetic'])}개")
        for c in changes['to_cosmetic'][:10]:
            print(f"  {c['name']}: {c['from_sector']} → SEC_COSMETIC/{c['to_sub']} ({c['reason']})")
        
        print(f"\n→ SEC_HOLDING으로 변경: {len(changes['to_holding'])}개")
        for c in changes['to_holding'][:20]:
            print(f"  {c['name']}: {c['from_sector']} → SEC_HOLDING/{c['to_sub']} ({c['reason']})")
        if len(changes['to_holding']) > 20:
            print(f"  ... 외 {len(changes['to_holding']) - 20}개")
        
        print(f"\n→ 지주회사 유형 변경: {len(changes['holding_type_changes'])}개")
        for c in changes['holding_type_changes'][:10]:
            print(f"  {c['name']}: {c['from_sub']} → {c['to_sub']} ({c['reason']})")
        
        print(f"\n→ 변경 없음: {changes['no_change']}개")
        print(f"→ 오류: {len(changes['errors'])}개")
        
        # 최종 분포
        print("\n" + "=" * 80)
        print("[최종 섹터 분포]")
        print("=" * 80)
        
        final_sectors = db.query(InvestorSector).filter(
            InvestorSector.is_primary == True
        ).all()
        
        final_dist = Counter(s.major_sector for s in final_sectors)
        for sector, count in final_dist.most_common(25):
            diff = count - current_dist.get(sector, 0)
            diff_str = f"(+{diff})" if diff > 0 else f"({diff})" if diff < 0 else ""
            print(f"  {sector}: {count}개 {diff_str}")
        
        # SEC_HOLDING sub_sector 분포
        holding_sectors = [s for s in final_sectors if s.major_sector == 'SEC_HOLDING']
        holding_sub_dist = Counter(s.sub_sector for s in holding_sectors)
        print(f"\n[SEC_HOLDING Sub-sector 분포]")
        for sub, count in holding_sub_dist.most_common():
            print(f"  {sub}: {count}개")
        
        # 결과 저장
        output_path = project_root / 'reports' / 'reclassify_all_result.json'
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(changes, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n결과 저장: {output_path}")
        
        return changes
        
    finally:
        db.close()


if __name__ == "__main__":
    reclassify_all()

