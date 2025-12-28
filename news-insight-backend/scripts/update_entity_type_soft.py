# -*- coding: utf-8 -*-
"""
Entity Type Soft 업데이트 스크립트 (GPT 피드백: 임시 방이라도 만들어라)

지주사 138개에 대해 classification_meta에 entity_type 임시 저장
boosting_log JSONB 필드 활용 (DB 마이그레이션 없이)
"""
import sys
import os

# Windows 환경에서 UTF-8 인코딩 설정
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')
    os.environ['PYTHONIOENCODING'] = 'utf-8'
else:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')

sys.path.insert(0, '.')

from app.db import SessionLocal
from app.models.company_detail import CompanyDetail
from app.models.stock import Stock
from app.models.investor_sector import InvestorSector
from app.services.entity_type_classifier import classify_entity_type, update_classification_meta

def update_entity_type_soft(dry_run=True):
    """Entity Type Soft 업데이트 (boosting_log 활용)"""
    db = SessionLocal()
    
    try:
        print("=" * 80, flush=True)
        print("Entity Type Soft 업데이트", flush=True)
        print("=" * 80, flush=True)
        
        if dry_run:
            print("\n⚠️  DRY RUN 모드 (실제 수정하지 않음)", flush=True)
        else:
            print("\n✅ 실제 수정 모드", flush=True)
        
        # 지주회사 후보 조회
        holdings = db.query(Stock).filter(
            Stock.stock_name.like('%홀딩스%') | 
            Stock.stock_name.like('%지주%') |
            Stock.stock_name.like('%Holdings%')
        ).all()
        
        print(f"\n지주회사 후보: {len(holdings)}개", flush=True)
        print("\n" + "=" * 80, flush=True)
        print("Entity Type 분류 및 업데이트", flush=True)
        print("=" * 80, flush=True)
        
        success_count = 0
        error_count = 0
        entity_type_stats = {}
        
        for idx, stock in enumerate(holdings, 1):
            detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == stock.ticker
            ).order_by(CompanyDetail.updated_at.desc()).first()
            
            sector = db.query(InvestorSector).filter(
                InvestorSector.ticker == stock.ticker,
                InvestorSector.is_primary == True
            ).first()
            
            if not sector:
                continue
            
            # Entity Type 분류
            entity_type, entity_conf, entity_meta = classify_entity_type(stock, detail)
            
            # 통계
            entity_type_stats[entity_type] = entity_type_stats.get(entity_type, 0) + 1
            
            # boosting_log에 classification_meta 저장 (임시)
            existing_boosting_log = sector.boosting_log or {}
            classification_meta = update_classification_meta(
                existing_boosting_log.get('classification_meta'),
                entity_type,
                entity_conf,
                entity_meta
            )
            
            # boosting_log 업데이트
            updated_boosting_log = existing_boosting_log.copy()
            updated_boosting_log['classification_meta'] = classification_meta
            
            print(f"\n{idx}. {stock.stock_name} ({stock.ticker})", flush=True)
            print(f"   Entity Type: {entity_type} (confidence: {entity_conf:.2f})", flush=True)
            print(f"   Current Sector: {sector.major_sector}", flush=True)
            print(f"   Holding Type: {entity_meta.get('holding_type', 'N/A')}", flush=True)
            
            if not dry_run:
                try:
                    sector.boosting_log = updated_boosting_log
                    db.commit()
                    print(f"   ✅ 업데이트 완료", flush=True)
                    success_count += 1
                except Exception as e:
                    db.rollback()
                    print(f"   ❌ 업데이트 실패: {e}", flush=True)
                    error_count += 1
            else:
                print(f"   [DRY RUN] 업데이트 예정", flush=True)
                success_count += 1
            
            if idx % 20 == 0:
                print(f"\n  진행: {idx}/{len(holdings)} ({idx/len(holdings)*100:.1f}%)", flush=True)
        
        print("\n" + "=" * 80, flush=True)
        print("업데이트 결과", flush=True)
        print("=" * 80, flush=True)
        print(f"성공: {success_count}개", flush=True)
        print(f"실패: {error_count}개", flush=True)
        print(f"\nEntity Type 통계:", flush=True)
        for entity_type, count in sorted(entity_type_stats.items(), key=lambda x: x[1], reverse=True):
            print(f"  {entity_type}: {count}개", flush=True)
        
        if dry_run:
            print("\n⚠️  실제 업데이트를 원하시면 --apply 플래그를 사용하세요:", flush=True)
            print("  python scripts/update_entity_type_soft.py --apply", flush=True)
        
    except Exception as e:
        import traceback
        print(f"\n❌ 오류 발생: {e}", flush=True)
        traceback.print_exc()
        db.rollback()
        raise
    finally:
        db.close()

def main():
    import sys
    dry_run = '--apply' not in sys.argv
    
    update_entity_type_soft(dry_run=dry_run)

if __name__ == '__main__':
    main()

