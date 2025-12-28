# -*- coding: utf-8 -*-
"""
전수 재분류 스크립트 (GPT 피드백: Soft 업데이트 실행 및 전수 재분류)

2,600개 기업의 섹터와 엔티티 타입을 최신 로직으로 재분류
"""
import sys
import os
import json
from datetime import datetime

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
from app.services.sector_classifier import classify_sector_rule_based
from app.services.entity_type_classifier import classify_entity_type, update_classification_meta
from app.services.sector_classifier import classify_sector_rule_based

def reclassify_all_companies(dry_run=True, batch_size=100, ticker_filter=None):
    """전수 재분류 실행
    
    Args:
        dry_run: DRY RUN 모드 여부
        batch_size: 배치 크기
        ticker_filter: 특정 티커만 재분류 (예: '096770')
    """
    db = SessionLocal()
    
    try:
        print("=" * 80, flush=True)
        print("전수 재분류 실행", flush=True)
        if ticker_filter:
            print(f"필터: {ticker_filter}만 재분류", flush=True)
        print("=" * 80, flush=True)
        
        if dry_run:
            print("\n⚠️  DRY RUN 모드 (실제 수정하지 않음)", flush=True)
        else:
            print("\n✅ 실제 수정 모드", flush=True)
        
        # 전체 기업 조회 (티커 필터 적용)
        if ticker_filter:
            all_stocks = db.query(Stock).filter(Stock.ticker == ticker_filter).all()
        else:
            all_stocks = db.query(Stock).all()
        total_count = len(all_stocks)
        
        print(f"\n전체 기업 수: {total_count}개", flush=True)
        print(f"배치 크기: {batch_size}개", flush=True)
        
        stats = {
            'total': total_count,
            'processed': 0,
            'updated': 0,
            'hold': 0,
            'error': 0,
            'sector_changes': {},
            'confidence_distribution': {'HIGH': 0, 'MEDIUM': 0, 'LOW': 0, 'HOLD': 0},
            'entity_type_distribution': {},
            # 🆕 DRY RUN 산출물: Top100 변화 리스트
            'top100_sector_changes': [],
            'top100_entity_type_changes': [],
            'top100_hold': [],
            'top200_hold_count': 0
        }
        
        for idx, stock in enumerate(all_stocks, 1):
            try:
                detail = db.query(CompanyDetail).filter(
                    CompanyDetail.ticker == stock.ticker
                ).order_by(CompanyDetail.updated_at.desc()).first()
                
                if not detail:
                    continue
                
                # 기존 섹터 조회
                existing_sector = db.query(InvestorSector).filter(
                    InvestorSector.ticker == stock.ticker,
                    InvestorSector.is_primary == True
                ).first()
                
                # 재분류
                new_sector, new_sub, new_vc, new_conf, boosting_log_from_classifier = classify_sector_rule_based(
                    detail, stock.stock_name if stock else None, ticker=stock.ticker
                )
                
                # Entity Type 분류
                entity_type, entity_conf, entity_meta = classify_entity_type(stock, detail)
                
                # 통계 업데이트
                stats['processed'] += 1
                
                if new_conf and (new_conf == 'HOLD' or new_conf.startswith('HOLD:')):
                    stats['hold'] += 1
                    if new_conf.startswith('HOLD:'):
                        hold_reason = new_conf.split(':', 1)[1]
                        # 🆕 수정: HOLD_ 접두사 중복 방지
                        if not hold_reason.startswith('HOLD_'):
                            stats['confidence_distribution'][f'HOLD_{hold_reason}'] = stats['confidence_distribution'].get(f'HOLD_{hold_reason}', 0) + 1
                        else:
                            stats['confidence_distribution'][hold_reason] = stats['confidence_distribution'].get(hold_reason, 0) + 1
                    else:
                        stats['confidence_distribution']['HOLD'] += 1
                else:
                    stats['confidence_distribution'][new_conf] = stats['confidence_distribution'].get(new_conf, 0) + 1
                
                stats['entity_type_distribution'][entity_type] = stats['entity_type_distribution'].get(entity_type, 0) + 1
                
                # 섹터 변경 확인
                old_entity_type = None
                if existing_sector:
                    old_sector = existing_sector.major_sector
                    # 기존 Entity Type 추출
                    if existing_sector.boosting_log and isinstance(existing_sector.boosting_log, dict):
                        old_meta = existing_sector.boosting_log.get('classification_meta', {})
                        old_entity_type = old_meta.get('entity_type')
                    
                    if new_sector and new_sector != old_sector:
                        change_key = f"{old_sector} → {new_sector}"
                        stats['sector_changes'][change_key] = stats['sector_changes'].get(change_key, 0) + 1
                        stats['updated'] += 1
                        
                        # 🆕 Top100 섹터 변경 리스트 수집
                        change_info = {
                            'ticker': stock.ticker,
                            'name': stock.stock_name,
                            'market_cap': stock.market_cap,
                            'old_sector': old_sector,
                            'new_sector': new_sector,
                            'confidence': new_conf
                        }
                        stats['top100_sector_changes'].append(change_info)
                elif new_sector:
                    stats['updated'] += 1
                
                # 🆕 Entity Type 변경 확인
                if old_entity_type and old_entity_type != entity_type:
                    change_info = {
                        'ticker': stock.ticker,
                        'name': stock.stock_name,
                        'market_cap': stock.market_cap,
                        'old_entity_type': old_entity_type,
                        'new_entity_type': entity_type
                    }
                    stats['top100_entity_type_changes'].append(change_info)
                
                # 🆕 HOLD Top100 리스트 수집
                if new_conf and (new_conf == 'HOLD' or new_conf.startswith('HOLD:')):
                    # detail이 None인 경우 안전하게 처리
                    has_revenue_data = False
                    if detail and hasattr(detail, 'revenue_by_segment'):
                        has_revenue_data = bool(detail.revenue_by_segment and isinstance(detail.revenue_by_segment, dict) and len(detail.revenue_by_segment) > 0)
                    
                    hold_info = {
                        'ticker': stock.ticker,
                        'name': stock.stock_name,
                        'market_cap': stock.market_cap,
                        'hold_reason': new_conf.split(':', 1)[1] if ':' in new_conf else 'HOLD',
                        'has_revenue_data': has_revenue_data
                    }
                    stats['top100_hold'].append(hold_info)
                    
                    # 🆕 Top200 HOLD 카운트
                    if stock.market_cap and stock.market_cap >= 1000000000000:  # 1조 이상 (대략 Top 200)
                        stats['top200_hold_count'] += 1
                
                # DB 업데이트 (실제 모드일 때만)
                # 🆕 수정: HOLD 기업도 저장 (new_sector가 None이어도 confidence는 저장)
                if not dry_run:
                    # 🆕 boosting_log 병합: classify_sector_rule_based의 반환값을 우선 사용
                    # 이렇게 하면 hold_reason_code가 누락되지 않음
                    if boosting_log_from_classifier and boosting_log_from_classifier.get('classification_meta'):
                        # classify_sector_rule_based의 classification_meta를 기본으로 사용
                        classification_meta = boosting_log_from_classifier['classification_meta'].copy()
                    else:
                        classification_meta = {}
                    
                    # entity_type 정보는 entity_type_classifier 결과로 덮어쓰기 (우선순위 높음)
                    entity_type_meta = update_classification_meta(
                        None, entity_type, entity_conf, entity_meta
                    )
                    classification_meta.update(entity_type_meta)
                    
                    # 🆕 HOLD인 경우 hold_reason_code가 confidence에서 추출되어야 함
                    if new_conf and new_conf.startswith('HOLD:'):
                        hold_reason_code = new_conf.split(':', 1)[1] if ':' in new_conf else 'HOLD_UNKNOWN'
                        classification_meta['hold_reason_code'] = hold_reason_code
                        classification_meta['hold_reason'] = hold_reason_code  # 하위 호환성
                    
                    # boosting_log 업데이트
                    # 🆕 기존 boosting_log를 유지하되 classification_meta는 새로 설정
                    if existing_sector and existing_sector.boosting_log:
                        boosting_log = existing_sector.boosting_log.copy() if isinstance(existing_sector.boosting_log, dict) else {}
                    else:
                        boosting_log = {}
                    
                    # classification_meta는 항상 새로 설정 (덮어쓰기)
                    boosting_log['classification_meta'] = classification_meta
                    
                    # 🆕 classification_method도 boosting_log_from_classifier에서 가져오기
                    if boosting_log_from_classifier and boosting_log_from_classifier.get('classification_method'):
                        boosting_log['classification_method'] = boosting_log_from_classifier['classification_method']
                    
                    if existing_sector:
                        # 기존 레코드 업데이트 (HOLD인 경우도 confidence 저장)
                        existing_sector.major_sector = new_sector  # None일 수 있음 (HOLD)
                        existing_sector.sub_sector = new_sub
                        existing_sector.value_chain = new_vc
                        existing_sector.confidence = new_conf  # 🆕 HOLD도 저장
                        existing_sector.boosting_log = boosting_log
                        existing_sector.updated_at = datetime.utcnow()
                    else:
                        # 🆕 새 레코드 생성 (HOLD인 경우도 생성 - confidence와 boosting_log 저장 필요)
                        # HOLD인 경우 id는 ticker만 사용
                        sector_id = f"{stock.ticker}_{new_sector}" if new_sector else f"{stock.ticker}_HOLD"
                        classification_method = boosting_log.get('classification_method', 'RULE_BASED') if boosting_log else 'RULE_BASED'
                        
                        new_investor_sector = InvestorSector(
                            id=sector_id,
                            ticker=stock.ticker,
                            major_sector=new_sector,  # None일 수 있음 (HOLD)
                            sub_sector=new_sub,
                            value_chain=new_vc,
                            confidence=new_conf,  # HOLD도 저장
                            is_primary=True,
                            classification_method=classification_method,
                            boosting_log=boosting_log
                        )
                        db.add(new_investor_sector)
                    
                    # 배치 커밋
                    if idx % batch_size == 0:
                        db.commit()
                        print(f"  진행: {idx}/{total_count} ({idx/total_count*100:.1f}%) - 커밋 완료", flush=True)
                
                # 진행 상황 출력
                if idx % 100 == 0:
                    print(f"  진행: {idx}/{total_count} ({idx/total_count*100:.1f}%)", flush=True)
                    
            except Exception as e:
                stats['error'] += 1
                # stock.ticker 접근 시 세션이 만료될 수 있으므로 안전하게 처리
                ticker_str = getattr(stock, 'ticker', 'UNKNOWN') if hasattr(stock, 'ticker') else 'UNKNOWN'
                print(f"  ❌ 오류 ({ticker_str}): {e}", flush=True)
                if not dry_run:
                    try:
                        db.rollback()
                    except:
                        pass  # 이미 롤백된 경우 무시
                continue
        
        # 최종 커밋
        if not dry_run:
            db.commit()
        
        # 결과 리포트
        print("\n" + "=" * 80, flush=True)
        print("재분류 결과", flush=True)
        print("=" * 80, flush=True)
        
        print(f"\n[처리 통계]", flush=True)
        print(f"  전체: {stats['total']}개", flush=True)
        print(f"  처리 완료: {stats['processed']}개", flush=True)
        print(f"  업데이트: {stats['updated']}개", flush=True)
        print(f"  HOLD: {stats['hold']}개 ({stats['hold']/stats['processed']*100:.1f}%)", flush=True)
        print(f"  오류: {stats['error']}개", flush=True)
        
        print(f"\n[Confidence 분포]", flush=True)
        for conf, count in sorted(stats['confidence_distribution'].items(), key=lambda x: x[1], reverse=True):
            if count > 0:
                print(f"  {conf}: {count}개", flush=True)
        
        print(f"\n[Entity Type 분포]", flush=True)
        for entity, count in sorted(stats['entity_type_distribution'].items(), key=lambda x: x[1], reverse=True):
            print(f"  {entity}: {count}개", flush=True)
        
        if stats['sector_changes']:
            print(f"\n[섹터 변경 Top 10]", flush=True)
            sorted_changes = sorted(stats['sector_changes'].items(), key=lambda x: x[1], reverse=True)[:10]
            for change, count in sorted_changes:
                print(f"  {change}: {count}개", flush=True)
        
        # 🆕 Top100 변화 리스트 정렬 및 출력
        # 시가총액 기준 정렬
        stats['top100_sector_changes'].sort(key=lambda x: x.get('market_cap', 0) or 0, reverse=True)
        stats['top100_entity_type_changes'].sort(key=lambda x: x.get('market_cap', 0) or 0, reverse=True)
        stats['top100_hold'].sort(key=lambda x: x.get('market_cap', 0) or 0, reverse=True)
        
        # Top100만 유지
        stats['top100_sector_changes'] = stats['top100_sector_changes'][:100]
        stats['top100_entity_type_changes'] = stats['top100_entity_type_changes'][:100]
        stats['top100_hold'] = stats['top100_hold'][:100]
        
        print(f"\n[Top100 섹터 변경]", flush=True)
        print(f"  총 {len(stats['top100_sector_changes'])}개", flush=True)
        for i, change in enumerate(stats['top100_sector_changes'][:10], 1):
            print(f"  {i}. {change['name']} ({change['ticker']}): {change['old_sector']} → {change['new_sector']}", flush=True)
        
        print(f"\n[Top100 Entity Type 변경]", flush=True)
        print(f"  총 {len(stats['top100_entity_type_changes'])}개", flush=True)
        for i, change in enumerate(stats['top100_entity_type_changes'][:10], 1):
            print(f"  {i}. {change['name']} ({change['ticker']}): {change['old_entity_type']} → {change['new_entity_type']}", flush=True)
        
        print(f"\n[Top100 HOLD]", flush=True)
        print(f"  총 {len(stats['top100_hold'])}개", flush=True)
        print(f"  Top200 HOLD: {stats['top200_hold_count']}개", flush=True)
        top200_hold_ratio = (stats['top200_hold_count'] / 200 * 100) if stats['top200_hold_count'] > 0 else 0
        print(f"  Top200 HOLD 비율: {top200_hold_ratio:.1f}%", flush=True)
        for i, hold in enumerate(stats['top100_hold'][:10], 1):
            print(f"  {i}. {hold['name']} ({hold['ticker']}): {hold['hold_reason']}", flush=True)
        
        # 🆕 Go/No-Go 판정
        can_apply = top200_hold_ratio <= 10.0
        
        # 리포트 저장
        report = {
            'generated_at': datetime.now().isoformat(),
            'dry_run': dry_run,
            'stats': stats,
            'go_no_go': {
                'top200_hold_ratio': top200_hold_ratio,
                'top200_hold_count': stats['top200_hold_count'],
                'can_apply': can_apply
            }
        }
        
        os.makedirs('reports', exist_ok=True)
        report_file = 'reports/reclassify_all_companies_report.json'
        with open(report_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, ensure_ascii=False, indent=2, default=str)
        
        print(f"\n✅ 리포트 저장: {report_file}", flush=True)
        
        # 🆕 Go/No-Go 판정 출력
        print(f"\n[Go/No-Go 판정]", flush=True)
        if can_apply:
            print(f"  ✅ Go: Top200 HOLD 비율 {top200_hold_ratio:.1f}% ≤ 10%", flush=True)
            if not dry_run:
                print(f"  → --apply 실행 가능", flush=True)
        else:
            print(f"  ❌ No-Go: Top200 HOLD 비율 {top200_hold_ratio:.1f}% > 10%", flush=True)
            print(f"  → Top200 HOLD 비율을 10% 이하로 낮춘 후 재실행하세요.", flush=True)
        
        print("=" * 80, flush=True)
        
        return report
        
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
    
    # --ticker 옵션 파싱
    ticker_filter = None
    if '--ticker' in sys.argv:
        ticker_idx = sys.argv.index('--ticker')
        if ticker_idx + 1 < len(sys.argv):
            ticker_filter = sys.argv[ticker_idx + 1]
    
    reclassify_all_companies(dry_run=dry_run, ticker_filter=ticker_filter)

if __name__ == '__main__':
    main()

