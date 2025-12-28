# -*- coding: utf-8 -*-
"""
Audit Trail 시각화 준비 스크립트 (GPT 피드백: Audit Trail 시각화 준비)

classification_meta에 저장된 정보를 활용하여 분류 근거를 시각화할 수 있는 데이터 생성
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

def generate_audit_trail(sample_tickers=None, limit=50):
    """Audit Trail 데이터 생성"""
    db = SessionLocal()
    
    try:
        print("=" * 80, flush=True)
        print("Audit Trail 시각화 데이터 생성", flush=True)
        print("=" * 80, flush=True)
        
        # 샘플 기업 선택
        if sample_tickers:
            stocks = db.query(Stock).filter(Stock.ticker.in_(sample_tickers)).all()
        else:
            # 다양한 섹터와 Confidence 레벨의 기업 선택
            sectors = db.query(InvestorSector).filter(
                InvestorSector.is_primary == True
            ).limit(limit).all()
            
            tickers = [s.ticker for s in sectors]
            stocks = db.query(Stock).filter(Stock.ticker.in_(tickers)).all()
        
        print(f"\n선택된 기업: {len(stocks)}개", flush=True)
        
        audit_trails = []
        
        for stock in stocks:
            detail = db.query(CompanyDetail).filter(
                CompanyDetail.ticker == stock.ticker
            ).order_by(CompanyDetail.updated_at.desc()).first()
            
            sector = db.query(InvestorSector).filter(
                InvestorSector.ticker == stock.ticker,
                InvestorSector.is_primary == True
            ).first()
            
            if not sector:
                continue
            
            # classification_meta 추출
            boosting_log = sector.boosting_log or {}
            classification_meta = boosting_log.get('classification_meta', {})
            
            # Audit Trail 구성
            audit_trail = {
                'ticker': stock.ticker,
                'name': stock.stock_name,
                'industry': stock.industry_raw,
                'market_cap': stock.market_cap,
                'classification': {
                    'major_sector': sector.major_sector,
                    'sub_sector': sector.sub_sector,
                    'value_chain': sector.value_chain,
                    'confidence': sector.confidence,
                    'classification_method': sector.classification_method
                },
                'scores': {
                    'rule_score': sector.rule_score,
                    'embedding_score': sector.embedding_score,
                    'bge_score': sector.bge_score,
                    'gpt_score': sector.gpt_score,
                    'ensemble_score': sector.ensemble_score
                },
                'entity_type': {
                    'type': classification_meta.get('entity_type'),
                    'confidence': classification_meta.get('entity_confidence'),
                    'holding_type': classification_meta.get('holding_type'),
                    'evidence': classification_meta.get('evidence', [])
                },
                'revenue_data': {
                    'has_data': bool(detail and detail.revenue_by_segment),
                    'revenue_by_segment': detail.revenue_by_segment if detail else None,
                    'coverage': _calculate_coverage(detail.revenue_by_segment if detail else None)
                },
                'reasoning': {
                    'classification_reasoning': sector.classification_reasoning,
                    'hold_reason': classification_meta.get('hold_reason_code') if sector.confidence and sector.confidence.startswith('HOLD:') else None
                },
                'metadata': {
                    'last_updated': sector.updated_at.isoformat() if sector.updated_at else None,
                    'rule_version': sector.rule_version,
                    'rule_confidence': sector.rule_confidence
                }
            }
            
            audit_trails.append(audit_trail)
        
        # 시각화용 요약 생성
        summary = {
            'total_companies': len(audit_trails),
            'by_sector': {},
            'by_confidence': {},
            'by_entity_type': {},
            'coverage_stats': {
                'with_revenue_data': len([a for a in audit_trails if a['revenue_data']['has_data']]),
                'avg_coverage': sum([a['revenue_data']['coverage'] for a in audit_trails if a['revenue_data']['has_data']]) / max(len([a for a in audit_trails if a['revenue_data']['has_data']]), 1)
            }
        }
        
        for trail in audit_trails:
            sector = trail['classification']['major_sector']
            summary['by_sector'][sector] = summary['by_sector'].get(sector, 0) + 1
            
            confidence = trail['classification']['confidence']
            summary['by_confidence'][confidence] = summary['by_confidence'].get(confidence, 0) + 1
            
            entity_type = trail['entity_type']['type']
            if entity_type:
                summary['by_entity_type'][entity_type] = summary['by_entity_type'].get(entity_type, 0) + 1
        
        # 결과 저장
        result = {
            'generated_at': datetime.now().isoformat(),
            'summary': summary,
            'audit_trails': audit_trails
        }
        
        # 콘솔 출력
        print("\n" + "=" * 80, flush=True)
        print("Audit Trail 요약", flush=True)
        print("=" * 80, flush=True)
        
        print(f"\n[전체 통계]", flush=True)
        print(f"  총 기업: {summary['total_companies']}개", flush=True)
        print(f"  매출 데이터 보유: {summary['coverage_stats']['with_revenue_data']}개", flush=True)
        print(f"  평균 커버리지: {summary['coverage_stats']['avg_coverage']:.1f}%", flush=True)
        
        print(f"\n[섹터별 분포]", flush=True)
        for sector, count in sorted(summary['by_sector'].items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {sector}: {count}개", flush=True)
        
        print(f"\n[Confidence 분포]", flush=True)
        for conf, count in sorted(summary['by_confidence'].items(), key=lambda x: x[1], reverse=True):
            print(f"  {conf}: {count}개", flush=True)
        
        print(f"\n[Entity Type 분포]", flush=True)
        for entity, count in sorted(summary['by_entity_type'].items(), key=lambda x: x[1], reverse=True):
            print(f"  {entity}: {count}개", flush=True)
        
        # 샘플 Audit Trail 출력
        print(f"\n[샘플 Audit Trail (Top 3)]", flush=True)
        for i, trail in enumerate(audit_trails[:3], 1):
            print(f"\n{i}. {trail['name']} ({trail['ticker']})", flush=True)
            print(f"   섹터: {trail['classification']['major_sector']} (Confidence: {trail['classification']['confidence']})", flush=True)
            print(f"   Entity Type: {trail['entity_type']['type']}", flush=True)
            print(f"   매출 데이터: {'있음' if trail['revenue_data']['has_data'] else '없음'}", flush=True)
            if trail['revenue_data']['has_data']:
                print(f"   커버리지: {trail['revenue_data']['coverage']:.1f}%", flush=True)
            if trail['reasoning']['classification_reasoning']:
                print(f"   분류 근거: {trail['reasoning']['classification_reasoning'][:100]}...", flush=True)
        
        # 파일 저장
        os.makedirs('reports', exist_ok=True)
        output_file = 'reports/audit_trail_data.json'
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2, default=str)
        
        # HTML 시각화 템플릿 생성 (간단한 예시)
        html_file = 'reports/audit_trail_visualization.html'
        _generate_html_template(audit_trails[:20], html_file)
        
        print(f"\n✅ Audit Trail 데이터 저장:", flush=True)
        print(f"  - JSON: {output_file}", flush=True)
        print(f"  - HTML: {html_file}", flush=True)
        print("=" * 80, flush=True)
        
        return result
        
    except Exception as e:
        import traceback
        print(f"\n❌ 오류 발생: {e}", flush=True)
        traceback.print_exc()
        raise
    finally:
        db.close()

def _calculate_coverage(revenue_by_segment):
    """매출 데이터 커버리지 계산"""
    if not revenue_by_segment or not isinstance(revenue_by_segment, dict):
        return 0.0
    
    total_pct = sum(p for p in revenue_by_segment.values() if isinstance(p, (int, float)))
    return total_pct

def _generate_html_template(audit_trails, output_file):
    """HTML 시각화 템플릿 생성"""
    html_content = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Audit Trail 시각화</title>
    <style>
        body {{
            font-family: 'Malgun Gothic', sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            border-bottom: 3px solid #4CAF50;
            padding-bottom: 10px;
        }}
        .audit-item {{
            border: 1px solid #ddd;
            margin: 15px 0;
            padding: 15px;
            border-radius: 5px;
            background: #fafafa;
        }}
        .audit-item h3 {{
            color: #2196F3;
            margin-top: 0;
        }}
        .info-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 10px;
            margin: 10px 0;
        }}
        .info-item {{
            background: white;
            padding: 8px;
            border-radius: 4px;
            border-left: 3px solid #4CAF50;
        }}
        .info-item strong {{
            color: #666;
            display: block;
            margin-bottom: 5px;
        }}
        .confidence {{
            display: inline-block;
            padding: 3px 8px;
            border-radius: 3px;
            font-size: 0.9em;
            font-weight: bold;
        }}
        .confidence.HIGH {{
            background: #4CAF50;
            color: white;
        }}
        .confidence.MEDIUM {{
            background: #FF9800;
            color: white;
        }}
        .confidence.LOW {{
            background: #f44336;
            color: white;
        }}
        .confidence.HOLD {{
            background: #9E9E9E;
            color: white;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Audit Trail 시각화</h1>
        <p>생성 일시: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>총 {len(audit_trails)}개 기업</p>
"""
    
    for trail in audit_trails:
        conf_class = trail['classification']['confidence'].split(':')[0] if ':' in trail['classification']['confidence'] else trail['classification']['confidence']
        html_content += f"""
        <div class="audit-item">
            <h3>{trail['name']} ({trail['ticker']})</h3>
            <div class="info-grid">
                <div class="info-item">
                    <strong>섹터</strong>
                    {trail['classification']['major_sector']} / {trail['classification']['sub_sector'] or 'N/A'}
                </div>
                <div class="info-item">
                    <strong>Confidence</strong>
                    <span class="confidence {conf_class}">{trail['classification']['confidence']}</span>
                </div>
                <div class="info-item">
                    <strong>Entity Type</strong>
                    {trail['entity_type']['type'] or 'OPERATING'}
                </div>
                <div class="info-item">
                    <strong>매출 데이터</strong>
                    {'있음' if trail['revenue_data']['has_data'] else '없음'}
                    {f" (커버리지: {trail['revenue_data']['coverage']:.1f}%)" if trail['revenue_data']['has_data'] else ''}
                </div>
            </div>
"""
        if trail['reasoning']['classification_reasoning']:
            html_content += f"""
            <div style="margin-top: 10px; padding: 10px; background: #e3f2fd; border-radius: 4px;">
                <strong>분류 근거:</strong><br>
                {trail['reasoning']['classification_reasoning'][:200]}...
            </div>
"""
        html_content += """
        </div>
"""
    
    html_content += """
    </div>
</body>
</html>
"""
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_content)

if __name__ == '__main__':
    import sys
    sample_tickers = sys.argv[1].split(',') if len(sys.argv) > 1 and sys.argv[1] else None
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 50
    
    generate_audit_trail(sample_tickers=sample_tickers, limit=limit)

