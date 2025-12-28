# -*- coding: utf-8 -*-
import sys
sys.path.insert(0, '.')
import pandas as pd
from app.db import SessionLocal
from app.models import CompanyDetail, Edge, Stock, EconomicVariable

s = SessionLocal()

# Companies
details = s.query(CompanyDetail).all()
company_data = []
for d in details:
    stock = s.query(Stock).filter(Stock.ticker == d.ticker).first()
    company_data.append({
        'ticker': d.ticker,
        'company_name': stock.stock_name if stock else '',
        'market': stock.market if stock else '',
        'industry_raw': stock.industry_raw if stock else '',
        'biz_summary': (d.biz_summary[:200] if d.biz_summary else ''),
        'products': str(d.products)[:200] if d.products else '',
        'supply_chain_count': len(d.supply_chain) if d.supply_chain else 0,
        'latest_report_date': str(d.latest_report_date) if d.latest_report_date else '',
        'is_financial': 'Y' if d.financial_value_chain else 'N'
    })

df = pd.DataFrame(company_data)
df.to_excel('data/export_company_details.xlsx', index=False)

# Edges
edges = s.query(Edge).all()
edge_data = [{'source': e.source_id, 'target': e.target_id, 'type': e.relation_type, 'weight': e.weight} for e in edges]
pd.DataFrame(edge_data).to_excel('data/export_edges.xlsx', index=False)

# Variables
variables = s.query(EconomicVariable).all()
var_data = [{'code': v.code, 'name_ko': v.name_ko, 'category': v.category} for v in variables]
pd.DataFrame(var_data).to_excel('data/export_economic_variables.xlsx', index=False)

s.close()

with open('data/export_summary.txt', 'w', encoding='utf-8') as f:
    f.write(f"Companies: {len(company_data)}\n")
    f.write(f"Edges: {len(edge_data)}\n")
    f.write(f"Variables: {len(var_data)}\n")

