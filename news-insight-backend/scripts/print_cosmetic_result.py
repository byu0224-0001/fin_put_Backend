# -*- coding: utf-8 -*-
import json
import sys
sys.stdout.reconfigure(encoding='utf-8')

with open('reports/toss_cosmetic_keywords.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

print('=== Top 20 키워드 (키워드 + 제품 합산) ===')
for kw, count in data['top_keywords']:
    print(f'  {kw}: {count}회')

print()
print('=== 키워드만 Top 15 ===')
for kw, count in data['keyword_only']:
    print(f'  {kw}: {count}회')

