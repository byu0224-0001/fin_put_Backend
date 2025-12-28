# -*- coding: utf-8 -*-
"""상장 폐지 의심 기업 확인"""
import sys
import os
from pathlib import Path
from datetime import datetime

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

if sys.platform == 'win32':
    os.environ['PYTHONIOENCODING'] = 'utf-8'

from pykrx import stock

# 상장 폐지 의심 기업
tickers_to_check = ['151910', '010620', '335890', '006390', '102280', '014200']
names_to_check = ['퓨처코어', 'HD현대미포', '비올', '한일현대시멘트', '쌍방울', '광림']

print('=== 상장 폐지 의심 기업 확인 ===')
today = datetime.now().strftime('%Y%m%d')

kospi = set(stock.get_market_ticker_list(today, market='KOSPI'))
kosdaq = set(stock.get_market_ticker_list(today, market='KOSDAQ'))
all_tickers = kospi | kosdaq

print(f'현재 상장: KOSPI {len(kospi)}개, KOSDAQ {len(kosdaq)}개')
print()

for ticker, old_name in zip(tickers_to_check, names_to_check):
    if ticker in all_tickers:
        new_name = stock.get_market_ticker_name(ticker)
        market = 'KOSPI' if ticker in kospi else 'KOSDAQ'
        print(f'{ticker}: O 상장중 ({market}) - {new_name}')
    else:
        print(f'{ticker}: X 상장폐지 또는 티커변경 - (구 {old_name})')

# HD현대미포 새 티커 검색
print()
print('=== "미포" 또는 "HD현대" 검색 ===')
for t in all_tickers:
    n = stock.get_market_ticker_name(t)
    if n and ('미포' in n or 'HD현대' in n):
        market = 'KOSPI' if t in kospi else 'KOSDAQ'
        print(f'{t}: {n} ({market})')

