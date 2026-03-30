from surge_scanner import run_surge_scan
results, sess = run_surge_scan('daytrade')
print('세션:', sess)
print('결과 종목수:', len(results))
for r in results[:5]:
    print(r['symbol'], r['change_pct'], r['vol_ratio'], r['score'])
