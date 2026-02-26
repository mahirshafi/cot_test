import requests
import json
import zipfile
import io
import csv
from datetime import datetime

CFTC_URLS = [
    "https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip",
    "https://www.cftc.gov/files/dea/history/fut_fin_xls_{year}.zip",
]

CURRENCY_CODES = {
    "EUR": "099741",
    "GBP": "096742",
    "JPY": "097741",
    "CHF": "092741",
    "CAD": "090741",
    "AUD": "232741",
    "NZD": "112741",
}

NAME_MAP = {
    "EUR": "EURO FX",
    "GBP": "BRITISH POUND",
    "JPY": "JAPANESE YEN",
    "CHF": "SWISS FRANC",
    "CAD": "CANADIAN DOLLAR",
    "AUD": "AUSTRALIAN DOLLAR",
    "NZD": "NEW ZEALAND DOLLAR",
}

def fetch_zip(year):
    for url_template in CFTC_URLS:
        url = url_template.format(year=year)
        print(f"Trying {url}...")
        try:
            resp = requests.get(url, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
            print(f"  HTTP {resp.status_code}, size: {len(resp.content)} bytes")
            if resp.status_code == 200 and len(resp.content) > 1000:
                return resp.content
        except Exception as e:
            print(f"  Error: {e}")
    return None

def parse_zip(content):
    rows = []
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            print(f"  Files in zip: {z.namelist()}")
            for filename in z.namelist():
                if filename.endswith('.txt') or filename.endswith('.csv'):
                    print(f"  Reading {filename}...")
                    with z.open(filename) as f:
                        text = io.TextIOWrapper(f, encoding='utf-8', errors='replace')
                        reader = csv.DictReader(text)
                        for row in reader:
                            rows.append(dict(row))
                    print(f"  -> {len(rows)} rows")
                    # Print columns from first row for debugging
                    if rows:
                        print(f"  Columns: {list(rows[0].keys())}")
                    break
    except Exception as e:
        print(f"  Parse error: {e}")
    return rows

def parse_date(row):
    for key in ['Report_Date_as_YYYY-MM-DD', 'As_of_Date_In_Form_YYMMDD']:
        val = row.get(key, '').strip()
        if val:
            try:
                if '-' in val:
                    return datetime.strptime(val[:10], '%Y-%m-%d')
                elif len(val) == 6:
                    return datetime.strptime(val, '%y%m%d')
            except:
                pass
    return datetime(2000, 1, 1)

def safe_int(row, key):
    try:
        return int(float(row.get(key, 0) or 0))
    except:
        return 0

def fetch_cot_data():
    current_year = datetime.now().year
    all_rows = []

    for year in [current_year, current_year - 1]:
        content = fetch_zip(year)
        if content:
            rows = parse_zip(content)
            all_rows.extend(rows)
            print(f"Total rows: {len(all_rows)}")

    if not all_rows:
        print("ERROR: No data fetched")
        with open("cot_data.json", "w") as f:
            json.dump({"updated_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
                       "error": "Could not fetch CFTC data", "data": {}}, f, indent=2)
        return

    results = {}

    for currency, code in CURRENCY_CODES.items():
        rows = [r for r in all_rows if r.get('CFTC_Contract_MarketCode', '').strip() == code]

        if not rows:
            search = NAME_MAP[currency]
            rows = [r for r in all_rows if search in r.get('Market_and_Exchange_Names', '').upper()]

        print(f"{currency}: {len(rows)} rows found")
        if not rows:
            continue

        rows.sort(key=parse_date, reverse=True)

        # Remove duplicate dates (keep first = most recent entry per date)
        seen_dates = set()
        unique_rows = []
        for r in rows:
            d = parse_date(r).strftime('%Y-%m-%d')
            if d not in seen_dates:
                seen_dates.add(d)
                unique_rows.append(r)
        rows = unique_rows

        weekly_data = []
        for row in rows[:52]:
            try:
                date_str = parse_date(row).strftime('%Y-%m-%d')

                # TFF report column names:
                # Leveraged Funds (hedge funds, CTAs) = speculative money
                lev_long  = safe_int(row, 'Lev_Money_Positions_Long_All')
                lev_short = safe_int(row, 'Lev_Money_Positions_Short_All')

                # Asset Managers (institutional)
                asset_long  = safe_int(row, 'Asset_Mgr_Positions_Long_All')
                asset_short = safe_int(row, 'Asset_Mgr_Positions_Short_All')

                # Dealer/Intermediary
                dealer_long  = safe_int(row, 'Dealer_Positions_Long_All')
                dealer_short = safe_int(row, 'Dealer_Positions_Short_All')

                # Non-reportable (small specs)
                nonrept_long  = safe_int(row, 'NonRept_Positions_Long_All')
                nonrept_short = safe_int(row, 'NonRept_Positions_Short_All')

                # Net positions
                net_lev   = lev_long - lev_short
                net_asset = asset_long - asset_short
                # Combined speculative net (Lev Funds + Asset Mgr) = total large spec
                net_spec  = net_lev + net_asset

                weekly_data.append({
                    "date":         date_str,
                    # Leveraged Funds (primary COT signal)
                    "lev_long":     lev_long,
                    "lev_short":    lev_short,
                    "net_lev":      net_lev,
                    # Asset Managers
                    "asset_long":   asset_long,
                    "asset_short":  asset_short,
                    "net_asset":    net_asset,
                    # Dealer
                    "dealer_long":  dealer_long,
                    "dealer_short": dealer_short,
                    # Non-reportable
                    "nonrept_long":  nonrept_long,
                    "nonrept_short": nonrept_short,
                    # Combined spec net (used for COT Index)
                    "net_noncomm":  net_spec,
                    # Legacy field aliases for dashboard compatibility
                    "noncomm_long":  lev_long,
                    "noncomm_short": lev_short,
                    "comm_long":     asset_long,
                    "comm_short":    asset_short,
                    "net_comm":      net_asset,
                })
            except Exception as e:
                print(f"  Row error: {e}")

        if not weekly_data:
            continue

        nets = [w["net_noncomm"] for w in weekly_data]
        max_net, min_net = max(nets), min(nets)
        rng = max_net - min_net if max_net != min_net else 1

        for i, w in enumerate(weekly_data):
            w["cot_index"] = round((w["net_noncomm"] - min_net) / rng * 100, 1)
            w["wow_change"] = w["net_noncomm"] - weekly_data[i+1]["net_noncomm"] if i < len(weekly_data)-1 else 0

        results[currency] = {
            "weeks":     weekly_data,
            "latest":    weekly_data[0],
            "52w_high":  max_net,
            "52w_low":   min_net,
            "cot_index": weekly_data[0]["cot_index"],
        }
        print(f"  -> {currency}: {len(weekly_data)} weeks | Lev net: {weekly_data[0]['net_lev']:+,} | Asset net: {weekly_data[0]['net_asset']:+,} | COT Index: {results[currency]['cot_index']}")

    output = {
        "updated_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
        "data": results
    }

    with open("cot_data.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nSaved cot_data.json with {len(results)} currencies")

if __name__ == "__main__":
    fetch_cot_data()
