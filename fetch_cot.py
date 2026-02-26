import requests
import json
import zipfile
import io
import csv
from datetime import datetime

# TFF report — contains EUR, GBP, JPY, CHF, CAD, AUD, NZD
TFF_URLS = [
    "https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip",
    "https://www.cftc.gov/files/dea/history/fut_fin_xls_{year}.zip",
]

# Legacy report — contains USD Index (ICE Futures)
LEGACY_URLS = [
    "https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip",   # try TFF first (USD may be here too)
    "https://www.cftc.gov/files/dea/history/com_fin_txt_{year}.zip",   # legacy financial futures
    "https://www.cftc.gov/files/dea/history/fut_2_txt_{year}.zip",     # legacy futures only all
]

# Currencies from TFF report
TFF_CODES = {
    "EUR": "099741",
    "GBP": "096742",
    "JPY": "097741",
    "CHF": "092741",
    "CAD": "090741",
    "AUD": "232741",
    "NZD": "112741",
}

# USD Index from Legacy report
USD_CODE = "098662"

NAME_MAP = {
    "EUR": "EURO FX",
    "GBP": "BRITISH POUND",
    "JPY": "JAPANESE YEN",
    "CHF": "SWISS FRANC",
    "CAD": "CANADIAN DOLLAR",
    "AUD": "AUSTRALIAN DOLLAR",
    "NZD": "NEW ZEALAND DOLLAR",
    "USD": "U.S. DOLLAR INDEX",
}

def fetch_zip(url):
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
                    with z.open(filename) as f:
                        text = io.TextIOWrapper(f, encoding='utf-8', errors='replace')
                        reader = csv.DictReader(text)
                        for row in reader:
                            rows.append(dict(row))
                    print(f"  -> {len(rows)} rows from {filename}")
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

def process_tff_currency(rows, code, name_search):
    """Process TFF report rows for a currency — uses Lev_Money and Asset_Mgr columns"""
    matched = [r for r in rows if r.get('CFTC_Contract_MarketCode', '').strip() == code]
    if not matched:
        matched = [r for r in rows if name_search in r.get('Market_and_Exchange_Names', '').upper()]
    if not matched:
        return []

    matched.sort(key=parse_date, reverse=True)

    # Deduplicate by date
    seen, unique = set(), []
    for r in matched:
        d = parse_date(r).strftime('%Y-%m-%d')
        if d not in seen:
            seen.add(d)
            unique.append(r)

    weekly = []
    for row in unique[:52]:
        lev_long   = safe_int(row, 'Lev_Money_Positions_Long_All')
        lev_short  = safe_int(row, 'Lev_Money_Positions_Short_All')
        asset_long = safe_int(row, 'Asset_Mgr_Positions_Long_All')
        asset_short= safe_int(row, 'Asset_Mgr_Positions_Short_All')
        dealer_long= safe_int(row, 'Dealer_Positions_Long_All')
        dealer_short=safe_int(row, 'Dealer_Positions_Short_All')
        nr_long    = safe_int(row, 'NonRept_Positions_Long_All')
        nr_short   = safe_int(row, 'NonRept_Positions_Short_All')
        net_lev    = lev_long - lev_short
        net_asset  = asset_long - asset_short
        net_spec   = net_lev + net_asset
        weekly.append({
            "date": parse_date(row).strftime('%Y-%m-%d'),
            "lev_long": lev_long, "lev_short": lev_short, "net_lev": net_lev,
            "asset_long": asset_long, "asset_short": asset_short, "net_asset": net_asset,
            "dealer_long": dealer_long, "dealer_short": dealer_short,
            "nonrept_long": nr_long, "nonrept_short": nr_short,
            "net_noncomm": net_spec,
            "noncomm_long": lev_long, "noncomm_short": lev_short,
            "comm_long": asset_long, "comm_short": asset_short, "net_comm": net_asset,
        })
    return weekly

def process_legacy_usd(rows):
    """Process Legacy report rows for USD Index — uses NonComm columns"""
    matched = [r for r in rows if r.get('CFTC_Contract_MarketCode', '').strip() == USD_CODE]
    if not matched:
        matched = [r for r in rows if 'U.S. DOLLAR INDEX' in r.get('Market_and_Exchange_Names', '').upper()
                   or 'USD INDEX' in r.get('Market_and_Exchange_Names', '').upper()]

    print(f"USD: {len(matched)} rows found")
    if not matched:
        # Print sample market names to help debug
        names = list(set(r.get('Market_and_Exchange_Names', '') for r in rows[:200]))
        print(f"  Sample market names: {names[:10]}")
        return []

    matched.sort(key=parse_date, reverse=True)

    seen, unique = set(), []
    for r in matched:
        d = parse_date(r).strftime('%Y-%m-%d')
        if d not in seen:
            seen.add(d)
            unique.append(r)

    weekly = []
    for row in unique[:52]:
        # Legacy report uses NonComm columns
        nc_long  = safe_int(row, 'NonComm_Positions_Long_All')
        nc_short = safe_int(row, 'NonComm_Positions_Short_All')
        c_long   = safe_int(row, 'Comm_Positions_Long_All')
        c_short  = safe_int(row, 'Comm_Positions_Short_All')
        nr_long  = safe_int(row, 'NonRept_Positions_Long_All')
        nr_short = safe_int(row, 'NonRept_Positions_Short_All')
        net_nc   = nc_long - nc_short
        weekly.append({
            "date": parse_date(row).strftime('%Y-%m-%d'),
            "lev_long": nc_long, "lev_short": nc_short, "net_lev": net_nc,
            "asset_long": c_long, "asset_short": c_short, "net_asset": c_long - c_short,
            "dealer_long": 0, "dealer_short": 0,
            "nonrept_long": nr_long, "nonrept_short": nr_short,
            "net_noncomm": net_nc,
            "noncomm_long": nc_long, "noncomm_short": nc_short,
            "comm_long": c_long, "comm_short": c_short, "net_comm": c_long - c_short,
        })
    return weekly

def add_cot_index(weekly):
    if not weekly:
        return weekly
    nets = [w["net_noncomm"] for w in weekly]
    max_net, min_net = max(nets), min(nets)
    rng = max_net - min_net if max_net != min_net else 1
    for i, w in enumerate(weekly):
        w["cot_index"] = round((w["net_noncomm"] - min_net) / rng * 100, 1)
        w["wow_change"] = w["net_noncomm"] - weekly[i+1]["net_noncomm"] if i < len(weekly)-1 else 0
    return weekly

def fetch_cot_data():
    current_year = datetime.now().year
    tff_rows = []
    legacy_rows = []

    # Fetch TFF data (all currencies except USD)
    for year in [current_year, current_year - 1]:
        for url_template in TFF_URLS:
            content = fetch_zip(url_template.format(year=year))
            if content:
                tff_rows.extend(parse_zip(content))
                break

    # Fetch Legacy data for USD Index
    # USD Index is in the Legacy Futures Only report
    legacy_url_templates = [
        "https://www.cftc.gov/files/dea/history/fut_2_txt_{year}.zip",
        "https://www.cftc.gov/files/dea/history/f_year{year}.zip",
    ]
    for year in [current_year, current_year - 1]:
        for url_template in legacy_url_templates:
            content = fetch_zip(url_template.format(year=year))
            if content:
                rows = parse_zip(content)
                if rows:
                    legacy_rows.extend(rows)
                    break

    print(f"\nTFF rows: {len(tff_rows)}, Legacy rows: {len(legacy_rows)}")

    if not tff_rows:
        print("ERROR: No TFF data")
        with open("cot_data.json", "w") as f:
            json.dump({"updated_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
                       "error": "Could not fetch TFF data", "data": {}}, f, indent=2)
        return

    results = {}

    # Process TFF currencies
    for code, cftc_code in TFF_CODES.items():
        weekly = process_tff_currency(tff_rows, cftc_code, NAME_MAP[code])
        if not weekly:
            print(f"{code}: no data found")
            continue
        weekly = add_cot_index(weekly)
        results[code] = {
            "weeks": weekly, "latest": weekly[0],
            "52w_high": max(w["net_noncomm"] for w in weekly),
            "52w_low":  min(w["net_noncomm"] for w in weekly),
            "cot_index": weekly[0]["cot_index"],
        }
        print(f"{code}: {len(weekly)} weeks, COT Index={results[code]['cot_index']}")

    # Process USD from Legacy
    if legacy_rows:
        weekly = process_legacy_usd(legacy_rows)
        if weekly:
            weekly = add_cot_index(weekly)
            results["USD"] = {
                "weeks": weekly, "latest": weekly[0],
                "52w_high": max(w["net_noncomm"] for w in weekly),
                "52w_low":  min(w["net_noncomm"] for w in weekly),
                "cot_index": weekly[0]["cot_index"],
            }
            print(f"USD: {len(weekly)} weeks, COT Index={results['USD']['cot_index']}")
        else:
            print("USD: no data found in legacy file")
    else:
        print("USD: legacy file not fetched — will use synthetic in dashboard")

    output = {
        "updated_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
        "data": results
    }
    with open("cot_data.json", "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nSaved cot_data.json with {len(results)} currencies")

if __name__ == "__main__":
    fetch_cot_data()
