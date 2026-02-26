import requests
import json
import zipfile
import io
import csv
from datetime import datetime

TFF_URLS = [
    "https://www.cftc.gov/files/dea/history/fut_fin_txt_{year}.zip",
    "https://www.cftc.gov/files/dea/history/fut_fin_xls_{year}.zip",
]

# Legacy Futures Only — contains USD Index (ICE Futures)
LEGACY_URLS = [
    "https://www.cftc.gov/files/dea/history/fut_2_txt_{year}.zip",
    "https://www.cftc.gov/files/dea/history/com_2_txt_{year}.zip",
]

TFF_CODES = {
    "EUR": "099741",
    "GBP": "096742",
    "JPY": "097741",
    "CHF": "092741",
    "CAD": "090741",
    "AUD": "232741",
    "NZD": "112741",
}

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
                    # Print first row keys for debugging
                    if rows:
                        code_col = [k for k in rows[0].keys() if 'Market_Code' in k]
                        print(f"  Market code columns: {code_col}")
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

def find_rows(all_rows, code, name_search):
    """Try matching by both possible column name variants + name fallback"""
    # Try both column name variants
    for col in ['CFTC_Contract_Market_Code', 'CFTC_Contract_MarketCode']:
        matched = [r for r in all_rows if r.get(col, '').strip() == code]
        if matched:
            print(f"  Matched {len(matched)} rows via {col}")
            return matched
    # Fallback: name search
    matched = [r for r in all_rows if name_search.upper() in r.get('Market_and_Exchange_Names', '').upper()]
    if matched:
        print(f"  Matched {len(matched)} rows via name '{name_search}'")
    return matched

def process_tff_rows(rows):
    """Process TFF-format rows using Lev_Money and Asset_Mgr columns"""
    rows.sort(key=parse_date, reverse=True)
    seen, unique = set(), []
    for r in rows:
        d = parse_date(r).strftime('%Y-%m-%d')
        if d not in seen:
            seen.add(d)
            unique.append(r)

    weekly = []
    for row in unique[:52]:
        lev_long    = safe_int(row, 'Lev_Money_Positions_Long_All')
        lev_short   = safe_int(row, 'Lev_Money_Positions_Short_All')
        asset_long  = safe_int(row, 'Asset_Mgr_Positions_Long_All')
        asset_short = safe_int(row, 'Asset_Mgr_Positions_Short_All')
        dealer_long = safe_int(row, 'Dealer_Positions_Long_All')
        dealer_short= safe_int(row, 'Dealer_Positions_Short_All')
        nr_long     = safe_int(row, 'NonRept_Positions_Long_All')
        nr_short    = safe_int(row, 'NonRept_Positions_Short_All')
        net_lev     = lev_long - lev_short
        net_asset   = asset_long - asset_short
        weekly.append({
            "date": parse_date(row).strftime('%Y-%m-%d'),
            "lev_long": lev_long, "lev_short": lev_short, "net_lev": net_lev,
            "asset_long": asset_long, "asset_short": asset_short, "net_asset": net_asset,
            "dealer_long": dealer_long, "dealer_short": dealer_short,
            "nonrept_long": nr_long, "nonrept_short": nr_short,
            "net_noncomm": net_lev + net_asset,
            "noncomm_long": lev_long, "noncomm_short": lev_short,
            "comm_long": asset_long, "comm_short": asset_short, "net_comm": net_asset,
        })
    return weekly

def process_legacy_rows(rows):
    """Process Legacy-format rows using NonComm columns"""
    rows.sort(key=parse_date, reverse=True)
    seen, unique = set(), []
    for r in rows:
        d = parse_date(r).strftime('%Y-%m-%d')
        if d not in seen:
            seen.add(d)
            unique.append(r)

    weekly = []
    for row in unique[:52]:
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

def build_result(weekly):
    return {
        "weeks": weekly,
        "latest": weekly[0],
        "52w_high": max(w["net_noncomm"] for w in weekly),
        "52w_low":  min(w["net_noncomm"] for w in weekly),
        "cot_index": weekly[0]["cot_index"],
    }

def fetch_cot_data():
    current_year = datetime.now().year
    tff_rows = []

    # Fetch TFF data
    for year in [current_year, current_year - 1]:
        for url_template in TFF_URLS:
            content = fetch_zip(url_template.format(year=year))
            if content:
                tff_rows.extend(parse_zip(content))
                break

    print(f"\nTotal TFF rows: {len(tff_rows)}")

    if not tff_rows:
        with open("cot_data.json", "w") as f:
            json.dump({"updated_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
                       "error": "Could not fetch TFF data", "data": {}}, f, indent=2)
        return

    results = {}

    # Process TFF currencies (EUR, GBP, JPY, CHF, CAD, AUD, NZD)
    for code, cftc_code in TFF_CODES.items():
        print(f"\n{code}: searching...")
        matched = find_rows(tff_rows, cftc_code, NAME_MAP[code])
        if not matched:
            print(f"  -> no data found")
            continue
        weekly = add_cot_index(process_tff_rows(matched))
        results[code] = build_result(weekly)
        print(f"  -> {len(weekly)} weeks | net_lev: {weekly[0]['net_lev']:+,} | COT Index: {weekly[0]['cot_index']}")

    # Try USD in TFF first (it might be there)
    print(f"\nUSD: searching in TFF...")
    usd_matched = find_rows(tff_rows, USD_CODE, NAME_MAP["USD"])

    if usd_matched:
        weekly = add_cot_index(process_tff_rows(usd_matched))
        results["USD"] = build_result(weekly)
        print(f"  -> USD found in TFF! {len(weekly)} weeks | COT Index: {weekly[0]['cot_index']}")
    else:
        # Fetch Legacy file for USD — fetch BOTH years for full 52-week history
        print("  USD not in TFF, fetching Legacy file...")
        legacy_rows = []
        for year in [current_year, current_year - 1]:
            fetched = False
            for url_template in LEGACY_URLS:
                content = fetch_zip(url_template.format(year=year))
                if content:
                    rows = parse_zip(content)
                    if rows:
                        legacy_rows.extend(rows)
                        fetched = True
                        break
            if not fetched:
                print(f"  Warning: could not fetch legacy data for {year}")

        print(f"\nTotal Legacy rows: {len(legacy_rows)}")
        if legacy_rows:
            usd_matched = find_rows(legacy_rows, USD_CODE, NAME_MAP["USD"])
            if usd_matched:
                weekly = add_cot_index(process_legacy_rows(usd_matched))
                results["USD"] = build_result(weekly)
                print(f"  -> USD found in Legacy! {len(weekly)} weeks | COT Index: {weekly[0]['cot_index']}")
            else:
                print("  USD not found in Legacy either")
                # Print sample names to debug
                sample_names = list(set(r.get('Market_and_Exchange_Names','') for r in legacy_rows[:500]))
                icus = [n for n in sample_names if 'DOLLAR' in n.upper() or 'ICUS' in n.upper()]
                print(f"  Dollar-related markets found: {icus[:10]}")

    output = {
        "updated_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
        "data": results
    }
    with open("cot_data.json", "w") as f:
        json.dump(output, f, indent=2)
    print(f"\nSaved cot_data.json with {len(results)} currencies: {list(results.keys())}")

if __name__ == "__main__":
    fetch_cot_data()
