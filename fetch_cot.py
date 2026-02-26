import requests
import json
import zipfile
import io
import csv
from datetime import datetime

# CFTC Financial Futures COT report URLs (try both formats)
CFTC_URLS = [
    "https://www.cftc.gov/files/dea/history/fut_fin_xls_{year}.zip",
    "https://www.cftc.gov/files/dea/history/fin_fut_xls_{year}.zip",
]

# CFTC market codes for currency futures
CURRENCY_CODES = {
    "EUR": "099741",
    "GBP": "096742",
    "JPY": "097741",
    "CHF": "092741",
    "CAD": "090741",
    "AUD": "232741",
    "NZD": "112741",
}

def fetch_zip(year):
    for url_template in CFTC_URLS:
        url = url_template.format(year=year)
        print(f"Trying {url}...")
        try:
            resp = requests.get(url, timeout=60)
            if resp.status_code == 200:
                print(f"Got data from {url}")
                return resp.content
            else:
                print(f"  HTTP {resp.status_code}")
        except Exception as e:
            print(f"  Error: {e}")
    return None

def parse_zip(content):
    rows = []
    try:
        with zipfile.ZipFile(io.BytesIO(content)) as z:
            print(f"Files in zip: {z.namelist()}")
            for filename in z.namelist():
                if filename.endswith('.txt') or filename.endswith('.csv'):
                    print(f"Reading {filename}...")
                    with z.open(filename) as f:
                        text = io.TextIOWrapper(f, encoding='utf-8', errors='replace')
                        reader = csv.DictReader(text)
                        for row in reader:
                            rows.append(row)
                    print(f"  -> {len(rows)} rows loaded")
                    break
    except Exception as e:
        print(f"Error parsing zip: {e}")
    return rows

def fetch_cot_data():
    current_year = datetime.now().year
    all_rows = []

    for year in [current_year, current_year - 1]:
        content = fetch_zip(year)
        if content:
            rows = parse_zip(content)
            all_rows.extend(rows)
            print(f"Total rows so far: {len(all_rows)}")

    if not all_rows:
        print("ERROR: No data fetched at all!")
        fallback = {
            "updated_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
            "error": "Could not fetch CFTC data",
            "data": {}
        }
        with open("cot_data.json", "w") as f:
            json.dump(fallback, f, indent=2)
        return

    if all_rows:
        print(f"\nColumn names: {list(all_rows[0].keys())[:10]}...")

    results = {}

    for currency, code in CURRENCY_CODES.items():
        currency_rows = [r for r in all_rows
                        if r.get('CFTC_Contract_MarketCode', '').strip() == code]

        if not currency_rows:
            name_map = {
                "EUR": "EURO FX",
                "GBP": "BRITISH POUND",
                "JPY": "JAPANESE YEN",
                "CHF": "SWISS FRANC",
                "CAD": "CANADIAN DOLLAR",
                "AUD": "AUSTRALIAN DOLLAR",
                "NZD": "NEW ZEALAND DOLLAR",
            }
            search = name_map.get(currency, currency)
            currency_rows = [r for r in all_rows
                            if search in r.get('Market_and_Exchange_Names', '').upper()]

        print(f"{currency}: {len(currency_rows)} rows found")

        if not currency_rows:
            continue

        def parse_date(row):
            for key in ['Report_Date_as_YYYY-MM-DD', 'As_of_Date_In_Form_YYMMDD']:
                val = row.get(key, '').strip()
                if val:
                    try:
                        if len(val) == 6:
                            return datetime.strptime(val, '%y%m%d')
                        else:
                            return datetime.strptime(val[:10], '%Y-%m-%d')
                    except:
                        pass
            return datetime(2000, 1, 1)

        currency_rows.sort(key=parse_date, reverse=True)

        weekly_data = []
        for row in currency_rows[:52]:
            try:
                date_obj = parse_date(row)
                date_str = date_obj.strftime('%Y-%m-%d')

                def safe_int(key):
                    try:
                        return int(float(row.get(key, 0) or 0))
                    except:
                        return 0

                noncomm_long  = safe_int('NonComm_Positions_Long_All')
                noncomm_short = safe_int('NonComm_Positions_Short_All')
                comm_long     = safe_int('Comm_Positions_Long_All')
                comm_short    = safe_int('Comm_Positions_Short_All')
                nonrept_long  = safe_int('NonRept_Positions_Long_All')
                nonrept_short = safe_int('NonRept_Positions_Short_All')

                net_noncomm = noncomm_long - noncomm_short
                net_comm    = comm_long - comm_short

                weekly_data.append({
                    "date":          date_str,
                    "noncomm_long":  noncomm_long,
                    "noncomm_short": noncomm_short,
                    "comm_long":     comm_long,
                    "comm_short":    comm_short,
                    "nonrept_long":  nonrept_long,
                    "nonrept_short": nonrept_short,
                    "net_noncomm":   net_noncomm,
                    "net_comm":      net_comm,
                })
            except Exception as e:
                print(f"  Row parse error for {currency}: {e}")

        if not weekly_data:
            continue

        nets = [w["net_noncomm"] for w in weekly_data]
        max_net = max(nets)
        min_net = min(nets)
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
        print(f"  {currency}: {len(weekly_data)} weeks, COT Index = {results[currency]['cot_index']}")

    output = {
        "updated_at": datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC'),
        "data": results
    }

    with open("cot_data.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nSaved cot_data.json with {len(results)} currencies")

if __name__ == "__main__":
    fetch_cot_data()
