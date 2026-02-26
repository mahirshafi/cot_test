import requests
import json
import zipfile
import io
import csv
from datetime import datetime, timedelta

# CFTC Futures-Only COT report (Legacy format)
CFTC_URL = "https://www.cftc.gov/files/dea/history/fut_fin_xls_{year}.zip"

# CFTC market codes for currency futures
CURRENCY_CODES = {
    "EUR": "099741",  # Euro FX
    "GBP": "096742",  # British Pound
    "JPY": "097741",  # Japanese Yen
    "CHF": "092741",  # Swiss Franc
    "CAD": "090741",  # Canadian Dollar
    "AUD": "232741",  # Australian Dollar
    "NZD": "112741",  # New Zealand Dollar
}

def fetch_cot_data():
    results = {}
    current_year = datetime.now().year
    years_to_try = [current_year, current_year - 1]

    all_rows = []

    for year in years_to_try:
        url = CFTC_URL.format(year=year)
        print(f"Fetching {url}...")
        try:
            resp = requests.get(url, timeout=30)
            resp.raise_for_status()
            with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                # Find the CSV file inside the zip
                csv_files = [f for f in z.namelist() if f.endswith('.txt') or f.endswith('.csv')]
                if not csv_files:
                    print(f"No CSV found in zip for {year}")
                    continue
                with z.open(csv_files[0]) as f:
                    reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8', errors='replace'))
                    for row in reader:
                        all_rows.append(row)
            print(f"Loaded {year} data: {len(all_rows)} rows so far")
        except Exception as e:
            print(f"Error fetching {year}: {e}")

    if not all_rows:
        print("No data fetched, exiting")
        return {}

    # Parse and organize by currency
    for currency, code in CURRENCY_CODES.items():
        # Filter rows for this currency
        currency_rows = [r for r in all_rows if r.get('CFTC_Contract_MarketCode', '').strip() == code]
        if not currency_rows:
            # Try partial match on market name
            currency_rows = [r for r in all_rows if currency in r.get('Market_and_Exchange_Names', '').upper()]

        if not currency_rows:
            print(f"No data found for {currency} (code: {code})")
            continue

        # Sort by date descending
        def parse_date(row):
            try:
                return datetime.strptime(row.get('As_of_Date_In_Form_YYMMDD', '000101'), '%y%m%d')
            except:
                try:
                    return datetime.strptime(row.get('Report_Date_as_YYYY-MM-DD', '2000-01-01'), '%Y-%m-%d')
                except:
                    return datetime(2000, 1, 1)

        currency_rows.sort(key=parse_date, reverse=True)

        # Take last 52 weeks
        weekly_data = []
        for row in currency_rows[:52]:
            try:
                date_str = row.get('Report_Date_as_YYYY-MM-DD') or row.get('As_of_Date_In_Form_YYMMDD', '')
                if len(date_str) == 6:
                    date_str = datetime.strptime(date_str, '%y%m%d').strftime('%Y-%m-%d')

                noncomm_long = int(row.get('NonComm_Positions_Long_All', 0) or 0)
                noncomm_short = int(row.get('NonComm_Positions_Short_All', 0) or 0)
                comm_long = int(row.get('Comm_Positions_Long_All', 0) or 0)
                comm_short = int(row.get('Comm_Positions_Short_All', 0) or 0)
                nonrept_long = int(row.get('NonRept_Positions_Long_All', 0) or 0)
                nonrept_short = int(row.get('NonRept_Positions_Short_All', 0) or 0)

                net_noncomm = noncomm_long - noncomm_short
                net_comm = comm_long - comm_short

                weekly_data.append({
                    "date": date_str,
                    "noncomm_long": noncomm_long,
                    "noncomm_short": noncomm_short,
                    "comm_long": comm_long,
                    "comm_short": comm_short,
                    "nonrept_long": nonrept_long,
                    "nonrept_short": nonrept_short,
                    "net_noncomm": net_noncomm,
                    "net_comm": net_comm,
                })
            except Exception as e:
                print(f"Error parsing row for {currency}: {e}")
                continue

        if weekly_data:
            # Calculate COT Index (0-100) over available history
            nets = [w["net_noncomm"] for w in weekly_data]
            max_net = max(nets)
            min_net = min(nets)
            rng = max_net - min_net if max_net != min_net else 1

            for w in weekly_data:
                w["cot_index"] = round((w["net_noncomm"] - min_net) / rng * 100, 1)

            # Week-over-week change
            for i in range(len(weekly_data)):
                if i < len(weekly_data) - 1:
                    weekly_data[i]["wow_change"] = weekly_data[i]["net_noncomm"] - weekly_data[i+1]["net_noncomm"]
                else:
                    weekly_data[i]["wow_change"] = 0

            results[currency] = {
                "weeks": weekly_data,
                "latest": weekly_data[0] if weekly_data else {},
                "52w_high": max_net,
                "52w_low": min_net,
                "cot_index": weekly_data[0]["cot_index"] if weekly_data else 0,
            }
            print(f"{currency}: {len(weekly_data)} weeks loaded, COT Index: {results[currency]['cot_index']}")

    output = {
        "updated_at": datetime.utcnow().strftime('%Y-%m-%d %Human:%M UTC'),
        "data": results
    }

    with open("cot_data.json", "w") as f:
        json.dump(output, f, indent=2)

    print(f"\nSaved cot_data.json with {len(results)} currencies")
    return output

if __name__ == "__main__":
    fetch_cot_data()
