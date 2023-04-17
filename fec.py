import requests
import argparse
import json


# API url for schedule A receipts, including contributions from individuals
api_url = "https://api.open.fec.gov/v1/schedules/schedule_a/"


def download_entries_year_range(start, end, key = "DEMO_KEY"):
    start = (start//2+1)*2
    end = (end//2+2)*2
    parameters = {
        "sort_hide_null": "false",
        "per_page": "100",
        "sort_nulls_last": "false",
        "sort": "-contribution_receipt_date",
        "page": "1",
        "sort_null_only": "false",
        "api_key": key
    }
    entries = []
    for year in range(start, end, 2):
        print(year)
        parameters["two_year_transaction_period"] = year

        response = requests.get(api_url, params=parameters)
        results = response.json()["results"]

        entries += results
    return entries
    
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch data from FEC API")
    parser.add_argument("-k", "--api_key", metavar="", default="DEMO_KEY", help="your FEC API key, default: DEMO_KEY")
    parser.add_argument("-s", "--start", metavar="", default=1990, help="The first year for which data is requested. The data is returned in 2 year chunks and earlier data may be returned.")
    parser.add_argument("-e", "--end", metavar="", default=2025, help="The last year for which data is requested. The data is returned in 2 year chunks and later data may be returned.")
    parser.add_argument("-o", "--output", metavar="", default="fec_scheduleA.json", help="Output file name. Default: fec_scheduleA.json")
    args = parser.parse_args()

    if args.api_key == "DEMO_KEY":
        print("Warning: Using DEMO_KEY. This API key is rate-limited and should not be used for production. Get a personal key at https://api.data.gov/signup.")
    
    start = int(args.start)
    end = int(args.end)

    entries = download_entries_year_range(start, end, args.api_key)
    json.dump(entries, open(args.output, "w"))

