import requests
import argparse
import pandas
from pandas import json_normalize

# API url for schedule A receipts, including contributions from individuals
api_url = "https://api.open.fec.gov/v1/schedules/schedule_a/"


def download_scheduleA_year_range(start, end, key = "DEMO_KEY"):
    """Fetches all Schedule A filings of campaign contributions and loans for the given two-year periods.

    Args:
        start (int): starting year of two-year periods
        end (int): ending year of two-year periods
        key (str): API key for accessing the FEC API (default: "DEMO_KEY")

    Returns:
        list: A list of contribution and loan items from FEC API.
    """
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
    pagination_string = ""
    for year in range(start, end, 2):
        page = 0
        pagination = {"pages": 1}
        while page < pagination["pages"]:
            parameters["two_year_transaction_period"] = year

            response = requests.get(api_url+pagination_string, params=parameters)
            response = response.json()
            results = response["results"]

            entries += results

            pagination = response["pagination"]
            pagination_string = "?"+"&".join([key+"="+str(value) for key,value in pagination["last_indexes"].items()])
            page += 1
            print(pagination)
            print(page, pagination["pages"])
            

    return entries
    
def fec_scheduleA_year_range(start, end, key = "DEMO_KEY"):
    """Returns a panda DataFrame with campaign contributions and loans by cycle.

    Args:
        start (int): starting year of two-year periods
        end (int): ending year of two-year periods
        key (str): API key for accessing the FEC API (default: "DEMO_KEY")

    Returns:
        pandas.DataFrame: A DataFrame of contribution and loan items by cycle.
    """
    entries = download_scheduleA_year_range(start, end, key)
    df = pandas.DataFrame(json_normalize(entries))
    return df

def filter_contributions(df):
    """Filters a DataFrame of Schedule A filings to retain only contribution items.

    Args:
        df (pandas.DataFrame): A DataFrame of Schedule A filings.

    Returns:
        pandas.DataFrame: A new DataFrame containing only contribution items.
    """
    contribution_lines = ["11AI", "11AII", "11BI", "11BII"]
    contributions_df = df[df["line_number"].isin(contribution_lines)].copy()
    return contributions_df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch data from FEC API")
    parser.add_argument("-k", "--api_key", metavar="", default="DEMO_KEY", help="your FEC API key, default: DEMO_KEY")
    parser.add_argument("-s", "--start", metavar="", default=1990, help="The first year for which data is requested. The data is returned in 2 year chunks and earlier data may be returned.")
    parser.add_argument("-e", "--end", metavar="", default=2025, help="The last year for which data is requested. The data is returned in 2 year chunks and later data may be returned.")
    parser.add_argument("-o", "--output", metavar="", default="fec_scheduleA.csv", help="Output file name. Default: fec_scheduleA.json")
    args = parser.parse_args()

    if args.api_key == "DEMO_KEY":
        print("Warning: Using DEMO_KEY. This API key is rate-limited and should not be used for production. Get a personal key at https://api.data.gov/signup.")
    
    start = int(args.start)
    end = int(args.end)

    data = download_scheduleA_year_range(2022, 2024, args.api_key)
    data.to_csv(args.output)

