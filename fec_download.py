import requests
import argparse
import os
import json
import pandas
import time
from time import sleep
from pandas import json_normalize
from tqdm import tqdm

# API url for schedule A receipts, including contributions from individuals
api_url = "https://api.open.fec.gov/v1/schedules/schedule_a/"

rate_limit = 1000 # per hour
start_time = time.time()
request_count = 0

def make_request(url, params, max_tries = 20, sleep_time = 1):
    """Make a request to a URL, checking for HTTP error codes and retrying if the request fails.
    Args:
        url (str) : The URL to request.
        max_tries (int): The maximum number of times to attempt the request (default: 3).
        sleep_time (int): The time to wait (in seconds) between retries (default: 1).
    
    Returns:
        The response object if the request is successful. Otherwise raises the last exception.
    """
    global request_count
    attempt = 0
    while attempt < max_tries:
        elapsed_time = time.time() - start_time
        rate = (request_count / elapsed_time) * 3600
        if rate > rate_limit:
            sleep(3600/1000+1)
        try:
            request_count += 1
            response = requests.get(url, params=params)
            response.raise_for_status()
            return response
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                # rate limit hit. Wait for a moment longer
                sleep(10)
            print(f"HTTP error ({e.response.status_code}): {e.response.reason}")
            if attempt == max_tries - 1:
                raise e
        except requests.exceptions.RequestException as e:
            print(f"Request Error: {e}")
            if attempt == max_tries - 1:
                raise e
        attempt += 1
        sleep(sleep_time)

def checkpoint_filename(year, employer):
    if not os.path.exists("checkpoints"):
        os.makedirs("checkpoints")
    if employer is not None:
        return f"checkpoints/fec_download_checkpoint_{year}_{employer}.json"
    else: 
        return f"checkpoints/fec_download_checkpoint_{year}.json"

def checkpoint_dump(pagination, entries, page, year, employer):
    checkpoint = {"pagination": pagination, "entries": entries, "page": page}
    with open(checkpoint_filename(year, employer), 'w') as checkpoint_file:
        json.dump(checkpoint, checkpoint_file)

def checkpoint_read(year, employer):
    filename = checkpoint_filename(year, employer)
    try:
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                checkpoint = json.load(f)
            page = checkpoint["page"]
            entries = checkpoint["entries"]
            pagination = checkpoint["pagination"]
            return page, entries, pagination
    except:
        pass
    page = 0
    entries = []
    pagination = {"pages": 1, "last_indexes": {}}
    return page, entries, pagination

def download_pages(parameters):
    year = parameters["two_year_transaction_period"]
    employer = parameters["contributor_employer"]
    page, entries_year, pagination = checkpoint_read(year, employer)

    while page < pagination["pages"]:
        for key, value in pagination["last_indexes"].items():
            parameters[key] = value

        response = make_request(api_url, params=parameters)
        response = response.json()
        results = response["results"]

        entries_year += results

        pagination = response["pagination"]
        if pagination["last_indexes"] is None:
            print(f"Warning: pagination data for {employer} for years {year-2} to {year} is None. Successfully downloaded {len(entries_year)} entries.")
            break
        page += 1

        if page % 10 == 0:
            checkpoint_dump(pagination, entries_year, page, year, employer)
    return entries_year

def download_pages_tqdm(parameters):
    year = parameters["two_year_transaction_period"]
    if "contributor_employer" in parameters:
        employer = parameters["contributor_employer"]
    else:
        employer = None
    page, entries_year, pagination = checkpoint_read(year, employer)

    with tqdm(total=pagination['pages'], desc=f"Downloading years {year-2} to {year}",  miniters=1) as pbar:
          pbar.update(page)
          while page < pagination["pages"]:
            for key, value in pagination["last_indexes"].items():
                parameters[key] = value

            response = make_request(api_url, params=parameters)
            response = response.json()
            results = response["results"]

            entries_year += results

            pagination = response["pagination"]
            page += 1

            if page % 10 == 0:
                checkpoint_dump(pagination, entries_year, page, year, employer)

            pbar.total = pagination['pages']
            pbar.update(1)
    return entries_year


def download_scheduleA_year_range(start, end, api_key = "DEMO_KEY", employer = None):
    """Fetches all Schedule A filings of campaign contributions and loans for the given two-year periods.

    Args:
        start (int): starting year of two-year periods
        end (int): ending year of two-year periods
        api_key (str): API key for accessing the FEC API (default: "DEMO_KEY")

    Returns:
        list: A list of contribution and loan items from FEC API.
    """
    start = (start//2+1)*2
    end = (end//2+2)*2
    entries = []

    for year in range(start, end, 2):
        parameters = {
            "sort_hide_null": "false",
            "per_page": "100",
            "sort_nulls_last": "false",
            "sort": "-contribution_receipt_date",
            "page": "1",
            "sort_null_only": "false",
            "api_key": api_key,
            "page": 0
        }
        if employer is not None:
            parameters["contributor_employer"] = employer
        parameters["two_year_transaction_period"] = year

        if employer is not None:
            entries += download_pages(parameters)
        else:
            entries += download_pages_tqdm(parameters)

    return entries
    
def fec_scheduleA_year_range(start, end, key = "DEMO_KEY", employer=None):
    """Returns a panda DataFrame with campaign contributions and loans by cycle.

    Args:
        start (int): starting year of two-year periods
        end (int): ending year of two-year periods
        key (str): API key for accessing the FEC API (default: "DEMO_KEY")

    Returns:
        pandas.DataFrame: A DataFrame of contribution and loan items by cycle.
    """
    entries = download_scheduleA_year_range(start, end, key, employer)
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


def main():
    parser = argparse.ArgumentParser(description="Fetch data from FEC API")
    parser.add_argument("-k", "--api_key", metavar="", default="DEMO_KEY", help="your FEC API key, default: DEMO_KEY")
    parser.add_argument("-s", "--start", metavar="", default=1990, help="The first year for which data is requested. The data is returned in 2 year chunks and earlier data may be returned.")
    parser.add_argument("-e", "--end", metavar="", default=2025, help="The last year for which data is requested. The data is returned in 2 year chunks and later data may be returned.")
    parser.add_argument("-E", "--employer", metavar="", default=None, help="The employer for which data is requested. If not provided, all employers are requested.")
    parser.add_argument("-o", "--output", metavar="", default=None, help="Output file name. Default: fec_scheduleA_[EMPLOYER_]START_END.json")
    args = parser.parse_args()

    if args.api_key == "DEMO_KEY":
        print("Warning: Using DEMO_KEY. This API key is rate-limited and should not be used for production. Get a personal key at https://api.data.gov/signup.")
    
    start = int(args.start)
    end = int(args.end)

    if args.output is None:
        if args.employer is not None:
            output_filename = f"fec_scheduleA_{args.employer}_{start}_{end}.json"
        else:
            output_filename = f"fec_scheduleA_{start}_{end}.json"

    data = fec_scheduleA_year_range(start, end, args.api_key, args.employer)
    data.to_csv(args.output)


if __name__ == "__main__":
    main()
