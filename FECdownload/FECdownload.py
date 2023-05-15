import requests
import os
import json
import pandas
import time
from time import sleep
from pandas import json_normalize
from tqdm import tqdm

# API url for schedule A receipts, including contributions from individuals
api_url = "https://api.open.fec.gov/v1/schedules/schedule_a/"

rate_limit = 900 # per hour
start_time = time.time()
request_count = 1

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
            #print(f"waiting for due to rate limit ({rate}/{rate_limit})")
            sleep(3600/rate_limit+1)
        try:
            request_count += 1
            request_start = time.time()
            response = requests.get(url, params=params)
            #print(f"actual request took {time.time()- request_start} seconds")
            response.raise_for_status()
            return response, time.time() - request_start
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                # rate limit hit. Wait for a moment longer
                print(f"rate: {rate} {request_count}")
                sleep(5*3600/rate_limit)
            print(f"HTTP error ({e.response.status_code}): {e.response.reason}")
            print(e.response.text)
            if attempt == max_tries - 1:
                raise e
        except requests.exceptions.RequestException as e:
            print(f"Request Error: {e}")
            if attempt == max_tries - 1:
                raise e
        attempt += 1
        sleep(sleep_time)

def checkpoint_filename(year, employer, last_name):
    if not os.path.exists("checkpoints"):
        os.makedirs("checkpoints")
    name = f"checkpoints/fec_download_checkpoint_{year}"
    if employer is not None:
        name = f"{name}_e_{employer}.json"
    if last_name is not None:
        name = f"{name}_n_{last_name}.json"
    
    return f"{name}.json"

def checkpoint_dump(pagination, entries, entry, year, employer, last_name):
    checkpoint = {"pagination": pagination, "entries": entries, "entry": entry}
    with open(checkpoint_filename(year, employer, last_name), 'w') as checkpoint_file:
        json.dump(checkpoint, checkpoint_file, indent=4)

def checkpoint_read(year, employer, last_name):
    filename = checkpoint_filename(year, employer, last_name)
    try:
        if os.path.exists(filename):
            with open(filename, 'r') as f:
                checkpoint = json.load(f)
            entry = checkpoint["entry"]
            entries = checkpoint["entries"]
            pagination = checkpoint["pagination"]
            return entry, entries, pagination
    except:
        pass
    entry = 0
    entries = []
    pagination = {"count": 1, "last_indexes": {}}
    return entry, entries, pagination


def download_pages(parameters):
    year = parameters["two_year_transaction_period"]
    employer = None
    name = None
    message = f"Downloading years {year-2} to {year}"
    if "contributor_employer" in parameters:
        employer = parameters["contributor_employer"]
        message = f"{message} for employer {employer}"
    if "contributor_name" in parameters:
        name = parameters["contributor_name"]
        message = f"{message} for name {name}"
    entry, entries_year, pagination = checkpoint_read(year, employer, name)

    print(message)

    if "last_indexes" in pagination and pagination["last_indexes"] is None:
        return entries_year

    while True:
        if "last_indexes" in pagination:
            if pagination["last_indexes"] is None:
                break
            for key, value in pagination["last_indexes"].items():
                parameters[key] = value

        response, timing = make_request(api_url, params=parameters)
        response = response.json()
        results = response["results"]

        entries_year += results

        pagination = response["pagination"]
        entry += parameters["per_page"]

        checkpoint_dump(pagination, entries_year, entry, year, employer, name)

        if len(results) < parameters["per_page"]:
            print(len(results))
            break

        if timing > 20 and parameters["per_page"] > 1:
            parameters["per_page"] //= 2
        if timing < 2 and parameters["per_page"] <= 90:
            parameters["per_page"] += 10
            #print(f"reducing per page to {parameters['per_page']} (timing {timing})")

    return entries_year



def download_pages_tqdm(parameters):
    year = parameters["two_year_transaction_period"]
    employer = None
    name = None
    message = f"Downloading years {year-2} to {year}"
    if "contributor_employer" in parameters:
        employer = parameters["contributor_employer"]
        message = f"{message} for employer {employer}"
    if "contributor_name" in parameters:
        name = parameters["contributor_name"]
        message = f"{message} for name {name}"
    entry, entries_year, pagination = checkpoint_read(year, employer, name)

    if "last_indexes" in pagination and pagination["last_indexes"] is None:
        return entries_year

    with tqdm(
        total=pagination['count'],
        desc=message,
        miniters=1
    ) as pbar:
        pbar.update(entry)
        while True:
            if "last_indexes" in pagination:
                if pagination["last_indexes"] is None:
                    break
                for key, value in pagination["last_indexes"].items():
                    parameters[key] = value

            response, timing = make_request(api_url, params=parameters)
            response = response.json()
            results = response["results"]

            entries_year += results

            pagination = response["pagination"]
            entry += parameters["per_page"]

            checkpoint_dump(pagination, entries_year, entry, year, employer, name)

            if len(results) < parameters["per_page"]:
                break

            if timing > 20 and parameters["per_page"] > 1:
                parameters["per_page"] //= 2
            if timing < 2 and parameters["per_page"] <= 90:
                parameters["per_page"] += 10
                #print(f"reducing per page to {parameters['per_page']} (timing {timing})")

            pbar.total = pagination['count']
            pbar.n = entry
            pbar.update(entry - pbar.n)
    return entries_year


def download_scheduleA_year_range(start, end, api_key = "DEMO_KEY", employer = None, name = None):
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
            "per_page": 100,
            "sort_nulls_last": "false",
            "page": 1,
            "api_key": api_key,
        }
        if employer is not None:
            parameters["contributor_employer"] = employer
        if name is not None:
            parameters["contributor_name"] = name
        parameters["two_year_transaction_period"] = year

        entries += download_pages_tqdm(parameters)

    return entries
    
def fec_scheduleA_year_range(start, end, key = "DEMO_KEY", employer=None, name=None):
    """Returns a panda DataFrame with campaign contributions and loans by cycle.

    Args:
        start (int): starting year of two-year periods
        end (int): ending year of two-year periods
        key (str): API key for accessing the FEC API (default: "DEMO_KEY")

    Returns:
        pandas.DataFrame: A DataFrame of contribution and loan items by cycle.
    """
    entries = download_scheduleA_year_range(start, end, key, employer, name)
    df = pandas.DataFrame(json_normalize(entries))
    return df

