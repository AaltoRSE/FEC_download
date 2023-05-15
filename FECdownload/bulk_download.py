import requests
import tqdm
import datetime
import os

bulk_contributions_url = "https://www.fec.gov/files/bulk-downloads/{y}/indiv{y2}.zip"
bulk_committee_url = "https://www.fec.gov/files/bulk-downloads/{y}/cm{y2}.zip"
this_year = datetime.date.today().year
first_year = 1980
last_year = (this_year//2+1)*2

chunk_size = 1024
unit = "KB"

def download_contibutions():
    if not os.path.isdir("bulk_data"):
        os.makedirs("bulk_data")

    for year in range(first_year, last_year, 2):
        filename = f"bulk_data/contributions_{year}.zip"
        if os.path.exists(filename):
            continue

        url = bulk_contributions_url.format(y=str(year), y2=str(year)[-2:])
        print(f"Downloading {url}")

        response = requests.get(url, stream = True)
        total_size = int(response.headers.get('content-length', 0))//chunk_size
        with open(filename, 'wb') as f:
            for chunk in tqdm.tqdm(response.iter_content(chunk_size=chunk_size), total=total_size, unit=unit):
                f.write(chunk)


def download_committees():
    if not os.path.isdir("bulk_data"):
        os.makedirs("bulk_data")

    for year in range(first_year, last_year, 2):
        filename = f"bulk_data/committees_{year}.zip"
        if os.path.exists(filename):
            continue

        url = bulk_committee_url.format(y=str(year), y2=str(year)[-2:])
        print(f"Downloading {url}")

        response = requests.get(url, stream = True)
        total_size = int(response.headers.get('content-length', 0))//chunk_size
        with open(filename, 'wb') as f:
            for chunk in tqdm.tqdm(response.iter_content(chunk_size=chunk_size), total=total_size, unit=unit):
                f.write(chunk)


    