import requests
import tqdm
import datetime
import os

bulk_contributions_url = "https://www.fec.gov/files/bulk-downloads/{y}/indiv{y2}.zip"
bulk_committee_url = "https://www.fec.gov/files/bulk-downloads/{y}/cm{y2}.zip"
this_year = datetime.date.today().year
first_year = 1980
last_year = (this_year//2+1)*2

def download_bulk_data():
    if not os.path.isdir("bulk_data"):
        os.makedirs("bulk_data")

    for year in range(first_year, last_year, 2):
        filename = f"bulk_data/{year}.zip"
        if os.path.exists(filename):
            continue

        url = bulk_contributions_url.format(y=str(year), y2=str(year)[-2:])
        print(f"Downloading {url}")

        response = requests.get(url, stream = True)
        total_size = int(response.headers.get('content-length', 0))
        with open(filename, 'wb') as f:
            for chunk in tqdm.tqdm(response.iter_content(chunk_size=1024), total=total_size, unit='KB'):
                f.write(chunk)


if __name__ == "__main__":
    download_bulk_data()
    