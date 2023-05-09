# FEC Download

Simple script for downloading individual contributions from the OpenFEC API.

## Installation

```bash
pip install git+https://github.com/AaltoRSE/FEC_download.git
```

## Usage

As a script:
```bash
download_scheduleA -k YOUR_API_KEY -s START_YEAR -e END_YEAR -E employer
```

As a package:
```python
from FECdownload import fec_scheduleA_year_range
fec_scheduleA_year_range(start_year, end_year, key=api_key, employer=company)
```
