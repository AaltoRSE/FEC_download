# FEC Download

Simple script for downloading individual contributions from the OpenFEC API.

## Installation

```bash
pip install pip install git+https://github.com/rantahar/FEC_download/tree/master
```

## Usage

As a script:
```bash
fec_download.py -k YOUR_API_KEY -s START_YEAR -e END_YEAR
```

As a package:
```python
from fec_download import fec_scheduleA_year_range
fec_scheduleA_year_range(start_year, end_year, key=api_key, employer=company)
```
