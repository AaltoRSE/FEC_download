import pandas as pd
from thefuzz import fuzz
from nicknames import NickNamer

nicknamer = NickNamer()

def canonize_name(series):
    r = series.str.lower()
    r = r.str.replace(',', '')
    r = r.str.replace('.', '')
    r = r.str.replace(' mr ', ' ')
    r = r.str.replace(' ms ', ' ')
    r = r.str.replace(' m ', ' ')
    r = r.str.replace(' jr ', ' ')
    r = r.str.strip()
    return r

def fuzzy_match(x, value, matcher = fuzz.ratio, threshold = 95):
    if x == value:
        return True
    r = matcher(x, value)
    if r >= threshold:
        return True
    return False
