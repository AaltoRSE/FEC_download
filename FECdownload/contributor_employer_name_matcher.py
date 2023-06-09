# 1. Dowload by employer
# 2. Filter out non-exact matches to "EY"
# 3. Fuzzy filter by last name
# 4. Match to auditor by fuzzy last and fuzzy first name or exact nick name

import os
import pandas as pd
import argparse
from pathlib import Path
import glob
from nicknames import NickNamer

from . import fec_scheduleA_year_range
from . import utils

def FEC_match_name_and_employer(
    target_donors_dataset = None,
    employer_column = "employer",
    first_name_column = "first_name",
    middle_name_column = "middle_name",
    last_name_column = "last_name",
    api_filter_by_employers = ["Google", "Microsoft", "MSFT"],
    exact_match_employers = [],
    partial_match_employers = ["Google", "Microsoft", "MSFT"],
    first_name_fuzzy_ratio = 0.7,
    api_key = "DEMO_KEY",
):
    ''' Download individual contributions data from the FEC API and match by employer, 
    first name and middle name initial.
    
    Requires an exact match to the last name, since it's used to directly filter the data
    requested from the API.

    First names are matched using fuzzy matching or exact matching to possible nicknames. 
    Middle names must match to the first letter, or in case of ambiguous middle names, must
    match exactly.

    Parameters:
    -----------
    target_donors_dataset: str or pandas.DataFrame
        Dataset containing the names to match
    employer_column: str
        Column in the target donors dataset containing the employer
    first_name_column: str
        Column in the target donors dataset containing the first name
    middle_name_column: str
        Column in the target donors dataset containing the middle name
    last_name_column: str
        Column in the target donors dataset containing the last name
    api_filter_by_employers: list of str
        List of employers to filter the data by. The API uses partial matching, so
        names containing a given string will match.
    exact_match_employers: list of str
        List of employers to match exactly.
    partial_match_employers: list of str
        List of employers to match partially. Names containing the string will match.
    first_name_fuzzy_ratio: float
        Fuzzy matching ratio for the first name.
    api_key: str
        FEC API key
    '''
    nicknamer = NickNamer()
    if target_donors_dataset is None:
        raise ValueError("target_donors_dataset must be provided")

    if type(target_donors_dataset) == pd.DataFrame:
        target_data = target_donors_dataset
    elif target_donors_dataset.endswith(".dta"):
        target_data = pd.read_stata(target_donors_dataset)
    elif target_donors_dataset.endswith(".csv"):
        target_data = pd.read_csv(target_donors_dataset)
    target_data = target_data[~target_data[last_name_column].isna()]
    target_data = target_data[~target_data[first_name_column].isna()]

    if api_key == "DEMO_KEY":
        print("Warning: Using DEMO_KEY. This API key is rate-limited and should not be used for production. Get a personal key at https://api.data.gov/signup.")

    # step 1: Download by employer, filtering by last name for data after 2018
    employer_path = "by_employer/downloaded"
    for employer in api_filter_by_employers:
        filename = f"{employer_path}_{employer}.csv"
        if not os.path.exists(filename):
            # touch the file to reserve, in case there is another process downloading
            Path(filename).touch()
            try:
                df = fec_scheduleA_year_range(1979, 2017, key=api_key, employer=employer)
                df.to_csv(filename)
            except:
                # in case of an issue, remove the reservation file
                os.remove(filename)
                raise
            
    # From 2019, filter by last name as well
    employer_name_path = "by_name_and_employer/downloaded"
    for employer in api_filter_by_employers:
        target_names = target_data[target_data[employer_column].str.contains(employer)]
        names = target_names[last_name_column].unique()

        for n, name in enumerate(names):
            filename = f"{employer_name_path}_{employer}_{name}.csv"
            if not os.path.exists(filename):
                print(f"{employer}, {name}, {n} / {len(names)}")
                # touch the file to reserve, in case there is another process downloading
                Path(filename).touch()
                try:
                    df = fec_scheduleA_year_range(2019, 2024, key=api_key, employer=employer, name=name)
                    df.to_csv(filename)
                except:
                    # in case of an issue, remove the reservation file
                    os.remove(filename)
                    raise


    if os.path.exists("downloaded.csv"):
        df = pd.read_csv("downloaded.csv")
    else:
        files = glob.glob(f'{employer_path}*.csv')+glob.glob(f'{employer_name_path}*.csv')
        dfs = []
        for f in files:
            dfs.append(pd.read_csv(f))
        df = pd.concat(dfs)
        df.to_csv(f"downloaded.csv")


    # step 2: Filter by employer, should be exact match to "EY" or contain any of the other names.
    if os.path.exists("filtered_by_employer.csv"):
        df = pd.read_csv("filtered_by_employer.csv")
    else:
        match = df["contributor_employer"].str == exact_match_employers[0]
        for employer in exact_match_employers[1:]:
            match = match | df["contributor_employer"].str == employer
        for employer in partial_match_employers:
            match = match | df["contributor_employer"].str.contains(employer)
        df = df[match]

        df.to_csv("filtered_by_employer.csv", index=False)


    # step 3: Filter and match to auditor names
    df = df[~df["contributor_last_name"].isna()]
    df = df[~df["contributor_first_name"].isna()]
    df["canonical_last_name"] = utils.canonize_name(df["contributor_last_name"])
    df["canonical_first_name"] = utils.canonize_name(df["contributor_first_name"])
    df["canonical_middle_name"] = utils.canonize_name(df["contributor_middle_name"])

    target_data = pd.read_stata(target_donors_dataset)
    target_data = target_data[~target_data[last_name_column].isna()]
    target_data["canonical_last_name"] = utils.canonize_name(target_data[last_name_column])
    target_data["canonical_first_name"] = utils.canonize_name(target_data[first_name_column])
    target_data["canonical_middle_name"] = utils.canonize_name(target_data[middle_name_column])

    df.drop_duplicates(inplace=True)


    # Here we filter match by last name
    if os.path.exists("matched_first_name.csv"):
        df = pd.read_csv("matched_first_name.csv")
    else:
        # Match by first name and middle name initial. When there are multipl
        # matches, require a full match to the middle name.
        # When the middle name is omitted, match if there is only one target,
        # or if one of the targets does not have a middle name.
        ln_col = 'canonical_last_name'
        id_col = 'original_index'
        fnx_col = 'canonical_first_name_x'
        mnx_col = 'canonical_middle_name_x'
        fny_col = 'canonical_first_name_y'
        mny_col = 'canonical_middle_name_y'

        df = df.reset_index().rename(columns={'index': id_col})

        merged = pd.merge(df, target_data, on=ln_col)
        print("df", df.shape)
        print("remaining", merged.shape)

        matches_mask = (
            (merged[fnx_col] == merged[fny_col]) &
            ((merged[mnx_col].str[0] == merged[mny_col].str[0]) |
             (merged[mnx_col] == ""))
        )
        matched = merged[matches_mask]

        # If there are multiple possible matches to a transaction, require a
        # full match to the middle name.
        duplicated = matched[matched.duplicated(subset=[id_col, fnx_col, ln_col], keep=False)]
        duplicated = duplicated[~(duplicated[mnx_col] == duplicated[mny_col])]
        matches_mask[duplicated.index] = False

        match_df = merged[matches_mask]
        merged = merged[~merged[id_col].isin(match_df[id_col].unique())]


        # Now add nicknames and canonicals of each first name in the target dataset
        merged['nickname'] = merged[fny_col].apply(
            lambda name: list(nicknamer.nicknames_of(name)) + list(nicknamer.canonicals_of(name)))
        exploded = merged.explode('nickname').reset_index()
        matches_mask = (
            (exploded['nickname'] == exploded[fnx_col]) &
            ((exploded[mnx_col].str[0] == exploded[mny_col].str[0]) |
             (exploded[mnx_col] == ""))
        )
        matched = exploded[matches_mask]
        duplicated = matched[matched.duplicated(subset=[id_col, fnx_col, ln_col], keep=False)]
        duplicated = duplicated[~(duplicated[mnx_col] == duplicated[mny_col])]
        matches_mask[duplicated.index] = False

        nickname_match_df = exploded[matches_mask].drop(columns='nickname')
        match_df = pd.concat([match_df, nickname_match_df])

        merged = merged[~merged[id_col].isin(match_df[id_col].unique())]


        # Finally try fuzzy matching to first name
        middle_name_matches = merged[
            merged[mnx_col].str[0] == merged[mny_col].str[0]
        ].reset_index()
        matches_mask = middle_name_matches.apply(
            lambda row: utils.fuzzy_match(row[fnx_col], row[fny_col], threshold=first_name_fuzzy_ratio),
            axis=1
        )
        matched = middle_name_matches[matches_mask]
        duplicated = matched[matched.duplicated(subset=[id_col, fnx_col, ln_col], keep=False)]
        duplicated = duplicated[~(duplicated[mnx_col] == duplicated[mny_col])]
        matches_mask[duplicated.index] = False

        fuzzy_matched = middle_name_matches[matches_mask]
        match_df = pd.concat([match_df, fuzzy_matched])
        merged = merged[~merged[id_col].isin(match_df[id_col].unique())]


        match_df.drop(columns=[ln_col, id_col, fnx_col, mnx_col, fny_col, mny_col], inplace=True)
        match_df.to_csv("matched_names.csv", index=False)
        merged.to_csv("unmatched.csv", index=False)

def main():
    parser = argparse.ArgumentParser(description="Match FEC individual contributor data from the API with first, middle and last names.")
    parser.add_argument("-k", "--api_key", metavar="", default="DEMO_KEY", help="your FEC API key, default: DEMO_KEY")
    args = parser.parse_args()
    
    FEC_match_name_and_employer()


if __name__ == "__main__":
    main()
    