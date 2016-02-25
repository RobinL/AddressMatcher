from data_preprocessing.utils import print_comparisons
from data_preprocessing.utils import fix_nfa
from data_preprocessing.utils import add_space_to_postcode
import re

import pandas as pd
df = pd.read_csv("all_addresses.csv")
df = df.fillna("")
df = df[["full_address", "Match.Status"]]

# Correct basic elements of punctuation and make upper case
df["address_cleaned"] = df["full_address"].str.replace("."," ").str.replace("'", "").str.upper()

# Get rid of double spaces
df["address_cleaned"] = df["address_cleaned"].apply(lambda x: re.sub("\s{2,100}", " ", x))

# If there exists a full postcode without a space in it, add a space
df["address_cleaned"] = df["address_cleaned"].apply(add_space_to_postcode)

# Fix postcode
from data_preprocessing.parse_out_postcode import fix_postcode
df["address_cleaned"] = df["address_cleaned"].apply(fix_postcode)
df["address_cleaned"] = df["address_cleaned"].apply(fix_nfa)

# Fix abbreviations etc
from data_preprocessing.use_dicts_to_normalise_address import process_address_string
path = "/Users/robinlinacre/Documents/python_projects/partial_address_matcher/data_preprocessing/data/custom_data.txt"
df["address_cleaned"] = df["address_cleaned"].apply(process_address_string, args=[path])

# Fix misspellings
import re
def translate_add(full_address, translate_dict):
    import re
    full_address = full_address.upper()
    pattern = re.compile(r'\b(' + '|'.join(translate_dict.keys()) + r')\b')
    result = pattern.sub(lambda x: translate_dict[x.group()], full_address)
    return result.upper()

mis_df = pd.read_csv("all_misspellings_with_scores_final.csv")
mis_df = mis_df[mis_df["score_ratio"]>0.8][["word","correct_spelling"]]
translate = mis_df[~pd.isnull(mis_df["correct_spelling"])][["word","correct_spelling"]].to_dict(orient="records")
translate_dict = {d["word"]: d["correct_spelling"] for d in translate}


df["translated_address"] = df["address_cleaned"].apply(translate_add, args=(translate_dict,))

df.to_csv("all_addresses_for_matching.csv")