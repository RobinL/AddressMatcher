# This file demonstrates how you can use the address matcher to match two arbitrary lists of data
# It's super inefficient because no attempt is made to find 'close matches'
# You need to use proper data getter to make things more efficient

import pandas as pd
from address_matcher import Matcher, Address

import logging

logging.root.setLevel("INFO")

match_candidates = pd.read_csv("test_data/las1.csv") #list of strings to match
match_candidates["name1"] = match_candidates["name1"].str.upper()
match_targets  = pd.read_csv("test_data/las2.csv")  #list to match to
match_targets["name2"] = match_targets["name2"].str.upper()

# Get term frequencies in targets - this should be done from tokenised address
# otherwise it's possible that matching tokens in the addresses won't be in the freq table
from collections import Counter

tokens = []
for a in list(match_targets["name2"]):
    address = Address(a)
    tokens.extend(address.tokens_original_order_postcode)

df_freq = pd.Series(Counter(tokens)).reset_index()
df_freq.columns = ["term", "freq"]
df_freq["freq"] = df_freq["freq"]*1.0/df_freq["freq"].sum()

from data_getters.in_memory import DataGetter_Memory
dg_mem = DataGetter_Memory(match_targets["name2"], df_freq)

address = Address("MIDDLESBOBUGH")

logging.info("Candidate for matching: {}".format(address.full_address))
matcher = Matcher(dg_mem,address)
matcher.load_potential_matches()
matcher.find_match()
logging.info("Best match            : {}".format(matcher.best_match.full_address))
logging.info("---")

for a in list(match_candidates["name1"])[14:]:

    address = Address(a)
    logging.info("Candidate for matching: {}".format(address.full_address))
    matcher = Matcher(dg_mem,address)
    matcher.load_potential_matches()
    matcher.find_match()
    logging.info("Best match            : {}".format(matcher.best_match.full_address))
    logging.info("---")
