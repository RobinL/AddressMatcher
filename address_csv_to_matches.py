# This script demonstrates how to go from a csv of addresses to a table containing the address and its matched uprn

import pandas as pd
from address_matcher import Matcher, Address
from data_getters.abp import DataGetter_ABP
import logging
import psycopg2

logging.root.setLevel("INFO")

addresses = pd.read_csv("test_data/address_list.csv")

# For the address matcher to work we need a connection to a database with
# a table of addresses and a table of token frequencies

con_string_freq = "host='localhost' dbname='postgres' user='postgres' password='' options='-c statement_timeout=400'"
freq_con = psycopg2.connect(con_string_freq)

con_string_data = "host='localhost' dbname='postgres' user='postgres' password='' options='-c statement_timeout=400'"
data_conn = psycopg2.connect(con_string_data)

data_getter_abp = DataGetter_ABP(freq_conn=freq_con, data_conn=data_conn, SEARCH_INTENSITY=5000)

# Simple utility function that takes an address string and returns the match object
# This contains the list of potential matches, the best matches etc
def get_matches(address_string):
    address = Address(address_string, data_getter=data_getter_abp)
    matcher_abp = Matcher(data_getter_abp,address)
    matcher_abp.load_potential_matches()
    matcher_abp.find_match()
    return matcher_abp

for r in addresses.iterrows():
    index = r[0]
    row = r[1]
    matches = get_matches(row["full_address"])
    logging.info(row["full_address"])
    addresses.loc[index, "uprn"] = matches.best_match.id
    addresses.loc[index, "abp_full_address"] = matches.best_match.full_address
    addresses.loc[index, "score"] = matches.best_match.match_score
    logging.info(matches.best_match.full_address)



