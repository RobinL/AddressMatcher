from address_matcher import Matcher, Address
from data_getters.abp import DataGetter_ABP

import logging
import psycopg2

logging.root.setLevel("DEBUG")

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

matches = get_matches("church wynd 11")

logging.info("The match tokens were : {}".format(matches.address_to_match.tokens_original_order_postcode))
logging.info("")
logging.info("Single match          : {}".format(matches.one_match_only))
logging.info("Best match            : {}".format(matches.best_match.full_address))
logging.info("Best match score      : {}".format(matches.best_match.match_score))
logging.info("Distinguishability    : {}".format(matches.distinguishability))



