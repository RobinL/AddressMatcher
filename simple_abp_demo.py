from address_matcher import Matcher, Address
from data_getters.abp import DataGetter_ABP

import logging
import psycopg2

logging.root.setLevel("INFO")

# For the address matcher to work we need a connection to a database with
# a table of addresses and a table of token frequencies

con_string_freq = "host='localhost' dbname='postgres' user='postgres' password='' options='-c statement_timeout=400'"
freq_con = psycopg2.connect(con_string_freq)

con_string_data = "host='localhost' dbname='postgres' user='postgres' password='' options='-c statement_timeout=400'"
data_conn = psycopg2.connect(con_string_data)

data_getter_abp = DataGetter_ABP(freq_conn=freq_con, data_conn=data_conn, SEARCH_INTENSITY=50000, MAX_RESULTS=1000)

# Simple utility function that takes an address string and returns the match object
# This contains the list of potential matches, the best matches etc
def get_matches(address_string):
    address = Address(address_string, data_getter=data_getter_abp)
    matcher_abp = Matcher(data_getter_abp,address)
    matcher_abp.load_potential_matches()
    matcher_abp.find_match()
    return matcher_abp

matches = get_matches("51, CHAMBERHALL BUSINESS PARK, CHAMBERHALL GREEN, BURY, LANCS, BL9 0AP")
#SHANKLYS SOLICITORS PRESTIGE HOUSE 142, BURY OLD ROAD, WHITEFIELD, MANCHESTER, M45 6AT
# ACORN BUSINESS CENTRE PT 2ND FLR, ACORN BUSINESS CENTRE, FOUNTAIN STREET NORTH, BURY, LANCS, BL9 0LD
# 23, BENSON STREET, BURY, LANCS, BL9 7EP
# 155, BURY NEW ROAD, WHITEFIELD, MANCHESTER, M45 6AA

logging.info("The match tokens were : {}".format(matches.address_to_match.tokens_original_order_postcode))
logging.info(" ")
logging.info("Single match          : {}".format(matches.one_match_only))
logging.info("Best match            : {}".format(matches.best_match.full_address))
logging.info("Best match score      : {}".format(matches.best_match.match_score))
logging.info("Distinguishability    : {}".format(matches.distinguishability))


# for m in matches.potential_matches:
#     print m
