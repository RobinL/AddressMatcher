__author__ = 'RLinacre'

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

#Want a 'frequency getter' type object.

data_getter_abp = DataGetter_ABP(freq_conn=freq_con, data_conn=data_conn)

address = Address("Ministry Justice France", data_getter=data_getter_abp)

matcher_abp = Matcher(data_getter_abp,address)
matcher_abp.load_potential_matches()
matcher_abp.find_match()
print matcher_abp.one_match_only
print matcher_abp.best_match
print "\n".join([repr(m) for m in matcher_abp.potential_matches[:5]])