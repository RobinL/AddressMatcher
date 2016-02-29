from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

import psycopg2

from address_matcher import Address
from data_getters.abp import DataGetter_ABP


from matching_utils import match_id_and_commit

# For the address matcher to work we need a connection to a database with
# a table of addresses and a table of token frequencies

con_string_freq = "host='localhost' dbname='postgres' user='postgres' password='' options='-c statement_timeout=400'"
freq_con = psycopg2.connect(con_string_freq)

con_string_data = "host='localhost' dbname='postgres' user='postgres' password='' options='-c statement_timeout=400'"
data_conn = psycopg2.connect(con_string_data)

# This one's for looking up whether points fit into local authorities - not no timeout is set
con_string_la = "host='localhost' dbname='postgres' user='postgres' password=''"
la_conn = psycopg2.connect(con_string_la)

data_getter_abp = DataGetter_ABP(freq_conn=freq_con, data_conn=data_conn, SEARCH_INTENSITY=500)

engine = create_engine('postgresql://postgres:@localhost:5432/postgres')

Base = automap_base()

# reflect the tables
Base.prepare(engine, reflect=True)

Address = Base.classes.all_addresses_with_match_info

session = Session(engine)

match_id_and_commit(0,session, data_getter_abp, la_conn, Address)