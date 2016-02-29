# Grab a list of all the ids of records which need to be matched
import threading

from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

import psycopg2

from address_matcher import Address
from data_getters.abp import DataGetter_ABP

from matching_utils import match_id_and_commit

import logging
logging.root.setLevel("INFO")

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
ids = session.query(Address.id).filter(Address.match_attempted == False).all()
# ids = session.query(Address.id).all()

ids = [id[0] for id in ids]
# ids = ids[:2000]

num_threads = 6

counter = 0

address_threads = []

import datetime
start = datetime.datetime.now()
print str(start)



def new_thread():
    global counter
    freq_con = psycopg2.connect(con_string_freq)
    data_conn = psycopg2.connect(con_string_data)
    data_getter_abp = DataGetter_ABP(freq_conn=freq_con, data_conn=data_conn, SEARCH_INTENSITY=500)
    la_conn = psycopg2.connect(con_string_la)
    session = Session(engine)


    while len(ids) > 0:
        id = ids.pop()
        match_id_and_commit(id,session, data_getter_abp, la_conn, Address)
        counter +=1
        if counter % 50 ==0:
            print counter


for i in range(num_threads):
    t = threading.Thread(target=new_thread)
    address_threads.append(t)
    t.start() #start this thread

for x in address_threads:
    x.join()

end = datetime.datetime.now()
delta = end-start
print str(end)
print str(delta)