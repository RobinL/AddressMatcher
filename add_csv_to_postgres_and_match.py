# This script demonstrates how to go from a csv of addresses
# to that table in postgres
# to matched uprns, including multithreading

# We might want to do this is we want to restart the address matcher (i.e. write results to the database as we go
# rather than doing everything in memory

import pandas as pd
from address_matcher import Matcher, Address
from data_getters.abp import DataGetter_ABP
import logging
import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
import threading
from sqlalchemy.orm import Session
import sys

from sqlalchemy.ext.automap import automap_base

logging.root.setLevel("INFO")

addresses = pd.read_csv("test_data/address_list.csv")

# We want a table
new_columns = ["match_attempted",
"score",
"abp_full_address"]

for c in new_columns:
    addresses[c] = None

addresses["match_attempted"] = False


# Now write this table out to postgres
con_string = "host='localhost' dbname='postgres' user='postgres' password=''"


engine = create_engine('postgresql://postgres:@localhost:5432/postgres')
addresses.to_sql("temp_delete", engine,
          index=True,
          index_label = "id",
          if_exists='replace',
          dtype = {'full_address': sqlalchemy.types.String,
                    'match_attempted': sqlalchemy.types.BOOLEAN,
                    'score': sqlalchemy.types.NUMERIC,
                    'abp_full_address': sqlalchemy.types.String
})

sql = """
ALTER TABLE temp_delete ADD PRIMARY KEY (id);
"""
import psycopg2
con_string = "host='localhost' dbname='postgres' user='postgres' password=''"
conn = psycopg2.connect(con_string)
cur = conn.cursor()
cur.execute(sql)
conn.commit()


con_string_freq = "host='localhost' dbname='postgres' user='postgres' password='' options='-c statement_timeout=400'"
freq_con = psycopg2.connect(con_string_freq)

con_string_data = "host='localhost' dbname='postgres' user='postgres' password='' options='-c statement_timeout=400'"
data_conn = psycopg2.connect(con_string_data)


data_getter_abp = DataGetter_ABP(freq_conn=freq_con, data_conn=data_conn, SEARCH_INTENSITY=500)

engine = create_engine('postgresql://postgres:@localhost:5432/postgres')

Base = automap_base()

# reflect the tables
Base.prepare(engine, reflect=True)

Addresses = Base.classes.temp_delete

session = Session(engine)
ids = session.query(Addresses.id).filter(Addresses.match_attempted == False).all()

ids = [id[0] for id in ids]

num_threads = 6

counter = 0

address_threads = []


def get_matches(address_string, data_getter):
    address = Address(address_string, data_getter=data_getter_abp)
    matcher_abp = Matcher(data_getter,address)
    matcher_abp.load_potential_matches()
    matcher_abp.find_match()
    return matcher_abp


def match_id_and_commit(id, session, data_getter, Address):
    this_address = session.query(Addresses).filter(Addresses.id == id).one()


    matches = get_matches(this_address.full_address, data_getter)
    this_address.score = matches.best_match.match_score
    this_address.abp_full_address = matches.best_match.full_address
    this_address.match_attempted = True
    session.add(this_address)
    session.commit()
    try:
        matches = get_matches(this_address.full_address, data_getter)
        this_address.score = matches.best_match.match_score
        this_address.abp_full_address = matches.best_match.full_address
        this_address.match_attempted = True
        session.add(this_address)
        session.commit()

    except Exception as e:
        print id
        print(sys.exc_info()[0])

import datetime
start = datetime.datetime.now()
print str(start)

def new_thread():
    global counter
    freq_con = psycopg2.connect(con_string_freq)
    data_conn = psycopg2.connect(con_string_data)
    data_getter_abp = DataGetter_ABP(freq_conn=freq_con, data_conn=data_conn, SEARCH_INTENSITY=500)
    session = Session(engine)


    while len(ids) > 0:
        id = ids.pop()
        match_id_and_commit(id,session, data_getter_abp, Address)
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

