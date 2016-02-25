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

import psycopg2
con_string = "host='localhost' dbname='postgres' user='postgres' password=''"
conn = psycopg2.connect(con_string)
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:@localhost:5432/postgres')


# df.to_sql("all_addresses_fixed", engine, schema="temp")

import psycopg2
con_string = "host='localhost' dbname='postgres' user='postgres' password=''"
conn = psycopg2.connect(con_string)
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:@localhost:5432/postgres')


sql = """
create table temp.partial_address_term_frequencies_all_fixed as
select word,
count(*) as occurrences,
1.0000 as freq from
(select regexp_split_to_table(upper(address_cleaned), '[^\w]+|\s+') as word from temp.all_addresses_fixed) as t
where word != ''
group by word
order by count(*) desc;
"""
cur = conn.cursor()
cur.execute(sql)
conn.commit()
