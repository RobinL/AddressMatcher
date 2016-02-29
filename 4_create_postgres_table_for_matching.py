import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
df = pd.read_csv("all_addresses_for_matching.csv")
df = df[["full_address", "Match.Status", "address_cleaned"]]
df = df.fillna("")

new_columns = ["match_attempted",
"num_la_matches",
"probability",
"score",
"match_desc",
"abp_full_address",
"abp_postcode",
"single_match",
"local_authority",
"distinguishability",
"alternative_matches_string",
"match_manual_check",
"match_manual_check_timestamp"]

for c in new_columns:
    df[c] = None

df["match_attempted"] = False

df.rename(columns={"Match.Status":"matchcode_match_status"}, inplace=True)

# Now write this table out to postgres
con_string = "host='localhost' dbname='postgres' user='postgres' password=''"


engine = create_engine('postgresql://postgres:@localhost:5432/postgres')
df.to_sql("all_addresses_with_match_info", engine,
          index=True,
          index_label = "id",
          if_exists='replace',
          dtype = {'full_address': sqlalchemy.types.String,
                    'matchcode_match_status': sqlalchemy.types.String,
                    'address_cleaned': sqlalchemy.types.String,
                    'match_attempted': sqlalchemy.types.BOOLEAN,
                    'num_la_matches': sqlalchemy.types.NUMERIC,
                    'probability': sqlalchemy.types.NUMERIC,
                    'score': sqlalchemy.types.NUMERIC,
                    'match_desc': sqlalchemy.types.String,
                    'abp_full_address': sqlalchemy.types.String,
                    'abp_postcode': sqlalchemy.types.String,
                    'single_match': sqlalchemy.types.BOOLEAN,
                    'local_authority': sqlalchemy.types.String,
                    'distinguishability': sqlalchemy.types.NUMERIC,
                    'alternative_matches_string': sqlalchemy.types.String,
                    'match_manual_check': sqlalchemy.types.String,
                    'match_manual_check_timestamp' : sqlalchemy.types.DateTime

})


sql = """
ALTER TABLE all_addresses_with_match_info ADD PRIMARY KEY (id);
"""
import psycopg2
con_string = "host='localhost' dbname='postgres' user='postgres' password=''"
conn = psycopg2.connect(con_string)
cur = conn.cursor()
cur.execute(sql)
conn.commit()