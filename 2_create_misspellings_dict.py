from data_getters.abp import DataGetter_ABP
import Levenshtein
import pandas as pd

import psycopg2
con_string = "host='localhost' dbname='postgres' user='postgres' password=''"
conn = psycopg2.connect(con_string)
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:@localhost:5432/postgres')

con_string = "host='localhost' dbname='postgres' user='postgres' password='' options='-c statement_timeout=400'"
freq_con = psycopg2.connect(con_string)
data_conn = psycopg2.connect(con_string)

data_getter_abp = DataGetter_ABP(freq_conn=freq_con, data_conn=data_conn)

def get_misspellings(token):
    sql = """
      SELECT *
      FROM term_frequencies
      WHERE term % '{}'
      """

    df = pd.read_sql(sql.format(token), conn)
    return df

def distance_score(str1, str2):
    """
    Computes a ratio of number of edits to length of string
    """
    str1 = unicode(str1)
    str2 = unicode(str2)
    d = Levenshtein.distance(str1,str2)
    return d


def distance_score_ratio(str1, str2):
    """
    Computes a ratio of number of edits to length of string
    """
    str1 = unicode(str1)
    str2 = unicode(str2)
    d = Levenshtein.distance(str1,str2)
    length = max(len(str1), len(str2))
    return 1 - (d*1.0/length)

def score_misspellings(df_mis, word):
    for r in df_mis.iterrows():
        row = r[1]
        index = r[0]

        df_mis.loc[index, "score"] = distance_score(row["term"].upper(),word.upper())
        df_mis.loc[index, "score_ratio"] = distance_score_ratio(row["term"].upper(),word.upper())
    try:
        df_mis.sort(["score_ratio", "occurrences"])
        return df_mis.iloc[0]
    except:
        return None


df = pd.read_sql("select * from temp.partial_address_term_frequencies_all_fixed_2016", conn)

counter = 1
for r in df.iterrows():
    if counter % 1000 == 0:
        print counter
    counter +=1

    row = r[1]
    index = r[0]
    word = row["word"]
    found = None
    if len(word) > 3:
        try:
            found = data_getter_abp.get_freq(word)
        except:
            print counter
            print word
    else:
        continue


    if found is None:

        df_mis = get_misspellings(word)
        correct_spelling = score_misspellings(df_mis,word)

        try:

            df.loc[index,"correct_spelling"] = correct_spelling['term'].upper()
            df.loc[index, "score"] = correct_spelling['score']
            df.loc[index, "score_ratio"] = correct_spelling['score_ratio']
#             print "word is {} correct spelling is {} score {}".format(word,correct_spelling["term"], correct_spelling["score_ratio"])


        except:
            pass

df2 = df[~pd.isnull(df["correct_spelling"])]
df2.to_csv("all_misspellings_with_scores_final_2016.csv")