# In the following I assume that you already have a table of term frequencies from AddressBase Premium

# First you will want a table of term frequencies from your messy data
import psycopg2
sql = """
create table partial_address_term_frequencies as
select word, 
count(*) as occurrences,
1.0000 as freq from 
(select regexp_split_to_table(upper(full_address), '[^\w]+|\s+') as word from temp.random_sample) as t
where word != ''
group by word
order by count(*) desc;
"""
con_string = "host='localhost' dbname='postgres' user='postgres' password=''"
conn = psycopg2.connect(con_string)
cur = data_conn.cursor()
cur.execute(sql)
conn.commit()


# Read this into a dataframe
df = pd.read_sql("select * from temp.partial_address_term_frequencies", conn)

# General approach - go through this dataset one row at a time.  
# First look to see whether the 'messy token' appears in addressbase premium
# If it does, ignore it.  If it doesn't exist, it's a potential misspelling

# If it's a mispelling, we need a query that can look up 'similar' tokens in addressbase premium
# We use our table of term frequencies in ABP for this (which is a list of all tokens)
# We need a sql query that will do this - we can use 'trigram'
# https://viget.com/extend/alternatives-to-sphinx-fuzzy-string-matching-in-postgresql
#    see def get_misspellings()

# This requires a new index on the addressbase freq table - you can use the following code
#   create extension pg_trgm;
#   CREATE INDEX items_name_trigram_index
#   ON term_frequencies
#   USING gist (term gist_trgm_ops);

# We then have a list of 'potentially similar' tokens to the 'messy token'
# We can score each one of these potentially similar tokens using edit distance
#     see def distance score() and def score_misspellings 
# We then pick the closest match, an if it's above some theshold accept it as a misspelling
# This gives us a table of 'misspellings' and 'correct spellings' - 
# We can then apply this to the original dataset to correct all the misspellings which are encountered.

import Levenshtein

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
    length = max(len(str1), len(str2))
    return 1 - (d*1.0/length)


def score_misspellings(df_mis, word):
    for r in df_mis.iterrows():
        row = r[1]
        index = r[0]

        df_mis.loc[index, "score"] = distance_score(row["term"].upper(),word.upper())
    try:
        df_mis.sort(["score", "occurrences"])
        return df_mis.iloc[0]
    except:
        return None

    
for r in df.iterrows():
    row = r[1]
    index = r[0]
    word = row["word"]
    found = None
    if len(word) > 3:
        found = data_getter_abp.get_freq(word)
    else:
        continue
    
    if found is None:

        df_mis = get_misspellings(word)
        correct_spelling = score_misspellings(df_mis,word)
        
        try:
            if correct_spelling["score"]>0.75:
                df.loc[index,"correct_spelling"] = correct_spelling['term'].upper()
                print "word is {} correct spelling is {} score {}".format(word,correct_spelling["term"], correct_spelling["score"])
            else:
                pass
        except:
            pass
        
translate_dict = df[~pd.isnull(df["correct_spelling"])][["word","correct_spelling"]].to_dict(orient="records")
translate_dict = {d["word"]: d["correct_spelling"] for d in translate}

# Finally correct the spellings on the original file using this dict
df = pd.read_csv("random_sample.csv", dtype={'full_address': str})
df["full_address"] = df["full_address"].fillna("")

import re
def translate_add(full_address, translate_dict):
    import re
    full_address = full_address.upper()
    pattern = re.compile(r'\b(' + '|'.join(translate_dict.keys()) + r')\b')
    result = pattern.sub(lambda x: translate_dict[x.group()], full_address)
    return result.upper()

df["translated_address"] = df["full_address"].apply(translate_add, args=(translate_dict,))

df.to_csv("random_sample_translated.csv", encoding="utf-8")