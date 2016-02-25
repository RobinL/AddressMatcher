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


# This one's for looking up whether points fit into local authorities - not no timeout is set
con_string_la = "host='localhost' dbname='postgres' user='postgres' password=''"
la_conn = psycopg2.connect(con_string_la)

data_getter_abp = DataGetter_ABP(freq_conn=freq_con, data_conn=data_conn, SEARCH_INTENSITY=5000)

# Simple utility function that takes an address string and returns the match object
# This contains the list of potential matches, the best matches etc
def get_matches(address_string):
    address = Address(address_string, data_getter=data_getter_abp)
    matcher_abp = Matcher(data_getter_abp,address)
    matcher_abp.load_potential_matches()
    matcher_abp.find_match()
    return matcher_abp


def get_num_la_matches(matcher):
    """
    Given a list of points return how many distinct local authorities they belong to
    """

    sql = """
    select distinct code, name
    from las
    where {}
    """

    if matcher.potential_matches:
        wkt_points = [p.geom_wkt for p in matcher.potential_matches]
        wkt_points = ["st_contains(geom, st_geomfromtext('{}'))".format(p) for p in wkt_points]
        or_clause = " or \n".join(wkt_points)
        cur = la_conn.cursor()
        cur.execute(sql.format(or_clause))
        return len(cur.fetchall())
    else:
        return None

def get_la_best_match(matcher):

    sql = """
    select distinct code, name
    from las
    where {}
    """

    if "SRID" in matcher.best_match.geom_wkt:
        contains = "st_contains(geom, st_geomfromtext('{}'))".format(matcher.best_match.geom_wkt)
        sql_new = sql.format(contains)
        cur = la_conn.cursor()
        cur.execute(sql_new)
        record = cur.fetchall()
        return record[0][1]



