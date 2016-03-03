
from address_matcher import Address, Matcher
import sys

# Simple utility function that takes an address string and returns the match object
# This contains the list of potential matches, the best matches etc
def get_matches(address_string, data_getter):
    address = Address(address_string, data_getter=data_getter)
    matcher_abp = Matcher(data_getter,address)
    matcher_abp.load_potential_matches()
    matcher_abp.find_match()
    return matcher_abp


def match_id_and_commit(id, session, data_getter, la_conn, Address):
    this_address = session.query(Address).filter(Address.id == id).one()

    try:
        matches = get_matches(this_address.address_cleaned, data_getter)

        try:
            # Only print matches if they have different postcodes
            stored_pc = set()
            to_join = []
            for m in matches.potential_matches:
                if m.postcode not in stored_pc:
                    to_join.append(repr(m))
                    stored_pc.add(m.postcode)
            this_address.alternative_matches_string = u"\n".join(to_join[:10])
        except:
            pass

        matches.potential_matches = [p for p in matches.potential_matches if p.relative_score > -0.1]

        num_la_matches = get_num_la_matches(matches, la_conn)

        this_address.num_la_matches = num_la_matches
        this_address.probability = matches.best_match.probability
        this_address.score = matches.best_match.match_score
        this_address.match_desc = matches.best_match.match_description
        this_address.abp_full_address = matches.best_match.full_address
        this_address.abp_postcode = matches.best_match.postcode
        this_address.single_match = matches.one_match_only
        this_address.distinguishability = matches.distinguishability
        this_address.match_attempted = True

        try:
            this_address.local_authority = get_la_best_match(matches, la_conn)
        except:
            pass

        session.add(this_address)
        session.commit()

    except Exception as e:
        print id
        print(sys.exc_info()[0])

def get_num_la_matches(address_matcher, la_conn):
    """
    Given a list of points return how many distinct local authorities they belong to
    """

    sql = """
    select distinct code, name
    from las
    where {}
    """

    if address_matcher.potential_matches:
        wkt_points = [p.geom_wkt for p in address_matcher.potential_matches]
        wkt_points = ["st_contains(geom, st_geomfromtext('{}'))".format(p) for p in wkt_points]
        or_clause = " or \n".join(wkt_points)
        cur = la_conn.cursor()
        cur.execute(sql.format(or_clause))
        return len(cur.fetchall())
    else:
        return None

def get_la_best_match(address_matcher, la_conn):

    sql = """
    select distinct code, name
    from las
    where {}
    """

    if "SRID" in address_matcher.best_match.geom_wkt:
        contains = "st_contains(geom, st_geomfromtext('{}'))".format(address_matcher.best_match.geom_wkt)
        sql_new = sql.format(contains)
        cur = la_conn.cursor()
        cur.execute(sql_new)
        record = cur.fetchall()
        return record[0][1]

import Levenshtein
def misordered(target_address,matched_address):
    """
    This function returns a score that indicates whether
    the matching tokens are in the same order, and if not,
    by how much they are out

    Returns 1 if same order, and a 0 < number < 1
    if out of order.

    The worse they are out of order, the nearer 0
    """

    # First get tokens which are in both addresses
    t1 = target_address.split(" ")
    t2 = matched_address.split(" ")

    in_both = set(t1).intersection(set(t2))

    #Get the location of the tokens which are in both in the target address, and sort - what order do they appear in
    in_order_of_app_target = [t1.index(i) for i in in_both]
    in_order_of_app_target.sort()

    # Note we're not interested in their location, only their order - see http://stackoverflow.com/questions/6422700/how-to-get-indices-of-a-sorted-array-in-python
    target_order = sorted(range(len(in_order_of_app_target)),key=lambda x:in_order_of_app_target[x])

    in_order_of_app_target_tokens =  [t1[i] for i in in_order_of_app_target]
    in_order_of_app_matched = [t2.index(i) for i in in_order_of_app_target_tokens]
    matched_order = sorted(range(len(in_order_of_app_matched)),key=lambda x:in_order_of_app_matched[x])

    #This is a bit of a hack but probaby works ok
    str_t = "".join([repr(i) for i in target_order])
    str_m = "".join([repr(i) for i in matched_order])

    return Levenshtein.ratio(str_t, str_m)



