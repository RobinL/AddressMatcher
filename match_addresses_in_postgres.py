# Grab a list of all the ids of records which need to be matched
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:@localhost:5432/postgres')

Base = automap_base()


# reflect the tables
Base.prepare(engine, reflect=True)

Address = Base.classes.all_addresses_with_match_info

session = Session(engine)
ids = session.query(Address.id).filter(Address.match_attempted == False).all()
ids = [id[0] for id in ids]

for id in ids:


    this_address = session.query(Address).filter(Address.id == id).one()
    break


    try:
        matches = get_matches(row["translated_address"])

        matches.potential_matches = [p for p in matches.potential_matches if p.relative_score > -0.1]

        num_la_matches = get_num_la_matches(matches)

        this_address.num_la_matches = num_la_matches
        this_address.probability = matches.best_match.probability
        this_address.score = matches.best_match.match_score
        this_address.match_desc = matches.best_match.match_description
        this_address.abp_full_address = matches.best_match.full_address


        df.loc[index, "num_la_matches"] = num_la_matches
        df.loc[index, "probability"] = matches.best_match.probability
        df.loc[index, "score"] = matches.best_match.match_score
        df.loc[index, "match_desc"] = matches.best_match.match_description
        df.loc[index, "abp_full_address"] = matches.best_match.full_address
        df.loc[index, "abp_postcode"] = matches.best_match.postcode
        df.loc[index, "single_match"] = matches.one_match_only
        try:
            df.loc[index, "local_authority"] = get_la_best_match(matches)
        except:
            pass
    except:
        print index