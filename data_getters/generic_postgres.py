import psycopg2
from address_matcher.data_getter_abc import DataGetterABC
from address_matcher.other_functions import memoize
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)
import pandas as pd
from pandas.io.sql import DatabaseError
import random

class DataGetter_Postgres_Generic(DataGetterABC):

    """
    The ability to narrow down a full dataset to a list of potential matches is critical to an efficient address
    matching algorithm.

    Each potential match will be scored and this is quite computationally intensive.  So the fewer potential matches
    the better.

    This class provides a generic template for a strategy to narrow down the full dataset to a list of potential
    matches, using full text searches in postgres.  You need a database with a fts index to ensure fts searches
    take ~10-100ms.

    It also provides a sample implementation of the get_freq function, which is used to create a probabilitic
    score for each match.
    """

    def __init__(self):
      
        con_string_freq = "host='localhost' dbname='abp' user='postgres' password='fsa_password' options='-c statement_timeout=300'"
        self.freq_con = psycopg2.connect(con_string_freq)

        con_string_data = "host='localhost' dbname='matching_data' user='postgres' password='fsa_password' options='-c statement_timeout=300'"
        self.data_con = psycopg2.connect(con_string_data)

        #SQL similar to the below will go here
        self.address_SQL = u"""
            select 
                id,
                full_address
            from dataase_of_addresses
            where to_tsvector('english',full_address)
                    @@ to_tsquery('english','{0}')
            limit {1};
            """

        self.freq_SQL = u"""
            select * 
            from term_frequencies 
            where term = '{}'
        """

        self.max_results = 101


    @memoize
    def get_freq(self,term):

        term = term.lower()

        SQL = self.freq_SQL.format(term)
        df = pd.read_sql(SQL,self.freq_con)
        #logger.debug("hit database term freq potenital token")

        if len(df)==1:
            main_prob = df.loc[0,"freq"]
        elif len(df)==0:
            main_prob = 3.0E-7
        else:
            logger.info("returned more than one result for a single term {}".format(potenital_token.lower()))
        return main_prob
    
    def df_to_address_objects(self,df):

        address_list= df.to_dict(orient="records")

        return_list = []

        for uprn, address_dict in address_list.values():
            id = address_dict["id"]
            address = Address(address_dict["full_address"])
            address.postcode = None
            address.business = None
            address.id = address_dict["id"]
            address.geom_wkt = None

            return_list.append(address) 

        return return_list


    def get_potential_matches_from_address(self, address):

            """
            Does full text searches using the tokens of the address.

            Begins by searching for all the terms, then 
            deletes the most specific and searches on the remainder etc.

            Stops when it finds a result with >0 matches
            Returns nothing if all it can find is a result with > 500 matches

        
            """

            def get_potential_matches(sub_tokens):
                
                #Create a FTS SQL query using all these tokens

                search_tokens = " & ".join(sub_tokens)
              
                #logger.debug("before {}".format(search_tokens))
                SQL = self.token_SQL.format(search_tokens, limit)
                # logger.debug(SQL)
                try:
                    df = pd.read_sql(SQL,self.data_con)
                except DatabaseError as e:
                    logger.debug("db error")
                    df = pd.DataFrame()

                
                return df

            return_list = []

            logger.debug("------------")
            logger.debug("looking for: " + address.full_address)

            tokens = address.ordered_tokens_postcode
            limit = self.max_results

            #If the address has two token or less, don't even try to match
            if len(tokens)<3:
                return return_list

            #Start with full list of tokens (i.e. very specific)
            #and get more general by dropping the most specific tokens one
            #at a time until we find a match

            #Remember that the ordered tokens start with the postcode reversed.  
            #e.g. if address is 1 CHAPEL LANE, TOTTERNHOE, DUNSTABLE, LU6 2BZ
            #ordered tokens will be 2BZ LU6, 1, CHAPEL LANE, TOTTERNHOE, DUNSTABLE etc.

            #Do a specific to general search i.e. FTS
            #2BZ LU6, 1, CHAPEL LANE, TOTTERNHOE, DUNSTABLE
            #LU6, 1, CHAPEL LANE, TOTTERNHOE, DUNSTABLE
            #1, CHAPEL LANE, TOTTERNHOE, DUNSTABLE
            #etc

            for i in range(len(tokens)):

                sub_tokens = tokens[i:]
                if len(sub_tokens)<=3:
                    df= pd.DataFrame()
                    break

                df = get_potential_matches(sub_tokens)

                #If there's a single match, then we've very likely found the right address.  Return just the one
                # if len(df) == 1:
                #     return self.df_to_address_objects(df)

                if len(df)>0 and len(df)<limit:
                    return_list.extend(self.df_to_address_objects(df))
                    break



            #Now try going in the opposite direction - i.e. getting rid of the latter
            #parts of the address first

            #Do a specific to general search i.e. FTS
            #2BZ LU6, 1, CHAPEL LANE, TOTTERNHOE, DUNSTABLE
            #2BZ LU6, 1, CHAPEL LANE, TOTTERNHOE
            #2BZ LU6, 1, CHAPEL LANE
            #2BZ LU6, 1, CHAPEL 
            #etc
            #This ends with just a postcode search

            for i in range(1,len(tokens)):
                sub_tokens = tokens[:-i]
                if len(sub_tokens)<=2: #to make sure it ends with a postcode search
                    break
                df = get_potential_matches(sub_tokens)

                #If there's a single match, then we've very likely found the right address.  Return just the one
                # if len(df) == 1:
                #     return self.df_to_address_objects(df)

                if len(df)>0 and len(df)<limit:
                    # logger.debug(sub_tokens)
                    return_list.extend(self.df_to_address_objects(df))
                    break



            #If we still haven't found anything make a last ditch attempt by taking random selections
            # of the tokens
            num_tokens = len(tokens)

            if len(return_list) < 1:  

                tried = []
                num_tokens = len(tokens)
                if num_tokens > 10:
                        take = num_tokens-5 #at least 6
                elif num_tokens > 8:
                    take = num_tokens-4 #at least 5
                else:
                    take = num_tokens-1


                if address.postcode:
                    tokens = tokens + list(reversed(address.postcode.split(" ")))

                for i in range(500):


                    sub_tokens = random.sample(tokens, take)
                    
                    # logger.debug(", ".join(sub_tokens))
                    if tuple(sub_tokens) in tried: 
                        continue

                    df = get_potential_matches(sub_tokens)
                    
                    tried.append(tuple(sub_tokens))

                    if len(df)>0 and len(df)<limit:
                        return_list.extend(self.df_to_address_objects(df))
                        break

            #Finally deduplicate based on text of address 

            final_list = []
            full_address_set = set()
            for a in return_list:
                if a.full_address not in full_address_set:
                    final_list.append(a)
                    full_address_set.add(a.full_address)




            return final_list 