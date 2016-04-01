import psycopg2
from data_getters.data_getter_abc import DataGetterABC
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

    def __init__(self, freq_conn = None, data_conn=None, SEARCH_INTENSITY = 500, MAX_RESULTS=250):

        self.freq_con = freq_conn

        self.data_con = data_conn

        self.SEARCH_INTENSITY = SEARCH_INTENSITY

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

        self.max_results = MAX_RESULTS


    @memoize
    def get_freq(self,term):

        term = term.lower()

        SQL = self.freq_SQL.format(term)
        df = pd.read_sql(SQL,self.freq_con)
        #logger.debug("hit database term freq potenital token")

        if len(df)==1:
            main_prob = df.loc[0,"freq"]
        elif len(df)==0:
            main_prob = None #If there is a matching token which we've never seen...
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

            tokens = address.tokens_original_order_postcode
            # Get rid of tokens which aren't in AddressBasePremium

            if address.tokens_specific_to_general_by_freq:
                tokens_orig = [t for t in tokens if t in address.tokens_specific_to_general_by_freq]
            else:
                tokens_orig = tokens
            tokens_ordered = address.tokens_specific_to_general_by_freq
            limit = self.max_results

            #If the address has two token or less, don't even try to match
            if len(tokens)<3:
                return return_list

            #Start with full list of tokens 
            #and get more general by dropping the tokens left to right
            #at a time until we find a match


            #1, CHAPEL LANE, TOTTERNHOE, DUNSTABLE LU6 2BZ
            #CHAPEL LANE, TOTTERNHOE, DUNSTABLE LU6 2BZ
            #LANE, TOTTERNHOE, DUNSTABLE LU6 2BZ
            #TOTTERNHOE, DUNSTABLE LU6 2BZ
            #etc

            for tokens in [tokens_orig, tokens_ordered]:
                for i in range(len(tokens)):

                    sub_tokens = tokens[i:]
                    if len(sub_tokens)<3:
                        df= pd.DataFrame()
                        break

                    df = get_potential_matches(sub_tokens)

                    # If there's a single match, then we've very likely found the right address.  Return just the one
                    # if len(df) == 1:
                    #     return self.df_to_address_objects(df)

                    if len(df)>0 and len(df)<limit:
                        return_list.extend(self.df_to_address_objects(df))
                        break

            #Now try going in the opposite direction - i.e. getting rid of the latter
            #parts of the address first

            #Do a specific to general search i.e. FTS
            #1, CHAPEL LANE, TOTTERNHOE, DUNSTABLE LU6 2BZ
            #1, CHAPEL LANE, TOTTERNHOE, DUNSTABLE LU6 
            #1, CHAPEL LANE, TOTTERNHOE, DUNSTABLE
            #1, CHAPEL LANE, TOTTERNHOE, 
            #etc


            for tokens in [tokens_orig, tokens_ordered]:
                for i in range(1,len(tokens)):
                    sub_tokens = tokens[:-i]
                    if len(sub_tokens)<3: #to make sure it ends with a postcode search
                        break
                    df = get_potential_matches(sub_tokens)

                    # If there's a single match, then we've very likely found the right address.  Return just the one
                    # if len(df) == 1:
                    #     return self.df_to_address_objects(df)

                    if len(df)>0 and len(df)<limit:
                        return_list.extend(self.df_to_address_objects(df))
                        break

            if len(df) == 1:
                return self.df_to_address_objects(df)

            #If we still haven't found anything make a last ditch attempt by taking random selections
            # of the tokens
            num_tokens = len(tokens_ordered)

            if len(return_list) < 1 and num_tokens > 3:

                tried = []
                num_tokens = len(tokens_ordered)
                if num_tokens > 10:
                        take = num_tokens-5 #at least 6
                elif num_tokens > 8:
                    take = num_tokens-4 #at least 5
                elif num_tokens > 3:
                    take = num_tokens-1


                for i in range(self.SEARCH_INTENSITY):

                    sub_tokens = random.sample(tokens_ordered, take)
                    
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